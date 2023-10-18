use std::{net::SocketAddr, time::Duration};

use anyhow::bail;
use log::{error, info, trace};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    sync::mpsc,
    try_join,
};
use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};
use uuid::Uuid;

use crate::{
    packet::{ClientPacket, ServerPacket},
    server::{Plate, ServerHandle, Ticket, Timestamp},
};

const TCP_MAX_SIZE: usize = 65535;

pub struct Connection {
    uuid: Uuid,
    command_tx: mpsc::Sender<ConnectionCommand>,
    server_cancellation_token: CancellationToken,
    cancellation_token: CancellationToken,
    packet_tx: mpsc::Sender<ServerPacket>,
    server_handle: ServerHandle,
}

#[derive(Clone)]
pub struct ConnectionHandle {
    command_tx: mpsc::Sender<ConnectionCommand>,
    server_cancellation_token: CancellationToken,
    cancellation_token: CancellationToken,
}

#[derive(Debug)]
enum ConnectionCommand {
    HandlePacket { packet: ClientPacket },
    SendHeartbeat,
    SendTicket(Ticket),
    Error(anyhow::Error),
}

impl Connection {
    pub async fn run(
        tcp_stream: TcpStream,
        socket_addr: SocketAddr,
        server_cancellation_token: CancellationToken,
        server_handle: ServerHandle,
    ) -> (ConnectionHandle, Uuid) {
        trace!("Connection from {socket_addr}");

        let cancellation_token = CancellationToken::new();

        let (command_tx, command_rx) = mpsc::channel(1000);
        let (packet_tx, packet_rx) = mpsc::channel(1000);
        let connection = Connection {
            uuid: Uuid::new_v4(),
            command_tx,
            server_cancellation_token,
            cancellation_token,
            packet_tx,
            server_handle: server_handle.clone(),
        };
        let handle = connection.handle();
        let handle_ = connection.handle();
        let connection_uuid = connection.uuid;
        let connection_uuid_ = connection.uuid;

        tokio::spawn(async move {
            tokio::spawn(command_receive_loop(command_rx, connection));

            let (read_stream, write_stream) = tcp_stream.into_split();
            let read_task = read_loop(read_stream, handle.clone());
            let write_task = write_loop(write_stream, handle.clone(), packet_rx);

            if let Err(e) = try_join!(read_task, write_task) {
                let _ = handle.error(e).await;
            }

            let _ = server_handle.connection_closed(connection_uuid).await;
            handle.shutdown();

            trace!("Connection from {socket_addr} closed.");
        });

        (handle_, connection_uuid_)
    }

    async fn handle_command(&mut self, command: ConnectionCommand) {
        match command {
            ConnectionCommand::HandlePacket { packet } => {
                if let Err(e) = self.handle_packet(packet).await {
                    self.error(e).await;
                }
            }
            ConnectionCommand::SendHeartbeat => {
                let _ = self.packet_tx.send(ServerPacket::Heartbeat).await;
            }
            ConnectionCommand::SendTicket(ticket) => {
                info!("Connection {} sending ticket {:?}", self.uuid, ticket);
                let _ = self
                    .packet_tx
                    .send(ServerPacket::Ticket {
                        plate: ticket.plate.into_string(),
                        road: ticket.road,
                        mile1: ticket.mile1,
                        timestamp1: ticket.timestamp1.into_u32(),
                        mile2: ticket.mile2,
                        timestamp2: ticket.timestamp2.into_u32(),
                        speed: ticket.speed,
                    })
                    .await;
            }
            ConnectionCommand::Error(error) => {
                let _ = self.error(error).await;
            }
        }
    }

    async fn handle_packet(&mut self, packet: ClientPacket) -> anyhow::Result<()> {
        match packet {
            ClientPacket::Plate { plate, timestamp } => {
                self.server_handle
                    .plate_observed(self.uuid, Plate::new(plate)?, Timestamp::new(timestamp))
                    .await?;
            }
            ClientPacket::WantHeartbeat { interval } => {
                self.server_handle
                    .request_heartbeat(self.uuid, interval)
                    .await?
            }
            ClientPacket::IAmCamera { road, mile, limit } => {
                self.server_handle
                    .register_camera(self.uuid, road, mile, limit)
                    .await?
            }
            ClientPacket::IAmDispatcher { roads } => {
                self.server_handle
                    .register_dispatcher(self.uuid, roads)
                    .await?
            }
        }

        Ok(())
    }

    async fn error(&mut self, error: anyhow::Error) {
        error!("Connection error {}: {}", self.uuid, error.to_string());
        let _ = self
            .packet_tx
            .send(ServerPacket::Error {
                message: error.to_string(),
            })
            .await;
        self.cancellation_token.cancel();
    }

    fn handle(&self) -> ConnectionHandle {
        ConnectionHandle {
            command_tx: self.command_tx.clone(),
            server_cancellation_token: self.server_cancellation_token.clone(),
            cancellation_token: self.cancellation_token.clone(),
        }
    }
}

async fn command_receive_loop(
    mut command_rx: mpsc::Receiver<ConnectionCommand>,
    mut connection: Connection,
) {
    let connection_handle = connection.handle();
    loop {
        let command = tokio::select! {
            v = command_rx.recv() => v,
            _ = connection_handle.wait_shutdown() => return,
            _ = connection_handle.wait_server_shutdown() => return,
        };

        let Some(command) = command else { break };

        connection.handle_command(command).await;
    }
}

async fn read_loop(
    mut read_stream: OwnedReadHalf,
    connection_handle: ConnectionHandle,
) -> anyhow::Result<()> {
    let mut data = Vec::new();
    loop {
        let mut buffer = vec![0; TCP_MAX_SIZE];
        let size = tokio::select! {
            v = read_stream.read(&mut buffer) => v?,
            _ = connection_handle.wait_shutdown() => return Ok(()),
            _ = connection_handle.wait_server_shutdown() => return Ok(()),
        };
        if size == 0 {
            bail!("Connection closed");
        }
        buffer.truncate(size);

        data.extend(buffer.into_iter());

        loop {
            let (read_data, packet) = match ClientPacket::parse(&data) {
                Ok(packet) => packet,
                Err(nom::Err::Incomplete(_)) => break,
                Err(e) => {
                    connection_handle
                        .error(anyhow::Error::msg(e.to_string()))
                        .await;
                    return Ok(());
                }
            };
            data.drain(0..(data.len() - read_data.len()));

            connection_handle.handle_packet(packet).await;
        }
    }
}

async fn write_loop(
    mut write_stream: OwnedWriteHalf,
    connection_handle: ConnectionHandle,
    mut packet_rx: mpsc::Receiver<ServerPacket>,
) -> anyhow::Result<()> {
    let mut times_failed = 0;
    loop {
        let packet = tokio::select! {
            v = packet_rx.recv() => match v {
                Some(v) => v,
                None => return Ok(())
            },
            _ = connection_handle.wait_shutdown() => {
                // bad hack to get errors to send before closing
                times_failed += 1;
                if times_failed == 4 {
                    return Ok(())
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            },
            _ = connection_handle.wait_server_shutdown() => return Ok(()),
        };

        write_stream.write_all(&packet.to_bytes()).await?;
    }
}

impl ConnectionHandle {
    async fn handle_packet(&self, packet: ClientPacket) {
        let _ = self
            .command_tx
            .send(ConnectionCommand::HandlePacket { packet })
            .await;
    }

    pub async fn send_heartbeat(&self) {
        let _ = self.command_tx.send(ConnectionCommand::SendHeartbeat).await;
    }

    pub async fn send_ticket(&self, ticket: Ticket) {
        let _ = self
            .command_tx
            .send(ConnectionCommand::SendTicket(ticket))
            .await;
    }

    async fn error(&self, error: anyhow::Error) {
        let _ = self.command_tx.send(ConnectionCommand::Error(error)).await;
    }

    pub fn wait_server_shutdown(&self) -> WaitForCancellationFuture {
        self.server_cancellation_token.cancelled()
    }

    pub fn wait_shutdown(&self) -> WaitForCancellationFuture {
        self.cancellation_token.cancelled()
    }

    pub fn shutdown(&self) {
        self.cancellation_token.cancel()
    }
}
