use std::{io, net::SocketAddr};

use chrono::NaiveDateTime;
use log::{debug, error, info};
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

use crate::packets::{ClientPacket, ServerPacket};

const TCP_MAX_SIZE: usize = 65535;

struct PriceEntry {
    time: NaiveDateTime,
    price: i32,
}

pub struct Connection {
    command_tx: mpsc::Sender<ConnectionCommand>,
    packet_tx: mpsc::Sender<ServerPacket>,
    server_cancellation_token: CancellationToken,
    cancellation_token: CancellationToken,
    prices: Vec<PriceEntry>,
}

impl Connection {
    pub async fn run(
        tcp_stream: TcpStream,
        socket_addr: SocketAddr,
        server_cancellation_token: CancellationToken,
    ) {
        info!("Connection from {socket_addr}");

        let cancellation_token = CancellationToken::new();

        let (command_tx, command_rx) = mpsc::channel(100);
        let (packet_tx, packet_rx) = mpsc::channel(100);
        let connection = Connection {
            command_tx,
            packet_tx,
            server_cancellation_token,
            cancellation_token,
            prices: Vec::new(),
        };
        let handle = connection.handle();

        tokio::spawn(command_receive_loop(command_rx, connection));

        let (read_stream, write_stream) = tcp_stream.into_split();
        let read_task = read_loop(read_stream, handle.clone());
        let write_task = write_loop(write_stream, handle.clone(), packet_rx);

        if let Err(e) = try_join!(read_task, write_task) {
            error!("Error: {e}");
        }

        handle.cancellation_token.cancel();

        info!("Connection from {socket_addr} closed.");
    }

    async fn handle_command(&mut self, command: ConnectionCommand) {
        match command {
            ConnectionCommand::HandlePacket(packet) => self.handle_packet(packet).await,
        }
    }

    async fn handle_packet(&mut self, packet: ClientPacket) {
        match packet {
            ClientPacket::Insert { time, price } => {
                debug!("Adding {price} at time {time}");
                self.prices.push(PriceEntry { time, price });
            }
            ClientPacket::Query { min_time, max_time } => {
                let prices_within_range = self
                    .prices
                    .iter()
                    .filter(|p| p.time >= min_time && p.time <= max_time)
                    .map(|p| p.price as i64)
                    .collect::<Vec<_>>();

                let average = if prices_within_range.is_empty() {
                    0
                } else {
                    prices_within_range.iter().sum::<i64>() / prices_within_range.len() as i64
                };
                debug!("Returning {average} for prices between {min_time} and {max_time}");
                let _ = self
                    .packet_tx
                    .send(ServerPacket::QueryResponse {
                        mean_price: average as i32,
                    })
                    .await;
            }
        }
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
            _ = connection_handle.server_has_shutdown() => return,
        };

        let command = match command {
            Some(v) => v,
            None => return,
        };
        connection.handle_command(command).await;
    }
}

async fn read_loop(
    mut read_stream: OwnedReadHalf,
    connection_handle: ConnectionHandle,
) -> std::io::Result<()> {
    let mut data = Vec::new();
    loop {
        let mut buffer = vec![0; TCP_MAX_SIZE];
        let size = tokio::select! {
            v = read_stream.read(&mut buffer) => v?,
            _ = connection_handle.has_shutdown() => return Ok(()),
            _ = connection_handle.server_has_shutdown() => return Ok(()),
        };
        buffer.truncate(size);

        data.extend(buffer.into_iter());

        while data.len() >= 9 {
            let mut remainder = data.split_off(9);

            let packet = match &data[0] {
                b'I' => ClientPacket::Insert {
                    time: bytes_as_datetime(&data[1..=4])?,
                    price: i32::from_be_bytes(data[5..=8].try_into().unwrap()),
                },
                b'Q' => ClientPacket::Query {
                    min_time: bytes_as_datetime(&data[1..=4])?,
                    max_time: bytes_as_datetime(&data[5..=8])?,
                },
                _ => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Not a valid packet",
                    ))
                }
            };

            connection_handle.handle_packet(packet).await;

            std::mem::swap(&mut data, &mut remainder);
        }
    }
}

async fn write_loop(
    mut write_stream: OwnedWriteHalf,
    connection_handle: ConnectionHandle,
    mut packet_rx: mpsc::Receiver<ServerPacket>,
) -> io::Result<()> {
    loop {
        let packet = tokio::select! {
            v = packet_rx.recv() => v,
            _ = connection_handle.has_shutdown() => return Ok(()),
            _ = connection_handle.server_has_shutdown() => return Ok(()),
        };
        let packet = match packet {
            Some(v) => v,
            None => return Ok(()),
        };

        let data = match packet {
            ServerPacket::QueryResponse { mean_price } => mean_price.to_be_bytes(),
        };

        write_stream.write_all(&data).await?;
    }
}

#[derive(Clone)]
struct ConnectionHandle {
    command_tx: mpsc::Sender<ConnectionCommand>,
    server_cancellation_token: CancellationToken,
    cancellation_token: CancellationToken,
}

impl ConnectionHandle {
    async fn handle_packet(&self, client_packet: ClientPacket) {
        let _ = self
            .command_tx
            .send(ConnectionCommand::HandlePacket(client_packet))
            .await;
    }

    pub fn server_has_shutdown(&self) -> WaitForCancellationFuture {
        self.server_cancellation_token.cancelled()
    }

    pub fn has_shutdown(&self) -> WaitForCancellationFuture {
        self.cancellation_token.cancelled()
    }
}

#[derive(Debug)]
enum ConnectionCommand {
    HandlePacket(ClientPacket),
}

fn bytes_as_datetime(data: &[u8]) -> io::Result<NaiveDateTime> {
    let seconds = i32::from_be_bytes(data.try_into().unwrap());
    NaiveDateTime::from_timestamp_opt(seconds as i64, 0).ok_or(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "Invalid number of seconds",
    ))
}
