use std::{collections::HashMap, sync::Arc};

use log::{error, info, trace};
use tokio::{net::UdpSocket, sync::mpsc};
use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};

use crate::packet::ClientPacket;

pub struct Server {
    command_tx: mpsc::Sender<ServerCommand>,
    cancellation_token: CancellationToken,
    data: HashMap<Vec<u8>, Vec<u8>>,
    socket: Arc<UdpSocket>,
}

#[derive(Clone)]
pub struct ServerHandle {
    command_tx: mpsc::Sender<ServerCommand>,
    cancellation_token: CancellationToken,
}

enum ServerCommand {
    HandlePacket(ClientPacket),
}

impl Server {
    pub async fn run() -> ServerHandle {
        info!("Server starting");

        let socket = Arc::new(UdpSocket::bind("0.0.0.0:4444").await.unwrap());

        let (command_tx, command_rx) = mpsc::channel(100);
        let cancellation_token = CancellationToken::new();

        let mut data = HashMap::new();
        data.insert(b"version".to_vec(), b"Camas' Key-Value Store 1.0".to_vec());

        let server = Server {
            command_tx: command_tx.clone(),
            cancellation_token: cancellation_token.clone(),
            data,
            socket: socket.clone(),
        };
        let handle = server.handle();

        tokio::spawn(command_receive_loop(command_rx, server));
        tokio::spawn(listen_loop(socket, handle.clone()));

        handle
    }

    async fn handle_command(&mut self, command: ServerCommand) {
        let ServerCommand::HandlePacket(packet) = command;
        match packet {
            ClientPacket::Insert { key, value } => {
                if key == b"version" {
                    return;
                }
                trace!("Insert {key:x?}={value:x?}");
                self.data.insert(key, value);
            }
            ClientPacket::Retrieve { socket_addr, key } => {
                let value = self.data.get(&key).map(Vec::clone).unwrap_or_else(Vec::new);
                trace!("Retrieve {key:?}={value:?}");
                let mut response = Vec::new();
                response.extend(key.into_iter());
                response.push(b'=');
                response.extend(value.into_iter());
                let _ = self.socket.send_to(&response, socket_addr).await;
            }
        }
    }

    fn handle(&self) -> ServerHandle {
        ServerHandle {
            command_tx: self.command_tx.clone(),
            cancellation_token: self.cancellation_token.clone(),
        }
    }
}

async fn command_receive_loop(mut command_rx: mpsc::Receiver<ServerCommand>, mut server: Server) {
    let server_handle = server.handle();
    loop {
        let command = tokio::select! {
            _ = server_handle.wait_shutdown() => return,
            command = command_rx.recv() => command,
        };
        let command = match command {
            Some(v) => v,
            None => return,
        };
        server.handle_command(command).await;
    }
}

async fn listen_loop(socket: Arc<UdpSocket>, server_handle: ServerHandle) {
    loop {
        let mut buffer = vec![0; 1000];
        let recv_result = tokio::select! {
            v = socket.recv_from(&mut buffer) => v,
            _ = server_handle.wait_shutdown() => return,
        };
        let (size, socket_addr) = match recv_result {
            Ok(v) => v,
            Err(e) => {
                error!("Error listening to connections. {}", e);
                server_handle.shutdown();
                return;
            }
        };
        buffer.truncate(size);

        let packet = match buffer.iter().position(|v| v == &b'=') {
            Some(index) => {
                let value = buffer.split_off(index + 1);
                buffer.truncate(index);
                ClientPacket::Insert { key: buffer, value }
            }
            None => ClientPacket::Retrieve {
                socket_addr,
                key: buffer,
            },
        };

        let _ = server_handle.handle_packet(packet).await;
    }
}

impl ServerHandle {
    async fn handle_packet(&self, packet: ClientPacket) {
        let _ = self
            .command_tx
            .send(ServerCommand::HandlePacket(packet))
            .await;
    }

    pub fn shutdown(&self) {
        self.cancellation_token.cancel();
    }

    pub fn wait_shutdown(&self) -> WaitForCancellationFuture {
        self.cancellation_token.cancelled()
    }
}
