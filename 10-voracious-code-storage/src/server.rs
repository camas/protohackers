use std::{collections::BTreeMap, net::SocketAddr};

use log::info;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};
use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};

use crate::{
    connection::Connection,
    model::FileSystem,
    packet::{ClientPacket, Entry, PacketError, ServerPacket},
};

pub struct Server {
    cancellation_token: CancellationToken,
    command_tx: mpsc::Sender<Command>,
    filesystem: FileSystem,
}

#[derive(Clone)]
pub struct ServerHandle {
    cancellation_token: CancellationToken,
    command_tx: mpsc::Sender<Command>,
}

enum Command {
    NewConnection {
        tcp_stream: TcpStream,
        remote_address: SocketAddr,
    },
    HandlePacket {
        packet: ClientPacket,
        result_tx: oneshot::Sender<ServerPacket>,
    },
}

impl Server {
    pub async fn run() -> ServerHandle {
        info!("Server starting");

        let cancellation_token = CancellationToken::new();
        let (command_tx, command_rx) = mpsc::channel(10000);
        let listener = TcpListener::bind("0.0.0.0:4444").await.unwrap();

        let server = Server {
            cancellation_token: cancellation_token.clone(),
            command_tx,
            filesystem: FileSystem::default(),
        };
        let handle = server.handle();

        tokio::spawn(accept_loop(listener, server.handle()));
        tokio::spawn(command_loop(command_rx, server));

        handle
    }

    async fn handle_command(&mut self, command: Command) {
        match command {
            Command::NewConnection {
                tcp_stream,
                remote_address,
            } => {
                Connection::run(self.handle(), tcp_stream, remote_address);
            }
            Command::HandlePacket { packet, result_tx } => {
                let _ = result_tx.send(self.handle_packet(packet).await);
            }
        }
    }

    async fn handle_packet(&mut self, packet: ClientPacket) -> ServerPacket {
        match packet {
            ClientPacket::Help => ServerPacket::HelpMessage,
            ClientPacket::Get {
                file_path,
                revision,
            } => {
                let Some(file) = self.filesystem.get_file(&file_path) else {
                    return ServerPacket::Error {
                        message: "no such file".to_string(),
                    };
                };

                let data = match revision {
                    Some(revision) => {
                        let revision = revision as usize;
                        if file.revisions.len() < revision || revision == 0 {
                            return ServerPacket::Error {
                                message: PacketError::NoSuchRevision.to_string(),
                            };
                        }
                        &file.revisions[revision - 1]
                    }
                    None => file.revisions.last().unwrap(),
                };

                ServerPacket::GetResponse { data: data.clone() }
            }
            ClientPacket::PutPartial { .. } => unreachable!(),
            ClientPacket::Put { file_path, data } => {
                let revision = self.filesystem.put_file(&file_path, data);
                ServerPacket::PutResponse { revision }
            }
            ClientPacket::List { directory_path } => {
                let Some(directory) = self.filesystem.get_directory(&directory_path) else {
                    return ServerPacket::ListResponse {
                        entries: Vec::new(),
                    };
                };

                let mut entries = directory
                    .files
                    .iter()
                    .map(|(name, file)| {
                        (
                            name.clone(),
                            Entry::File {
                                name: name.clone(),
                                revision: file.revisions.len() as u32,
                            },
                        )
                    })
                    .collect::<BTreeMap<_, _>>();
                for directory_name in directory.directories.keys() {
                    if entries.contains_key(directory_name) {
                        continue;
                    }
                    entries.insert(
                        directory_name.clone(),
                        Entry::Directory {
                            name: directory_name.clone(),
                        },
                    );
                }

                let entries = entries.into_values().collect::<Vec<_>>();

                ServerPacket::ListResponse { entries }
            }
        }
    }

    fn handle(&self) -> ServerHandle {
        ServerHandle {
            cancellation_token: self.cancellation_token.clone(),
            command_tx: self.command_tx.clone(),
        }
    }
}

async fn accept_loop(listener: TcpListener, server_handle: ServerHandle) {
    loop {
        let session = tokio::select! {
            v = listener.accept() => v,
            _ = server_handle.wait_shutdown() => return,

        };
        let Ok((tcp_stream, remote_address)) = session else {
            info!("Listener closed");
            server_handle.shutdown();
            return;
        };
        info!("New connection from {}", remote_address);
        server_handle
            .new_connection(tcp_stream, remote_address)
            .await;
    }
}

async fn command_loop(mut command_rx: mpsc::Receiver<Command>, mut server: Server) {
    let server_handle = server.handle();
    loop {
        let command = tokio::select! {
            v = command_rx.recv() => v,
            _ = server_handle.wait_shutdown() => return,
        };
        let Some(command) = command else {
            return;
        };

        server.handle_command(command).await;
    }
}

impl ServerHandle {
    async fn new_connection(&self, tcp_stream: TcpStream, remote_address: SocketAddr) {
        let _ = self
            .command_tx
            .send(Command::NewConnection {
                tcp_stream,
                remote_address,
            })
            .await;
    }

    pub async fn handle_packet(&self, packet: ClientPacket) -> Option<ServerPacket> {
        let (result_tx, result_rx) = oneshot::channel();
        let _ = self
            .command_tx
            .send(Command::HandlePacket { packet, result_tx })
            .await;
        result_rx.await.ok()
    }

    pub fn shutdown(&self) {
        self.cancellation_token.cancel();
    }

    pub fn wait_shutdown(&self) -> WaitForCancellationFuture {
        self.cancellation_token.cancelled()
    }
}
