use std::net::SocketAddr;

use log::{error, info, trace};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};
use uuid::Uuid;

use crate::connection::{Connection, ConnectionHandle};

pub struct Server {
    command_tx: mpsc::Sender<ServerCommand>,
    cancellation_token: CancellationToken,
    connections: Vec<ConnectionHandle>,
}

#[derive(Clone)]
pub struct ServerHandle {
    command_tx: mpsc::Sender<ServerCommand>,
    cancellation_token: CancellationToken,
}

enum ServerCommand {
    NewConnection(TcpStream, SocketAddr),
    UserJoined {
        connection_uuid: Uuid,
        username: String,
    },
    UserLeft {
        connection_uuid: Uuid,
        username: String,
    },
    ChatMessage {
        connection_uuid: Uuid,
        username: String,
        message: String,
    },
}

impl Server {
    pub async fn run() -> ServerHandle {
        info!("Server starting");

        let listener = TcpListener::bind("0.0.0.0:4444").await.unwrap();

        let (command_tx, command_rx) = mpsc::channel(100);
        let cancellation_token = CancellationToken::new();

        let server = Server {
            command_tx: command_tx.clone(),
            cancellation_token: cancellation_token.clone(),
            connections: Vec::new(),
        };
        let handle = server.handle();

        tokio::spawn(command_receive_loop(command_rx, server));
        tokio::spawn(listen_loop(listener, handle.clone()));

        handle
    }

    async fn handle_command(&mut self, command: ServerCommand) {
        match command {
            ServerCommand::NewConnection(tcp_stream, socket_addr) => {
                self.new_connection(tcp_stream, socket_addr).await;
            }
            ServerCommand::UserJoined {
                connection_uuid,
                username,
            } => {
                self.user_joined(connection_uuid, username).await;
            }
            ServerCommand::UserLeft {
                connection_uuid,
                username,
            } => {
                self.user_left(connection_uuid, username).await;
            }
            ServerCommand::ChatMessage {
                connection_uuid,
                username,
                message,
            } => {
                self.chat_message(connection_uuid, username, message).await;
            }
        };
    }

    async fn new_connection(&mut self, tcp_stream: TcpStream, socket_addr: SocketAddr) {
        let connection_handle = Connection::run(
            tcp_stream,
            socket_addr,
            self.handle(),
            self.cancellation_token.clone(),
        )
        .await;
        self.connections.push(connection_handle);
    }

    async fn user_joined(&mut self, connection_uuid: Uuid, username: String) {
        trace!("* {username} joined");
        if let Some(connection) = self.connection(connection_uuid) {
            let connection = connection.clone();
            let other_usernames = futures::future::join_all(
                self.other_connections(connection_uuid)
                    .map(|conn| conn.username()),
            )
            .await;
            let other_usernames = other_usernames.into_iter().flatten().collect::<Vec<_>>();
            connection.users_in_room(other_usernames).await;
        }
        for conn in self.other_connections(connection_uuid) {
            conn.user_joined(username.clone()).await;
        }
    }

    async fn user_left(&mut self, connection_uuid: Uuid, username: String) {
        trace!("* {username} left");
        for conn in self.other_connections(connection_uuid) {
            conn.user_left(username.clone()).await;
        }
    }

    async fn chat_message(&mut self, connection_uuid: Uuid, username: String, message: String) {
        trace!("[{username}] {message}");
        for conn in self.other_connections(connection_uuid) {
            conn.chat_message(username.clone(), message.clone()).await;
        }
    }

    fn handle(&self) -> ServerHandle {
        ServerHandle {
            command_tx: self.command_tx.clone(),
            cancellation_token: self.cancellation_token.clone(),
        }
    }

    fn connection(&self, connection_uuid: Uuid) -> Option<&ConnectionHandle> {
        self.connections
            .iter()
            .find(|conn| conn.uuid() == &connection_uuid)
    }

    fn alive_connections(&mut self) -> impl Iterator<Item = &ConnectionHandle> {
        self.connections.retain(|conn| !conn.has_shutdown());
        self.connections.iter()
    }

    fn other_connections(
        &mut self,
        connection_uuid: Uuid,
    ) -> impl Iterator<Item = &ConnectionHandle> {
        self.alive_connections()
            .filter(move |conn| conn.uuid() != &connection_uuid)
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

async fn listen_loop(tcp_listener: TcpListener, server_handle: ServerHandle) {
    loop {
        let tcp_stream = tokio::select! {
            _ = server_handle.wait_shutdown() => return,
            tcp_stream = tcp_listener.accept() => tcp_stream,
        };
        let (tcp_stream, socket_addr) = match tcp_stream {
            Ok(v) => v,
            Err(e) => {
                error!("Error listening to connections. {}", e);
                server_handle.shutdown();
                return;
            }
        };
        server_handle.new_connection(tcp_stream, socket_addr).await;
    }
}

impl ServerHandle {
    async fn new_connection(&self, tcp_stream: TcpStream, socket_addr: SocketAddr) {
        let _ = self
            .command_tx
            .send(ServerCommand::NewConnection(tcp_stream, socket_addr))
            .await;
    }

    pub async fn user_joined(&self, connection_uuid: Uuid, username: String) {
        let _ = self
            .command_tx
            .send(ServerCommand::UserJoined {
                connection_uuid,
                username,
            })
            .await;
    }

    pub async fn user_left(&self, connection_uuid: Uuid, username: String) {
        let _ = self
            .command_tx
            .send(ServerCommand::UserLeft {
                connection_uuid,
                username,
            })
            .await;
    }

    pub async fn chat_message(&self, connection_uuid: Uuid, username: String, message: String) {
        let _ = self
            .command_tx
            .send(ServerCommand::ChatMessage {
                connection_uuid,
                username,
                message,
            })
            .await;
    }

    pub fn shutdown(&self) {
        self.cancellation_token.cancel();
    }

    pub fn wait_shutdown(&self) -> WaitForCancellationFuture {
        self.cancellation_token.cancelled()
    }
}
