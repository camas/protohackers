use std::net::SocketAddr;

use log::{error, info};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    sync::{mpsc, oneshot},
    try_join,
};
use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};
use uuid::Uuid;

use crate::{
    packets::{ClientPacket, ServerPacket},
    server::ServerHandle,
};

const TCP_MAX_SIZE: usize = 65535;

pub struct Connection {
    uuid: Uuid,
    command_tx: mpsc::Sender<ConnectionCommand>,
    packet_tx: mpsc::Sender<ServerPacket>,
    server_cancellation_token: CancellationToken,
    cancellation_token: CancellationToken,
    state: ConnectionState,
    server_handle: ServerHandle,
}

#[derive(Clone)]
pub struct ConnectionHandle {
    uuid: Uuid,
    command_tx: mpsc::Sender<ConnectionCommand>,
    server_cancellation_token: CancellationToken,
    cancellation_token: CancellationToken,
}

#[derive(Debug, Clone)]
enum ConnectionState {
    Joining,
    Joined { username: String },
}

#[derive(Debug)]
enum ConnectionCommand {
    HandlePacket(ClientPacket),
    UserJoined { username: String },
    UserLeft { username: String },
    UsersInRoom { usernames: Vec<String> },
    GetUsername(oneshot::Sender<String>),
    ChatMessage { username: String, message: String },
}

impl Connection {
    pub async fn run(
        tcp_stream: TcpStream,
        socket_addr: SocketAddr,
        server_handle: ServerHandle,
        server_cancellation_token: CancellationToken,
    ) -> ConnectionHandle {
        info!("Connection from {socket_addr}");

        let cancellation_token = CancellationToken::new();

        let (command_tx, command_rx) = mpsc::channel(100);
        let (packet_tx, packet_rx) = mpsc::channel(100);
        let _ = packet_tx.send(ServerPacket::WelcomeMessage).await;
        let connection = Connection {
            uuid: Uuid::new_v4(),
            command_tx,
            packet_tx,
            server_handle,
            server_cancellation_token,
            cancellation_token,
            state: ConnectionState::Joining,
        };
        let handle = connection.handle();
        let handle_ = handle.clone();

        tokio::spawn(async move {
            tokio::spawn(command_receive_loop(command_rx, connection));

            let (read_stream, write_stream) = tcp_stream.into_split();
            let read_task = read_loop(read_stream, handle.clone());
            let write_task = write_loop(write_stream, handle.clone(), packet_rx);

            if let Err(e) = try_join!(read_task, write_task) {
                error!("Error: {e}");
            }

            handle.cancellation_token.cancel();

            info!("Connection from {socket_addr} closed.");
        });

        handle_
    }

    async fn handle_command(&mut self, command: ConnectionCommand) {
        match command {
            ConnectionCommand::HandlePacket(packet) => self.handle_packet(packet).await,
            ConnectionCommand::GetUsername(response_tx) => match &self.state {
                ConnectionState::Joined { username } => {
                    let _ = response_tx.send(username.clone());
                }
                ConnectionState::Joining => {}
            },
            ConnectionCommand::UserJoined { username } => {
                if !self.joined() {
                    return;
                }
                let _ = self
                    .packet_tx
                    .send(ServerPacket::UserJoined { username })
                    .await;
            }
            ConnectionCommand::UserLeft { username } => {
                if !self.joined() {
                    return;
                }
                let _ = self
                    .packet_tx
                    .send(ServerPacket::UserLeft { username })
                    .await;
            }
            ConnectionCommand::UsersInRoom { usernames } => {
                let _ = self
                    .packet_tx
                    .send(ServerPacket::UsersInRoom { usernames })
                    .await;
            }
            ConnectionCommand::ChatMessage { username, message } => {
                if !self.joined() {
                    return;
                }
                let _ = self
                    .packet_tx
                    .send(ServerPacket::ChatMessage { username, message })
                    .await;
            }
        }
    }

    async fn handle_packet(&mut self, packet: ClientPacket) {
        let ClientPacket::Line(line) = packet;

        match &self.state {
            ConnectionState::Joining => {
                if !valid_username(&line) {
                    self.shutdown();
                    return;
                }
                let username = line;
                self.state = ConnectionState::Joined {
                    username: username.clone(),
                };
                self.server_handle.user_joined(self.uuid, username).await;
            }
            ConnectionState::Joined { username } => {
                self.server_handle
                    .chat_message(self.uuid, username.clone(), line)
                    .await;
            }
        }
    }

    fn handle(&self) -> ConnectionHandle {
        ConnectionHandle {
            uuid: self.uuid,
            command_tx: self.command_tx.clone(),
            server_cancellation_token: self.server_cancellation_token.clone(),
            cancellation_token: self.cancellation_token.clone(),
        }
    }

    fn joined(&self) -> bool {
        matches!(self.state, ConnectionState::Joined { .. })
    }

    fn shutdown(&self) {
        self.cancellation_token.cancel();
    }
}

fn valid_username(username: &str) -> bool {
    !username.is_empty() && username.chars().all(|c| c.is_ascii_alphanumeric())
}

async fn command_receive_loop(
    mut command_rx: mpsc::Receiver<ConnectionCommand>,
    mut connection: Connection,
) {
    let connection_handle = connection.handle();
    loop {
        let command = tokio::select! {
            v = command_rx.recv() => v,
            _ = connection_handle.wait_shutdown() => break,
            _ = connection_handle.wait_server_shutdown() => return,
        };

        let command = match command {
            Some(v) => v,
            None => break,
        };
        connection.handle_command(command).await;
    }

    if let ConnectionState::Joined { username } = connection.state {
        let _ = connection
            .server_handle
            .user_left(connection.uuid, username)
            .await;
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
            connection_handle.shutdown();
            return Ok(());
        }
        buffer.truncate(size);

        data.extend(buffer.into_iter());

        while let Some(split_index) = data.iter().position(|x| x == &b'\n') {
            let remainder = data.split_off(split_index + 1);

            let line = String::from_utf8(std::mem::replace(&mut data, remainder))?;
            let line = line.trim_end();

            let packet = ClientPacket::Line(line.to_string());

            connection_handle.handle_packet(packet).await;
        }
    }
}

async fn write_loop(
    mut write_stream: OwnedWriteHalf,
    connection_handle: ConnectionHandle,
    mut packet_rx: mpsc::Receiver<ServerPacket>,
) -> anyhow::Result<()> {
    loop {
        let packet = tokio::select! {
            v = packet_rx.recv() => match v {
                Some(v) => v,
                None => return Ok(())
            },
            _ = connection_handle.wait_shutdown() => return Ok(()),
            _ = connection_handle.wait_server_shutdown() => return Ok(()),
        };

        let mut data = match packet {
            ServerPacket::WelcomeMessage => {
                "Welcome to budgetchat! What shall I call you?".to_string()
            }
            ServerPacket::ChatMessage { username, message } => format!("[{username}] {message}"),
            ServerPacket::UserJoined { username } => format!("* {username} has joined the room"),
            ServerPacket::UserLeft { username } => format!("* {username} has left the room"),
            ServerPacket::UsersInRoom { usernames } => {
                let joined_usernames = usernames.join(", ");
                format!("* This room contains {joined_usernames}")
            }
        };
        data.push('\n');

        write_stream.write_all(&data.into_bytes()).await?;
    }
}

impl ConnectionHandle {
    pub fn uuid(&self) -> &Uuid {
        &self.uuid
    }

    pub async fn user_joined(&self, username: String) {
        let _ = self
            .command_tx
            .send(ConnectionCommand::UserJoined { username })
            .await;
    }

    pub async fn user_left(&self, username: String) {
        let _ = self
            .command_tx
            .send(ConnectionCommand::UserLeft { username })
            .await;
    }

    pub async fn users_in_room(&self, usernames: Vec<String>) {
        let _ = self
            .command_tx
            .send(ConnectionCommand::UsersInRoom { usernames })
            .await;
    }

    pub async fn username(&self) -> Option<String> {
        let (response_tx, response_rx) = oneshot::channel();
        let _ = self
            .command_tx
            .send(ConnectionCommand::GetUsername(response_tx))
            .await;
        response_rx.await.ok()
    }

    pub async fn chat_message(&self, username: String, message: String) {
        let _ = self
            .command_tx
            .send(ConnectionCommand::ChatMessage { username, message })
            .await;
    }

    async fn handle_packet(&self, client_packet: ClientPacket) {
        let _ = self
            .command_tx
            .send(ConnectionCommand::HandlePacket(client_packet))
            .await;
    }

    // pub async fn get_state(&self) -> ConnectionState {
    //     let (response_tx, response_rx) = oneshot::channel();
    // }

    pub fn wait_server_shutdown(&self) -> WaitForCancellationFuture {
        self.server_cancellation_token.cancelled()
    }

    pub fn wait_shutdown(&self) -> WaitForCancellationFuture {
        self.cancellation_token.cancelled()
    }

    pub fn shutdown(&self) {
        self.cancellation_token.cancel()
    }

    pub fn has_shutdown(&self) -> bool {
        self.cancellation_token.is_cancelled()
    }
}
