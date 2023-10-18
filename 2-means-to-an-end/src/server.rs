use std::net::SocketAddr;

use log::{error, info};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};

use crate::connection::Connection;

pub struct Server {
    command_tx: mpsc::Sender<ServerCommand>,
    cancellation_token: CancellationToken,
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
        };
        let handle = server.handle();

        tokio::spawn(command_receive_loop(command_rx, server));
        tokio::spawn(listen_loop(listener, handle.clone()));

        handle
    }

    async fn handle_command(&mut self, command: ServerCommand) {
        match command {
            ServerCommand::NewConnection(tcp_stream, socket_addr) => {
                self.new_connection(tcp_stream, socket_addr);
            }
        };
    }

    fn new_connection(&mut self, tcp_stream: TcpStream, socket_addr: SocketAddr) {
        tokio::spawn(Connection::run(
            tcp_stream,
            socket_addr,
            self.cancellation_token.clone(),
        ));
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
            _ = server_handle.has_shutdown() => return,
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
            _ = server_handle.has_shutdown() => return,
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

#[derive(Clone)]
pub struct ServerHandle {
    command_tx: mpsc::Sender<ServerCommand>,
    cancellation_token: CancellationToken,
}

impl ServerHandle {
    async fn new_connection(&self, tcp_stream: TcpStream, socket_addr: SocketAddr) {
        let _ = self
            .command_tx
            .send(ServerCommand::NewConnection(tcp_stream, socket_addr))
            .await;
    }

    pub fn shutdown(&self) {
        self.cancellation_token.cancel();
    }

    pub fn has_shutdown(&self) -> WaitForCancellationFuture {
        self.cancellation_token.cancelled()
    }
}

enum ServerCommand {
    NewConnection(TcpStream, SocketAddr),
}
