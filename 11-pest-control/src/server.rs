use std::{collections::HashMap, net::SocketAddr};

use log::info;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};

use crate::{
    authority::{AuthorityClient, AuthorityClientHandle},
    connection::run_connection,
    message::SitePopulation,
    model::SiteID,
};

pub struct Server {
    cancellation_token: CancellationToken,
    command_tx: mpsc::Sender<Command>,
    authority_clients: HashMap<SiteID, AuthorityClientHandle>,
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
    SiteVisit {
        site_id: SiteID,
        populations: Vec<SitePopulation>,
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
            authority_clients: HashMap::new(),
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
                run_connection(self.handle(), tcp_stream, remote_address);
            }
            Command::SiteVisit {
                site_id,
                populations,
            } => {
                self.add_site_visit(site_id, populations).await;
            }
        }
    }

    async fn add_site_visit(&mut self, site_id: SiteID, populations: Vec<SitePopulation>) {
        let handle = self.handle();
        let authority_client = match self.authority_clients.entry(site_id) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let authority_client =
                    AuthorityClient::run(site_id, handle, self.cancellation_token.clone());
                entry.insert(authority_client)
            }
        };

        authority_client.site_visit(populations).await;
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

    pub async fn site_visit(&self, site_id: SiteID, populations: Vec<SitePopulation>) {
        let _ = self
            .command_tx
            .send(Command::SiteVisit {
                site_id,
                populations,
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
