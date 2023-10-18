use std::net::SocketAddr;

use anyhow::anyhow;
use log::{error, trace};
use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::{
    message::Message,
    server::ServerHandle,
    utilities::{read_message, write_message, PROTOCOL, PROTOCOL_VERSION},
};

pub(crate) fn run_connection(
    server: ServerHandle,
    tcp_stream: TcpStream,
    remote_address: SocketAddr,
) {
    trace!("Running connection handler for client at {remote_address}");
    tokio::spawn(async move {
        let command_task = tokio::spawn(connection_loop(server.clone(), tcp_stream));
        tokio::select! {
            result = command_task => {
                if let Err(error) = result {
                    error!("Command task failed: {error}");
                }
            }
            _ = server.wait_shutdown() => {}
        }
        trace!("Connection from {remote_address} closed");
    });
}

async fn connection_loop(server: ServerHandle, mut tcp_stream: TcpStream) -> anyhow::Result<()> {
    let mut data = Vec::new();

    macro_rules! exit {
        ($error:expr) => {{
            let _ = tcp_stream
                .write_all(
                    &Message::Error {
                        message: $error.to_string(),
                    }
                    .to_bytes(),
                )
                .await;
            return Err(anyhow!($error));
        }};
    }

    let hello_message = Message::Hello {
        protocol: PROTOCOL.to_string(),
        version: PROTOCOL_VERSION,
    };
    tokio::select! {
        _ = write_message(&mut tcp_stream, hello_message) => (),
        _ = server.wait_shutdown() => return Err(anyhow!("Server shutdown")),
    };

    let message = tokio::select! {
        m = read_message(&mut tcp_stream, &mut data) => m,
        _ = server.wait_shutdown() => return Err(anyhow!("Server shutdown")),
    };
    match message {
        Ok(Message::Hello { .. }) => {}
        Ok(message) => {
            error!("Invalid first message sent: {message:?}");
            exit!("Invalid first message");
        }
        Err(e) => {
            error!("Error while reading message: {e}");
            exit!(e);
        }
    }

    loop {
        let message = tokio::select! {
            m = read_message(&mut tcp_stream, &mut data) => m,
            _ = server.wait_shutdown() => return Err(anyhow!("Server shutdown")),
        };
        match message {
            Ok(Message::SiteVisit {
                site_id,
                populations,
            }) => {
                server.site_visit(site_id, populations).await;
            }
            Ok(Message::Error { message }) => {
                error!("Client sent error: {message}");
                return Err(anyhow!(message));
            }
            Ok(message) => {
                error!("Client sent a message that wasn't SiteVisit: {message:?}");
                exit!("Invalid client message");
            }
            Err(error) => {
                error!("Error parsing message: {error}");
                exit!("Error parsing message");
            }
        }
    }
}
