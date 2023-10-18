use std::net::SocketAddr;

use log::{error, info};
use regex::Regex;
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

const TCP_MAX_SIZE: usize = 65535;

pub struct Connection {
    command_tx: mpsc::Sender<ConnectionCommand>,
    server_cancellation_token: CancellationToken,
    cancellation_token: CancellationToken,
    client_message_tx: mpsc::Sender<Message>,
    remote_message_tx: mpsc::Sender<Message>,
}

#[derive(Clone)]
pub struct ConnectionHandle {
    command_tx: mpsc::Sender<ConnectionCommand>,
    server_cancellation_token: CancellationToken,
    cancellation_token: CancellationToken,
}

#[derive(Debug)]
enum ConnectionCommand {
    HandleMessage(Message, MessageSource),
}

#[derive(Debug, Clone, Copy)]
enum MessageSource {
    Client,
    Remote,
}

#[derive(Debug)]
struct Message(String);

impl Connection {
    pub async fn run(
        tcp_stream: TcpStream,
        socket_addr: SocketAddr,
        server_cancellation_token: CancellationToken,
    ) {
        info!("Connection from {socket_addr}");

        let Ok(remote_tcp_stream) = TcpStream::connect("chat.protohackers.com:16963").await else {
            error!("Failed to connect to remote!");
            return;
        };
        info!("Connected to remote");

        let cancellation_token = CancellationToken::new();

        let (command_tx, command_rx) = mpsc::channel(100);
        let (client_message_tx, client_message_rx) = mpsc::channel(100);
        let (remote_message_tx, remote_message_rx) = mpsc::channel(100);
        let connection = Connection {
            command_tx,
            server_cancellation_token,
            cancellation_token,
            client_message_tx,
            remote_message_tx,
        };
        let handle = connection.handle();

        tokio::spawn(async move {
            tokio::spawn(command_receive_loop(command_rx, connection));

            let (client_read_stream, client_write_stream) = tcp_stream.into_split();
            let (remote_read_stream, remote_write_stream) = remote_tcp_stream.into_split();
            let client_read_task =
                read_loop(client_read_stream, handle.clone(), MessageSource::Client);
            let client_write_task =
                write_loop(client_write_stream, handle.clone(), client_message_rx);
            let remote_read_task =
                read_loop(remote_read_stream, handle.clone(), MessageSource::Remote);
            let remote_write_task =
                write_loop(remote_write_stream, handle.clone(), remote_message_rx);

            if let Err(e) = try_join!(
                client_read_task,
                client_write_task,
                remote_read_task,
                remote_write_task
            ) {
                error!("Error: {e}");
            }

            handle.shutdown();

            info!("Connection from {socket_addr} closed.");
        });
    }

    async fn handle_command(&mut self, command: ConnectionCommand) {
        let ConnectionCommand::HandleMessage(message, message_source) = command;

        let edited_message = Message(replace_address(message.0));
        let _ = match message_source {
            MessageSource::Client => self.remote_message_tx.send(edited_message).await,
            MessageSource::Remote => self.client_message_tx.send(edited_message).await,
        };
    }

    fn handle(&self) -> ConnectionHandle {
        ConnectionHandle {
            command_tx: self.command_tx.clone(),
            server_cancellation_token: self.server_cancellation_token.clone(),
            cancellation_token: self.cancellation_token.clone(),
        }
    }
}

fn replace_address(message: String) -> String {
    let mut edited_message = message;
    let mut index = 0;
    while let Some(address_match) = Regex::new("(^| )(7[a-zA-Z0-9]{25,34})($| )")
        .unwrap()
        .captures_at(&edited_message, index)
    {
        let address_match = address_match.get(2).unwrap();
        index = address_match.start() + "7YWHMfk9JZe0LM0g1ZauHuiSxhI".len() - 1;
        let mut new_message = edited_message[..address_match.start()].to_string();
        new_message.push_str("7YWHMfk9JZe0LM0g1ZauHuiSxhI");
        new_message.push_str(&edited_message[(address_match.start() + address_match.len())..]);
        edited_message = new_message;
    }
    edited_message
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
    message_source: MessageSource,
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

            connection_handle
                .handle_message(Message(line.to_string()), message_source)
                .await;
        }
    }
}

async fn write_loop(
    mut write_stream: OwnedWriteHalf,
    connection_handle: ConnectionHandle,
    mut message_rx: mpsc::Receiver<Message>,
) -> anyhow::Result<()> {
    loop {
        let message = tokio::select! {
            v = message_rx.recv() => match v {
                Some(v) => v,
                None => return Ok(())
            },
            _ = connection_handle.wait_shutdown() => return Ok(()),
            _ = connection_handle.wait_server_shutdown() => return Ok(()),
        };

        let mut data = message.0;
        data.push('\n');

        write_stream.write_all(&data.into_bytes()).await?;
    }
}

impl ConnectionHandle {
    async fn handle_message(&self, message: Message, message_source: MessageSource) {
        let _ = self
            .command_tx
            .send(ConnectionCommand::HandleMessage(message, message_source))
            .await;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_address_replace() {
        assert_eq!(
            replace_address("7AAAABBBBCCCCDDDDEEEEGGGGHHHHH".to_string()),
            "7YWHMfk9JZe0LM0g1ZauHuiSxhI"
        );
        assert_eq!(
            replace_address(" 7AAAABBBBCCCCDDDDEEEEGGGGHHHHH".to_string()),
            " 7YWHMfk9JZe0LM0g1ZauHuiSxhI"
        );
        assert_eq!(
            replace_address("7AAAABBBBCCCCDDDDEEEEGGGGHHHHH ".to_string()),
            "7YWHMfk9JZe0LM0g1ZauHuiSxhI "
        );
        assert_eq!(
            replace_address(" 7AAAABBBBCCCCDDDDEEEEGGGGHHHHH ".to_string()),
            " 7YWHMfk9JZe0LM0g1ZauHuiSxhI "
        );
        assert_eq!(
            replace_address("7AAAABBBBCCCCDDDDEEEEGGGGHHHHH 7AAAABBBBCCCCDDDDEEEEGGGGHHHHH 7AAAABBBBCCCCDDDDEEEEGGGGHHHHH".to_string()),
            "7YWHMfk9JZe0LM0g1ZauHuiSxhI 7YWHMfk9JZe0LM0g1ZauHuiSxhI 7YWHMfk9JZe0LM0g1ZauHuiSxhI"
        );
    }
}
