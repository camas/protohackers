use log::{error, info};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};

use crate::cipher::CipherSpecification;

pub struct Server {
    cancellation_token: CancellationToken,
}

#[derive(Clone)]
pub struct ServerHandle {
    cancellation_token: CancellationToken,
}

impl Server {
    pub async fn run() -> ServerHandle {
        info!("Server starting");

        let cancellation_token = CancellationToken::new();
        let listener = TcpListener::bind("0.0.0.0:4444").await.unwrap();

        let server = Server {
            cancellation_token: cancellation_token.clone(),
        };
        let handle = server.handle();

        tokio::spawn(accept_loop(listener, server.handle()));

        handle
    }

    fn handle(&self) -> ServerHandle {
        ServerHandle {
            cancellation_token: self.cancellation_token.clone(),
        }
    }
}

async fn accept_loop(listener: TcpListener, server_handle: ServerHandle) {
    loop {
        let session = tokio::select! {
            _ = server_handle.wait_shutdown() => return,
            v = listener.accept() => v,
        };
        let Ok((stream, remote_address)) = session else {
            info!("Listener closed");
            server_handle.shutdown();
            return;
        };
        info!("New connection from {}", remote_address);
        tokio::spawn(connection_task(stream, server_handle.clone()));
    }
}

async fn connection_task(mut stream: TcpStream, server_handle: ServerHandle) {
    let mut data = Vec::new();
    while !data.contains(&b'\x00') {
        let mut buffer = vec![0; 65535];
        let size = tokio::select! {
            v = stream.read(&mut buffer) => v,
            _ = server_handle.wait_shutdown() => return,
        };
        let Ok(size) = size else {
            return;
        };
        buffer.truncate(size);
        data.extend(buffer);
    }

    let split_index = data.iter().position(|v| v == &b'\x00').unwrap();
    let remaining_data = data.split_off(split_index + 1);
    let Ok((_, cipher_specification)) = CipherSpecification::parse(&data) else {
        error!("Bad cipher spec: {:?}", data);
        return;
    };

    if cipher_specification.is_noop() {
        error!("No-op cipher spec");
        return;
    }
    info!("Cipher spec: {:?}", cipher_specification);

    let mut recv_size = remaining_data.len() as u8;
    let mut send_size = 0_u8;
    let mut decoded_data = remaining_data
        .into_iter()
        .enumerate()
        .map(|(i, v)| cipher_specification.decode(v, i as u8))
        .collect::<Vec<_>>();

    loop {
        while let Some(split_index) = decoded_data.iter().position(|v| v == &b'\n') {
            let remainder = decoded_data.split_off(split_index + 1);
            let Ok(mut line) = String::from_utf8(std::mem::replace(&mut decoded_data, remainder))
            else {
                return;
            };
            line.pop();

            info!("Received parts: {}", line);

            let mut biggest_part = line
                .split(',')
                .map(|part| {
                    let (size, _) = part.split_once('x').unwrap();
                    let size = size.parse::<i32>().unwrap();
                    (size, part)
                })
                .max_by_key(|(a, _)| *a)
                .unwrap()
                .1
                .to_string();
            info!("Biggest part: {}", biggest_part);
            biggest_part.push('\n');

            let biggest_encoded = biggest_part
                .as_bytes()
                .iter()
                .enumerate()
                .map(|(i, v)| cipher_specification.encode(*v, send_size.wrapping_add(i as u8)))
                .collect::<Vec<_>>();
            send_size = send_size.wrapping_add(biggest_part.len() as u8);

            tokio::select! {
                _ = stream.write_all(&biggest_encoded) => (),
                _ = server_handle.wait_shutdown() => return,
            }
        }

        let mut buffer = vec![0; 65535];
        let size = tokio::select! {
            v = stream.read(&mut buffer) => v,
            _ = server_handle.wait_shutdown() => return,
        };
        let Ok(size) = size else {
            return;
        };
        buffer.truncate(size);

        let buffer_len = buffer.len();
        decoded_data.extend(
            buffer
                .into_iter()
                .enumerate()
                .map(|(i, v)| cipher_specification.decode(v, recv_size.wrapping_add(i as u8))),
        );
        recv_size = recv_size.wrapping_add(buffer_len as u8);
    }
}

impl ServerHandle {
    pub fn shutdown(&self) {
        self.cancellation_token.cancel();
    }

    pub fn wait_shutdown(&self) -> WaitForCancellationFuture {
        self.cancellation_token.cancelled()
    }
}
