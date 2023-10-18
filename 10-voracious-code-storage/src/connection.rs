use std::net::SocketAddr;

use log::{info, trace};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::{
    packet::{ClientPacket, PacketError, ServerPacket},
    server::ServerHandle,
};

const TCP_MAX_SIZE: usize = 65535;

pub struct Connection {}

impl Connection {
    pub fn run(server_handle: ServerHandle, tcp_stream: TcpStream, remote_address: SocketAddr) {
        tokio::spawn(async move {
            stream_loop(tcp_stream, server_handle).await;
            info!("{remote_address} disconnected");
        });
    }
}

async fn stream_loop(mut tcp_stream: TcpStream, server: ServerHandle) {
    macro_rules! write_or_return {
        ($packet:expr) => {
            let bytes = $packet.to_bytes();
            tokio::select! {
                _ = tcp_stream.write_all(&bytes) => (),
                _ = server.wait_shutdown() => return,
            };
        };
    }

    let mut data = Vec::new();

    macro_rules! read_while {
        ($condition:expr) => {
            while $condition {
                let mut buffer = vec![0; TCP_MAX_SIZE];
                let size = tokio::select! {
                    v = tcp_stream.read(&mut buffer) => v,
                    _ = server.wait_shutdown() => return,
                };
                let Ok(size) = size else {
                    return;
                };
                if size == 0 {
                    return;
                }

                buffer.truncate(size);
                data.extend(buffer);
            }
        };
    }

    loop {
        write_or_return!(ServerPacket::Ready);

        read_while!(!data.iter().any(|v| *v == b'\n'));

        let split_index = data.iter().position(|v| *v == b'\n').unwrap();

        let mut line_bytes = data.split_off(split_index + 1);
        std::mem::swap(&mut data, &mut line_bytes);

        let Ok(mut line) = String::from_utf8(line_bytes) else {
            write_or_return!(ServerPacket::Error {
                message: PacketError::InvalidUtf8.to_string(),
            });
            return;
        };
        line.pop();

        trace!("< {line}");

        let packet = match ClientPacket::parse(line) {
            Ok(v) => v,
            Err(e) => {
                write_or_return!(ServerPacket::Error {
                    message: e.to_string(),
                });
                if matches!(e, PacketError::IllegalMethod(_)) {
                    return;
                } else {
                    continue;
                }
            }
        };
        let packet = match packet {
            ClientPacket::PutPartial { file_path, size } => {
                let size = size as usize;
                read_while!(data.len() < size);
                let mut put_data = data.split_off(size);
                std::mem::swap(&mut data, &mut put_data);

                if put_data
                    .iter()
                    .any(|v| matches!(v, 0..=8 | 14..=31 | 127..=159))
                {
                    write_or_return!(ServerPacket::Error {
                        message: PacketError::InvalidUtf8.to_string(),
                    });
                    continue;
                }
                ClientPacket::Put {
                    file_path,
                    data: put_data,
                }
            }
            other => other,
        };
        let Some(response) = server.handle_packet(packet).await else {
            return;
        };

        write_or_return!(response);
    }
}
