use log::{error, info};
use num_prime::nt_funcs::is_prime64;
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    task, try_join,
};

const TCP_MAX_SIZE: usize = 65535;

#[tokio::main]
async fn main() {
    flexi_logger::Logger::try_with_str("trace")
        .unwrap()
        .adaptive_format_for_stdout(flexi_logger::AdaptiveFormat::WithThread)
        .log_to_stdout()
        .start()
        .unwrap();

    Server::run().await;
}

struct Server {}

impl Server {
    async fn run() {
        info!("Server starting");

        let listener = TcpListener::bind("0.0.0.0:4444").await.unwrap();

        loop {
            let (tcp_stream, addr) = listener.accept().await.unwrap();
            info!("Client connected {}", addr);
            task::spawn(connection_loop(tcp_stream));
        }
    }
}

async fn connection_loop(tcp_stream: TcpStream) {
    let (mut read_stream, mut write_stream) = tcp_stream.into_split();

    let (client_packet_tx, mut client_packet_rx) = mpsc::channel::<ClientPacket>(1000);

    let read_task = task::spawn(async move {
        let mut current_line = String::new();
        'main: loop {
            let mut buffer = vec![0; TCP_MAX_SIZE];
            let size = match read_stream.read(&mut buffer).await {
                Ok(size) => size,
                Err(error) => {
                    error!("Error: {}", error);
                    break;
                }
            };

            if size == 0 {
                continue;
            }

            buffer.truncate(size);
            let string_data = match String::from_utf8(buffer) {
                Ok(v) => v,
                Err(e) => {
                    error!("Error parsing data as string: {}", e);
                    break;
                }
            };
            current_line += &string_data;

            while let Some((line, remainder)) = current_line.split_once('\n') {
                match serde_json::from_str::<serde_json::Value>(line) {
                    Ok(json) => {
                        if client_packet_tx
                            .send(ClientPacket::Json(json))
                            .await
                            .is_err()
                        {
                            break 'main;
                        }
                    }
                    Err(e) => {
                        error!("Malformed json! {}", e);
                        let _ = client_packet_tx.send(ClientPacket::Malformed).await;
                        break 'main;
                    }
                }

                current_line = remainder.to_string();
            }
        }
    });

    let write_task = task::spawn(async move {
        'main: loop {
            let packet = match client_packet_rx.recv().await {
                Some(packet) => packet,
                None => return,
            };

            let response_packet = match packet {
                ClientPacket::Json(json) => handle_json(json),
                ClientPacket::Malformed => ServerPacket::Malformed,
            };

            let response_data = match response_packet {
                ServerPacket::IsPrime(is_prime) => {
                    let json = json!({"method": "isPrime", "prime": is_prime});
                    let mut json_string = json.to_string();
                    json_string.push('\n');
                    json_string.into_bytes()
                }
                ServerPacket::Malformed => b"MALFORMED\n".to_vec(),
            };

            if let Err(e) = write_stream.write_all(&response_data).await {
                error!("Error sending data. {}", e);
                break 'main;
            }
        }
    });

    try_join!(read_task, write_task).unwrap();

    info!("Disconnected");
}

fn handle_json(json: serde_json::Value) -> ServerPacket {
    let method = match json.get("method") {
        Some(v) => v,
        None => return ServerPacket::Malformed,
    };
    match method {
        serde_json::Value::String(value) if value == "isPrime" => {}
        _ => return ServerPacket::Malformed,
    };

    let number = match json.get("number") {
        Some(v) => v,
        None => return ServerPacket::Malformed,
    };

    let integer = match number {
        serde_json::Value::Number(number) if number.is_i64() => number.as_i64().unwrap(),
        serde_json::Value::Number(number) if number.is_f64() => {
            return ServerPacket::IsPrime(false)
        }
        _ => return ServerPacket::Malformed,
    };

    if integer.is_negative() {
        return ServerPacket::IsPrime(false);
    }

    ServerPacket::IsPrime(is_prime64(integer.unsigned_abs()))
}

#[derive(Debug)]
enum ClientPacket {
    Json(serde_json::Value),
    Malformed,
}

#[derive(Debug)]
enum ServerPacket {
    IsPrime(bool),
    Malformed,
}
