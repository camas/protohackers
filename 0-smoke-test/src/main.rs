use log::{error, info};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    task, try_join,
};

#[tokio::main]
async fn main() {
    flexi_logger::Logger::try_with_str("trace")
        .unwrap()
        .adaptive_format_for_stdout(flexi_logger::AdaptiveFormat::WithThread)
        .log_to_stdout()
        .start()
        .unwrap();

    let mut server = Server::new().await;
    server.run().await;
}

struct Server {
    listener: TcpListener,
}

impl Server {
    async fn new() -> Server {
        info!("Server starting");

        let listener = TcpListener::bind("0.0.0.0:4444").await.unwrap();

        Server { listener }
    }

    async fn run(&mut self) {
        loop {
            let (tcp_stream, addr) = self.listener.accept().await.unwrap();
            info!("Client connected {}", addr);
            task::spawn(connection_loop(tcp_stream)).await.unwrap();
        }
    }
}

async fn connection_loop(tcp_stream: TcpStream) {
    let (mut read_stream, mut write_stream) = tcp_stream.into_split();

    let (event_tx, mut event_rx) = mpsc::channel::<Event>(1000);

    let read_task = task::spawn(async move {
        loop {
            let mut buffer = [0_u8; 65535];
            let size = match read_stream.read(&mut buffer).await {
                Ok(size) => size,
                Err(error) => {
                    error!("Error: {}", error);
                    break;
                }
            };

            if size == 0 {
                event_tx.send(Event::Eof).await.unwrap();
                break;
            } else {
                let data = buffer[..size].to_vec();
                if (event_tx.send(Event::Data(data)).await).is_err() {
                    break;
                }
            }
        }
    });

    let write_task = task::spawn(async move {
        loop {
            let event = match event_rx.recv().await {
                Some(event) => event,
                None => break,
            };

            match event {
                Event::Data(data) => match write_stream.write(&data).await {
                    Ok(size) => {
                        if size != data.len() {
                            panic!("Error. Size {} not same as data {}", size, data.len());
                        }
                    }
                    Err(error) => {
                        error!("Error: {}", error);
                        break;
                    }
                },
                Event::Eof => break,
            }
        }
    });

    try_join!(read_task, write_task).unwrap();

    info!("Disconnected");
}

#[derive(Debug)]
enum Event {
    Data(Vec<u8>),
    Eof,
}
