use crate::server::Server;
use log::info;

mod connection;
mod packet;
mod server;

#[tokio::main]
async fn main() {
    flexi_logger::Logger::try_with_str("trace")
        .unwrap()
        .adaptive_format_for_stdout(flexi_logger::AdaptiveFormat::WithThread)
        .log_to_stdout()
        .start()
        .unwrap();

    let server = Server::run().await;

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Shutdown signal received. Shutting down");
        }
        _ = server.wait_shutdown() => {}
    };
    server.shutdown();

    info!("Server shutdown");
}
