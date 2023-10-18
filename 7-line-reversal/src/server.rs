use log::info;

use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};

use crate::lrcp::{LrcpListener, LrcpSession};

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
        let listener = LrcpListener::bind("0.0.0.0:4444", cancellation_token.clone())
            .await
            .unwrap();

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

async fn accept_loop(mut lrcp_listener: LrcpListener, server_handle: ServerHandle) {
    loop {
        let session = tokio::select! {
            _ = server_handle.wait_shutdown() => return,
            v = lrcp_listener.accept() => v,
        };
        let Some(session) = session else {
            info!("Listener closed");
            server_handle.shutdown();
            return;
        };
        info!("New session {}", session.id());
        tokio::spawn(reverse_task(session, server_handle.clone()));
    }
}

async fn reverse_task(mut lrcp_session: LrcpSession, server_handle: ServerHandle) {
    let mut data = Vec::new();
    loop {
        let buffer = tokio::select! {
            _ = server_handle.wait_shutdown() => return,
            v = lrcp_session.recv() => v,
        };
        let Some(buffer) = buffer else {
            return;
        };
        data.extend(buffer);

        while let Some(split_index) = data.iter().position(|v| v == &b'\n') {
            let remainder = data.split_off(split_index + 1);
            let mut line = String::from_utf8(std::mem::replace(&mut data, remainder)).unwrap();

            line.pop();
            info!("{} received '{}'", lrcp_session.id(), line);
            let mut reversed = line.chars().rev().collect::<String>();
            info!("{} sending  '{}'", lrcp_session.id(), reversed);
            reversed.push('\n');

            lrcp_session.send(reversed.into_bytes()).await;
        }
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
