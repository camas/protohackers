use log::{info, trace};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    select,
    sync::{mpsc, oneshot},
};
use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};
use uuid::Uuid;

use crate::{
    model::Job,
    packet::{ClientPacket, ServerPacket},
    server::ServerHandle,
};

const TCP_MAX_SIZE: usize = 65535;

pub struct Connection {
    id: Uuid,
    cancellation_token: CancellationToken,
    server_cancellation_token: CancellationToken,
    command_tx: mpsc::Sender<Command>,
    jobs: Vec<Job>,
    packet_tx: mpsc::Sender<ServerPacket>,
    server_handle: ServerHandle,
}

#[derive(Clone)]
pub struct ConnectionHandle {
    cancellation_token: CancellationToken,
    server_cancellation_token: CancellationToken,
    command_tx: mpsc::Sender<Command>,
}

enum Command {
    HandlePacket(ClientPacket),
    SendError(String),
    Shutdown,
    AddJob(Job),
    NoJob,
    TryDeleteJob {
        job_id: u64,
        result_tx: oneshot::Sender<bool>,
    },
    JobDeletedOk,
    JobPutOk {
        job_id: u64,
    },
}

impl Connection {
    pub fn start(
        id: Uuid,
        tcp_stream: TcpStream,
        server_cancellation_token: CancellationToken,
        server_handle: ServerHandle,
    ) -> ConnectionHandle {
        let cancellation_token = CancellationToken::new();
        let (command_tx, command_rx) = mpsc::channel(1000);
        let (packet_tx, packet_rx) = mpsc::channel(1000);
        let connection = Connection {
            id,
            cancellation_token,
            server_cancellation_token,
            command_tx,
            jobs: Vec::new(),
            packet_tx,
            server_handle,
        };

        let handle = connection.handle();

        let (tcp_read_stream, tcp_write_stream) = tcp_stream.into_split();
        tokio::spawn(async move {
            let handle = connection.handle();

            let read_task = tokio::spawn(read_loop(tcp_read_stream, connection.handle()));
            let write_task =
                tokio::spawn(write_loop(tcp_write_stream, packet_rx, connection.handle()));
            tokio::spawn(command_loop(command_rx, connection));

            select!(_ = read_task => (), _ = write_task => ());

            handle.shutdown().await;
        });

        handle
    }

    async fn handle_command(&mut self, command: Command) {
        match command {
            Command::HandlePacket(packet) => self.handle_packet(packet).await,
            Command::SendError(message) => {
                let _ = self
                    .packet_tx
                    .send(ServerPacket::Error {
                        message: Some(message),
                    })
                    .await;
            }
            Command::Shutdown => {
                info!(
                    "{} shutdown. Jobs aborted {:?}",
                    self.id,
                    self.jobs.iter().map(|job| job.id).collect::<Vec<_>>()
                );
                self.server_handle
                    .connection_shutdown(self.id, std::mem::take(&mut self.jobs))
                    .await;
                self.cancellation_token.cancel();
            }
            Command::AddJob(job) => {
                self.jobs.push(job.clone());
                let _ = self
                    .packet_tx
                    .send(ServerPacket::OkGetResponse {
                        job_id: job.id,
                        data: job.data,
                        priority: job.priority,
                        queue: job.queue,
                    })
                    .await;
            }
            Command::NoJob => {
                let _ = self.packet_tx.send(ServerPacket::NoJob).await;
            }
            Command::TryDeleteJob { job_id, result_tx } => {
                let result = match self.jobs.iter().position(|job| job.id == job_id) {
                    Some(result_index) => {
                        self.jobs.remove(result_index);
                        true
                    }
                    None => false,
                };
                let _ = result_tx.send(result);
            }
            Command::JobDeletedOk => {
                let _ = self.packet_tx.send(ServerPacket::OkDeleteResponse).await;
            }
            Command::JobPutOk { job_id } => {
                let _ = self
                    .packet_tx
                    .send(ServerPacket::OkPutResponse { job_id })
                    .await;
            }
        }
    }

    async fn handle_packet(&mut self, packet: ClientPacket) {
        match packet {
            ClientPacket::Put {
                queue,
                priority,
                data,
            } => {
                self.server_handle
                    .put_job(self.id, queue, priority, data)
                    .await
            }
            ClientPacket::Get { queues, wait } => {
                self.server_handle.get_job(self.id, queues, wait).await
            }
            ClientPacket::Delete { job_id } => self.server_handle.delete_job(self.id, job_id).await,
            ClientPacket::Abort { job_id } => {
                let job_index = self.jobs.iter().position(|job| job.id == job_id);
                let Some(job_index) = job_index else {
                    let _ = self
                        .packet_tx
                        .send(ServerPacket::Error {
                            message: Some("Not working on job".to_string()),
                        })
                        .await;
                    return;
                };
                let job = self.jobs.remove(job_index);
                self.server_handle.abort_job(job).await;
                let _ = self.packet_tx.send(ServerPacket::OkAbortResponse).await;
            }
        }
    }

    fn handle(&self) -> ConnectionHandle {
        ConnectionHandle {
            cancellation_token: self.cancellation_token.clone(),
            server_cancellation_token: self.server_cancellation_token.clone(),
            command_tx: self.command_tx.clone(),
        }
    }
}

async fn command_loop(mut command_rx: mpsc::Receiver<Command>, mut connection: Connection) {
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

async fn read_loop(mut tcp_read_stream: OwnedReadHalf, handle: ConnectionHandle) {
    let mut data = Vec::new();
    loop {
        let mut buffer = vec![0; TCP_MAX_SIZE];
        let size = tokio::select! {
            v = tcp_read_stream.read(&mut buffer) => v,
            _ = handle.wait_shutdown() => return,
            _ = handle.wait_server_shutdown() => return,
        };
        let Ok(size) = size else {
            return;
        };
        if size == 0 {
            return;
        }

        buffer.truncate(size);
        data.extend(buffer);

        while let Some(split_index) = data.iter().position(|v| *v == b'\n') {
            let mut line_bytes = data.split_off(split_index + 1);
            std::mem::swap(&mut data, &mut line_bytes);

            let Ok(mut line) = String::from_utf8(line_bytes) else {
                handle.send_error("Invalid utf8".to_string()).await;
                continue;
            };
            line.pop();

            trace!("< {}", line);

            let Ok(json) = serde_json::from_str::<serde_json::Value>(&line) else {
                handle.send_error("Invalid json".to_string()).await;
                continue;
            };

            let packet = match ClientPacket::parse(json) {
                Ok(v) => v,
                Err(e) => {
                    handle.send_error(e.to_string()).await;
                    continue;
                }
            };

            handle.handle_packet(packet).await;
        }
    }
}

async fn write_loop(
    mut tcp_write_stream: OwnedWriteHalf,
    mut packet_rx: mpsc::Receiver<ServerPacket>,
    handle: ConnectionHandle,
) {
    loop {
        let packet = tokio::select! {
            v = packet_rx.recv() => v,
            _ = handle.wait_shutdown() => return,
            _ = handle.wait_server_shutdown() => return,
        };
        let Some(packet) = packet else {
            return;
        };

        let mut data = packet.to_json().to_string();
        trace!("> {}", data);
        data.push('\n');

        tokio::select! {
            _ = tcp_write_stream.write_all(data.as_bytes()) => (),
            _ = handle.wait_shutdown() => return,
            _ = handle.wait_server_shutdown() => return,
        }
    }
}

impl ConnectionHandle {
    async fn handle_packet(&self, packet: ClientPacket) {
        let _ = self.command_tx.send(Command::HandlePacket(packet)).await;
    }

    async fn send_error(&self, error_message: String) {
        let _ = self
            .command_tx
            .send(Command::SendError(error_message))
            .await;
    }

    pub async fn add_job(&self, job: Job) {
        let _ = self.command_tx.send(Command::AddJob(job)).await;
    }

    pub async fn no_job(&self) {
        let _ = self.command_tx.send(Command::NoJob).await;
    }

    /// Returns true if a job was deleted
    pub async fn try_delete_job(&self, job_id: u64) -> Option<bool> {
        let (result_tx, result_rx) = oneshot::channel();
        let _ = self
            .command_tx
            .send(Command::TryDeleteJob { job_id, result_tx })
            .await;
        result_rx.await.ok()
    }

    pub async fn job_deleted_ok(&self) {
        let _ = self.command_tx.send(Command::JobDeletedOk).await;
    }

    pub async fn job_put_ok(&self, job_id: u64) {
        let _ = self.command_tx.send(Command::JobPutOk { job_id }).await;
    }

    fn wait_server_shutdown(&self) -> WaitForCancellationFuture {
        self.server_cancellation_token.cancelled()
    }

    fn wait_shutdown(&self) -> WaitForCancellationFuture {
        self.cancellation_token.cancelled()
    }

    async fn shutdown(&self) {
        let _ = self.command_tx.send(Command::Shutdown).await;
    }
}
