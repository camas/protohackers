use std::{collections::HashMap, net::SocketAddr};

use log::info;

use rand::{thread_rng, Rng};
use sorted_vec::SortedVec;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};
use uuid::Uuid;

use crate::{
    connection::{Connection, ConnectionHandle},
    model::Job,
};

pub struct Server {
    cancellation_token: CancellationToken,
    command_tx: mpsc::Sender<Command>,
    connections: HashMap<Uuid, ConnectionHandle>,
    // TODO: don't think sorted vec does anything here. need fast insert/remove too
    job_queues: HashMap<String, SortedVec<Job>>,
    waiting: HashMap<Uuid, Vec<String>>,
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
    ConnectionShutdown {
        id: Uuid,
        aborted_jobs: Vec<Job>,
    },
    PutJob {
        connection_id: Uuid,
        queue: String,
        priority: u64,
        data: serde_json::Value,
    },
    GetJob {
        connection_id: Uuid,
        queues: Vec<String>,
        wait: bool,
    },
    DeleteJob {
        connection_id: Uuid,
        job_id: u64,
    },
    AbortJob {
        job: Job,
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
            connections: HashMap::new(),
            job_queues: HashMap::new(),
            waiting: HashMap::new(),
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
            } => self.handle_new_connection(tcp_stream, remote_address).await,
            Command::ConnectionShutdown { id, aborted_jobs } => {
                self.connection_shutdown(id, aborted_jobs).await
            }
            Command::PutJob {
                connection_id,
                queue,
                priority,
                data,
            } => {
                let id = thread_rng().gen::<u64>();
                let connection = self.connections.get(&connection_id).unwrap();
                connection.job_put_ok(id).await;
                let job = Job {
                    id,
                    priority,
                    queue,
                    data,
                };
                self.add_job(job).await;
            }
            Command::GetJob {
                connection_id,
                queues,
                wait,
            } => self.handle_get_job(connection_id, queues, wait).await,
            Command::DeleteJob {
                connection_id,
                job_id,
            } => {
                let mut deleted = false;
                if self.job_queues.values_mut().any(|queue| {
                    match queue.iter().position(|job| job.id == job_id) {
                        Some(index) => {
                            queue.remove_index(index);
                            true
                        }
                        None => false,
                    }
                }) {
                    deleted = true;
                }

                for connection in self.connections.values() {
                    if connection.try_delete_job(job_id).await.unwrap_or(false) {
                        deleted = true;
                    }
                }

                let connection = self.connections.get(&connection_id).unwrap();
                if deleted {
                    connection.job_deleted_ok().await;
                } else {
                    connection.no_job().await;
                }
            }
            Command::AbortJob { job } => self.add_job(job).await,
        }
    }

    async fn handle_new_connection(&mut self, tcp_stream: TcpStream, remote_address: SocketAddr) {
        let connection_id = Uuid::new_v4();
        info!("{} joined from {}", connection_id, remote_address);

        let connection_handle = Connection::start(
            connection_id,
            tcp_stream,
            self.cancellation_token.clone(),
            self.handle(),
        );
        self.connections.insert(connection_id, connection_handle);
    }

    async fn connection_shutdown(&mut self, id: Uuid, aborted_jobs: Vec<Job>) {
        self.connections.remove(&id);
        self.waiting.remove(&id);
        for job in aborted_jobs {
            self.add_job(job).await;
        }
    }

    async fn handle_get_job(&mut self, connection_id: Uuid, queues: Vec<String>, wait: bool) {
        let connection = self.connections.get(&connection_id).unwrap();
        let mut best = None;
        for queue_name in queues.iter() {
            let Some(queue) = self.job_queues.get_mut(queue_name) else {
                continue;
            };
            // let (queue_best_job_index, job) = queue
            //     .iter()
            //     .enumerate()
            //     .max_by_key(|(_, job)| job.priority)
            //     .unwrap();
            let (queue_best_job_index, job) = (0, queue.first().unwrap());

            let better = match best {
                Some((_, _, best_priority)) => best_priority < job.priority,
                None => true,
            };
            if better {
                best = Some((queue_name, queue_best_job_index, job.priority));
            }
        }

        if let Some((queue_name, queue_index, _)) = best {
            let queue = self.job_queues.get_mut(queue_name).unwrap();
            let job = queue.remove_index(queue_index);
            if queue.is_empty() {
                self.job_queues.remove(queue_name);
            }
            connection.add_job(job).await;
            return;
        }

        if wait {
            self.waiting.insert(connection_id, queues);
        } else {
            connection.no_job().await;
        }
    }

    async fn add_job(&mut self, job: Job) {
        let waiting_entry = self
            .waiting
            .iter()
            .find(|(_, wanted)| wanted.contains(&job.queue));
        if let Some((&connection_id, _)) = waiting_entry {
            let connection = self.connections.get(&connection_id).unwrap();
            connection.add_job(job).await;
            self.waiting.remove(&connection_id);
            return;
        }

        let queue = self.job_queues.entry(job.queue.clone()).or_default();
        queue.push(job);
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
    pub fn shutdown(&self) {
        self.cancellation_token.cancel();
    }

    pub fn wait_shutdown(&self) -> WaitForCancellationFuture {
        self.cancellation_token.cancelled()
    }

    async fn new_connection(&self, tcp_stream: TcpStream, remote_address: SocketAddr) {
        let _ = self
            .command_tx
            .send(Command::NewConnection {
                tcp_stream,
                remote_address,
            })
            .await;
    }

    pub async fn connection_shutdown(&self, id: Uuid, aborted_jobs: Vec<Job>) {
        let _ = self
            .command_tx
            .send(Command::ConnectionShutdown { id, aborted_jobs })
            .await;
    }

    pub async fn put_job(
        &self,
        connection_id: Uuid,
        queue: String,
        priority: u64,
        data: serde_json::Value,
    ) {
        let _ = self
            .command_tx
            .send(Command::PutJob {
                connection_id,
                queue,
                priority,
                data,
            })
            .await;
    }

    pub async fn get_job(&self, connection_id: Uuid, queues: Vec<String>, wait: bool) {
        let _ = self
            .command_tx
            .send(Command::GetJob {
                connection_id,
                queues,
                wait,
            })
            .await;
    }

    pub async fn delete_job(&self, connection_id: Uuid, job_id: u64) {
        let _ = self
            .command_tx
            .send(Command::DeleteJob {
                connection_id,
                job_id,
            })
            .await;
    }

    pub async fn abort_job(&self, job: Job) {
        let _ = self.command_tx.send(Command::AbortJob { job }).await;
    }
}
