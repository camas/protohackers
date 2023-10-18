use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    time::Duration,
};

use anyhow::bail;
use log::{error, info, trace};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};
use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};
use uuid::Uuid;

use crate::connection::{Connection, ConnectionHandle};

pub struct Server {
    command_tx: mpsc::Sender<ServerCommand>,
    cancellation_token: CancellationToken,
    connections: HashMap<Uuid, ConnectionHandle>,
    cameras: HashMap<Uuid, CameraData>,
    dispatchers: HashMap<Uuid, DispatcherData>,
    observations: Vec<Observation>,
    created_tickets: HashSet<(Plate, u32)>,
    ticket_queue: Vec<Ticket>,
    // Could probably be handled by the connection
    heartbeats: HashSet<Uuid>,
}

#[derive(Clone)]
pub struct ServerHandle {
    command_tx: mpsc::Sender<ServerCommand>,
    cancellation_token: CancellationToken,
}

enum ServerCommand {
    NewConnection(TcpStream, SocketAddr),
    ConnectionClosed {
        connection_uuid: Uuid,
    },
    PlateObserved {
        connection_uuid: Uuid,
        plate: Plate,
        timestamp: Timestamp,
        result_tx: oneshot::Sender<anyhow::Result<()>>,
    },
    RequestHeartbeat {
        connection_uuid: Uuid,
        interval_deciseconds: u32,
        result_tx: oneshot::Sender<anyhow::Result<()>>,
    },
    RegisterCamera {
        connection_uuid: Uuid,
        road: u16,
        mile: u16,
        limit: u16,
        result_tx: oneshot::Sender<anyhow::Result<()>>,
    },
    RegisterDispatcher {
        connection_uuid: Uuid,
        roads: Vec<u16>,
        result_tx: oneshot::Sender<anyhow::Result<()>>,
    },
    SendHeartbeat {
        connection_uuid: Uuid,
        result_tx: oneshot::Sender<anyhow::Result<()>>,
    },
}

#[derive(Debug, Clone, Copy)]
pub struct Timestamp(u32);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Plate(String);

struct CameraData {
    road: u16,
    mile: u16,
    limit: u16,
}

struct DispatcherData {
    roads: Vec<u16>,
}

struct Observation {
    plate: Plate,
    road: u16,
    mile: u16,
    time: Timestamp,
}

#[derive(Debug, Clone)]
pub struct Ticket {
    pub plate: Plate,
    pub road: u16,
    pub mile1: u16,
    pub timestamp1: Timestamp,
    pub mile2: u16,
    pub timestamp2: Timestamp,
    pub speed: u16,
}

impl Server {
    pub async fn run() -> ServerHandle {
        info!("Server starting");

        let listener = TcpListener::bind("0.0.0.0:4444").await.unwrap();

        let (command_tx, command_rx) = mpsc::channel(1000);
        let cancellation_token = CancellationToken::new();

        let server = Server {
            command_tx: command_tx.clone(),
            cancellation_token: cancellation_token.clone(),
            connections: HashMap::new(),
            cameras: HashMap::new(),
            dispatchers: HashMap::new(),
            observations: Vec::new(),
            created_tickets: HashSet::new(),
            ticket_queue: Vec::new(),
            heartbeats: HashSet::new(),
        };
        let handle = server.handle();

        tokio::spawn(command_receive_loop(command_rx, server));
        tokio::spawn(listen_loop(listener, handle.clone()));

        handle
    }

    async fn handle_command(&mut self, command: ServerCommand) {
        match command {
            ServerCommand::NewConnection(tcp_stream, socket_addr) => {
                self.new_connection(tcp_stream, socket_addr).await;
            }
            ServerCommand::ConnectionClosed { connection_uuid } => {
                self.connections.remove(&connection_uuid);
                self.cameras.remove(&connection_uuid);
                self.dispatchers.remove(&connection_uuid);
                info!("Connection closed {connection_uuid}");
            }
            ServerCommand::PlateObserved {
                connection_uuid,
                plate,
                timestamp,
                result_tx,
            } => {
                let result = self
                    .handle_plate_observed(connection_uuid, plate, timestamp)
                    .await;
                let _ = result_tx.send(result);
            }
            ServerCommand::RequestHeartbeat {
                connection_uuid,
                interval_deciseconds,
                result_tx,
            } => {
                let result = self
                    .handle_request_heartbeat(connection_uuid, interval_deciseconds)
                    .await;
                let _ = result_tx.send(result);
            }
            ServerCommand::RegisterCamera {
                connection_uuid,
                road,
                mile,
                limit,
                result_tx,
            } => {
                let result = self
                    .handle_register_camera(connection_uuid, road, mile, limit)
                    .await;
                let _ = result_tx.send(result);
            }
            ServerCommand::RegisterDispatcher {
                connection_uuid,
                roads,
                result_tx,
            } => {
                let result = self.handle_register_dipatcher(connection_uuid, roads).await;
                let _ = result_tx.send(result);
            }
            ServerCommand::SendHeartbeat {
                connection_uuid,
                result_tx,
            } => {
                let result = self.handle_send_heartbeat(connection_uuid).await;
                let _ = result_tx.send(result);
            }
        };
    }

    async fn new_connection(&mut self, tcp_stream: TcpStream, socket_addr: SocketAddr) {
        let (handle, uuid) = Connection::run(
            tcp_stream,
            socket_addr,
            self.cancellation_token.clone(),
            self.handle(),
        )
        .await;
        self.connections.insert(uuid, handle);
        trace!("New connection {uuid}");
    }

    async fn handle_plate_observed(
        &mut self,
        connection_uuid: Uuid,
        plate: Plate,
        timestamp: Timestamp,
    ) -> anyhow::Result<()> {
        let Some(_connection) = self.connections.get(&connection_uuid) else {
            bail!("Invalid connection");
        };
        let Some(camera_data) = self.cameras.get(&connection_uuid) else {
            bail!("Not a camera");
        };

        info!(
            "Observation {:?} {:?} {:?} {:?}",
            plate, timestamp, camera_data.road, camera_data.limit
        );

        for other_observation in self.observations.iter() {
            if other_observation.road != camera_data.road || other_observation.plate != plate {
                continue;
            }

            let mile_delta = other_observation.mile.abs_diff(camera_data.mile);
            let time_secs_delta = other_observation.time.0.abs_diff(timestamp.0);
            let speed_100times_mph = (mile_delta as u64 * 60 * 60 * 100) / time_secs_delta as u64;
            if speed_100times_mph <= camera_data.limit as u64 * 100 {
                continue;
            }

            let min_time = timestamp.0.min(other_observation.time.0);
            let max_time = timestamp.0.max(other_observation.time.0);
            let mut days = (min_time / 86400)..=(max_time / 86400);
            if days.any(|day| self.created_tickets.contains(&(plate.clone(), day))) {
                continue;
            }

            for day in (min_time / 86400)..=(max_time / 86400) {
                self.created_tickets.insert((plate.clone(), day));
            }

            let (mile1, timestamp1, mile2, timestamp2) = if min_time == timestamp.0 {
                (
                    camera_data.mile,
                    timestamp,
                    other_observation.mile,
                    other_observation.time,
                )
            } else {
                (
                    other_observation.mile,
                    other_observation.time,
                    camera_data.mile,
                    timestamp,
                )
            };

            let dispatcher = self
                .dispatchers
                .iter()
                .find(|(_, v)| v.roads.contains(&camera_data.road));
            let ticket = Ticket {
                plate: plate.clone(),
                road: camera_data.road,
                mile1,
                timestamp1,
                mile2,
                timestamp2,
                speed: speed_100times_mph as u16,
            };
            match dispatcher {
                Some((uuid, _)) => {
                    let dispatcher_handle = self.connections.get(uuid).unwrap();
                    dispatcher_handle.send_ticket(ticket).await;
                }
                None => self.ticket_queue.push(ticket),
            }
        }

        self.observations.push(Observation {
            plate,
            road: camera_data.road,
            mile: camera_data.mile,
            time: timestamp,
        });

        Ok(())
    }

    async fn handle_request_heartbeat(
        &mut self,
        connection_uuid: Uuid,
        interval_deciseconds: u32,
    ) -> anyhow::Result<()> {
        if !self.heartbeats.insert(connection_uuid) {
            bail!("Heartbeat already requested");
        }
        if interval_deciseconds == 0 {
            return Ok(());
        }

        tokio::spawn(heartbeat_loop(
            connection_uuid,
            interval_deciseconds,
            self.handle(),
        ));

        Ok(())
    }

    async fn handle_send_heartbeat(&mut self, connection_uuid: Uuid) -> anyhow::Result<()> {
        let Some(connection_handle) = self.connections.get(&connection_uuid) else {
            bail!("Connection closed");
        };

        connection_handle.send_heartbeat().await;

        Ok(())
    }

    async fn handle_register_camera(
        &mut self,
        connection_uuid: Uuid,
        road: u16,
        mile: u16,
        limit: u16,
    ) -> anyhow::Result<()> {
        if self.dispatchers.contains_key(&connection_uuid) {
            bail!("Already registered as a dispatcher");
        }

        let cameras_entry = self.cameras.entry(connection_uuid);
        match cameras_entry {
            std::collections::hash_map::Entry::Occupied(_) => {
                bail!("Already registered as a camera")
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(CameraData { road, mile, limit });
            }
        }

        Ok(())
    }

    async fn handle_register_dipatcher(
        &mut self,
        connection_uuid: Uuid,
        roads: Vec<u16>,
    ) -> anyhow::Result<()> {
        if self.cameras.contains_key(&connection_uuid) {
            bail!("Already registered as a camera");
        }
        match self.dispatchers.entry(connection_uuid) {
            std::collections::hash_map::Entry::Occupied(_) => {
                bail!("Already registered as a dispatcher")
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(DispatcherData {
                    roads: roads.clone(),
                });
            }
        }
        let connection = self.connections.get(&connection_uuid).unwrap();
        let mut to_send = Vec::new();
        self.ticket_queue.retain_mut(|ticket| {
            let retain = !roads.contains(&ticket.road);
            if !retain {
                to_send.push(ticket.clone());
            }
            retain
        });
        for ticket in to_send {
            connection.send_ticket(ticket).await;
        }

        Ok(())
    }

    fn handle(&self) -> ServerHandle {
        ServerHandle {
            command_tx: self.command_tx.clone(),
            cancellation_token: self.cancellation_token.clone(),
        }
    }
}

async fn command_receive_loop(mut command_rx: mpsc::Receiver<ServerCommand>, mut server: Server) {
    let server_handle = server.handle();
    loop {
        let command = tokio::select! {
            _ = server_handle.wait_shutdown() => return,
            command = command_rx.recv() => command,
        };
        let command = match command {
            Some(v) => v,
            None => return,
        };
        server.handle_command(command).await;
    }
}

async fn listen_loop(tcp_listener: TcpListener, server_handle: ServerHandle) {
    loop {
        let tcp_stream = tokio::select! {
            _ = server_handle.wait_shutdown() => return,
            tcp_stream = tcp_listener.accept() => tcp_stream,
        };
        let (tcp_stream, socket_addr) = match tcp_stream {
            Ok(v) => v,
            Err(e) => {
                error!("Error listening to connections. {}", e);
                server_handle.shutdown();
                return;
            }
        };
        server_handle.new_connection(tcp_stream, socket_addr).await;
    }
}

async fn heartbeat_loop(
    connection_uuid: Uuid,
    interval_deciseconds: u32,
    server_handle: ServerHandle,
) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(interval_deciseconds as u64 * 100)) => {}
            _ = server_handle.wait_shutdown() => return,
        }
        let _ = server_handle.send_heartbeat(connection_uuid).await;
    }
}

impl ServerHandle {
    async fn new_connection(&self, tcp_stream: TcpStream, socket_addr: SocketAddr) {
        let _ = self
            .command_tx
            .send(ServerCommand::NewConnection(tcp_stream, socket_addr))
            .await;
    }

    pub async fn connection_closed(&self, connection_uuid: Uuid) {
        let _ = self
            .command_tx
            .send(ServerCommand::ConnectionClosed { connection_uuid })
            .await;
    }

    pub async fn plate_observed(
        &self,
        connection_uuid: Uuid,
        plate: Plate,
        timestamp: Timestamp,
    ) -> anyhow::Result<()> {
        let (result_tx, result_rx) = oneshot::channel();
        let _ = self
            .command_tx
            .send(ServerCommand::PlateObserved {
                connection_uuid,
                plate,
                timestamp,
                result_tx,
            })
            .await;
        result_rx.await.unwrap_or_else(|_| bail!("Server closed"))
    }

    pub async fn request_heartbeat(
        &self,
        connection_uuid: Uuid,
        interval_deciseconds: u32,
    ) -> anyhow::Result<()> {
        let (result_tx, result_rx) = oneshot::channel();
        let _ = self
            .command_tx
            .send(ServerCommand::RequestHeartbeat {
                connection_uuid,
                interval_deciseconds,
                result_tx,
            })
            .await;
        result_rx.await.unwrap_or_else(|_| bail!("Server closed"))
    }

    pub async fn register_camera(
        &self,
        connection_uuid: Uuid,
        road: u16,
        mile: u16,
        limit: u16,
    ) -> anyhow::Result<()> {
        let (result_tx, result_rx) = oneshot::channel();
        let _ = self
            .command_tx
            .send(ServerCommand::RegisterCamera {
                connection_uuid,
                road,
                mile,
                limit,
                result_tx,
            })
            .await;
        result_rx.await.unwrap_or_else(|_| bail!("Server closed"))
    }

    pub async fn register_dispatcher(
        &self,
        connection_uuid: Uuid,
        roads: Vec<u16>,
    ) -> anyhow::Result<()> {
        let (result_tx, result_rx) = oneshot::channel();
        let _ = self
            .command_tx
            .send(ServerCommand::RegisterDispatcher {
                connection_uuid,
                roads,
                result_tx,
            })
            .await;
        result_rx.await.unwrap_or_else(|_| bail!("Server closed"))
    }

    async fn send_heartbeat(&self, connection_uuid: Uuid) -> anyhow::Result<()> {
        let (result_tx, result_rx) = oneshot::channel();
        let _ = self
            .command_tx
            .send(ServerCommand::SendHeartbeat {
                connection_uuid,
                result_tx,
            })
            .await;
        result_rx.await.unwrap_or_else(|_| bail!("Server closed"))
    }

    pub fn shutdown(&self) {
        self.cancellation_token.cancel();
    }

    pub fn wait_shutdown(&self) -> WaitForCancellationFuture {
        self.cancellation_token.cancelled()
    }
}

impl Plate {
    pub fn new(value: String) -> anyhow::Result<Plate> {
        if !value
            .chars()
            .all(|c| matches!(c, '0'..='9' | 'a'..='z' | 'A'..='Z'))
        {
            bail!("Invalid plate");
        }

        Ok(Plate(value))
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl Timestamp {
    pub fn new(value: u32) -> Timestamp {
        Timestamp(value)
    }

    pub fn into_u32(self) -> u32 {
        self.0
    }
}
