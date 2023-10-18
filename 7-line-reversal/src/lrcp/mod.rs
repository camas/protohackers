use std::{
    collections::{hash_map::Entry, HashMap},
    io,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use log::trace;
use thiserror::Error;
use tokio::{
    net::{ToSocketAddrs, UdpSocket},
    sync::mpsc,
    time::sleep,
};
use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};

use self::packet::Packet;

mod packet;

type Result<T> = std::result::Result<T, LrcpError>;

const RETRANSMISSION_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_PACKET_SIZE: usize = 999;

struct Manager {
    command_tx: mpsc::Sender<Command>,
    cancellation_token: CancellationToken,
    send_packet_tx: mpsc::Sender<(Packet, SocketAddr)>,
    sessions: HashMap<i32, SessionInfo>,
    session_tx: mpsc::Sender<LrcpSession>,
}

#[derive(Clone)]
struct ManagerHandle {
    command_tx: mpsc::Sender<Command>,
    cancellation_token: CancellationToken,
}

pub struct LrcpListener {
    manager_handle: ManagerHandle,
    session_rx: mpsc::Receiver<LrcpSession>,
}

pub struct LrcpSession {
    id: i32,
    data_rx: mpsc::Receiver<Vec<u8>>,
    manager_handle: ManagerHandle,
}

struct SessionInfo {
    remote_address: SocketAddr,
    sent_length: i32,
    received_length: i32,
    data_tx: mpsc::Sender<Vec<u8>>,
    to_send: Vec<u8>,
}

enum Command {
    HandlePacket(Packet, SocketAddr),
    CheckAcknowledged { session_id: i32, position: i32 },
    SendData { session_id: i32, data: Vec<u8> },
}

#[derive(Debug, Error)]
pub enum LrcpError {
    #[error("io error: {0:?}")]
    IoError(#[from] io::Error),
}

impl LrcpListener {
    pub async fn bind<A: ToSocketAddrs>(
        address: A,
        cancellation_token: CancellationToken,
    ) -> Result<LrcpListener> {
        let (session_tx, session_rx) = mpsc::channel(100);

        let manager_handle = Manager::start(address, cancellation_token, session_tx).await?;

        Ok(LrcpListener {
            manager_handle,
            session_rx,
        })
    }

    pub async fn accept(&mut self) -> Option<LrcpSession> {
        self.session_rx.recv().await
    }
}

impl Drop for LrcpListener {
    fn drop(&mut self) {
        self.manager_handle.shutdown();
    }
}

impl LrcpSession {
    pub fn id(&self) -> i32 {
        self.id
    }

    pub async fn send(&mut self, data: Vec<u8>) {
        self.manager_handle.send_data(self.id, data).await;
    }

    pub async fn recv(&mut self) -> Option<Vec<u8>> {
        self.data_rx.recv().await
    }
}

impl Manager {
    async fn start<A: ToSocketAddrs>(
        address: A,
        cancellation_token: CancellationToken,
        session_tx: mpsc::Sender<LrcpSession>,
    ) -> Result<ManagerHandle> {
        let udp_socket = Arc::new(UdpSocket::bind(address).await?);

        let (command_tx, command_rx) = mpsc::channel(100);
        let (send_packet_tx, send_packet_rx) = mpsc::channel(100);
        let manager = Manager {
            command_tx,
            cancellation_token,
            send_packet_tx,
            sessions: HashMap::new(),
            session_tx,
        };
        let handle = manager.handle();

        tokio::spawn(command_receive_loop(command_rx, manager));
        tokio::spawn(read_loop(udp_socket.clone(), handle.clone()));
        tokio::spawn(write_loop(udp_socket, send_packet_rx, handle.clone()));

        Ok(handle)
    }

    async fn handle_command(&mut self, command: Command) {
        match command {
            Command::HandlePacket(packet, socket_address) => {
                self.handle_packet(packet, socket_address).await
            }
            Command::CheckAcknowledged {
                session_id,
                position,
            } => {
                self.check_acknowledged(session_id, position).await;
            }
            Command::SendData { session_id, data } => {
                self.handle_send_data(session_id, data).await;
            }
        }
    }

    async fn handle_packet(&mut self, packet: Packet, remote_address: SocketAddr) {
        match packet {
            Packet::Connect { session } => self.handle_connect(session, remote_address).await,
            Packet::Data {
                session,
                position,
                data,
            } => {
                self.handle_data(session, position, data, remote_address)
                    .await
            }
            Packet::Acknowledge { session, length } => {
                self.handle_acknowledge(session, length, remote_address)
                    .await
            }
            Packet::Close { session } => {
                self.handle_close(session, remote_address).await;
            }
        }
    }

    async fn handle_connect(&mut self, session_id: i32, remote_address: SocketAddr) {
        self.send_acknowledge(session_id, 0, remote_address).await;

        let Entry::Vacant(entry) = self.sessions.entry(session_id) else {
            return;
        };

        let (data_tx, data_rx) = mpsc::channel(100);

        entry.insert(SessionInfo {
            remote_address,
            sent_length: 0,
            received_length: 0,
            data_tx,
            to_send: Vec::new(),
        });

        let _ = self
            .session_tx
            .send(LrcpSession {
                id: session_id,
                manager_handle: self.handle(),
                data_rx,
            })
            .await;
    }

    async fn handle_data(
        &mut self,
        session_id: i32,
        position: i32,
        mut data: Vec<u8>,
        remote_address: SocketAddr,
    ) {
        let Some(session_info) = self.sessions.get_mut(&session_id) else {
            self.send_close(session_id, remote_address).await;
            return;
        };

        if position > session_info.received_length {
            let received_length = session_info.received_length;
            self.send_acknowledge(session_id, received_length, remote_address)
                .await;
            return;
        }

        let len_data_already_received = session_info.received_length - position;
        if len_data_already_received >= data.len() as i32 {
            let received_length = session_info.received_length;
            self.send_acknowledge(session_id, received_length, remote_address)
                .await;
            return;
        }
        if len_data_already_received > 0 {
            data.drain(0..len_data_already_received as usize);
        }

        session_info.received_length += data.len() as i32;
        if !data.is_empty() {
            let _ = session_info.data_tx.send(data).await;
        }

        let received_length = session_info.received_length;
        self.send_acknowledge(session_id, received_length, remote_address)
            .await;
    }

    async fn handle_acknowledge(
        &mut self,
        session_id: i32,
        length: i32,
        remote_address: SocketAddr,
    ) {
        let Some(session_info) = self.sessions.get_mut(&session_id) else {
            self.send_close(session_id, remote_address).await;
            return;
        };

        if length <= session_info.sent_length {
            return;
        }

        if length > session_info.sent_length + session_info.to_send.len() as i32 {
            self.send_close(session_id, remote_address).await;
            return;
        }

        let newly_acknowledged_len = length - session_info.sent_length;
        if newly_acknowledged_len == 0 {
            return;
        }

        session_info.sent_length += newly_acknowledged_len;
        session_info
            .to_send
            .drain(0..newly_acknowledged_len as usize);

        self.send_session_data(session_id, remote_address).await;
    }

    async fn handle_close(&mut self, session_id: i32, remote_address: SocketAddr) {
        self.send_close(session_id, remote_address).await;
        self.sessions.remove(&session_id);
    }

    async fn check_acknowledged(&self, session_id: i32, position: i32) {
        let Some(session_info) = self.sessions.get(&session_id) else {
            return;
        };
        if session_info.sent_length >= position {
            return;
        }
        self.send_session_data(session_id, session_info.remote_address)
            .await;
    }

    async fn send_session_data(&self, session_id: i32, remote_address: SocketAddr) {
        let Some(session_info) = self.sessions.get(&session_id) else {
            return;
        };

        if session_info.to_send.is_empty() {
            return;
        }

        let chunk_max_size = MAX_PACKET_SIZE
            - session_id.to_string().len()
            - session_info.sent_length.to_string().len()
            - b"/data////".len();

        let mut chunks = Vec::new();
        let mut current_chunk = Vec::new();
        for v in session_info.to_send.iter() {
            let added_len = match v {
                b'\\' | b'/' => 2,
                _ => 1,
            };
            if current_chunk.len() + added_len > chunk_max_size {
                chunks.push(current_chunk);
                current_chunk = Vec::new();
            }
            current_chunk.push(*v);
        }
        if !current_chunk.is_empty() {
            chunks.push(current_chunk);
        }

        let mut current_data_sent_len = 0;
        for chunk in chunks {
            self.send_data(
                session_id,
                session_info.sent_length + current_data_sent_len as i32,
                chunk.to_vec(),
                remote_address,
            )
            .await;
            current_data_sent_len += chunk.len();
        }

        let handle = self.handle();
        let position = session_info.sent_length + session_info.to_send.len() as i32;
        tokio::spawn(async move {
            sleep(RETRANSMISSION_TIMEOUT).await;
            handle.check_acknowledged(session_id, position).await;
        });
    }

    async fn handle_send_data(&mut self, session_id: i32, data: Vec<u8>) {
        let Some(session_info) = self.sessions.get_mut(&session_id) else {
            return;
        };
        session_info.to_send.extend(data);
        let remote_address = session_info.remote_address;
        self.send_session_data(session_id, remote_address).await;
    }

    async fn send_packet(&self, packet: Packet, socket_address: SocketAddr) {
        let _ = self.send_packet_tx.send((packet, socket_address)).await;
    }

    async fn send_acknowledge(&self, session_id: i32, length: i32, remote_address: SocketAddr) {
        self.send_packet(
            Packet::Acknowledge {
                session: session_id,
                length,
            },
            remote_address,
        )
        .await;
    }

    async fn send_data(
        &self,
        session_id: i32,
        position: i32,
        data: Vec<u8>,
        remote_address: SocketAddr,
    ) {
        self.send_packet(
            Packet::Data {
                session: session_id,
                position,
                data,
            },
            remote_address,
        )
        .await;
    }

    async fn send_close(&self, session_id: i32, remote_address: SocketAddr) {
        self.send_packet(
            Packet::Close {
                session: session_id,
            },
            remote_address,
        )
        .await;
    }

    fn handle(&self) -> ManagerHandle {
        ManagerHandle {
            command_tx: self.command_tx.clone(),
            cancellation_token: self.cancellation_token.clone(),
        }
    }
}

impl ManagerHandle {
    async fn handle_packet(&self, packet: Packet, socket_address: SocketAddr) {
        let _ = self
            .command_tx
            .send(Command::HandlePacket(packet, socket_address))
            .await;
    }

    async fn check_acknowledged(&self, session_id: i32, position: i32) {
        let _ = self
            .command_tx
            .send(Command::CheckAcknowledged {
                session_id,
                position,
            })
            .await;
    }

    async fn send_data(&self, session_id: i32, data: Vec<u8>) {
        let _ = self
            .command_tx
            .send(Command::SendData { session_id, data })
            .await;
    }

    fn wait_shutdown(&self) -> WaitForCancellationFuture {
        self.cancellation_token.cancelled()
    }

    fn shutdown(&self) {
        self.cancellation_token.cancel();
    }
}

async fn command_receive_loop(mut command_rx: mpsc::Receiver<Command>, mut manager: Manager) {
    let manager_handle = manager.handle();
    loop {
        let command = tokio::select! {
            _ = manager_handle.wait_shutdown() => return,
            command = command_rx.recv() => command,
        };
        let command = match command {
            Some(v) => v,
            None => return,
        };
        manager.handle_command(command).await;
    }
}

async fn read_loop(udp_socket: Arc<UdpSocket>, handle: ManagerHandle) {
    loop {
        let mut buffer = vec![0; 1000];
        let recv_result = tokio::select! {
            v = udp_socket.recv_from(&mut buffer) => v,
            _ = handle.wait_shutdown() => return,
        };

        let (size, remote_address) = match recv_result {
            Ok(v) => v,
            Err(_) => return,
        };
        buffer.truncate(size);

        let (_, packet) = match Packet::parse(&buffer) {
            Ok(v) => v,
            Err(_) => continue,
        };

        trace!("< {} {}", remote_address, packet);
        handle.handle_packet(packet, remote_address).await;
    }
}

async fn write_loop(
    udp_socket: Arc<UdpSocket>,
    mut send_packet_rx: mpsc::Receiver<(Packet, SocketAddr)>,
    handle: ManagerHandle,
) {
    loop {
        let packet = tokio::select! {
            v = send_packet_rx.recv() => v,
            _ = handle.wait_shutdown() => return,
        };
        let Some((packet, remote_address)) = packet else {
            return;
        };
        trace!("> {} {}", remote_address, packet);
        let send_result = udp_socket.send_to(&packet.to_bytes(), remote_address).await;
        if send_result.is_err() {
            return;
        }
    }
}
