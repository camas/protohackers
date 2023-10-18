use std::net::SocketAddr;

pub enum ClientPacket {
    Insert {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Retrieve {
        socket_addr: SocketAddr,
        key: Vec<u8>,
    },
}
