use nom::{
    branch::alt,
    bytes::streaming::tag,
    combinator::{map, map_res},
    multi::{length_count, length_data},
    number::streaming::{be_u16, be_u32, be_u8},
    sequence::tuple,
    IResult, Parser,
};

#[derive(Debug)]
pub enum ClientPacket {
    Plate { plate: String, timestamp: u32 },
    WantHeartbeat { interval: u32 },
    IAmCamera { road: u16, mile: u16, limit: u16 },
    IAmDispatcher { roads: Vec<u16> },
}

#[derive(Debug)]
pub enum ServerPacket {
    Error {
        message: String,
    },
    Ticket {
        plate: String,
        road: u16,
        mile1: u16,
        timestamp1: u32,
        mile2: u16,
        timestamp2: u32,
        speed: u16,
    },
    Heartbeat,
}

impl ClientPacket {
    pub fn parse(input: &[u8]) -> IResult<&[u8], ClientPacket> {
        alt((
            parse_plate,
            parse_want_heartbeat,
            parse_i_am_camera,
            parse_i_am_dispatcher,
        ))
        .parse(input)
    }
}

impl ServerPacket {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut data = Vec::new();
        match self {
            ServerPacket::Error { message } => {
                data.push(0x10);
                write_string(&mut data, message);
            }
            ServerPacket::Ticket {
                plate,
                road,
                mile1,
                timestamp1,
                mile2,
                timestamp2,
                speed,
            } => {
                data.push(0x21);
                write_string(&mut data, plate);
                data.extend(road.to_be_bytes());
                data.extend(mile1.to_be_bytes());
                data.extend(timestamp1.to_be_bytes());
                data.extend(mile2.to_be_bytes());
                data.extend(timestamp2.to_be_bytes());
                data.extend(speed.to_be_bytes());
            }
            ServerPacket::Heartbeat => {
                data.push(0x41);
            }
        }
        data
    }
}

fn parse_plate(input: &[u8]) -> IResult<&[u8], ClientPacket> {
    map(
        tuple((tag(b"\x20"), parse_string, be_u32)),
        |(_, plate, timestamp)| ClientPacket::Plate { plate, timestamp },
    )
    .parse(input)
}

fn parse_want_heartbeat(input: &[u8]) -> IResult<&[u8], ClientPacket> {
    map(tuple((tag(b"\x40"), be_u32)), |(_, interval)| {
        ClientPacket::WantHeartbeat { interval }
    })
    .parse(input)
}

fn parse_i_am_camera(input: &[u8]) -> IResult<&[u8], ClientPacket> {
    map(
        tuple((tag(b"\x80"), be_u16, be_u16, be_u16)),
        |(_, road, mile, limit)| ClientPacket::IAmCamera { road, mile, limit },
    )
    .parse(input)
}

fn parse_i_am_dispatcher(input: &[u8]) -> IResult<&[u8], ClientPacket> {
    map(
        tuple((tag(b"\x81"), length_count(be_u8, be_u16))),
        |(_, roads)| ClientPacket::IAmDispatcher { roads },
    )
    .parse(input)
}

fn parse_string(input: &[u8]) -> IResult<&[u8], String> {
    map_res(length_data(be_u8), |data: &[u8]| {
        String::from_utf8(data.to_vec())
    })
    .parse(input)
}

fn write_string(data: &mut Vec<u8>, string: &str) {
    assert!(string.len() <= u8::MAX as usize);
    data.extend((string.len() as u8).to_be_bytes());
    data.extend(string.as_bytes());
}
