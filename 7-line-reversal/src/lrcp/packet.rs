use std::{
    borrow::Cow,
    fmt::{Display, Write},
};

use nom::{
    branch::alt,
    bytes::complete::{tag, take, take_while1},
    character::is_digit,
    combinator::{all_consuming, map, map_res},
    multi::many_till,
    sequence::{pair, tuple},
    IResult, Parser,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PacketError {
    #[error("Number too large")]
    NumberTooLarge,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Packet {
    Connect {
        session: i32,
    },
    Data {
        session: i32,
        position: i32,
        data: Vec<u8>,
    },
    Acknowledge {
        session: i32,
        length: i32,
    },
    Close {
        session: i32,
    },
}

impl Packet {
    pub fn parse(input: &[u8]) -> IResult<&[u8], Packet> {
        all_consuming(alt((
            parse_connect,
            parse_data,
            parse_acknowledge,
            parse_close,
        )))
        .parse(input)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        match self {
            Packet::Connect { session } => {
                buffer.extend(format!("/connect/{session}/").bytes());
            }
            Packet::Data {
                session,
                position,
                data,
            } => {
                buffer.extend(format!("/data/{session}/{position}/").bytes());
                buffer.extend(escape_data(data).iter());
                buffer.push(b'/');
            }
            Packet::Acknowledge { session, length } => {
                buffer.extend(format!("/ack/{session}/{length}/").bytes());
            }
            Packet::Close { session } => {
                buffer.extend(format!("/close/{session}/").bytes());
            }
        }
        buffer
    }
}

fn escape_data(data: &Vec<u8>) -> Cow<'_, Vec<u8>> {
    // premature optimisation. I wanted to use Cow

    if !data.iter().any(|&v| v == b'\\' || v == b'/') {
        return Cow::Borrowed(data);
    }

    let mut buffer = Vec::new();
    for v in data.iter() {
        if matches!(v, b'\\' | b'/') {
            buffer.push(b'\\');
        }
        buffer.push(*v);
    }
    Cow::Owned(buffer)
}

fn parse_connect(input: &[u8]) -> IResult<&[u8], Packet> {
    map(
        tuple((tag(b"/connect/"), number, tag(b"/"))),
        |(_, session, _)| Packet::Connect { session },
    )
    .parse(input)
}

fn parse_data(input: &[u8]) -> IResult<&[u8], Packet> {
    map(
        tuple((
            tag(b"/data/"),
            number,
            tag(b"/"),
            number,
            tag(b"/"),
            many_till(escaped_byte, tag(b"/")),
        )),
        |(_, session, _, position, _, (data, _))| Packet::Data {
            session,
            position,
            data: data.to_vec(),
        },
    )
    .parse(input)
}

fn parse_acknowledge(input: &[u8]) -> IResult<&[u8], Packet> {
    map(
        tuple((tag(b"/ack/"), number, tag(b"/"), number, tag(b"/"))),
        |(_, session, _, length, _)| Packet::Acknowledge { session, length },
    )
    .parse(input)
}

fn parse_close(input: &[u8]) -> IResult<&[u8], Packet> {
    map(
        tuple((tag(b"/close/"), number, tag(b"/"))),
        |(_, session, _)| Packet::Close { session },
    )
    .parse(input)
}

fn escaped_byte(input: &[u8]) -> IResult<&[u8], u8> {
    alt((
        map(
            pair(tag::<_, &[u8], _>(b"\\"), alt((tag(b"\\"), tag(b"/")))),
            |(_, v)| v[0],
        ),
        map(take(1_usize), |v: &[u8]| v[0]),
    ))
    .parse(input)
}

fn number(input: &[u8]) -> IResult<&[u8], i32> {
    map_res(take_while1(is_digit), |digits: &[u8]| {
        if digits.len() > "2147483648".len() {
            return Err(PacketError::NumberTooLarge);
        }
        String::from_utf8(digits.to_vec())
            .unwrap()
            .parse::<i32>()
            .map_err(|_| PacketError::NumberTooLarge)
    })
    .parse(input)
}

impl Display for Packet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Packet::Connect { session } => {
                f.write_str("/connect/")?;
                f.write_str(&session.to_string())?;
                f.write_char('/')
            }
            Packet::Data {
                session,
                position,
                data,
            } => {
                f.write_str("/data/")?;
                f.write_str(&session.to_string())?;
                f.write_char('/')?;
                f.write_str(&position.to_string())?;
                f.write_char('/')?;
                let data_str = String::from_utf8_lossy(data)
                    .replace('\n', "\\n")
                    .replace('\\', "\\\\")
                    .replace('/', "\\/");
                if data_str.len() > 40 {
                    f.write_str(&data_str[..40])?;
                    f.write_str("...")?;
                } else {
                    f.write_str(&data_str)?;
                }
                f.write_char('/')
            }
            Packet::Acknowledge { session, length } => {
                f.write_str("/ack/")?;
                f.write_str(&session.to_string())?;
                f.write_char('/')?;
                f.write_str(&length.to_string())?;
                f.write_char('/')
            }
            Packet::Close { session } => {
                f.write_str("/close/")?;
                f.write_str(&session.to_string())?;
                f.write_char('/')
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_connect() {
        let result = Packet::parse(b"/connect/7419234/").unwrap().1;
        assert_eq!(result, Packet::Connect { session: 7419234 });
    }

    #[test]
    fn parse_connect_bytes_after_end_errors() {
        let result = Packet::parse(b"/connect/7419234/padding");
        assert!(result.is_err());
    }

    #[test]
    fn parse_connect_session_too_big_errors() {
        let result = Packet::parse(b"/connect/2147483647/");
        assert!(result.is_ok());

        let result = Packet::parse(b"/connect/2147483648/");
        assert!(result.is_err());
    }

    #[test]
    fn parse_connect_negative_session_errors() {
        let result = Packet::parse(b"/connect/-10/");
        assert!(result.is_err());
    }

    #[test]
    fn parse_data() {
        let result = Packet::parse(b"/data/0/0/test/").unwrap().1;
        assert_eq!(
            result,
            Packet::Data {
                session: 0,
                position: 0,
                data: b"test".to_vec()
            }
        );
    }

    #[test]
    fn parse_data_escaped() {
        let result = Packet::parse(b"/data/0/0/te\\\\st\\/bb/").unwrap().1;
        assert_eq!(
            result,
            Packet::Data {
                session: 0,
                position: 0,
                data: b"te\\st/bb".to_vec()
            }
        );
    }

    #[test]
    fn parse_data_escaped_at_end() {
        let result = Packet::parse(b"/data/0/0/test\\\\/").unwrap().1;
        assert_eq!(
            result,
            Packet::Data {
                session: 0,
                position: 0,
                data: b"test\\".to_vec()
            }
        );
    }

    #[test]
    fn parse_data_end_escaped_errors() {
        let result = Packet::parse(b"/data/0/0/test\\/");
        assert!(result.is_err());
    }

    #[test]
    fn parse_acknowledge() {
        let result = Packet::parse(b"/ack/7419234/12/").unwrap().1;
        assert_eq!(
            result,
            Packet::Acknowledge {
                session: 7419234,
                length: 12
            }
        );
    }

    #[test]
    fn parse_close() {
        let result = Packet::parse(b"/close/125/").unwrap().1;
        assert_eq!(result, Packet::Close { session: 125 });
    }
}
