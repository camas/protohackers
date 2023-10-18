use anyhow::anyhow;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::message::Message;

pub const TCP_MAX_SIZE: usize = 65535;
pub const PROTOCOL: &str = "pestcontrol";
pub const PROTOCOL_VERSION: u32 = 1;

pub async fn read_message(
    tcp_stream: &mut TcpStream,
    data: &mut Vec<u8>,
) -> anyhow::Result<Message> {
    loop {
        match Message::parse(data) {
            Ok((remaining, parsed_message)) => {
                let message_bytes = &data[..(data.len() - remaining.len())];
                if !parsed_message.validate(message_bytes) {
                    return Err(anyhow!("Invalid message"));
                }
                std::mem::swap(data, &mut remaining.to_vec());
                return Ok(parsed_message.message);
            }
            Err(nom::Err::Incomplete(_)) => (),
            Err(_) => return Err(anyhow!("Error parsing message")),
        }

        let mut buffer = vec![0; TCP_MAX_SIZE];
        let size = tcp_stream.read(&mut buffer).await?;
        if size == 0 {
            return Err(anyhow!("Stream closed"));
        }
        buffer.truncate(size);
        data.extend(buffer);
    }
}

pub async fn write_message(tcp_stream: &mut TcpStream, message: Message) -> anyhow::Result<()> {
    let message_bytes = message.to_bytes();
    tcp_stream.write_all(&message_bytes).await?;

    Ok(())
}

// pub fn bytes_to_readable_string(data: &[u8]) -> String {
//     let mut result = String::new();
//     for v in data {
//         match *v {
//             v if (0x20..=0x7e).contains(&v) => result.push(v as char),
//             b'\n' => result.push_str("\\n"),
//             b'\r' => result.push_str("\\r"),
//             b'\t' => result.push_str("\\t"),
//             _ => result.push_str(&format!("\\x{v:02}")),
//         }
//     }
//     result
// }
