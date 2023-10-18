use thiserror::Error;

use crate::model::{DirectoryPath, FilePath, PathError};

#[derive(Debug, Error)]
pub enum PacketError {
    #[error("Invalid utf8")]
    InvalidUtf8,
    #[error("illegal method: {0}")]
    IllegalMethod(String),
    #[error("usage: GET file [revision]")]
    GetWrongArgCount,
    #[error("usage: PUT file length newline data")]
    PutWrongArgCount,
    #[error("usage: LIST dir")]
    ListWrongArgCount,
    #[error("no such revision")]
    NoSuchRevision,
    #[error("{0}")]
    PathError(#[from] PathError),
}

pub enum ClientPacket {
    Help,
    Get {
        file_path: FilePath,
        revision: Option<u32>,
    },
    PutPartial {
        file_path: FilePath,
        size: u64,
    },
    Put {
        file_path: FilePath,
        data: Vec<u8>,
    },
    List {
        directory_path: DirectoryPath,
    },
}

pub enum ServerPacket {
    Error { message: String },
    HelpMessage,
    Ready,
    GetResponse { data: Vec<u8> },
    PutResponse { revision: u32 },
    ListResponse { entries: Vec<Entry> },
}

pub enum Entry {
    File { name: String, revision: u32 },
    Directory { name: String },
}

impl ClientPacket {
    pub fn parse(line: String) -> Result<ClientPacket, PacketError> {
        parse_client_packet(line)
    }
}

fn parse_client_packet(line: String) -> Result<ClientPacket, PacketError> {
    let words = line.split(' ').collect::<Vec<_>>();
    if words.is_empty() {
        return Err(PacketError::IllegalMethod("".to_string()));
    }

    Ok(match &words[0].to_ascii_lowercase()[..] {
        "help" => ClientPacket::Help,
        "get" => {
            if words.len() != 2 && words.len() != 3 {
                return Err(PacketError::GetWrongArgCount);
            }
            let file_path = FilePath::from_str(words[1])?;
            let revision = if words.len() == 3 {
                let revision_string = words[2];
                if !revision_string.starts_with('r') {
                    return Err(PacketError::NoSuchRevision);
                }
                let Ok(revision) = revision_string[1..].parse::<u32>() else {
                    return Err(PacketError::NoSuchRevision);
                };
                Some(revision)
            } else {
                None
            };

            ClientPacket::Get {
                file_path,
                revision,
            }
        }
        "put" => {
            if words.len() != 3 {
                return Err(PacketError::PutWrongArgCount);
            }
            let file_path = FilePath::from_str(words[1])?;
            let size = words[2].parse::<u64>().unwrap_or(0);
            ClientPacket::PutPartial { file_path, size }
        }
        "list" => {
            if words.len() != 2 {
                return Err(PacketError::ListWrongArgCount);
            }
            let directory_path = DirectoryPath::from_str(words[1])?;
            ClientPacket::List { directory_path }
        }
        _ => return Err(PacketError::IllegalMethod(words[0].to_string())),
    })
}

impl ServerPacket {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            ServerPacket::Error { message } => format!("ERR {message}\n").into_bytes(),
            ServerPacket::HelpMessage => b"OK usage: HELP|GET|PUT|LIST\n".to_vec(),
            ServerPacket::Ready => b"READY\n".to_vec(),
            ServerPacket::GetResponse { data } => {
                let mut buffer = format!("OK {}\n", data.len()).into_bytes();
                buffer.extend(data);
                buffer
            }
            ServerPacket::PutResponse { revision } => format!("OK r{revision}\n").into_bytes(),
            ServerPacket::ListResponse { entries } => {
                let mut buffer = format!("OK {}\n", entries.len()).into_bytes();
                for entry in entries {
                    buffer.extend(
                        match entry {
                            Entry::File {
                                name: filename,
                                revision,
                            } => {
                                format!("{} r{}\n", filename, revision)
                            }
                            Entry::Directory { name: dirname } => format!("{}/ DIR\n", dirname),
                        }
                        .into_bytes(),
                    );
                }
                buffer
            }
        }
    }
}
