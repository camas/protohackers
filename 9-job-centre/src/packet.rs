use serde_json::json;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PacketError {
    #[error("No 'request' type passed")]
    NoRequest,
    #[error("Request type not a string")]
    RequestNotStr,
    #[error("Invalid request type")]
    InvalidRequestType,
    #[error("No queue passed")]
    NoQueue,
    #[error("'queue' not a string")]
    QueueNotStr,
    #[error("No 'job' passed")]
    NoJob,
    #[error("No 'pri' passed")]
    NoPriority,
    #[error("'pri' not an integer")]
    PriorityNotInt,
    #[error("'No 'queues' passed")]
    NoQueues,
    #[error("'queues' is not an array")]
    QueuesNotArray,
    #[error("'wait' is not a boolean")]
    WaitNotBool,
    #[error("no 'id' passed")]
    NoId,
    #[error("'id' is not an integer")]
    IdNotInt,
}

pub enum ClientPacket {
    Put {
        queue: String,
        priority: u64,
        data: serde_json::Value,
    },
    Get {
        queues: Vec<String>,
        wait: bool,
    },
    Delete {
        job_id: u64,
    },
    Abort {
        job_id: u64,
    },
}

pub enum ServerPacket {
    NoJob,
    Error {
        message: Option<String>,
    },
    OkPutResponse {
        job_id: u64,
    },
    OkGetResponse {
        job_id: u64,
        data: serde_json::Value,
        priority: u64,
        queue: String,
    },
    OkDeleteResponse,
    OkAbortResponse,
}

impl ClientPacket {
    pub fn parse(input: serde_json::Value) -> Result<ClientPacket, PacketError> {
        let request = input
            .get("request")
            .ok_or(PacketError::NoRequest)?
            .as_str()
            .ok_or(PacketError::RequestNotStr)?;

        match request {
            "put" => {
                let queue = input
                    .get("queue")
                    .ok_or(PacketError::NoQueue)?
                    .as_str()
                    .ok_or(PacketError::QueueNotStr)?
                    .to_string();
                let data = input.get("job").ok_or(PacketError::NoJob)?.clone();
                let priority = input
                    .get("pri")
                    .ok_or(PacketError::NoPriority)?
                    .as_u64()
                    .ok_or(PacketError::PriorityNotInt)?;

                Ok(ClientPacket::Put {
                    queue,
                    priority,
                    data,
                })
            }
            "get" => {
                let queues = input
                    .get("queues")
                    .ok_or(PacketError::NoQueues)?
                    .as_array()
                    .ok_or(PacketError::QueuesNotArray)?
                    .iter()
                    .map(|queue| {
                        queue
                            .as_str()
                            .map(|v| v.to_string())
                            .ok_or(PacketError::QueueNotStr)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if queues.is_empty() {
                    return Err(PacketError::NoQueues);
                }
                let wait = match input.get("wait") {
                    Some(v) => v.as_bool().ok_or(PacketError::WaitNotBool),
                    None => Ok(false),
                }?;

                Ok(ClientPacket::Get { queues, wait })
            }
            "delete" => {
                let job_id = input
                    .get("id")
                    .ok_or(PacketError::NoId)?
                    .as_u64()
                    .ok_or(PacketError::IdNotInt)?;

                Ok(ClientPacket::Delete { job_id })
            }
            "abort" => {
                let job_id = input
                    .get("id")
                    .ok_or(PacketError::NoId)?
                    .as_u64()
                    .ok_or(PacketError::IdNotInt)?;

                Ok(ClientPacket::Abort { job_id })
            }
            _ => Err(PacketError::InvalidRequestType),
        }
    }
}

impl ServerPacket {
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            ServerPacket::NoJob => json!({"status": "no-job"}),
            ServerPacket::Error { message } => {
                if let Some(message) = message {
                    json!({"status": "error", "error": message})
                } else {
                    json!({"status": "error"})
                }
            }
            ServerPacket::OkPutResponse { job_id } => json!({"status": "ok", "id": job_id}),
            ServerPacket::OkGetResponse {
                job_id,
                data,
                priority,
                queue,
            } => {
                json!({"status": "ok", "id": job_id, "job": data, "pri": priority, "queue": queue})
            }
            ServerPacket::OkDeleteResponse => json!({"status": "ok"}),
            ServerPacket::OkAbortResponse => json!({"status": "ok"}),
        }
    }
}
