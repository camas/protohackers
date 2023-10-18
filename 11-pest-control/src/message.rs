use std::{collections::HashMap, io::Write};

use nom::{
    bytes::complete::take,
    combinator::{all_consuming, fail, map, map_opt, map_res, verify},
    multi::{length_count, length_data},
    number::{complete, streaming},
    sequence::{pair, tuple},
    IResult,
};

use crate::model::{PolicyID, SiteID, SpeciesAction};

const MAX_MESSAGE_LENGTH: u32 = 100000;

#[derive(Debug)]
pub enum Message {
    Hello {
        protocol: String,
        version: u32,
    },
    Error {
        message: String,
    },
    Ok,
    DialAuthority {
        site_id: SiteID,
    },
    TargetPopulations {
        site_id: SiteID,
        populations: Vec<PopulationTarget>,
    },
    CreatePolicy {
        species: String,
        action: SpeciesAction,
    },
    DeletePolicy {
        policy_id: PolicyID,
    },
    PolicyResult {
        policy_id: PolicyID,
    },
    SiteVisit {
        site_id: SiteID,
        populations: Vec<SitePopulation>,
    },
}

#[derive(Debug)]
pub struct PopulationTarget {
    pub(crate) species: String,
    pub(crate) min: u32,
    pub(crate) max: u32,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SitePopulation {
    pub(crate) species: String,
    pub(crate) count: u32,
}

pub struct ParsedMessage {
    pub message: Message,
    pub checksum: u8,
}

impl Message {
    pub fn parse(input: &[u8]) -> IResult<&[u8], ParsedMessage> {
        parse_message(input)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buffer = b"\x00\x00\x00\x00\x00".to_vec();
        let message_type;
        match self {
            Message::Hello { protocol, version } => {
                message_type = 0x50;
                write_string(&mut buffer, protocol);
                buffer.write_all(&version.to_be_bytes()).unwrap();
            }
            Message::Error { message } => {
                message_type = 0x51;
                write_string(&mut buffer, message);
            }
            Message::DialAuthority { site_id } => {
                message_type = 0x53;
                buffer.write_all(&site_id.0.to_be_bytes()).unwrap();
            }
            Message::CreatePolicy { species, action } => {
                message_type = 0x55;
                write_string(&mut buffer, species);
                buffer.push(match action {
                    SpeciesAction::Cull => 0x90,
                    SpeciesAction::Conserve => 0xa0,
                });
            }
            Message::DeletePolicy { policy_id } => {
                message_type = 0x56;
                buffer.write_all(&policy_id.0.to_be_bytes()).unwrap();
            }
            _ => unreachable!(),
        }

        buffer[0] = message_type;
        let len = (buffer.len() as u32 + 1).to_be_bytes();
        buffer.splice(1..=4, len);

        let checksum = 0_u8.wrapping_sub(buffer.iter().fold(0_u8, |a, b| a.wrapping_add(*b)));
        buffer.push(checksum);

        buffer
    }
}

fn write_string(buffer: &mut Vec<u8>, value: &str) {
    buffer
        .write_all(&(value.len() as u32).to_be_bytes())
        .unwrap();
    buffer.write_all(value.as_bytes()).unwrap();
}

impl ParsedMessage {
    pub fn validate(&self, message_bytes: &[u8]) -> bool {
        let sum = message_bytes.iter().fold(0_u8, |a, b| a.wrapping_add(*b));
        if sum != 0 {
            return false;
        }

        match &self.message {
            Message::Hello { protocol, version } => {
                if protocol != "pestcontrol" || *version != 1 {
                    return false;
                }
            }
            Message::SiteVisit { populations, .. } => {
                let mut seen = HashMap::<String, u32>::new();
                for population in populations.iter() {
                    if let Some(existing) = seen.get(&population.species) {
                        if *existing != population.count {
                            return false;
                        }
                    } else {
                        seen.insert(population.species.clone(), population.count);
                    }
                }
            }
            _ => {}
        }

        true
    }
}

fn parse_message(input: &[u8]) -> IResult<&[u8], ParsedMessage> {
    map_res(
        tuple((
            streaming::be_u8,
            length_data(map(
                verify(streaming::be_u32, |&v| {
                    (6..=MAX_MESSAGE_LENGTH).contains(&v)
                }),
                |v| v - 6,
            )),
            streaming::be_u8,
        )),
        |(message_type, message_data, checksum)| -> Result<_, nom::Err<_>> {
            let (_, message) = parse_message_data(message_type, message_data)?;
            Ok(ParsedMessage { message, checksum })
        },
    )(input)
}

fn parse_message_data(message_type: u8, message_data: &[u8]) -> IResult<&[u8], Message> {
    match message_type {
        0x50 => all_consuming(map(
            pair(parse_string, complete::be_u32),
            |(protocol, version)| Message::Hello { protocol, version },
        ))(message_data),
        0x51 => {
            all_consuming(map(parse_string, |message| Message::Error { message }))(message_data)
        }
        0x52 => all_consuming(map(take(0_u32), |_| Message::Ok))(message_data),
        0x53 => all_consuming(map(parse_siteid, |site_id| Message::DialAuthority {
            site_id,
        }))(message_data),
        0x54 => all_consuming(map(
            pair(
                parse_siteid,
                length_count(
                    complete::be_u32,
                    tuple((parse_string, complete::be_u32, complete::be_u32)),
                ),
            ),
            |(site_id, populations)| Message::TargetPopulations {
                site_id,
                populations: populations
                    .into_iter()
                    .map(|(species, min, max)| PopulationTarget { species, min, max })
                    .collect(),
            },
        ))(message_data),
        0x55 => all_consuming(map(
            pair(parse_string, parse_action),
            |(species, action)| Message::CreatePolicy { species, action },
        ))(message_data),
        0x56 => all_consuming(map(parse_policyid, |policy_id| Message::DeletePolicy {
            policy_id,
        }))(message_data),
        0x57 => all_consuming(map(parse_policyid, |policy_id| Message::PolicyResult {
            policy_id,
        }))(message_data),
        0x58 => all_consuming(map(
            pair(
                parse_siteid,
                length_count(complete::be_u32, pair(parse_string, complete::be_u32)),
            ),
            |(site_id, populations)| Message::SiteVisit {
                site_id,
                populations: populations
                    .into_iter()
                    .map(|(species, count)| SitePopulation { species, count })
                    .collect(),
            },
        ))(message_data),
        _ => fail(message_data),
    }
}

fn parse_siteid(input: &[u8]) -> IResult<&[u8], SiteID> {
    map(complete::be_u32, SiteID)(input)
}

fn parse_policyid(input: &[u8]) -> IResult<&[u8], PolicyID> {
    map(complete::be_u32, PolicyID)(input)
}

fn parse_action(input: &[u8]) -> IResult<&[u8], SpeciesAction> {
    map_opt(complete::be_u8, |v| match v {
        0x90 => Some(SpeciesAction::Cull),
        0xa0 => Some(SpeciesAction::Conserve),
        _ => None,
    })(input)
}

fn parse_string(input: &[u8]) -> IResult<&[u8], String> {
    map_res(length_data(complete::be_u32), |v: &[u8]| {
        String::from_utf8(v.to_vec())
    })(input)
}

#[cfg(test)]
mod tests {
    use crate::utilities::{PROTOCOL, PROTOCOL_VERSION};

    use super::*;

    #[test]
    fn parse_hello() {
        let data = b"\x50\x00\x00\x00\x19\x00\x00\x00\x0bpestcontrol\x00\x00\x00\x01\xce";

        let (remaining, result) = Message::parse(data).unwrap();

        assert_eq!(remaining.len(), 0);
        assert!(result.validate(data));
        assert!(matches!(result.message, Message::Hello { .. }));
        let Message::Hello { protocol, version } = result.message else {
            unreachable!()
        };
        assert_eq!(protocol, "pestcontrol");
        assert_eq!(version, 1);
    }

    #[test]
    fn serialize_hello() {
        let hello_message = Message::Hello {
            protocol: PROTOCOL.to_string(),
            version: PROTOCOL_VERSION,
        };

        let serialized = hello_message.to_bytes();

        let expected = b"\x50\x00\x00\x00\x19\x00\x00\x00\x0bpestcontrol\x00\x00\x00\x01\xce";
        assert_eq!(serialized, expected);
    }
}
