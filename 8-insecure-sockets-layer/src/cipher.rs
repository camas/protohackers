use nom::{
    branch::alt,
    bytes::complete::tag,
    combinator::{all_consuming, map},
    multi::many0,
    number::complete::le_u8,
    sequence::{pair, terminated},
    IResult, Parser,
};

#[derive(Debug, Clone)]
pub struct CipherSpecification {
    operations: Vec<CipherOperation>,
}

#[derive(Debug, Clone, Copy)]
enum CipherOperation {
    ReverseBits,
    Xor(u8),
    XorPos,
    Add(u8),
    AddPos,
}

impl CipherSpecification {
    fn new(given_operations: Vec<CipherOperation>) -> CipherSpecification {
        let mut operations = Vec::new();
        for operation in given_operations {
            match operation {
                CipherOperation::ReverseBits => {
                    if matches!(operations.last(), Some(CipherOperation::ReverseBits)) {
                        operations.pop();
                    } else {
                        operations.push(operation);
                    }
                }
                CipherOperation::Xor(v) => {
                    if v == 0 {
                        continue;
                    }
                    if matches!(operations.last(), Some(CipherOperation::Xor(_))) {
                        let Some(CipherOperation::Xor(last_v)) = operations.pop() else {
                            unreachable!();
                        };
                        let v = v ^ last_v;
                        if v == 0 {
                            continue;
                        }
                        operations.push(CipherOperation::Xor(v));
                        continue;
                    }
                    operations.push(operation);
                }
                CipherOperation::XorPos => {
                    if matches!(operations.last(), Some(CipherOperation::XorPos)) {
                        operations.pop();
                    } else {
                        operations.push(operation);
                    }
                }
                CipherOperation::Add(v) => {
                    if v == 0 {
                        continue;
                    }
                    if matches!(operations.last(), Some(CipherOperation::Add(_))) {
                        let Some(CipherOperation::Add(last_v)) = operations.pop() else {
                            unreachable!();
                        };
                        let v = v.wrapping_add(last_v);
                        if v == 0 {
                            continue;
                        }
                        operations.push(CipherOperation::Add(v));
                        continue;
                    }
                    operations.push(operation);
                }
                CipherOperation::AddPos => {
                    if operations.len() >= 255 {
                        if operations[operations.len() - 255..]
                            .iter()
                            .all(|op| matches!(op, CipherOperation::AddPos))
                        {
                            operations.truncate(operations.len() - 255);
                        }
                    } else {
                        operations.push(operation);
                    }
                }
            }
        }

        CipherSpecification { operations }
    }

    pub fn parse(input: &[u8]) -> IResult<&[u8], CipherSpecification> {
        all_consuming(map(
            terminated(many0(parse_operation), tag(b"\x00")),
            CipherSpecification::new,
        ))
        .parse(input)
    }

    pub fn encode(&self, value: u8, stream_offset: u8) -> u8 {
        let mut result = value;
        for operation in self.operations.iter() {
            match operation {
                CipherOperation::ReverseBits => result = result.reverse_bits(),
                CipherOperation::Xor(other) => result ^= *other,
                CipherOperation::XorPos => result ^= stream_offset,
                CipherOperation::Add(other) => result = result.wrapping_add(*other),
                CipherOperation::AddPos => result = result.wrapping_add(stream_offset),
            }
        }
        result
    }

    pub fn decode(&self, value: u8, stream_offset: u8) -> u8 {
        let mut result = value;
        for operation in self.operations.iter().rev() {
            match operation {
                CipherOperation::ReverseBits => result = result.reverse_bits(),
                CipherOperation::Xor(other) => result ^= *other,
                CipherOperation::XorPos => result ^= stream_offset,
                CipherOperation::Add(other) => result = result.wrapping_sub(*other),
                CipherOperation::AddPos => result = result.wrapping_sub(stream_offset),
            }
        }
        result
    }

    pub fn is_noop(&self) -> bool {
        self.operations.is_empty()
    }
}

fn parse_operation(input: &[u8]) -> IResult<&[u8], CipherOperation> {
    alt((
        map(tag(b"\x01"), |_| CipherOperation::ReverseBits),
        map(pair(tag(b"\x02"), le_u8), |(_, v)| CipherOperation::Xor(v)),
        map(tag(b"\x03"), |_| CipherOperation::XorPos),
        map(pair(tag(b"\x04"), le_u8), |(_, v)| CipherOperation::Add(v)),
        map(tag(b"\x05"), |_| CipherOperation::AddPos),
    ))
    .parse(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_for_256_addpos_operations() {
        let result = CipherSpecification::new(vec![CipherOperation::AddPos; 256]);
        assert!(result.operations.is_empty());

        let result = CipherSpecification::new(vec![CipherOperation::AddPos; 257]);
        assert_eq!(result.operations.len(), 1);
    }
}
