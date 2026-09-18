use rax::frame::MAX_FRAME_BYTES;

const CONTINUATION: u8 = 0x0;
const TEXT: u8 = 0x1;
const BINARY: u8 = 0x2;
const CLOSE: u8 = 0x8;

#[derive(Debug)]
pub(crate) enum WsMessage {
    Binary(Vec<u8>),
    Close(Option<u16>),
}

enum Parsed {
    Incomplete,
    Invalid,
    Frame {
        consumed: usize,
        fin: bool,
        opcode: u8,
        payload: Vec<u8>,
    },
}

#[derive(Default)]
pub(crate) struct WsReader {
    buffer: Vec<u8>,
    fragments: Option<(u8, Vec<u8>)>,
    broken: bool,
}

impl WsReader {
    pub(crate) fn feed(&mut self, bytes: &[u8]) -> Vec<WsMessage> {
        let mut messages = Vec::new();
        if self.broken {
            return messages;
        }
        self.buffer.extend_from_slice(bytes);
        loop {
            match parse(&self.buffer) {
                Parsed::Incomplete => return messages,
                Parsed::Invalid => {
                    self.broken = true;
                    self.buffer = Vec::new();
                    return messages;
                }
                Parsed::Frame {
                    consumed,
                    fin,
                    opcode,
                    payload,
                } => {
                    self.buffer.drain(..consumed);
                    if let Some(message) = self.assemble(fin, opcode, payload) {
                        messages.push(message);
                    }
                }
            }
        }
    }

    fn assemble(&mut self, fin: bool, opcode: u8, payload: Vec<u8>) -> Option<WsMessage> {
        let (opcode, payload) = match opcode {
            CLOSE => {
                let code = match payload.as_slice() {
                    [high, low, ..] => Some(u16::from_be_bytes([*high, *low])),
                    _ => None,
                };
                return Some(WsMessage::Close(code));
            }
            TEXT | BINARY if !fin => {
                self.fragments = Some((opcode, payload));
                return None;
            }
            CONTINUATION => {
                let (first, mut held) = self.fragments.take()?;
                held.extend_from_slice(&payload);
                if !fin {
                    self.fragments = Some((first, held));
                    return None;
                }
                (first, held)
            }
            other => (other, payload),
        };
        (opcode == BINARY).then_some(WsMessage::Binary(payload))
    }
}

fn parse(buffer: &[u8]) -> Parsed {
    let &[first, second, ..] = buffer else {
        return Parsed::Incomplete;
    };
    let (length, mut at) = match second & 0x7f {
        126 => match buffer.get(2..4) {
            Some(&[high, low]) => (u64::from(u16::from_be_bytes([high, low])), 4),
            _ => return Parsed::Incomplete,
        },
        127 => match buffer
            .get(2..10)
            .and_then(|raw| <[u8; 8]>::try_from(raw).ok())
        {
            Some(raw) => (u64::from_be_bytes(raw), 10),
            None => return Parsed::Incomplete,
        },
        short => (u64::from(short), 2),
    };
    let Ok(length) = usize::try_from(length) else {
        return Parsed::Invalid;
    };
    if length > MAX_FRAME_BYTES {
        return Parsed::Invalid;
    }
    let mask = if second & 0x80 != 0 {
        let Some(key) = buffer.get(at..at + 4) else {
            return Parsed::Incomplete;
        };
        at += 4;
        Some(key.to_vec())
    } else {
        None
    };
    let Some(raw) = buffer.get(at..at + length) else {
        return Parsed::Incomplete;
    };
    let payload = match mask {
        Some(key) => raw
            .iter()
            .zip(key.iter().cycle())
            .map(|(byte, key)| byte ^ key)
            .collect(),
        None => raw.to_vec(),
    };
    Parsed::Frame {
        consumed: at + length,
        fin: first & 0x80 != 0,
        opcode: first & 0x0f,
        payload,
    }
}
