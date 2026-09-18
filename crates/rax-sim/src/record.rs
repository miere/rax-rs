use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use rax::frame::ResumeOutcome;
use rax::{Frame, GatewayMessage, NodeMessage, Open};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::SimError;
use crate::ws::WsMessage;

/// What a WebSocket API reports when the peer sent a close frame with no code.
pub const CLOSE_NO_STATUS: u16 = 1005;

/// What a WebSocket API reports when the socket ended with no close frame at all.
pub const CLOSE_ABNORMAL: u16 = 1006;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ConnectionId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    NodeToGateway,
    GatewayToNode,
}

/// A delivery-layer frame as it crossed the proxy, with the payload kept as JSON so a message
/// this build does not know is still recorded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "k", rename_all = "snake_case")]
pub enum WireFrame {
    #[serde(rename = "msg")]
    Message {
        seq: u64,
        ack: u64,
        payload: Value,
    },
    Ack {
        ack: u64,
    },
    Resume {
        last_seen: u64,
        epoch: u64,
    },
    Resumed {
        last_seen: u64,
        epoch: u64,
        ok: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    Close {
        code: Option<u16>,
    },
    Undecodable {
        error: String,
    },
}

impl WireFrame {
    fn decode(bytes: &[u8]) -> Self {
        let undecodable = |error: String| Self::Undecodable { error };
        match Frame::decode(bytes) {
            Err(err) => undecodable(err.to_string()),
            Ok(Frame::Message { seq, ack, payload }) => match serde_json::from_str(payload.get()) {
                Ok(payload) => Self::Message {
                    seq: seq.get(),
                    ack,
                    payload,
                },
                Err(err) => undecodable(err.to_string()),
            },
            Ok(Frame::Ack { ack }) => Self::Ack { ack: ack.get() },
            Ok(Frame::Resume { last_seen, epoch }) => Self::Resume { last_seen, epoch },
            Ok(Frame::Resumed {
                last_seen,
                epoch,
                outcome,
            }) => {
                let (ok, reason) = match outcome {
                    ResumeOutcome::Accepted => (true, None),
                    ResumeOutcome::Refused { reason } => (false, Some(reason)),
                };
                Self::Resumed {
                    last_seen,
                    epoch,
                    ok,
                    reason,
                }
            }
        }
    }
}

/// Durations serialise as whole microseconds since the simulator started.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    #[serde(with = "micros")]
    pub at: Duration,
    pub connection: ConnectionId,
    /// Absent until the gateway's `resumed` names the link; sequence numbers repeat across links.
    pub epoch: Option<u64>,
    pub direction: Direction,
    pub frame: WireFrame,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Handshake {
    pub connection: ConnectionId,
    #[serde(with = "micros")]
    pub at: Duration,
    /// The request target exactly as sent, query included, so a test can prove no token is in it.
    pub target: String,
    pub authorization: Option<String>,
    /// Absent when the connection ended before anything answered the upgrade.
    pub status: Option<u16>,
    pub first_frame: Option<WireFrame>,
    /// Set once an upgraded connection ends: the node's close code, or the codes below.
    pub close_code: Option<u16>,
}

impl Handshake {
    pub fn path(&self) -> &str {
        self.target
            .split_once('?')
            .map_or(self.target.as_str(), |(path, _)| path)
    }
}

/// A message as the peer consumed it: replayed frames appear once, at their first crossing.
#[derive(Debug, Clone, PartialEq)]
pub struct Delivered<M> {
    pub at: Duration,
    pub epoch: u64,
    pub message: M,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transcript {
    pub entries: Vec<Entry>,
}

impl Transcript {
    pub fn node_messages(&self) -> Vec<Delivered<Open<NodeMessage>>> {
        self.delivered(Direction::NodeToGateway)
    }

    pub fn gateway_messages(&self) -> Vec<Delivered<Open<GatewayMessage>>> {
        self.delivered(Direction::GatewayToNode)
    }

    pub fn write_json_lines(&self, writer: impl Write) -> Result<(), SimError> {
        let mut writer = BufWriter::new(writer);
        for entry in &self.entries {
            serde_json::to_writer(&mut writer, entry)?;
            writer.write_all(b"\n")?;
        }
        writer.flush()?;
        Ok(())
    }

    pub fn write_to(&self, path: impl AsRef<Path>) -> Result<(), SimError> {
        self.write_json_lines(File::create(path)?)
    }

    fn delivered<M: DeserializeOwned>(&self, direction: Direction) -> Vec<Delivered<M>> {
        let mut seen = HashSet::new();
        self.entries
            .iter()
            .filter(|entry| entry.direction == direction)
            .filter_map(|entry| match (&entry.frame, entry.epoch) {
                (WireFrame::Message { seq, payload, .. }, Some(epoch))
                    if seen.insert((epoch, *seq)) =>
                {
                    let message = serde_json::from_value(payload.clone()).ok()?;
                    Some(Delivered {
                        at: entry.at,
                        epoch,
                        message,
                    })
                }
                _ => None,
            })
            .collect()
    }
}

#[derive(Default)]
struct Live {
    epoch: Option<u64>,
    node_close: Option<u16>,
    upgraded: bool,
}

pub(crate) struct Records {
    started: Instant,
    pub(crate) handshakes: Vec<Handshake>,
    pub(crate) transcript: Transcript,
    live: HashMap<ConnectionId, Live>,
    next: u64,
}

impl Records {
    pub(crate) fn new() -> Self {
        Self {
            started: Instant::now(),
            handshakes: Vec::new(),
            transcript: Transcript::default(),
            live: HashMap::new(),
            next: 0,
        }
    }

    pub(crate) fn started(&self) -> Instant {
        self.started
    }

    pub(crate) fn now(&self) -> Duration {
        truncate(self.started.elapsed())
    }

    pub(crate) fn connect(&mut self) -> ConnectionId {
        self.next += 1;
        let id = ConnectionId(self.next);
        self.live.insert(id, Live::default());
        id
    }

    pub(crate) fn handshake(
        &mut self,
        connection: ConnectionId,
        target: String,
        authorization: Option<String>,
    ) {
        self.handshakes.push(Handshake {
            connection,
            at: self.now(),
            target,
            authorization,
            status: None,
            first_frame: None,
            close_code: None,
        });
    }

    pub(crate) fn answered(&mut self, connection: ConnectionId, status: u16) {
        if let Some(handshake) = self.handshake_mut(connection) {
            handshake.status = Some(status);
        }
        if let Some(live) = self.live.get_mut(&connection) {
            live.upgraded = status == 101;
        }
    }

    pub(crate) fn observe(
        &mut self,
        connection: ConnectionId,
        direction: Direction,
        message: WsMessage,
    ) {
        let frame = match message {
            WsMessage::Binary(bytes) => WireFrame::decode(&bytes),
            WsMessage::Close(code) => WireFrame::Close { code },
        };
        let live = self.live.entry(connection).or_default();
        match (&frame, direction) {
            (WireFrame::Resumed { epoch, .. }, Direction::GatewayToNode) => {
                live.epoch = Some(*epoch);
            }
            (WireFrame::Close { code }, Direction::NodeToGateway) => {
                live.node_close
                    .get_or_insert(code.unwrap_or(CLOSE_NO_STATUS));
            }
            _ => {}
        }
        let epoch = live.epoch;
        if direction == Direction::NodeToGateway
            && !matches!(frame, WireFrame::Close { .. })
            && let Some(handshake) = self.handshake_mut(connection)
            && handshake.first_frame.is_none()
        {
            handshake.first_frame = Some(frame.clone());
        }
        self.transcript.entries.push(Entry {
            at: self.now(),
            connection,
            epoch,
            direction,
            frame,
        });
    }

    pub(crate) fn ended(&mut self, connection: ConnectionId) {
        let Some(live) = self.live.remove(&connection) else {
            return;
        };
        if !live.upgraded {
            return;
        }
        if let Some(handshake) = self.handshake_mut(connection) {
            handshake.close_code = Some(live.node_close.unwrap_or(CLOSE_ABNORMAL));
        }
    }

    fn handshake_mut(&mut self, connection: ConnectionId) -> Option<&mut Handshake> {
        self.handshakes
            .iter_mut()
            .rev()
            .find(|handshake| handshake.connection == connection)
    }
}

pub(crate) fn truncate(elapsed: Duration) -> Duration {
    Duration::from_micros(u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX))
}

pub(crate) mod micros {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub(crate) fn serialize<S: Serializer>(
        at: &Duration,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(u64::try_from(at.as_micros()).unwrap_or(u64::MAX))
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Duration, D::Error> {
        Ok(Duration::from_micros(u64::deserialize(deserializer)?))
    }

    pub(crate) mod option {
        use std::time::Duration;

        use serde::{Deserialize, Deserializer, Serializer};

        pub(crate) fn serialize<S: Serializer>(
            at: &Option<Duration>,
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            match at {
                Some(at) => super::serialize(at, serializer),
                None => serializer.serialize_none(),
            }
        }

        pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
            deserializer: D,
        ) -> Result<Option<Duration>, D::Error> {
            Ok(Option::<u64>::deserialize(deserializer)?.map(Duration::from_micros))
        }
    }
}
