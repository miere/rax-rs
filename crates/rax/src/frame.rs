use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

/// Compared for strict equality, not negotiated: a mismatch is a deployment error, best refused at
/// the first frame.
pub const VERSION: u16 = 1;

/// A frame above this is refused before it is decoded.
pub const MAX_FRAME_BYTES: usize = 8 << 20;

/// The delivery layer. It never reads its payload, so a message change can never break delivery.
#[derive(Debug, Clone)]
pub enum Frame {
    Message {
        seq: NonZeroU64,
        /// Piggybacked acknowledgement; zero means nothing consumed yet, which is why sequences
        /// start at 1.
        ack: u64,
        payload: Box<RawValue>,
    },
    /// Takes no sequence number, or two idle peers would acknowledge each other's acks forever.
    Ack {
        ack: NonZeroU64,
    },
    Resume {
        last_seen: u64,
        epoch: u64,
    },
    Resumed {
        last_seen: u64,
        epoch: u64,
        outcome: ResumeOutcome,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResumeOutcome {
    Accepted,
    /// The peer must fail its open turns: carrying on past a hole would lose events silently.
    Refused {
        reason: String,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("rax: frame of {size} bytes exceeds the {MAX_FRAME_BYTES}-byte ceiling")]
    TooLarge { size: usize },
    #[error("rax: decode frame: {0}")]
    Malformed(#[from] serde_json::Error),
    #[error("rax: peer speaks version {peer}, this build speaks {VERSION}")]
    VersionMismatch { peer: u16 },
    #[error("rax: {kind} frame {problem}")]
    Invalid {
        kind: &'static str,
        problem: &'static str,
    },
}

impl Frame {
    pub fn encode(&self) -> Result<Vec<u8>, FrameError> {
        let raw = serde_json::to_vec(&WireFrame::from(self))?;
        if raw.len() > MAX_FRAME_BYTES {
            return Err(FrameError::TooLarge { size: raw.len() });
        }
        Ok(raw)
    }

    pub fn decode(raw: &[u8]) -> Result<Self, FrameError> {
        if raw.len() > MAX_FRAME_BYTES {
            return Err(FrameError::TooLarge { size: raw.len() });
        }
        let wire: WireFrame = serde_json::from_slice(raw)?;
        wire.try_into()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Kind {
    #[serde(rename = "msg")]
    Message,
    #[serde(rename = "ack")]
    Ack,
    #[serde(rename = "resume")]
    Resume,
    #[serde(rename = "resumed")]
    Resumed,
}

#[derive(Serialize, Deserialize)]
struct WireFrame {
    v: u16,
    k: Kind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    seq: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ack: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    r: Option<WireResume>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    p: Option<Box<RawValue>>,
}

#[derive(Serialize, Deserialize)]
struct WireResume {
    last_seen: u64,
    epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ok: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

impl From<&Frame> for WireFrame {
    fn from(frame: &Frame) -> Self {
        let bare = |k| WireFrame {
            v: VERSION,
            k,
            seq: None,
            ack: None,
            r: None,
            p: None,
        };
        match frame {
            Frame::Message { seq, ack, payload } => WireFrame {
                seq: Some(seq.get()),
                ack: (*ack > 0).then_some(*ack),
                p: Some(payload.clone()),
                ..bare(Kind::Message)
            },
            Frame::Ack { ack } => WireFrame {
                ack: Some(ack.get()),
                ..bare(Kind::Ack)
            },
            Frame::Resume { last_seen, epoch } => WireFrame {
                r: Some(WireResume {
                    last_seen: *last_seen,
                    epoch: *epoch,
                    ok: None,
                    reason: None,
                }),
                ..bare(Kind::Resume)
            },
            Frame::Resumed {
                last_seen,
                epoch,
                outcome,
            } => {
                let (ok, reason) = match outcome {
                    ResumeOutcome::Accepted => (true, None),
                    ResumeOutcome::Refused { reason } => (false, Some(reason.clone())),
                };
                WireFrame {
                    r: Some(WireResume {
                        last_seen: *last_seen,
                        epoch: *epoch,
                        ok: Some(ok),
                        reason,
                    }),
                    ..bare(Kind::Resumed)
                }
            }
        }
    }
}

impl TryFrom<WireFrame> for Frame {
    type Error = FrameError;

    fn try_from(wire: WireFrame) -> Result<Self, FrameError> {
        if wire.v != VERSION {
            return Err(FrameError::VersionMismatch { peer: wire.v });
        }
        let invalid = |kind, problem| Err(FrameError::Invalid { kind, problem });
        match wire.k {
            Kind::Message => {
                if wire.r.is_some() {
                    return invalid("msg", "carries resume state");
                }
                let Some(seq) = wire.seq.and_then(NonZeroU64::new) else {
                    return invalid("msg", "carries no sequence number");
                };
                let Some(payload) = wire.p else {
                    return invalid("msg", "carries no payload");
                };
                Ok(Frame::Message {
                    seq,
                    ack: wire.ack.unwrap_or(0),
                    payload,
                })
            }
            Kind::Ack => {
                if wire.seq.is_some() || wire.r.is_some() || wire.p.is_some() {
                    return invalid("ack", "carries more than an acknowledgement");
                }
                let Some(ack) = wire.ack.and_then(NonZeroU64::new) else {
                    return invalid("ack", "acknowledges nothing");
                };
                Ok(Frame::Ack { ack })
            }
            Kind::Resume => {
                if wire.seq.is_some() || wire.p.is_some() {
                    return invalid("resume", "consumed a sequence number or carries a payload");
                }
                let Some(r) = wire.r else {
                    return invalid("resume", "carries no resume state");
                };
                Ok(Frame::Resume {
                    last_seen: r.last_seen,
                    epoch: r.epoch,
                })
            }
            Kind::Resumed => {
                if wire.seq.is_some() || wire.p.is_some() {
                    return invalid("resumed", "consumed a sequence number or carries a payload");
                }
                let Some(r) = wire.r else {
                    return invalid("resumed", "carries no resume state");
                };
                let outcome = match (r.ok, r.reason) {
                    (Some(true), None) => ResumeOutcome::Accepted,
                    (Some(false) | None, Some(reason)) => ResumeOutcome::Refused { reason },
                    _ => return invalid("resumed", "neither accepts nor gives a reason to refuse"),
                };
                Ok(Frame::Resumed {
                    last_seen: r.last_seen,
                    epoch: r.epoch,
                    outcome,
                })
            }
        }
    }
}
