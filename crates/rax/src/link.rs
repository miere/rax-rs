use std::collections::VecDeque;
use std::num::NonZeroU64;

use serde_json::value::RawValue;

use crate::frame::{Frame, ResumeOutcome};

/// Never the epoch of a real link, so a dialler with nothing to resume asks with it and is always
/// given a fresh link.
pub const NO_EPOCH: u64 = 0;

/// Delivery state for one end of a link, without I/O: the caller owns the socket, the clock and
/// the handler, so the same rules hold under any runtime.
#[derive(Debug)]
pub struct Link {
    config: LinkConfig,
    next_seq: u64,
    unacked: VecDeque<Sent>,
    unacked_bytes: usize,
    peer_ack: u64,
    last_seen: u64,
    acked_through: u64,
    bytes_since_ack: usize,
    outstanding: Option<NonZeroU64>,
}

#[derive(Debug, Clone)]
pub struct LinkConfig {
    /// Bytes, not frames: an attachment chunk is megabytes and a text event is bytes, so a frame
    /// count bounds nothing useful.
    pub window_bytes: usize,
    /// A frame count alone deadlocks when a few large frames fill the window first, so a
    /// consumed-byte trigger at half the window runs alongside it.
    pub ack_threshold: u64,
    /// Identifies the retransmit buffer: a peer that restarted never held it, so a resume across
    /// epochs must be refused rather than silently dropping frames.
    pub epoch: u64,
    /// Seeds the receive watermark so a replayed prefix is dropped as a duplicate.
    pub resume_from: u64,
}

impl Default for LinkConfig {
    fn default() -> Self {
        Self {
            window_bytes: 256 << 10,
            ack_threshold: 8,
            epoch: 0,
            resume_from: 0,
        }
    }
}

#[derive(Debug)]
struct Sent {
    seq: NonZeroU64,
    payload: Box<RawValue>,
}

/// Proof that a payload was handed out and not yet consumed. It is not `Clone`, so a frame can
/// only be acknowledged once, and only by whoever received it.
#[derive(Debug)]
#[must_use = "a delivery that is never consumed is never acknowledged, and the peer's window stalls"]
pub struct Delivery {
    seq: NonZeroU64,
    len: usize,
}

impl Delivery {
    pub fn seq(&self) -> NonZeroU64 {
        self.seq
    }
}

#[derive(Debug)]
pub enum Received {
    Deliver {
        delivery: Delivery,
        payload: Box<RawValue>,
    },
    /// A replayed frame this end already consumed.
    Duplicate,
    Acknowledged,
    ResumeRequested {
        last_seen: u64,
        epoch: u64,
    },
    Resumed {
        outcome: ResumeOutcome,
    },
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LinkError {
    #[error("rax: sequence gap: expected frame {want}, received {got}")]
    SequenceGap { want: u64, got: u64 },
    #[error("rax: frame {pending} is still being consumed; frames are delivered one at a time")]
    DeliveryOutstanding { pending: u64 },
    #[error("rax: the window is full ({pending} bytes unacknowledged)")]
    WindowFull { pending: usize },
    #[error("rax: peer acknowledged frame {ack}, only {sent} were sent")]
    AckBeyondSent { ack: u64, sent: u64 },
    #[error("rax: not resumable: {0}")]
    NotResumable(String),
}

impl Link {
    pub fn new(config: LinkConfig) -> Self {
        Self {
            last_seen: config.resume_from,
            acked_through: config.resume_from,
            config,
            next_seq: 0,
            unacked: VecDeque::new(),
            unacked_bytes: 0,
            peer_ack: 0,
            bytes_since_ack: 0,
            outstanding: None,
        }
    }

    pub fn epoch(&self) -> u64 {
        self.config.epoch
    }

    pub fn last_seen(&self) -> u64 {
        self.last_seen
    }

    pub fn pending(&self) -> (usize, usize) {
        (self.unacked.len(), self.unacked_bytes)
    }

    /// An empty buffer always has room, or a payload larger than the window could never be sent.
    pub fn has_room(&self, payload_len: usize) -> bool {
        self.unacked_bytes == 0 || self.unacked_bytes + payload_len <= self.config.window_bytes
    }

    /// Buffers the payload until the peer acknowledges it; that buffer, not the socket write, is
    /// what carries a frame across a reconnect.
    pub fn send(&mut self, payload: Box<RawValue>) -> Result<Frame, LinkError> {
        let len = payload.get().len();
        if !self.has_room(len) {
            return Err(LinkError::WindowFull {
                pending: self.unacked_bytes,
            });
        }
        self.next_seq += 1;
        let seq = NonZeroU64::new(self.next_seq).expect("sequence starts at 1");
        self.unacked_bytes += len;
        self.unacked.push_back(Sent {
            seq,
            payload: payload.clone(),
        });
        self.acked_through = self.last_seen;
        self.bytes_since_ack = 0;
        Ok(Frame::Message {
            seq,
            ack: self.last_seen,
            payload,
        })
    }

    pub fn receive(&mut self, frame: Frame) -> Result<Received, LinkError> {
        match frame {
            Frame::Message { seq, ack, payload } => {
                self.apply_ack(ack)?;
                if seq.get() <= self.last_seen {
                    return Ok(Received::Duplicate);
                }
                if let Some(pending) = self.outstanding {
                    return Err(LinkError::DeliveryOutstanding {
                        pending: pending.get(),
                    });
                }
                if seq.get() != self.last_seen + 1 {
                    return Err(LinkError::SequenceGap {
                        want: self.last_seen + 1,
                        got: seq.get(),
                    });
                }
                self.outstanding = Some(seq);
                let delivery = Delivery {
                    seq,
                    len: payload.get().len(),
                };
                Ok(Received::Deliver { delivery, payload })
            }
            Frame::Ack { ack } => {
                self.apply_ack(ack.get())?;
                Ok(Received::Acknowledged)
            }
            Frame::Resume { last_seen, epoch } => {
                Ok(Received::ResumeRequested { last_seen, epoch })
            }
            Frame::Resumed { outcome, .. } => Ok(Received::Resumed { outcome }),
        }
    }

    /// Acknowledges only once the handler returns, never on decode: a frame acked and then lost
    /// to a crash would be a hole nobody can see.
    pub fn consumed(&mut self, delivery: Delivery) -> Option<Frame> {
        debug_assert_eq!(self.outstanding, Some(delivery.seq));
        self.outstanding = None;
        self.last_seen = delivery.seq.get();
        self.bytes_since_ack += delivery.len;
        let behind = self.last_seen - self.acked_through;
        if behind >= self.config.ack_threshold
            || self.bytes_since_ack >= self.config.window_bytes / 2
        {
            return self.ack();
        }
        None
    }

    /// For a keepalive tick: it proves the socket is alive, not that the agent is still working.
    pub fn ack(&mut self) -> Option<Frame> {
        let ack = NonZeroU64::new(self.last_seen)?;
        if self.last_seen == self.acked_through {
            return None;
        }
        self.acked_through = self.last_seen;
        self.bytes_since_ack = 0;
        Some(Frame::Ack { ack })
    }

    pub fn resume_request(&self) -> Frame {
        Frame::Resume {
            last_seen: self.last_seen,
            epoch: self.config.epoch,
        }
    }

    /// Refuses rather than guesses, so the peer fails its open turns explicitly. The answer carries
    /// this end's own `last_seen`, so the peer replays what this end never consumed.
    pub fn accept_resume(&mut self, last_seen: u64, epoch: u64) -> (Frame, Vec<Frame>) {
        let refuse = |this: &Self, reason: String| {
            let outcome = ResumeOutcome::Refused { reason };
            (
                Frame::Resumed {
                    last_seen: this.last_seen,
                    epoch: this.config.epoch,
                    outcome,
                },
                Vec::new(),
            )
        };
        if epoch == NO_EPOCH {
            return refuse(self, "nothing to resume; this is a fresh link".to_owned());
        }
        if epoch != self.config.epoch {
            return refuse(
                self,
                format!(
                    "resume is for epoch {epoch}, this link holds epoch {}",
                    self.config.epoch
                ),
            );
        }
        let replay = match self.replay_from(last_seen) {
            Ok(frames) => frames,
            Err(err) => return refuse(self, not_resumable_reason(err)),
        };
        if let Err(err) = self.apply_ack(last_seen) {
            return refuse(self, not_resumable_reason(err));
        }
        (
            Frame::Resumed {
                last_seen: self.last_seen,
                epoch: self.config.epoch,
                outcome: ResumeOutcome::Accepted,
            },
            replay,
        )
    }

    /// The dialling end's half of an accepted resume: the peer's `last_seen` releases what it
    /// consumed, and everything after it is sent again.
    pub fn resumed(&mut self, peer_last_seen: u64) -> Result<Vec<Frame>, LinkError> {
        let replay = self.replay_from(peer_last_seen)?;
        self.apply_ack(peer_last_seen)?;
        Ok(replay)
    }

    fn replay_from(&self, last_seen: u64) -> Result<Vec<Frame>, LinkError> {
        if last_seen > self.next_seq {
            return Err(LinkError::NotResumable(format!(
                "peer claims frame {last_seen}, only {} were sent",
                self.next_seq
            )));
        }
        if last_seen < self.peer_ack {
            return Err(LinkError::NotResumable(format!(
                "frames after {last_seen} were acknowledged and released"
            )));
        }
        Ok(self
            .unacked
            .iter()
            .filter(|sent| sent.seq.get() > last_seen)
            .map(|sent| Frame::Message {
                seq: sent.seq,
                ack: self.last_seen,
                payload: sent.payload.clone(),
            })
            .collect())
    }

    fn apply_ack(&mut self, ack: u64) -> Result<(), LinkError> {
        if ack > self.next_seq {
            return Err(LinkError::AckBeyondSent {
                ack,
                sent: self.next_seq,
            });
        }
        if ack <= self.peer_ack {
            return Ok(());
        }
        self.peer_ack = ack;
        while self
            .unacked
            .front()
            .is_some_and(|sent| sent.seq.get() <= ack)
        {
            let sent = self.unacked.pop_front().expect("front was checked");
            self.unacked_bytes -= sent.payload.get().len();
        }
        Ok(())
    }
}

fn not_resumable_reason(err: LinkError) -> String {
    match err {
        LinkError::NotResumable(reason) => reason,
        other => other.to_string(),
    }
}
