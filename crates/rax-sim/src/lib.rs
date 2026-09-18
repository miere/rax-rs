//! A proxy in front of a real `rax_tokio` gateway records the wire and injects faults, so what a
//! test asserts is what the node actually sent.

mod error;
mod inbox;
mod node;
mod policy;
mod proxy;
mod record;
mod report;
mod simulator;
mod turn;
mod ws;

pub use error::SimError;
pub use node::{CallRecord, LinkChange, NodeCallRequest, NodeMethod, Overlap, SimNode, Transfer};
pub use policy::{AnswerPolicy, NodeOptions, VerdictPolicy};
pub use record::{
    CLOSE_ABNORMAL, CLOSE_NO_STATUS, ConnectionId, Delivered, Direction, Entry, Handshake,
    Transcript, WireFrame,
};
pub use report::{Report, ResumeCounts, ToolCallReport, TurnReport};
pub use simulator::{SimConfig, Simulator};
pub use turn::{Match, PendingTurn, SimTurn};
