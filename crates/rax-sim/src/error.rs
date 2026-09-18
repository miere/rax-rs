use std::io;
use std::time::Duration;

use rax_tokio::{CallError, SendError};

/// Every wait is bounded and lists what did arrive, so a hung node fails a test with a diagnosis.
#[derive(Debug, thiserror::Error)]
pub enum SimError {
    #[error("rax-sim: waited {after:?} for {waiting_for}; arrived meanwhile: [{}]", .arrived.join(", "))]
    Timeout {
        waiting_for: String,
        after: Duration,
        arrived: Vec<String>,
    },
    #[error("rax-sim: {reason} while waiting for {waiting_for}; arrived: [{}]", .arrived.join(", "))]
    Ended {
        waiting_for: String,
        reason: String,
        arrived: Vec<String>,
    },
    #[error("rax-sim: `{call}` failed: {source}")]
    Call {
        call: &'static str,
        #[source]
        source: CallError,
    },
    #[error("rax-sim: sending {what} failed: {source}")]
    Send {
        what: &'static str,
        #[source]
        source: SendError,
    },
    #[error("rax-sim: the node answered `{call}` with {reply}")]
    UnexpectedReply { call: &'static str, reply: String },
    #[error("rax-sim: {0}")]
    Io(#[from] io::Error),
    #[error("rax-sim: json: {0}")]
    Json(#[from] serde_json::Error),
}

impl SimError {
    pub fn fault(&self) -> Option<&rax::Error> {
        match self {
            Self::Call {
                source: CallError::Fault(fault),
                ..
            } => Some(fault),
            _ => None,
        }
    }

    pub(crate) fn send(what: &'static str) -> impl FnOnce(SendError) -> Self {
        move |source| Self::Send { what, source }
    }
}
