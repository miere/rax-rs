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
    #[error("rax-sim: only `resource.read` can be served bytes, not {0:?}")]
    NotARead(crate::node::NodeMethod),
    #[error("rax-sim: only `tool.call` can be answered with a tool outcome, not {0:?}")]
    NotAToolCall(crate::node::NodeMethod),
    #[error("rax-sim: serving a resource failed: {0}")]
    Serve(String),
    #[error("rax-sim: {0}")]
    Io(#[from] io::Error),
    #[error("{0}")]
    Endpoint(#[from] rax::transport::EndpointError),
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
