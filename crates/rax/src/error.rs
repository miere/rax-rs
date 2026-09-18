use serde::{Deserialize, Serialize};

/// Keeps the producer's full text beside its kind: a discriminant without the text keeps the
/// branch and loses the diagnosis.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("{message}")]
pub struct Error {
    pub message: String,
    #[serde(flatten)]
    pub kind: ErrorKind,
}

/// Exists because a peer compares failures by identity, which serialisation destroys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ErrorKind {
    Cancelled,
    ToolCeiling,
    /// The node's owner already has a sign-in in front of them; the gateway must not start another.
    Credential,
    /// The receiver does not implement the method it was asked to serve.
    Unsupported,
    /// The node never had the session or could not restore it; the gateway opens a new one.
    UnknownSession,
    /// The session already has a turn open, and a node never queues a second one behind it.
    SessionBusy,
    Provider {
        provider: ProviderFailure,
    },
    Rpc {
        rpc: RpcFailure,
    },
    /// Also what a kind from a newer peer decodes to, so the text survives a version skew.
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderFailure {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_code: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default)]
    pub retryable: bool,
}

/// The structured half of a JSON-RPC fault from an ACP agent, which a text-only error flattens.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcFailure {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    pub code: i64,
}

impl Error {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind,
        }
    }
}
