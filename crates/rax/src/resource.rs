use serde::{Deserialize, Serialize};

use crate::id::TransferId;

/// Fetches what a gateway linked instead of inlining, because it outgrows a frame. A gateway
/// serves only URIs it sent to the asking node, so a node cannot read by guessing identifiers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadResource {
    pub uri: String,
    /// Lets the gateway refuse with `too_large` before sending bytes the node would drop.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
}

/// Precedes the bytes, which follow as chunks of this transfer on the same link.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceTransfer {
    pub transfer_id: TransferId,
    pub size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mimetype: Option<String>,
}
