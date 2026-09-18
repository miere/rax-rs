use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::id::TransferId;

pub const MAX_ATTACHMENT_BYTES: u64 = 100 << 20;

/// Base64 inflates by a third, so 4 MiB of data still fits the 8 MiB frame with the envelope.
pub const MAX_TRANSFER_CHUNK_BYTES: usize = 4 << 20;

/// Carries a reference, never the bytes: attachments outgrow a frame, so bytes always ride the
/// transfer's chunks, which precede this event on the same link.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attachment {
    pub transfer_id: TransferId,
    /// Declared up front so a transfer of a different length is a failure, not a truncated file.
    pub size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mimetype: Option<String>,
}

/// `seq` lets a receiver spot a missing chunk instead of delivering a truncated file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferChunk {
    pub transfer_id: TransferId,
    pub seq: u32,
    pub part: ChunkPart,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChunkPart {
    Data(#[serde(serialize_with = "to_base64", deserialize_with = "from_base64")] Vec<u8>),
    End,
    /// The sender could not read the rest; the receiver must drop what it holds.
    Failed(String),
}

fn to_base64<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&STANDARD.encode(bytes))
}

fn from_base64<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
    let encoded = String::deserialize(deserializer)?;
    STANDARD.decode(encoded).map_err(serde::de::Error::custom)
}
