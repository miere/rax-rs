//! What a node tells its gateway about itself. RAX carries the map and never reads it: each key
//! belongs to one gateway, named by its prefix, and only that gateway gives it a meaning.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Keys are `<gateway>_<name>`, so a TOML config can write them bare. A value may be a single
/// value or an object; its shape is the owning gateway's business.
pub type Metadata = BTreeMap<String, Value>;

/// The whole map, replacing the one declared at `initialize` or by an earlier update: a node never
/// sends a partial change, so a gateway never has to merge.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateMetadata {
    #[serde(default)]
    pub metadata: Metadata,
}

/// Why a gateway ended a link over metadata it owns but cannot accept. Terminal, like a rejected
/// credential: the node owner has to fix the configuration, and redialling would only repeat it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rejection {
    /// Absent when the refusal concerns the metadata as a whole rather than one key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    pub message: String,
}

/// Splits a key into the gateway that owns it and the name within that gateway. A gateway name
/// never contains an underscore, so the first one is the boundary.
pub fn owner(key: &str) -> Option<(&str, &str)> {
    key.split_once('_')
        .filter(|(gateway, name)| !gateway.is_empty() && !name.is_empty())
}

/// The entries one gateway owns, by their name within it.
pub fn owned_by<'a>(
    metadata: &'a Metadata,
    gateway: &'a str,
) -> impl Iterator<Item = (&'a str, &'a Value)> {
    metadata
        .iter()
        .filter_map(move |(key, value)| match owner(key) {
            Some((owner, name)) if owner == gateway => Some((name, value)),
            _ => None,
        })
}
