//! Files on a node that a gateway tool needs, such as one the agent wants posted to the
//! conversation. The node never sends a path: it sends an identifier it minted, which the gateway
//! redeems once, so a gateway cannot browse the machine.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::id::{LocalFileId, TransferId};

/// The `format` of a string in a tool's `input_schema` that takes the path of a file on the node.
/// A node acts on this marker alone, never on the tool's name.
pub const LOCAL_FILE_FORMAT: &str = "local-file";

/// Redeems an identifier a node put in a `tool.call`. A node answers `not_found` for one it does
/// not hold, whether it was redeemed, forgotten or never minted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadLocalFile {
    pub id: LocalFileId,
    /// Lets the node refuse with `too_large` before sending bytes the gateway would drop.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
}

/// Precedes the bytes, which follow as chunks of this transfer on the same link.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalFileTransfer {
    pub transfer_id: TransferId,
    pub size: u64,
    /// The file's base name, never its path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mimetype: Option<String>,
}

/// Whether a tool's `input_schema` marks any argument as a local file. A gateway lends such a tool
/// only to a node that declared `local_files`.
pub fn takes_local_files(schema: &Value) -> bool {
    if is_marked(schema) {
        return true;
    }
    let properties = schema.get("properties").and_then(Value::as_object);
    properties.is_some_and(|properties| properties.values().any(takes_local_files))
        || schema.get("items").is_some_and(takes_local_files)
}

/// Replaces every argument the schema marks as a local file with what `replace` makes of it. The
/// marker is read on any string the schema reaches through `properties` and `items`. The first
/// refusal stops the walk, because a call must not go out with a path still in it.
pub fn replace_local_files<E>(
    schema: &Value,
    arguments: &mut Value,
    replace: &mut impl FnMut(&str) -> Result<LocalFileId, E>,
) -> Result<(), E> {
    match arguments {
        Value::String(path) if is_marked(schema) => {
            *path = replace(path)?.0;
        }
        Value::Object(fields) => {
            let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
                return Ok(());
            };
            for (name, value) in fields {
                if let Some(schema) = properties.get(name) {
                    replace_local_files(schema, value, replace)?;
                }
            }
        }
        Value::Array(elements) => {
            let Some(schema) = schema.get("items") else {
                return Ok(());
            };
            for value in elements {
                replace_local_files(schema, value, replace)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn is_marked(schema: &Value) -> bool {
    schema.get("format").and_then(Value::as_str) == Some(LOCAL_FILE_FORMAT)
}
