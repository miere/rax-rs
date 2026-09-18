//! ACP's content blocks, field for field, so a node bridging an ACP agent relays them untouched.

use serde::{Deserialize, Serialize};

use crate::open::tagged;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    Text {
        text: String,
    },
    Image {
        data: String,
        #[serde(rename = "mimeType")]
        mime_type: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        uri: Option<String>,
    },
    Audio {
        data: String,
        #[serde(rename = "mimeType")]
        mime_type: String,
    },
    /// The scheme is never interpreted by RAX: a receiver that cannot follow it says so with
    /// `Unhandled`, which is how UI context reaches an agent without the protocol knowing any UI.
    ResourceLink {
        uri: String,
        name: String,
        #[serde(rename = "mimeType", default, skip_serializing_if = "Option::is_none")]
        mime_type: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        description: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        size: Option<u64>,
    },
    Resource {
        resource: EmbeddedResource,
    },
}

tagged!(
    ContentBlock,
    "type",
    ["text", "image", "audio", "resource_link", "resource"]
);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EmbeddedResource {
    Text {
        uri: String,
        text: String,
        #[serde(rename = "mimeType", default, skip_serializing_if = "Option::is_none")]
        mime_type: Option<String>,
    },
    Blob {
        uri: String,
        blob: String,
        #[serde(rename = "mimeType", default, skip_serializing_if = "Option::is_none")]
        mime_type: Option<String>,
    },
}

impl ContentBlock {
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text { text: text.into() }
    }

    pub fn link(uri: impl Into<String>, name: impl Into<String>) -> Self {
        Self::ResourceLink {
            uri: uri.into(),
            name: name.into(),
            mime_type: None,
            title: None,
            description: None,
            size: None,
        }
    }
}
