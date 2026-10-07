use serde::{Deserialize, Serialize};

use crate::content::ContentBlock;
use crate::id::SessionId;
use crate::metadata::Metadata;
use crate::open::{Open, Unhandled};
use crate::tool::ToolGroup;

/// Negotiated at `initialize`, unlike the frame version: nodes and gateways upgrade on their own
/// schedules, so an older peer must still be served.
pub const PROTOCOL_VERSION: u16 = 1;

/// The gateway offers the newest version it speaks; the node answers with the one it will speak,
/// and the gateway closes the link if it cannot speak that one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Initialize {
    pub protocol_version: u16,
    #[serde(default)]
    pub capabilities: GatewayCapabilities,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Initialized {
    pub protocol_version: u16,
    #[serde(default)]
    pub capabilities: NodeCapabilities,
    /// Not a capability: RAX never reads it, and only the gateway owning a key acts on it.
    #[serde(default, skip_serializing_if = "Metadata::is_empty")]
    pub metadata: Metadata,
}

/// Hints that save a round trip, never guarantees: a peer must still answer anything it receives
/// and cannot handle with `Unhandled`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayCapabilities {
    #[serde(default)]
    pub question: bool,
    #[serde(default)]
    pub plan: bool,
    #[serde(default)]
    pub sign_in: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resource_schemes: Vec<String>,
    /// Schemes whose links a node can fetch with `resource.read`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub readable_schemes: Vec<String>,
    /// Deprecated: the per-link catalogue, for nodes that do not declare `tool_groups`. A node
    /// that does ignores it and uses each session's groups instead. Absent means none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tools: Option<ToolGroup>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeCapabilities {
    /// Absent means unknown, which a gateway must treat as interruptible rather than as a refusal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interruptible: Option<bool>,
    #[serde(default)]
    pub prompt: PromptCapabilities,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resource_schemes: Vec<String>,
    /// Unlike the other capabilities this is a promise, not a hint: a gateway enforcing a tool
    /// policy must know whether a deny can actually stop a call.
    #[serde(default)]
    pub tool_gate: ToolGate,
    #[serde(default)]
    pub sessions: SessionDurability,
    /// The node takes its gateway tools from each `session.new`, so a gateway sends it groups
    /// there and no longer relies on the catalogue at `initialize`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub tool_groups: bool,
}

/// Whether a session outlives the node process is the node's to declare, because only the node
/// knows whether its agent can reload one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionDurability {
    /// Still a hint: a session the node fails to restore is faulted with `unknown_session`.
    Durable,
    /// Absent or unrecognised values mean this, so a gateway never assumes a restart kept context.
    #[default]
    #[serde(other)]
    Ephemeral,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolGate {
    /// Every tool call is announced and held until its verdict, whatever the agent would decide.
    EveryCall,
    /// Only calls the agent itself asks permission for are announced; others run unseen. Absent
    /// or unrecognised values mean this, so a gateway never assumes more than was promised.
    #[default]
    #[serde(other)]
    PermissionPrompts,
}

/// Text and resource links are always accepted, as in ACP.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptCapabilities {
    #[serde(default)]
    pub image: bool,
    #[serde(default)]
    pub audio: bool,
    #[serde(default)]
    pub embedded_resource: bool,
}

/// `context` is where the conversation lives, as resource links whose schemes only the two ends
/// agree on; the protocol carries no UI fields of its own.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewSession {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub context: Vec<Open<ContentBlock>>,
    /// Every gateway tool the session will see, fixed for its lifetime. Sent only to nodes that
    /// declared `tool_groups`; absent or empty means none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_groups: Vec<ToolGroup>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionCreated {
    pub session_id: SessionId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unhandled: Vec<Unhandled>,
}

/// Turn-scoped context, such as the document a mention came from, rides in `content` next to the
/// person's words.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Prompt {
    pub session_id: SessionId,
    pub content: Vec<Open<ContentBlock>>,
}

/// An acceptance, not the turn's outcome. Unhandled blocks do not reject the prompt: the gateway
/// decides whether the turn is still worth having.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptAccepted {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unhandled: Vec<Unhandled>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRef {
    pub session_id: SessionId,
}
