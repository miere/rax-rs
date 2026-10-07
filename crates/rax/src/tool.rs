use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::content::ContentBlock;
use crate::id::{SessionId, ToolCallId};
use crate::open::Open;

/// Announced before the tool runs, and the node must not run it until a [`ToolVerdict`] arrives:
/// the gateway is where a person approves and where an admin's policy lives.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: ToolCallId,
    /// The stable identifier a policy matches on; `title` is for people and may change per call.
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default)]
    pub kind: ToolKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub content: Vec<Open<ContentBlock>>,
}

/// ACP's tool kinds, so a policy can deny a whole class such as `execute` without naming each tool.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolKind {
    Read,
    Edit,
    Delete,
    Move,
    Search,
    Execute,
    Think,
    Fetch,
    SwitchMode,
    #[default]
    #[serde(other)]
    Other,
}

/// The longest namespace a group may have, so `mcp__<namespace>__<name>` stays within the tool
/// name limit of the agents a node runs.
pub const MAX_NAMESPACE_LEN: usize = 32;

/// Tools a gateway runs on a node's behalf, lent to one session when it opens. A node publishes
/// them to its agent and invokes them with `tool.call`; it never learns what any of them do, which
/// is what lets a gateway add one without the node shipping a release.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolGroup {
    /// The group's only identifier: unique within a session, and what qualifies every name on the
    /// node, so a gateway decides what its tools are called rather than answering to a constant
    /// compiled into the node.
    pub namespace: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<ToolDef>,
}

impl ToolGroup {
    /// `[a-z0-9_]`, at most [`MAX_NAMESPACE_LEN`] characters and never empty. A node refuses a
    /// group whose namespace is not, rather than publish a name its agent would mangle.
    pub fn is_valid_namespace(namespace: &str) -> bool {
        !namespace.is_empty()
            && namespace.len() <= MAX_NAMESPACE_LEN
            && namespace
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    }
}

/// The per-link catalogue a gateway sends at `initialize`, kept only for nodes that do not declare
/// `tool_groups`. It has the same shape as a group.
#[deprecated(note = "gateways lend tools per session in `session.new`; use `ToolGroup`")]
pub type ToolCatalogue = ToolGroup;

/// `kind` is the gateway's, not the node's: only the gateway knows whether its tool reads or
/// destroys, and a node that guessed from the name would mislabel every card it draws.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolDef {
    pub name: String,
    pub description: String,
    /// A JSON Schema object. Absent means the tool takes no arguments.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_schema: Option<Value>,
    #[serde(default)]
    pub kind: ToolKind,
}

/// Runs one tool a gateway lent. A gateway serves only groups the session was opened with, so a
/// node cannot reach a tool by guessing; anything else is `forbidden`, as for resources.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallTool {
    /// Binds the call to a conversation, because a gateway's tools act inside one — which thread
    /// to post to is the gateway's to resolve, and never travels to the node.
    pub session_id: SessionId,
    /// The group the tool belongs to. Absent means the deprecated catalogue sent at `initialize`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arguments: Option<Value>,
}

/// A tool that ran and failed is an answer the agent reads and may act on, so it is a reply with
/// `is_error`, not a fault. A fault means the call never ran at all.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolOutcome {
    pub content: String,
    #[serde(default)]
    pub is_error: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCallUpdate {
    pub id: ToolCallId,
    pub status: ToolCallStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub content: Vec<Open<ContentBlock>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallStatus {
    InProgress,
    Completed,
    Failed,
    /// Sent once the node has honoured a deny, so the UI settles the card from the node's word
    /// rather than assuming.
    Denied,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolVerdict {
    pub id: ToolCallId,
    #[serde(flatten)]
    pub decision: Decision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum Decision {
    Allow,
    /// `reason` is handed to the agent as the tool's result, so it must read as an answer.
    Deny {
        by: DeniedBy,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeniedBy {
    User,
    Policy,
    Timeout,
    /// Nobody could be asked, such as a call raised with no conversation to ask in.
    Unavailable,
    #[serde(other)]
    Other,
}

/// ACP's plan entry: the agent's own to-do list, shown as progress and never answered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanEntry {
    pub content: String,
    pub priority: PlanEntryPriority,
    pub status: PlanEntryStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanEntryPriority {
    High,
    Medium,
    Low,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanEntryStatus {
    Pending,
    InProgress,
    Completed,
}
