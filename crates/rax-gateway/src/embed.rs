//! What the embedder brings to the hub and the relay. Each has a default that needs nothing, which
//! is what a standalone gateway runs with: nobody to ask, no files of its own, no tools to lend.

use std::sync::Arc;

use async_trait::async_trait;
use rax::id::RequestId;
use rax::interaction::{SignInRequest, SignInSettled};
use rax::resource::ReadResource;
use rax::tool::{CallTool, Decision, DeniedBy, ToolCall, ToolGroup, ToolVerdict};
use rax::{Error, ErrorKind};
use rax_tokio::gateway::GatewayLink;
use tokio_util::sync::CancellationToken;

use crate::fleet::Node;
use crate::settings::Settings;
use crate::store::UserId;

/// A node, as a sign-in or an approval needs it.
#[derive(Clone)]
pub struct NodeRef {
    pub selector: String,
    pub name: String,
    pub owner: UserId,
    pub link: GatewayLink,
}

impl From<&Node> for NodeRef {
    fn from(node: &Node) -> Self {
        Self {
            selector: node.selector.clone(),
            name: node.name.clone(),
            owner: node.owner.clone(),
            link: node.link.clone(),
        }
    }
}

/// A client's call that only the node's owner may approve, because the client is not theirs.
pub struct ToolApproval {
    pub node: NodeRef,
    /// Whose client the agent is working for.
    pub requester: UserId,
    /// The client's name, as its token gives it.
    pub client: String,
    pub tool: ToolCall,
    /// Cancelled when the turn ends: nobody need be asked any more.
    pub turn: CancellationToken,
}

/// How a node's owner is reached when something only they can decide comes up.
#[async_trait]
pub trait OwnerChannel: Send + Sync + 'static {
    /// Whether the owner can be asked to sign in, which the gateway offers nodes at `initialize`.
    fn signs_in(&self) -> bool {
        false
    }

    /// Puts the call to the owner. It must reach a verdict on `ask.node.link`, now or once they
    /// answer; return as soon as the asking is under way.
    async fn ask_tool_approval(&self, ask: ToolApproval) {
        deny_unasked(ask).await;
    }

    /// Shows the owner a sign-in their node needs.
    async fn ask_sign_in(&self, node: NodeRef, request: SignInRequest) -> Result<(), Error> {
        let _ = (node, request);
        Err(Error::new(
            ErrorKind::Unsupported,
            "this gateway cannot reach the node's owner",
        ))
    }

    /// The node's own sign-in moved on.
    async fn sign_in_settled(&self, selector: &str, settled: SignInSettled) {
        let _ = (selector, settled);
    }
}

/// Denies a call that only the owner may approve, because they could not be asked.
pub async fn deny_unasked(ask: ToolApproval) {
    let verdict = ToolVerdict {
        id: ask.tool.id,
        decision: Decision::Deny {
            by: DeniedBy::Unavailable,
            reason: Some(
                "Only the node's owner can approve this, and they could not be asked.".into(),
            ),
        },
    };
    if let Err(err) = ask.node.link.verdict(verdict).await {
        tracing::warn!(node = %ask.node.name, error = %err, "could not rule on a client's tool call");
    }
}

/// Nobody to ask: every call only an owner may approve is denied, and no sign-in can be shown.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoOwner;

impl OwnerChannel for NoOwner {}

/// Resources the embedder links in prompts, which a node reads back with `resource.read`.
#[async_trait]
pub trait FileSource: Send + Sync + 'static {
    /// The schemes it serves, offered to nodes as readable at `initialize`.
    fn schemes(&self) -> Vec<String> {
        Vec::new()
    }

    /// Answers a node's read of a URI in one of [`Self::schemes`], or any other the relay does not
    /// serve.
    async fn serve(&self, link: &GatewayLink, selector: &str, id: RequestId, read: ReadResource) {
        let _ = selector;
        let never = Error::new(
            ErrorKind::Forbidden,
            format!("this gateway never sent {} to this node", read.uri),
        );
        if let Err(err) = link.fault(id, never).await {
            tracing::warn!(uri = %read.uri, error = %err, "could not answer a node's file read");
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoFiles;

impl FileSource for NoFiles {}

/// Tools the embedder runs on a node's behalf, in sessions it opened itself. A client's sessions
/// are lent the client's tools, which the relay serves.
#[async_trait]
pub trait Tools: Send + Sync + 'static {
    /// The deprecated per-link catalogue offered at `initialize`, for nodes that predate tool
    /// groups.
    fn catalogue(&self) -> Option<ToolGroup> {
        None
    }

    /// Answers a node's `tool.call` in a session the relay does not own.
    async fn serve(&self, link: &GatewayLink, node: &Node, id: RequestId, call: CallTool) {
        let unoffered = Error::new(
            ErrorKind::Forbidden,
            match &call.namespace {
                Some(namespace) => format!(
                    "this session was not lent a tool named {namespace}.{}",
                    call.name
                ),
                None => format!("this gateway publishes no tool named {}", call.name),
            },
        );
        if let Err(err) = link.fault(id, unoffered).await {
            tracing::debug!(node = %node.name, error = %err, "could not refuse a tool call");
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoTools;

impl Tools for NoTools {}

/// Everything the embedder brings, in one place.
#[derive(Clone)]
pub struct Embedding {
    pub settings: Settings,
    pub owner: Arc<dyn OwnerChannel>,
    pub files: Arc<dyn FileSource>,
    pub tools: Arc<dyn Tools>,
}

impl Default for Embedding {
    fn default() -> Self {
        Self {
            settings: Settings::default(),
            owner: Arc::new(NoOwner),
            files: Arc::new(NoFiles),
            tools: Arc::new(NoTools),
        }
    }
}
