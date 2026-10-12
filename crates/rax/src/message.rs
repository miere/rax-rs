//! Payloads, typed by direction: a node cannot build a prompt, and a gateway cannot build an event.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::activity::Activity;
use crate::attachment::TransferChunk;
use crate::credential::{CredentialHealth, CredentialRenewal};
use crate::error::Error;
use crate::event::{BackgroundEvent, Event};
use crate::id::{RequestId, SessionId};
use crate::interaction::{DisplayAnswer, SignInRequest, SignInSettled};
use crate::local_file::{LocalFileTransfer, ReadLocalFile};
use crate::metadata::{Rejection, UpdateMetadata};
use crate::open::{Open, Unhandled, tagged};
use crate::resource::{ReadResource, ResourceTransfer};
use crate::session::{
    Initialize, Initialized, NewSession, Prompt, PromptAccepted, SessionCreated, SessionRef,
};
use crate::tool::{CallTool, ToolOutcome, ToolVerdict};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "k")]
pub enum GatewayMessage {
    /// An unknown method still decodes, so the node can fault it by id instead of leaving the
    /// gateway waiting.
    #[serde(rename = "req")]
    Request {
        id: RequestId,
        #[serde(flatten)]
        call: Open<GatewayCall>,
    },
    #[serde(rename = "res")]
    Reply {
        id: RequestId,
        #[serde(flatten)]
        reply: NodeReply,
    },
    #[serde(rename = "fault")]
    Fault { id: RequestId, error: Error },
    #[serde(rename = "verdict")]
    Verdict { body: ToolVerdict },
    /// Answers a question, plan or sign-in, keyed by the id the node gave it.
    #[serde(rename = "answer")]
    Answer { body: DisplayAnswer },
    /// Only in answer to `resource.read`, after its reply.
    #[serde(rename = "chunk")]
    Chunk { body: TransferChunk },
    /// `stream` is absent when what went unhandled belonged to no turn.
    #[serde(rename = "unhandled")]
    Unhandled {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stream: Option<RequestId>,
        body: Unhandled,
    },
    /// The last message on a link: the gateway closes it next, and the node must not redial.
    #[serde(rename = "rejected")]
    Rejected { body: Rejection },
}

tagged!(
    GatewayMessage,
    "k",
    [
        "req",
        "res",
        "fault",
        "verdict",
        "answer",
        "chunk",
        "unhandled",
        "rejected"
    ]
);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "k")]
pub enum NodeMessage {
    #[serde(rename = "req")]
    Request {
        id: RequestId,
        #[serde(flatten)]
        call: Open<NodeCall>,
    },
    #[serde(rename = "res")]
    Reply {
        id: RequestId,
        #[serde(flatten)]
        reply: GatewayReply,
    },
    #[serde(rename = "fault")]
    Fault { id: RequestId, error: Error },
    /// `stream` is the id of the prompt call that opened the turn.
    #[serde(rename = "evt")]
    Event {
        stream: RequestId,
        body: Open<Event>,
    },
    /// Its own kind rather than an event, because a stream can end without a terminal event.
    #[serde(rename = "end")]
    End { stream: RequestId },
    /// Addressed by session: it belongs to no turn and would otherwise have nowhere to go.
    #[serde(rename = "bg")]
    Background {
        session_id: SessionId,
        body: Open<BackgroundEvent>,
    },
    /// An attachment's bytes ahead of its event, or the answer to `local_file.read` after its
    /// reply.
    #[serde(rename = "chunk")]
    Chunk { body: TransferChunk },
    #[serde(rename = "unhandled")]
    Unhandled {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stream: Option<RequestId>,
        body: Unhandled,
    },
}

tagged!(
    NodeMessage,
    "k",
    [
        "req",
        "res",
        "fault",
        "evt",
        "end",
        "bg",
        "chunk",
        "unhandled"
    ]
);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "method", content = "body")]
pub enum GatewayCall {
    #[serde(rename = "initialize")]
    Initialize(Initialize),
    #[serde(rename = "session.new")]
    NewSession(NewSession),
    /// The reply is an acceptance, not the turn's outcome; the outcome streams as events under
    /// this call's id.
    #[serde(rename = "prompt")]
    Prompt(Prompt),
    /// Succeeds for an unknown session: cancelling runs on teardown paths where an error is noise.
    #[serde(rename = "cancel")]
    Cancel(SessionRef),
    #[serde(rename = "session.close")]
    CloseSession(SessionRef),
    #[serde(rename = "credential.renew")]
    RenewCredential,
    /// Redeems an identifier the node put in a `tool.call` in place of a path.
    #[serde(rename = "local_file.read")]
    ReadLocalFile(ReadLocalFile),
    #[serde(rename = "close")]
    Close,
}

tagged!(
    GatewayCall,
    "method",
    [
        "initialize",
        "session.new",
        "prompt",
        "cancel",
        "session.close",
        "credential.renew",
        "local_file.read",
        "close",
    ]
);

/// Names its method so a reply decodes without the caller's pending-call table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "method", content = "body")]
pub enum GatewayReply {
    #[serde(rename = "initialize")]
    Initialize(Initialized),
    #[serde(rename = "session.new")]
    NewSession(SessionCreated),
    #[serde(rename = "prompt")]
    Prompt(PromptAccepted),
    #[serde(rename = "cancel")]
    Cancel,
    #[serde(rename = "session.close")]
    CloseSession,
    #[serde(rename = "credential.renew")]
    RenewCredential(CredentialRenewal),
    #[serde(rename = "local_file.read")]
    ReadLocalFile(LocalFileTransfer),
    #[serde(rename = "close")]
    Close,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "method", content = "body")]
pub enum NodeCall {
    /// A sign-in raised with no turn open; one raised inside a turn rides the stream instead.
    #[serde(rename = "sign_in")]
    SignIn(SignInRequest),
    /// Answered so the node sends the next update only once the gateway holds the last.
    #[serde(rename = "sign_in.settled")]
    SignInSettled(SignInSettled),
    /// Answered so reports go one at a time, and a recovery never overtakes the failure it ends.
    #[serde(rename = "credential.health")]
    CredentialHealth(CredentialHealth),
    #[serde(rename = "resource.read")]
    ReadResource(ReadResource),
    /// Runs a tool the gateway published in its catalogue at `initialize`.
    #[serde(rename = "tool.call")]
    CallTool(CallTool),
    /// Answered so updates go one at a time, and a later map never overtakes an earlier one.
    #[serde(rename = "metadata.update")]
    UpdateMetadata(UpdateMetadata),
    /// Answered so reports go one at a time, and an older count never overtakes a newer one.
    #[serde(rename = "activity")]
    Activity(Activity),
}

tagged!(
    NodeCall,
    "method",
    [
        "sign_in",
        "sign_in.settled",
        "credential.health",
        "resource.read",
        "tool.call",
        "metadata.update",
        "activity"
    ]
);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "method", content = "body")]
pub enum NodeReply {
    #[serde(rename = "sign_in")]
    SignIn,
    #[serde(rename = "sign_in.settled")]
    SignInSettled,
    #[serde(rename = "credential.health")]
    CredentialHealth,
    #[serde(rename = "resource.read")]
    ReadResource(ResourceTransfer),
    #[serde(rename = "tool.call")]
    CallTool(ToolOutcome),
    #[serde(rename = "metadata.update")]
    UpdateMetadata,
    #[serde(rename = "activity")]
    Activity,
}

#[derive(Debug, thiserror::Error)]
#[error("rax: {0}")]
pub struct PayloadError(#[from] serde_json::Error);

pub fn to_payload<T: Serialize>(message: &T) -> Result<Box<RawValue>, PayloadError> {
    Ok(serde_json::value::to_raw_value(message)?)
}

/// Decode into `Open<GatewayMessage>` or `Open<NodeMessage>`, so a message kind from a newer peer
/// can be answered with `Unhandled` instead of failing the link.
pub fn from_payload<T: DeserializeOwned>(payload: &RawValue) -> Result<T, PayloadError> {
    Ok(serde_json::from_str(payload.get())?)
}
