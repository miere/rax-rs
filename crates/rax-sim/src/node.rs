use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rax::credential::CredentialRenewal;
use rax::event::BackgroundEvent;
use rax::id::{RequestId, SessionId, ToolCallId, TransferId};
use rax::interaction::DisplayAnswer;
use rax::session::{
    GatewayCapabilities, Initialize, Initialized, NewSession, Prompt, SessionCreated, SessionRef,
};
use rax::{
    ContentBlock, Decision, Event, GatewayCall, GatewayMessage, GatewayReply, NodeCall, NodeReply,
    Open, PROTOCOL_VERSION, ToolCall, ToolVerdict, Unhandled,
};
use rax_tokio::Disconnect;
use rax_tokio::gateway::{GatewayLink, LinkEvent, LinkEvents, NodeIdentity, PendingReply};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::SimError;
use crate::inbox::{Inbox, lock};
use crate::policy::{AnswerPolicy, NodeOptions, VerdictPolicy};
use crate::record::{micros, truncate};
use crate::turn::{PendingTurn, SimTurn};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeMethod {
    SignIn,
    SignInSettled,
    CredentialHealth,
}

impl NodeMethod {
    pub fn of(call: &NodeCall) -> Self {
        match call {
            NodeCall::SignIn(_) => Self::SignIn,
            NodeCall::SignInSettled(_) => Self::SignInSettled,
            NodeCall::CredentialHealth(_) => Self::CredentialHealth,
        }
    }

    fn reply(self) -> NodeReply {
        match self {
            Self::SignIn => NodeReply::SignIn,
            Self::SignInSettled => NodeReply::SignInSettled,
            Self::CredentialHealth => NodeReply::CredentialHealth,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallRecord {
    pub id: RequestId,
    pub method: NodeMethod,
    #[serde(with = "micros")]
    pub arrived: Duration,
    #[serde(with = "micros::option")]
    pub answered: Option<Duration>,
}

/// A call that arrived while an earlier call of the checked methods was still unanswered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Overlap {
    pub earlier: RequestId,
    pub later: RequestId,
}

enum Mark {
    Arrived(usize),
    Answered(usize),
}

#[derive(Default)]
struct CallLog {
    records: Vec<CallRecord>,
    marks: Vec<Mark>,
}

impl CallLog {
    fn arrived(&mut self, id: RequestId, method: NodeMethod, arrived: Duration) -> usize {
        let index = self.records.len();
        self.records.push(CallRecord {
            id,
            method,
            arrived,
            answered: None,
        });
        self.marks.push(Mark::Arrived(index));
        index
    }

    fn answered(&mut self, index: usize, at: Duration) {
        if let Some(record) = self.records.get_mut(index)
            && record.answered.is_none()
        {
            record.answered = Some(at);
            self.marks.push(Mark::Answered(index));
        }
    }

    fn overlaps(&self, methods: &[NodeMethod]) -> Vec<Overlap> {
        let mut open: Vec<usize> = Vec::new();
        let mut overlaps = Vec::new();
        for mark in &self.marks {
            match *mark {
                Mark::Arrived(index) => {
                    let Some(record) = self.records.get(index) else {
                        continue;
                    };
                    if !methods.contains(&record.method) {
                        continue;
                    }
                    for earlier in open.iter().filter_map(|open| self.records.get(*open)) {
                        overlaps.push(Overlap {
                            earlier: earlier.id.clone(),
                            later: record.id.clone(),
                        });
                    }
                    open.push(index);
                }
                Mark::Answered(index) => open.retain(|open| *open != index),
            }
        }
        overlaps
    }
}

/// Dropping a request without answering leaves the node waiting, which is itself a scenario.
pub struct NodeCallRequest {
    pub id: RequestId,
    pub call: NodeCall,
    pub arrived: Duration,
    index: usize,
    link: GatewayLink,
    log: Arc<Mutex<CallLog>>,
    started: Instant,
}

impl NodeCallRequest {
    pub fn method(&self) -> NodeMethod {
        NodeMethod::of(&self.call)
    }

    pub async fn reply(self) -> Result<(), SimError> {
        self.mark_answered();
        let reply = self.method().reply();
        self.link
            .reply(self.id, reply)
            .await
            .map_err(SimError::send("a node call reply"))
    }

    pub async fn fault(self, error: rax::Error) -> Result<(), SimError> {
        self.mark_answered();
        self.link
            .fault(self.id, error)
            .await
            .map_err(SimError::send("a node call fault"))
    }

    fn mark_answered(&self) {
        lock(&self.log).answered(self.index, truncate(self.started.elapsed()));
    }
}

impl fmt::Debug for NodeCallRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeCallRequest")
            .field("id", &self.id)
            .field("call", &self.call)
            .field("arrived", &self.arrived)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum Transfer {
    Complete(Vec<u8>),
    Failed(String),
}

impl fmt::Debug for Transfer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Complete(bytes) => write!(f, "Complete({} bytes)", bytes.len()),
            Self::Failed(reason) => f.debug_tuple("Failed").field(reason).finish(),
        }
    }
}

#[derive(Debug)]
pub enum LinkChange {
    Resumed,
    Disconnected(Disconnect),
    /// Terminal: the node started a fresh link, which arrives as a new `SimNode`.
    Replaced,
    Closed,
}

pub(crate) struct NodeShared {
    pub(crate) link: GatewayLink,
    pub(crate) wait: Duration,
    started: Instant,
    options: Mutex<NodeOptions>,
    log: Arc<Mutex<CallLog>>,
    calls: Inbox<NodeCallRequest>,
    background: Inbox<(SessionId, Open<BackgroundEvent>)>,
    transfers: Inbox<(TransferId, Transfer)>,
    unhandled: Inbox<(Option<RequestId>, Unhandled)>,
    changes: Inbox<LinkChange>,
}

impl NodeShared {
    fn verdicts(&self) -> VerdictPolicy {
        lock(&self.options).verdicts.clone()
    }

    fn answers(&self) -> AnswerPolicy {
        lock(&self.options).answers.clone()
    }

    pub(crate) async fn apply_policies(&self, event: &Event) {
        if let Event::ToolCall { tool_call } = event {
            self.rule(tool_call).await;
        }
        if let Some(answer) = self.answers().answer(event)
            && let Err(err) = self.link.answer(answer).await
        {
            tracing::debug!(error = %err, "rax-sim policy answer not sent");
        }
    }

    async fn rule(&self, tool_call: &ToolCall) {
        let Some(decision) = self.verdicts().decide(tool_call) else {
            return;
        };
        let verdict = ToolVerdict {
            id: tool_call.id.clone(),
            decision,
        };
        if let Err(err) = self.link.verdict(verdict).await {
            tracing::debug!(error = %err, "rax-sim policy verdict not sent");
        }
    }

    async fn request(&self, id: RequestId, call: NodeCall) {
        let method = NodeMethod::of(&call);
        let arrived = truncate(self.started.elapsed());
        let index = lock(&self.log).arrived(id.clone(), method, arrived);
        if lock(&self.options).auto_reply.contains(&method) {
            lock(&self.log).answered(index, truncate(self.started.elapsed()));
            if let Err(err) = self.link.reply(id, method.reply()).await {
                tracing::debug!(error = %err, "rax-sim automatic reply not sent");
            }
            return;
        }
        self.calls.push(NodeCallRequest {
            id,
            call,
            arrived,
            index,
            link: self.link.clone(),
            log: self.log.clone(),
            started: self.started,
        });
    }
}

/// One fresh link. Everything the node sends outside a turn is queued here until a test reads it,
/// so an ignored queue never stalls the link.
#[derive(Clone)]
pub struct SimNode {
    pub(crate) shared: Arc<NodeShared>,
}

impl fmt::Debug for SimNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimNode")
            .field("identity", self.identity())
            .finish()
    }
}

impl SimNode {
    pub(crate) fn start(
        link: GatewayLink,
        events: LinkEvents,
        options: NodeOptions,
        wait: Duration,
        started: Instant,
    ) -> Self {
        let shared = Arc::new(NodeShared {
            link,
            wait,
            started,
            options: Mutex::new(options),
            log: Arc::default(),
            calls: Inbox::new(),
            background: Inbox::new(),
            transfers: Inbox::new(),
            unhandled: Inbox::new(),
            changes: Inbox::new(),
        });
        tokio::spawn(pump(events, shared.clone()));
        Self { shared }
    }

    pub fn identity(&self) -> &NodeIdentity {
        self.shared.link.identity()
    }

    pub fn link(&self) -> &GatewayLink {
        &self.shared.link
    }

    pub fn set_options(&self, options: NodeOptions) {
        *lock(&self.shared.options) = options;
    }

    pub fn set_verdicts(&self, verdicts: VerdictPolicy) {
        lock(&self.shared.options).verdicts = verdicts;
    }

    pub fn set_answers(&self, answers: AnswerPolicy) {
        lock(&self.shared.options).answers = answers;
    }

    pub async fn initialize(
        &self,
        capabilities: GatewayCapabilities,
    ) -> Result<Initialized, SimError> {
        let call = GatewayCall::Initialize(Initialize {
            protocol_version: PROTOCOL_VERSION,
            capabilities,
        });
        match self.call("initialize", call).await? {
            GatewayReply::Initialize(initialized) => Ok(initialized),
            other => Err(unexpected("initialize", &other)),
        }
    }

    pub async fn new_session(
        &self,
        context: Vec<Open<ContentBlock>>,
    ) -> Result<SessionCreated, SimError> {
        let call = GatewayCall::NewSession(NewSession { context });
        match self.call("session.new", call).await? {
            GatewayReply::NewSession(created) => Ok(created),
            other => Err(unexpected("session.new", &other)),
        }
    }

    /// Returns before the reply, so a test can cancel while the node is still starting the turn.
    pub async fn start_prompt(
        &self,
        session_id: impl Into<SessionId>,
        content: Vec<Open<ContentBlock>>,
    ) -> Result<PendingTurn, SimError> {
        let call = GatewayCall::Prompt(Prompt {
            session_id: session_id.into(),
            content,
        });
        let pending = self
            .shared
            .link
            .call(call)
            .await
            .map_err(|source| SimError::Call {
                call: "prompt",
                source,
            })?;
        Ok(PendingTurn::start(pending, self.shared.clone()))
    }

    pub async fn prompt(
        &self,
        session_id: impl Into<SessionId>,
        content: Vec<Open<ContentBlock>>,
    ) -> Result<SimTurn, SimError> {
        self.start_prompt(session_id, content)
            .await?
            .accepted()
            .await
    }

    pub async fn cancel(&self, session_id: impl Into<SessionId>) -> Result<(), SimError> {
        let call = GatewayCall::Cancel(SessionRef {
            session_id: session_id.into(),
        });
        match self.call("cancel", call).await? {
            GatewayReply::Cancel => Ok(()),
            other => Err(unexpected("cancel", &other)),
        }
    }

    pub async fn close_session(&self, session_id: impl Into<SessionId>) -> Result<(), SimError> {
        let call = GatewayCall::CloseSession(SessionRef {
            session_id: session_id.into(),
        });
        match self.call("session.close", call).await? {
            GatewayReply::CloseSession => Ok(()),
            other => Err(unexpected("session.close", &other)),
        }
    }

    pub async fn renew_credential(&self) -> Result<CredentialRenewal, SimError> {
        match self
            .call("credential.renew", GatewayCall::RenewCredential)
            .await?
        {
            GatewayReply::RenewCredential(renewal) => Ok(renewal),
            other => Err(unexpected("credential.renew", &other)),
        }
    }

    /// Sends the `close` call; the node decides how its socket ends.
    pub async fn close(&self) -> Result<(), SimError> {
        match self.call("close", GatewayCall::Close).await? {
            GatewayReply::Close => Ok(()),
            other => Err(unexpected("close", &other)),
        }
    }

    /// Ends the link from the gateway side, as a gateway that forgets a node would.
    pub async fn end_link(&self) {
        self.shared.link.close().await;
    }

    pub async fn raw_call(&self, call: Open<GatewayCall>) -> Result<GatewayReply, SimError> {
        self.call("raw call", call).await
    }

    /// A known kind is still validated, so only an unknown kind reaches the node unchecked.
    pub async fn send_raw_message(&self, message: Value) -> Result<(), SimError> {
        let message: Open<GatewayMessage> = serde_json::from_value(message)?;
        self.shared
            .link
            .relay(message)
            .await
            .map_err(SimError::send("a raw message"))
    }

    pub async fn verdict(
        &self,
        tool_call_id: impl Into<ToolCallId>,
        decision: Decision,
    ) -> Result<(), SimError> {
        let verdict = ToolVerdict {
            id: tool_call_id.into(),
            decision,
        };
        self.shared
            .link
            .verdict(verdict)
            .await
            .map_err(SimError::send("a verdict"))
    }

    pub async fn answer(&self, answer: DisplayAnswer) -> Result<(), SimError> {
        self.shared
            .link
            .answer(answer)
            .await
            .map_err(SimError::send("an answer"))
    }

    pub async fn next_node_call(&self) -> Result<NodeCallRequest, SimError> {
        let shared = &self.shared;
        shared.calls.next(shared.wait, "a node call").await
    }

    pub async fn next_background(&self) -> Result<(SessionId, Open<BackgroundEvent>), SimError> {
        let shared = &self.shared;
        shared
            .background
            .next(shared.wait, "a background event")
            .await
    }

    pub async fn next_transfer(&self) -> Result<(TransferId, Transfer), SimError> {
        let shared = &self.shared;
        shared.transfers.next(shared.wait, "a transfer").await
    }

    pub async fn transfer(&self, transfer_id: &TransferId) -> Result<Transfer, SimError> {
        let shared = &self.shared;
        let waiting_for = format!("transfer {transfer_id}");
        let (_, transfer) = shared
            .transfers
            .take(shared.wait, &waiting_for, |(id, _)| id == transfer_id)
            .await?;
        Ok(transfer)
    }

    pub async fn next_unhandled(&self) -> Result<(Option<RequestId>, Unhandled), SimError> {
        let shared = &self.shared;
        shared
            .unhandled
            .next(shared.wait, "an unhandled report")
            .await
    }

    pub async fn next_link_change(&self) -> Result<LinkChange, SimError> {
        let shared = &self.shared;
        shared.changes.next(shared.wait, "a link change").await
    }

    pub fn call_log(&self) -> Vec<CallRecord> {
        lock(&self.shared.log).records.clone()
    }

    pub fn call_overlaps(&self, methods: &[NodeMethod]) -> Vec<Overlap> {
        lock(&self.shared.log).overlaps(methods)
    }

    async fn call(
        &self,
        name: &'static str,
        call: impl Into<Open<GatewayCall>>,
    ) -> Result<GatewayReply, SimError> {
        let pending = self
            .shared
            .link
            .call(call)
            .await
            .map_err(|source| SimError::Call { call: name, source })?;
        settle(name, pending.reply, self.shared.wait).await
    }
}

pub(crate) async fn settle(
    name: &'static str,
    reply: PendingReply,
    wait: Duration,
) -> Result<GatewayReply, SimError> {
    match tokio::time::timeout(wait, reply).await {
        Err(_) => Err(SimError::Timeout {
            waiting_for: format!("the reply to `{name}`"),
            after: wait,
            arrived: Vec::new(),
        }),
        Ok(Err(source)) => Err(SimError::Call { call: name, source }),
        Ok(Ok(reply)) => Ok(reply),
    }
}

pub(crate) fn unexpected(call: &'static str, reply: &GatewayReply) -> SimError {
    SimError::UnexpectedReply {
        call,
        reply: format!("{reply:?}"),
    }
}

async fn pump(mut events: LinkEvents, shared: Arc<NodeShared>) {
    let mut ended = "the link ended";
    while let Some(event) = events.recv().await {
        match event {
            LinkEvent::Request { id, call } => shared.request(id, call).await,
            LinkEvent::Background { session_id, event } => {
                if let Open::Known(BackgroundEvent::ToolCall { tool_call }) = &event {
                    shared.rule(tool_call).await;
                }
                shared.background.push((session_id, event));
            }
            LinkEvent::Attachment { transfer_id, bytes } => {
                shared
                    .transfers
                    .push((transfer_id, Transfer::Complete(bytes)));
            }
            LinkEvent::TransferFailed {
                transfer_id,
                reason,
            } => shared
                .transfers
                .push((transfer_id, Transfer::Failed(reason))),
            LinkEvent::Unhandled { stream, body } => shared.unhandled.push((stream, body)),
            LinkEvent::Resumed => shared.changes.push(LinkChange::Resumed),
            LinkEvent::Disconnected { reason } => {
                shared.changes.push(LinkChange::Disconnected(reason));
            }
            LinkEvent::Replaced => {
                ended = "the node replaced the link";
                shared.changes.push(LinkChange::Replaced);
            }
            LinkEvent::Closed => {
                ended = "the link was closed";
                shared.changes.push(LinkChange::Closed);
            }
        }
    }
    shared.calls.end(ended, false);
    shared.background.end(ended, false);
    shared.transfers.end(ended, false);
    shared.unhandled.end(ended, false);
    shared.changes.end(ended, false);
}
