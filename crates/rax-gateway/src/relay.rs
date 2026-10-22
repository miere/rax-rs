//! The RAX API: a person's own client — an editor bridged over RAX, say — dials in as a gateway
//! with a client token, and this side plays the node toward it. Each session it opens
//! runs on a node of the fleet, and everything in between is relayed: prompts and turns one way,
//! tool calls, reads and questions the other.
//!
//! A bridge session lives in memory only and is never pinned. A bridge's link that does not come
//! back within RAX's `retain_for` takes its sessions with it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use rax::content::ContentBlock;
use rax::credential::CredentialRenewal;
use rax::event::BackgroundEvent;
use rax::id::{PromptId, RequestId, SessionId, ToolCallId};
use rax::interaction::{DisplayAnswer, DisplayOutcome};
use rax::open::{Subject, UnhandledReason};
use rax::resource::ReadResource;
use rax::session::{
    GatewayCapabilities, Initialize, Initialized, NewSession, NodeCapabilities, PROTOCOL_VERSION,
    Prompt, PromptAccepted, SessionCreated, SessionDurability, SessionRef, ToolGate,
};
use rax::tool::{
    CallTool, Decision, ToolCall, ToolCallStatus, ToolCallUpdate, ToolGroup, ToolVerdict,
};
use rax::{
    Error, ErrorKind, Event, GatewayCall, GatewayReply, NodeCall, NodeReply, Open, Unhandled,
};
use rax_tokio::CallError;
use rax_tokio::accept::GatewayIdentity;
use rax_tokio::gateway::{GatewayLink, StreamEvents};
use rax_tokio::node::{NewNodeLink, NodeEvent, NodeEvents, NodeHandle};

use tokio_util::sync::CancellationToken;

use crate::access::Access;
use crate::embed::{NodeRef, OwnerChannel, ToolApproval};
use crate::fleet::{Fleet, Node};
use crate::lent::Lent;
use crate::policy::{self, Ruling};
use crate::settings::Settings;
use crate::store::{Store, UserId};
use crate::transfers::Transfers;

/// The scheme a node reads a bridge's resources through: `bridge://<link id>/<original uri>`.
pub const SCHEME: &str = "bridge";
const LINK_ID_LEN: usize = 4;
/// Long enough for a node to fetch the files a prompt links before accepting it.
const PROMPT_TIMEOUT: Duration = Duration::from_secs(120);
/// The chunks precede the `attachment` event on the node's link, so the bytes are normally there.
const ATTACHMENT_WAIT: Duration = Duration::from_secs(60);

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// What one bridge session is, for an embedder to show.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Summary {
    pub owner: UserId,
    pub client: String,
    pub node: String,
}

#[derive(Clone)]
pub struct Relay {
    inner: Arc<Inner>,
}

struct Inner {
    access: Access,
    fleet: Fleet,
    store: Arc<dyn Store>,
    settings: Settings,
    lent: Lent,
    transfers: Transfers,
    owners: Arc<dyn OwnerChannel>,
    bridges: Mutex<HashMap<String, Arc<Bridge>>>,
    /// Which bridge session each node session belongs to, by node selector and node session id.
    routes: Mutex<HashMap<(String, SessionId), Route>>,
}

#[derive(Clone)]
struct Route {
    bridge: String,
    session: SessionId,
}

/// One client's link. Its id is short and random, and suffixes everything it lends a node.
struct Bridge {
    id: String,
    selector: String,
    owner: UserId,
    name: String,
    handle: NodeHandle,
    caps: Mutex<GatewayCapabilities>,
    sessions: Mutex<HashMap<SessionId, Seat>>,
    /// Calls held on a node until this bridge rules on them, and the node that holds each.
    verdicts: Mutex<HashMap<ToolCallId, GatewayLink>>,
    /// Questions and plans put to this bridge, and the node waiting on each answer.
    answers: Mutex<HashMap<PromptId, GatewayLink>>,
}

#[derive(Clone)]
struct Seat {
    selector: String,
    node_name: String,
    node_session: SessionId,
}

impl Bridge {
    fn seat(&self, session: &SessionId) -> Option<Seat> {
        lock(&self.sessions).get(session).cloned()
    }

    /// `bridge://<id>/<uri>`, so a node's read finds its way back here.
    fn wrap(&self, uri: &str) -> String {
        format!("{SCHEME}://{}/{uri}", self.id)
    }

    fn wrap_links(&self, blocks: Vec<Open<ContentBlock>>) -> Vec<Open<ContentBlock>> {
        blocks
            .into_iter()
            .map(|block| match block {
                Open::Known(ContentBlock::ResourceLink {
                    uri,
                    name,
                    mime_type,
                    title,
                    description,
                    size,
                }) => Open::Known(ContentBlock::ResourceLink {
                    uri: self.wrap(&uri),
                    name,
                    mime_type,
                    title,
                    description,
                    size,
                }),
                other => other,
            })
            .collect()
    }

    fn suffix(&self) -> String {
        format!("_{}", self.id)
    }
}

impl Relay {
    pub fn new(
        access: Access,
        fleet: Fleet,
        store: Arc<dyn Store>,
        settings: Settings,
        lent: Lent,
        transfers: Transfers,
        owners: Arc<dyn OwnerChannel>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                access,
                fleet,
                store,
                settings,
                lent,
                transfers,
                owners,
                bridges: Mutex::default(),
                routes: Mutex::default(),
            }),
        }
    }

    /// Serves one client's link until it ends for good, then closes its sessions on their nodes.
    pub async fn serve(&self, link: NewNodeLink) {
        let NewNodeLink {
            identity: GatewayIdentity(selector),
            handle,
            events,
        } = link;
        let snapshot = self.inner.access.snapshot();
        let Some(token) = snapshot.client(&selector).cloned() else {
            handle.close().await;
            return;
        };
        let bridge = Arc::new(Bridge {
            id: self.mint_id(),
            selector,
            owner: token.owner,
            name: token.name,
            handle,
            caps: Mutex::default(),
            sessions: Mutex::default(),
            verdicts: Mutex::default(),
            answers: Mutex::default(),
        });
        lock(&self.inner.bridges).insert(bridge.id.clone(), bridge.clone());
        tracing::info!(client = %bridge.name, owner = %bridge.owner, link = %bridge.id, "client attached over the RAX API");
        self.pump(&bridge, events).await;
        self.forget(&bridge).await;
        tracing::info!(client = %bridge.name, owner = %bridge.owner, link = %bridge.id, "client detached");
    }

    fn mint_id(&self) -> String {
        const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
        loop {
            let id: String = (0..LINK_ID_LEN)
                .map(|_| ALPHABET[rand::random_range(0..ALPHABET.len())] as char)
                .collect();
            if !lock(&self.inner.bridges).contains_key(&id) {
                return id;
            }
        }
    }

    async fn pump(&self, bridge: &Arc<Bridge>, mut events: NodeEvents) {
        while let Some(event) = events.recv().await {
            match event {
                NodeEvent::Request { id, call } => {
                    let (relay, bridge) = (self.clone(), bridge.clone());
                    tokio::spawn(async move { relay.request(&bridge, id, call).await });
                }
                NodeEvent::Verdict(verdict) => {
                    let link = lock(&bridge.verdicts).remove(&verdict.id);
                    match link {
                        Some(link) => {
                            if let Err(err) = link.verdict(verdict).await {
                                tracing::debug!(error = %err, "could not pass a verdict on to its node");
                            }
                        }
                        None => {
                            tracing::debug!(tool_call_id = %verdict.id, "verdict for a call no node is holding")
                        }
                    }
                }
                NodeEvent::Answer(answer) => {
                    let link = lock(&bridge.answers).remove(&answer.id);
                    match link {
                        Some(link) => {
                            if let Err(err) = link.answer(answer).await {
                                tracing::debug!(error = %err, "could not pass an answer on to its node");
                            }
                        }
                        None => {
                            tracing::debug!(prompt = %answer.id, "answer for a prompt no node is waiting on")
                        }
                    }
                }
                NodeEvent::Disconnected { reason } => {
                    tracing::info!(client = %bridge.name, %reason, "client disconnected; holding its link for a resume");
                }
                NodeEvent::Resumed => tracing::info!(client = %bridge.name, "client resumed"),
                NodeEvent::Unhandled { body, .. } => {
                    tracing::debug!(client = %bridge.name, subject = ?body.subject, "the client could not handle something");
                }
                NodeEvent::Fresh
                | NodeEvent::CredentialRejected
                | NodeEvent::Refused { .. }
                | NodeEvent::Rejected(_) => {}
            }
        }
    }

    /// The link is gone for good: every session it opened is closed on its node.
    async fn forget(&self, bridge: &Bridge) {
        lock(&self.inner.bridges).remove(&bridge.id);
        let seats: Vec<(SessionId, Seat)> = lock(&bridge.sessions).drain().collect();
        for (_, seat) in seats {
            self.release(&seat);
            if let Some(node) = self.inner.fleet.get(&seat.selector) {
                let close = GatewayCall::CloseSession(SessionRef {
                    session_id: seat.node_session.clone(),
                });
                if let Ok(pending) = node.link.call(close).await {
                    let _ = tokio::time::timeout(PROMPT_TIMEOUT, pending.reply).await;
                }
            }
        }
    }

    fn release(&self, seat: &Seat) {
        lock(&self.inner.routes).remove(&(seat.selector.clone(), seat.node_session.clone()));
        self.inner.fleet.session_ended(&seat.selector);
    }

    async fn request(&self, bridge: &Arc<Bridge>, id: RequestId, call: GatewayCall) {
        let handle = &bridge.handle;
        let answer = match call {
            GatewayCall::Initialize(offer) => {
                Ok(GatewayReply::Initialize(self.initialize(bridge, offer)))
            }
            GatewayCall::NewSession(request) => self
                .new_session(bridge, request)
                .await
                .map(GatewayReply::NewSession),
            GatewayCall::Prompt(prompt) => return self.prompt(bridge, id, prompt).await,
            GatewayCall::Cancel(session) => {
                self.forward(bridge, &session.session_id, |session_id| {
                    GatewayCall::Cancel(SessionRef { session_id })
                })
                .await;
                Ok(GatewayReply::Cancel)
            }
            GatewayCall::CloseSession(session) => {
                self.forward(bridge, &session.session_id, |session_id| {
                    GatewayCall::CloseSession(SessionRef { session_id })
                })
                .await;
                if let Some(seat) = lock(&bridge.sessions).remove(&session.session_id) {
                    self.release(&seat);
                }
                Ok(GatewayReply::CloseSession)
            }
            // The fleet's credentials belong to its nodes' owners, who repair them themselves.
            GatewayCall::RenewCredential => Ok(GatewayReply::RenewCredential(
                CredentialRenewal::NothingToRenew,
            )),
            // The relay declares no `local_files`, so it never put an identifier in a call.
            GatewayCall::ReadLocalFile(read) => Err(Error::new(
                ErrorKind::NotFound,
                format!("this gateway holds no file {}", read.id),
            )),
            GatewayCall::Close => {
                let _ = handle.reply(id, GatewayReply::Close).await;
                handle.close().await;
                return;
            }
        };
        let sent = match answer {
            Ok(reply) => handle.reply(id, reply).await,
            Err(error) => handle.fault(id, error).await,
        };
        if let Err(err) = sent {
            tracing::debug!(client = %bridge.name, error = %err, "could not answer the client");
        }
    }

    /// Answers like a node that runs every agent: the tools ride with each session, and every
    /// call is held for a verdict, as the nodes behind it hold them.
    fn initialize(&self, bridge: &Bridge, offer: Initialize) -> Initialized {
        *lock(&bridge.caps) = offer.capabilities;
        Initialized {
            protocol_version: PROTOCOL_VERSION,
            capabilities: NodeCapabilities {
                interruptible: Some(true),
                prompt: Default::default(),
                resource_schemes: Vec::new(),
                tool_gate: ToolGate::EveryCall,
                sessions: SessionDurability::Ephemeral,
                tool_groups: true,
                local_files: false,
                tool_resources: false,
                // Confinement is enforced here, on the nodes behind it, before a client is
                // routed to one.
                owner_only: false,
            },
            metadata: Default::default(),
        }
    }

    async fn new_session(
        &self,
        bridge: &Arc<Bridge>,
        request: NewSession,
    ) -> Result<SessionCreated, Error> {
        let snapshot = self.inner.access.snapshot();
        let node = self
            .inner
            .fleet
            .assign_where(&bridge.owner, &snapshot, |node| {
                node.capabilities.tool_groups
            })
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unsupported,
                    "no machine you may use is connected right now",
                )
            })?;
        let suffix = bridge.suffix();
        let mut unhandled = Vec::new();
        let mut groups = Vec::with_capacity(request.tool_groups.len());
        for group in request.tool_groups {
            let namespace = format!("{}{suffix}", group.namespace);
            if ToolGroup::is_valid_namespace(&namespace) {
                groups.push(ToolGroup { namespace, ..group });
            } else {
                unhandled.push(Unhandled {
                    subject: Subject::ToolGroup {
                        namespace: group.namespace,
                    },
                    reason: UnhandledReason::Other,
                    message: Some(format!(
                        "a lent namespace is suffixed with `{suffix}`, so it must be [a-z0-9_] and at most {} characters",
                        rax::tool::MAX_NAMESPACE_LEN - suffix.len()
                    )),
                });
            }
        }
        let opening = NewSession {
            context: bridge.wrap_links(request.context),
            tool_groups: groups.clone(),
        };
        let created = match call(&node.link, GatewayCall::NewSession(opening)).await {
            Ok(GatewayReply::NewSession(created)) => created,
            Ok(other) => {
                self.inner.fleet.session_ended(&node.selector);
                return Err(Error::new(
                    ErrorKind::Unknown,
                    format!("the node answered session.new with {other:?}"),
                ));
            }
            Err(err) => {
                self.inner.fleet.session_ended(&node.selector);
                return Err(fault_of(err));
            }
        };
        self.inner
            .lent
            .opened(&node.selector, &created.session_id, &groups);
        let session = SessionId(format!("{}-{:016x}", bridge.id, rand::random::<u64>()));
        let seat = Seat {
            selector: node.selector.clone(),
            node_name: node.name.clone(),
            node_session: created.session_id.clone(),
        };
        lock(&self.inner.routes).insert(
            (node.selector.clone(), created.session_id.clone()),
            Route {
                bridge: bridge.id.clone(),
                session: session.clone(),
            },
        );
        lock(&bridge.sessions).insert(session.clone(), seat);
        tracing::info!(client = %bridge.name, node = %node.name, %session, "opened a client session on a node");
        unhandled.extend(created.unhandled.into_iter().map(|mut unhandled| {
            if let Subject::ToolGroup { namespace } = &mut unhandled.subject
                && let Some(original) = namespace.strip_suffix(&suffix)
            {
                *namespace = original.to_owned();
            }
            unhandled
        }));
        Ok(SessionCreated {
            session_id: session,
            unhandled,
        })
    }

    /// Cancels and closes are forwarded when the session is still seated, and succeed either
    /// way, as a node answers them for sessions it does not have.
    async fn forward(
        &self,
        bridge: &Bridge,
        session: &SessionId,
        call_for: impl FnOnce(SessionId) -> GatewayCall,
    ) {
        let Some(seat) = bridge.seat(session) else {
            return;
        };
        let Some(node) = self.inner.fleet.get(&seat.selector) else {
            return;
        };
        if let Err(err) = call(&node.link, call_for(seat.node_session)).await {
            tracing::debug!(node = %node.name, error = %err, "the node did not take a forwarded call");
        }
    }

    async fn prompt(&self, bridge: &Arc<Bridge>, id: RequestId, prompt: Prompt) {
        let handle = &bridge.handle;
        let seated = bridge.seat(&prompt.session_id).and_then(|seat| {
            self.inner
                .fleet
                .get(&seat.selector)
                .map(|node| (seat, node))
        });
        let Some((seat, node)) = seated else {
            // The node went away with the session, as a dead node's always does: the client opens
            // a fresh one.
            if let Some(seat) = lock(&bridge.sessions).remove(&prompt.session_id) {
                self.release(&seat);
            }
            let gone = Error::new(
                ErrorKind::UnknownSession,
                format!("session {} is gone; open a new one", prompt.session_id),
            );
            let _ = handle.fault(id, gone).await;
            return;
        };
        let forwarded = GatewayCall::Prompt(Prompt {
            session_id: seat.node_session.clone(),
            content: bridge.wrap_links(prompt.content),
        });
        let pending = match node.link.call(forwarded).await {
            Ok(pending) => pending,
            Err(err) => {
                let _ = handle.fault(id, fault_of(err)).await;
                return;
            }
        };
        let accepted = tokio::time::timeout(PROMPT_TIMEOUT, pending.reply).await;
        let accepted = match accepted {
            Ok(Ok(GatewayReply::Prompt(accepted))) => accepted,
            Ok(Ok(other)) => {
                let wrong = Error::new(
                    ErrorKind::Unknown,
                    format!("the node answered prompt with {other:?}"),
                );
                let _ = handle.fault(id, wrong).await;
                return;
            }
            Ok(Err(err)) => {
                let error = fault_of(err);
                if error.kind == ErrorKind::UnknownSession
                    && let Some(seat) = lock(&bridge.sessions).remove(&prompt.session_id)
                {
                    self.release(&seat);
                }
                let _ = handle.fault(id, error).await;
                return;
            }
            Err(_) => {
                let late = Error::new(
                    ErrorKind::Unknown,
                    "the node did not take the prompt in time",
                );
                let _ = handle.fault(id, late).await;
                return;
            }
        };
        let accepted = PromptAccepted {
            unhandled: accepted.unhandled,
        };
        if handle
            .reply(id.clone(), GatewayReply::Prompt(accepted))
            .await
            .is_err()
        {
            return;
        }
        self.turn(bridge, &node, &id, pending.events).await;
        let _ = handle.end(id).await;
    }

    async fn turn(
        &self,
        bridge: &Arc<Bridge>,
        node: &Node,
        stream: &RequestId,
        mut events: StreamEvents,
    ) {
        // Dismisses any asking still waiting on the owner once the turn is over.
        let turn = CancellationToken::new();
        let _ended = turn.clone().drop_guard();
        while let Some(event) = events.recv().await {
            let Some(event) = self.relayed(bridge, node, &turn, event).await else {
                continue;
            };
            if let Err(err) = bridge.handle.event(stream.clone(), event).await {
                tracing::debug!(client = %bridge.name, error = %err, "could not relay a turn event");
                return;
            }
        }
    }

    /// What the client sees of one event from the node, or `None` when it sees nothing.
    async fn relayed(
        &self,
        bridge: &Arc<Bridge>,
        node: &Node,
        turn: &CancellationToken,
        event: Open<Event>,
    ) -> Option<Open<Event>> {
        let Open::Known(event) = event else {
            return Some(event);
        };
        let event = match event {
            Event::ToolCall { tool_call } => {
                return self
                    .rule(bridge, node, turn, tool_call)
                    .await
                    .map(Open::Known);
            }
            Event::Question { question } => {
                if !lock(&bridge.caps).question {
                    unavailable(node, &question.id).await;
                    return None;
                }
                lock(&bridge.answers).insert(question.id.clone(), node.link.clone());
                Event::Question { question }
            }
            Event::Plan { plan } => {
                if !lock(&bridge.caps).plan {
                    unavailable(node, &plan.id).await;
                    return None;
                }
                lock(&bridge.answers).insert(plan.id.clone(), node.link.clone());
                Event::Plan { plan }
            }
            // Sign-ins are the node owner's to finish, whoever's turn raised them.
            Event::SignIn { sign_in } => {
                let raised_by = NodeRef::from(node);
                if let Err(err) = self.inner.owners.ask_sign_in(raised_by, sign_in).await {
                    tracing::warn!(node = %node.name, error = %err, "could not raise a sign-in from a client's turn");
                }
                return None;
            }
            Event::SignInSettled { sign_in_settled } => {
                self.inner
                    .owners
                    .sign_in_settled(&node.selector, sign_in_settled)
                    .await;
                return None;
            }
            Event::Attachment { mut attachment } => {
                let bytes = self
                    .inner
                    .transfers
                    .claim(&node.selector, &attachment.transfer_id, ATTACHMENT_WAIT)
                    .await;
                let bytes = match bytes {
                    Ok(bytes) => bytes,
                    Err(reason) => {
                        tracing::warn!(node = %node.name, %reason, "an attachment for a client never arrived");
                        return None;
                    }
                };
                match bridge.handle.send_attachment(&bytes).await {
                    Ok(transfer_id) => attachment.transfer_id = transfer_id,
                    Err(err) => {
                        tracing::warn!(client = %bridge.name, error = %err, "could not pass an attachment on");
                        return None;
                    }
                }
                Event::Attachment { attachment }
            }
            other => other,
        };
        Some(Open::Known(event))
    }

    /// Rules on a call with the node owner's tool rules. When a person must decide, that person
    /// is the node's owner, because the tool runs on their machine: the client is asked only when
    /// it is the owner's own, and otherwise the owner is asked through the embedder while the client
    /// hears what it is waiting for. A call ruled here still shows, as an update the client draws but is
    /// not asked about.
    async fn rule(
        &self,
        bridge: &Arc<Bridge>,
        node: &Node,
        turn: &CancellationToken,
        tool_call: ToolCall,
    ) -> Option<Event> {
        let rules = policy::rules(&*self.inner.store, &node.owner).await;
        match policy::rule(&self.inner.settings, &rules, &tool_call.name) {
            Ruling::Ask if bridge.owner == node.owner => {
                lock(&bridge.verdicts).insert(tool_call.id.clone(), node.link.clone());
                Some(Event::ToolCall { tool_call })
            }
            Ruling::Ask => {
                let waiting = format!(
                    "Waiting for the owner of {} to approve the '{}' tool.",
                    node.name, tool_call.name
                );
                self.ask_owner(bridge, node, turn, tool_call).await;
                Some(Event::Status { text: waiting })
            }
            Ruling::Decided(decision) => {
                let status = match decision {
                    Decision::Allow => ToolCallStatus::InProgress,
                    Decision::Deny { .. } => ToolCallStatus::Denied,
                };
                let verdict = ToolVerdict {
                    id: tool_call.id.clone(),
                    decision,
                };
                if let Err(err) = node.link.verdict(verdict).await {
                    tracing::warn!(node = %node.name, error = %err, "could not rule on a client's tool call");
                }
                Some(Event::ToolCallUpdate {
                    tool_call_update: ToolCallUpdate {
                        id: tool_call.id,
                        status,
                        title: Some(tool_call.title.unwrap_or(tool_call.name)),
                        content: tool_call.content,
                        output: None,
                    },
                })
            }
        }
    }

    /// Puts a client's call to the node's owner. The verdict reaches the node from the owner,
    /// never from the client.
    async fn ask_owner(
        &self,
        bridge: &Bridge,
        node: &Node,
        turn: &CancellationToken,
        tool_call: ToolCall,
    ) {
        let ask = ToolApproval {
            node: NodeRef::from(node),
            requester: bridge.owner.clone(),
            client: bridge.name.clone(),
            tool: tool_call,
            turn: turn.clone(),
        };
        self.inner.owners.ask_tool_approval(ask).await;
    }

    fn route(&self, selector: &str, session: &SessionId) -> Option<(Arc<Bridge>, SessionId)> {
        let route = lock(&self.inner.routes)
            .get(&(selector.to_owned(), session.clone()))
            .cloned()?;
        let bridge = lock(&self.inner.bridges).get(&route.bridge).cloned()?;
        Some((bridge, route.session))
    }

    /// Whether a node's session belongs to a client rather than the embedder.
    pub fn owns(&self, selector: &str, session: &SessionId) -> bool {
        self.route(selector, session).is_some()
    }

    /// Something a client's session did with no turn open. A tool call is ruled on as in a turn.
    pub async fn background(&self, node: &Node, session: &SessionId, event: Open<BackgroundEvent>) {
        let Some((bridge, bridge_session)) = self.route(&node.selector, session) else {
            return;
        };
        let event = match event {
            Open::Known(BackgroundEvent::ToolCall { tool_call }) => {
                // No turn ends a background call's asking; the owner's channel times it out.
                let unbounded = CancellationToken::new();
                match self.rule(&bridge, node, &unbounded, tool_call).await {
                    Some(Event::ToolCall { tool_call }) => {
                        Open::Known(BackgroundEvent::ToolCall { tool_call })
                    }
                    Some(Event::ToolCallUpdate { tool_call_update }) => {
                        Open::Known(BackgroundEvent::ToolCallUpdate { tool_call_update })
                    }
                    Some(Event::Status { text }) => Open::Known(BackgroundEvent::Status { text }),
                    _ => return,
                }
            }
            other => other,
        };
        if let Err(err) = bridge.handle.background(bridge_session, event).await {
            tracing::debug!(client = %bridge.name, error = %err, "could not relay a background event");
        }
    }

    /// A node calls a tool its client session was lent. Any group but the ones that session was
    /// opened with is refused, whatever the node sends.
    pub async fn call_tool(&self, link: &GatewayLink, node: &str, id: RequestId, call: CallTool) {
        let selector = &link.identity().0;
        let forwarded = match self.route(selector, &call.session_id) {
            Some((bridge, session)) => {
                let lent = self.inner.lent.groups(selector, &call.session_id).await;
                let original = call
                    .namespace
                    .as_ref()
                    .filter(|namespace| lent.contains(namespace))
                    .and_then(|namespace| namespace.strip_suffix(&bridge.suffix()))
                    .map(str::to_owned);
                original.map(|namespace| {
                    let call = CallTool {
                        session_id: session,
                        namespace: Some(namespace),
                        name: call.name.clone(),
                        arguments: call.arguments.clone(),
                    };
                    (bridge, call)
                })
            }
            None => None,
        };
        let Some((bridge, forwarded)) = forwarded else {
            let refused = Error::new(
                ErrorKind::Forbidden,
                format!(
                    "this session was not lent a tool named {}.{}",
                    call.namespace.as_deref().unwrap_or_default(),
                    call.name
                ),
            );
            if let Err(err) = link.fault(id, refused).await {
                tracing::debug!(%node, error = %err, "could not refuse a tool call");
            }
            return;
        };
        let sent = match bridge.handle.call(NodeCall::CallTool(forwarded)).await {
            Ok(NodeReply::CallTool(outcome)) => {
                tracing::info!(%node, client = %bridge.name, tool = %call.name, failed = outcome.is_error, "relayed a tool call to a client");
                link.reply(id, NodeReply::CallTool(outcome)).await
            }
            Ok(other) => {
                let wrong = Error::new(
                    ErrorKind::Unknown,
                    format!("the client answered tool.call with {other:?}"),
                );
                link.fault(id, wrong).await
            }
            Err(err) => link.fault(id, fault_of(err)).await,
        };
        if let Err(err) = sent {
            tracing::debug!(%node, error = %err, "could not answer a relayed tool call");
        }
    }

    /// A node reads `bridge://<id>/<uri>`. Only a node holding a session from that client may,
    /// and the client is asked for the original URI.
    pub async fn read(&self, link: &GatewayLink, node: &str, id: RequestId, read: ReadResource) {
        let selector = &link.identity().0;
        let target = read
            .uri
            .strip_prefix(&format!("{SCHEME}://"))
            .and_then(|rest| rest.split_once('/'))
            .and_then(|(bridge, uri)| {
                let bridge = lock(&self.inner.bridges).get(bridge).cloned()?;
                let seated = lock(&bridge.sessions)
                    .values()
                    .any(|seat| &seat.selector == selector);
                seated.then(|| (bridge, uri.to_owned()))
            });
        let Some((bridge, uri)) = target else {
            let forbidden = Error::new(
                ErrorKind::Forbidden,
                format!("{} was never linked to this node", read.uri),
            );
            if let Err(err) = link.fault(id, forbidden).await {
                tracing::debug!(%node, error = %err, "could not refuse a read");
            }
            return;
        };
        let asked = ReadResource {
            uri,
            max_bytes: read.max_bytes,
        };
        match bridge.handle.read_resource(asked).await {
            Ok(resource) => {
                if let Err(err) = link
                    .serve_resource(id, &read, &resource.bytes, resource.mimetype)
                    .await
                {
                    tracing::debug!(%node, error = %err, "could not serve a client's resource");
                }
            }
            Err(err) => {
                if let Err(err) = link.fault(id, fault_of(err)).await {
                    tracing::debug!(%node, error = %err, "could not refuse a read");
                }
            }
        }
    }

    /// Closes the links of clients whose token or owner lost access.
    pub async fn close_clients(&self, selectors: &[String]) {
        let closing: Vec<Arc<Bridge>> = lock(&self.inner.bridges)
            .values()
            .filter(|bridge| selectors.contains(&bridge.selector))
            .cloned()
            .collect();
        for bridge in closing {
            tracing::info!(client = %bridge.name, owner = %bridge.owner, "access revoked; closing the client's link");
            bridge.handle.close().await;
        }
    }

    /// Every client session open right now.
    pub fn summaries(&self) -> Vec<Summary> {
        let bridges: Vec<Arc<Bridge>> = lock(&self.inner.bridges).values().cloned().collect();
        let mut summaries: Vec<Summary> = bridges
            .iter()
            .flat_map(|bridge| {
                lock(&bridge.sessions)
                    .values()
                    .map(|seat| Summary {
                        owner: bridge.owner.clone(),
                        client: bridge.name.clone(),
                        node: seat.node_name.clone(),
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        summaries
            .sort_by(|a, b| (&a.owner, &a.client, &a.node).cmp(&(&b.owner, &b.client, &b.node)));
        summaries
    }
}

async fn call(link: &GatewayLink, call: GatewayCall) -> Result<GatewayReply, CallError> {
    let pending = link.call(call).await?;
    match tokio::time::timeout(PROMPT_TIMEOUT, pending.reply).await {
        Ok(reply) => reply,
        Err(_) => Err(CallError::Closed),
    }
}

/// A fault passes through as the node or client gave it; a link that ended is the session's end.
fn fault_of(err: CallError) -> Error {
    match err {
        CallError::Fault(error) => error,
        CallError::LinkReset | CallError::Closed => Error::new(
            ErrorKind::UnknownSession,
            "the link to the other side ended; open a new session",
        ),
        other => Error::new(ErrorKind::Unknown, other.to_string()),
    }
}

/// A question or plan the client cannot show: nobody could be asked.
async fn unavailable(node: &Node, id: &PromptId) {
    let answer = DisplayAnswer {
        id: id.clone(),
        outcome: DisplayOutcome::Unavailable,
        answers: Default::default(),
        choice: None,
        user_id: None,
        note: None,
        code: None,
    };
    if let Err(err) = node.link.answer(answer).await {
        tracing::debug!(node = %node.name, error = %err, "could not answer a prompt nobody could see");
    }
}
