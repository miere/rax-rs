//! The node role: answers the gateway's calls and streams its turns. It rides on either transport
//! end — [`NodeLink::start`] dials, and an [`crate::accept::Acceptor`] serving `rax.v1.gateway`
//! hands one out per link.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures_util::Stream;
use rax::attachment::MAX_ATTACHMENT_BYTES;
use rax::event::BackgroundEvent;
use rax::id::{RequestId, SessionId, TransferId};
use rax::interaction::DisplayAnswer;
use rax::message::from_payload;
use rax::open::{Subject, UnhandledReason};
use rax::resource::{ReadResource, ResourceTransfer};
use rax::transport::{EndpointError, Role};
use rax::{
    Error, ErrorKind, Event, GatewayCall, GatewayMessage, GatewayReply, Link, NodeCall,
    NodeMessage, NodeReply, Open, Rejection, ToolVerdict, Unhandled,
};
use serde_json::Value;
use serde_json::value::RawValue;
use tokio::io::AsyncRead;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::accept::GatewayIdentity;
use crate::calls::Calls;
use crate::connection::{
    Core, Disconnect, Ended, Flow, Half, Lifecycle, Owner, Route, Terminal, lock,
};
use crate::dial::{DialConfig, supervise};
use crate::error::{CallError, SendError};
use crate::transfer::{self, Reassembly, Settled, TransferError};

/// A node's dialler is the usual case, so its configuration keeps the name it always had.
pub type NodeConfig = DialConfig;

/// On an accepted link the events end instead of reporting `Fresh`, a refusal or a replacement:
/// a fresh link from the same gateway arrives as a new [`NewNodeLink`].
#[derive(Debug)]
pub enum NodeEvent {
    /// The gateway holds no state for this node: fail open turns and expect `initialize` again.
    Fresh,
    Resumed,
    Disconnected {
        reason: Disconnect,
    },
    /// Terminal: redialling with the same token cannot succeed.
    CredentialRejected,
    /// Terminal: the gateway refused with a status that redialling will not change.
    Refused {
        status: u16,
    },
    /// Terminal: the gateway ended the link over metadata it cannot accept, and would again.
    Rejected(Rejection),
    Request {
        id: RequestId,
        call: GatewayCall,
    },
    Verdict(ToolVerdict),
    Answer(DisplayAnswer),
    Unhandled {
        stream: Option<RequestId>,
        body: Unhandled,
    },
}

pub struct NodeLink;

impl NodeLink {
    /// Dials as a node, which offers no subprotocol and so reaches any acceptor.
    pub fn start(config: NodeConfig) -> Result<(NodeHandle, NodeEvents), EndpointError> {
        let endpoints = config.resolve()?;
        let (shared, events) = Shared::new(
            config.unlinked(),
            config.events_capacity,
            Owner::dialler(),
            true,
        );
        let shutdown = shared.shutdown.clone();
        let supervisor = tokio::spawn(supervise(
            shared.clone(),
            Role::Node,
            config,
            endpoints,
            shutdown,
        ));
        shared.owner.supervised_by(supervisor);
        Ok((NodeHandle { shared }, events))
    }
}

/// A gateway that dialled this acceptor. This side plays the node toward it.
pub struct NewNodeLink {
    pub identity: GatewayIdentity,
    pub handle: NodeHandle,
    pub events: NodeEvents,
}

pub struct NodeEvents {
    receiver: mpsc::Receiver<NodeEvent>,
}

impl NodeEvents {
    pub async fn recv(&mut self) -> Option<NodeEvent> {
        self.receiver.recv().await
    }
}

impl Stream for NodeEvents {
    type Item = NodeEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<NodeEvent>> {
        self.receiver.poll_recv(cx)
    }
}

/// The bytes a gateway sent in answer to `resource.read`, checked against the size it declared.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resource {
    pub transfer_id: TransferId,
    pub mimetype: Option<String>,
    pub bytes: Vec<u8>,
}

enum Answered {
    Reply(NodeReply),
    Resource(Resource),
}

pub(crate) struct Shared {
    core: Arc<Core>,
    calls: Calls<Answered>,
    reading: Mutex<HashMap<TransferId, (RequestId, ResourceTransfer)>>,
    transfers: Mutex<Reassembly>,
    events: Mutex<Option<mpsc::Sender<NodeEvent>>>,
    shutdown: CancellationToken,
    owner: Owner,
    /// Only a dialler lives on across fresh links, so only it reports them.
    dialled: bool,
}

impl Shared {
    pub(crate) fn new(
        link: Link,
        capacity: usize,
        owner: Owner,
        dialled: bool,
    ) -> (Arc<Self>, NodeEvents) {
        let (events, receiver) = mpsc::channel(capacity.max(1));
        let shared = Arc::new(Self {
            core: Core::new(link),
            calls: Calls::new("n"),
            reading: Mutex::new(HashMap::new()),
            transfers: Mutex::new(Reassembly::default()),
            events: Mutex::new(Some(events)),
            shutdown: CancellationToken::new(),
            owner,
            dialled,
        });
        (shared, NodeEvents { receiver })
    }

    pub(crate) fn handle(self: &Arc<Self>) -> NodeHandle {
        NodeHandle {
            shared: self.clone(),
        }
    }

    async fn emit(&self, event: NodeEvent) -> bool {
        let Some(events) = lock(&self.events).clone() else {
            return false;
        };
        tokio::select! {
            sent = events.send(event) => sent.is_ok(),
            _ = self.shutdown.cancelled() => false,
        }
    }
}

impl Half for Shared {
    type Router = Router;

    fn core(&self) -> &Arc<Core> {
        &self.core
    }

    fn router(self: &Arc<Self>) -> Router {
        Router {
            shared: self.clone(),
        }
    }

    fn fail_all(&self, ended: Ended) {
        lock(&self.reading).clear();
        *lock(&self.transfers) = Reassembly::default();
        self.calls.fail_all(ended);
    }

    async fn notify(&self, event: Lifecycle) -> bool {
        let event = match event {
            Lifecycle::Fresh if !self.dialled => return true,
            Lifecycle::Fresh => NodeEvent::Fresh,
            Lifecycle::Resumed => NodeEvent::Resumed,
            Lifecycle::Disconnected(reason) => NodeEvent::Disconnected { reason },
        };
        self.emit(event).await
    }

    fn finish(&self, terminal: Terminal) {
        let ended = terminal.ended();
        if !self.core.end(ended) {
            return;
        }
        self.fail_all(ended);
        self.shutdown.cancel();
        let events = lock(&self.events).take();
        let last = match terminal {
            Terminal::CredentialRejected => NodeEvent::CredentialRejected,
            Terminal::Refused(status) => NodeEvent::Refused { status },
            Terminal::Closed | Terminal::Replaced => return,
        };
        if let Some(events) = events {
            tokio::spawn(async move {
                let _ = events.send(last).await;
            });
        }
    }
}

/// Sends wait for window room instead of failing, and are cancelled by dropping the future.
#[derive(Clone)]
pub struct NodeHandle {
    shared: Arc<Shared>,
}

impl NodeHandle {
    pub async fn reply(&self, id: RequestId, reply: GatewayReply) -> Result<(), SendError> {
        self.send(&NodeMessage::Reply { id, reply }).await
    }

    pub async fn fault(&self, id: RequestId, error: Error) -> Result<(), SendError> {
        self.send(&NodeMessage::Fault { id, error }).await
    }

    pub async fn event(
        &self,
        stream: RequestId,
        event: impl Into<Open<Event>>,
    ) -> Result<(), SendError> {
        let body = event.into();
        self.send(&NodeMessage::Event { stream, body }).await
    }

    pub async fn end(&self, stream: RequestId) -> Result<(), SendError> {
        self.send(&NodeMessage::End { stream }).await
    }

    pub async fn background(
        &self,
        session_id: SessionId,
        event: impl Into<Open<BackgroundEvent>>,
    ) -> Result<(), SendError> {
        let body = event.into();
        self.send(&NodeMessage::Background { session_id, body })
            .await
    }

    pub async fn unhandled(
        &self,
        stream: Option<RequestId>,
        body: Unhandled,
    ) -> Result<(), SendError> {
        self.send(&NodeMessage::Unhandled { stream, body }).await
    }

    pub async fn call(&self, call: impl Into<Open<NodeCall>>) -> Result<NodeReply, CallError> {
        Ok(match self.answered(call.into()).await? {
            Answered::Reply(reply) => reply,
            Answered::Resource(Resource {
                transfer_id,
                mimetype,
                bytes,
            }) => NodeReply::ReadResource(ResourceTransfer {
                transfer_id,
                size: bytes.len() as u64,
                mimetype,
            }),
        })
    }

    /// Returns once every byte has arrived, or fails if the transfer does.
    pub async fn read_resource(&self, read: ReadResource) -> Result<Resource, CallError> {
        match self.answered(NodeCall::ReadResource(read).into()).await? {
            Answered::Resource(resource) => Ok(resource),
            Answered::Reply(reply) => Err(CallError::Transfer(format!(
                "the gateway answered `resource.read` with {reply:?}"
            ))),
        }
    }

    async fn answered(&self, call: Open<NodeCall>) -> Result<Answered, CallError> {
        let shared = &self.shared;
        let generation = shared.core.generation();
        let (id, reply) = shared.calls.register();
        let forget = Forget {
            calls: &shared.calls,
            id: &id,
        };
        let request = NodeMessage::Request {
            id: id.clone(),
            call,
        };
        shared.core.send_in(generation, &request).await?;
        let result = reply.await.unwrap_or(Err(CallError::Closed));
        drop(forget);
        result
    }

    /// The caller announces the transfer with an `attachment` event once this returns.
    pub async fn send_attachment(&self, bytes: &[u8]) -> Result<TransferId, TransferError> {
        self.send_attachment_from(bytes, bytes.len() as u64).await
    }

    pub async fn send_attachment_from<R: AsyncRead + Unpin>(
        &self,
        reader: R,
        size: u64,
    ) -> Result<TransferId, TransferError> {
        let core = &self.shared.core;
        let generation = core.generation();
        let transfer_id = transfer::new_transfer_id();
        transfer::send(core, generation, transfer_id, reader, size, |body| {
            NodeMessage::Chunk { body }
        })
        .await
    }

    /// Tells a call that failed with the link apart from one the gateway refused.
    pub fn is_closed(&self) -> bool {
        self.shared.core.is_ended()
    }

    pub async fn close(&self) {
        self.shared.owner.close(&*self.shared).await;
    }

    async fn send(&self, message: &NodeMessage) -> Result<(), SendError> {
        self.shared.core.send(message).await
    }
}

struct Forget<'a> {
    calls: &'a Calls<Answered>,
    id: &'a RequestId,
}

impl Drop for Forget<'_> {
    fn drop(&mut self) {
        self.calls.forget(self.id);
    }
}

pub(crate) struct Router {
    shared: Arc<Shared>,
}

impl Router {
    async fn emit(&self, event: NodeEvent) -> Flow {
        match self.shared.emit(event).await {
            true => Flow::Continue,
            false => Flow::Stop,
        }
    }

    fn answer(&self, message: NodeMessage) {
        let core = self.shared.core.clone();
        let generation = core.generation();
        tokio::spawn(async move {
            if let Err(err) = core.send_in(generation, &message).await {
                tracing::debug!(error = %err, "rax automatic answer not sent");
            }
        });
    }

    fn expect_transfer(&self, id: RequestId, transfer: ResourceTransfer) {
        if transfer.size > MAX_ATTACHMENT_BYTES {
            let reason = format!(
                "the gateway declared {} bytes, over the {MAX_ATTACHMENT_BYTES}-byte ceiling",
                transfer.size
            );
            self.shared
                .calls
                .settle(&id, Err(CallError::Transfer(reason)));
            return;
        }
        lock(&self.shared.reading).insert(transfer.transfer_id.clone(), (id, transfer));
    }

    fn settle_transfer(&self, settled: Settled) {
        let (transfer_id, result) = match settled {
            Settled::Complete { transfer_id, bytes } => (transfer_id, Ok(bytes)),
            Settled::Failed {
                transfer_id,
                reason,
            } => (transfer_id, Err(reason)),
        };
        let Some((id, transfer)) = lock(&self.shared.reading).remove(&transfer_id) else {
            tracing::debug!(%transfer_id, "rax transfer nobody is reading");
            return;
        };
        let result = result.and_then(|bytes| {
            if bytes.len() as u64 == transfer.size {
                Ok(Answered::Resource(Resource {
                    transfer_id,
                    mimetype: transfer.mimetype,
                    bytes,
                }))
            } else {
                Err(format!(
                    "the gateway declared {} bytes and sent {}",
                    transfer.size,
                    bytes.len()
                ))
            }
        });
        self.shared
            .calls
            .settle(&id, result.map_err(CallError::Transfer));
    }

    fn malformed(&self, payload: &RawValue, err: impl std::fmt::Display) {
        tracing::warn!(error = %err, "rax dropped a malformed gateway message");
        let Ok(raw) = serde_json::from_str::<Value>(payload.get()) else {
            return;
        };
        if raw.get("k").and_then(Value::as_str) != Some("req") {
            return;
        }
        if let Some(id) = raw.get("id").and_then(Value::as_str) {
            self.answer(NodeMessage::Fault {
                id: id.into(),
                error: Error::new(ErrorKind::Unknown, format!("malformed request: {err}")),
            });
        }
    }
}

impl Route for Router {
    async fn route(&mut self, payload: Box<RawValue>) -> Flow {
        let message = match from_payload::<Open<GatewayMessage>>(&payload) {
            Ok(Open::Known(message)) => message,
            Ok(Open::Unknown { name, .. }) => {
                self.answer(NodeMessage::Unhandled {
                    stream: None,
                    body: Unhandled {
                        subject: Subject::MessageKind { name },
                        reason: UnhandledReason::UnsupportedType,
                        message: None,
                    },
                });
                return Flow::Continue;
            }
            Err(err) => {
                self.malformed(&payload, err);
                return Flow::Continue;
            }
        };
        let calls = &self.shared.calls;
        match message {
            GatewayMessage::Request {
                id,
                call: Open::Known(call),
            } => self.emit(NodeEvent::Request { id, call }).await,
            GatewayMessage::Request {
                id,
                call: Open::Unknown { name, .. },
            } => {
                self.answer(NodeMessage::Fault {
                    id,
                    error: Error::new(
                        ErrorKind::Unsupported,
                        format!("this node does not serve `{name}`"),
                    ),
                });
                Flow::Continue
            }
            GatewayMessage::Reply {
                id,
                reply: NodeReply::ReadResource(transfer),
            } => {
                self.expect_transfer(id, transfer);
                Flow::Continue
            }
            GatewayMessage::Reply { id, reply } => {
                if !calls.settle(&id, Ok(Answered::Reply(reply))) {
                    tracing::debug!(%id, "rax reply for a call nobody is waiting on");
                }
                Flow::Continue
            }
            GatewayMessage::Fault { id, error } => {
                if !calls.settle(&id, Err(CallError::Fault(error))) {
                    tracing::debug!(%id, "rax fault for a call nobody is waiting on");
                }
                Flow::Continue
            }
            GatewayMessage::Chunk { body } => {
                let settled = lock(&self.shared.transfers).accept(body);
                if let Some(settled) = settled {
                    self.settle_transfer(settled);
                }
                Flow::Continue
            }
            GatewayMessage::Verdict { body } => self.emit(NodeEvent::Verdict(body)).await,
            GatewayMessage::Answer { body } => self.emit(NodeEvent::Answer(body)).await,
            GatewayMessage::Unhandled { stream, body } => {
                self.emit(NodeEvent::Unhandled { stream, body }).await
            }
            GatewayMessage::Rejected { body } => {
                tracing::error!(key = ?body.key, reason = %body.message, "rax gateway rejected this node; not redialling");
                self.emit(NodeEvent::Rejected(body)).await;
                self.shared.finish(Terminal::Closed);
                Flow::Stop
            }
        }
    }
}
