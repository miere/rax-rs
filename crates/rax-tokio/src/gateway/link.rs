use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};

use futures_util::Stream;
use rax::event::BackgroundEvent;
use rax::id::{RequestId, SessionId, TransferId};
use rax::interaction::DisplayAnswer;
use rax::message::from_payload;
use rax::open::{Subject, UnhandledReason};
use rax::{
    Error, ErrorKind, Event, GatewayCall, GatewayMessage, GatewayReply, Link, NodeCall,
    NodeMessage, NodeReply, Open, ToolVerdict, Unhandled,
};
use serde_json::value::RawValue;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::{Hub, NodeIdentity};
use crate::calls::Calls;
use crate::connection::{Core, Disconnect, Ended, Flow, Route, lock};
use crate::error::{CallError, SendError};
use crate::transfer::{Reassembly, Settled};

#[derive(Debug)]
pub enum LinkEvent {
    Resumed,
    Disconnected {
        reason: Disconnect,
    },
    /// Terminal: the node started a fresh link, which arrives as a new `NewLink`.
    Replaced,
    /// Terminal: closed here, or the node stayed away longer than `retain_for`.
    Closed,
    Request {
        id: RequestId,
        call: NodeCall,
    },
    Background {
        session_id: SessionId,
        event: Open<BackgroundEvent>,
    },
    Attachment {
        transfer_id: TransferId,
        bytes: Vec<u8>,
    },
    TransferFailed {
        transfer_id: TransferId,
        reason: String,
    },
    Unhandled {
        stream: Option<RequestId>,
        body: Unhandled,
    },
}

pub struct NewLink {
    pub link: GatewayLink,
    pub events: LinkEvents,
}

pub struct LinkEvents {
    receiver: mpsc::Receiver<LinkEvent>,
}

impl LinkEvents {
    pub async fn recv(&mut self) -> Option<LinkEvent> {
        self.receiver.recv().await
    }
}

impl Stream for LinkEvents {
    type Item = LinkEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<LinkEvent>> {
        self.receiver.poll_recv(cx)
    }
}

pub struct PendingCall {
    pub id: RequestId,
    pub reply: PendingReply,
    pub events: StreamEvents,
}

pub struct PendingReply {
    receiver: oneshot::Receiver<Result<GatewayReply, CallError>>,
}

impl Future for PendingReply {
    type Output = Result<GatewayReply, CallError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.receiver)
            .poll(cx)
            .map(|settled| settled.unwrap_or(Err(CallError::Closed)))
    }
}

/// Ends when the node sends `end`, or at the reply for any call that is not a `prompt`.
pub struct StreamEvents {
    receiver: mpsc::Receiver<Open<Event>>,
}

impl StreamEvents {
    pub async fn recv(&mut self) -> Option<Open<Event>> {
        self.receiver.recv().await
    }
}

impl Stream for StreamEvents {
    type Item = Open<Event>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Open<Event>>> {
        self.receiver.poll_recv(cx)
    }
}

pub(crate) struct Shared {
    pub(crate) identity: NodeIdentity,
    pub(crate) core: Arc<Core>,
    calls: Calls<GatewayReply>,
    streams: Mutex<HashMap<RequestId, mpsc::Sender<Open<Event>>>>,
    events: Mutex<Option<mpsc::Sender<LinkEvent>>>,
    transfers: Mutex<Reassembly>,
    pub(crate) shutdown: CancellationToken,
    capacity: usize,
    hub: Weak<Hub>,
}

impl Shared {
    pub(crate) fn new(
        identity: NodeIdentity,
        link: Link,
        capacity: usize,
        hub: Weak<Hub>,
    ) -> (Arc<Self>, LinkEvents) {
        let (events, receiver) = mpsc::channel(capacity);
        let shared = Arc::new(Self {
            identity,
            core: Core::new(link),
            calls: Calls::new("g"),
            streams: Mutex::new(HashMap::new()),
            events: Mutex::new(Some(events)),
            transfers: Mutex::new(Reassembly::default()),
            shutdown: CancellationToken::new(),
            capacity,
            hub,
        });
        (shared, LinkEvents { receiver })
    }

    pub(crate) async fn emit(&self, event: LinkEvent) -> Flow {
        let Some(events) = lock(&self.events).clone() else {
            return Flow::Stop;
        };
        tokio::select! {
            _ = events.send(event) => Flow::Continue,
            _ = self.shutdown.cancelled() => Flow::Stop,
        }
    }

    pub(crate) fn finish(&self, terminal: LinkEvent, ended: Ended) {
        if !self.core.end(ended) {
            return;
        }
        self.calls.fail_all(ended);
        lock(&self.streams).clear();
        self.shutdown.cancel();
        if let Some(events) = lock(&self.events).take() {
            tokio::spawn(async move {
                let _ = events.send(terminal).await;
            });
        }
    }

    fn answer(&self, message: GatewayMessage) {
        let core = self.core.clone();
        tokio::spawn(async move {
            if let Err(err) = core.send(&message).await {
                tracing::debug!(error = %err, "rax automatic answer not sent");
            }
        });
    }
}

#[derive(Clone)]
pub struct GatewayLink {
    pub(crate) shared: Arc<Shared>,
}

impl GatewayLink {
    pub fn identity(&self) -> &NodeIdentity {
        &self.shared.identity
    }

    pub async fn call(&self, call: impl Into<Open<GatewayCall>>) -> Result<PendingCall, CallError> {
        let shared = &self.shared;
        let (id, receiver) = shared.calls.register();
        let (stream, events) = mpsc::channel(shared.capacity);
        lock(&shared.streams).insert(id.clone(), stream);
        let request = GatewayMessage::Request {
            id: id.clone(),
            call: call.into(),
        };
        if let Err(err) = shared.core.send(&request).await {
            shared.calls.forget(&id);
            lock(&shared.streams).remove(&id);
            return Err(err.into());
        }
        Ok(PendingCall {
            id,
            reply: PendingReply { receiver },
            events: StreamEvents { receiver: events },
        })
    }

    pub async fn verdict(&self, body: ToolVerdict) -> Result<(), SendError> {
        self.send(&GatewayMessage::Verdict { body }).await
    }

    pub async fn answer(&self, body: DisplayAnswer) -> Result<(), SendError> {
        self.send(&GatewayMessage::Answer { body }).await
    }

    pub async fn reply(&self, id: RequestId, reply: NodeReply) -> Result<(), SendError> {
        self.send(&GatewayMessage::Reply { id, reply }).await
    }

    pub async fn fault(&self, id: RequestId, error: Error) -> Result<(), SendError> {
        self.send(&GatewayMessage::Fault { id, error }).await
    }

    pub async fn unhandled(
        &self,
        stream: Option<RequestId>,
        body: Unhandled,
    ) -> Result<(), SendError> {
        self.send(&GatewayMessage::Unhandled { stream, body }).await
    }

    /// Relays a message this build may not know, as the protocol requires of an unknown kind.
    pub async fn relay(&self, message: Open<GatewayMessage>) -> Result<(), SendError> {
        self.shared.core.send(&message).await
    }

    /// Tells a stream that ended with the link apart from one the node closed with `end`.
    pub fn is_closed(&self) -> bool {
        self.shared.core.is_ended()
    }

    pub async fn close(&self) {
        if let Some(hub) = self.shared.hub.upgrade() {
            hub.forget(&self.shared);
        }
        self.shared.finish(LinkEvent::Closed, Ended::Closed);
    }

    async fn send(&self, message: &GatewayMessage) -> Result<(), SendError> {
        self.shared.core.send(message).await
    }
}

pub(crate) struct Router {
    pub(crate) shared: Arc<Shared>,
}

impl Router {
    async fn stream_event(&self, stream: RequestId, body: Open<Event>) -> Flow {
        let Some(events) = lock(&self.shared.streams).get(&stream).cloned() else {
            tracing::debug!(%stream, "rax event for a stream nobody is waiting on");
            return Flow::Continue;
        };
        tokio::select! {
            _ = events.send(body) => Flow::Continue,
            _ = self.shared.shutdown.cancelled() => Flow::Stop,
        }
    }

    fn settle(&self, id: RequestId, result: Result<GatewayReply, CallError>) {
        let keeps_streaming = matches!(result, Ok(GatewayReply::Prompt(_)));
        if !keeps_streaming {
            lock(&self.shared.streams).remove(&id);
        }
        if !self.shared.calls.settle(&id, result) {
            tracing::debug!(%id, "rax reply for a call nobody is waiting on");
        }
    }
}

impl Route for Router {
    async fn route(&mut self, payload: Box<RawValue>) -> Flow {
        let shared = &self.shared;
        let message = match from_payload::<Open<NodeMessage>>(&payload) {
            Ok(Open::Known(message)) => message,
            Ok(Open::Unknown { name, .. }) => {
                shared.answer(GatewayMessage::Unhandled {
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
                tracing::warn!(error = %err, "rax dropped a malformed node message");
                return Flow::Continue;
            }
        };
        match message {
            NodeMessage::Request {
                id,
                call: Open::Known(call),
            } => shared.emit(LinkEvent::Request { id, call }).await,
            NodeMessage::Request {
                id,
                call: Open::Unknown { name, .. },
            } => {
                shared.answer(GatewayMessage::Fault {
                    id,
                    error: Error::new(
                        ErrorKind::Unsupported,
                        format!("this gateway does not serve `{name}`"),
                    ),
                });
                Flow::Continue
            }
            NodeMessage::Reply { id, reply } => {
                self.settle(id, Ok(reply));
                Flow::Continue
            }
            NodeMessage::Fault { id, error } => {
                self.settle(id, Err(CallError::Fault(error)));
                Flow::Continue
            }
            NodeMessage::Event { stream, body } => self.stream_event(stream, body).await,
            NodeMessage::End { stream } => {
                lock(&shared.streams).remove(&stream);
                Flow::Continue
            }
            NodeMessage::Background { session_id, body } => {
                let event = LinkEvent::Background {
                    session_id,
                    event: body,
                };
                shared.emit(event).await
            }
            NodeMessage::Chunk { body } => {
                let settled = lock(&shared.transfers).accept(body);
                match settled {
                    None => Flow::Continue,
                    Some(Settled::Complete { transfer_id, bytes }) => {
                        shared
                            .emit(LinkEvent::Attachment { transfer_id, bytes })
                            .await
                    }
                    Some(Settled::Failed {
                        transfer_id,
                        reason,
                    }) => {
                        shared
                            .emit(LinkEvent::TransferFailed {
                                transfer_id,
                                reason,
                            })
                            .await
                    }
                }
            }
            NodeMessage::Unhandled { stream, body } => {
                shared.emit(LinkEvent::Unhandled { stream, body }).await
            }
        }
    }
}
