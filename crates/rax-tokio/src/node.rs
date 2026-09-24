//! The dialling end: one supervisor task per node keeps a single `Link` alive across sockets.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::{SinkExt, Stream, StreamExt};
use rax::attachment::MAX_ATTACHMENT_BYTES;
use rax::event::BackgroundEvent;
use rax::frame::ResumeOutcome;
use rax::id::{RequestId, SessionId, TransferId};
use rax::interaction::DisplayAnswer;
use rax::link::NO_EPOCH;
use rax::message::from_payload;
use rax::open::{Subject, UnhandledReason};
use rax::resource::{ReadResource, ResourceTransfer};
use rax::transport::{
    DEFAULT_WINDOW_BYTES, DEFAULT_WRITE_TIMEOUT, EndpointError, HANDSHAKE_TIMEOUT, Refusal,
    resolve_endpoint,
};
use rax::{
    Error, ErrorKind, Event, Frame, GatewayCall, GatewayMessage, GatewayReply, Link, LinkConfig,
    NodeCall, NodeMessage, NodeReply, Open, Rejection, ToolVerdict, Unhandled,
};
use serde_json::Value;
use serde_json::value::RawValue;
use tokio::io::AsyncRead;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_tls_with_config};
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::calls::Calls;
use crate::connection::{
    AttachError, Core, Disconnect, Ended, Flow, Queue, Route, Timing, drive, lock, socket_config,
};
use crate::error::{CallError, SendError};
use crate::transfer::{self, Reassembly, Settled, TransferError};

#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Addresses of one gateway cluster, tried in turn: a node stays on the one that last
    /// answered, and moves to the next when it is unreachable or refuses with a 5xx.
    pub endpoints: Vec<String>,
    pub token: String,
    pub window_bytes: usize,
    pub ack_threshold: u64,
    /// Also the liveness probe: a socket silent for three intervals is torn down and redialled.
    pub keepalive: Duration,
    pub backoff_min: Duration,
    pub backoff_max: Duration,
    pub handshake_timeout: Duration,
    pub write_timeout: Duration,
    /// Bounds undelivered events; a full channel stalls the reader, which is the back-pressure the
    /// protocol relies on.
    pub events_capacity: usize,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            endpoints: Vec::new(),
            token: String::new(),
            window_bytes: DEFAULT_WINDOW_BYTES,
            ack_threshold: LinkConfig::default().ack_threshold,
            keepalive: Duration::from_secs(20),
            backoff_min: Duration::from_millis(500),
            backoff_max: Duration::from_secs(30),
            handshake_timeout: HANDSHAKE_TIMEOUT,
            write_timeout: DEFAULT_WRITE_TIMEOUT,
            events_capacity: 64,
        }
    }
}

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
    pub fn start(config: NodeConfig) -> Result<(NodeHandle, NodeEvents), EndpointError> {
        if config.endpoints.is_empty() {
            return Err(EndpointError::Missing);
        }
        let endpoints = config
            .endpoints
            .iter()
            .map(|endpoint| resolve_endpoint(endpoint))
            .collect::<Result<Vec<_>, _>>()?;
        let (events, receiver) = mpsc::channel(config.events_capacity.max(1));
        let shared = Arc::new(Shared {
            core: Core::new(unlinked(&config)),
            calls: Calls::new("n"),
            reading: Mutex::new(HashMap::new()),
            transfers: Mutex::new(Reassembly::default()),
            shutdown: CancellationToken::new(),
            supervisor: Mutex::new(None),
            config,
        });
        let supervisor = tokio::spawn(supervise(shared.clone(), endpoints, events));
        *lock(&shared.supervisor) = Some(supervisor);
        Ok((NodeHandle { shared }, NodeEvents { receiver }))
    }
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

struct Shared {
    core: Arc<Core>,
    calls: Calls<Answered>,
    reading: Mutex<HashMap<TransferId, (RequestId, ResourceTransfer)>>,
    transfers: Mutex<Reassembly>,
    shutdown: CancellationToken,
    supervisor: Mutex<Option<JoinHandle<()>>>,
    config: NodeConfig,
}

impl Shared {
    fn fail_all(&self, ended: Ended) {
        lock(&self.reading).clear();
        *lock(&self.transfers) = Reassembly::default();
        self.calls.fail_all(ended);
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

    pub async fn close(&self) {
        let shared = &self.shared;
        shared.core.end(Ended::Closed);
        shared.fail_all(Ended::Closed);
        shared.shutdown.cancel();
        let supervisor = lock(&shared.supervisor).take();
        if let Some(supervisor) = supervisor {
            let _ = supervisor.await;
        }
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

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, thiserror::Error)]
enum DialError {
    #[error("gateway refused with HTTP {0}")]
    Refused(u16),
    #[error("{0}")]
    Io(String),
    #[error("handshake: {0}")]
    Handshake(String),
    #[error("handshake timed out")]
    Timeout,
    #[error("the link was closed")]
    Closed,
}

async fn supervise(shared: Arc<Shared>, endpoints: Vec<Url>, events: mpsc::Sender<NodeEvent>) {
    let config = &shared.config;
    let (mut current, mut failed_in_round, mut failures) = (0usize, 0usize, 0u32);
    loop {
        let endpoint = &endpoints[current];
        let dialled = tokio::select! {
            dialled = tokio::time::timeout(config.handshake_timeout, dial(&shared, endpoint)) => {
                dialled.unwrap_or(Err(DialError::Timeout))
            }
            _ = shared.shutdown.cancelled() => break,
        };
        match dialled {
            Ok((socket, queue, arrival)) => {
                (failed_in_round, failures) = (0, 0);
                if !emit(&shared, &events, arrival).await {
                    break;
                }
                let mut router = Router {
                    shared: shared.clone(),
                    events: events.clone(),
                };
                let timing = Timing {
                    keepalive: config.keepalive,
                    write_timeout: config.write_timeout,
                };
                let reason = drive(shared.core.clone(), socket, queue, timing, &mut router).await;
                if shared.core.is_ended() {
                    break;
                }
                tracing::info!(%reason, "rax link disconnected");
                if !emit(&shared, &events, NodeEvent::Disconnected { reason }).await {
                    break;
                }
            }
            Err(DialError::Closed) => break,
            Err(DialError::Refused(status)) if !Refusal::from_status(status).should_redial() => {
                let event = match Refusal::from_status(status) {
                    Refusal::CredentialRejected => NodeEvent::CredentialRejected,
                    _ => NodeEvent::Refused { status },
                };
                tracing::error!(status, "rax gateway refused this node; not redialling");
                shared.core.end(Ended::Closed);
                shared.fail_all(Ended::Closed);
                emit(&shared, &events, event).await;
                break;
            }
            Err(err) => {
                tracing::warn!(error = %err, %endpoint, "rax dial failed");
                current = (current + 1) % endpoints.len();
                failed_in_round += 1;
                if failed_in_round < endpoints.len() {
                    continue;
                }
                failed_in_round = 0;
                failures = failures.saturating_add(1);
            }
        }
        let delay = backoff(config.backoff_min, config.backoff_max, failures);
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = shared.shutdown.cancelled() => break,
        }
    }
    shared.core.end(Ended::Closed);
    shared.fail_all(Ended::Closed);
}

async fn emit(shared: &Shared, events: &mpsc::Sender<NodeEvent>, event: NodeEvent) -> bool {
    tokio::select! {
        sent = events.send(event) => sent.is_ok(),
        _ = shared.shutdown.cancelled() => false,
    }
}

fn backoff(min: Duration, max: Duration, failures: u32) -> Duration {
    let base = min
        .saturating_mul(2u32.saturating_pow(failures.min(16)))
        .min(max)
        .max(min);
    let low = base / 2;
    let spread = u64::try_from((base - low).as_millis()).unwrap_or(u64::MAX);
    low + Duration::from_millis(rand::random_range(0..=spread))
}

fn unlinked(config: &NodeConfig) -> Link {
    link_with_epoch(config, NO_EPOCH)
}

fn link_with_epoch(config: &NodeConfig, epoch: u64) -> Link {
    Link::new(LinkConfig {
        window_bytes: config.window_bytes,
        ack_threshold: config.ack_threshold,
        epoch,
        resume_from: 0,
    })
}

async fn dial(shared: &Shared, endpoint: &Url) -> Result<(Socket, Queue, NodeEvent), DialError> {
    let mut request = endpoint
        .as_str()
        .into_client_request()
        .map_err(|err| DialError::Io(err.to_string()))?;
    let bearer = HeaderValue::from_str(&format!("Bearer {}", shared.config.token))
        .map_err(|err| DialError::Io(err.to_string()))?;
    request.headers_mut().insert(AUTHORIZATION, bearer);
    let (mut socket, _) = connect_async_tls_with_config(request, Some(socket_config()), true, None)
        .await
        .map_err(|err| match err {
            WsError::Http(response) => DialError::Refused(response.status().as_u16()),
            other => DialError::Io(other.to_string()),
        })?;

    let (claim, _slot) = shared.core.claim().await;
    let resume = shared
        .core
        .resume_request()
        .encode()
        .map_err(|err| DialError::Handshake(err.to_string()))?;
    socket
        .send(Message::Binary(resume.into()))
        .await
        .map_err(|err| DialError::Io(err.to_string()))?;
    let Frame::Resumed {
        last_seen,
        epoch,
        outcome,
    } = first_frame(&mut socket).await?
    else {
        return Err(DialError::Handshake("expected `resumed`".to_owned()));
    };
    let core = &shared.core;
    match outcome {
        ResumeOutcome::Accepted => {
            let attached = core.attach(claim, |link| {
                if epoch == NO_EPOCH || link.epoch() != epoch {
                    return Err(format!(
                        "gateway accepted epoch {epoch}, this link holds {}",
                        link.epoch()
                    ));
                }
                link.resumed(last_seen).map_err(|err| err.to_string())
            });
            match attached {
                Ok(queue) => Ok((socket, queue, NodeEvent::Resumed)),
                Err(AttachError::Refused(reason)) => {
                    core.reset(unlinked(&shared.config));
                    Err(DialError::Handshake(reason))
                }
                Err(AttachError::Ended | AttachError::Superseded) => Err(DialError::Closed),
            }
        }
        ResumeOutcome::Refused { reason } => {
            if epoch == NO_EPOCH {
                return Err(DialError::Handshake(
                    "gateway refused without naming a fresh epoch".to_owned(),
                ));
            }
            tracing::info!(%reason, epoch, "rax gateway started a fresh link");
            match core.refresh(claim, link_with_epoch(&shared.config, epoch)) {
                Ok(queue) => {
                    shared.fail_all(Ended::Reset);
                    Ok((socket, queue, NodeEvent::Fresh))
                }
                Err(_) => Err(DialError::Closed),
            }
        }
    }
}

async fn first_frame(socket: &mut Socket) -> Result<Frame, DialError> {
    loop {
        match socket.next().await {
            Some(Ok(Message::Binary(bytes))) => {
                return Frame::decode(&bytes).map_err(|err| DialError::Handshake(err.to_string()));
            }
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
            Some(Ok(other)) => {
                return Err(DialError::Handshake(format!("unexpected {other:?}")));
            }
            Some(Err(err)) => return Err(DialError::Io(err.to_string())),
            None => return Err(DialError::Io("closed during the handshake".to_owned())),
        }
    }
}

struct Router {
    shared: Arc<Shared>,
    events: mpsc::Sender<NodeEvent>,
}

impl Router {
    async fn emit(&self, event: NodeEvent) -> Flow {
        tokio::select! {
            _ = self.events.send(event) => Flow::Continue,
            _ = self.shared.shutdown.cancelled() => Flow::Stop,
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
                self.shared.core.end(Ended::Closed);
                self.shared.fail_all(Ended::Closed);
                Flow::Stop
            }
        }
    }
}
