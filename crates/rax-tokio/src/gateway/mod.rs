//! The accepting end: authenticates before the upgrade and keeps one `Link` per node identity, so
//! a node that reconnects within `retain_for` resumes instead of starting over.

mod link;

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::{Stream, StreamExt};
use rax::frame::ResumeOutcome;
use rax::link::NO_EPOCH;
use rax::transport::{
    DEFAULT_WINDOW_BYTES, DEFAULT_WRITE_TIMEOUT, HANDSHAKE_TIMEOUT, PATH, bearer_token,
};
use rax::{Frame, Link, LinkConfig};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::{OwnedMutexGuard, mpsc};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::{WebSocketStream, accept_hdr_async_with_config};

pub use link::{
    GatewayLink, LinkEvent, LinkEvents, NewLink, PendingCall, PendingReply, StreamEvents,
};

use crate::connection::{AttachError, Ended, Queue, Timing, drive, lock, socket_config};
use link::{Router, Shared};

/// Read from the token, never from a payload, so a node can only ever speak for itself.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeIdentity(pub String);

pub trait Authenticator: Send + Sync + 'static {
    fn authenticate(&self, token: &str) -> Option<NodeIdentity>;
}

impl<F> Authenticator for F
where
    F: Fn(&str) -> Option<NodeIdentity> + Send + Sync + 'static,
{
    fn authenticate(&self, token: &str) -> Option<NodeIdentity> {
        self(token)
    }
}

#[derive(Debug, Clone)]
pub struct GatewayConfig {
    pub window_bytes: usize,
    pub ack_threshold: u64,
    /// Also the liveness probe: a socket silent for three intervals is torn down to await a resume.
    pub keepalive: Duration,
    pub handshake_timeout: Duration,
    pub write_timeout: Duration,
    /// How long a disconnected link waits for its node; after that a resume is refused.
    pub retain_for: Duration,
    pub events_capacity: usize,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            window_bytes: DEFAULT_WINDOW_BYTES,
            ack_threshold: LinkConfig::default().ack_threshold,
            keepalive: Duration::from_secs(20),
            handshake_timeout: HANDSHAKE_TIMEOUT,
            write_timeout: DEFAULT_WRITE_TIMEOUT,
            retain_for: Duration::from_secs(300),
            events_capacity: 64,
        }
    }
}

/// Stops accepting when dropped; links already handed out keep running until closed.
pub struct GatewayServer {
    local_addr: SocketAddr,
    accept: JoinHandle<()>,
}

impl GatewayServer {
    pub async fn bind(
        addr: impl ToSocketAddrs,
        authenticator: impl Authenticator,
        config: GatewayConfig,
    ) -> io::Result<(Self, NewLinks)> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        let (new_links, receiver) = mpsc::channel(config.events_capacity.max(1));
        let hub = Arc::new(Hub {
            authenticator: Box::new(authenticator),
            config,
            links: Mutex::new(HashMap::new()),
            new_links,
        });
        let accept = tokio::spawn(accept_loop(listener, hub));
        Ok((Self { local_addr, accept }, NewLinks { receiver }))
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

impl Drop for GatewayServer {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

pub struct NewLinks {
    receiver: mpsc::Receiver<NewLink>,
}

impl NewLinks {
    pub async fn recv(&mut self) -> Option<NewLink> {
        self.receiver.recv().await
    }
}

impl Stream for NewLinks {
    type Item = NewLink;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<NewLink>> {
        self.receiver.poll_recv(cx)
    }
}

pub(crate) struct Hub {
    authenticator: Box<dyn Authenticator>,
    config: GatewayConfig,
    links: Mutex<HashMap<NodeIdentity, Arc<Shared>>>,
    new_links: mpsc::Sender<NewLink>,
}

impl Hub {
    pub(crate) fn forget(&self, shared: &Arc<Shared>) {
        let mut links = lock(&self.links);
        if links
            .get(&shared.identity)
            .is_some_and(|held| Arc::ptr_eq(held, shared))
        {
            links.remove(&shared.identity);
        }
    }

    fn timing(&self) -> Timing {
        Timing {
            keepalive: self.config.keepalive,
            write_timeout: self.config.write_timeout,
        }
    }
}

async fn accept_loop(listener: TcpListener, hub: Arc<Hub>) {
    loop {
        match listener.accept().await {
            Ok((tcp, peer)) => {
                let _ = tcp.set_nodelay(true);
                tokio::spawn(serve(hub.clone(), tcp, peer));
            }
            Err(err) => tracing::warn!(error = %err, "rax accept failed"),
        }
    }
}

type Socket = WebSocketStream<TcpStream>;

struct Attached {
    shared: Arc<Shared>,
    queue: Queue,
    slot: OwnedMutexGuard<()>,
}

async fn serve(hub: Arc<Hub>, tcp: TcpStream, peer: SocketAddr) {
    let handshake = tokio::time::timeout(hub.config.handshake_timeout, handshake(&hub, tcp)).await;
    let (mut socket, identity, last_seen, epoch) = match handshake {
        Ok(Ok(accepted)) => accepted,
        Ok(Err(reason)) => {
            tracing::info!(%peer, %reason, "rax connection refused");
            return;
        }
        Err(_) => {
            tracing::info!(%peer, "rax handshake timed out");
            return;
        }
    };
    let Some(Attached {
        shared,
        queue,
        slot,
    }) = attach(&hub, identity, last_seen, epoch).await
    else {
        let _ = socket.close(None).await;
        return;
    };
    let mut router = Router {
        shared: shared.clone(),
    };
    let reason = drive(
        shared.core.clone(),
        socket,
        queue,
        hub.timing(),
        &mut router,
    )
    .await;
    if shared.core.is_ended() {
        return;
    }
    let claim = shared.core.current_claim();
    tokio::spawn(expire(hub.clone(), shared.clone(), claim));
    shared.emit(LinkEvent::Disconnected { reason }).await;
    drop(slot);
}

async fn expire(hub: Arc<Hub>, shared: Arc<Shared>, claim: u64) {
    tokio::time::sleep(hub.config.retain_for).await;
    if shared.core.current_claim() != claim || shared.core.is_attached() {
        return;
    }
    hub.forget(&shared);
    shared.finish(LinkEvent::Closed, Ended::Closed);
}

async fn handshake(hub: &Hub, tcp: TcpStream) -> Result<(Socket, NodeIdentity, u64, u64), String> {
    let mut identity = None;
    #[allow(clippy::result_large_err)]
    let callback = |request: &Request, response: Response| {
        if request.uri().path() != PATH {
            return Err(refusal(StatusCode::NOT_FOUND));
        }
        let token = request
            .headers()
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(bearer_token);
        match token.and_then(|token| hub.authenticator.authenticate(token)) {
            Some(node) => {
                identity = Some(node);
                Ok(response)
            }
            None => Err(refusal(StatusCode::UNAUTHORIZED)),
        }
    };
    let mut socket = accept_hdr_async_with_config(tcp, callback, Some(socket_config()))
        .await
        .map_err(|err| err.to_string())?;
    let identity = identity.ok_or("upgraded without an identity")?;
    loop {
        match socket.next().await {
            Some(Ok(Message::Binary(bytes))) => {
                return match Frame::decode(&bytes).map_err(|err| err.to_string())? {
                    Frame::Resume { last_seen, epoch } => Ok((socket, identity, last_seen, epoch)),
                    _ => Err("the first frame was not `resume`".to_owned()),
                };
            }
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
            Some(Ok(other)) => return Err(format!("unexpected {other:?} before `resume`")),
            Some(Err(err)) => return Err(err.to_string()),
            None => return Err("closed before `resume`".to_owned()),
        }
    }
}

fn refusal(status: StatusCode) -> ErrorResponse {
    let mut response = ErrorResponse::new(None);
    *response.status_mut() = status;
    response
}

async fn attach(
    hub: &Arc<Hub>,
    identity: NodeIdentity,
    last_seen: u64,
    epoch: u64,
) -> Option<Attached> {
    let held = lock(&hub.links).get(&identity).cloned();
    let unknown = || match epoch {
        NO_EPOCH => "nothing to resume; this is a fresh link".to_owned(),
        epoch => format!("this gateway holds no link for epoch {epoch}"),
    };
    let Some(shared) = held else {
        return fresh(hub, identity, None, unknown()).await;
    };
    if epoch == NO_EPOCH || shared.core.epoch() != epoch {
        return fresh(hub, identity, Some(shared), unknown()).await;
    }
    let (claim, slot) = shared.core.claim().await;
    let attached = shared.core.attach(claim, |link| {
        let (answer, replay) = link.accept_resume(last_seen, epoch);
        match answer {
            Frame::Resumed {
                outcome: ResumeOutcome::Refused { reason },
                ..
            } => Err(reason),
            answer => Ok(std::iter::once(answer).chain(replay).collect()),
        }
    });
    match attached {
        Ok(queue) => {
            let slot_holder = Attached {
                shared: shared.clone(),
                queue,
                slot,
            };
            shared.emit(LinkEvent::Resumed).await;
            Some(slot_holder)
        }
        Err(AttachError::Superseded) => None,
        Err(AttachError::Ended) => {
            drop(slot);
            let reason = "the link this node held was closed".to_owned();
            fresh(hub, identity, None, reason).await
        }
        Err(AttachError::Refused(reason)) => {
            drop(slot);
            fresh(hub, identity, Some(shared), reason).await
        }
    }
}

async fn fresh(
    hub: &Arc<Hub>,
    identity: NodeIdentity,
    previous: Option<Arc<Shared>>,
    reason: String,
) -> Option<Attached> {
    let config = &hub.config;
    let old_epoch = previous.as_ref().map(|shared| shared.core.epoch());
    let epoch = loop {
        let epoch = rand::random::<u64>();
        if epoch != NO_EPOCH && Some(epoch) != old_epoch {
            break epoch;
        }
    };
    let link = Link::new(LinkConfig {
        window_bytes: config.window_bytes,
        ack_threshold: config.ack_threshold,
        epoch,
        resume_from: 0,
    });
    let (shared, events) = Shared::new(
        identity.clone(),
        link,
        config.events_capacity.max(1),
        Arc::downgrade(hub),
    );
    let (claim, slot) = shared.core.claim().await;
    let refusal = Frame::Resumed {
        last_seen: 0,
        epoch,
        outcome: ResumeOutcome::Refused { reason },
    };
    let queue = shared
        .core
        .attach(claim, |_| Ok::<_, ()>(vec![refusal]))
        .ok()?;
    let replaced = lock(&hub.links).insert(identity, shared.clone());
    if let Some(replaced) = replaced {
        replaced.finish(LinkEvent::Replaced, Ended::Reset);
    }
    let new_link = NewLink {
        link: GatewayLink {
            shared: shared.clone(),
        },
        events,
    };
    if hub.new_links.send(new_link).await.is_err() {
        tracing::warn!("rax gateway has nobody accepting new links");
    }
    Some(Attached {
        shared,
        queue,
        slot,
    })
}
