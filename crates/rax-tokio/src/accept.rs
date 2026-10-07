//! The accepting end: authenticates before the upgrade and keeps one `Link` per dialler identity,
//! so a dialler that reconnects within `retain_for` resumes instead of starting over. The acceptor
//! answers `resumed` and mints epochs, whichever role the dialler plays.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::{Stream, StreamExt};
use rax::frame::ResumeOutcome;
use rax::link::NO_EPOCH;
use rax::transport::{
    DEFAULT_WINDOW_BYTES, DEFAULT_WRITE_TIMEOUT, HANDSHAKE_TIMEOUT, PATH, Role, bearer_token,
};
use rax::{Frame, Link, LinkConfig};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::sync::{OwnedMutexGuard, mpsc};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::header::{AUTHORIZATION, SEC_WEBSOCKET_PROTOCOL};
use tokio_tungstenite::tungstenite::http::{HeaderValue, StatusCode};
use tokio_tungstenite::{WebSocketStream, accept_hdr_async_with_config};

use crate::connection::{
    AttachError, Core, Half, Lifecycle, Owner, Queue, Terminal, Timing, drive, lock, socket_config,
};
use crate::gateway::{self, NewLink};
use crate::node::{self, NewNodeLink};

/// Read from the token, never from a payload, so a node can only ever speak for itself.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeIdentity(pub String);

/// A gateway-role dialler, read from its token the same way.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GatewayIdentity(pub String);

/// Judges a dialler's token for the role it offered. An acceptor only asks about roles it serves.
pub trait Authenticator: Send + Sync + 'static {
    fn authenticate(&self, token: &str) -> Option<NodeIdentity>;

    /// Refuses every gateway unless overridden, so a node-only authenticator stays one.
    fn authenticate_gateway(&self, _token: &str) -> Option<GatewayIdentity> {
        None
    }
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
pub struct AcceptConfig {
    /// The roles a dialler may offer. Any other is refused with `403` before the upgrade.
    pub roles: Vec<Role>,
    pub window_bytes: usize,
    pub ack_threshold: u64,
    /// Also the liveness probe: a socket silent for three intervals is torn down to await a resume.
    pub keepalive: Duration,
    pub handshake_timeout: Duration,
    pub write_timeout: Duration,
    /// How long a disconnected link waits for its dialler; after that a resume is refused.
    pub retain_for: Duration,
    pub events_capacity: usize,
}

impl Default for AcceptConfig {
    fn default() -> Self {
        Self {
            roles: vec![Role::Node],
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

/// A fresh link, named by the role of whoever dialled. This side plays the other.
pub enum Accepted {
    /// A node dialled; this side plays the gateway.
    Node(NewLink),
    /// A gateway dialled; this side plays the node.
    Gateway(NewNodeLink),
}

/// Stops accepting when dropped; links already handed out keep running until closed.
pub struct Acceptor {
    local_addr: SocketAddr,
    hub: Arc<Hub>,
    accept: JoinHandle<()>,
}

impl Acceptor {
    pub async fn bind(
        addr: impl ToSocketAddrs,
        authenticator: impl Authenticator,
        config: AcceptConfig,
    ) -> io::Result<(Self, AcceptedLinks)> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        let (accepted, receiver) = mpsc::channel(config.events_capacity.max(1));
        let hub = Arc::new(Hub {
            authenticator: Box::new(authenticator),
            config,
            links: Mutex::new(HashMap::new()),
            accepted,
            serving: AtomicBool::new(true),
        });
        let accept = tokio::spawn(accept_loop(listener, hub.clone()));
        let acceptor = Self {
            local_addr,
            hub,
            accept,
        };
        Ok((acceptor, AcceptedLinks { receiver }))
    }

    /// For a cluster member that stops serving, such as a leader that lost its lease: every
    /// dial is refused with 503 and every link is closed, so diallers move to another member.
    pub fn stop_serving(&self) {
        self.hub.serving.store(false, Ordering::SeqCst);
        let links: Vec<_> = lock(&self.hub.links)
            .drain()
            .map(|(_, link)| link)
            .collect();
        for link in links {
            link.finish(Terminal::Closed);
        }
    }

    pub fn start_serving(&self) {
        self.hub.serving.store(true, Ordering::SeqCst);
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

impl Drop for Acceptor {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

pub struct AcceptedLinks {
    pub(crate) receiver: mpsc::Receiver<Accepted>,
}

impl AcceptedLinks {
    pub async fn recv(&mut self) -> Option<Accepted> {
        self.receiver.recv().await
    }
}

impl Stream for AcceptedLinks {
    type Item = Accepted;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Accepted>> {
        self.receiver.poll_recv(cx)
    }
}

/// A dialler's identity within its role: a node and a gateway may share a name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Key {
    role: Role,
    identity: String,
}

/// The link held for one dialler, by the role it dialled as.
enum Held {
    Node(Arc<gateway::Shared>),
    Gateway(Arc<node::Shared>),
}

impl Held {
    fn core(&self) -> &Arc<Core> {
        match self {
            Self::Node(shared) => shared.core(),
            Self::Gateway(shared) => shared.core(),
        }
    }

    fn finish(&self, terminal: Terminal) {
        match self {
            Self::Node(shared) => shared.finish(terminal),
            Self::Gateway(shared) => shared.finish(terminal),
        }
    }
}

/// A role half the acceptor can create and hold, one per fresh link.
trait Accept: Half {
    fn held(held: &Held) -> Option<&Arc<Self>>;

    fn hold(self: &Arc<Self>) -> Held;

    fn create(key: &Key, link: Link, hub: &Arc<Hub>) -> (Arc<Self>, Accepted);
}

impl Accept for gateway::Shared {
    fn held(held: &Held) -> Option<&Arc<Self>> {
        match held {
            Held::Node(shared) => Some(shared),
            Held::Gateway(_) => None,
        }
    }

    fn hold(self: &Arc<Self>) -> Held {
        Held::Node(self.clone())
    }

    fn create(key: &Key, link: Link, hub: &Arc<Hub>) -> (Arc<Self>, Accepted) {
        let (shared, events) = Self::new(
            NodeIdentity(key.identity.clone()),
            link,
            hub.config.events_capacity,
            hub.owner(key),
        );
        let new_link = NewLink {
            link: gateway::GatewayLink {
                shared: shared.clone(),
            },
            events: Self::link_events(events),
        };
        (shared, Accepted::Node(new_link))
    }
}

impl Accept for node::Shared {
    fn held(held: &Held) -> Option<&Arc<Self>> {
        match held {
            Held::Gateway(shared) => Some(shared),
            Held::Node(_) => None,
        }
    }

    fn hold(self: &Arc<Self>) -> Held {
        Held::Gateway(self.clone())
    }

    fn create(key: &Key, link: Link, hub: &Arc<Hub>) -> (Arc<Self>, Accepted) {
        let (shared, events) = Self::new(link, hub.config.events_capacity, hub.owner(key), false);
        let new_link = NewNodeLink {
            identity: GatewayIdentity(key.identity.clone()),
            handle: shared.handle(),
            events,
        };
        (shared, Accepted::Gateway(new_link))
    }
}

pub(crate) struct Hub {
    authenticator: Box<dyn Authenticator>,
    config: AcceptConfig,
    links: Mutex<HashMap<Key, Held>>,
    accepted: mpsc::Sender<Accepted>,
    serving: AtomicBool,
}

impl Hub {
    pub(crate) fn forget(&self, key: &Key, core: &Arc<Core>) {
        let mut links = lock(&self.links);
        if links
            .get(key)
            .is_some_and(|held| Arc::ptr_eq(held.core(), core))
        {
            links.remove(key);
        }
    }

    fn owner(self: &Arc<Self>, key: &Key) -> Owner {
        Owner::Acceptor {
            hub: Arc::downgrade(self),
            key: key.clone(),
        }
    }

    fn timing(&self) -> Timing {
        Timing {
            keepalive: self.config.keepalive,
            write_timeout: self.config.write_timeout,
        }
    }

    fn authenticate(&self, role: Role, token: &str) -> Option<String> {
        match role {
            Role::Node => self
                .authenticator
                .authenticate(token)
                .map(|NodeIdentity(identity)| identity),
            Role::Gateway => self
                .authenticator
                .authenticate_gateway(token)
                .map(|GatewayIdentity(identity)| identity),
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

struct Attached<H> {
    half: Arc<H>,
    queue: Queue,
    slot: OwnedMutexGuard<()>,
}

struct Handshake<S> {
    socket: WebSocketStream<S>,
    key: Key,
    last_seen: u64,
    epoch: u64,
}

async fn serve<S>(hub: Arc<Hub>, stream: S, peer: SocketAddr)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let handshake = tokio::time::timeout(hub.config.handshake_timeout, handshake(&hub, stream));
    let handshake = match handshake.await {
        Ok(Ok(handshake)) => handshake,
        Ok(Err(reason)) => {
            tracing::info!(%peer, %reason, "rax connection refused");
            return;
        }
        Err(_) => {
            tracing::info!(%peer, "rax handshake timed out");
            return;
        }
    };
    match handshake.key.role {
        Role::Node => run::<gateway::Shared, S>(hub, handshake).await,
        Role::Gateway => run::<node::Shared, S>(hub, handshake).await,
    }
}

async fn run<H, S>(hub: Arc<Hub>, handshake: Handshake<S>)
where
    H: Accept,
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let Handshake {
        mut socket,
        key,
        last_seen,
        epoch,
    } = handshake;
    let Some(Attached { half, queue, slot }) =
        attach::<H>(&hub, key.clone(), last_seen, epoch).await
    else {
        let _ = socket.close(None).await;
        return;
    };
    let mut router = half.router();
    let reason = drive(
        half.core().clone(),
        socket,
        queue,
        hub.timing(),
        &mut router,
    )
    .await;
    if half.core().is_ended() {
        return;
    }
    let claim = half.core().current_claim();
    tokio::spawn(expire(hub.clone(), key, half.clone(), claim));
    half.notify(Lifecycle::Disconnected(reason)).await;
    drop(slot);
}

async fn expire<H: Accept>(hub: Arc<Hub>, key: Key, half: Arc<H>, claim: u64) {
    tokio::time::sleep(hub.config.retain_for).await;
    let core = half.core();
    if core.current_claim() != claim || core.is_attached() {
        return;
    }
    hub.forget(&key, core);
    half.finish(Terminal::Closed);
}

/// The WebSocket step: path, serving, role and bearer, each refused with its own status.
async fn upgrade<S>(hub: &Hub, stream: S) -> Result<(WebSocketStream<S>, Key), String>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut key = None;
    #[allow(clippy::result_large_err)]
    let callback = |request: &Request, mut response: Response| {
        if request.uri().path() != PATH {
            return Err(refusal(StatusCode::NOT_FOUND));
        }
        if !hub.serving.load(Ordering::SeqCst) {
            return Err(refusal(StatusCode::SERVICE_UNAVAILABLE));
        }
        let offered = request.headers().get(SEC_WEBSOCKET_PROTOCOL);
        let role = match offered.map(HeaderValue::to_str) {
            None => Some(Role::Node),
            Some(Ok(offered)) => Role::from_subprotocols(Some(offered)),
            Some(Err(_)) => None,
        };
        let Some(role) = role.filter(|role| hub.config.roles.contains(role)) else {
            return Err(refusal(StatusCode::FORBIDDEN));
        };
        let token = request
            .headers()
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(bearer_token);
        let Some(identity) = token.and_then(|token| hub.authenticate(role, token)) else {
            return Err(refusal(StatusCode::UNAUTHORIZED));
        };
        if offered.is_some() {
            let echoed = HeaderValue::from_static(role.subprotocol());
            response
                .headers_mut()
                .insert(SEC_WEBSOCKET_PROTOCOL, echoed);
        }
        key = Some(Key { role, identity });
        Ok(response)
    };
    let socket = accept_hdr_async_with_config(stream, callback, Some(socket_config()))
        .await
        .map_err(|err| err.to_string())?;
    let key = key.ok_or("upgraded without an identity")?;
    Ok((socket, key))
}

/// [`upgrade`], then the acceptor's half of resume up to the dialler's `resume`.
async fn handshake<S>(hub: &Hub, stream: S) -> Result<Handshake<S>, String>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut socket, key) = upgrade(hub, stream).await?;
    loop {
        match socket.next().await {
            Some(Ok(Message::Binary(bytes))) => {
                return match Frame::decode(&bytes).map_err(|err| err.to_string())? {
                    Frame::Resume { last_seen, epoch } => Ok(Handshake {
                        socket,
                        key,
                        last_seen,
                        epoch,
                    }),
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

async fn attach<H: Accept>(
    hub: &Arc<Hub>,
    key: Key,
    last_seen: u64,
    epoch: u64,
) -> Option<Attached<H>> {
    let held = lock(&hub.links).get(&key).and_then(H::held).cloned();
    let unknown = || match epoch {
        NO_EPOCH => "nothing to resume; this is a fresh link".to_owned(),
        epoch => format!("this acceptor holds no link for epoch {epoch}"),
    };
    let Some(half) = held else {
        return fresh::<H>(hub, key, None, unknown()).await;
    };
    if epoch == NO_EPOCH || half.core().epoch() != epoch {
        return fresh(hub, key, Some(half), unknown()).await;
    }
    let (claim, slot) = half.core().claim().await;
    let attached = half.core().attach(claim, |link| {
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
            half.notify(Lifecycle::Resumed).await;
            Some(Attached { half, queue, slot })
        }
        Err(AttachError::Superseded) => None,
        Err(AttachError::Ended) => {
            drop(slot);
            let reason = "the link this dialler held was closed".to_owned();
            fresh::<H>(hub, key, None, reason).await
        }
        Err(AttachError::Refused(reason)) => {
            drop(slot);
            fresh(hub, key, Some(half), reason).await
        }
    }
}

async fn fresh<H: Accept>(
    hub: &Arc<Hub>,
    key: Key,
    previous: Option<Arc<H>>,
    reason: String,
) -> Option<Attached<H>> {
    let config = &hub.config;
    let old_epoch = previous.as_ref().map(|half| half.core().epoch());
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
    let (half, accepted) = H::create(&key, link, hub);
    let (claim, slot) = half.core().claim().await;
    let refusal = Frame::Resumed {
        last_seen: 0,
        epoch,
        outcome: ResumeOutcome::Refused { reason },
    };
    let queue = half
        .core()
        .attach(claim, |_| Ok::<_, ()>(vec![refusal]))
        .ok()?;
    let replaced = lock(&hub.links).insert(key, half.hold());
    if let Some(replaced) = replaced {
        replaced.finish(Terminal::Replaced);
    }
    if hub.accepted.send(accepted).await.is_err() {
        tracing::warn!("rax acceptor has nobody taking new links");
    }
    Some(Attached { half, queue, slot })
}
