//! The dialling end: one supervisor task keeps a single `Link` alive across sockets, whichever role
//! rides on it. The dialler sends `resume`, adopts the acceptor's epochs and redials with backoff.

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rax::frame::ResumeOutcome;
use rax::link::NO_EPOCH;
use rax::transport::{
    DEFAULT_WINDOW_BYTES, DEFAULT_WRITE_TIMEOUT, EndpointError, HANDSHAKE_TIMEOUT, Refusal, Role,
    resolve_endpoint,
};
use rax::{Frame, Link, LinkConfig};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::error::{ProtocolError, SubProtocolError};
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::http::header::{AUTHORIZATION, SEC_WEBSOCKET_PROTOCOL};
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_tls_with_config};
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::connection::{
    AttachError, Ended, Half, Lifecycle, Queue, Terminal, Timing, drive, socket_config,
};

#[derive(Debug, Clone)]
pub struct DialConfig {
    /// Addresses of one acceptor cluster, tried in turn: a dialler stays on the one that last
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

impl Default for DialConfig {
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

impl DialConfig {
    pub(crate) fn resolve(&self) -> Result<Vec<Url>, EndpointError> {
        if self.endpoints.is_empty() {
            return Err(EndpointError::Missing);
        }
        self.endpoints
            .iter()
            .map(|endpoint| resolve_endpoint(endpoint))
            .collect()
    }

    pub(crate) fn unlinked(&self) -> Link {
        self.link_with_epoch(NO_EPOCH)
    }

    fn link_with_epoch(&self, epoch: u64) -> Link {
        Link::new(LinkConfig {
            window_bytes: self.window_bytes,
            ack_threshold: self.ack_threshold,
            epoch,
            resume_from: 0,
        })
    }

    fn timing(&self) -> Timing {
        Timing {
            keepalive: self.keepalive,
            write_timeout: self.write_timeout,
        }
    }
}

/// Starts a dialler for either role. The role is the half you ask for:
/// [`crate::node::NodeLink::start`] dials as a node, [`crate::gateway::GatewayLink::dial`] as a
/// gateway.
pub struct Dialler;

impl Dialler {
    pub fn node(
        config: DialConfig,
    ) -> Result<(crate::node::NodeHandle, crate::node::NodeEvents), EndpointError> {
        crate::node::NodeLink::start(config)
    }

    pub fn gateway(
        config: DialConfig,
    ) -> Result<(crate::gateway::GatewayLink, crate::gateway::DialledEvents), EndpointError> {
        crate::gateway::GatewayLink::dial(config)
    }
}

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, thiserror::Error)]
enum DialError {
    #[error("refused with HTTP {0}")]
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

pub(crate) async fn supervise<H: Half>(
    half: Arc<H>,
    role: Role,
    config: DialConfig,
    endpoints: Vec<Url>,
    shutdown: CancellationToken,
) {
    let (mut current, mut failed_in_round, mut failures) = (0usize, 0usize, 0u32);
    loop {
        let endpoint = &endpoints[current];
        let dialled = tokio::select! {
            dialled = tokio::time::timeout(config.handshake_timeout, dial(&*half, role, &config, endpoint)) => {
                dialled.unwrap_or(Err(DialError::Timeout))
            }
            _ = shutdown.cancelled() => break,
        };
        match dialled {
            Ok((socket, queue, arrival)) => {
                (failed_in_round, failures) = (0, 0);
                if !notify(&*half, &shutdown, arrival).await {
                    break;
                }
                let mut router = half.router();
                let core = half.core().clone();
                let reason = drive(core, socket, queue, config.timing(), &mut router).await;
                if half.core().is_ended() {
                    break;
                }
                tracing::info!(%reason, ?role, "rax link disconnected");
                if !notify(&*half, &shutdown, Lifecycle::Disconnected(reason)).await {
                    break;
                }
            }
            Err(DialError::Closed) => break,
            Err(DialError::Refused(status)) if !Refusal::from_status(status).should_redial() => {
                let terminal = match Refusal::from_status(status) {
                    Refusal::CredentialRejected => Terminal::CredentialRejected,
                    _ => Terminal::Refused(status),
                };
                tracing::error!(status, ?role, "rax link refused; not redialling");
                half.finish(terminal);
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
            _ = shutdown.cancelled() => break,
        }
    }
    half.finish(Terminal::Closed);
}

async fn notify<H: Half>(half: &H, shutdown: &CancellationToken, event: Lifecycle) -> bool {
    tokio::select! {
        sent = half.notify(event) => sent,
        _ = shutdown.cancelled() => false,
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

/// The WebSocket step: bearer and role go in the request, refusals come back as statuses.
async fn connect(role: Role, token: &str, endpoint: &Url) -> Result<Socket, DialError> {
    let mut request = endpoint
        .as_str()
        .into_client_request()
        .map_err(|err| DialError::Io(err.to_string()))?;
    let bearer = HeaderValue::from_str(&format!("Bearer {token}"))
        .map_err(|err| DialError::Io(err.to_string()))?;
    request.headers_mut().insert(AUTHORIZATION, bearer);
    // A node offers nothing, which means a node, so it still reaches an acceptor that predates
    // the subprotocol: one that echoed nothing back would fail the client's handshake.
    if role != Role::Node {
        let offered = HeaderValue::from_static(role.subprotocol());
        request
            .headers_mut()
            .insert(SEC_WEBSOCKET_PROTOCOL, offered);
    }
    let (socket, _) = connect_async_tls_with_config(request, Some(socket_config()), true, None)
        .await
        .map_err(|err| match err {
            WsError::Http(response) => DialError::Refused(response.status().as_u16()),
            // An acceptor that predates roles upgrades without echoing one, and would take this
            // dialler for a node; that is the same answer as a `403`.
            WsError::Protocol(ProtocolError::SecWebSocketSubProtocolError(
                SubProtocolError::NoSubProtocol,
            )) => DialError::Refused(403),
            other => DialError::Io(other.to_string()),
        })?;
    Ok(socket)
}

/// The dialler's half of resume, on top of [`connect`].
async fn dial<H: Half>(
    half: &H,
    role: Role,
    config: &DialConfig,
    endpoint: &Url,
) -> Result<(Socket, Queue, Lifecycle), DialError> {
    let mut socket = connect(role, &config.token, endpoint).await?;
    let core = half.core();
    let (claim, _slot) = core.claim().await;
    let resume = core
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
    match outcome {
        ResumeOutcome::Accepted => {
            let attached = core.attach(claim, |link| {
                if epoch == NO_EPOCH || link.epoch() != epoch {
                    return Err(format!(
                        "the acceptor accepted epoch {epoch}, this link holds {}",
                        link.epoch()
                    ));
                }
                link.resumed(last_seen).map_err(|err| err.to_string())
            });
            match attached {
                Ok(queue) => Ok((socket, queue, Lifecycle::Resumed)),
                Err(AttachError::Refused(reason)) => {
                    core.reset(config.unlinked());
                    Err(DialError::Handshake(reason))
                }
                Err(AttachError::Ended | AttachError::Superseded) => Err(DialError::Closed),
            }
        }
        ResumeOutcome::Refused { reason } => {
            if epoch == NO_EPOCH {
                return Err(DialError::Handshake(
                    "the acceptor refused without naming a fresh epoch".to_owned(),
                ));
            }
            tracing::info!(%reason, epoch, "rax acceptor started a fresh link");
            match core.refresh(claim, config.link_with_epoch(epoch)) {
                Ok(queue) => {
                    half.fail_all(Ended::Reset);
                    Ok((socket, queue, Lifecycle::Fresh))
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
