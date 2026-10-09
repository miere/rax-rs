//! The gateway role: opens sessions, sends prompts and rules on tool calls. It rides on either
//! transport end — [`GatewayServer`] accepts nodes, and [`GatewayLink::dial`] dials an acceptor
//! that plays the node.

mod link;

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::Stream;
use rax::LinkConfig;
use rax::transport::{
    DEFAULT_WINDOW_BYTES, DEFAULT_WRITE_TIMEOUT, EndpointError, HANDSHAKE_TIMEOUT, Role,
};
use tokio::net::ToSocketAddrs;

pub use crate::accept::{Authenticator, NodeIdentity};
pub use link::{
    DialledEvent, DialledEvents, GatewayLink, LinkEvent, LinkEvents, LocalFile, NewLink,
    PendingCall, PendingReply, StreamEvents,
};

use crate::accept::{AcceptConfig, Accepted, AcceptedLinks, Acceptor};
use crate::connection::Owner;
use crate::dial::{DialConfig, supervise};
pub(crate) use link::Shared;

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

impl From<GatewayConfig> for AcceptConfig {
    fn from(config: GatewayConfig) -> Self {
        Self {
            roles: vec![Role::Node],
            window_bytes: config.window_bytes,
            ack_threshold: config.ack_threshold,
            keepalive: config.keepalive,
            handshake_timeout: config.handshake_timeout,
            write_timeout: config.write_timeout,
            retain_for: config.retain_for,
            events_capacity: config.events_capacity,
        }
    }
}

/// An [`Acceptor`] that serves nodes only. Stops accepting when dropped; links already handed
/// out keep running until closed.
pub struct GatewayServer {
    acceptor: Acceptor,
}

impl GatewayServer {
    pub async fn bind(
        addr: impl ToSocketAddrs,
        authenticator: impl Authenticator,
        config: GatewayConfig,
    ) -> io::Result<(Self, NewLinks)> {
        let (acceptor, accepted) = Acceptor::bind(addr, authenticator, config.into()).await?;
        Ok((Self { acceptor }, NewLinks { accepted }))
    }

    /// For a cluster member that stops serving, such as a leader that lost its lease: every
    /// dial is refused with 503 and every link is closed, so nodes move to another member.
    pub fn stop_serving(&self) {
        self.acceptor.stop_serving();
    }

    pub fn start_serving(&self) {
        self.acceptor.start_serving();
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.acceptor.local_addr()
    }
}

pub struct NewLinks {
    accepted: AcceptedLinks,
}

impl NewLinks {
    pub async fn recv(&mut self) -> Option<NewLink> {
        std::future::poll_fn(|cx| self.poll_link(cx)).await
    }

    /// The acceptor serves nodes only, so nothing else arrives; skip it rather than trust that.
    fn poll_link(&mut self, cx: &mut Context<'_>) -> Poll<Option<NewLink>> {
        loop {
            match self.accepted.receiver.poll_recv(cx) {
                Poll::Ready(Some(Accepted::Node(link))) => return Poll::Ready(Some(link)),
                Poll::Ready(Some(Accepted::Gateway(_))) => {}
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl Stream for NewLinks {
    type Item = NewLink;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<NewLink>> {
        self.poll_link(cx)
    }
}

impl GatewayLink {
    /// Dials as a gateway, offering `rax.v1.gateway`, to an acceptor that plays the node. The
    /// link lives across fresh links: on [`DialledEvent::Fresh`], send `initialize` again.
    pub fn dial(config: DialConfig) -> Result<(Self, DialledEvents), EndpointError> {
        let endpoints = config.resolve()?;
        let (shared, events) = Shared::new(
            NodeIdentity(String::new()),
            config.unlinked(),
            config.events_capacity,
            Owner::dialler(),
        );
        let shutdown = shared.shutdown.clone();
        let supervisor = tokio::spawn(supervise(
            shared.clone(),
            Role::Gateway,
            config,
            endpoints,
            shutdown,
        ));
        shared.owner.supervised_by(supervisor);
        Ok((Self { shared }, Shared::dialled_events(events)))
    }
}
