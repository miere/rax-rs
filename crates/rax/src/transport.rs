//! One WebSocket per link. Either role may dial: the dialler names the role it plays as a
//! subprotocol, and the acceptor plays the other. A server never dials a person's machine, so it
//! can never become a client of every laptop that ever connected to it.

use std::net::IpAddr;
use std::time::Duration;

use url::Url;

pub const PATH: &str = "/rax/v1/link";

/// A header, never a query parameter: a token in a URL lands in every access log and proxy trace,
/// and it is the whole of a node's identity.
pub const AUTHORIZATION_SCHEME: &str = "Bearer";

/// Which end of the message protocol a peer plays. Delivery never reads it: the dialler resumes
/// and the acceptor keeps links, whichever role each plays.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    Node,
    Gateway,
}

impl Role {
    /// The `Sec-WebSocket-Protocol` a dialler of this role offers.
    pub fn subprotocol(self) -> &'static str {
        match self {
            Self::Node => "rax.v1.node",
            Self::Gateway => "rax.v1.gateway",
        }
    }

    /// The role a dialler offered. No subprotocol means a node, which is every dialler that
    /// predates the header; a list is read in order, and the first known entry wins.
    pub fn from_subprotocols(offered: Option<&str>) -> Option<Self> {
        let Some(offered) = offered else {
            return Some(Self::Node);
        };
        offered
            .split(',')
            .map(str::trim)
            .find_map(|protocol| match protocol {
                "rax.v1.node" => Some(Self::Node),
                "rax.v1.gateway" => Some(Self::Gateway),
                _ => None,
            })
    }

    /// The role the other end of a link plays.
    pub fn peer(self) -> Self {
        match self {
            Self::Node => Self::Gateway,
            Self::Gateway => Self::Node,
        }
    }
}

/// Below a typical loopback socket buffer, so a stalled peer parks the sender on the window,
/// which can be cancelled, rather than in the write, which cannot.
pub const DEFAULT_WINDOW_BYTES: usize = 256 << 10;

/// A teardown trigger, not a latency budget: a peer that has not drained a socket buffer in half a
/// minute is dead, not slow.
pub const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EndpointError {
    #[error("rax: no address to dial")]
    Missing,
    #[error("rax: address {address:?}: {reason}")]
    Malformed { address: String, reason: String },
    #[error("rax: address {0:?} must be ws:// or wss://")]
    Scheme(String),
    /// The credential travels in a header, so only loopback may skip TLS.
    #[error("rax: address {0:?} is plain ws:// to a non-loopback host; wss:// is required")]
    Insecure(String),
}

pub fn resolve_endpoint(address: &str) -> Result<Url, EndpointError> {
    let trimmed = address.trim();
    if trimmed.is_empty() {
        return Err(EndpointError::Missing);
    }
    let mut url = Url::parse(trimmed).map_err(|err| EndpointError::Malformed {
        address: address.to_owned(),
        reason: err.to_string(),
    })?;
    match url.scheme() {
        "wss" => {}
        "ws" if is_loopback(&url) => {}
        "ws" => return Err(EndpointError::Insecure(address.to_owned())),
        _ => return Err(EndpointError::Scheme(address.to_owned())),
    }
    if url.path().is_empty() || url.path() == "/" {
        url.set_path(PATH);
    }
    Ok(url)
}

fn is_loopback(url: &Url) -> bool {
    match url.host() {
        Some(url::Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(ip)) => IpAddr::V4(ip).is_loopback(),
        Some(url::Host::Ipv6(ip)) => IpAddr::V6(ip).is_loopback(),
        None => false,
    }
}

pub fn bearer_token(authorization: &str) -> Option<&str> {
    let rest = authorization.trim().strip_prefix(AUTHORIZATION_SCHEME)?;
    let token = rest.strip_prefix(' ')?.trim();
    (!token.is_empty()).then_some(token)
}

/// Refusals are HTTP statuses before the upgrade, so the dialler reads a reason instead of a
/// bare close frame; each tells the dialler whether redialling can help.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// Retrying will not fix it, so a dialler must say so loudly instead of looping.
    CredentialRejected,
    /// The acceptor does not serve the role the dialler offered, and never will for this build.
    RoleRefused,
    /// Nothing is wrong with the dialler; waiting is the right response.
    GatewayUnavailable,
    Other,
}

impl Refusal {
    pub fn from_status(status: u16) -> Self {
        match status {
            401 => Self::CredentialRejected,
            403 => Self::RoleRefused,
            500..=599 => Self::GatewayUnavailable,
            _ => Self::Other,
        }
    }

    pub fn should_redial(self) -> bool {
        matches!(self, Self::GatewayUnavailable)
    }
}
