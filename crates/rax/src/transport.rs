//! One WebSocket per node, dialled by the node. There is no way for a gateway to dial a node: an
//! inbound-only gateway can never become a client of every laptop in the fleet.

use std::net::IpAddr;
use std::time::Duration;

use url::Url;

pub const PATH: &str = "/rax/v1/link";

/// A header, never a query parameter: a token in a URL lands in every access log and proxy trace,
/// and it is the whole of a node's identity.
pub const AUTHORIZATION_SCHEME: &str = "Bearer";

/// Below a typical loopback socket buffer, so a stalled peer parks the sender on the window,
/// which can be cancelled, rather than in the write, which cannot.
pub const DEFAULT_WINDOW_BYTES: usize = 256 << 10;

/// A teardown trigger, not a latency budget: a peer that has not drained a socket buffer in half a
/// minute is dead, not slow.
pub const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EndpointError {
    #[error("rax: no gateway address")]
    Missing,
    #[error("rax: gateway address {address:?}: {reason}")]
    Malformed { address: String, reason: String },
    #[error("rax: gateway address {0:?} must be ws:// or wss://")]
    Scheme(String),
    /// The credential travels in a header, so only loopback may skip TLS.
    #[error("rax: gateway address {0:?} is plain ws:// to a non-loopback host; wss:// is required")]
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
/// bare close frame; each tells the node whether redialling can help.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// Retrying will not fix it, so a node must say so loudly instead of looping.
    CredentialRejected,
    /// Nothing is wrong with the node; waiting is the right response.
    GatewayUnavailable,
    Other,
}

impl Refusal {
    pub fn from_status(status: u16) -> Self {
        match status {
            401 => Self::CredentialRejected,
            500..=599 => Self::GatewayUnavailable,
            _ => Self::Other,
        }
    }

    pub fn should_redial(self) -> bool {
        matches!(self, Self::GatewayUnavailable)
    }
}
