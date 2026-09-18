use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// Names no owner and no node: the gateway reads both off the connection, so a node can only
/// ever report on its own credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialHealth {
    pub credential: String,
    #[serde(default)]
    pub degraded: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "time::serde::rfc3339::option"
    )]
    pub since: Option<OffsetDateTime>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "time::serde::rfc3339::option"
    )]
    pub expires_at: Option<OffsetDateTime>,
}

/// Says what the node did, so nobody is told a sign-in is on its way when one was already open
/// or there was nothing to sign in to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CredentialRenewal {
    Started,
    AlreadyRunning,
    NothingToRenew,
}
