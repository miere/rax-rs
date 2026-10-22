//! The gateway's configuration: who administers it, who may join, the nodes they run and the
//! clients they hold. Backends differ only in how far their leader lock reaches.

mod leader;
mod model;
#[cfg(feature = "sqlite")]
mod sqlite;

use async_trait::async_trait;

#[cfg(feature = "sqlite")]
pub use leader::SqliteLeader;
pub use leader::{Leader, Lease};
pub use model::{
    Grant, NodeToken, Scope, ScopeError, ToolMode, ToolModeError, UserConfig, UserId, UserIdError,
    UserToken,
};
#[cfg(feature = "sqlite")]
pub use sqlite::{LeaderLock, SqliteStore};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[cfg(feature = "sqlite")]
    #[error("store: {0}")]
    Sqlite(rusqlite::Error),
    #[error("store: {path}: {source}")]
    Io {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// Another gateway holds this database; running two would serve every node twice.
    #[error("store: another gateway already holds {0}")]
    Locked(std::path::PathBuf),
    /// Whatever a backend outside this crate reports, already worded, such as `firestore: …`.
    #[error("store: {0}")]
    Backend(String),
    /// A guarded write lost to a concurrent one; callers that race on purpose treat it as a no.
    #[error("store: a concurrent write won: {0}")]
    Conflict(String),
    #[error("store: a stored value is not valid: {0}")]
    Corrupt(String),
    #[error("store: the background task stopped: {0}")]
    Task(String),
}

/// A row that failed to read comes back as the error that failed it, so a value SQLite holds is
/// judged exactly as any other backend judges it.
#[cfg(feature = "sqlite")]
impl From<rusqlite::Error> for StoreError {
    fn from(err: rusqlite::Error) -> Self {
        match err {
            rusqlite::Error::FromSqlConversionFailure(column, kind, inner) => {
                match inner.downcast::<StoreError>() {
                    Ok(store) => *store,
                    Err(other) => Self::Sqlite(rusqlite::Error::FromSqlConversionFailure(
                        column, kind, other,
                    )),
                }
            }
            other => Self::Sqlite(other),
        }
    }
}

pub type Result<T> = std::result::Result<T, StoreError>;

/// Every write is visible to the next read by any process on the same database, which is how a
/// CLI change reaches the live gateway.
#[async_trait]
pub trait Store: Send + Sync + 'static {
    async fn admin(&self) -> Result<Option<UserId>>;
    async fn set_admin(&self, user: &UserId) -> Result<()>;

    async fn grants(&self) -> Result<Vec<Grant>>;
    /// Idempotent: approving an approved user keeps the original approval.
    async fn approve(&self, user: &UserId, by: &UserId) -> Result<Grant>;
    /// Returns whether there was a grant to revoke.
    async fn revoke(&self, user: &UserId) -> Result<bool>;

    async fn users(&self) -> Result<Vec<UserConfig>>;
    async fn user(&self, user: &UserId) -> Result<UserConfig>;
    async fn set_allowed(&self, user: &UserId, allowed: bool) -> Result<()>;
    async fn set_tool_mode(&self, user: &UserId, mode: ToolMode) -> Result<()>;
    /// Returns whether the tool was not on the owner's whitelist before.
    async fn whitelist_tool(&self, user: &UserId, tool: &str) -> Result<bool>;
    /// Returns whether the tool was on the owner's whitelist.
    async fn unwhitelist_tool(&self, user: &UserId, tool: &str) -> Result<bool>;

    async fn node_tokens(&self) -> Result<Vec<NodeToken>>;
    async fn add_node_token(&self, token: &NodeToken) -> Result<()>;
    /// Returns whether a live token was revoked.
    async fn revoke_node_token(&self, selector: &str) -> Result<bool>;
    /// Reversible, unlike revoking: a disabled node keeps its credential and its link, it is just
    /// left out of routing until this is called again with `false`.
    async fn set_node_disabled(&self, selector: &str, disabled: bool) -> Result<()>;

    async fn user_tokens(&self) -> Result<Vec<UserToken>>;
    async fn add_user_token(&self, token: &UserToken) -> Result<()>;
    /// Returns whether a live token was revoked.
    async fn revoke_user_token(&self, selector: &str) -> Result<bool>;
}
