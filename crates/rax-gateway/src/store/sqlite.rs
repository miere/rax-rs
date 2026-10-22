use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension, Row, params};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::{
    Grant, NodeToken, Result, Scope, Store, StoreError, ToolMode, UserConfig, UserId, UserToken,
};
use crate::principal::{self, Principals};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS grants (
    user_id TEXT PRIMARY KEY,
    approved_by TEXT NOT NULL,
    approved_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    allowed INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS tool_modes (
    user_id TEXT PRIMARY KEY,
    mode TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS tool_whitelist (
    user_id TEXT NOT NULL,
    tool TEXT NOT NULL,
    PRIMARY KEY (user_id, tool)
);
CREATE TABLE IF NOT EXISTS node_tokens (
    selector TEXT PRIMARY KEY,
    secret_hash TEXT NOT NULL,
    owner TEXT NOT NULL,
    name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    revoked_at TEXT,
    disabled_at TEXT
);
CREATE TABLE IF NOT EXISTS user_tokens (
    selector TEXT PRIMARY KEY,
    secret_hash TEXT NOT NULL,
    owner TEXT NOT NULL,
    name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    revoked_at TEXT,
    scopes TEXT NOT NULL DEFAULT 'rax'
);
";

const ADMIN: &str = "admin";

/// A database file on this machine. Any number of processes may open it; only one may hold
/// [`LeaderLock`], which is what keeps a second gateway from serving the same nodes.
///
/// An embedder that keeps more beside the gateway's tables adds its own with [`Self::extend`] and
/// reaches them through [`Self::run`], on the same connection.
#[derive(Clone)]
pub struct SqliteStore {
    connection: Arc<Mutex<Connection>>,
    principals: Arc<dyn Principals>,
}

/// Held for as long as this process serves; the OS releases it if the process dies.
#[derive(Debug)]
pub struct LeaderLock {
    _file: File,
}

impl SqliteStore {
    /// Takes any id [`UserId::parse`] takes.
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with(path, principal::any())
    }

    /// Reads back only the ids `principals` takes: a row holding any other is corrupt.
    pub fn open_with(path: &Path, principals: Arc<dyn Principals>) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| StoreError::Io {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        let connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.execute_batch(SCHEMA)?;
        add_disabled_at_column(&connection)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            principals,
        })
    }

    /// Creates the embedder's own tables. Idempotent statements only, as for the gateway's.
    pub fn extend(&self, schema: &str) -> Result<()> {
        Ok(guard(&self.connection).execute_batch(schema)?)
    }

    /// An id read from a row, judged as every other backend judges it.
    pub fn user_id(&self, raw: &str) -> Result<UserId> {
        user_id(&*self.principals, raw)
    }

    /// For use inside a row mapping: a bad id fails the row with the same error.
    pub fn row_user_id(&self, raw: &str) -> rusqlite::Result<UserId> {
        row_user_id(&*self.principals, raw)
    }

    pub fn lock_leader(path: &Path) -> Result<LeaderLock> {
        let lock_path = lock_path(path);
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .map_err(|source| StoreError::Io {
                path: lock_path.clone(),
                source,
            })?;
        match file.try_lock() {
            Ok(()) => Ok(LeaderLock { _file: file }),
            Err(std::fs::TryLockError::WouldBlock) => Err(StoreError::Locked(path.to_path_buf())),
            Err(std::fs::TryLockError::Error(source)) => Err(StoreError::Io {
                path: lock_path,
                source,
            }),
        }
    }

    pub async fn run<T, F>(&self, work: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    {
        let connection = self.connection.clone();
        tokio::task::spawn_blocking(move || work(&mut guard(&connection)))
            .await
            .map_err(|err| StoreError::Task(err.to_string()))?
    }
}

/// `node_tokens` predates the `disabled_at` column, and `CREATE TABLE IF NOT EXISTS` above never
/// alters a table that already exists, so an existing database needs this one-off patch to grow
/// the column. If a schema change ever needs more than this, it's time to give SQLite a real
/// migration runner instead of patching `open` again.
fn add_disabled_at_column(connection: &Connection) -> Result<()> {
    match connection.execute("ALTER TABLE node_tokens ADD COLUMN disabled_at TEXT", []) {
        Ok(_) => Ok(()),
        Err(rusqlite::Error::SqliteFailure(_, Some(ref msg)))
            if msg.contains("duplicate column") =>
        {
            Ok(())
        }
        Err(err) => Err(err.into()),
    }
}

fn guard(connection: &Mutex<Connection>) -> MutexGuard<'_, Connection> {
    connection
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn lock_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".lock");
    path.with_file_name(name)
}

fn now() -> OffsetDateTime {
    OffsetDateTime::now_utc()
}

fn stamp(at: OffsetDateTime) -> Result<String> {
    at.format(&Rfc3339)
        .map_err(|err| StoreError::Corrupt(err.to_string()))
}

fn parse_stamp(raw: &str) -> rusqlite::Result<OffsetDateTime> {
    OffsetDateTime::parse(raw, &Rfc3339).map_err(|err| invalid(raw, err))
}

fn user_config(db: &Connection, user: UserId) -> Result<UserConfig> {
    let allowed: Option<bool> = db
        .query_row(
            "SELECT allowed FROM users WHERE user_id = ?1",
            [user.as_str()],
            |row| row.get(0),
        )
        .optional()?;
    let mode: Option<String> = db
        .query_row(
            "SELECT mode FROM tool_modes WHERE user_id = ?1",
            [user.as_str()],
            |row| row.get(0),
        )
        .optional()?;
    let mut query = db.prepare("SELECT tool FROM tool_whitelist WHERE user_id = ?1")?;
    let whitelist = query
        .query_map([user.as_str()], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<_>>()?;
    let tool_mode = match mode {
        Some(raw) => raw
            .parse()
            .map_err(|err: super::ToolModeError| StoreError::Corrupt(err.to_string()))?,
        None => ToolMode::default(),
    };
    Ok(UserConfig {
        user,
        allowed: allowed.unwrap_or(false),
        tool_mode,
        whitelist,
    })
}

fn user_id(principals: &dyn Principals, raw: &str) -> Result<UserId> {
    principals
        .parse(raw)
        .map_err(|err| StoreError::Corrupt(err.to_string()))
}

fn row_user_id(principals: &dyn Principals, raw: &str) -> rusqlite::Result<UserId> {
    user_id(principals, raw).map_err(failed)
}

/// Carries a [`StoreError`] out of a row mapping, which `From<rusqlite::Error>` unwraps again.
fn failed(err: StoreError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(err))
}

fn invalid(raw: &str, err: impl std::fmt::Display) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        format!("{raw:?}: {err}").into(),
    )
}

fn grant_row(principals: &dyn Principals, row: &Row<'_>) -> rusqlite::Result<Grant> {
    Ok(Grant {
        user: row_user_id(principals, &row.get::<_, String>(0)?)?,
        approved_by: row_user_id(principals, &row.get::<_, String>(1)?)?,
        approved_at: parse_stamp(&row.get::<_, String>(2)?)?,
    })
}

fn token_row(principals: &dyn Principals, row: &Row<'_>) -> rusqlite::Result<NodeToken> {
    Ok(NodeToken {
        selector: row.get(0)?,
        secret_hash: row.get(1)?,
        owner: row_user_id(principals, &row.get::<_, String>(2)?)?,
        name: row.get(3)?,
        created_at: parse_stamp(&row.get::<_, String>(4)?)?,
        revoked_at: row
            .get::<_, Option<String>>(5)?
            .map(|raw| parse_stamp(&raw))
            .transpose()?,
        disabled_at: row
            .get::<_, Option<String>>(6)?
            .map(|raw| parse_stamp(&raw))
            .transpose()?,
    })
}

fn user_token_row(principals: &dyn Principals, row: &Row<'_>) -> rusqlite::Result<UserToken> {
    Ok(UserToken {
        selector: row.get(0)?,
        secret_hash: row.get(1)?,
        owner: row_user_id(principals, &row.get::<_, String>(2)?)?,
        name: row.get(3)?,
        created_at: parse_stamp(&row.get::<_, String>(4)?)?,
        revoked_at: row
            .get::<_, Option<String>>(5)?
            .map(|raw| parse_stamp(&raw))
            .transpose()?,
        scopes: {
            let raw = row.get::<_, String>(6)?;
            Scope::split(&raw).map_err(|err| failed(StoreError::Corrupt(err.to_string())))?
        },
    })
}

#[async_trait]
impl Store for SqliteStore {
    async fn admin(&self) -> Result<Option<UserId>> {
        let principals = self.principals.clone();
        self.run(move |db| {
            let raw: Option<String> = db
                .query_row(
                    "SELECT value FROM settings WHERE key = ?1",
                    [ADMIN],
                    |row| row.get(0),
                )
                .optional()?;
            raw.map(|raw| user_id(&*principals, &raw)).transpose()
        })
        .await
    }

    async fn set_admin(&self, user: &UserId) -> Result<()> {
        let user = user.to_string();
        self.run(move |db| {
            db.execute(
                "INSERT INTO settings (key, value) VALUES (?1, ?2)
                 ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                params![ADMIN, user],
            )?;
            Ok(())
        })
        .await
    }

    async fn grants(&self) -> Result<Vec<Grant>> {
        let principals = self.principals.clone();
        self.run(move |db| {
            let mut query = db.prepare(
                "SELECT user_id, approved_by, approved_at FROM grants ORDER BY approved_at",
            )?;
            let grants = query
                .query_map([], |row| grant_row(&*principals, row))?
                .collect::<rusqlite::Result<_>>()?;
            Ok(grants)
        })
        .await
    }

    async fn approve(&self, user: &UserId, by: &UserId) -> Result<Grant> {
        let (user, by, at) = (user.to_string(), by.to_string(), stamp(now())?);
        let principals = self.principals.clone();
        self.run(move |db| {
            db.execute(
                "INSERT INTO grants (user_id, approved_by, approved_at) VALUES (?1, ?2, ?3)
                 ON CONFLICT (user_id) DO NOTHING",
                params![user, by, at],
            )?;
            Ok(db.query_row(
                "SELECT user_id, approved_by, approved_at FROM grants WHERE user_id = ?1",
                [&user],
                |row| grant_row(&*principals, row),
            )?)
        })
        .await
    }

    async fn revoke(&self, user: &UserId) -> Result<bool> {
        let user = user.to_string();
        self.run(move |db| Ok(db.execute("DELETE FROM grants WHERE user_id = ?1", [user])? > 0))
            .await
    }

    async fn users(&self) -> Result<Vec<UserConfig>> {
        let principals = self.principals.clone();
        self.run(move |db| {
            let mut query = db.prepare(
                "SELECT user_id FROM users UNION SELECT user_id FROM tool_modes
                 UNION SELECT user_id FROM tool_whitelist ORDER BY user_id",
            )?;
            let ids = query
                .query_map([], |row| {
                    row_user_id(&*principals, &row.get::<_, String>(0)?)
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            ids.into_iter().map(|id| user_config(db, id)).collect()
        })
        .await
    }

    async fn user(&self, user: &UserId) -> Result<UserConfig> {
        let id = user.clone();
        self.run(move |db| user_config(db, id)).await
    }

    async fn set_tool_mode(&self, user: &UserId, mode: ToolMode) -> Result<()> {
        let user = user.to_string();
        self.run(move |db| {
            db.execute(
                "INSERT INTO tool_modes (user_id, mode) VALUES (?1, ?2)
                 ON CONFLICT (user_id) DO UPDATE SET mode = excluded.mode",
                params![user, mode.as_str()],
            )?;
            Ok(())
        })
        .await
    }

    async fn whitelist_tool(&self, user: &UserId, tool: &str) -> Result<bool> {
        let (user, tool) = (user.to_string(), tool.to_owned());
        self.run(move |db| {
            Ok(db.execute(
                "INSERT INTO tool_whitelist (user_id, tool) VALUES (?1, ?2) ON CONFLICT DO NOTHING",
                params![user, tool],
            )? > 0)
        })
        .await
    }

    async fn unwhitelist_tool(&self, user: &UserId, tool: &str) -> Result<bool> {
        let (user, tool) = (user.to_string(), tool.to_owned());
        self.run(move |db| {
            Ok(db.execute(
                "DELETE FROM tool_whitelist WHERE user_id = ?1 AND tool = ?2",
                params![user, tool],
            )? > 0)
        })
        .await
    }

    async fn set_allowed(&self, user: &UserId, allowed: bool) -> Result<()> {
        let user = user.to_string();
        self.run(move |db| {
            db.execute(
                "INSERT INTO users (user_id, allowed) VALUES (?1, ?2)
                 ON CONFLICT (user_id) DO UPDATE SET allowed = excluded.allowed",
                params![user, allowed],
            )?;
            Ok(())
        })
        .await
    }

    async fn node_tokens(&self) -> Result<Vec<NodeToken>> {
        let principals = self.principals.clone();
        self.run(move |db| {
            let mut query = db.prepare(
                "SELECT selector, secret_hash, owner, name, created_at, revoked_at, disabled_at
                 FROM node_tokens ORDER BY created_at",
            )?;
            let tokens = query
                .query_map([], |row| token_row(&*principals, row))?
                .collect::<rusqlite::Result<_>>()?;
            Ok(tokens)
        })
        .await
    }

    async fn add_node_token(&self, token: &NodeToken) -> Result<()> {
        let token = token.clone();
        let created = stamp(token.created_at)?;
        let revoked = token.revoked_at.map(stamp).transpose()?;
        let disabled = token.disabled_at.map(stamp).transpose()?;
        self.run(move |db| {
            db.execute(
                "INSERT INTO node_tokens (selector, secret_hash, owner, name, created_at, revoked_at, disabled_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    token.selector,
                    token.secret_hash,
                    token.owner.as_str(),
                    token.name,
                    created,
                    revoked,
                    disabled
                ],
            )?;
            Ok(())
        })
        .await
    }

    async fn revoke_node_token(&self, selector: &str) -> Result<bool> {
        let (selector, at) = (selector.to_owned(), stamp(now())?);
        self.run(move |db| {
            let revoked = db.execute(
                "UPDATE node_tokens SET revoked_at = ?2 WHERE selector = ?1 AND revoked_at IS NULL",
                params![selector, at],
            )?;
            Ok(revoked > 0)
        })
        .await
    }

    async fn set_node_disabled(&self, selector: &str, disabled: bool) -> Result<()> {
        let selector = selector.to_owned();
        let at = disabled.then(|| stamp(now())).transpose()?;
        self.run(move |db| {
            db.execute(
                "UPDATE node_tokens SET disabled_at = ?2 WHERE selector = ?1",
                params![selector, at],
            )?;
            Ok(())
        })
        .await
    }

    async fn user_tokens(&self) -> Result<Vec<UserToken>> {
        let principals = self.principals.clone();
        self.run(move |db| {
            let mut query = db.prepare(
                "SELECT selector, secret_hash, owner, name, created_at, revoked_at, scopes
                 FROM user_tokens ORDER BY created_at",
            )?;
            let tokens = query
                .query_map([], |row| user_token_row(&*principals, row))?
                .collect::<rusqlite::Result<_>>()?;
            Ok(tokens)
        })
        .await
    }

    async fn add_user_token(&self, token: &UserToken) -> Result<()> {
        let token = token.clone();
        let created = stamp(token.created_at)?;
        let revoked = token.revoked_at.map(stamp).transpose()?;
        let scopes = Scope::join(&token.scopes);
        self.run(move |db| {
            db.execute(
                "INSERT INTO user_tokens (selector, secret_hash, owner, name, created_at, revoked_at, scopes)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    token.selector,
                    token.secret_hash,
                    token.owner.as_str(),
                    token.name,
                    created,
                    revoked,
                    scopes
                ],
            )?;
            Ok(())
        })
        .await
    }

    async fn revoke_user_token(&self, selector: &str) -> Result<bool> {
        let (selector, at) = (selector.to_owned(), stamp(now())?);
        self.run(move |db| {
            let revoked = db.execute(
                "UPDATE user_tokens SET revoked_at = ?2 WHERE selector = ?1 AND revoked_at IS NULL",
                params![selector, at],
            )?;
            Ok(revoked > 0)
        })
        .await
    }
}
