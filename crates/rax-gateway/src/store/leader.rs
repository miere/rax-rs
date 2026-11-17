#[cfg(feature = "sqlite")]
use std::path::PathBuf;
#[cfg(feature = "sqlite")]
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
#[cfg(feature = "firestore")]
use serde_json::{Value, json};

use super::Result;
#[cfg(any(feature = "sqlite", feature = "firestore"))]
use super::StoreError;
#[cfg(feature = "firestore")]
use super::firestore::{FirestoreStore, Write, string_value};
#[cfg(feature = "sqlite")]
use super::sqlite::{LeaderLock, SqliteStore};

/// Proof of leadership until the next renewal. `version` is whatever the backend needs to tell a
/// renewal of this lease apart from someone else's takeover.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    pub holder: String,
    pub version: String,
}

/// One gateway serves at a time: only the holder serves nodes, and whatever else its embedder runs.
#[async_trait]
pub trait Leader: Send + Sync + 'static {
    /// How long a lease outlives its last renewal; renew well within it.
    fn ttl(&self) -> Duration;
    /// `None` means someone else leads right now.
    async fn acquire(&self, holder: &str) -> Result<Option<Lease>>;
    /// `None` means the lease was lost, and the caller must stop serving at once.
    async fn renew(&self, lease: &Lease) -> Result<Option<Lease>>;
    async fn release(&self, lease: Lease) -> Result<()>;
}

/// The OS file lock: one gateway per machine, released by the kernel if the process dies.
#[cfg(feature = "sqlite")]
pub struct SqliteLeader {
    path: PathBuf,
    held: Mutex<Option<LeaderLock>>,
}

#[cfg(feature = "sqlite")]
impl SqliteLeader {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            held: Mutex::new(None),
        }
    }

    fn held(&self) -> std::sync::MutexGuard<'_, Option<LeaderLock>> {
        self.held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(feature = "sqlite")]
#[async_trait]
impl Leader for SqliteLeader {
    fn ttl(&self) -> Duration {
        Duration::from_secs(30)
    }

    async fn acquire(&self, holder: &str) -> Result<Option<Lease>> {
        let lease = Lease {
            holder: holder.to_owned(),
            version: "flock".to_owned(),
        };
        let mut held = self.held();
        if held.is_some() {
            return Ok(Some(lease));
        }
        match SqliteStore::lock_leader(&self.path) {
            Ok(lock) => {
                *held = Some(lock);
                Ok(Some(lease))
            }
            Err(StoreError::Locked(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    async fn renew(&self, lease: &Lease) -> Result<Option<Lease>> {
        Ok(self.held().is_some().then(|| lease.clone()))
    }

    async fn release(&self, _lease: Lease) -> Result<()> {
        self.held().take();
        Ok(())
    }
}

#[cfg(feature = "firestore")]
const LEASE: &str = "leases/leader";

/// A renewed lease on one document, `leases/leader` under the store's root. Every write is guarded
/// by the update time of the read that justified it, so of two gateways racing for an expired
/// lease exactly one wins.
#[cfg(feature = "firestore")]
pub struct FirestoreLeader {
    store: FirestoreStore,
    ttl: Duration,
}

#[cfg(feature = "firestore")]
impl FirestoreLeader {
    pub fn new(store: FirestoreStore, ttl: Duration) -> Self {
        Self { store, ttl }
    }

    fn fields(&self, holder: &str) -> Value {
        json!({
            "holder": string_value(holder),
            "lease_seconds": {"integerValue": self.ttl.as_secs().max(1).to_string()},
            "released": {"booleanValue": false},
        })
    }

    async fn write(&self, holder: &str, guarded: Write) -> Result<Option<Lease>> {
        let write = guarded
            .transforms(json!([{"fieldPath": "acquired_at", "setToServerValue": "REQUEST_TIME"}]));
        match self.store.commit(vec![write]).await {
            Ok(result) => {
                let version = result["writeResults"][0]["updateTime"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned();
                Ok(Some(Lease {
                    holder: holder.to_owned(),
                    version,
                }))
            }
            Err(StoreError::Conflict(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

#[cfg(feature = "firestore")]
#[async_trait]
impl Leader for FirestoreLeader {
    fn ttl(&self) -> Duration {
        self.ttl
    }

    async fn acquire(&self, holder: &str) -> Result<Option<Lease>> {
        let (current, read_time) = self.store.read_with_time(LEASE).await?;
        let update = self.store.update(LEASE, self.fields(holder));
        let Some(doc) = current else {
            return self.write(holder, update.if_absent()).await;
        };
        let held_by_me = doc.string("holder").ok().as_deref() == Some(holder);
        let expired = doc.bool("released")
            || match (doc.time("acquired_at")?, doc.integer("lease_seconds")) {
                (Some(acquired), Some(seconds)) => {
                    read_time >= acquired + time::Duration::seconds(seconds)
                }
                _ => true,
            };
        if held_by_me || expired {
            self.write(holder, update.if_unchanged(doc.update_time()))
                .await
        } else {
            Ok(None)
        }
    }

    async fn renew(&self, lease: &Lease) -> Result<Option<Lease>> {
        let update = self.store.update(LEASE, self.fields(&lease.holder));
        self.write(&lease.holder, update.if_unchanged(&lease.version))
            .await
    }

    async fn release(&self, lease: Lease) -> Result<()> {
        let write = self
            .store
            .update(LEASE, json!({"released": {"booleanValue": true}}))
            .mask(["released"])
            .if_unchanged(&lease.version);
        match self.store.commit(vec![write]).await {
            Ok(_) | Err(StoreError::Conflict(_)) => Ok(()),
            Err(err) => Err(err),
        }
    }
}
