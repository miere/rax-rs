//! A node's attachments arrive twice: as transfer bytes on the link, and separately as an
//! `attachment` event on the turn. Whichever comes first waits here for the other.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use rax::id::TransferId;
use tokio::sync::oneshot;
use tokio::time::Instant;

pub type Delivered = Result<Vec<u8>, String>;

/// Bytes whose event has not come yet, or an event waiting for its bytes.
enum Transfer {
    Arrived(Delivered, Instant),
    Awaited(oneshot::Sender<Delivered>),
}

/// Bytes nobody claims within this are dropped, so a node cannot pile up memory.
const UNCLAIMED: Duration = Duration::from_secs(600);

/// Cheap to clone; every clone holds the same transfers.
#[derive(Clone, Default)]
pub struct Transfers {
    transfers: Arc<Mutex<HashMap<(String, TransferId), Transfer>>>,
}

impl Transfers {
    fn transfers(&self) -> MutexGuard<'_, HashMap<(String, TransferId), Transfer>> {
        self.transfers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The link finished (or failed) a transfer from `node`.
    pub fn arrived(&self, node: &str, transfer_id: TransferId, delivered: Delivered) {
        let mut transfers = self.transfers();
        transfers.retain(|_, t| !matches!(t, Transfer::Arrived(_, at) if at.elapsed() > UNCLAIMED));
        let key = (node.to_owned(), transfer_id);
        match transfers.remove(&key) {
            Some(Transfer::Awaited(waiter)) => {
                let _ = waiter.send(delivered);
            }
            _ => {
                transfers.insert(key, Transfer::Arrived(delivered, Instant::now()));
            }
        }
    }

    /// The bytes an `attachment` event names, waiting up to `wait` for them to arrive.
    pub async fn claim(&self, node: &str, transfer_id: &TransferId, wait: Duration) -> Delivered {
        let key = (node.to_owned(), transfer_id.clone());
        let waiting = {
            let mut transfers = self.transfers();
            match transfers.remove(&key) {
                Some(Transfer::Arrived(delivered, _)) => return delivered,
                _ => {
                    let (waiter, waiting) = oneshot::channel();
                    transfers.insert(key.clone(), Transfer::Awaited(waiter));
                    waiting
                }
            }
        };
        match tokio::time::timeout(wait, waiting).await {
            Ok(Ok(delivered)) => delivered,
            _ => {
                self.transfers().remove(&key);
                Err(format!(
                    "its bytes did not arrive within {}s",
                    wait.as_secs()
                ))
            }
        }
    }
}
