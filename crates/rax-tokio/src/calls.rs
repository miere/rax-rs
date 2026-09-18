use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use rax::id::RequestId;
use tokio::sync::oneshot;

use crate::connection::{Ended, lock};
use crate::error::CallError;

pub(crate) type Settle<R> = oneshot::Sender<Result<R, CallError>>;

pub(crate) struct Calls<R> {
    prefix: &'static str,
    next: AtomicU64,
    pending: Mutex<HashMap<RequestId, Settle<R>>>,
}

impl<R> Calls<R> {
    pub(crate) fn new(prefix: &'static str) -> Self {
        Self {
            prefix,
            next: AtomicU64::new(1),
            pending: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn register(&self) -> (RequestId, oneshot::Receiver<Result<R, CallError>>) {
        let n = self.next.fetch_add(1, Ordering::Relaxed);
        let id = RequestId(format!("{}{n}", self.prefix));
        let (settle, reply) = oneshot::channel();
        lock(&self.pending).insert(id.clone(), settle);
        (id, reply)
    }

    pub(crate) fn settle(&self, id: &RequestId, result: Result<R, CallError>) -> bool {
        match lock(&self.pending).remove(id) {
            Some(settle) => {
                let _ = settle.send(result);
                true
            }
            None => false,
        }
    }

    pub(crate) fn forget(&self, id: &RequestId) {
        lock(&self.pending).remove(id);
    }

    pub(crate) fn fail_all(&self, ended: Ended) {
        let pending = std::mem::take(&mut *lock(&self.pending));
        for settle in pending.into_values() {
            let _ = settle.send(Err(ended.call_error()));
        }
    }
}
