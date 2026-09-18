use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tokio::sync::Notify;

use crate::error::SimError;

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

struct Ending {
    reason: String,
    clean: bool,
}

struct State<T> {
    queue: VecDeque<T>,
    arrived: Vec<String>,
    ending: Option<Ending>,
}

pub(crate) struct Inbox<T> {
    state: Mutex<State<T>>,
    signal: Notify,
}

impl<T: Debug> Inbox<T> {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(State {
                queue: VecDeque::new(),
                arrived: Vec::new(),
                ending: None,
            }),
            signal: Notify::new(),
        }
    }

    pub(crate) fn push(&self, item: T) {
        let mut state = lock(&self.state);
        state.arrived.push(format!("{item:?}"));
        state.queue.push_back(item);
        drop(state);
        self.signal.notify_waiters();
    }

    pub(crate) fn end(&self, reason: impl Into<String>, clean: bool) {
        let mut state = lock(&self.state);
        if state.ending.is_none() {
            state.ending = Some(Ending {
                reason: reason.into(),
                clean,
            });
        }
        drop(state);
        self.signal.notify_waiters();
    }

    pub(crate) fn ended_cleanly(&self) -> bool {
        lock(&self.state)
            .ending
            .as_ref()
            .is_some_and(|ending| ending.clean)
    }

    pub(crate) async fn next(&self, wait: Duration, waiting_for: &str) -> Result<T, SimError> {
        self.take(wait, waiting_for, |_| true).await
    }

    pub(crate) async fn take(
        &self,
        wait: Duration,
        waiting_for: &str,
        wanted: impl Fn(&T) -> bool,
    ) -> Result<T, SimError> {
        let found = tokio::time::timeout(wait, async {
            loop {
                let signal = self.signal.notified();
                tokio::pin!(signal);
                signal.as_mut().enable();
                {
                    let mut state = lock(&self.state);
                    if let Some(index) = state.queue.iter().position(&wanted)
                        && let Some(item) = state.queue.remove(index)
                    {
                        return Ok(item);
                    }
                    if let Some(ending) = &state.ending {
                        return Err(ending.reason.clone());
                    }
                }
                signal.await;
            }
        })
        .await;
        let arrived = || lock(&self.state).arrived.clone();
        match found {
            Ok(Ok(item)) => Ok(item),
            Ok(Err(reason)) => Err(SimError::Ended {
                waiting_for: waiting_for.to_owned(),
                reason,
                arrived: arrived(),
            }),
            Err(_) => Err(SimError::Timeout {
                waiting_for: waiting_for.to_owned(),
                after: wait,
                arrived: arrived(),
            }),
        }
    }
}
