//! Which tool groups each session was opened with, so a call for any other is refused whatever the
//! node sends. Sessions are recorded when they open. One this process did not open — a node that
//! stayed up across a gateway restart — is unknown here, unless the embedder can [`Recall`] it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;
use rax::id::SessionId;
use rax::tool::ToolGroup;

/// What an embedder that keeps its sessions across restarts knows about one it opened before.
#[async_trait]
pub trait Recall: Send + Sync + 'static {
    /// The groups a session this process did not open was lent, or nothing.
    async fn groups(&self, selector: &str, session: &SessionId) -> Vec<String>;
}

#[derive(Clone, Default)]
pub struct Lent {
    recall: Option<Arc<dyn Recall>>,
    sessions: Arc<Mutex<HashMap<NodeSession, Vec<String>>>>,
}

/// A session id is the node's own, so it is only unique beside the node's selector.
type NodeSession = (String, SessionId);

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Lent {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn recalling(recall: Arc<dyn Recall>) -> Self {
        Self {
            recall: Some(recall),
            sessions: Arc::default(),
        }
    }

    pub fn opened(&self, selector: &str, session: &SessionId, groups: &[ToolGroup]) {
        let namespaces = groups.iter().map(|group| group.namespace.clone()).collect();
        lock(&self.sessions).insert((selector.to_owned(), session.clone()), namespaces);
    }

    /// Forgets every session of a node whose link ended. A durable session that comes back is
    /// found through [`Recall`].
    pub fn detached(&self, selector: &str) {
        lock(&self.sessions).retain(|(node, _), _| node != selector);
    }

    pub async fn groups(&self, selector: &str, session: &SessionId) -> Vec<String> {
        let key = (selector.to_owned(), session.clone());
        if let Some(groups) = lock(&self.sessions).get(&key) {
            return groups.clone();
        }
        match &self.recall {
            Some(recall) => recall.groups(selector, session).await,
            None => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Pinned;

    #[async_trait]
    impl Recall for Pinned {
        async fn groups(&self, selector: &str, session: &SessionId) -> Vec<String> {
            match (selector, session.0.as_str()) {
                ("node-a", "s1") => vec!["chat".to_owned()],
                _ => Vec::new(),
            }
        }
    }

    #[tokio::test]
    async fn what_this_process_opened_wins_and_goes_with_the_node() {
        let lent = Lent::recalling(Arc::new(Pinned));
        let s1 = SessionId("s1".into());
        assert_eq!(lent.groups("node-a", &s1).await, ["chat"]);
        assert!(lent.groups("node-b", &s1).await.is_empty());

        lent.opened("node-a", &s1, &[]);
        assert!(lent.groups("node-a", &s1).await.is_empty());
        lent.detached("node-a");
        assert_eq!(lent.groups("node-a", &s1).await, ["chat"]);
        assert!(Lent::new().groups("node-a", &s1).await.is_empty());
    }
}
