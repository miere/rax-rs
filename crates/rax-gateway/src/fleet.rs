//! The nodes attached right now, and which one a new conversation goes to.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use rax::Activity;
use rax::session::NodeCapabilities;
use rax_tokio::gateway::GatewayLink;
use time::OffsetDateTime;

use crate::access::Snapshot;
use crate::node_access::NodeAccess;
use crate::store::UserId;

#[derive(Clone)]
pub struct Node {
    pub selector: String,
    pub owner: UserId,
    pub name: String,
    pub link: GatewayLink,
    pub capabilities: NodeCapabilities,
    /// False while the socket is down and RAX holds the link for a resume. Such a node takes no
    /// new work: a turn sent to it would wait for a socket that may never come back.
    pub connected: bool,
    /// Who its owner lets in, as the node last declared it.
    pub access: NodeAccess,
}

impl Node {
    pub fn admits(&self, user: &UserId) -> bool {
        self.access.admits(&self.owner, user)
    }
}

/// What a node last said it was doing. Only the node knows: it may serve other gateways, and run
/// work no turn of this one covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reported {
    pub activity: Activity,
    /// When it last went from working to idle or back, as this gateway heard it.
    pub since: OffsetDateTime,
}

struct Entry {
    node: Node,
    attachment: u64,
    sessions: usize,
    /// `None` until the node reports, and for good on a node too old to.
    activity: Option<Reported>,
}

#[derive(Clone, Default)]
pub struct Fleet {
    nodes: Arc<Mutex<HashMap<String, Entry>>>,
    attachments: Arc<AtomicU64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Summary {
    pub selector: String,
    pub owner: UserId,
    pub name: String,
    pub sessions: usize,
    pub connected: bool,
    /// Who its owner lets in, as the node last declared it.
    pub access: NodeAccess,
    pub activity: Option<Reported>,
}

impl Fleet {
    fn nodes(&self) -> MutexGuard<'_, HashMap<String, Entry>> {
        self.nodes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The returned attachment detaches only this link, so a late close of a node's old link
    /// cannot detach its new one.
    pub fn attach(&self, node: Node) -> u64 {
        let attachment = self.attachments.fetch_add(1, Ordering::Relaxed);
        let entry = Entry {
            node,
            attachment,
            sessions: 0,
            activity: None,
        };
        self.nodes().insert(entry.node.selector.clone(), entry);
        attachment
    }

    pub fn detach(&self, selector: &str, attachment: u64) -> bool {
        let mut nodes = self.nodes();
        let current = nodes
            .get(selector)
            .is_some_and(|entry| entry.attachment == attachment);
        if current {
            nodes.remove(selector);
        }
        current
    }

    pub fn set_connected(&self, selector: &str, attachment: u64, connected: bool) {
        if let Some(entry) = self.nodes().get_mut(selector)
            && entry.attachment == attachment
        {
            entry.node.connected = connected;
        }
    }

    /// Only for the link it was declared on, so an update racing a reconnect cannot land on the
    /// node's next link.
    pub fn set_access(&self, selector: &str, attachment: u64, access: NodeAccess) {
        if let Some(entry) = self.nodes().get_mut(selector)
            && entry.attachment == attachment
        {
            entry.node.access = access;
        }
    }

    /// True when the report differs from the last, so the node's row needs drawing again. Only
    /// for the link it came on, as with `set_access`.
    pub fn set_activity(&self, selector: &str, attachment: u64, activity: Activity) -> bool {
        let mut nodes = self.nodes();
        let Some(entry) = nodes
            .get_mut(selector)
            .filter(|entry| entry.attachment == attachment)
        else {
            return false;
        };
        let since = match entry.activity {
            Some(last) if last.activity == activity => return false,
            Some(last) if last.activity.is_idle() == activity.is_idle() => last.since,
            _ => OffsetDateTime::now_utc(),
        };
        entry.activity = Some(Reported { activity, since });
        true
    }

    pub fn get(&self, selector: &str) -> Option<Node> {
        self.nodes().get(selector).map(|entry| entry.node.clone())
    }

    /// A person's own nodes when any is attached, otherwise anyone's that lets them in; never a
    /// mix. Least live sessions wins, and the choice counts as a session at once.
    pub fn assign(&self, user: &UserId, access: &Snapshot) -> Option<Node> {
        let mut nodes = self.nodes();
        let selector = serving(user, access, &nodes)
            .min_by(|a, b| {
                a.sessions
                    .cmp(&b.sessions)
                    .then_with(|| a.node.selector.cmp(&b.node.selector))
            })
            .map(|entry| entry.node.selector.clone())?;
        let entry = nodes.get_mut(&selector)?;
        entry.sessions += 1;
        Some(entry.node.clone())
    }

    /// [`Self::assign`], among the nodes that also pass `fits`, such as those that take tool
    /// groups.
    pub fn assign_where(
        &self,
        user: &UserId,
        access: &Snapshot,
        fits: impl Fn(&Node) -> bool,
    ) -> Option<Node> {
        let mut nodes = self.nodes();
        let selector = serving(user, access, &nodes)
            .filter(|entry| fits(&entry.node))
            .min_by(|a, b| {
                a.sessions
                    .cmp(&b.sessions)
                    .then_with(|| a.node.selector.cmp(&b.node.selector))
            })
            .map(|entry| entry.node.selector.clone())?;
        let entry = nodes.get_mut(&selector)?;
        entry.sessions += 1;
        Some(entry.node.clone())
    }

    /// The nodes a person may move a thread onto, by name. Drawn from the same set `assign` picks
    /// from, so a picker cannot offer a machine that assignment would refuse.
    pub fn choices(&self, user: &UserId, access: &Snapshot) -> Vec<Summary> {
        let nodes = self.nodes();
        let mut choices: Vec<Summary> = serving(user, access, &nodes)
            .map(|entry| Summary {
                selector: entry.node.selector.clone(),
                owner: entry.node.owner.clone(),
                name: entry.node.name.clone(),
                sessions: entry.sessions,
                connected: entry.node.connected,
                access: entry.node.access.clone(),
                activity: entry.activity,
            })
            .collect();
        choices.sort_by(|a, b| a.name.cmp(&b.name));
        choices
    }

    /// True when there are machines this person would be given but none of their owners lets
    /// them in, which is a refusal rather than an outage.
    pub fn shuts_out(&self, user: &UserId, access: &Snapshot) -> bool {
        let nodes = self.nodes();
        let mut offered = candidates(user, access, &nodes).peekable();
        offered.peek().is_some() && !offered.any(|entry| entry.node.admits(user))
    }

    pub fn session_ended(&self, selector: &str) {
        if let Some(entry) = self.nodes().get_mut(selector) {
            entry.sessions = entry.sessions.saturating_sub(1);
        }
    }

    pub fn session_resumed(&self, selector: &str) {
        if let Some(entry) = self.nodes().get_mut(selector) {
            entry.sessions += 1;
        }
    }

    pub fn summaries(&self) -> Vec<Summary> {
        let mut summaries: Vec<Summary> = self
            .nodes()
            .values()
            .map(|entry| Summary {
                selector: entry.node.selector.clone(),
                owner: entry.node.owner.clone(),
                name: entry.node.name.clone(),
                sessions: entry.sessions,
                connected: entry.node.connected,
                access: entry.node.access.clone(),
                activity: entry.activity,
            })
            .collect();
        summaries.sort_by(|a, b| a.name.cmp(&b.name));
        summaries
    }
}

/// Of the machines that would take this person on, those whose owners let them in.
fn serving<'a>(
    user: &UserId,
    access: &Snapshot,
    nodes: &'a HashMap<String, Entry>,
) -> impl Iterator<Item = &'a Entry> {
    candidates(user, access, nodes).filter(|entry| entry.node.admits(user))
}

/// Whether or not their owners let this person in. Own nodes shadow everyone else's entirely:
/// somebody with a machine of their own never lands on another person's, so neither may the
/// picker put them there.
fn candidates<'a>(
    user: &UserId,
    access: &Snapshot,
    nodes: &'a HashMap<String, Entry>,
) -> impl Iterator<Item = &'a Entry> {
    let own_live = nodes
        .values()
        .any(|entry| entry.node.connected && &entry.node.owner == user);
    nodes.values().filter(move |entry| {
        entry.node.connected
            && (!own_live || &entry.node.owner == user)
            && access.is_enabled(&entry.node.selector)
    })
}
