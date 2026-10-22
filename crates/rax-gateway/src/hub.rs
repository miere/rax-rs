//! The RAX side: one listener for nodes and for clients, told apart by the role they dial as. It
//! introduces itself to nodes, keeps the fleet current, hands clients to the relay and closes the
//! links of nodes and clients whose token or owner loses access.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rax::Open;
use rax::event::BackgroundEvent;
use rax::id::SessionId;
use rax::open::{Subject, UnhandledReason};
use rax::session::{GatewayCapabilities, Initialize, Initialized, PROTOCOL_VERSION};
use rax::{ErrorKind, GatewayCall, GatewayReply, NodeCall, NodeReply, Unhandled};
use rax_tokio::Role;
use rax_tokio::accept::{AcceptConfig, Accepted, AcceptedLinks, Acceptor};

use rax_tokio::gateway::{GatewayLink, LinkEvent, LinkEvents, NewLink};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::access::{Access, Snapshot};
use crate::embed::{Embedding, NodeRef};
use crate::fleet::{Fleet, Node};
use crate::lent::Lent;
use crate::node_access;
use crate::port::Port;
use crate::relay::{self, Relay};
use crate::settings::Settings;
use crate::store::Store;
use crate::transfers::Transfers;

pub const REFRESH: Duration = Duration::from_secs(5);
pub const INITIALIZE_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a link that dropped is held for its node or client to resume. A client's sessions
/// are closed on their nodes once it lapses.
pub const RETAIN_FOR: Duration = Duration::from_secs(300);

/// What the embedder may want to hear about, such as a node that left taking its conversations
/// with it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FleetChange {
    Attached {
        selector: String,
    },
    Gone {
        selector: String,
    },
    /// The node said it is doing something else than it last said.
    Activity {
        selector: String,
    },
}

/// Something a node's session did with no turn open, in a session the relay does not own, for the
/// embedder to show where it opened it: a background task finishing, the answer the agent wrote
/// about it, or a tool one of its sub-agents wants to run.
#[derive(Debug, Clone)]
pub struct Background {
    pub selector: String,
    pub session_id: SessionId,
    pub event: Open<BackgroundEvent>,
}

pub fn capabilities(embedding: &Embedding) -> GatewayCapabilities {
    let mut readable_schemes = embedding.files.schemes();
    readable_schemes.push(relay::SCHEME.into());
    GatewayCapabilities {
        question: true,
        plan: true,
        sign_in: embedding.owner.signs_in(),
        resource_schemes: embedding.settings.resource_schemes.clone(),
        readable_schemes,
        tools: embedding.tools.catalogue(),
        owner_only: true,
    }
}

pub struct Hub {
    /// Serves nodes, which dial as `rax.v1.node`, clients, which dial as `rax.v1.gateway`, and the
    /// HTTP API, all on one port.
    pub server: Port,
    pub changes: mpsc::Receiver<FleetChange>,
    pub backgrounds: mpsc::Receiver<Background>,
}

/// Every collaborator a node's link is served with; bundling them would only move the list.
#[allow(clippy::too_many_arguments)]
pub async fn start(
    listen: SocketAddr,
    retain_for: Duration,
    access: Access,
    fleet: Fleet,
    transfers: Transfers,
    lent: Lent,
    relay: Relay,
    embedding: Embedding,
    http: axum::Router,
    shutdown: CancellationToken,
) -> std::io::Result<Hub> {
    let config = AcceptConfig {
        roles: vec![Role::Node, Role::Gateway],
        retain_for,
        ..AcceptConfig::default()
    };
    let (acceptor, new_links) = Acceptor::unbound(access.clone(), config);
    let server = Port::bind(listen, acceptor, http).await?;
    let (changes, receiver) = mpsc::channel(64);
    let (backgrounds, background_receiver) = mpsc::channel(256);
    let serving = Serving {
        access,
        fleet,
        transfers,
        lent,
        relay,
        embedding,
        changes,
        backgrounds,
    };
    tokio::spawn(accept(new_links, serving, shutdown));
    Ok(Hub {
        server,
        changes: receiver,
        backgrounds: background_receiver,
    })
}

/// Reloads access every `every` and closes any node or client that lost it.
pub async fn refresh(
    store: Arc<dyn Store>,
    settings: Settings,
    access: Access,
    fleet: Fleet,
    relay: Relay,
    every: Duration,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            () = tokio::time::sleep(every) => {}
            () = shutdown.cancelled() => return,
        }
        let snapshot = match Snapshot::load(&*store, &settings).await {
            Ok(snapshot) => snapshot,
            Err(err) => {
                tracing::warn!(error = %err, "could not reload access; keeping the last known rules");
                continue;
            }
        };
        apply(&access, &fleet, &relay, snapshot).await;
    }
}

/// Puts `snapshot` in place and closes the link of every node and client it no longer admits.
/// Whoever replaces the snapshot must close these links: the next refresh finds nothing lost.
pub async fn apply(access: &Access, fleet: &Fleet, relay: &Relay, snapshot: Snapshot) {
    let lost = access.replace(snapshot);
    for selector in lost.nodes {
        if let Some(node) = fleet.get(&selector) {
            tracing::info!(node = %node.name, owner = %node.owner, "access revoked; closing the node's link");
            node.link.close().await;
        }
    }
    relay.close_clients(&lost.clients).await;
}

/// What every node's link is served with.
#[derive(Clone)]
struct Serving {
    access: Access,
    fleet: Fleet,
    transfers: Transfers,
    lent: Lent,
    relay: Relay,
    embedding: Embedding,
    changes: mpsc::Sender<FleetChange>,
    backgrounds: mpsc::Sender<Background>,
}

async fn accept(mut new_links: AcceptedLinks, serving: Serving, shutdown: CancellationToken) {
    loop {
        let new_link = tokio::select! {
            new_link = new_links.recv() => new_link,
            () = shutdown.cancelled() => return,
        };
        match new_link {
            Some(Accepted::Node(NewLink { link, events })) => {
                tokio::spawn(serve(link, events, serving.clone()));
            }
            Some(Accepted::Gateway(client)) => {
                let relay = serving.relay.clone();
                tokio::spawn(async move { relay.serve(client).await });
            }
            None => return,
        }
    }
}

async fn serve(link: GatewayLink, mut events: LinkEvents, serving: Serving) {
    let Serving {
        access,
        fleet,
        transfers,
        lent,
        relay,
        embedding,
        changes,
        backgrounds,
    } = serving;
    let Embedding {
        settings,
        owner: owners,
        files,
        tools,
    } = embedding.clone();
    let selector = link.identity().0.clone();
    let snapshot = access.snapshot();
    let (Some(owner), Some(name)) = (
        snapshot.owner(&selector).cloned(),
        snapshot.node_name(&selector).map(str::to_owned),
    ) else {
        link.close().await;
        return;
    };
    let initialized = match initialize(&link, &embedding).await {
        Ok(initialized) => initialized,
        Err(reason) => {
            tracing::warn!(node = %name, %reason, "node did not initialize; closing its link");
            link.close().await;
            return;
        }
    };
    // Info, not an alert: only the node's owner can fix their configuration, and the node tells
    // them why it stopped.
    let read = match node_access::read(&initialized.metadata, &settings) {
        Ok(read) => read,
        Err(rejection) => {
            tracing::info!(node = %name, %owner, reason = %rejection.message, "rejected the node's metadata; closing its link");
            link.reject(rejection).await;
            return;
        }
    };
    report_ignored(&link, &name, &read.ignored).await;
    let capabilities = initialized.capabilities;
    // Read once: a node whose credential changes ends its link and declares it again.
    let owner_only = capabilities.owner_only;
    let access = read.access.confined(owner_only);
    tracing::info!(node = %name, %owner, tool_gate = ?capabilities.tool_gate, ?access, "node attached");
    let node = Node {
        selector: selector.clone(),
        owner: owner.clone(),
        name: name.clone(),
        link: link.clone(),
        capabilities,
        connected: true,
        access,
    };
    let attachment = fleet.attach(node.clone());
    let _ = changes
        .send(FleetChange::Attached {
            selector: selector.clone(),
        })
        .await;
    while let Some(event) = events.recv().await {
        match event {
            LinkEvent::Closed | LinkEvent::Replaced => break,
            LinkEvent::Disconnected { reason } => {
                tracing::info!(node = %name, %reason, "node disconnected; holding its link for a resume");
                fleet.set_connected(&selector, attachment, false);
            }
            LinkEvent::Resumed => {
                tracing::info!(node = %name, "node resumed");
                fleet.set_connected(&selector, attachment, true);
            }
            LinkEvent::Request {
                id,
                call: NodeCall::ReadResource(read),
            } if read.uri.starts_with(&format!("{}://", relay::SCHEME)) => {
                let (relay, link, name) = (relay.clone(), link.clone(), name.clone());
                tokio::spawn(async move { relay.read(&link, &name, id, read).await });
            }
            LinkEvent::Request {
                id,
                call: NodeCall::ReadResource(read),
            } => {
                let (files, link, selector) = (files.clone(), link.clone(), selector.clone());
                tokio::spawn(async move { files.serve(&link, &selector, id, read).await });
            }
            // A client's session is lent the client's groups, never the embedder's; a session the
            // embedder opened is lent the embedder's alone.
            LinkEvent::Request {
                id,
                call: NodeCall::CallTool(call),
            } if relay.owns(&selector, &call.session_id) => {
                let (relay, link, name) = (relay.clone(), link.clone(), name.clone());
                tokio::spawn(async move { relay.call_tool(&link, &name, id, call).await });
            }
            LinkEvent::Request {
                id,
                call: NodeCall::CallTool(call),
            } => {
                let (tools, link, node) = (tools.clone(), link.clone(), node.clone());
                tokio::spawn(async move { tools.serve(&link, &node, id, call).await });
            }
            LinkEvent::Request {
                id,
                call: NodeCall::CredentialHealth(health),
            } => {
                // Logged, not DMed: the node asks its owner to sign in, and that request is the one
                // message they need.
                if health.degraded {
                    tracing::error!(node = %name, %owner, credential = %health.credential, reason = ?health.reason, since = ?health.since, expires_at = ?health.expires_at, "a node credential is failing");
                } else {
                    tracing::info!(node = %name, credential = %health.credential, "node credential health");
                }
                if let Err(err) = link.reply(id, NodeReply::CredentialHealth).await {
                    tracing::debug!(node = %name, error = %err, "could not answer a node call");
                }
            }
            LinkEvent::Request {
                id,
                call: NodeCall::SignIn(request),
            } => {
                let sent = match owners.ask_sign_in(NodeRef::from(&node), request).await {
                    Ok(()) => link.reply(id, NodeReply::SignIn).await,
                    Err(error) => link.fault(id, error).await,
                };
                if let Err(err) = sent {
                    tracing::debug!(node = %name, error = %err, "could not answer a node call");
                }
            }
            LinkEvent::Request {
                id,
                call: NodeCall::SignInSettled(settled),
            } => {
                owners.sign_in_settled(&selector, settled).await;
                if let Err(err) = link.reply(id, NodeReply::SignInSettled).await {
                    tracing::debug!(node = %name, error = %err, "could not answer a node call");
                }
            }
            LinkEvent::Request {
                id,
                call: NodeCall::UpdateMetadata(update),
            } => match node_access::read(&update.metadata, &settings) {
                Ok(read) => {
                    report_ignored(&link, &name, &read.ignored).await;
                    let access = read.access.confined(owner_only);
                    tracing::info!(node = %name, ?access, "node updated its metadata");
                    fleet.set_access(&selector, attachment, access);
                    if let Err(err) = link.reply(id, NodeReply::UpdateMetadata).await {
                        tracing::debug!(node = %name, error = %err, "could not answer a node call");
                    }
                }
                // The last good policy is not kept: a node whose owner meant something else must
                // not go on serving under what they no longer want.
                Err(rejection) => {
                    tracing::info!(node = %name, %owner, reason = %rejection.message, "rejected the node's metadata; closing its link");
                    let fault = rax::Error::new(ErrorKind::Rejected, rejection.message.clone());
                    if let Err(err) = link.fault(id, fault).await {
                        tracing::debug!(node = %name, error = %err, "could not answer a node call");
                    }
                    link.reject(rejection).await;
                }
            },
            LinkEvent::Request {
                id,
                call: NodeCall::Activity(activity),
            } => {
                if fleet.set_activity(&selector, attachment, activity) {
                    tracing::debug!(node = %name, turns = activity.turns, background_tasks = activity.background_tasks, "node activity");
                    let change = FleetChange::Activity {
                        selector: selector.clone(),
                    };
                    // Never waited for: it only redraws a row, and a busy node sends many.
                    if changes.try_send(change).is_err() {
                        tracing::debug!(node = %name, "nobody to tell of a node's activity");
                    }
                }
                if let Err(err) = link.reply(id, NodeReply::Activity).await {
                    tracing::debug!(node = %name, error = %err, "could not answer a node call");
                }
            }
            LinkEvent::Background { session_id, event } if relay.owns(&selector, &session_id) => {
                if let Some(node) = fleet.get(&selector) {
                    relay.background(&node, &session_id, event).await;
                }
            }
            LinkEvent::Background { session_id, event } => {
                let background = Background {
                    selector: selector.clone(),
                    session_id,
                    event,
                };
                if backgrounds.send(background).await.is_err() {
                    tracing::debug!(node = %name, "nobody to show a background event");
                }
            }
            LinkEvent::Attachment { transfer_id, bytes } => {
                transfers.arrived(&selector, transfer_id, Ok(bytes));
            }
            LinkEvent::TransferFailed {
                transfer_id,
                reason,
            } => {
                tracing::debug!(node = %name, %transfer_id, %reason, "transfer failed");
                transfers.arrived(&selector, transfer_id, Err(reason));
            }
            LinkEvent::Unhandled { body, .. } => {
                tracing::warn!(node = %name, subject = ?body.subject, reason = ?body.reason, "node could not handle something the gateway sent");
            }
        }
    }
    if fleet.detach(&selector, attachment) {
        lent.detached(&selector);
        tracing::info!(node = %name, "node detached");
        let _ = changes.send(FleetChange::Gone { selector }).await;
    }
}

/// Tells the node, and so its owner, which keys this gateway did not read. Nobody on this side
/// needs to hear about another gateway's settings.
async fn report_ignored(link: &GatewayLink, name: &str, keys: &[String]) {
    for key in keys {
        let body = Unhandled {
            subject: Subject::MetadataKey { key: key.clone() },
            reason: UnhandledReason::UnsupportedType,
            message: Some(format!("this gateway ignores `{key}`")),
        };
        if let Err(err) = link.unhandled(None, body).await {
            tracing::debug!(node = %name, error = %err, "could not report an ignored metadata key");
        }
    }
}

async fn initialize(link: &GatewayLink, embedding: &Embedding) -> Result<Initialized, String> {
    let offer = GatewayCall::Initialize(Initialize {
        protocol_version: PROTOCOL_VERSION,
        capabilities: capabilities(embedding),
    });
    let pending = link.call(offer).await.map_err(|err| err.to_string())?;
    let reply = tokio::time::timeout(INITIALIZE_TIMEOUT, pending.reply)
        .await
        .map_err(|_| "no answer to initialize".to_owned())?
        .map_err(|err| err.to_string())?;
    match reply {
        GatewayReply::Initialize(initialized)
            if initialized.protocol_version == PROTOCOL_VERSION =>
        {
            Ok(initialized)
        }
        GatewayReply::Initialize(initialized) => Err(format!(
            "the node speaks RAX {}, this gateway speaks {PROTOCOL_VERSION}",
            initialized.protocol_version
        )),
        other => Err(format!("answered initialize with {other:?}")),
    }
}
