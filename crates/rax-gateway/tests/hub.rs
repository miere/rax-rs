#![cfg(feature = "sqlite")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::time::Duration;

use rax::session::{Initialized, NodeCapabilities, ToolGate};
use rax::{Activity, GatewayCall, GatewayReply, NodeCall, NodeReply};
use rax_gateway::access::{Access, Snapshot};
use rax_gateway::fleet::Fleet;
use rax_gateway::hub::{self, FleetChange};
use rax_gateway::lent::Lent;
use rax_gateway::relay::Relay;
use rax_gateway::store::{NodeToken, SqliteStore, Store, UserId};
use rax_gateway::transfers::Transfers;
use rax_gateway::{Embedding, Settings, token};
use rax_tokio::node::{NodeConfig, NodeEvent, NodeEvents, NodeHandle, NodeLink};
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;

fn user(raw: &str) -> UserId {
    UserId::parse(raw).unwrap()
}

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(20), future)
        .await
        .expect("timed out")
}

struct Rig {
    _dir: tempfile::TempDir,
    store: Arc<dyn Store>,
    access: Access,
    fleet: Fleet,
    relay: Relay,
    hub: hub::Hub,
    shutdown: CancellationToken,
}

async fn rig() -> Rig {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn Store> = Arc::new(SqliteStore::open(&dir.path().join("config.db")).unwrap());
    store.set_admin(&user("U0ADMIN01")).await.unwrap();
    store
        .approve(&user("U0PERSON1"), &user("U0ADMIN01"))
        .await
        .unwrap();
    let access = Access::reloading(
        Snapshot::load(&*store, &Settings::default()).await.unwrap(),
        store.clone(),
        Settings::default(),
    );
    let fleet = Fleet::default();
    let shutdown = CancellationToken::new();
    let (lent, transfers, embedding) = (Lent::new(), Transfers::default(), Embedding::default());
    let relay = Relay::new(
        access.clone(),
        fleet.clone(),
        store.clone(),
        embedding.settings.clone(),
        lent.clone(),
        transfers.clone(),
        embedding.owner.clone(),
    );
    let hub = hub::start(
        "127.0.0.1:0".parse().unwrap(),
        hub::RETAIN_FOR,
        access.clone(),
        fleet.clone(),
        transfers,
        lent,
        relay.clone(),
        embedding,
        axum::Router::new(),
        shutdown.clone(),
    )
    .await
    .unwrap();
    Rig {
        _dir: dir,
        store,
        access,
        fleet,
        relay,
        hub,
        shutdown,
    }
}

impl Rig {
    async fn mint(&self, owner: &str) -> (String, String) {
        let minted = self.mint_quietly(owner).await;
        self.access.replace(
            Snapshot::load(&*self.store, &Settings::default())
                .await
                .unwrap(),
        );
        minted
    }

    /// Only the store hears about it, as when the CLI mints while the gateway runs.
    async fn mint_quietly(&self, owner: &str) -> (String, String) {
        let minted = token::mint(&Settings::default().node_prefix);
        self.store
            .add_node_token(&NodeToken {
                selector: minted.selector.clone(),
                secret_hash: minted.secret_hash,
                owner: user(owner),
                name: "laptop".into(),
                created_at: OffsetDateTime::now_utc(),
                revoked_at: None,
                disabled_at: None,
            })
            .await
            .unwrap();
        (minted.selector, minted.token)
    }

    fn dial(&self, token: &str) -> (NodeHandle, NodeEvents) {
        NodeLink::start(NodeConfig {
            endpoints: vec![format!("ws://{}", self.hub.server.local_addr())],
            token: token.to_owned(),
            backoff_min: Duration::from_millis(10),
            backoff_max: Duration::from_millis(50),
            ..Default::default()
        })
        .unwrap()
    }
}

async fn answer_initialize(node: &NodeHandle, events: &mut NodeEvents) {
    loop {
        match within(events.recv()).await.unwrap() {
            NodeEvent::Request {
                id,
                call: GatewayCall::Initialize(offer),
            } => {
                assert_eq!(offer.capabilities.readable_schemes, ["bridge"]);
                assert!(
                    !offer.capabilities.sign_in,
                    "nobody here can show a sign-in"
                );
                let reply = GatewayReply::Initialize(Initialized {
                    protocol_version: rax::PROTOCOL_VERSION,
                    capabilities: NodeCapabilities {
                        tool_gate: ToolGate::EveryCall,
                        ..Default::default()
                    },
                    metadata: Default::default(),
                });
                node.reply(id, reply).await.unwrap();
                return;
            }
            NodeEvent::Fresh => {}
            other => panic!("expected initialize, got {other:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_granted_node_attaches_and_is_assigned_to_its_owner() {
    let mut rig = rig().await;
    let (selector, token) = rig.mint("U0PERSON1").await;
    let (node, mut events) = rig.dial(&token);
    answer_initialize(&node, &mut events).await;

    assert_eq!(
        within(rig.hub.changes.recv()).await.unwrap(),
        FleetChange::Attached {
            selector: selector.clone()
        }
    );
    let snapshot = rig.access.snapshot();
    let assigned = rig.fleet.assign(&user("U0PERSON1"), &snapshot).unwrap();
    assert_eq!(assigned.selector, selector);
    assert_eq!(assigned.capabilities.tool_gate, ToolGate::EveryCall);
    assert!(rig.fleet.assign(&user("U0GUEST01"), &snapshot).is_none());
    node.close().await;
    rig.shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_revoked_grant_disconnects_the_node_and_its_redial_is_refused() {
    let mut rig = rig().await;
    let (selector, token) = rig.mint("U0PERSON1").await;
    let (node, mut events) = rig.dial(&token);
    answer_initialize(&node, &mut events).await;
    within(rig.hub.changes.recv()).await.unwrap();
    tokio::spawn(hub::refresh(
        rig.store.clone(),
        Settings::default(),
        rig.access.clone(),
        rig.fleet.clone(),
        rig.relay.clone(),
        Duration::from_millis(50),
        rig.shutdown.clone(),
    ));

    rig.store.revoke(&user("U0PERSON1")).await.unwrap();
    assert_eq!(
        within(rig.hub.changes.recv()).await.unwrap(),
        FleetChange::Gone { selector }
    );
    loop {
        match within(events.recv()).await {
            Some(NodeEvent::CredentialRejected) => break,
            Some(_) => {}
            None => panic!("node events ended before the credential was rejected"),
        }
    }
    rig.shutdown.cancel();
}

async fn report(node: &NodeHandle, turns: u32, background_tasks: u32) {
    let activity = Activity {
        turns,
        background_tasks,
    };
    let answered = within(node.call(NodeCall::Activity(activity))).await;
    assert!(matches!(answered, Ok(NodeReply::Activity)), "{answered:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_says_what_it_is_doing_and_only_a_change_is_passed_on() {
    let mut rig = rig().await;
    let (selector, token) = rig.mint("U0PERSON1").await;
    let (node, mut events) = rig.dial(&token);
    answer_initialize(&node, &mut events).await;
    within(rig.hub.changes.recv()).await.unwrap();
    // A node that has said nothing is not known to be idle.
    assert_eq!(rig.fleet.summaries()[0].activity, None);
    let changed = FleetChange::Activity {
        selector: selector.clone(),
    };

    report(&node, 0, 0).await;
    assert_eq!(within(rig.hub.changes.recv()).await.unwrap(), changed);
    let idle = rig.fleet.summaries()[0].activity.unwrap();
    assert!(idle.activity.is_idle());

    report(&node, 1, 0).await;
    assert_eq!(within(rig.hub.changes.recv()).await.unwrap(), changed);
    let working = rig.fleet.summaries()[0].activity.unwrap();
    assert_eq!(working.activity.turns, 1);
    assert!(working.since >= idle.since);

    // Still working: the counts move, and the moment it started working does not.
    report(&node, 1, 2).await;
    assert_eq!(within(rig.hub.changes.recv()).await.unwrap(), changed);
    let busier = rig.fleet.summaries()[0].activity.unwrap();
    assert_eq!(busier.activity.background_tasks, 2);
    assert_eq!(busier.since, working.since);

    report(&node, 1, 2).await;
    let quiet = tokio::time::timeout(Duration::from_millis(300), rig.hub.changes.recv()).await;
    assert!(
        quiet.is_err(),
        "a report that changed nothing was passed on"
    );
    node.close().await;
    rig.shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_forged_token_never_gets_a_link() {
    let rig = rig().await;
    let (_, token) = rig.mint("U0PERSON1").await;
    let (_node, mut events) = rig.dial(&format!("{token}forged"));
    assert!(matches!(
        within(events.recv()).await,
        Some(NodeEvent::CredentialRejected)
    ));
    rig.shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_started_right_after_its_token_is_minted_is_admitted() {
    let mut rig = rig().await;
    let (selector, token) = rig.mint_quietly("U0PERSON1").await;
    let (node, mut events) = rig.dial(&token);
    answer_initialize(&node, &mut events).await;
    assert_eq!(
        within(rig.hub.changes.recv()).await.unwrap(),
        FleetChange::Attached { selector }
    );
    node.close().await;
    rig.shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn two_nodes_minted_and_started_within_a_second_are_both_admitted() {
    let mut rig = rig().await;
    let (first, token) = rig.mint_quietly("U0PERSON1").await;
    let (laptop, mut laptop_events) = rig.dial(&token);
    answer_initialize(&laptop, &mut laptop_events).await;
    assert_eq!(
        within(rig.hub.changes.recv()).await.unwrap(),
        FleetChange::Attached { selector: first }
    );

    let (second, token) = rig.mint_quietly("U0PERSON1").await;
    let (desktop, mut desktop_events) = rig.dial(&token);
    answer_initialize(&desktop, &mut desktop_events).await;
    assert_eq!(
        within(rig.hub.changes.recv()).await.unwrap(),
        FleetChange::Attached { selector: second }
    );
    laptop.close().await;
    desktop.close().await;
    rig.shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_token_revoked_before_it_first_dials_is_still_refused() {
    let rig = rig().await;
    let (selector, token) = rig.mint_quietly("U0PERSON1").await;
    rig.store.revoke_node_token(&selector).await.unwrap();
    let (_node, mut events) = rig.dial(&token);
    assert!(matches!(
        within(events.recv()).await,
        Some(NodeEvent::CredentialRejected)
    ));
    rig.shutdown.cancel();
}

async fn http_get(addr: std::net::SocketAddr, path: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let request = format!("GET {path} HTTP/1.1\r\nhost: gateway\r\nconnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    within(stream.read_to_string(&mut response)).await.unwrap();
    response
}

#[tokio::test(flavor = "multi_thread")]
async fn rax_links_and_http_share_one_port() {
    let (acceptor, mut links) = rax_tokio::accept::Acceptor::unbound(
        |_: &str| Some(rax_tokio::gateway::NodeIdentity("node-1".into())),
        rax_tokio::accept::AcceptConfig::default(),
    );
    let http = axum::Router::new().route("/ping", axum::routing::get(|| async { "pong" }));
    let port = rax_gateway::port::Port::bind("127.0.0.1:0".parse().unwrap(), acceptor, http)
        .await
        .unwrap();

    let (node, mut events) = NodeLink::start(NodeConfig {
        endpoints: vec![format!("ws://{}", port.local_addr())],
        token: "anything".into(),
        ..Default::default()
    })
    .unwrap();
    assert!(matches!(
        within(events.recv()).await,
        Some(NodeEvent::Fresh)
    ));
    assert!(within(links.recv()).await.is_some());
    let answered = http_get(port.local_addr(), "/ping").await;
    assert!(answered.starts_with("HTTP/1.1 200"), "{answered}");
    assert!(answered.ends_with("pong"), "{answered}");

    port.stop_serving();
    let refused = http_get(port.local_addr(), "/ping").await;
    assert!(refused.starts_with("HTTP/1.1 503"), "{refused}");
    port.start_serving();
    assert!(
        http_get(port.local_addr(), "/ping")
            .await
            .starts_with("HTTP/1.1 200")
    );
    node.close().await;
}
