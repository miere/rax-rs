#![cfg(feature = "sqlite")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! The RAX API: a client dials in as a gateway, rax-sim's "gateway dials" mode standing in for
//! an editor bridge, and its sessions run on nodes of the fleet.

use std::sync::Arc;
use std::time::Duration;

use rax::content::ContentBlock;
use rax::event::StopReason;
use rax::id::{RequestId, SessionId};
use rax::resource::ReadResource;
use rax::session::{
    GatewayCapabilities, Initialized, NewSession, NodeCapabilities, PromptAccepted, SessionCreated,
    ToolGate,
};
use rax::tool::{
    CallTool, Decision, DeniedBy, ToolCall, ToolCallStatus, ToolDef, ToolGroup, ToolKind,
    ToolOutcome,
};
use rax::{ErrorKind, Event, GatewayCall, GatewayReply, NodeCall, NodeReply, Open};
use rax_gateway::access::{Access, Snapshot};
use rax_gateway::fleet::Fleet;
use rax_gateway::hub;
use rax_gateway::lent::Lent;
use rax_gateway::relay::Relay;
use rax_gateway::store::{NodeToken, SqliteStore, Store, ToolMode, UserId};
use rax_gateway::transfers::Transfers;
use rax_gateway::{Embedding, Settings, roles, token};
use rax_sim::{LinkChange, Match, NodeMethod, NodeOptions, SimNode};
use rax_tokio::CallError;
use rax_tokio::dial::DialConfig;
use rax_tokio::node::{NodeConfig, NodeEvent, NodeEvents, NodeHandle, NodeLink};
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;

const OWNER: &str = "U0PERSON1";
const ADMIN: &str = "U0ADMIN01";
const GUEST: &str = "U0GUEST01";

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

async fn rig(retain_for: Duration) -> Rig {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn Store> = Arc::new(SqliteStore::open(&dir.path().join("config.db")).unwrap());
    store.set_admin(&user(ADMIN)).await.unwrap();
    roles::grant(&*store, &user(OWNER), &user(ADMIN))
        .await
        .unwrap();
    let access = Access::reloading(
        Snapshot::load(&*store, &Settings::default()).await.unwrap(),
        store.clone(),
        Settings::default(),
    );
    let fleet = Fleet::default();
    let lent = Lent::new();
    let transfers = Transfers::default();
    let embedding = Embedding::default();
    let relay = Relay::new(
        access.clone(),
        fleet.clone(),
        store.clone(),
        embedding.settings.clone(),
        lent.clone(),
        transfers.clone(),
        embedding.owner.clone(),
    );
    let shutdown = CancellationToken::new();
    let hub = hub::start(
        "127.0.0.1:0".parse().unwrap(),
        retain_for,
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

/// A node of the fleet, driven by hand.
struct Machine {
    selector: String,
    handle: NodeHandle,
    events: NodeEvents,
    sessions: usize,
}

impl Rig {
    fn url(&self) -> String {
        format!("ws://{}", self.hub.server.local_addr())
    }

    async fn reload(&self) {
        hub::apply(
            &self.access,
            &self.fleet,
            &self.relay,
            Snapshot::load(&*self.store, &Settings::default())
                .await
                .unwrap(),
        )
        .await;
    }

    /// A node of the owner's, attached and initialized.
    async fn machine(&mut self, name: &str, tool_groups: bool) -> Machine {
        let minted = token::mint(&Settings::default().node_prefix);
        self.store
            .add_node_token(&NodeToken {
                selector: minted.selector.clone(),
                secret_hash: minted.secret_hash,
                owner: user(OWNER),
                name: name.into(),
                created_at: OffsetDateTime::now_utc(),
                revoked_at: None,
                disabled_at: None,
            })
            .await
            .unwrap();
        self.reload().await;
        let (handle, mut events) = NodeLink::start(NodeConfig {
            endpoints: vec![self.url()],
            token: minted.token,
            backoff_min: Duration::from_millis(10),
            backoff_max: Duration::from_millis(50),
            ..Default::default()
        })
        .unwrap();
        loop {
            match within(events.recv()).await.unwrap() {
                NodeEvent::Request {
                    id,
                    call: GatewayCall::Initialize(offer),
                } => {
                    assert!(
                        offer
                            .capabilities
                            .readable_schemes
                            .contains(&"bridge".to_owned()),
                        "{offer:?}"
                    );
                    let reply = GatewayReply::Initialize(Initialized {
                        protocol_version: rax::PROTOCOL_VERSION,
                        capabilities: NodeCapabilities {
                            tool_gate: ToolGate::EveryCall,
                            tool_groups,
                            ..Default::default()
                        },
                        metadata: serde_json::from_value(
                            serde_json::json!({"gateway_access": {"policy": "always_allow"}}),
                        )
                        .unwrap(),
                    });
                    handle.reply(id, reply).await.unwrap();
                    break;
                }
                NodeEvent::Fresh => {}
                other => panic!("expected initialize, got {other:?}"),
            }
        }
        within(self.hub.changes.recv()).await.unwrap();
        Machine {
            selector: minted.selector,
            handle,
            events,
            sessions: 0,
        }
    }

    /// A client of `owner`'s, dialled in and initialized.
    async fn client(&self, owner: &str) -> SimNode {
        let minted = roles::mint_client(
            &*self.store,
            &Settings::default(),
            &user(owner),
            "editor",
            rax_gateway::store::Scope::legacy(),
        )
        .await
        .unwrap();
        self.reload().await;
        let client = self.dial(&minted.token);
        assert!(matches!(
            client.next_link_change().await.unwrap(),
            LinkChange::Fresh
        ));
        client
            .initialize(GatewayCapabilities::default())
            .await
            .unwrap();
        client
    }

    fn dial(&self, token: &str) -> SimNode {
        let config = DialConfig {
            endpoints: vec![self.url()],
            token: token.to_owned(),
            backoff_min: Duration::from_millis(10),
            backoff_max: Duration::from_millis(50),
            keepalive: Duration::from_secs(1),
            ..Default::default()
        };
        SimNode::dial(config, NodeOptions::default(), Duration::from_secs(10)).unwrap()
    }
}

impl Machine {
    async fn next(&mut self) -> NodeEvent {
        within(self.events.recv()).await.expect("node events ended")
    }

    /// Answers the next `session.new`, returning the request as the node saw it.
    async fn opens(&mut self) -> NewSession {
        let NodeEvent::Request {
            id,
            call: GatewayCall::NewSession(request),
        } = self.next().await
        else {
            panic!("expected session.new")
        };
        self.sessions += 1;
        let created = SessionCreated {
            session_id: SessionId(format!("{}-s{}", &self.selector[..4], self.sessions)),
            unhandled: vec![],
        };
        self.handle
            .reply(id, GatewayReply::NewSession(created))
            .await
            .unwrap();
        request
    }

    /// Accepts the next prompt and returns its stream and content.
    async fn prompted(&mut self) -> (RequestId, Vec<Open<ContentBlock>>) {
        let NodeEvent::Request {
            id,
            call: GatewayCall::Prompt(prompt),
        } = self.next().await
        else {
            panic!("expected a prompt")
        };
        self.handle
            .reply(id.clone(), GatewayReply::Prompt(PromptAccepted::default()))
            .await
            .unwrap();
        (id, prompt.content)
    }

    async fn finish(&self, stream: &RequestId) {
        let stop_reason = Some(StopReason::EndTurn);
        self.handle
            .event(stream.clone(), Event::Complete { stop_reason })
            .await
            .unwrap();
        self.handle.end(stream.clone()).await.unwrap();
    }

    fn session(&self, n: usize) -> SessionId {
        SessionId(format!("{}-s{n}", &self.selector[..4]))
    }

    async fn call_tool(&self, session: SessionId, namespace: &str) -> Result<NodeReply, CallError> {
        self.handle
            .call(NodeCall::CallTool(CallTool {
                session_id: session,
                namespace: Some(namespace.to_owned()),
                name: "read".into(),
                arguments: None,
            }))
            .await
    }
}

fn editor_group() -> ToolGroup {
    ToolGroup {
        namespace: "editor".into(),
        tools: vec![ToolDef {
            name: "read".into(),
            description: "Reads an open document".into(),
            input_schema: Some(serde_json::json!({"type": "object"})),
            kind: ToolKind::Read,
        }],
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_token_opens_the_api_and_revoking_it_closes_the_link() {
    let rig = rig(hub::RETAIN_FOR).await;
    // A node's token never opens the client role.
    let node_token = token::mint(&Settings::default().node_prefix).token;
    let refused = rig.dial(&node_token);
    assert!(matches!(
        refused.next_link_change().await.unwrap(),
        LinkChange::Refused { status: 401 }
    ));

    let client = rig.client(OWNER).await;
    let selector = rig
        .store
        .user_tokens()
        .await
        .unwrap()
        .pop()
        .unwrap()
        .selector;
    rig.store.revoke_user_token(&selector).await.unwrap();
    rig.reload().await;
    loop {
        match client.next_link_change().await.unwrap() {
            LinkChange::Refused { status } => {
                assert_eq!(status, 401);
                break;
            }
            LinkChange::Disconnected(_) => {}
            other => panic!("expected the link to end, got {other:?}"),
        }
    }
    rig.shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_session_goes_to_the_least_loaded_node_that_takes_tool_groups() {
    let mut rig = rig(hub::RETAIN_FOR).await;
    let _old = rig.machine("old", false).await;
    let mut busy = rig.machine("busy", true).await;
    let mut idle = rig.machine("idle", true).await;
    let client = rig.client(OWNER).await;

    // Whichever node the first session lands on, the second goes to the other.
    let opening = tokio::spawn({
        let client = client.clone();
        async move { client.new_session(vec![]).await }
    });
    let first_on_busy = tokio::select! {
        _ = busy.opens() => true,
        _ = idle.opens() => false,
    };
    within(opening).await.unwrap().unwrap();
    let opening = tokio::spawn({
        let client = client.clone();
        async move { client.new_session(vec![]).await }
    });
    if first_on_busy {
        idle.opens().await;
    } else {
        busy.opens().await;
    }
    within(opening).await.unwrap().unwrap();
    let summaries = rig.relay.summaries();
    let mut nodes: Vec<&str> = summaries.iter().map(|s| s.node.as_str()).collect();
    nodes.sort();
    assert_eq!(nodes, ["busy", "idle"]);
    rig.shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn lent_groups_are_suffixed_and_refused_to_every_other_session() {
    let mut rig = rig(hub::RETAIN_FOR).await;
    let mut node = rig.machine("laptop", true).await;
    let client = rig.client(OWNER).await;

    let opening = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .open_session(NewSession {
                    context: vec![],
                    tool_groups: vec![editor_group()],
                })
                .await
        }
    });
    let request = node.opens().await;
    let lent = request.tool_groups[0].namespace.clone();
    assert!(
        lent.starts_with("editor_") && lent.len() == "editor_".len() + 4,
        "{lent}"
    );
    let session = within(opening).await.unwrap().unwrap().session_id;

    let calling = tokio::spawn({
        let handle = node.handle.clone();
        let (session, lent) = (node.session(1), lent.clone());
        async move {
            handle
                .call(NodeCall::CallTool(CallTool {
                    session_id: session,
                    namespace: Some(lent),
                    name: "read".into(),
                    arguments: Some(serde_json::json!({"path": "a.rs"})),
                }))
                .await
        }
    });
    let asked = client.next_node_call().await.unwrap();
    let NodeCall::CallTool(call) = &asked.call else {
        panic!("expected a tool call")
    };
    assert_eq!(call.namespace.as_deref(), Some("editor"));
    assert_eq!(call.session_id, session);
    asked
        .answer_tool(ToolOutcome {
            content: "fn main() {}".into(),
            is_error: false,
            resources: Vec::new(),
        })
        .await
        .unwrap();
    let Ok(NodeReply::CallTool(outcome)) = within(calling).await.unwrap() else {
        panic!("expected the client's answer")
    };
    assert_eq!(outcome.content, "fn main() {}");

    // A second session was lent nothing, and no session was lent a group of the embedder's.
    let opening = tokio::spawn({
        let client = client.clone();
        async move { client.new_session(vec![]).await }
    });
    node.opens().await;
    within(opening).await.unwrap().unwrap();
    for (session, namespace) in [(node.session(2), lent.as_str()), (node.session(1), "chat")] {
        let Err(CallError::Fault(fault)) = node.call_tool(session, namespace).await else {
            panic!("expected a refusal")
        };
        assert_eq!(fault.kind, ErrorKind::Forbidden);
    }
    rig.shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_reads_what_the_client_linked_through_bridge_uris() {
    let mut rig = rig(hub::RETAIN_FOR).await;
    let mut node = rig.machine("laptop", true).await;
    let other = rig.machine("desktop", false).await;
    let client = rig.client(OWNER).await;
    let opening = tokio::spawn({
        let client = client.clone();
        async move { client.new_session(vec![]).await }
    });
    node.opens().await;
    let session = within(opening).await.unwrap().unwrap().session_id;

    let link = ContentBlock::link("file:///work/a.rs", "a.rs").into();
    let prompting = tokio::spawn({
        let client = client.clone();
        async move { client.prompt(session, vec![link]).await }
    });
    let (stream, content) = node.prompted().await;
    let Open::Known(ContentBlock::ResourceLink { uri, .. }) = &content[0] else {
        panic!("expected the link")
    };
    assert!(
        uri.starts_with("bridge://") && uri.ends_with("/file:///work/a.rs"),
        "{uri}"
    );

    let reading = tokio::spawn({
        let (handle, uri) = (node.handle.clone(), uri.clone());
        async move {
            handle
                .read_resource(ReadResource {
                    uri,
                    max_bytes: None,
                })
                .await
        }
    });
    let asked = client.next_node_call().await.unwrap();
    assert_eq!(asked.method(), NodeMethod::ReadResource);
    let NodeCall::ReadResource(read) = &asked.call else {
        panic!("expected a read")
    };
    assert_eq!(read.uri, "file:///work/a.rs");
    asked
        .serve(b"fn main() {}", Some("text/x-rust".into()))
        .await
        .unwrap();
    let resource = within(reading).await.unwrap().unwrap();
    assert_eq!(resource.bytes, b"fn main() {}");

    // A node with no session from this client cannot read through it.
    let Err(CallError::Fault(fault)) = other
        .handle
        .read_resource(ReadResource {
            uri: uri.clone(),
            max_bytes: None,
        })
        .await
    else {
        panic!("expected a refusal")
    };
    assert_eq!(fault.kind, ErrorKind::Forbidden);

    node.finish(&stream).await;
    let mut turn = within(prompting).await.unwrap().unwrap();
    turn.expect(Match::complete(StopReason::EndTurn))
        .await
        .unwrap();
    rig.shutdown.cancel();
}

fn bash(id: &str) -> ToolCall {
    ToolCall {
        id: id.into(),
        name: "Bash".into(),
        title: Some("ls".into()),
        kind: ToolKind::Execute,
        input: None,
        content: vec![],
    }
}

async fn verdict(node: &mut Machine) -> Decision {
    loop {
        if let NodeEvent::Verdict(verdict) = node.next().await {
            return verdict.decision;
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_owners_rules_decide_at_once_and_only_a_person_is_asked_through_the_client() {
    let mut rig = rig(hub::RETAIN_FOR).await;
    let mut node = rig.machine("laptop", true).await;
    let client = rig.client(OWNER).await;
    let opening = tokio::spawn({
        let client = client.clone();
        async move { client.new_session(vec![]).await }
    });
    node.opens().await;
    let session = within(opening).await.unwrap().unwrap().session_id;

    // Always allowed: decided here, and the client only draws it.
    let prompting = tokio::spawn({
        let (client, session) = (client.clone(), session.clone());
        async move {
            client
                .prompt(session, vec![ContentBlock::text("go").into()])
                .await
        }
    });
    let (stream, _) = node.prompted().await;
    let mut turn = within(prompting).await.unwrap().unwrap();
    node.handle
        .event(
            stream.clone(),
            Event::ToolCall {
                tool_call: bash("tc1"),
            },
        )
        .await
        .unwrap();
    assert_eq!(verdict(&mut node).await, Decision::Allow);
    turn.expect(Match::tool_call_update(ToolCallStatus::InProgress))
        .await
        .unwrap();
    node.finish(&stream).await;
    turn.expect(Match::complete(StopReason::EndTurn))
        .await
        .unwrap();
    assert!(
        !turn
            .seen()
            .iter()
            .any(|event| matches!(event, Open::Known(Event::ToolCall { .. }))),
        "the client was asked about a call the rules decided"
    );

    // Whitelist only, and off it: the client asks its person, whose deny reaches the node.
    rig.store
        .set_tool_mode(&user(OWNER), ToolMode::AllowedWhitelist)
        .await
        .unwrap();
    let prompting = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .prompt(session, vec![ContentBlock::text("again").into()])
                .await
        }
    });
    let (stream, _) = node.prompted().await;
    let mut turn = within(prompting).await.unwrap().unwrap();
    node.handle
        .event(
            stream.clone(),
            Event::ToolCall {
                tool_call: bash("tc2"),
            },
        )
        .await
        .unwrap();
    turn.expect(Match::tool_call("Bash")).await.unwrap();
    let deny = Decision::Deny {
        by: DeniedBy::User,
        reason: Some("not now".into()),
    };
    turn.verdict("tc2", deny.clone()).await.unwrap();
    assert_eq!(verdict(&mut node).await, deny);
    node.finish(&stream).await;
    rig.shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_that_stays_away_takes_its_sessions_with_it() {
    let mut rig = rig(Duration::from_millis(300)).await;
    let mut node = rig.machine("laptop", true).await;
    let client = rig.client(OWNER).await;
    let opening = tokio::spawn({
        let client = client.clone();
        async move { client.new_session(vec![]).await }
    });
    node.opens().await;
    within(opening).await.unwrap().unwrap();
    assert_eq!(rig.fleet.summaries()[0].sessions, 1);

    client.end_link().await;
    loop {
        if let NodeEvent::Request {
            id,
            call: GatewayCall::CloseSession(closed),
        } = node.next().await
        {
            assert_eq!(closed.session_id, node.session(1));
            node.handle
                .reply(id, GatewayReply::CloseSession)
                .await
                .unwrap();
            break;
        }
    }
    within(async {
        while !rig.relay.summaries().is_empty() || rig.fleet.summaries()[0].sessions != 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    rig.shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn only_the_nodes_owner_approves_a_call_from_someone_elses_client() {
    let mut rig = rig(hub::RETAIN_FOR).await;
    let mut node = rig.machine("laptop", true).await;
    rig.store
        .set_tool_mode(&user(OWNER), ToolMode::AllowedWhitelist)
        .await
        .unwrap();
    // A guest with no machine of their own lands on the owner's, which lets everyone in.
    let client = rig.client(GUEST).await;
    let opening = tokio::spawn({
        let client = client.clone();
        async move { client.new_session(vec![]).await }
    });
    node.opens().await;
    let session = within(opening).await.unwrap().unwrap().session_id;
    let prompting = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .prompt(session, vec![ContentBlock::text("go").into()])
                .await
        }
    });
    let (stream, _) = node.prompted().await;
    let mut turn = within(prompting).await.unwrap().unwrap();
    node.handle
        .event(
            stream.clone(),
            Event::ToolCall {
                tool_call: bash("tc1"),
            },
        )
        .await
        .unwrap();

    // The guest hears what it waits on but is never asked, and with no channel to reach the owner
    // through, nobody approves it.
    let Decision::Deny { by, .. } = verdict(&mut node).await else {
        panic!("a call only the owner may approve was allowed")
    };
    assert_eq!(by, DeniedBy::Unavailable);
    turn.verdict("tc1", Decision::Allow).await.unwrap();
    node.finish(&stream).await;
    turn.expect(Match::complete(StopReason::EndTurn))
        .await
        .unwrap();
    assert!(
        !turn
            .seen()
            .iter()
            .any(|event| matches!(event, Open::Known(Event::ToolCall { .. }))),
        "someone else's client was asked to approve a call on the owner's machine"
    );
    assert!(turn.seen().iter().any(|event| matches!(
        event,
        Open::Known(Event::Status { text }) if text.contains("owner of laptop")
    )));
    assert!(
        quiet_verdicts(&mut node).await,
        "the guest's verdict reached the node"
    );
    rig.shutdown.cancel();
}

/// No further verdict arrives on the node within a moment.
async fn quiet_verdicts(node: &mut Machine) -> bool {
    tokio::time::timeout(Duration::from_millis(300), async {
        loop {
            if let NodeEvent::Verdict(_) = node.next().await {
                return;
            }
        }
    })
    .await
    .is_err()
}
