#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! The "gateway dials" mode: the simulated gateway dials an acceptor that plays the node.

mod support;

use std::time::Duration;

use rax::event::StopReason;
use rax::session::GatewayCapabilities;
use rax::tool::{CallTool, ToolOutcome};
use rax::{ErrorKind, NodeCall, PROTOCOL_VERSION};
use rax_sim::{LinkChange, Match, NodeOptions, SimNode};
use rax_tokio::Role;
use rax_tokio::accept::{
    AcceptConfig, Accepted, AcceptedLinks, Acceptor, Authenticator, GatewayIdentity, NodeIdentity,
};
use rax_tokio::dial::DialConfig;
use rax_tokio::node::NewNodeLink;
use support::*;

const USER_TOKEN: &str = "mrtg_user_0123456789abcdef_c2VjcmV0LXNlY3JldC1zZWNyZXQtc2VjcmV0LXNlY3I";

struct Users;

impl Authenticator for Users {
    fn authenticate(&self, _token: &str) -> Option<NodeIdentity> {
        None
    }

    fn authenticate_gateway(&self, token: &str) -> Option<GatewayIdentity> {
        (token == USER_TOKEN).then(|| GatewayIdentity("user-1".to_owned()))
    }
}

async fn acceptor() -> (Acceptor, AcceptedLinks) {
    let config = AcceptConfig {
        roles: vec![Role::Gateway],
        keepalive: Duration::from_secs(1),
        ..Default::default()
    };
    Acceptor::bind("127.0.0.1:0", Users, config).await.unwrap()
}

fn dial(acceptor: &Acceptor, token: &str, options: NodeOptions) -> SimNode {
    let config = DialConfig {
        endpoints: vec![format!("ws://{}", acceptor.local_addr())],
        token: token.to_owned(),
        keepalive: Duration::from_secs(1),
        backoff_min: Duration::from_millis(5),
        backoff_max: Duration::from_millis(50),
        ..Default::default()
    };
    SimNode::dial(config, options, Duration::from_secs(10)).unwrap()
}

async fn accepted(links: &mut AcceptedLinks) -> NewNodeLink {
    match within(links.recv()).await.unwrap() {
        Accepted::Gateway(link) => link,
        Accepted::Node(_) => panic!("expected a gateway"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dialling_sim_drives_a_turn_on_an_accepting_node() {
    let (acceptor, mut links) = acceptor().await;
    let sim = dial(&acceptor, USER_TOKEN, NodeOptions::default());
    assert!(matches!(
        sim.next_link_change().await.unwrap(),
        LinkChange::Fresh
    ));
    let NewNodeLink { handle, events, .. } = accepted(&mut links).await;
    let turn = script(|turn| async move {
        turn.say("hello from the server").await;
        turn.finish().await;
    });
    let node = TestNode::serving(handle.clone(), events, turn);

    let initialized = sim
        .initialize(GatewayCapabilities::default())
        .await
        .unwrap();
    assert_eq!(initialized.protocol_version, PROTOCOL_VERSION);
    let session = sim.new_session(vec![]).await.unwrap().session_id;
    let mut prompt = sim.prompt(session.clone(), text("hi")).await.unwrap();
    prompt
        .expect(Match::message_contains("hello"))
        .await
        .unwrap();
    prompt
        .expect(Match::complete(StopReason::EndTurn))
        .await
        .unwrap();

    // The accepting node calls back into the gateway that dialled it.
    let call = tokio::spawn(async move {
        handle
            .call(NodeCall::CallTool(CallTool {
                session_id: session,
                namespace: Some("editor_x7k2".into()),
                name: "read".into(),
                arguments: None,
            }))
            .await
    });
    let request = sim.next_node_call().await.unwrap();
    request
        .answer_tool(ToolOutcome {
            content: "never".into(),
            is_error: false,
            resources: Vec::new(),
        })
        .await
        .unwrap();
    let fault = within(call).await.unwrap().unwrap_err();
    let rax_tokio::CallError::Fault(fault) = fault else {
        panic!("expected a fault, got {fault:?}")
    };
    assert_eq!(fault.kind, ErrorKind::Forbidden);
    drop(node);
    sim.end_link().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dialling_sim_hears_its_token_refused() {
    let (acceptor, _links) = acceptor().await;
    let sim = dial(&acceptor, "not-a-user-token", NodeOptions::default());
    assert!(matches!(
        sim.next_link_change().await.unwrap(),
        LinkChange::Refused { status: 401 }
    ));
    assert!(sim.next_link_change().await.is_err());
}
