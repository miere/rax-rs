#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! A gateway that dials and a node that accepts: delivery follows the connection, messages follow
//! the role.

mod support;

use std::net::SocketAddr;
use std::time::Duration;

use rax::session::{Initialize, Initialized};
use rax::{GatewayCall, GatewayReply, NodeCall};
use rax_tokio::accept::{
    AcceptConfig, Accepted, AcceptedLinks, Acceptor, Authenticator, GatewayIdentity, NodeIdentity,
};
use rax_tokio::dial::{DialConfig, Dialler};
use rax_tokio::gateway::{DialledEvent, DialledEvents, GatewayLink, LinkEvent};
use rax_tokio::node::{NewNodeLink, NodeEvent, NodeEvents, NodeLink};
use rax_tokio::{CallError, Role};
use support::*;

const USER_TOKEN: &str = "user-token";
const USER: &str = "user-1";

struct Tokens;

impl Authenticator for Tokens {
    fn authenticate(&self, token: &str) -> Option<NodeIdentity> {
        (token == TOKEN).then(|| NodeIdentity(IDENTITY.to_owned()))
    }

    fn authenticate_gateway(&self, token: &str) -> Option<GatewayIdentity> {
        (token == USER_TOKEN).then(|| GatewayIdentity(USER.to_owned()))
    }
}

fn accept_config(roles: Vec<Role>) -> AcceptConfig {
    AcceptConfig {
        roles,
        keepalive: Duration::from_secs(1),
        handshake_timeout: Duration::from_secs(5),
        ..Default::default()
    }
}

async fn acceptor(config: AcceptConfig) -> (Acceptor, AcceptedLinks) {
    Acceptor::bind("127.0.0.1:0", Tokens, config).await.unwrap()
}

fn dial_config(addr: SocketAddr, token: &str) -> DialConfig {
    DialConfig {
        token: token.to_owned(),
        ..node_config(addr)
    }
}

async fn next_dialled(events: &mut DialledEvents) -> DialledEvent {
    within(events.recv()).await.expect("dialled events ended")
}

async fn accepted_gateway(accepted: &mut AcceptedLinks) -> NewNodeLink {
    match within(accepted.recv()).await.unwrap() {
        Accepted::Gateway(link) => link,
        Accepted::Node(_) => panic!("expected a gateway"),
    }
}

/// The gateway role sends `initialize` whichever side dialled.
async fn initialize(
    link: &GatewayLink,
    node: &rax_tokio::node::NodeHandle,
    events: &mut NodeEvents,
) {
    let pending = link
        .call(GatewayCall::Initialize(Initialize {
            protocol_version: 1,
            capabilities: Default::default(),
        }))
        .await
        .unwrap();
    let NodeEvent::Request {
        id,
        call: GatewayCall::Initialize(_),
    } = next(events).await
    else {
        panic!("expected initialize")
    };
    node.reply(
        id,
        GatewayReply::Initialize(Initialized {
            protocol_version: 1,
            capabilities: Default::default(),
            metadata: Default::default(),
        }),
    )
    .await
    .unwrap();
    let reply = within(pending.reply).await.unwrap();
    assert!(matches!(reply, GatewayReply::Initialize(_)));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_gateway_dials_a_node_and_initialize_round_trips() {
    let (acceptor, mut accepted) = acceptor(accept_config(vec![Role::Gateway])).await;
    let (link, mut link_events) =
        Dialler::gateway(dial_config(acceptor.local_addr(), USER_TOKEN)).unwrap();
    assert!(matches!(
        next_dialled(&mut link_events).await,
        DialledEvent::Fresh
    ));
    let NewNodeLink {
        identity,
        handle: node,
        events: mut node_events,
    } = accepted_gateway(&mut accepted).await;
    assert_eq!(identity, GatewayIdentity(USER.to_owned()));

    initialize(&link, &node, &mut node_events).await;

    // Calls run the other way too: the node asks the gateway that dialled it.
    let node_call = tokio::spawn({
        let node = node.clone();
        async move { node.call(NodeCall::ReadResource(read("file:///a"))).await }
    });
    let DialledEvent::Link(LinkEvent::Request {
        id,
        call: NodeCall::ReadResource(_),
    }) = next_dialled(&mut link_events).await
    else {
        panic!("expected resource.read")
    };
    link.fault(id, rax::Error::new(rax::ErrorKind::NotFound, "gone"))
        .await
        .unwrap();
    let Err(CallError::Fault(fault)) = within(node_call).await.unwrap() else {
        panic!("expected a fault")
    };
    assert_eq!(fault.kind, rax::ErrorKind::NotFound);
    link.close().await;
}

fn read(uri: &str) -> rax::resource::ReadResource {
    rax::resource::ReadResource {
        uri: uri.to_owned(),
        max_bytes: None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn one_acceptor_serves_both_roles_and_a_node_still_offers_nothing() {
    let (acceptor, mut accepted) = acceptor(accept_config(vec![Role::Node, Role::Gateway])).await;
    let (node, mut node_events) = NodeLink::start(node_config(acceptor.local_addr())).unwrap();
    assert!(matches!(next(&mut node_events).await, NodeEvent::Fresh));
    let Accepted::Node(new_link) = within(accepted.recv()).await.unwrap() else {
        panic!("expected a node")
    };
    assert_eq!(new_link.link.identity(), &NodeIdentity(IDENTITY.to_owned()));

    let (link, mut link_events) =
        Dialler::gateway(dial_config(acceptor.local_addr(), USER_TOKEN)).unwrap();
    assert!(matches!(
        next_dialled(&mut link_events).await,
        DialledEvent::Fresh
    ));
    let bridge = accepted_gateway(&mut accepted).await;
    assert_eq!(bridge.identity, GatewayIdentity(USER.to_owned()));
    link.close().await;
    node.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_acceptor_refuses_a_role_it_does_not_serve_and_the_dialler_stops() {
    let (server, _new_links) = gateway(gateway_config()).await;
    let (_link, mut events) =
        Dialler::gateway(dial_config(server.local_addr(), USER_TOKEN)).unwrap();
    assert!(matches!(
        next_dialled(&mut events).await,
        DialledEvent::Refused { status: 403 }
    ));
    assert!(within(events.recv()).await.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_gateway_token_is_judged_as_a_gateway_token() {
    let (acceptor, _accepted) = acceptor(accept_config(vec![Role::Node, Role::Gateway])).await;
    // A node's token does not open the gateway role, and the dialler stops.
    let (_link, mut events) = Dialler::gateway(dial_config(acceptor.local_addr(), TOKEN)).unwrap();
    assert!(matches!(
        next_dialled(&mut events).await,
        DialledEvent::CredentialRejected
    ));
    // Nor does a gateway's token open the node role.
    let (_node, mut node_events) =
        NodeLink::start(dial_config(acceptor.local_addr(), USER_TOKEN)).unwrap();
    assert!(matches!(
        next(&mut node_events).await,
        NodeEvent::CredentialRejected
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dialling_gateway_resumes_and_a_call_survives_the_drop() {
    let (acceptor, mut accepted) = acceptor(accept_config(vec![Role::Gateway])).await;
    let proxy = Proxy::start(acceptor.local_addr()).await;
    let (link, mut link_events) = Dialler::gateway(dial_config(proxy.addr(), USER_TOKEN)).unwrap();
    assert!(matches!(
        next_dialled(&mut link_events).await,
        DialledEvent::Fresh
    ));
    let NewNodeLink {
        handle: node,
        events: mut node_events,
        ..
    } = accepted_gateway(&mut accepted).await;

    proxy.sever();
    assert!(matches!(
        next_dialled(&mut link_events).await,
        DialledEvent::Link(LinkEvent::Disconnected { .. })
    ));
    assert!(matches!(
        next_dialled(&mut link_events).await,
        DialledEvent::Link(LinkEvent::Resumed)
    ));
    loop {
        match next(&mut node_events).await {
            NodeEvent::Resumed => break,
            NodeEvent::Disconnected { .. } => {}
            other => panic!("unexpected {other:?}"),
        }
    }
    initialize(&link, &node, &mut node_events).await;
    link.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dialling_gateway_starts_fresh_when_the_acceptor_lost_its_link() {
    let (acceptor, mut accepted) = acceptor(accept_config(vec![Role::Gateway])).await;
    let (link, mut link_events) =
        Dialler::gateway(dial_config(acceptor.local_addr(), USER_TOKEN)).unwrap();
    assert!(matches!(
        next_dialled(&mut link_events).await,
        DialledEvent::Fresh
    ));
    let first = accepted_gateway(&mut accepted).await;
    let pending = link
        .call(GatewayCall::Initialize(Initialize {
            protocol_version: 1,
            capabilities: Default::default(),
        }))
        .await
        .unwrap();

    // The acceptor forgets every link, as a member that lost its lease does.
    acceptor.stop_serving();
    acceptor.start_serving();
    // The accepted node ends rather than reporting a fresh link; that arrives as a new one.
    let mut events = first.events;
    while let Some(event) = within(events.recv()).await {
        assert!(!matches!(event, NodeEvent::Fresh));
    }

    loop {
        match next_dialled(&mut link_events).await {
            DialledEvent::Fresh => break,
            DialledEvent::Link(LinkEvent::Disconnected { .. }) => {}
            other => panic!("unexpected {other:?}"),
        }
    }
    assert!(matches!(
        within(pending.reply).await,
        Err(CallError::LinkReset)
    ));
    let NewNodeLink {
        handle: node,
        events: mut node_events,
        ..
    } = accepted_gateway(&mut accepted).await;
    initialize(&link, &node, &mut node_events).await;
    link.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_accepted_node_ends_when_its_gateway_stays_away() {
    let config = AcceptConfig {
        retain_for: Duration::from_millis(200),
        ..accept_config(vec![Role::Gateway])
    };
    let (acceptor, mut accepted) = acceptor(config).await;
    let (link, mut link_events) =
        Dialler::gateway(dial_config(acceptor.local_addr(), USER_TOKEN)).unwrap();
    assert!(matches!(
        next_dialled(&mut link_events).await,
        DialledEvent::Fresh
    ));
    let NewNodeLink {
        handle: node,
        events: mut node_events,
        ..
    } = accepted_gateway(&mut accepted).await;

    link.close().await;
    while let Some(event) = within(node_events.recv()).await {
        assert!(matches!(event, NodeEvent::Disconnected { .. }), "{event:?}");
    }
    assert!(node.is_closed());
    let reply = node.call(NodeCall::ReadResource(read("file:///a"))).await;
    assert!(matches!(reply, Err(CallError::Closed)));
}
