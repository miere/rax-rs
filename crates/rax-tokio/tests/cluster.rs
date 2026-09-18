#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod support;

use rax_tokio::gateway::LinkEvent;
use rax_tokio::node::{NodeEvent, NodeLink};
use support::*;

#[tokio::test(flavor = "multi_thread")]
async fn a_node_moves_to_the_member_that_takes_over_serving() {
    let (leader, mut leader_links) = gateway(gateway_config()).await;
    let (standby, mut standby_links) = gateway(gateway_config()).await;
    standby.stop_serving();
    let mut config = node_config(leader.local_addr());
    config.endpoints = vec![
        format!("ws://{}", standby.local_addr()),
        format!("ws://{}", leader.local_addr()),
    ];
    let (node, mut node_events) = NodeLink::start(config).unwrap();
    assert!(matches!(next(&mut node_events).await, NodeEvent::Fresh));
    let mut first = within(leader_links.recv()).await.unwrap();

    standby.start_serving();
    leader.stop_serving();
    assert!(matches!(
        next_link(&mut first.events).await,
        LinkEvent::Closed
    ));
    loop {
        match next(&mut node_events).await {
            NodeEvent::Disconnected { .. } => {}
            NodeEvent::Fresh => break,
            other => panic!("expected a fresh link, got {other:?}"),
        }
    }
    within(standby_links.recv()).await.unwrap();
    node.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_member_that_is_not_serving_keeps_a_lone_node_redialling() {
    let (standby, mut standby_links) = gateway(gateway_config()).await;
    standby.stop_serving();
    let (node, mut node_events) = NodeLink::start(node_config(standby.local_addr())).unwrap();
    let waited =
        tokio::time::timeout(std::time::Duration::from_millis(300), node_events.recv()).await;
    assert!(waited.is_err(), "a 503 must not end the node: {waited:?}");

    standby.start_serving();
    assert!(matches!(next(&mut node_events).await, NodeEvent::Fresh));
    within(standby_links.recv()).await.unwrap();
    node.close().await;
}
