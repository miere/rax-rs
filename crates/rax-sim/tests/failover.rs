#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod support;

use rax_sim::{NodeOptions, Simulator};
use rax_tokio::node::{NodeConfig, NodeEvent, NodeEvents, NodeHandle, NodeLink};
use support::*;

async fn pair() -> (Simulator, Simulator) {
    (
        simulator(NodeOptions::default()).await,
        simulator(NodeOptions::default()).await,
    )
}

fn across(first: &Simulator, second: &Simulator) -> NodeConfig {
    NodeConfig {
        endpoints: vec![first.url(), second.url()],
        ..node_config(first)
    }
}

fn start(config: NodeConfig) -> (NodeHandle, NodeEvents) {
    NodeLink::start(config).unwrap()
}

async fn next_settled(events: &mut NodeEvents) -> NodeEvent {
    loop {
        match within(events.recv()).await.expect("node events ended") {
            NodeEvent::Disconnected { .. } => {}
            settled => return settled,
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unreachable_first_gateway_hands_the_node_to_the_second() {
    let (first, second) = pair().await;
    first.hang_up_upgrades();
    let (node, mut events) = start(across(&first, &second));

    assert!(matches!(next_settled(&mut events).await, NodeEvent::Fresh));
    second.next_node().await.unwrap();
    node.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_standby_refusing_with_503_is_skipped() {
    let (first, second) = pair().await;
    first.refuse_upgrades(503);
    let (node, mut events) = start(across(&first, &second));

    assert!(matches!(next_settled(&mut events).await, NodeEvent::Fresh));
    second.next_node().await.unwrap();
    let tried = first.handshakes();
    assert!(!tried.is_empty());
    assert!(tried.iter().all(|handshake| handshake.status == Some(503)));
    node.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejected_credential_ends_the_node_without_trying_the_rest() {
    let (first, second) = pair().await;
    first.refuse_upgrades(401);
    let (_node, mut events) = start(across(&first, &second));

    assert!(matches!(
        next_settled(&mut events).await,
        NodeEvent::CredentialRejected
    ));
    assert!(within(events.recv()).await.is_none());
    assert!(second.handshakes().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_stays_on_the_gateway_that_last_answered() {
    let (first, second) = pair().await;
    first.refuse_upgrades(503);
    let (node, mut events) = start(across(&first, &second));
    assert!(matches!(next_settled(&mut events).await, NodeEvent::Fresh));
    second.next_node().await.unwrap();

    first.accept_upgrades();
    let tried_first = first.handshakes().len();
    assert!(tried_first > 0);
    second.sever();
    assert!(matches!(
        next_settled(&mut events).await,
        NodeEvent::Resumed
    ));
    assert_eq!(first.handshakes().len(), tried_first);
    node.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn when_its_gateway_goes_away_the_node_moves_on_and_starts_fresh() {
    let (first, second) = pair().await;
    second.refuse_upgrades(503);
    let (node, mut events) = start(across(&first, &second));
    assert!(matches!(next_settled(&mut events).await, NodeEvent::Fresh));
    first.next_node().await.unwrap();

    second.accept_upgrades();
    first.hang_up_upgrades();
    first.sever();
    assert!(matches!(next_settled(&mut events).await, NodeEvent::Fresh));
    second.next_node().await.unwrap();
    node.close().await;
}
