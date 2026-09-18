#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod support;

use rax_sim::{LinkChange, NodeOptions, WireFrame};
use rax_tokio::node::NodeEvent;
use support::*;

#[tokio::test(flavor = "multi_thread")]
async fn a_handshake_records_the_bearer_the_path_and_a_fresh_resume() {
    let sim = simulator(NodeOptions::default()).await;
    let mut node = TestNode::start(node_config(&sim), script(|_| async {}));
    assert!(matches!(node.next().await, NodeEvent::Fresh));
    let sim_node = sim.next_node().await.unwrap();
    assert_eq!(sim_node.identity().0, IDENTITY);

    let handshakes = sim.handshakes();
    assert_eq!(handshakes.len(), 1);
    let handshake = &handshakes[0];
    assert_eq!(handshake.path(), "/rax/v1/link");
    assert!(!handshake.target.contains(TOKEN), "{}", handshake.target);
    assert_eq!(
        handshake.authorization.as_deref(),
        Some(format!("Bearer {TOKEN}").as_str())
    );
    assert_eq!(handshake.status, Some(101));
    assert_eq!(
        handshake.first_frame,
        Some(WireFrame::Resume {
            last_seen: 0,
            epoch: 0
        })
    );
    assert_eq!(sim.report().resumes.fresh, 1);
    node.handle.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refusal_mode_answers_every_upgrade_until_it_is_cleared() {
    let sim = simulator(NodeOptions::default()).await;
    sim.refuse_upgrades(503);
    let mut node = TestNode::start(node_config(&sim), script(|_| async {}));

    let refused = |handshakes: &[rax_sim::Handshake]| {
        handshakes
            .iter()
            .filter(|handshake| handshake.status == Some(503))
            .count()
    };
    sim.wait_for_handshakes("three 503 refusals", |handshakes| refused(handshakes) >= 3)
        .await
        .unwrap();
    sim.accept_upgrades();

    assert!(matches!(node.next().await, NodeEvent::Fresh));
    sim.next_node().await.unwrap();
    let handshakes = sim.handshakes();
    assert_eq!(handshakes.last().unwrap().status, Some(101));
    let report = sim.report();
    assert_eq!(report.handshakes_by_status.get(&101), Some(&1));
    assert_eq!(
        report.handshakes_by_status.get(&503),
        Some(&refused(&handshakes))
    );
    node.handle.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_revoked_token_closes_the_live_link_and_its_redial_gets_401() {
    let sim = simulator(NodeOptions::default()).await;
    let mut node = TestNode::start(node_config(&sim), script(|_| async {}));
    assert!(matches!(node.next().await, NodeEvent::Fresh));
    let sim_node = sim.next_node().await.unwrap();

    sim.revoke(TOKEN).await;

    assert!(matches!(
        node.next_settled().await,
        NodeEvent::CredentialRejected
    ));
    assert!(matches!(
        sim_node.next_link_change().await.unwrap(),
        LinkChange::Closed
    ));
    let handshakes = sim.handshakes();
    let statuses: Vec<_> = handshakes
        .iter()
        .map(|handshake| handshake.status)
        .collect();
    assert_eq!(statuses, [Some(101), Some(401)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn close_codes_tell_a_clean_close_from_a_severed_socket() {
    let sim = simulator(NodeOptions::default()).await;
    let mut clean = TestNode::start(node_config(&sim), script(|_| async {}));
    assert!(matches!(clean.next().await, NodeEvent::Fresh));
    clean.handle.close().await;
    let closed = sim
        .wait_for_handshakes("the clean close", |handshakes| {
            handshakes[0].close_code.is_some()
        })
        .await
        .unwrap();
    assert_eq!(closed[0].close_code, Some(1000));

    let mut severed = TestNode::start(node_config(&sim), script(|_| async {}));
    assert!(matches!(severed.next().await, NodeEvent::Fresh));
    sim.sever();
    assert!(matches!(severed.next_settled().await, NodeEvent::Resumed));
    let closed = sim
        .wait_for_handshakes("the severed close", |handshakes| {
            handshakes.len() > 1 && handshakes[1].close_code.is_some()
        })
        .await
        .unwrap();
    assert_eq!(closed[1].close_code, Some(rax_sim::CLOSE_ABNORMAL));
    severed.handle.close().await;
}
