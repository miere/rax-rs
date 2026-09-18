#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod support;

use std::sync::Arc;
use std::time::Duration;

use rax::event::BackgroundEvent;
use rax::session::GatewayCapabilities;
use rax::{ContentBlock, Event, NodeMessage, Open};
use rax_sim::{LinkChange, NodeOptions};
use rax_tokio::node::NodeEvent;
use support::*;
use tokio::sync::Notify;

const EVENTS: usize = 10;

fn said(events: &[Open<Event>]) -> Vec<String> {
    events
        .iter()
        .filter_map(|event| match event {
            Open::Known(Event::Message {
                content: Open::Known(ContentBlock::Text { text }),
            }) => Some(text.clone()),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_severed_socket_mid_turn_resumes_and_loses_nothing() {
    let sim = simulator(NodeOptions::default()).await;
    let halfway = Arc::new(Notify::new());
    let gate = halfway.clone();
    let turn_script = script(move |turn| {
        let gate = gate.clone();
        async move {
            for i in 1..=EVENTS {
                if i == EVENTS / 2 + 1 {
                    gate.notified().await;
                }
                turn.say(&i.to_string()).await;
            }
            turn.finish().await;
        }
    });
    let mut node = TestNode::start(node_config(&sim), turn_script);
    assert!(matches!(node.next().await, NodeEvent::Fresh));
    let sim_node = sim.next_node().await.unwrap();

    let mut turn = sim_node.prompt("s1", text("count")).await.unwrap();
    let mut events = Vec::new();
    for _ in 0..EVENTS / 2 {
        events.push(turn.next_event().await.unwrap());
    }
    sim.sever();
    halfway.notify_one();

    assert!(matches!(node.next_settled().await, NodeEvent::Resumed));
    loop {
        match sim_node.next_link_change().await.unwrap() {
            LinkChange::Disconnected(_) => {}
            LinkChange::Resumed => break,
            other => panic!("expected a resume, got {other:?}"),
        }
    }
    events.extend(turn.until_end().await.unwrap());

    let expected: Vec<String> = (1..=EVENTS).map(|i| i.to_string()).collect();
    assert_eq!(said(&events), expected);
    let delivered = sim.transcript().node_messages();
    let on_stream = |wanted: fn(&NodeMessage) -> bool| {
        delivered
            .iter()
            .filter(
                |delivered| matches!(&delivered.message, Open::Known(message) if wanted(message)),
            )
            .count()
    };
    assert_eq!(
        on_stream(|message| matches!(message, NodeMessage::Event { .. })),
        EVENTS + 1
    );
    assert_eq!(
        on_stream(|message| matches!(message, NodeMessage::End { .. })),
        1
    );
    let report = sim.report();
    assert_eq!(report.resumes.accepted, 1);
    assert!(report.turns[0].to_end.is_some(), "{report:?}");
    node.handle.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restart_forgets_the_link_and_the_node_starts_fresh() {
    let sim = simulator(NodeOptions::default()).await;
    let mut node = TestNode::start(node_config(&sim), script(|_| async {}));
    assert!(matches!(node.next().await, NodeEvent::Fresh));
    let before = sim.next_node().await.unwrap();
    before
        .initialize(GatewayCapabilities::default())
        .await
        .unwrap();

    sim.restart().await.unwrap();

    assert!(matches!(node.next_settled().await, NodeEvent::Fresh));
    let after = sim.next_node().await.unwrap();
    after
        .initialize(GatewayCapabilities::default())
        .await
        .unwrap();
    loop {
        match before.next_link_change().await.unwrap() {
            LinkChange::Closed => break,
            LinkChange::Disconnected(_) => {}
            other => panic!("expected the old link to close, got {other:?}"),
        }
    }
    assert!(
        before
            .initialize(GatewayCapabilities::default())
            .await
            .is_err()
    );
    let resumes = sim.report().resumes;
    assert_eq!(
        (resumes.fresh, resumes.refused, resumes.accepted),
        (1, 1, 0)
    );
    node.handle.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn pausing_reading_parks_the_node_on_its_window_until_reading_resumes() {
    const STATUSES: usize = 400;
    let sim = simulator(NodeOptions::default()).await;
    let mut node = TestNode::start(node_config(&sim), script(|_| async {}));
    assert!(matches!(node.next().await, NodeEvent::Fresh));
    let sim_node = sim.next_node().await.unwrap();

    sim.pause_reading();
    let padding = "x".repeat(1 << 10);
    let mut sender = tokio::spawn({
        let handle = node.handle.clone();
        async move {
            for i in 1..=STATUSES {
                let text = format!("{i} {padding}");
                handle
                    .background("s1".into(), BackgroundEvent::Status { text })
                    .await?;
            }
            Ok::<_, rax_tokio::SendError>(())
        }
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(300), &mut sender)
            .await
            .is_err(),
        "the node must park on its window while the simulator is not reading"
    );
    sim.resume_reading();

    for i in 1..=STATUSES {
        let (_, event) = sim_node.next_background().await.unwrap();
        let Open::Known(BackgroundEvent::Status { text }) = event else {
            panic!("expected status {i}, got {event:?}")
        };
        assert!(text.starts_with(&format!("{i} ")), "expected status {i}");
    }
    within(sender).await.unwrap().unwrap();
    node.handle.close().await;
}
