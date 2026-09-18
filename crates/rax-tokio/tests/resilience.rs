#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod support;

use std::sync::Arc;
use std::time::Duration;

use rax::credential::CredentialHealth;
use rax::event::BackgroundEvent;
use rax::{Decision, NodeCall, Open, ToolVerdict};
use rax_tokio::CallError;
use rax_tokio::gateway::LinkEvent;
use rax_tokio::node::{NodeEvent, NodeLink};
use support::*;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, watch};

const N: usize = 200;
const SEVERS: usize = 6;

struct Severer {
    proxy: Proxy,
    severed: Mutex<usize>,
    node_resumed: watch::Receiver<usize>,
    gateway_resumed: watch::Receiver<usize>,
}

impl Severer {
    async fn sever(&self) {
        let mut severed = self.severed.lock().await;
        *severed += 1;
        let target = *severed;
        self.proxy.sever();
        within(self.node_resumed.clone().wait_for(|n| *n >= target))
            .await
            .unwrap();
        within(self.gateway_resumed.clone().wait_for(|n| *n >= target))
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn severed_sockets_lose_and_repeat_nothing_in_either_direction() {
    let (server, mut new_links) = gateway(gateway_config_with_window(2048)).await;
    let proxy = Proxy::start(server.local_addr()).await;
    let mut config = node_config(proxy.addr());
    config.window_bytes = 2048;
    config.ack_threshold = 4;
    let Pair {
        node,
        mut node_events,
        link,
        mut link_events,
    } = connect(&mut new_links, config).await;

    let (node_resumed_tx, node_resumed) = watch::channel(0usize);
    let (gateway_resumed_tx, gateway_resumed) = watch::channel(0usize);
    let (verdicts_tx, mut verdicts_seen) = watch::channel(0usize);
    let (statuses_tx, mut statuses_seen) = watch::channel(0usize);

    let node_collector = tokio::spawn(async move {
        let mut verdicts = Vec::new();
        while let Some(event) = node_events.recv().await {
            match event {
                NodeEvent::Verdict(verdict) => {
                    verdicts.push(verdict.id.0);
                    verdicts_tx.send_replace(verdicts.len());
                }
                NodeEvent::Resumed => {
                    node_resumed_tx.send_modify(|n| *n += 1);
                }
                NodeEvent::Disconnected { .. } => {}
                other => panic!("node saw {other:?}"),
            }
        }
        verdicts
    });
    let gateway_collector = tokio::spawn(async move {
        let mut statuses = Vec::new();
        while let Some(event) = link_events.recv().await {
            match event {
                LinkEvent::Background {
                    event: Open::Known(BackgroundEvent::Status { text }),
                    ..
                } => {
                    statuses.push(text);
                    statuses_tx.send_replace(statuses.len());
                }
                LinkEvent::Resumed => {
                    gateway_resumed_tx.send_modify(|n| *n += 1);
                }
                LinkEvent::Disconnected { .. } => {}
                LinkEvent::Closed => break,
                other => panic!("gateway saw {other:?}"),
            }
        }
        statuses
    });

    let severer = Arc::new(Severer {
        proxy,
        severed: Mutex::new(0),
        node_resumed: node_resumed.clone(),
        gateway_resumed: gateway_resumed.clone(),
    });
    let every = N / SEVERS * 2;
    let node_sender = tokio::spawn({
        let (node, severer) = (node.clone(), severer.clone());
        async move {
            for i in 1..=N {
                let status = BackgroundEvent::Status {
                    text: i.to_string(),
                };
                node.background("s1".into(), status).await.unwrap();
                if i % every == every / 2 {
                    severer.sever().await;
                }
            }
        }
    });
    let gateway_sender = tokio::spawn({
        let (link, severer) = (link.clone(), severer.clone());
        async move {
            for i in 1..=N {
                let verdict = ToolVerdict {
                    id: i.to_string().into(),
                    decision: Decision::Allow,
                };
                link.verdict(verdict).await.unwrap();
                if i % every == 0 {
                    severer.sever().await;
                }
            }
        }
    });
    within(node_sender).await.unwrap();
    within(gateway_sender).await.unwrap();
    within(verdicts_seen.wait_for(|n| *n >= N)).await.unwrap();
    within(statuses_seen.wait_for(|n| *n >= N)).await.unwrap();

    node.close().await;
    link.close().await;
    let expected: Vec<String> = (1..=N).map(|i| i.to_string()).collect();
    assert_eq!(within(node_collector).await.unwrap(), expected);
    assert_eq!(within(gateway_collector).await.unwrap(), expected);
    assert_eq!(*node_resumed.borrow(), SEVERS);
    assert_eq!(*gateway_resumed.borrow(), SEVERS);
}

fn health() -> NodeCall {
    NodeCall::CredentialHealth(CredentialHealth {
        credential: "anthropic".into(),
        degraded: false,
        reason: None,
        since: None,
        expires_at: None,
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn a_gateway_that_lost_the_link_makes_the_node_start_fresh() {
    let (first, mut first_links) = gateway(gateway_config()).await;
    let (second, mut second_links) = gateway(gateway_config()).await;
    let proxy = Proxy::start(first.local_addr()).await;
    let Pair {
        node,
        mut node_events,
        mut link_events,
        ..
    } = connect(&mut first_links, node_config(proxy.addr())).await;

    let call = tokio::spawn({
        let node = node.clone();
        async move { node.call(health()).await }
    });
    assert!(matches!(
        next_link(&mut link_events).await,
        LinkEvent::Request { .. }
    ));

    proxy.retarget(second.local_addr());
    proxy.sever();
    assert!(matches!(
        next(&mut node_events).await,
        NodeEvent::Disconnected { .. }
    ));
    assert!(matches!(next(&mut node_events).await, NodeEvent::Fresh));
    assert!(matches!(
        within(call).await.unwrap(),
        Err(CallError::LinkReset)
    ));
    within(second_links.recv()).await.unwrap();
    node.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_link_retained_past_its_deadline_is_closed_and_refused() {
    let config = rax_tokio::gateway::GatewayConfig {
        retain_for: Duration::from_millis(50),
        ..gateway_config()
    };
    let (server, mut new_links) = gateway(config).await;
    let proxy = Proxy::start(server.local_addr()).await;
    let Pair {
        node,
        mut node_events,
        mut link_events,
        ..
    } = connect(&mut new_links, node_config(proxy.addr())).await;

    let call = tokio::spawn({
        let node = node.clone();
        async move { node.call(health()).await }
    });
    assert!(matches!(
        next_link(&mut link_events).await,
        LinkEvent::Request { .. }
    ));

    proxy.retarget(closed_port().await);
    proxy.sever();
    assert!(matches!(
        next_link(&mut link_events).await,
        LinkEvent::Disconnected { .. }
    ));
    assert!(matches!(
        next_link(&mut link_events).await,
        LinkEvent::Closed
    ));
    assert!(within(link_events.recv()).await.is_none());

    proxy.retarget(server.local_addr());
    assert!(matches!(
        next(&mut node_events).await,
        NodeEvent::Disconnected { .. }
    ));
    assert!(matches!(next(&mut node_events).await, NodeEvent::Fresh));
    assert!(matches!(
        within(call).await.unwrap(),
        Err(CallError::LinkReset)
    ));
    within(new_links.recv()).await.unwrap();
    node.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejected_credential_stops_dialling() {
    let (server, _new_links) = gateway(gateway_config()).await;
    let proxy = Proxy::start(server.local_addr()).await;
    let mut config = node_config(proxy.addr());
    config.token = "not-the-token".into();
    let (_node, mut node_events) = NodeLink::start(config).unwrap();

    assert!(matches!(
        next(&mut node_events).await,
        NodeEvent::CredentialRejected
    ));
    assert!(within(node_events.recv()).await.is_none());
    assert_eq!(proxy.accepts(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_full_window_parks_the_sender_until_the_gateway_reads_again() {
    let config = rax_tokio::gateway::GatewayConfig {
        events_capacity: 4,
        ..gateway_config_with_window(512)
    };
    let (server, mut new_links) = gateway(config).await;
    let mut node_config = node_config(server.local_addr());
    node_config.window_bytes = 512;
    node_config.ack_threshold = 2;
    let Pair {
        node,
        mut link_events,
        ..
    } = connect(&mut new_links, node_config).await;

    let mut sender = tokio::spawn({
        let node = node.clone();
        async move {
            for i in 1..=N {
                let status = BackgroundEvent::Status {
                    text: i.to_string(),
                };
                node.background("s1".into(), status).await?;
            }
            Ok::<_, rax_tokio::SendError>(())
        }
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(300), &mut sender)
            .await
            .is_err(),
        "the sender must park on the window, not finish or fail"
    );

    for i in 1..=N {
        let LinkEvent::Background {
            event: Open::Known(BackgroundEvent::Status { text }),
            ..
        } = next_link(&mut link_events).await
        else {
            panic!("expected status {i}")
        };
        assert_eq!(text, i.to_string());
    }
    within(sender).await.unwrap().unwrap();
    node.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_wrong_path_is_refused_before_the_upgrade_and_not_redialled() {
    let (server, _new_links) = gateway(gateway_config()).await;
    let proxy = Proxy::start(server.local_addr()).await;
    let mut config = node_config(proxy.addr());
    config.endpoints = vec![format!("ws://{}/elsewhere", proxy.addr())];
    let (_node, mut node_events) = NodeLink::start(config).unwrap();

    assert!(matches!(
        next(&mut node_events).await,
        NodeEvent::Refused { status: 404 }
    ));
    assert!(within(node_events.recv()).await.is_none());
    assert_eq!(proxy.accepts(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tls_dial_fails_and_redials_rather_than_crashing() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = node_config(listener.local_addr().unwrap());
    config.endpoints = vec![format!("wss://{}", listener.local_addr().unwrap())];
    let (node, _node_events) = NodeLink::start(config).unwrap();
    for _ in 0..2 {
        let (socket, _) = within(listener.accept()).await.unwrap();
        drop(socket);
    }
    node.close().await;
}
