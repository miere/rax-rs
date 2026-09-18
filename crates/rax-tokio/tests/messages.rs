#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod support;

use rax::credential::CredentialHealth;
use rax::event::StopReason;
use rax::session::{Initialize, Initialized, Prompt, PromptAccepted};
use rax::tool::ToolKind;
use rax::{
    ContentBlock, Decision, Error, ErrorKind, Event, GatewayCall, GatewayReply, NodeCall,
    NodeReply, Open, ToolCall, ToolVerdict,
};
use rax_tokio::CallError;
use rax_tokio::gateway::{LinkEvent, NodeIdentity};
use rax_tokio::node::NodeEvent;
use serde_json::json;
use support::*;

#[tokio::test(flavor = "multi_thread")]
async fn the_first_connection_is_fresh_and_initialize_round_trips() {
    let (server, mut new_links) = gateway(gateway_config()).await;
    let Pair {
        node,
        mut node_events,
        link,
        ..
    } = connect(&mut new_links, node_config(server.local_addr())).await;
    assert_eq!(link.identity(), &NodeIdentity(IDENTITY.to_owned()));

    let pending = link
        .call(GatewayCall::Initialize(Initialize {
            protocol_version: 1,
            capabilities: Default::default(),
        }))
        .await
        .unwrap();
    let NodeEvent::Request {
        id,
        call: GatewayCall::Initialize(offer),
    } = next(&mut node_events).await
    else {
        panic!("expected initialize")
    };
    assert_eq!(id, pending.id);
    node.reply(
        id,
        GatewayReply::Initialize(Initialized {
            protocol_version: offer.protocol_version,
            capabilities: Default::default(),
        }),
    )
    .await
    .unwrap();
    let reply = within(pending.reply).await.unwrap();
    assert!(matches!(
        reply,
        GatewayReply::Initialize(Initialized {
            protocol_version: 1,
            ..
        })
    ));
    node.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_prompt_turn_streams_in_order_and_ends() {
    let (server, mut new_links) = gateway(gateway_config()).await;
    let Pair {
        node,
        mut node_events,
        link,
        ..
    } = connect(&mut new_links, node_config(server.local_addr())).await;

    let mut turn = link
        .call(GatewayCall::Prompt(Prompt {
            session_id: "s1".into(),
            content: vec![ContentBlock::text("clean up").into()],
        }))
        .await
        .unwrap();
    let NodeEvent::Request {
        id,
        call: GatewayCall::Prompt(_),
    } = next(&mut node_events).await
    else {
        panic!("expected a prompt")
    };
    node.reply(id.clone(), GatewayReply::Prompt(PromptAccepted::default()))
        .await
        .unwrap();
    let tool_call = ToolCall {
        id: "tc1".into(),
        name: "Bash".into(),
        title: Some("rm -rf target".into()),
        kind: ToolKind::Execute,
        input: Some(json!({"command": "rm -rf target"})),
        content: vec![],
    };
    node.event(
        id.clone(),
        Event::ToolCall {
            tool_call: tool_call.clone(),
        },
    )
    .await
    .unwrap();

    assert!(matches!(
        within(turn.reply).await,
        Ok(GatewayReply::Prompt(_))
    ));
    assert_eq!(
        within(turn.events.recv()).await,
        Some(Open::Known(Event::ToolCall { tool_call }))
    );
    link.verdict(ToolVerdict {
        id: "tc1".into(),
        decision: Decision::Allow,
    })
    .await
    .unwrap();

    let NodeEvent::Verdict(verdict) = next(&mut node_events).await else {
        panic!("expected a verdict")
    };
    assert_eq!(verdict.id, "tc1".into());
    let complete = Event::Complete {
        stop_reason: Some(StopReason::EndTurn),
    };
    node.event(id.clone(), complete.clone()).await.unwrap();
    node.end(id).await.unwrap();

    assert_eq!(
        within(turn.events.recv()).await,
        Some(Open::Known(complete))
    );
    assert_eq!(within(turn.events.recv()).await, None);
    node.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn node_calls_get_their_reply_or_their_fault() {
    let (server, mut new_links) = gateway(gateway_config()).await;
    let Pair {
        node,
        link,
        mut link_events,
        ..
    } = connect(&mut new_links, node_config(server.local_addr())).await;
    let health = || {
        NodeCall::CredentialHealth(CredentialHealth {
            credential: "anthropic".into(),
            degraded: true,
            reason: Some("token expired".into()),
            since: None,
            expires_at: None,
        })
    };

    let call = tokio::spawn({
        let node = node.clone();
        async move { node.call(health()).await }
    });
    let LinkEvent::Request {
        id,
        call: NodeCall::CredentialHealth(report),
    } = next_link(&mut link_events).await
    else {
        panic!("expected credential.health")
    };
    assert!(report.degraded);
    link.reply(id, NodeReply::CredentialHealth).await.unwrap();
    assert!(matches!(
        within(call).await.unwrap(),
        Ok(NodeReply::CredentialHealth)
    ));

    let call = tokio::spawn({
        let node = node.clone();
        async move { node.call(health()).await }
    });
    let LinkEvent::Request { id, .. } = next_link(&mut link_events).await else {
        panic!("expected credential.health")
    };
    link.fault(
        id,
        Error::new(ErrorKind::Credential, "a sign-in is already open"),
    )
    .await
    .unwrap();
    let Err(CallError::Fault(fault)) = within(call).await.unwrap() else {
        panic!("expected a fault")
    };
    assert_eq!(fault.kind, ErrorKind::Credential);
    node.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_method_is_faulted_unsupported_by_the_node_runtime() {
    let (server, mut new_links) = gateway(gateway_config()).await;
    let Pair {
        node,
        mut node_events,
        link,
        ..
    } = connect(&mut new_links, node_config(server.local_addr())).await;

    let mut fork = link
        .call(Open::Unknown {
            name: "session.fork".into(),
            raw: json!({"method": "session.fork", "body": {"session_id": "s1"}}),
        })
        .await
        .unwrap();
    let Err(CallError::Fault(fault)) = within(fork.reply).await else {
        panic!("expected a fault")
    };
    assert_eq!(fault.kind, ErrorKind::Unsupported);
    assert_eq!(within(fork.events.recv()).await, None);

    link.call(GatewayCall::RenewCredential).await.unwrap();
    assert!(
        matches!(
            next(&mut node_events).await,
            NodeEvent::Request {
                call: GatewayCall::RenewCredential,
                ..
            }
        ),
        "the unknown call must never reach the application"
    );
    node.close().await;
}
