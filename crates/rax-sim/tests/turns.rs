#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod support;

use std::collections::BTreeSet;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use rax::attachment::Attachment;
use rax::credential::CredentialHealth;
use rax::event::StopReason;
use rax::resource::ReadResource;
use rax::session::{GatewayCapabilities, SessionDurability, ToolGate};
use rax::tool::{DeniedBy, ToolCallStatus};
use rax::{Decision, Event, NodeCall, NodeReply, PROTOCOL_VERSION};
use rax_sim::{Entry, Match, NodeMethod, NodeOptions, Overlap, Transfer, VerdictPolicy};
use rax_tokio::CallError;
use rax_tokio::node::{NodeEvent, NodeLink};
use support::*;
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};

fn tool_turn() -> Script {
    script(|turn| async move {
        turn.say("working on it").await;
        for (id, name) in [("tc1", "Bash"), ("tc2", "Write")] {
            turn.tool_call(id, name).await;
            let status = match turn.verdict().await.decision {
                Decision::Allow => {
                    turn.update(id, ToolCallStatus::InProgress).await;
                    ToolCallStatus::Completed
                }
                Decision::Deny { .. } => ToolCallStatus::Denied,
            };
            turn.update(id, status).await;
        }
        turn.finish().await;
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn a_prompt_turn_runs_under_a_scripted_verdict_policy() {
    let verdicts = VerdictPolicy::script(|call| match call.name.as_str() {
        "Bash" => Some(Decision::Allow),
        _ => Some(Decision::Deny {
            by: DeniedBy::Policy,
            reason: Some("writes are not allowed here".into()),
        }),
    });
    let sim = simulator(NodeOptions {
        verdicts,
        ..Default::default()
    })
    .await;
    let mut node = TestNode::start(node_config(&sim), tool_turn());
    assert!(matches!(node.next().await, NodeEvent::Fresh));
    let sim_node = sim.next_node().await.unwrap();

    let initialized = sim_node
        .initialize(GatewayCapabilities::default())
        .await
        .unwrap();
    assert_eq!(initialized.protocol_version, PROTOCOL_VERSION);
    assert_eq!(initialized.capabilities.tool_gate, ToolGate::EveryCall);
    assert_eq!(
        initialized.capabilities.sessions,
        SessionDurability::Ephemeral
    );
    let session = sim_node.new_session(vec![]).await.unwrap().session_id;
    let mut turn = sim_node.prompt(session, text("tidy up")).await.unwrap();

    turn.expect(Match::message_contains("working"))
        .await
        .unwrap();
    turn.expect(Match::tool_call("Bash")).await.unwrap();
    turn.expect(Match::tool_call_update(ToolCallStatus::Completed))
        .await
        .unwrap();
    turn.expect(Match::tool_call("Write")).await.unwrap();
    turn.expect(Match::tool_call_update(ToolCallStatus::Denied))
        .await
        .unwrap();
    turn.expect(Match::complete(StopReason::EndTurn))
        .await
        .unwrap();
    assert_eq!(turn.until_end().await.unwrap(), vec![]);
    assert_eq!(turn.seen().len(), 7);

    let report = sim.report();
    let [turn_report] = report.turns.as_slice() else {
        panic!("expected one turn: {report:?}")
    };
    assert_eq!(&turn_report.stream, turn.id());
    assert!(!turn_report.faulted);
    assert_eq!(turn_report.reply_before_first_event, Some(true));
    let (reply, first, end) = (
        turn_report.to_reply.unwrap(),
        turn_report.to_first_event.unwrap(),
        turn_report.to_end.unwrap(),
    );
    assert!(reply <= first && first <= end, "{turn_report:?}");
    assert_eq!(report.tool_calls.len(), 2);
    for tool_call in &report.tool_calls {
        assert!(tool_call.verdict_latency.is_some(), "{tool_call:?}");
        assert!(tool_call.verdict_to_update.is_some(), "{tool_call:?}");
        assert!(tool_call.verdict_latency.unwrap() <= end, "{tool_call:?}");
    }
    assert_eq!(report.resumes.fresh, 1);
    assert_eq!(report.resumes.accepted + report.resumes.refused, 0);
    let json: rax_sim::Report =
        serde_json::from_str(&serde_json::to_string(&report).unwrap()).unwrap();
    assert_eq!(json, report);

    let path = format!(
        "{}/prompt-turn-{}.jsonl",
        env!("CARGO_TARGET_TMPDIR"),
        std::process::id()
    );
    let transcript = sim.transcript();
    transcript.write_to(&path).unwrap();
    let written: Vec<Entry> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(written, transcript.entries);
    std::fs::remove_file(&path).unwrap();
    node.handle.close().await;
}

struct Broken;

impl AsyncRead for Broken {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Err(io::Error::other("disk went away")))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn attachments_are_reassembled_and_linked_to_their_event() {
    let sim = simulator(NodeOptions::default()).await;
    let bytes: Vec<u8> = (0..300u32 << 10).map(|i| (i % 251) as u8).collect();
    let sent = bytes.clone();
    let turn_script = script(move |turn| {
        let bytes = sent.clone();
        async move {
            let transfer_id = turn.node.send_attachment(&bytes).await.unwrap();
            let attachment = Attachment {
                transfer_id,
                size: bytes.len() as u64,
                filename: Some("report.bin".into()),
                title: None,
                comment: None,
                mimetype: None,
            };
            turn.event(Event::Attachment { attachment }).await;
            let reader = (&b"a few bytes"[..]).chain(Broken);
            let _ = turn.node.send_attachment_from(reader, 64).await;
            turn.finish().await;
        }
    });
    let mut node = TestNode::start(node_config(&sim), turn_script);
    assert!(matches!(node.next().await, NodeEvent::Fresh));
    let sim_node = sim.next_node().await.unwrap();

    let mut turn = sim_node.prompt("s1", text("send the file")).await.unwrap();
    let Event::Attachment { attachment } = turn.expect(Match::attachment()).await.unwrap() else {
        panic!("expected an attachment")
    };
    assert_eq!(attachment.size, bytes.len() as u64);
    let received = sim_node.transfer(&attachment.transfer_id).await.unwrap();
    assert!(received == Transfer::Complete(bytes), "{received:?}");
    let (_, failed) = sim_node.next_transfer().await.unwrap();
    assert!(
        matches!(&failed, Transfer::Failed(reason) if reason.contains("disk went away")),
        "{failed:?}"
    );
    turn.until_end().await.unwrap();
    node.handle.close().await;
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
async fn node_calls_sent_before_the_last_was_answered_are_flagged() {
    let sim = simulator(NodeOptions::default()).await;
    let (node, mut events) = NodeLink::start(node_config(&sim)).unwrap();
    assert!(matches!(
        within(events.recv()).await,
        Some(NodeEvent::Fresh)
    ));
    let sim_node = sim.next_node().await.unwrap();

    let concurrent = [(); 2].map(|_| {
        let node = node.clone();
        tokio::spawn(async move { node.call(health()).await })
    });
    let first = sim_node.next_node_call().await.unwrap();
    let second = sim_node.next_node_call().await.unwrap();
    let expected = Overlap {
        earlier: first.id.clone(),
        later: second.id.clone(),
    };
    first.reply().await.unwrap();
    second.reply().await.unwrap();
    for call in concurrent {
        assert!(matches!(
            within(call).await.unwrap(),
            Ok(NodeReply::CredentialHealth)
        ));
    }
    let overlaps = sim_node.call_overlaps(&[NodeMethod::CredentialHealth]);
    assert_eq!(overlaps, std::slice::from_ref(&expected));

    let sequential = tokio::spawn({
        let node = node.clone();
        async move {
            node.call(health()).await.unwrap();
            node.call(health()).await.unwrap();
        }
    });
    for _ in 0..2 {
        sim_node
            .next_node_call()
            .await
            .unwrap()
            .reply()
            .await
            .unwrap();
    }
    within(sequential).await.unwrap();
    assert_eq!(
        sim_node.call_overlaps(&[NodeMethod::CredentialHealth]),
        [expected]
    );
    assert!(
        sim_node
            .call_overlaps(&[NodeMethod::SignInSettled])
            .is_empty()
    );

    sim_node.set_options(NodeOptions {
        auto_reply: BTreeSet::from([NodeMethod::CredentialHealth]),
        ..Default::default()
    });
    within(node.call(health())).await.unwrap();
    let log = sim_node.call_log();
    assert_eq!(log.len(), 5);
    assert!(
        log.iter().all(|record| record.answered.is_some()),
        "{log:?}"
    );
    node.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_resource_read_is_served_or_refused_as_gone() {
    let sim = simulator(NodeOptions::default()).await;
    let (node, mut events) = NodeLink::start(node_config(&sim)).unwrap();
    assert!(matches!(
        within(events.recv()).await,
        Some(NodeEvent::Fresh)
    ));
    let sim_node = sim.next_node().await.unwrap();
    let read = || ReadResource {
        uri: "gateway://files/F1".into(),
        max_bytes: None,
    };

    let reading = tokio::spawn({
        let node = node.clone();
        async move { node.read_resource(read()).await }
    });
    let call = sim_node.next_node_call().await.unwrap();
    assert_eq!(call.method(), NodeMethod::ReadResource);
    call.serve(b"quarterly figures", Some("text/csv".into()))
        .await
        .unwrap();
    let resource = within(reading).await.unwrap().unwrap();
    assert_eq!(resource.bytes, b"quarterly figures");

    let reading = tokio::spawn({
        let node = node.clone();
        async move { node.read_resource(read()).await }
    });
    sim_node
        .next_node_call()
        .await
        .unwrap()
        .reply()
        .await
        .unwrap();
    let Err(CallError::Fault(fault)) = within(reading).await.unwrap() else {
        panic!("expected a fault")
    };
    assert_eq!(fault.kind, rax::ErrorKind::NotFound);
    node.close().await;
}
