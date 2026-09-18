use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use rax::event::BackgroundEvent;
use rax::id::{RequestId, SessionId, ToolCallId};
use rax::link::NO_EPOCH;
use rax::{Event, GatewayCall, GatewayMessage, NodeMessage, Open, ToolCall};
use serde::{Deserialize, Serialize};

use crate::record::{ConnectionId, Delivered, Direction, Handshake, Transcript, WireFrame, micros};

/// Measured at the proxy, so the numbers include the node's network stack but not the simulator's.
/// Durations serialise as whole microseconds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Report {
    pub turns: Vec<TurnReport>,
    pub tool_calls: Vec<ToolCallReport>,
    pub resumes: ResumeCounts,
    pub handshakes_by_status: BTreeMap<u16, usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnReport {
    pub epoch: u64,
    pub stream: RequestId,
    pub session_id: SessionId,
    #[serde(with = "micros")]
    pub sent: Duration,
    #[serde(with = "micros::option")]
    pub to_reply: Option<Duration>,
    pub faulted: bool,
    #[serde(with = "micros::option")]
    pub to_first_event: Option<Duration>,
    #[serde(with = "micros::option")]
    pub to_end: Option<Duration>,
    /// The protocol requires the acceptance first; absent when either never arrived.
    pub reply_before_first_event: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCallReport {
    pub epoch: u64,
    /// Absent for a background tool call, which belongs to no turn.
    pub stream: Option<RequestId>,
    pub id: ToolCallId,
    pub name: String,
    #[serde(with = "micros")]
    pub announced: Duration,
    #[serde(with = "micros::option")]
    pub verdict_latency: Option<Duration>,
    #[serde(with = "micros::option")]
    pub verdict_to_update: Option<Duration>,
}

/// A fresh dial is a refusal too, but an expected one, so it is counted apart.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumeCounts {
    pub fresh: usize,
    pub accepted: usize,
    pub refused: usize,
}

pub(crate) fn build(handshakes: &[Handshake], transcript: &Transcript) -> Report {
    let node = transcript.node_messages();
    let gateway = transcript.gateway_messages();
    let mut handshakes_by_status = BTreeMap::new();
    for status in handshakes.iter().filter_map(|handshake| handshake.status) {
        *handshakes_by_status.entry(status).or_insert(0) += 1;
    }
    Report {
        turns: turns(&node, &gateway),
        tool_calls: tool_calls(&node, &gateway),
        resumes: resumes(transcript),
        handshakes_by_status,
    }
}

fn turns(
    node: &[Delivered<Open<NodeMessage>>],
    gateway: &[Delivered<Open<GatewayMessage>>],
) -> Vec<TurnReport> {
    gateway
        .iter()
        .filter_map(|sent| {
            let Open::Known(GatewayMessage::Request {
                id,
                call: Open::Known(GatewayCall::Prompt(prompt)),
            }) = &sent.message
            else {
                return None;
            };
            let on_link = || {
                node.iter()
                    .enumerate()
                    .filter(|(_, delivered)| delivered.epoch == sent.epoch)
            };
            let reply = on_link().find_map(|(index, delivered)| match &delivered.message {
                Open::Known(NodeMessage::Reply { id: replied, .. }) if replied == id => {
                    Some((index, delivered.at, false))
                }
                Open::Known(NodeMessage::Fault { id: faulted, .. }) if faulted == id => {
                    Some((index, delivered.at, true))
                }
                _ => None,
            });
            let first_event = on_link().find(|(_, delivered)| {
                matches!(&delivered.message, Open::Known(NodeMessage::Event { stream, .. }) if stream == id)
            });
            let end = on_link().find(|(_, delivered)| {
                matches!(&delivered.message, Open::Known(NodeMessage::End { stream }) if stream == id)
            });
            let since = |at: Duration| at.saturating_sub(sent.at);
            Some(TurnReport {
                epoch: sent.epoch,
                stream: id.clone(),
                session_id: prompt.session_id.clone(),
                sent: sent.at,
                to_reply: reply.map(|(_, at, _)| since(at)),
                faulted: reply.is_some_and(|(_, _, faulted)| faulted),
                to_first_event: first_event.map(|(_, delivered)| since(delivered.at)),
                to_end: end.map(|(_, delivered)| since(delivered.at)),
                reply_before_first_event: reply
                    .zip(first_event)
                    .map(|((reply, _, _), (event, _))| reply < event),
            })
        })
        .collect()
}

fn tool_calls(
    node: &[Delivered<Open<NodeMessage>>],
    gateway: &[Delivered<Open<GatewayMessage>>],
) -> Vec<ToolCallReport> {
    node.iter()
        .enumerate()
        .filter_map(|(index, announced)| {
            let (stream, tool_call) = announced_tool_call(&announced.message)?;
            let verdict = gateway.iter().find(|sent| {
                sent.epoch == announced.epoch
                    && sent.at >= announced.at
                    && matches!(&sent.message, Open::Known(GatewayMessage::Verdict { body }) if body.id == tool_call.id)
            });
            let update = verdict.and_then(|verdict| {
                node.iter().skip(index + 1).find(|later| {
                    later.epoch == announced.epoch
                        && later.at >= verdict.at
                        && updates(&later.message, &tool_call.id)
                })
            });
            Some(ToolCallReport {
                epoch: announced.epoch,
                stream,
                id: tool_call.id.clone(),
                name: tool_call.name.clone(),
                announced: announced.at,
                verdict_latency: verdict.map(|verdict| verdict.at.saturating_sub(announced.at)),
                verdict_to_update: verdict
                    .zip(update)
                    .map(|(verdict, update)| update.at.saturating_sub(verdict.at)),
            })
        })
        .collect()
}

fn announced_tool_call(message: &Open<NodeMessage>) -> Option<(Option<RequestId>, &ToolCall)> {
    match message {
        Open::Known(NodeMessage::Event {
            stream,
            body: Open::Known(Event::ToolCall { tool_call }),
        }) => Some((Some(stream.clone()), tool_call)),
        Open::Known(NodeMessage::Background {
            body: Open::Known(BackgroundEvent::ToolCall { tool_call }),
            ..
        }) => Some((None, tool_call)),
        _ => None,
    }
}

fn updates(message: &Open<NodeMessage>, id: &ToolCallId) -> bool {
    match message {
        Open::Known(NodeMessage::Event {
            body: Open::Known(Event::ToolCallUpdate { tool_call_update }),
            ..
        }) => tool_call_update.id == *id,
        Open::Known(NodeMessage::Background {
            body: Open::Known(BackgroundEvent::ToolCallUpdate { tool_call_update }),
            ..
        }) => tool_call_update.id == *id,
        _ => false,
    }
}

fn resumes(transcript: &Transcript) -> ResumeCounts {
    let mut asked: HashMap<ConnectionId, u64> = HashMap::new();
    let mut counts = ResumeCounts::default();
    for entry in &transcript.entries {
        match (&entry.frame, entry.direction) {
            (WireFrame::Resume { epoch, .. }, Direction::NodeToGateway) => {
                asked.entry(entry.connection).or_insert(*epoch);
            }
            (WireFrame::Resumed { ok, .. }, Direction::GatewayToNode) => {
                match (ok, asked.get(&entry.connection)) {
                    (true, _) => counts.accepted += 1,
                    (false, Some(&NO_EPOCH)) => counts.fresh += 1,
                    (false, _) => counts.refused += 1,
                }
            }
            _ => {}
        }
    }
    counts
}
