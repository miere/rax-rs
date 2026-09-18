#![allow(dead_code, clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use rax::credential::CredentialRenewal;
use rax::event::StopReason;
use rax::id::{RequestId, SessionId};
use rax::session::{Initialized, NodeCapabilities, PromptAccepted, SessionCreated, ToolGate};
use rax::tool::{ToolCallStatus, ToolCallUpdate, ToolKind};
use rax::{ContentBlock, Event, GatewayCall, GatewayReply, ToolCall, ToolVerdict};
use rax_sim::{NodeOptions, SimConfig, Simulator};
use rax_tokio::gateway::GatewayConfig;
use rax_tokio::node::{NodeConfig, NodeEvent, NodeEvents, NodeHandle, NodeLink};
use tokio::sync::{Mutex, mpsc};

pub const TOKEN: &str = "mrtg_node_0123456789abcdef_c2VjcmV0LXNlY3JldC1zZWNyZXQtc2VjcmV0LXNlY3I";
pub const IDENTITY: &str = "node-1";

pub async fn within<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(30), future)
        .await
        .expect("test guard timed out")
}

pub async fn simulator(node: NodeOptions) -> Simulator {
    let config = SimConfig {
        gateway: GatewayConfig {
            keepalive: Duration::from_secs(1),
            handshake_timeout: Duration::from_secs(5),
            ..Default::default()
        },
        node,
        ..Default::default()
    };
    Simulator::start(config.with_token(TOKEN, IDENTITY))
        .await
        .unwrap()
}

pub fn node_config(sim: &Simulator) -> NodeConfig {
    NodeConfig {
        endpoints: vec![sim.url()],
        token: TOKEN.to_owned(),
        keepalive: Duration::from_secs(1),
        backoff_min: Duration::from_millis(5),
        backoff_max: Duration::from_millis(50),
        handshake_timeout: Duration::from_secs(5),
        ..Default::default()
    }
}

pub fn text(text: &str) -> Vec<rax::Open<ContentBlock>> {
    vec![ContentBlock::text(text).into()]
}

pub type Script = Arc<dyn Fn(Turn) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

pub fn script<F, Fut>(turn: F) -> Script
where
    F: Fn(Turn) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    Arc::new(move |t| Box::pin(turn(t)))
}

pub struct Turn {
    pub node: NodeHandle,
    pub stream: RequestId,
    pub session_id: SessionId,
    verdicts: Arc<Mutex<mpsc::UnboundedReceiver<ToolVerdict>>>,
}

impl Turn {
    pub async fn event(&self, event: Event) {
        self.node.event(self.stream.clone(), event).await.unwrap();
    }

    pub async fn say(&self, text: &str) {
        let content = ContentBlock::text(text).into();
        self.event(Event::Message { content }).await;
    }

    pub async fn tool_call(&self, id: &str, name: &str) {
        let tool_call = ToolCall {
            id: id.into(),
            name: name.into(),
            title: None,
            kind: ToolKind::Execute,
            input: None,
            content: vec![],
        };
        self.event(Event::ToolCall { tool_call }).await;
    }

    pub async fn update(&self, id: &str, status: ToolCallStatus) {
        let tool_call_update = ToolCallUpdate {
            id: id.into(),
            status,
            title: None,
            content: vec![],
            output: None,
        };
        self.event(Event::ToolCallUpdate { tool_call_update }).await;
    }

    pub async fn verdict(&self) -> ToolVerdict {
        self.verdicts.lock().await.recv().await.unwrap()
    }

    pub async fn finish(&self) {
        let stop_reason = Some(StopReason::EndTurn);
        self.event(Event::Complete { stop_reason }).await;
        self.node.end(self.stream.clone()).await.unwrap();
    }
}

/// A minimal node: answers every gateway call and runs `script` for each prompt.
pub struct TestNode {
    pub handle: NodeHandle,
    lifecycle: mpsc::UnboundedReceiver<NodeEvent>,
}

impl TestNode {
    pub fn start(config: NodeConfig, script: Script) -> Self {
        let (handle, events) = NodeLink::start(config).unwrap();
        let (lifecycle_tx, lifecycle) = mpsc::unbounded_channel();
        tokio::spawn(serve(handle.clone(), events, script, lifecycle_tx));
        Self { handle, lifecycle }
    }

    pub async fn next(&mut self) -> NodeEvent {
        within(self.lifecycle.recv())
            .await
            .expect("node events ended")
    }

    /// Skips disconnects, whose number depends on how fast each side notices a dead socket.
    pub async fn next_settled(&mut self) -> NodeEvent {
        loop {
            match self.next().await {
                NodeEvent::Disconnected { .. } => {}
                other => return other,
            }
        }
    }
}

async fn serve(
    handle: NodeHandle,
    mut events: NodeEvents,
    script: Script,
    lifecycle: mpsc::UnboundedSender<NodeEvent>,
) {
    let (verdicts_tx, verdicts) = mpsc::unbounded_channel();
    let verdicts = Arc::new(Mutex::new(verdicts));
    let mut sessions = 0;
    while let Some(event) = events.recv().await {
        let NodeEvent::Request { id, call } = event else {
            match event {
                NodeEvent::Verdict(verdict) => {
                    let _ = verdicts_tx.send(verdict);
                }
                other => {
                    let _ = lifecycle.send(other);
                }
            }
            continue;
        };
        let reply = match call {
            GatewayCall::Initialize(offer) => GatewayReply::Initialize(Initialized {
                protocol_version: offer.protocol_version,
                capabilities: NodeCapabilities {
                    interruptible: Some(true),
                    tool_gate: ToolGate::EveryCall,
                    ..Default::default()
                },
            }),
            GatewayCall::NewSession(_) => {
                sessions += 1;
                GatewayReply::NewSession(SessionCreated {
                    session_id: format!("s{sessions}").into(),
                    unhandled: vec![],
                })
            }
            GatewayCall::Prompt(prompt) => {
                let _ = handle
                    .reply(id.clone(), GatewayReply::Prompt(PromptAccepted::default()))
                    .await;
                tokio::spawn(script(Turn {
                    node: handle.clone(),
                    stream: id,
                    session_id: prompt.session_id,
                    verdicts: verdicts.clone(),
                }));
                continue;
            }
            GatewayCall::Cancel(_) => GatewayReply::Cancel,
            GatewayCall::CloseSession(_) => GatewayReply::CloseSession,
            GatewayCall::RenewCredential => {
                GatewayReply::RenewCredential(CredentialRenewal::NothingToRenew)
            }
            GatewayCall::Close => GatewayReply::Close,
        };
        let _ = handle.reply(id, reply).await;
    }
}
