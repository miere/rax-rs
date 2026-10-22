#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! The binary as someone would run it: minting from the command line, then serving a node and a
//! client of the same person, whose session lands on the node.

use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use rax::id::SessionId;
use rax::session::{GatewayCapabilities, Initialized, NodeCapabilities, SessionCreated, ToolGate};
use rax::{GatewayCall, GatewayReply};
use rax_sim::{LinkChange, NodeOptions, SimNode};
use rax_tokio::dial::DialConfig;
use rax_tokio::node::{NodeConfig, NodeEvent, NodeLink};

const BIN: &str = env!("CARGO_BIN_EXE_rax-gateway");

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(20), future)
        .await
        .expect("timed out")
}

fn gateway(dir: &Path, args: &[&str]) -> std::process::Output {
    Command::new(BIN)
        .arg("--config")
        .arg(dir.join("rax-gateway.toml"))
        .args(args)
        .output()
        .unwrap()
}

fn token(dir: &Path, args: &[&str]) -> String {
    let out = gateway(dir, args);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().to_owned()
}

/// Killed when the test ends, however it ends.
struct Serving(Child);

impl Drop for Serving {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_runs_a_session_on_its_owners_node_through_the_binary() {
    let dir = tempfile::tempdir().unwrap();
    let addr = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    std::fs::write(
        dir.path().join("rax-gateway.toml"),
        format!("listen = \"{addr}\"\ndatabase = \"gateway.db\"\n"),
    )
    .unwrap();
    assert!(
        gateway(dir.path(), &["admin", "set", "alice"])
            .status
            .success()
    );
    let node_token = token(
        dir.path(),
        &["node", "mint", "--owner", "alice", "--name", "laptop"],
    );
    let client_token = token(
        dir.path(),
        &["client", "mint", "--owner", "alice", "--name", "editor"],
    );
    assert!(node_token.starts_with("rax_node_") && client_token.starts_with("rax_user_"));

    let _serving = Serving(
        Command::new(BIN)
            .arg("--config")
            .arg(dir.path().join("rax-gateway.toml"))
            .arg("serve")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    within(async {
        while tokio::net::TcpStream::connect(addr).await.is_err() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    let second = gateway(dir.path(), &["serve"]);
    assert!(
        !second.status.success(),
        "a second gateway served the same database"
    );

    let url = format!("ws://{addr}");
    let (node, mut events) = NodeLink::start(NodeConfig {
        endpoints: vec![url.clone()],
        token: node_token,
        ..Default::default()
    })
    .unwrap();
    loop {
        match within(events.recv()).await.unwrap() {
            NodeEvent::Request {
                id,
                call: GatewayCall::Initialize(_),
            } => {
                let reply = GatewayReply::Initialize(Initialized {
                    protocol_version: rax::PROTOCOL_VERSION,
                    capabilities: NodeCapabilities {
                        tool_gate: ToolGate::EveryCall,
                        tool_groups: true,
                        ..Default::default()
                    },
                    metadata: Default::default(),
                });
                node.reply(id, reply).await.unwrap();
                break;
            }
            NodeEvent::Fresh => {}
            other => panic!("expected initialize, got {other:?}"),
        }
    }

    let client = SimNode::dial(
        DialConfig {
            endpoints: vec![url],
            token: client_token,
            ..Default::default()
        },
        NodeOptions::default(),
        Duration::from_secs(10),
    )
    .unwrap();
    assert!(matches!(
        within(client.next_link_change()).await.unwrap(),
        LinkChange::Fresh
    ));
    within(client.initialize(GatewayCapabilities::default()))
        .await
        .unwrap();
    // The node may attach a moment after it answers; the client retries until it has.
    let opening = tokio::spawn({
        let client = client.clone();
        async move {
            loop {
                match client.new_session(vec![]).await {
                    Ok(created) => return created,
                    Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
                }
            }
        }
    });
    loop {
        if let NodeEvent::Request {
            id,
            call: GatewayCall::NewSession(_),
        } = within(events.recv()).await.unwrap()
        {
            let created = SessionCreated {
                session_id: SessionId("on-laptop".into()),
                unhandled: vec![],
            };
            node.reply(id, GatewayReply::NewSession(created))
                .await
                .unwrap();
            break;
        }
    }
    let created = within(opening).await.unwrap();
    assert_ne!(
        created.session_id.0, "on-laptop",
        "the client saw the node's own id"
    );
}
