use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rax_tokio::gateway::{GatewayConfig, GatewayServer, NewLinks, NodeIdentity};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::error::SimError;
use crate::inbox::{Inbox, lock};
use crate::node::SimNode;
use crate::policy::NodeOptions;
use crate::proxy::{Faults, Proxy, Upgrades};
use crate::record::{Handshake, Records, Transcript};
use crate::report::{self, Report};

type Tokens = Arc<Mutex<HashMap<String, NodeIdentity>>>;

#[derive(Debug, Clone)]
pub struct SimConfig {
    pub tokens: HashMap<String, NodeIdentity>,
    pub gateway: GatewayConfig,
    pub node: NodeOptions,
    /// Bounds every wait the simulator offers, so a silent node fails a test instead of hanging it.
    pub wait: Duration,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            tokens: HashMap::new(),
            gateway: GatewayConfig::default(),
            node: NodeOptions::default(),
            wait: Duration::from_secs(10),
        }
    }
}

impl SimConfig {
    pub fn with_token(mut self, token: impl Into<String>, identity: impl Into<String>) -> Self {
        self.tokens
            .insert(token.into(), NodeIdentity(identity.into()));
        self
    }
}

struct Gateway {
    _server: GatewayServer,
    forward: JoinHandle<()>,
}

impl Drop for Gateway {
    fn drop(&mut self) {
        self.forward.abort();
    }
}

/// The node dials the proxy, never the gateway, so every byte is recorded and every fault can be
/// injected without the node's cooperation.
pub struct Simulator {
    tokens: Tokens,
    config: SimConfig,
    faults: Arc<Faults>,
    records: Arc<watch::Sender<Records>>,
    proxy: Proxy,
    gateway: tokio::sync::Mutex<Gateway>,
    nodes: Arc<Mutex<Vec<SimNode>>>,
    arrivals: Arc<Inbox<SimNode>>,
}

impl Simulator {
    pub async fn start(config: SimConfig) -> Result<Self, SimError> {
        let tokens: Tokens = Arc::new(Mutex::new(config.tokens.clone()));
        let records = Arc::new(watch::Sender::new(Records::new()));
        let nodes = Arc::default();
        let arrivals = Arc::new(Inbox::new());
        let (server, new_links) = bind(&tokens, &config).await?;
        let faults = Arc::new(Faults::new(server.local_addr()));
        let proxy = Proxy::start(faults.clone(), records.clone()).await?;
        let simulator = Self {
            gateway: tokio::sync::Mutex::new(Gateway {
                _server: server,
                forward: tokio::spawn(forward(
                    new_links,
                    ForwardTo {
                        nodes: Arc::clone(&nodes),
                        arrivals: Arc::clone(&arrivals),
                        options: config.node.clone(),
                        wait: config.wait,
                        records: records.clone(),
                    },
                )),
            }),
            tokens,
            config,
            faults,
            records,
            proxy,
            nodes,
            arrivals,
        };
        Ok(simulator)
    }

    pub fn url(&self) -> String {
        format!("ws://{}", self.proxy.addr())
    }

    pub fn allow(&self, token: impl Into<String>, identity: impl Into<String>) {
        lock(&self.tokens).insert(token.into(), NodeIdentity(identity.into()));
    }

    /// Refuses the token's next handshake and closes its identity's live link, as a gateway
    /// rechecking credentials would.
    pub async fn revoke(&self, token: &str) {
        let Some(identity) = lock(&self.tokens).remove(token) else {
            return;
        };
        let revoked: Vec<SimNode> = lock(&self.nodes)
            .iter()
            .filter(|node| *node.identity() == identity)
            .cloned()
            .collect();
        for node in revoked {
            node.end_link().await;
        }
    }

    pub fn refuse_upgrades(&self, status: u16) {
        self.faults.set_upgrades(Upgrades::Refuse(status));
    }

    /// Closes each connection without an HTTP answer, which a node sees as an unreachable gateway.
    pub fn hang_up_upgrades(&self) {
        self.faults.set_upgrades(Upgrades::HangUp);
    }

    pub fn accept_upgrades(&self) {
        self.faults.set_upgrades(Upgrades::Forward);
    }

    /// Drops every live TCP connection both ways; the gateway keeps each link, so nodes resume.
    pub fn sever(&self) {
        self.faults.sever();
    }

    /// Swaps in a gateway that holds no links. The proxy points at it before any connection is
    /// cut, so the node's next resume is refused.
    pub async fn restart(&self) -> Result<(), SimError> {
        let (server, new_links) = bind(&self.tokens, &self.config).await?;
        let mut gateway = self.gateway.lock().await;
        self.faults.retarget(server.local_addr());
        self.faults.sever();
        *gateway = Gateway {
            _server: server,
            forward: tokio::spawn(forward(
                new_links,
                ForwardTo {
                    nodes: Arc::clone(&self.nodes),
                    arrivals: Arc::clone(&self.arrivals),
                    options: self.config.node.clone(),
                    wait: self.config.wait,
                    records: self.records.clone(),
                },
            )),
        };
        drop(gateway);
        let forgotten = std::mem::take(&mut *lock(&self.nodes));
        for node in forgotten {
            node.end_link().await;
        }
        Ok(())
    }

    /// The node's writes pile up in its window rather than in the simulator, exercising back-pressure.
    pub fn pause_reading(&self) {
        self.faults.set_reading(false);
    }

    pub fn resume_reading(&self) {
        self.faults.set_reading(true);
    }

    pub async fn next_node(&self) -> Result<SimNode, SimError> {
        self.arrivals.next(self.config.wait, "a fresh link").await
    }

    pub fn handshakes(&self) -> Vec<Handshake> {
        self.records.borrow().handshakes.clone()
    }

    pub async fn wait_for_handshakes(
        &self,
        waiting_for: &str,
        done: impl Fn(&[Handshake]) -> bool,
    ) -> Result<Vec<Handshake>, SimError> {
        let mut records = self.records.subscribe();
        let waited = tokio::time::timeout(
            self.config.wait,
            records.wait_for(|records| done(&records.handshakes)),
        )
        .await;
        match waited {
            Ok(Ok(records)) => Ok(records.handshakes.clone()),
            Ok(Err(_)) => Err(SimError::Ended {
                waiting_for: waiting_for.to_owned(),
                reason: "the simulator stopped".to_owned(),
                arrived: Vec::new(),
            }),
            Err(_) => Err(SimError::Timeout {
                waiting_for: waiting_for.to_owned(),
                after: self.config.wait,
                arrived: self
                    .handshakes()
                    .iter()
                    .map(|handshake| format!("{handshake:?}"))
                    .collect(),
            }),
        }
    }

    pub fn transcript(&self) -> Transcript {
        self.records.borrow().transcript.clone()
    }

    pub fn write_transcript(&self, path: impl AsRef<Path>) -> Result<(), SimError> {
        self.transcript().write_to(path)
    }

    pub fn report(&self) -> Report {
        let records = self.records.borrow();
        report::build(&records.handshakes, &records.transcript)
    }
}

impl Drop for Simulator {
    fn drop(&mut self) {
        self.faults.sever();
    }
}

async fn bind(tokens: &Tokens, config: &SimConfig) -> Result<(GatewayServer, NewLinks), SimError> {
    let tokens = tokens.clone();
    let authenticate = move |token: &str| lock(&tokens).get(token).cloned();
    Ok(GatewayServer::bind("127.0.0.1:0", authenticate, config.gateway.clone()).await?)
}

struct ForwardTo {
    nodes: Arc<Mutex<Vec<SimNode>>>,
    arrivals: Arc<Inbox<SimNode>>,
    options: NodeOptions,
    wait: Duration,
    records: Arc<watch::Sender<Records>>,
}

async fn forward(mut new_links: NewLinks, to: ForwardTo) {
    let started = to.records.borrow().started();
    while let Some(new_link) = new_links.recv().await {
        let node = SimNode::start(
            new_link.link,
            new_link.events,
            to.options.clone(),
            to.wait,
            started,
        );
        let mut nodes = lock(&to.nodes);
        nodes.retain(|node| !node.link().is_closed());
        nodes.push(node.clone());
        drop(nodes);
        to.arrivals.push(node);
    }
}
