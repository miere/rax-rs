#![allow(dead_code, clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::future::Future;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rax_tokio::gateway::{
    GatewayConfig, GatewayLink, GatewayServer, LinkEvent, LinkEvents, NewLink, NewLinks,
    NodeIdentity,
};
use rax_tokio::node::{NodeConfig, NodeEvent, NodeEvents, NodeHandle, NodeLink};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub const TOKEN: &str = "node-token";
pub const IDENTITY: &str = "node-1";

pub async fn within<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(30), future)
        .await
        .expect("test guard timed out")
}

pub fn gateway_config() -> GatewayConfig {
    GatewayConfig {
        keepalive: Duration::from_secs(1),
        handshake_timeout: Duration::from_secs(5),
        ..Default::default()
    }
}

pub fn gateway_config_with_window(window_bytes: usize) -> GatewayConfig {
    GatewayConfig {
        window_bytes,
        ack_threshold: 4,
        ..gateway_config()
    }
}

pub fn node_config(addr: SocketAddr) -> NodeConfig {
    NodeConfig {
        endpoint: format!("ws://{addr}"),
        token: TOKEN.to_owned(),
        keepalive: Duration::from_secs(1),
        backoff_min: Duration::from_millis(5),
        backoff_max: Duration::from_millis(50),
        handshake_timeout: Duration::from_secs(5),
        ..Default::default()
    }
}

pub async fn gateway(config: GatewayConfig) -> (GatewayServer, NewLinks) {
    let authenticate = |token: &str| (token == TOKEN).then(|| NodeIdentity(IDENTITY.to_owned()));
    GatewayServer::bind("127.0.0.1:0", authenticate, config)
        .await
        .unwrap()
}

pub struct Pair {
    pub node: NodeHandle,
    pub node_events: NodeEvents,
    pub link: GatewayLink,
    pub link_events: LinkEvents,
}

pub async fn connect(new_links: &mut NewLinks, config: NodeConfig) -> Pair {
    let (node, mut node_events) = NodeLink::start(config).unwrap();
    assert!(matches!(next(&mut node_events).await, NodeEvent::Fresh));
    let NewLink { link, events } = within(new_links.recv()).await.unwrap();
    Pair {
        node,
        node_events,
        link,
        link_events: events,
    }
}

pub async fn next(events: &mut NodeEvents) -> NodeEvent {
    within(events.recv()).await.expect("node events ended")
}

pub async fn next_link(events: &mut LinkEvents) -> LinkEvent {
    within(events.recv()).await.expect("link events ended")
}

pub struct Proxy {
    addr: SocketAddr,
    upstream: Arc<Mutex<SocketAddr>>,
    connections: Arc<Mutex<Vec<JoinHandle<()>>>>,
    accepts: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl Proxy {
    pub async fn start(upstream: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let upstream = Arc::new(Mutex::new(upstream));
        let connections = Arc::new(Mutex::new(Vec::new()));
        let accepts = Arc::new(AtomicUsize::new(0));
        let task = tokio::spawn({
            let (upstream, connections, accepts) =
                (upstream.clone(), connections.clone(), accepts.clone());
            async move {
                while let Ok((mut down, _)) = listener.accept().await {
                    accepts.fetch_add(1, Ordering::SeqCst);
                    let target = *upstream.lock().unwrap();
                    let (go, started) = oneshot::channel::<()>();
                    let relay = tokio::spawn(async move {
                        if started.await.is_err() {
                            return;
                        }
                        let Ok(mut up) = TcpStream::connect(target).await else {
                            return;
                        };
                        let _ = tokio::io::copy_bidirectional(&mut down, &mut up).await;
                    });
                    connections.lock().unwrap().push(relay);
                    let _ = go.send(());
                }
            }
        });
        Self {
            addr,
            upstream,
            connections,
            accepts,
            task,
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn accepts(&self) -> usize {
        self.accepts.load(Ordering::SeqCst)
    }

    pub fn retarget(&self, upstream: SocketAddr) {
        *self.upstream.lock().unwrap() = upstream;
    }

    pub fn sever(&self) {
        for relay in self.connections.lock().unwrap().drain(..) {
            relay.abort();
        }
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.sever();
        self.task.abort();
    }
}

pub async fn closed_port() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}
