//! One listening port for everything the gateway serves over the network: RAX links, which open with
//! an upgrade request to `/rax/v1/link`, and the HTTP API beside them. Each connection is routed by
//! the path of its first request line, read without consuming it, so the side it goes to sees the
//! connection from its first byte.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use rax_tokio::accept::Acceptor;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Long enough for any client to send a request line; a connection that sends none is dropped.
const REQUEST_LINE_WAIT: Duration = Duration::from_secs(10);
/// A request line longer than this is not one either protocol sends.
const REQUEST_LINE_MAX: usize = 4096;
const UNAVAILABLE: &str =
    r#"{"error":"unavailable","message":"this gateway is not serving right now"}"#;

/// Stops listening when dropped.
pub struct Port {
    local_addr: SocketAddr,
    acceptor: Arc<Acceptor>,
    serving: Arc<AtomicBool>,
    accept: JoinHandle<()>,
}

impl Port {
    pub async fn bind(
        listen: SocketAddr,
        acceptor: Acceptor,
        http: axum::Router,
    ) -> io::Result<Self> {
        let listener = TcpListener::bind(listen).await?;
        let local_addr = listener.local_addr()?;
        let acceptor = Arc::new(acceptor);
        let serving = Arc::new(AtomicBool::new(true));
        let (handed, receiver) = mpsc::channel(64);
        tokio::spawn(async move {
            let handed = Handed {
                receiver,
                local_addr,
            };
            if let Err(err) = axum::serve(handed, http).await {
                tracing::error!(error = %err, "the HTTP API stopped");
            }
        });
        let accept = tokio::spawn(accept(listener, acceptor.clone(), serving.clone(), handed));
        Ok(Self {
            local_addr,
            acceptor,
            serving,
            accept,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// For a cluster member that stops serving: RAX dials are refused with 503 and their links
    /// closed, and HTTP requests are answered 503, so callers move to another member.
    pub fn stop_serving(&self) {
        self.serving.store(false, Ordering::SeqCst);
        self.acceptor.stop_serving();
    }

    pub fn start_serving(&self) {
        self.acceptor.start_serving();
        self.serving.store(true, Ordering::SeqCst);
    }
}

impl Drop for Port {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

async fn accept(
    listener: TcpListener,
    acceptor: Arc<Acceptor>,
    serving: Arc<AtomicBool>,
    handed: mpsc::Sender<(TcpStream, SocketAddr)>,
) {
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(err) => {
                tracing::warn!(error = %err, "accept failed");
                continue;
            }
        };
        let _ = stream.set_nodelay(true);
        let (acceptor, serving, handed) = (acceptor.clone(), serving.clone(), handed.clone());
        tokio::spawn(async move {
            let path = match tokio::time::timeout(REQUEST_LINE_WAIT, request_path(&stream)).await {
                Ok(Some(path)) => path,
                _ => return,
            };
            if path == rax::transport::PATH || path.starts_with("/rax/") {
                acceptor.serve(stream, peer).await;
            } else if serving.load(Ordering::SeqCst) {
                let _ = handed.send((stream, peer)).await;
            } else {
                let mut stream = stream;
                // Read the request first: closing over unread bytes resets the connection, and
                // the caller would see that instead of the answer.
                drain_head(&mut stream).await;
                let response = format!(
                    "HTTP/1.1 503 Service Unavailable\r\ncontent-type: application/json\r\nretry-after: 5\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{UNAVAILABLE}",
                    UNAVAILABLE.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
                let mut rest = [0u8; 1024];
                let _ = tokio::time::timeout(REQUEST_LINE_WAIT, async {
                    while matches!(stream.read(&mut rest).await, Ok(read) if read > 0) {}
                })
                .await;
            }
        });
    }
}

/// Reads up to the end of the request's headers, so a body-less request is fully consumed.
async fn drain_head(stream: &mut TcpStream) {
    let mut head = Vec::new();
    let mut chunk = [0u8; 1024];
    let _ = tokio::time::timeout(REQUEST_LINE_WAIT, async {
        while !head.windows(4).any(|window| window == b"\r\n\r\n") && head.len() < 64 * 1024 {
            match stream.read(&mut chunk).await {
                Ok(read) if read > 0 => head.extend_from_slice(&chunk[..read]),
                _ => return,
            }
        }
    })
    .await;
}

/// The path of the connection's first request line, read without consuming anything.
async fn request_path(stream: &TcpStream) -> Option<String> {
    let mut buffer = vec![0u8; REQUEST_LINE_MAX];
    loop {
        let read = stream.peek(&mut buffer).await.ok()?;
        if read == 0 {
            return None;
        }
        if let Some(end) = buffer[..read].windows(2).position(|pair| pair == b"\r\n") {
            let line = std::str::from_utf8(&buffer[..end]).ok()?;
            let target = line.split(' ').nth(1)?;
            let path = target.split('?').next().unwrap_or(target);
            return Some(path.to_owned());
        }
        if read == buffer.len() {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// The HTTP side's listener: connections the port routed to it.
struct Handed {
    receiver: mpsc::Receiver<(TcpStream, SocketAddr)>,
    local_addr: SocketAddr,
}

impl axum::serve::Listener for Handed {
    type Io = TcpStream;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (TcpStream, SocketAddr) {
        match self.receiver.recv().await {
            Some(accepted) => accepted,
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local_addr)
    }
}
