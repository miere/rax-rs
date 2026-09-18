use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::inbox::lock;
use crate::record::{ConnectionId, Direction, Records};
use crate::ws::WsReader;

const MAX_HEAD_BYTES: usize = 64 << 10;
const READ_BUFFER_BYTES: usize = 64 << 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Upgrades {
    Forward,
    Refuse(u16),
    HangUp,
}

pub(crate) struct Faults {
    upstream: Mutex<SocketAddr>,
    upgrades: Mutex<Upgrades>,
    sever: Mutex<CancellationToken>,
    reading: watch::Sender<bool>,
}

impl Faults {
    pub(crate) fn new(upstream: SocketAddr) -> Self {
        Self {
            upstream: Mutex::new(upstream),
            upgrades: Mutex::new(Upgrades::Forward),
            sever: Mutex::new(CancellationToken::new()),
            reading: watch::Sender::new(true),
        }
    }

    pub(crate) fn retarget(&self, upstream: SocketAddr) {
        *lock(&self.upstream) = upstream;
    }

    pub(crate) fn set_upgrades(&self, upgrades: Upgrades) {
        *lock(&self.upgrades) = upgrades;
    }

    pub(crate) fn sever(&self) {
        let severed = std::mem::take(&mut *lock(&self.sever));
        severed.cancel();
    }

    pub(crate) fn set_reading(&self, reading: bool) {
        self.reading.send_replace(reading);
    }
}

pub(crate) struct Proxy {
    addr: SocketAddr,
    accept: JoinHandle<()>,
}

impl Proxy {
    pub(crate) async fn start(
        faults: Arc<Faults>,
        records: Arc<watch::Sender<Records>>,
    ) -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let accept = tokio::spawn(accept(listener, Arc::new(Context { faults, records })));
        Ok(Self { addr, accept })
    }

    pub(crate) fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

struct Context {
    faults: Arc<Faults>,
    records: Arc<watch::Sender<Records>>,
}

impl Context {
    fn record(&self, change: impl FnOnce(&mut Records)) {
        self.records.send_modify(change);
    }
}

async fn accept(listener: TcpListener, context: Arc<Context>) {
    loop {
        match listener.accept().await {
            Ok((node, _)) => {
                let _ = node.set_nodelay(true);
                let mut connection = ConnectionId(0);
                context.record(|records| connection = records.connect());
                tokio::spawn(serve(context.clone(), node, connection));
            }
            Err(err) => tracing::warn!(error = %err, "rax-sim proxy accept failed"),
        }
    }
}

async fn serve(context: Arc<Context>, node: TcpStream, connection: ConnectionId) {
    let severed = lock(&context.faults.sever).clone();
    tokio::select! {
        _ = severed.cancelled() => {}
        relayed = relay(&context, node, connection) => {
            if let Err(err) = relayed {
                tracing::debug!(error = %err, "rax-sim proxy connection ended");
            }
        }
    }
    context.record(|records| records.ended(connection));
}

async fn relay(context: &Context, mut node: TcpStream, connection: ConnectionId) -> io::Result<()> {
    let Some((request, early)) = read_head(&mut node).await? else {
        return Ok(());
    };
    let (target, authorization) = parse_request(&request);
    context.record(|records| records.handshake(connection, target, authorization));
    let upgrades = *lock(&context.faults.upgrades);
    match upgrades {
        Upgrades::Forward => {}
        Upgrades::HangUp => return Ok(()),
        Upgrades::Refuse(status) => {
            let response = format!(
                "HTTP/1.1 {status} Refused by the simulator\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
            );
            node.write_all(response.as_bytes()).await?;
            context.record(|records| records.answered(connection, status));
            return Ok(());
        }
    }
    let upstream = *lock(&context.faults.upstream);
    let mut gateway = TcpStream::connect(upstream).await?;
    gateway.set_nodelay(true)?;
    gateway.write_all(&request).await?;
    let Some((response, late)) = read_head(&mut gateway).await? else {
        return Ok(());
    };
    let status = parse_status(&response);
    if let Some(status) = status {
        context.record(|records| records.answered(connection, status));
    }
    node.write_all(&response).await?;
    if status != Some(101) {
        node.write_all(&late).await?;
        gateway.write_all(&early).await?;
        tokio::io::copy_bidirectional(&mut node, &mut gateway).await?;
        return Ok(());
    }
    let (node_read, node_write) = node.into_split();
    let (gateway_read, gateway_write) = gateway.into_split();
    let reading = context.faults.reading.subscribe();
    let to_gateway = pump(
        context,
        connection,
        Direction::NodeToGateway,
        early,
        node_read,
        gateway_write,
        Some(reading),
    );
    let to_node = pump(
        context,
        connection,
        Direction::GatewayToNode,
        late,
        gateway_read,
        node_write,
        None,
    );
    tokio::select! {
        ended = to_gateway => ended,
        ended = to_node => ended,
    }
}

async fn pump(
    context: &Context,
    connection: ConnectionId,
    direction: Direction,
    mut chunk: Vec<u8>,
    mut from: impl AsyncRead + Unpin,
    mut to: impl AsyncWrite + Unpin,
    mut reading: Option<watch::Receiver<bool>>,
) -> io::Result<()> {
    let mut frames = WsReader::default();
    let mut buffer = vec![0u8; READ_BUFFER_BYTES];
    loop {
        if !chunk.is_empty() {
            let messages = frames.feed(&chunk);
            if !messages.is_empty() {
                context.record(|records| {
                    for message in messages {
                        records.observe(connection, direction, message);
                    }
                });
            }
            to.write_all(&chunk).await?;
            chunk.clear();
        }
        let read = match reading.as_mut() {
            None => from.read(&mut buffer).await?,
            Some(reading) => {
                if reading.wait_for(|on| *on).await.is_err() {
                    return Ok(());
                }
                tokio::select! {
                    read = from.read(&mut buffer) => read?,
                    _ = reading.wait_for(|on| !*on) => continue,
                }
            }
        };
        if read == 0 {
            return Ok(());
        }
        chunk.extend_from_slice(&buffer[..read]);
    }
}

async fn read_head(stream: &mut TcpStream) -> io::Result<Option<(Vec<u8>, Vec<u8>)>> {
    let mut head = Vec::new();
    let mut buffer = [0u8; 4096];
    loop {
        if let Some(end) = head.windows(4).position(|window| window == b"\r\n\r\n") {
            let rest = head.split_off(end + 4);
            return Ok(Some((head, rest)));
        }
        if head.len() > MAX_HEAD_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "an HTTP head larger than 64 KiB",
            ));
        }
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            return Ok(None);
        }
        head.extend_from_slice(&buffer[..read]);
    }
}

fn parse_request(head: &[u8]) -> (String, Option<String>) {
    let text = String::from_utf8_lossy(head);
    let mut lines = text.split("\r\n");
    let target = lines
        .next()
        .and_then(|line| line.split(' ').nth(1))
        .unwrap_or_default()
        .to_owned();
    let authorization = lines
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.trim().eq_ignore_ascii_case("authorization"))
        .map(|(_, value)| value.trim().to_owned());
    (target, authorization)
}

fn parse_status(head: &[u8]) -> Option<u16> {
    String::from_utf8_lossy(head)
        .split("\r\n")
        .next()?
        .split(' ')
        .nth(1)?
        .parse()
        .ok()
}
