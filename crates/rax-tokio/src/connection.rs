use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use rax::frame::MAX_FRAME_BYTES;
use rax::link::Delivery;
use rax::message::to_payload;
use rax::{Frame, Link, LinkError, Received};
use serde::Serialize;
use serde_json::value::RawValue;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Notify, OwnedMutexGuard, mpsc};
use tokio::time::{Instant, MissedTickBehavior, interval_at, timeout};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::protocol::frame::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::{Bytes, Message};

use crate::error::{CallError, SendError};

const FRAME_ENVELOPE_BYTES: usize = 128;

/// Why a socket went away. The link itself survives all of these until the peer refuses a resume.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum Disconnect {
    #[error("the peer closed the socket")]
    PeerClosed,
    #[error("socket error: {0}")]
    Io(String),
    #[error("a write stalled past the write timeout")]
    WriteTimeout,
    #[error("nothing arrived for three keepalive intervals")]
    Idle,
    #[error("undecodable frame: {0}")]
    Frame(String),
    #[error("protocol violation: {0}")]
    Protocol(String),
    #[error("delivery failed: {0}")]
    Link(LinkError),
    #[error("a newer connection took over the link")]
    Superseded,
    #[error("the link was closed")]
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Ended {
    Closed,
    Reset,
}

impl Ended {
    pub(crate) fn send_error(self) -> SendError {
        match self {
            Self::Closed => SendError::Closed,
            Self::Reset => SendError::LinkReset,
        }
    }

    pub(crate) fn call_error(self) -> CallError {
        match self {
            Self::Closed => CallError::Closed,
            Self::Reset => CallError::LinkReset,
        }
    }
}

pub(crate) enum Outbound {
    Frame(Frame),
    Close,
}

pub(crate) struct Queue {
    id: u64,
    frames: mpsc::UnboundedReceiver<Outbound>,
}

pub(crate) enum AttachError<R> {
    Ended,
    Superseded,
    Refused(R),
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Timing {
    pub(crate) keepalive: Duration,
    pub(crate) write_timeout: Duration,
}

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

pub(crate) fn socket_config() -> WebSocketConfig {
    WebSocketConfig::default()
        .max_message_size(Some(MAX_FRAME_BYTES))
        .max_frame_size(Some(MAX_FRAME_BYTES))
}

struct State {
    link: Link,
    generation: u64,
    writer: Option<(u64, mpsc::UnboundedSender<Outbound>)>,
    next_writer: u64,
    claim: u64,
    ended: Option<Ended>,
}

impl State {
    fn write(&mut self, frame: Frame) {
        if let Some((_, writer)) = &self.writer
            && writer.send(Outbound::Frame(frame)).is_err()
        {
            self.writer = None;
        }
    }

    fn install(&mut self, frames: Vec<Frame>) -> Queue {
        let (writer, queue) = mpsc::unbounded_channel();
        for frame in frames {
            let _ = writer.send(Outbound::Frame(frame));
        }
        self.next_writer += 1;
        self.writer = Some((self.next_writer, writer));
        Queue {
            id: self.next_writer,
            frames: queue,
        }
    }
}

pub(crate) struct Core {
    state: Mutex<State>,
    room: Notify,
    slot: Arc<tokio::sync::Mutex<()>>,
}

impl Core {
    pub(crate) fn new(link: Link) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                link,
                generation: 0,
                writer: None,
                next_writer: 0,
                claim: 0,
                ended: None,
            }),
            room: Notify::new(),
            slot: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    pub(crate) fn generation(&self) -> u64 {
        lock(&self.state).generation
    }

    pub(crate) fn epoch(&self) -> u64 {
        lock(&self.state).link.epoch()
    }

    pub(crate) fn resume_request(&self) -> Frame {
        lock(&self.state).link.resume_request()
    }

    pub(crate) fn is_attached(&self) -> bool {
        lock(&self.state).writer.is_some()
    }

    pub(crate) fn current_claim(&self) -> u64 {
        lock(&self.state).claim
    }

    pub(crate) async fn send<T: Serialize>(&self, message: &T) -> Result<(), SendError> {
        let generation = self.generation();
        self.send_in(generation, message).await
    }

    pub(crate) async fn send_in<T: Serialize>(
        &self,
        generation: u64,
        message: &T,
    ) -> Result<(), SendError> {
        let payload = to_payload(message)?;
        self.send_payload(generation, payload).await
    }

    async fn send_payload(&self, generation: u64, payload: Box<RawValue>) -> Result<(), SendError> {
        let size = payload.get().len();
        if size + FRAME_ENVELOPE_BYTES > MAX_FRAME_BYTES {
            return Err(SendError::TooLarge { size });
        }
        let mut payload = Some(payload);
        loop {
            let room = self.room.notified();
            tokio::pin!(room);
            room.as_mut().enable();
            {
                let mut state = lock(&self.state);
                if let Some(ended) = state.ended {
                    return Err(ended.send_error());
                }
                if state.generation != generation {
                    return Err(SendError::LinkReset);
                }
                if state.link.has_room(size)
                    && let Some(payload) = payload.take()
                {
                    let frame = state.link.send(payload)?;
                    state.write(frame);
                    return Ok(());
                }
            }
            room.await;
        }
    }

    pub(crate) async fn claim(&self) -> (u64, OwnedMutexGuard<()>) {
        let claim = {
            let mut state = lock(&self.state);
            state.claim += 1;
            state.writer = None;
            state.claim
        };
        (claim, self.slot.clone().lock_owned().await)
    }

    pub(crate) fn attach<R>(
        &self,
        claim: u64,
        prepare: impl FnOnce(&mut Link) -> Result<Vec<Frame>, R>,
    ) -> Result<Queue, AttachError<R>> {
        let mut state = lock(&self.state);
        if state.ended.is_some() {
            return Err(AttachError::Ended);
        }
        if state.claim != claim {
            return Err(AttachError::Superseded);
        }
        let frames = prepare(&mut state.link).map_err(AttachError::Refused)?;
        let queue = state.install(frames);
        drop(state);
        self.room.notify_waiters();
        Ok(queue)
    }

    pub(crate) fn refresh(&self, claim: u64, link: Link) -> Result<Queue, AttachError<()>> {
        let mut state = lock(&self.state);
        if state.ended.is_some() {
            return Err(AttachError::Ended);
        }
        if state.claim != claim {
            return Err(AttachError::Superseded);
        }
        state.link = link;
        state.generation += 1;
        let queue = state.install(Vec::new());
        drop(state);
        self.room.notify_waiters();
        Ok(queue)
    }

    pub(crate) fn reset(&self, link: Link) {
        let mut state = lock(&self.state);
        state.link = link;
        state.generation += 1;
        drop(state);
        self.room.notify_waiters();
    }

    pub(crate) fn detach(&self, queue_id: u64) {
        let mut state = lock(&self.state);
        if state.writer.as_ref().is_some_and(|(id, _)| *id == queue_id) {
            state.writer = None;
        }
    }

    pub(crate) fn end(&self, ended: Ended) -> bool {
        let mut state = lock(&self.state);
        if state.ended.is_some() {
            return false;
        }
        state.ended = Some(ended);
        if let Some((_, writer)) = state.writer.take() {
            let _ = writer.send(Outbound::Close);
        }
        drop(state);
        self.room.notify_waiters();
        true
    }

    pub(crate) fn is_ended(&self) -> bool {
        lock(&self.state).ended.is_some()
    }

    fn receive(&self, frame: Frame) -> Result<Received, LinkError> {
        let received = lock(&self.state).link.receive(frame);
        self.room.notify_waiters();
        received
    }

    fn consumed(&self, delivery: Delivery) {
        let mut state = lock(&self.state);
        if let Some(ack) = state.link.consumed(delivery) {
            state.write(ack);
        }
    }

    fn keepalive(&self) -> Option<Frame> {
        lock(&self.state).link.ack()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Flow {
    Continue,
    Stop,
}

pub(crate) trait Route {
    async fn route(&mut self, payload: Box<RawValue>) -> Flow;
}

pub(crate) async fn drive<S, R>(
    core: Arc<Core>,
    socket: WebSocketStream<S>,
    queue: Queue,
    timing: Timing,
    router: &mut R,
) -> Disconnect
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    R: Route,
{
    let queue_id = queue.id;
    let (sink, mut stream) = socket.split();
    let mut writer = tokio::spawn(write_loop(sink, queue, core.clone(), timing));
    let idle = timing.keepalive.saturating_mul(3);
    let reason = loop {
        let message = tokio::select! {
            biased;
            ended = &mut writer => {
                break ended.unwrap_or_else(|err| Disconnect::Io(err.to_string()));
            }
            message = timeout(idle, stream.next()) => message,
        };
        let bytes = match message {
            Err(_) => break Disconnect::Idle,
            Ok(None) | Ok(Some(Ok(Message::Close(_)))) => break Disconnect::PeerClosed,
            Ok(Some(Err(err))) => break Disconnect::Io(err.to_string()),
            Ok(Some(Ok(Message::Binary(bytes)))) => bytes,
            Ok(Some(Ok(Message::Text(_)))) => {
                break Disconnect::Protocol("text messages carry no frames".to_owned());
            }
            Ok(Some(Ok(_))) => continue,
        };
        let frame = match Frame::decode(&bytes) {
            Ok(frame) => frame,
            Err(err) => break Disconnect::Frame(err.to_string()),
        };
        match core.receive(frame) {
            Err(err) => break Disconnect::Link(err),
            Ok(Received::Deliver { delivery, payload }) => {
                if router.route(payload).await == Flow::Stop {
                    drop(delivery);
                    break Disconnect::Closed;
                }
                core.consumed(delivery);
            }
            Ok(Received::Duplicate | Received::Acknowledged) => {}
            Ok(Received::ResumeRequested { .. } | Received::Resumed { .. }) => {
                break Disconnect::Protocol("resume after the handshake".to_owned());
            }
        }
    };
    core.detach(queue_id);
    writer.abort();
    reason
}

async fn write_loop<S>(
    mut sink: SplitSink<WebSocketStream<S>, Message>,
    mut queue: Queue,
    core: Arc<Core>,
    timing: Timing,
) -> Disconnect
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut tick = interval_at(Instant::now() + timing.keepalive, timing.keepalive);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        let result = tokio::select! {
            outbound = queue.frames.recv() => match outbound {
                None => return Disconnect::Superseded,
                Some(Outbound::Close) => {
                    let normal = CloseFrame {
                        code: CloseCode::Normal,
                        reason: "".into(),
                    };
                    let _ = write(&mut sink, Message::Close(Some(normal)), timing).await;
                    return Disconnect::Closed;
                }
                Some(Outbound::Frame(frame)) => match frame.encode() {
                    Ok(raw) => write(&mut sink, Message::Binary(raw.into()), timing).await,
                    Err(err) => return Disconnect::Frame(err.to_string()),
                },
            },
            _ = tick.tick() => keepalive(&mut sink, &core, timing).await,
        };
        if let Err(reason) = result {
            return reason;
        }
    }
}

async fn keepalive<S>(
    sink: &mut SplitSink<WebSocketStream<S>, Message>,
    core: &Core,
    timing: Timing,
) -> Result<(), Disconnect>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if let Some(ack) = core.keepalive() {
        let raw = ack
            .encode()
            .map_err(|err| Disconnect::Frame(err.to_string()))?;
        write(sink, Message::Binary(raw.into()), timing).await?;
    }
    write(sink, Message::Ping(Bytes::new()), timing).await
}

async fn write<S>(
    sink: &mut SplitSink<WebSocketStream<S>, Message>,
    message: Message,
    timing: Timing,
) -> Result<(), Disconnect>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match timeout(timing.write_timeout, sink.send(message)).await {
        Err(_) => Err(Disconnect::WriteTimeout),
        Ok(Err(err)) => Err(Disconnect::Io(err.to_string())),
        Ok(Ok(())) => Ok(()),
    }
}
