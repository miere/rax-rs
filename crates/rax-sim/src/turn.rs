use std::fmt;
use std::mem::discriminant;
use std::sync::Arc;

use rax::event::StopReason;
use rax::id::{RequestId, ToolCallId};
use rax::interaction::{DisplayAnswer, SignInState};
use rax::session::PromptAccepted;
use rax::tool::ToolCallStatus;
use rax::{ContentBlock, Decision, ErrorKind, Event, GatewayReply, Open};
use rax_tokio::gateway::{PendingCall, PendingReply, StreamEvents};
use tokio::time::Instant;

use crate::error::SimError;
use crate::inbox::Inbox;
use crate::node::{NodeShared, settle, unexpected};

const CLOSED_BY_END: &str = "the node sent `end`";
const CLOSED_BY_LINK: &str = "the link ended before `end`";

/// A test-side condition on an event, named so a failed wait says what it was waiting for.
#[derive(Clone)]
pub struct Match {
    description: String,
    predicate: Arc<dyn Fn(&Event) -> bool + Send + Sync>,
}

impl fmt::Debug for Match {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.description)
    }
}

impl Match {
    pub fn when(
        description: impl Into<String>,
        predicate: impl Fn(&Event) -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            description: description.into(),
            predicate: Arc::new(predicate),
        }
    }

    pub fn message_contains(text: impl Into<String>) -> Self {
        let text = text.into();
        Self::when(format!("a message containing {text:?}"), move |event| {
            matches!(
                event,
                Event::Message { content: Open::Known(ContentBlock::Text { text: body }) }
                    if body.contains(&text)
            )
        })
    }

    pub fn tool_call(name: impl Into<String>) -> Self {
        let name = name.into();
        Self::when(
            format!("a tool_call named {name:?}"),
            move |event| matches!(event, Event::ToolCall { tool_call } if tool_call.name == name),
        )
    }

    pub fn tool_call_update(status: ToolCallStatus) -> Self {
        Self::when(format!("a tool_call_update {status:?}"), move |event| {
            matches!(
                event,
                Event::ToolCallUpdate { tool_call_update } if tool_call_update.status == status
            )
        })
    }

    pub fn question() -> Self {
        Self::when("a question", |event| {
            matches!(event, Event::Question { .. })
        })
    }

    pub fn plan() -> Self {
        Self::when("a plan", |event| matches!(event, Event::Plan { .. }))
    }

    pub fn sign_in() -> Self {
        Self::when("a sign_in", |event| matches!(event, Event::SignIn { .. }))
    }

    pub fn sign_in_settled(state: SignInState) -> Self {
        Self::when(format!("a sign_in_settled {state:?}"), move |event| {
            matches!(
                event,
                Event::SignInSettled { sign_in_settled } if sign_in_settled.state == state
            )
        })
    }

    pub fn attachment() -> Self {
        Self::when("an attachment", |event| {
            matches!(event, Event::Attachment { .. })
        })
    }

    pub fn complete(stop_reason: StopReason) -> Self {
        Self::when(
            format!("complete {stop_reason:?}"),
            move |event| matches!(event, Event::Complete { stop_reason: Some(reason) } if *reason == stop_reason),
        )
    }

    /// Compares the kind only, so `ErrorKind::Provider` matches whatever provider failed.
    pub fn error(kind: ErrorKind) -> Self {
        Self::when(
            format!("an error of kind {kind:?}"),
            move |event| matches!(event, Event::Error { error } if discriminant(&error.kind) == discriminant(&kind)),
        )
    }

    pub fn matches(&self, event: &Event) -> bool {
        (self.predicate)(event)
    }
}

/// A prompt the node has not answered yet. Its events are already being collected and ruled on.
pub struct PendingTurn {
    id: RequestId,
    reply: PendingReply,
    events: Arc<Inbox<Open<Event>>>,
    node: Arc<NodeShared>,
}

impl PendingTurn {
    pub(crate) fn start(pending: PendingCall, node: Arc<NodeShared>) -> Self {
        let PendingCall { id, reply, events } = pending;
        let inbox = Arc::new(Inbox::new());
        tokio::spawn(pump(events, inbox.clone(), node.clone()));
        Self {
            id,
            reply,
            events: inbox,
            node,
        }
    }

    pub fn id(&self) -> &RequestId {
        &self.id
    }

    pub async fn accepted(self) -> Result<SimTurn, SimError> {
        match settle("prompt", self.reply, self.node.wait).await? {
            GatewayReply::Prompt(accepted) => Ok(SimTurn {
                id: self.id,
                accepted,
                events: self.events,
                seen: Vec::new(),
                node: self.node,
            }),
            other => Err(unexpected("prompt", &other)),
        }
    }
}

pub struct SimTurn {
    id: RequestId,
    accepted: PromptAccepted,
    events: Arc<Inbox<Open<Event>>>,
    seen: Vec<Open<Event>>,
    node: Arc<NodeShared>,
}

impl SimTurn {
    pub fn id(&self) -> &RequestId {
        &self.id
    }

    pub fn accepted(&self) -> &PromptAccepted {
        &self.accepted
    }

    /// Every event read so far, including those `expect` skipped.
    pub fn seen(&self) -> &[Open<Event>] {
        &self.seen
    }

    pub async fn next_event(&mut self) -> Result<Open<Event>, SimError> {
        let event = self.events.next(self.node.wait, "the next event").await?;
        self.seen.push(event.clone());
        Ok(event)
    }

    /// Skips events until one matches, all within one wait.
    pub async fn expect(&mut self, wanted: Match) -> Result<Event, SimError> {
        let deadline = Instant::now() + self.node.wait;
        let waiting_for = format!("{wanted:?}");
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let event = self.events.next(left, &waiting_for).await?;
            self.seen.push(event.clone());
            if let Open::Known(event) = event
                && wanted.matches(&event)
            {
                return Ok(event);
            }
        }
    }

    /// Fails if the link ended first: a turn cut off by a reset never sent its `end`.
    pub async fn until_end(&mut self) -> Result<Vec<Open<Event>>, SimError> {
        let mut rest = Vec::new();
        loop {
            match self.events.next(self.node.wait, "`end`").await {
                Ok(event) => {
                    self.seen.push(event.clone());
                    rest.push(event);
                }
                Err(SimError::Ended { .. }) if self.events.ended_cleanly() => return Ok(rest),
                Err(err) => return Err(err),
            }
        }
    }

    pub async fn verdict(
        &self,
        tool_call_id: impl Into<ToolCallId>,
        decision: Decision,
    ) -> Result<(), SimError> {
        let verdict = rax::ToolVerdict {
            id: tool_call_id.into(),
            decision,
        };
        self.node
            .link
            .verdict(verdict)
            .await
            .map_err(SimError::send("a verdict"))
    }

    pub async fn answer(&self, answer: DisplayAnswer) -> Result<(), SimError> {
        self.node
            .link
            .answer(answer)
            .await
            .map_err(SimError::send("an answer"))
    }
}

async fn pump(mut events: StreamEvents, inbox: Arc<Inbox<Open<Event>>>, node: Arc<NodeShared>) {
    while let Some(event) = events.recv().await {
        let known = event.known().cloned();
        inbox.push(event);
        if let Some(known) = known {
            node.apply_policies(&known).await;
        }
    }
    if node.link.is_closed() {
        inbox.end(CLOSED_BY_LINK, false);
    } else {
        inbox.end(CLOSED_BY_END, true);
    }
}
