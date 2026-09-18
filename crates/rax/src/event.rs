use serde::{Deserialize, Serialize};

use crate::attachment::Attachment;
use crate::content::ContentBlock;
use crate::error::Error;
use crate::interaction::{PlanRequest, QuestionRequest, SignInRequest, SignInSettled};
use crate::open::{Open, tagged};
use crate::tool::{PlanEntry, ToolCall, ToolCallUpdate};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    Message {
        content: Open<ContentBlock>,
    },
    Status {
        text: String,
    },
    Complete {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stop_reason: Option<StopReason>,
    },
    Error {
        error: Error,
    },
    ToolCall {
        tool_call: ToolCall,
    },
    ToolCallUpdate {
        tool_call_update: ToolCallUpdate,
    },
    /// The agent's progress list, replaced whole each time.
    PlanUpdate {
        entries: Vec<PlanEntry>,
    },
    Attachment {
        attachment: Attachment,
    },
    Question {
        question: QuestionRequest,
    },
    /// A plan put to a person for approval, unlike `plan_update`, which only reports progress.
    Plan {
        plan: PlanRequest,
    },
    SignIn {
        sign_in: SignInRequest,
    },
    /// Rides the turn's stream in order, so the card settles before the turn that raised it ends.
    SignInSettled {
        sign_in_settled: SignInSettled,
    },
}

tagged!(
    Event,
    "type",
    [
        "message",
        "status",
        "complete",
        "error",
        "tool_call",
        "tool_call_update",
        "plan_update",
        "attachment",
        "question",
        "plan",
        "sign_in",
        "sign_in_settled",
    ]
);

/// What a session may emit with no turn open. Questions, plans and sign-ins are absent by
/// construction: with no turn, nobody is there to answer them. Tool calls stay, because a policy
/// can still rule on them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BackgroundEvent {
    Message {
        content: Open<ContentBlock>,
    },
    Status {
        text: String,
    },
    Complete {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stop_reason: Option<StopReason>,
    },
    Error {
        error: Error,
    },
    ToolCall {
        tool_call: ToolCall,
    },
    ToolCallUpdate {
        tool_call_update: ToolCallUpdate,
    },
    PlanUpdate {
        entries: Vec<PlanEntry>,
    },
    Attachment {
        attachment: Attachment,
    },
}

tagged!(
    BackgroundEvent,
    "type",
    [
        "message",
        "status",
        "complete",
        "error",
        "tool_call",
        "tool_call_update",
        "plan_update",
        "attachment",
    ]
);

/// ACP's stop reasons.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    EndTurn,
    MaxTokens,
    MaxTurnRequests,
    Refusal,
    Cancelled,
    #[serde(other)]
    Other,
}
