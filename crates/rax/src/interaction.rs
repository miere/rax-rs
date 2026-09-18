//! Prompts a node raises for a person to answer. None carries a destination: the gateway draws
//! each one in the conversation its turn belongs to, so a node cannot aim a card anywhere.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::id::PromptId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuestionRequest {
    pub id: PromptId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub questions: Vec<Question>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Question {
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header: Option<String>,
    pub question: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<QuestionOption>,
    #[serde(default)]
    pub multi_select: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuestionOption {
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanRequest {
    pub id: PromptId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub plan: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanChoice {
    Proceed,
    Revise,
    Cancel,
}

/// The node signs in with its own environment; the gateway only draws the card.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignInRequest {
    pub id: PromptId,
    pub tool: String,
    /// Absent while `command` waits for the owner's approval: nothing has run to produce a link.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default)]
    pub needs_code: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
}

/// Expects no answer: the node's own sign-in process decides how a sign-in ends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignInSettled {
    pub id: PromptId,
    pub state: SignInState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignInState {
    Working,
    Ready,
    /// Holds a finished sign-in until the gateway confirms its owner still has access, because
    /// losing access must win over finishing.
    Confirming,
    Success,
    Failed,
    TimedOut,
    Cancelled,
}

impl SignInState {
    /// Shared by both peers so they stop listening for a sign-in at the same moment.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Success | Self::Failed | Self::TimedOut | Self::Cancelled
        )
    }
}

/// One shape answers questions, plans and sign-ins, so each hop carries a single kind back.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DisplayAnswer {
    pub id: PromptId,
    pub outcome: DisplayOutcome,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub answers: BTreeMap<String, Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub choice: Option<PlanChoice>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// What the owner typed into a sign-in card, for the node's own sign-in process only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DisplayOutcome {
    Answered,
    TimedOut,
    Dismissed,
    Denied,
    /// Lets a sign-in's command start; the sign-in stays open for its link and code.
    Approved,
    /// Not a refusal: the person wants to talk the options over, and the agent must hear it so.
    Chat,
    NoConversation,
    Unavailable,
}
