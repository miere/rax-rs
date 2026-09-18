use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use rax::interaction::{DisplayAnswer, DisplayOutcome};
use rax::tool::DeniedBy;
use rax::{Decision, Event, ToolCall};

use crate::node::NodeMethod;

type Decide = dyn Fn(&ToolCall) -> Option<Decision> + Send + Sync;
type Answer = dyn Fn(&Event) -> Option<DisplayAnswer> + Send + Sync;

/// Applies to turn and background tool calls alike, as a gateway's policy would.
#[derive(Clone, Default)]
pub enum VerdictPolicy {
    #[default]
    Manual,
    AllowAll,
    DenyAll {
        by: DeniedBy,
        reason: Option<String>,
    },
    /// `None` never answers, so a test can drive the node into its own verdict timeout.
    Script(Arc<Decide>),
}

impl VerdictPolicy {
    pub fn script(decide: impl Fn(&ToolCall) -> Option<Decision> + Send + Sync + 'static) -> Self {
        Self::Script(Arc::new(decide))
    }

    pub(crate) fn decide(&self, call: &ToolCall) -> Option<Decision> {
        match self {
            Self::Manual => None,
            Self::AllowAll => Some(Decision::Allow),
            Self::DenyAll { by, reason } => Some(Decision::Deny {
                by: *by,
                reason: reason.clone(),
            }),
            Self::Script(decide) => decide(call),
        }
    }
}

impl fmt::Debug for VerdictPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Manual => f.write_str("Manual"),
            Self::AllowAll => f.write_str("AllowAll"),
            Self::DenyAll { by, reason } => f
                .debug_struct("DenyAll")
                .field("by", by)
                .field("reason", reason)
                .finish(),
            Self::Script(_) => f.write_str("Script"),
        }
    }
}

#[derive(Clone, Default)]
pub enum AnswerPolicy {
    #[default]
    Manual,
    /// Settles every question, plan and sign-in with this outcome and nothing else.
    Outcome(DisplayOutcome),
    /// Sees every turn event, so a sign-in can be approved and then answered with its code.
    Script(Arc<Answer>),
}

impl AnswerPolicy {
    pub fn script(
        answer: impl Fn(&Event) -> Option<DisplayAnswer> + Send + Sync + 'static,
    ) -> Self {
        Self::Script(Arc::new(answer))
    }

    pub(crate) fn answer(&self, event: &Event) -> Option<DisplayAnswer> {
        match self {
            Self::Manual => None,
            Self::Outcome(outcome) => {
                let id = match event {
                    Event::Question { question } => question.id.clone(),
                    Event::Plan { plan } => plan.id.clone(),
                    Event::SignIn { sign_in } => sign_in.id.clone(),
                    _ => return None,
                };
                Some(DisplayAnswer {
                    id,
                    outcome: *outcome,
                    answers: BTreeMap::new(),
                    choice: None,
                    user_id: None,
                    note: None,
                    code: None,
                })
            }
            Self::Script(answer) => answer(event),
        }
    }
}

impl fmt::Debug for AnswerPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Manual => f.write_str("Manual"),
            Self::Outcome(outcome) => f.debug_tuple("Outcome").field(outcome).finish(),
            Self::Script(_) => f.write_str("Script"),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct NodeOptions {
    pub verdicts: VerdictPolicy,
    pub answers: AnswerPolicy,
    /// Calls answered as they arrive; they still enter the call log but never the call queue.
    pub auto_reply: BTreeSet<NodeMethod>,
}
