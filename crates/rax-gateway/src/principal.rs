//! Who counts as a person. The gateway takes any id with no whitespace in it; an embedder whose
//! people have a stricter shape, such as Slack's `U…` ids, says so here, and every place an id
//! comes in — a store row, a node's allow list, the CLI — holds to it.

use std::sync::Arc;

use crate::store::{UserId, UserIdError};

pub trait Principals: Send + Sync + 'static {
    /// What one id is called in messages, such as "Slack user ID".
    fn noun(&self) -> &str {
        "user ID"
    }

    fn parse(&self, raw: &str) -> Result<UserId, UserIdError> {
        UserId::parse(raw)
    }
}

/// Any id [`UserId::parse`] takes.
#[derive(Debug, Clone, Copy, Default)]
pub struct AnyId;

impl Principals for AnyId {}

pub fn any() -> Arc<dyn Principals> {
    Arc::new(AnyId)
}
