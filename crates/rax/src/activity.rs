//! What a node is doing right now, as only the node can know it: a gateway sees the turns it
//! opened, never another gateway's, nor work the agent runs outside any turn.

use serde::{Deserialize, Serialize};

/// Counts for the whole node, replacing the last report. Only counts, so no gateway learns what
/// another's conversations are.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Activity {
    /// Turns open for every gateway the node serves.
    #[serde(default)]
    pub turns: u32,
    /// Work running outside any turn that a restart would lose.
    #[serde(default)]
    pub background_tasks: u32,
}

impl Activity {
    /// Stopping the node now interrupts nothing.
    pub fn is_idle(&self) -> bool {
        self.turns == 0 && self.background_tasks == 0
    }
}
