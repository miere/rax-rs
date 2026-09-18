use std::fmt;

use serde::{Deserialize, Serialize};

macro_rules! wire_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_owned())
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }
    };
}

wire_id!(
    /// Matched only within the direction that minted it: both peers number their own calls, so
    /// the same id can be live in each direction at once.
    RequestId
);

wire_id!(
    /// Minted by the node, because only the agent backend knows whether a conversation resumes.
    SessionId
);

wire_id!(
    /// Minted by whoever raises the prompt, so an answer can never reach a prompt it was not for.
    PromptId
);

wire_id!(TransferId);

wire_id!(ToolCallId);
