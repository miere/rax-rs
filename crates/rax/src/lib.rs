//! RAX (Remote Agentic eXperience) is the protocol a node running an agent speaks to the chat
//! gateway that fronts it. It has three layers, each unable to read the one above it:
//!
//! - [`transport`]: one authenticated WebSocket per node, always dialled by the node.
//! - [`frame`] and [`link`]: sequenced, acknowledged, resumable delivery of opaque payloads.
//! - [`message`]: the calls, replies and turn events those payloads carry.

pub mod attachment;
pub mod content;
pub mod credential;
pub mod error;
pub mod event;
pub mod frame;
pub mod id;
pub mod interaction;
pub mod link;
pub mod message;
pub mod open;
pub mod session;
pub mod tool;
pub mod transport;

pub use content::ContentBlock;
pub use error::{Error, ErrorKind};
pub use event::{BackgroundEvent, Event};
pub use frame::{Frame, FrameError, VERSION};
pub use link::{Delivery, Link, LinkConfig, LinkError, Received};
pub use message::{GatewayCall, GatewayMessage, GatewayReply, NodeCall, NodeMessage, NodeReply};
pub use open::{Open, Unhandled};
pub use session::PROTOCOL_VERSION;
pub use tool::{Decision, ToolCall, ToolVerdict};
