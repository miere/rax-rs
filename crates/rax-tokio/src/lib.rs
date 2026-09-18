//! A Tokio runtime for both ends of a RAX link. [`node`] dials and [`gateway`] accepts; each keeps
//! one `rax::Link` per node across sockets, so a dropped connection loses and repeats nothing.

mod calls;
mod connection;
mod error;
pub mod gateway;
pub mod node;
mod transfer;

pub use connection::Disconnect;
pub use error::{CallError, SendError};
pub use transfer::TransferError;
