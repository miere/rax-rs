//! A Tokio runtime for both ends of a RAX link. The transport ends follow the connection —
//! [`dial`] connects and resumes, [`accept`] authenticates and keeps links — and the role ends
//! follow the messages: [`node`] and [`gateway`] each ride on either. Each keeps one `rax::Link`
//! across sockets, so a dropped connection loses and repeats nothing.

pub mod accept;
mod calls;
mod connection;
pub mod dial;
mod error;
pub mod gateway;
pub mod node;
mod transfer;

pub use connection::Disconnect;
pub use error::{CallError, SendError};
pub use rax::transport::Role;
pub use transfer::TransferError;
