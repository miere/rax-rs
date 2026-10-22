//! An opinionated RAX gateway: the nodes people run, the clients that use them, and who may do
//! what. Whatever is the embedder's own — who a person is, where the data lives, how a node's
//! owner is asked something — sits behind a trait with a default that needs nothing.

pub mod access;
pub mod embed;
pub mod fleet;
pub mod hub;
pub mod lent;
pub mod node_access;
pub mod policy;
pub mod port;
pub mod principal;
pub mod relay;
pub mod roles;
pub mod settings;
pub mod store;
pub mod token;
pub mod transfers;

pub use embed::{Embedding, FileSource, OwnerChannel, Tools};
pub use principal::Principals;
pub use settings::Settings;
