//! An opinionated RAX gateway: the nodes people run, the clients that use them, and who may do
//! what. Whatever is the embedder's own — who a person is, where the data lives, how a node's
//! owner is asked something — sits behind a trait with a default that needs nothing.

pub mod principal;
pub mod store;

pub use principal::Principals;
