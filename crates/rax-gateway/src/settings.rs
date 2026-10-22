//! What an embedder calls things. Every default names nothing but the gateway, so a standalone one
//! needs no settings at all.

use std::sync::Arc;

use crate::principal::{self, Principals};

#[derive(Clone)]
pub struct Settings {
    /// The prefix of the metadata keys this gateway owns, so a node sets its allow list under
    /// `<name>_access`.
    pub name: String,
    /// What a node's credential starts with, such as `rax_node_`.
    pub node_prefix: String,
    /// What a client's credential starts with, such as `rax_user_`.
    pub user_prefix: String,
    pub principals: Arc<dyn Principals>,
    /// Tools the embedder lends that only reach the people in the conversation, so no owner is
    /// asked before they run, as with RAX's own question, plan and sign-in tools.
    pub talking_tools: Vec<String>,
    /// The schemes of the resources the embedder links in prompts, which the gateway offers at
    /// `initialize`.
    pub resource_schemes: Vec<String>,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            name: "gateway".to_owned(),
            node_prefix: "rax_node_".to_owned(),
            user_prefix: "rax_user_".to_owned(),
            principals: principal::any(),
            talking_tools: Vec::new(),
            resource_schemes: Vec::new(),
        }
    }
}
