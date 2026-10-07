#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use rax::transport::{Refusal, Role};

#[test]
fn no_subprotocol_means_a_node() {
    assert_eq!(Role::from_subprotocols(None), Some(Role::Node));
}

#[test]
fn a_dialler_names_its_role_as_a_subprotocol() {
    for role in [Role::Node, Role::Gateway] {
        assert_eq!(
            Role::from_subprotocols(Some(role.subprotocol())),
            Some(role)
        );
    }
    assert_eq!(
        Role::from_subprotocols(Some("chat, rax.v1.gateway")),
        Some(Role::Gateway)
    );
    assert_eq!(Role::from_subprotocols(Some("rax.v2.node")), None);
    assert_eq!(Role::Gateway.peer(), Role::Node);
}

#[test]
fn a_refused_role_is_not_redialled() {
    assert_eq!(Refusal::from_status(403), Refusal::RoleRefused);
    assert!(!Refusal::RoleRefused.should_redial());
    assert!(!Refusal::from_status(401).should_redial());
    assert!(Refusal::from_status(503).should_redial());
}
