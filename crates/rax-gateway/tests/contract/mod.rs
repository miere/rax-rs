#![allow(dead_code, clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use rax_gateway::store::{NodeToken, Scope, Store, ToolMode, UserConfig, UserId, UserToken};
use time::OffsetDateTime;

pub fn user(raw: &str) -> UserId {
    UserId::parse(raw).unwrap()
}

fn stamp() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_789_000_000).unwrap()
}

pub async fn the_admin_is_unset_until_assigned(store: &dyn Store) {
    assert_eq!(store.admin().await.unwrap(), None);
    store.set_admin(&user("U0ADMIN01")).await.unwrap();
    assert_eq!(store.admin().await.unwrap(), Some(user("U0ADMIN01")));
}

pub async fn approving_twice_keeps_the_first_approval_and_revoking_removes_it(store: &dyn Store) {
    let first = store
        .approve(&user("U0PERSON1"), &user("U0ADMIN01"))
        .await
        .unwrap();
    let again = store
        .approve(&user("U0PERSON1"), &user("U0OTHER01"))
        .await
        .unwrap();
    assert_eq!(first, again);
    assert_eq!(store.grants().await.unwrap(), [first]);
    assert!(store.revoke(&user("U0PERSON1")).await.unwrap());
    assert!(!store.revoke(&user("U0PERSON1")).await.unwrap());
    assert!(store.grants().await.unwrap().is_empty());
}

pub async fn a_person_is_not_pre_authorised_until_the_admin_says_so(store: &dyn Store) {
    assert!(!store.user(&user("U0PERSON1")).await.unwrap().allowed);
    store.set_allowed(&user("U0PERSON1"), true).await.unwrap();
    assert!(store.user(&user("U0PERSON1")).await.unwrap().allowed);
    store.set_allowed(&user("U0PERSON1"), false).await.unwrap();
    let users = store.users().await.unwrap();
    assert_eq!(users.len(), 1);
    assert!(!users[0].allowed);
}

pub async fn a_node_owners_tool_rules_are_kept_apart_from_the_rest_of_their_settings(
    store: &dyn Store,
) {
    let owner = user("U0PERSON1");
    let fresh = store.user(&owner).await.unwrap();
    assert_eq!(fresh.tool_mode, ToolMode::AlwaysAllowed);
    assert!(fresh.whitelist.is_empty());

    store
        .set_tool_mode(&owner, ToolMode::AllowedWhitelist)
        .await
        .unwrap();
    assert!(store.whitelist_tool(&owner, "Bash").await.unwrap());
    assert!(!store.whitelist_tool(&owner, "Bash").await.unwrap());
    assert!(
        store
            .whitelist_tool(&owner, "mcp__slack__send")
            .await
            .unwrap()
    );
    store.set_allowed(&owner, true).await.unwrap();

    let config = store.user(&owner).await.unwrap();
    assert!(config.allowed, "setting allowed lost nothing else");
    assert_eq!(config.tool_mode, ToolMode::AllowedWhitelist);
    assert_eq!(
        config
            .whitelist
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["Bash", "mcp__slack__send"]
    );
    assert!(store.unwhitelist_tool(&owner, "Bash").await.unwrap());
    assert!(!store.unwhitelist_tool(&owner, "Bash").await.unwrap());
    store.set_tool_mode(&owner, ToolMode::Denied).await.unwrap();
    let users = store.users().await.unwrap();
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].tool_mode, ToolMode::Denied);
    assert_eq!(users[0].whitelist.len(), 1);
    assert_eq!(
        store.user(&user("U0SOMEONE")).await.unwrap(),
        UserConfig::new(user("U0SOMEONE"))
    );
}

pub async fn a_token_is_revoked_once(store: &dyn Store) {
    let token = NodeToken {
        selector: "0123456789abcdef".into(),
        secret_hash: "hash".into(),
        owner: user("U0PERSON1"),
        name: "laptop".into(),
        created_at: stamp(),
        revoked_at: None,
        disabled_at: None,
    };
    store.add_node_token(&token).await.unwrap();
    assert_eq!(
        store.node_tokens().await.unwrap(),
        std::slice::from_ref(&token)
    );
    assert!(
        store.add_node_token(&token).await.is_err(),
        "a selector was reused"
    );
    assert!(store.revoke_node_token(&token.selector).await.unwrap());
    assert!(!store.revoke_node_token(&token.selector).await.unwrap());
    assert!(!store.revoke_node_token("ffffffffffffffff").await.unwrap());
    assert!(store.node_tokens().await.unwrap()[0].revoked_at.is_some());
}

pub async fn a_user_token_is_kept_apart_from_node_tokens_and_revoked_once(store: &dyn Store) {
    let token = UserToken {
        selector: "0123456789abcdef".into(),
        secret_hash: "hash".into(),
        owner: user("U0PERSON1"),
        name: "editor".into(),
        created_at: stamp(),
        revoked_at: None,
        scopes: [Scope::Rax].into(),
    };
    store.add_user_token(&token).await.unwrap();
    assert_eq!(
        store.user_tokens().await.unwrap(),
        std::slice::from_ref(&token)
    );
    assert!(store.node_tokens().await.unwrap().is_empty());
    assert!(
        store.add_user_token(&token).await.is_err(),
        "a selector was reused"
    );
    assert!(!store.revoke_node_token(&token.selector).await.unwrap());
    assert!(store.revoke_user_token(&token.selector).await.unwrap());
    assert!(!store.revoke_user_token(&token.selector).await.unwrap());
    assert!(store.user_tokens().await.unwrap()[0].revoked_at.is_some());
}
