#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod contract;

use contract::*;
use std::sync::Arc;

use rax_gateway::Principals;
use rax_gateway::store::{
    Leader, SqliteLeader, SqliteStore, Store, StoreError, UserId, UserIdError,
};

fn open() -> (tempfile::TempDir, SqliteStore) {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteStore::open(&dir.path().join("config.db")).unwrap();
    (dir, store)
}

macro_rules! contract {
    ($($name:ident),+ $(,)?) => {$(
        #[tokio::test]
        async fn $name() {
            let (_dir, store) = open();
            contract::$name(&store).await;
        }
    )+};
}

contract!(
    the_admin_is_unset_until_assigned,
    approving_twice_keeps_the_first_approval_and_revoking_removes_it,
    a_person_is_not_pre_authorised_until_the_admin_says_so,
    a_node_owners_tool_rules_are_kept_apart_from_the_rest_of_their_settings,
    a_token_is_revoked_once,
    a_user_token_is_kept_apart_from_node_tokens_and_revoked_once,
);

/// Ids that start with `U`, as an embedder with stricter people would have them.
struct OnlyU;

impl Principals for OnlyU {
    fn parse(&self, raw: &str) -> Result<UserId, UserIdError> {
        match raw.starts_with('U') {
            true => UserId::parse(raw),
            false => Err(UserIdError::new(
                raw,
                format!("{raw:?} does not start with U"),
            )),
        }
    }
}

#[tokio::test]
async fn a_stored_id_the_embedder_would_not_take_is_corrupt_wherever_it_is_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.db");
    let loose = SqliteStore::open(&path).unwrap();
    loose.set_admin(&user("alice")).await.unwrap();
    loose.approve(&user("bob"), &user("alice")).await.unwrap();
    loose.set_allowed(&user("carol"), true).await.unwrap();

    let strict = SqliteStore::open_with(&path, Arc::new(OnlyU)).unwrap();
    for err in [
        strict.admin().await.map(drop).unwrap_err(),
        strict.grants().await.map(drop).unwrap_err(),
        strict.users().await.map(drop).unwrap_err(),
    ] {
        match err {
            StoreError::Corrupt(message) => assert!(message.contains("does not start with U")),
            other => panic!("not corrupt: {other}"),
        }
    }
}

#[tokio::test]
async fn a_stored_scope_this_gateway_does_not_know_is_corrupt() {
    let (_dir, store) = open();
    store
        .run(|db| {
            Ok(db.execute(
                "INSERT INTO user_tokens (selector, secret_hash, owner, name, created_at, scopes)
                 VALUES ('0123456789abcdef', 'h', 'U0PERSON1', 'n', '2026-01-01T00:00:00Z', 'admin')",
                [],
            )?)
        })
        .await
        .unwrap();
    match store.user_tokens().await.unwrap_err() {
        StoreError::Corrupt(message) => assert!(message.contains("not a token scope")),
        other => panic!("not corrupt: {other}"),
    }
}

#[tokio::test]
async fn a_second_process_sees_every_write() {
    let (dir, store) = open();
    let cli = SqliteStore::open(&dir.path().join("config.db")).unwrap();
    cli.approve(&user("U0PERSON1"), &user("U0ADMIN01"))
        .await
        .unwrap();
    assert_eq!(store.grants().await.unwrap().len(), 1);
}

#[test]
fn only_one_gateway_holds_the_leader_lock_and_it_frees_on_drop() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.db");
    let held = SqliteStore::lock_leader(&path).unwrap();
    assert!(matches!(
        SqliteStore::lock_leader(&path),
        Err(StoreError::Locked(_))
    ));
    drop(held);
    SqliteStore::lock_leader(&path).unwrap();
}

#[tokio::test]
async fn one_sqlite_leader_per_machine_until_it_releases() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.db");
    let (first, second) = (SqliteLeader::new(path.clone()), SqliteLeader::new(path));
    let lease = first.acquire("a").await.unwrap().unwrap();
    assert_eq!(second.acquire("b").await.unwrap(), None);
    assert!(first.renew(&lease).await.unwrap().is_some());
    first.release(lease).await.unwrap();
    assert!(second.acquire("b").await.unwrap().is_some());
}
