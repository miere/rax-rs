#![cfg(feature = "firestore")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Runs only with `FIRESTORE_EMULATOR_HOST` set; every test gets a collection of its own so they
//! never see each other's documents.

mod contract;

use std::sync::Arc;
use std::time::Duration;

use contract::*;
use rax_gateway::Principals;
use rax_gateway::store::firestore::string_value;
use rax_gateway::store::{
    FirestoreLeader, FirestoreOptions, FirestoreStore, Leader, NodeToken, Scope, Store, StoreError,
    ToolMode, UserId, UserIdError, UserToken,
};
use serde_json::{Value, json};
use time::OffsetDateTime;

fn options() -> Option<FirestoreOptions> {
    let host = std::env::var("FIRESTORE_EMULATOR_HOST").ok()?;
    let mut options = FirestoreOptions::new(format!("test-{:016x}", rand::random::<u64>()));
    options.project_id = Some("demo-rax".into());
    options.emulator_host = Some(host);
    Some(options)
}

async fn open() -> Option<FirestoreStore> {
    Some(FirestoreStore::open(options()?).await.unwrap())
}

macro_rules! contract {
    ($($name:ident),+ $(,)?) => {$(
        #[tokio::test]
        async fn $name() {
            let Some(store) = open().await else {
                eprintln!("skipped: FIRESTORE_EMULATOR_HOST is not set");
                return;
            };
            contract::$name(&store).await;
        }
    )+};
}

contract!(
    the_admin_is_unset_until_assigned,
    approving_twice_keeps_the_first_approval_and_revoking_removes_it,
    a_person_is_not_pre_authorised_until_the_admin_says_so,
    a_node_owners_tool_rules_are_kept_apart_from_the_rest_of_their_settings,
    any_rule_or_mode_counts_as_an_owners_own_rules,
    seeding_gives_an_owner_without_rules_the_read_only_whitelist,
    a_token_is_revoked_once,
    a_user_token_is_kept_apart_from_node_tokens_and_revoked_once,
);

fn stamp() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_789_000_000).unwrap()
}

/// The fields of the document at `path`, minus any that vary from run to run.
async fn fields(store: &FirestoreStore, path: &str, varying: &[&str]) -> Value {
    let doc = store.get(path).await.unwrap().expect(path);
    let mut fields = doc.fields().clone();
    for field in varying {
        assert!(fields.remove(*field).is_some(), "{path} has no {field}");
    }
    Value::Object(fields)
}

/// The paths, field names and encodings every deployed gateway already holds; changing any of
/// them strands its data.
#[tokio::test]
async fn the_documents_keep_their_layout() {
    let Some(store) = open().await else {
        return;
    };
    let (admin, person) = (user("U0ADMIN01"), user("U0PERSON1"));
    store.set_admin(&admin).await.unwrap();
    assert_eq!(
        fields(&store, "", &[]).await,
        json!({"admin": {"stringValue": "U0ADMIN01"}})
    );

    store.approve(&person, &admin).await.unwrap();
    assert_eq!(
        fields(&store, "grants/U0PERSON1", &["approved_at"]).await,
        json!({"user_id": {"stringValue": "U0PERSON1"}, "approved_by": {"stringValue": "U0ADMIN01"}})
    );

    store.set_allowed(&person, true).await.unwrap();
    store
        .set_tool_mode(&person, ToolMode::AlwaysAllowed)
        .await
        .unwrap();
    store.whitelist_tool(&person, "Read").await.unwrap();
    store.deny_tool(&person, "Bash").await.unwrap();
    assert_eq!(
        fields(&store, "users/U0PERSON1", &[]).await,
        json!({
            "user_id": {"stringValue": "U0PERSON1"},
            "allowed": {"booleanValue": true},
            "tool_mode": {"stringValue": "always-allowed"},
            "tool_whitelist": {"arrayValue": {"values": [{"stringValue": "Read"}]}},
            "tool_denylist": {"arrayValue": {"values": [{"stringValue": "Bash"}]}},
        })
    );

    store
        .add_node_token(&NodeToken {
            selector: "0123456789abcdef".into(),
            secret_hash: "hash".into(),
            owner: person.clone(),
            name: "laptop".into(),
            created_at: stamp(),
            revoked_at: None,
            disabled_at: None,
        })
        .await
        .unwrap();
    store
        .set_node_disabled("0123456789abcdef", false)
        .await
        .unwrap();
    assert_eq!(
        fields(&store, "node_tokens/0123456789abcdef", &[]).await,
        json!({
            "selector": {"stringValue": "0123456789abcdef"},
            "secret_hash": {"stringValue": "hash"},
            "owner": {"stringValue": "U0PERSON1"},
            "name": {"stringValue": "laptop"},
            "created_at": {"timestampValue": "2026-09-10T00:26:40Z"},
            "disabled_at": {"nullValue": null},
        })
    );

    store
        .add_user_token(&UserToken {
            selector: "fedcba9876543210".into(),
            secret_hash: "hash".into(),
            owner: person.clone(),
            name: "editor".into(),
            created_at: stamp(),
            revoked_at: None,
            scopes: Scope::legacy(),
        })
        .await
        .unwrap();
    let scopes: Vec<Value> = Scope::legacy()
        .iter()
        .map(|scope| string_value(scope.as_str()))
        .collect();
    assert_eq!(
        fields(&store, "user_tokens/fedcba9876543210", &[]).await,
        json!({
            "selector": {"stringValue": "fedcba9876543210"},
            "secret_hash": {"stringValue": "hash"},
            "owner": {"stringValue": "U0PERSON1"},
            "name": {"stringValue": "editor"},
            "created_at": {"timestampValue": "2026-09-10T00:26:40Z"},
            "scopes": {"arrayValue": {"values": scopes}},
        })
    );

    let leader = FirestoreLeader::new(store.clone(), Duration::from_secs(30));
    leader.acquire("gateway-a").await.unwrap().unwrap();
    assert_eq!(
        fields(&store, "leases/leader", &["acquired_at"]).await,
        json!({
            "holder": {"stringValue": "gateway-a"},
            "lease_seconds": {"integerValue": "30"},
            "released": {"booleanValue": false},
        })
    );
}

/// What an embedder keeps beside the gateway's documents, through the public plumbing.
#[tokio::test]
async fn an_embedder_keeps_its_own_documents_under_the_same_root() {
    let Some(store) = open().await else {
        return;
    };
    let write = store.update(
        "notes/a",
        json!({"node": string_value("n1"), "text": string_value("hello")}),
    );
    store.commit(vec![write.clone()]).await.unwrap();
    assert!(matches!(
        store.commit(vec![write.if_absent()]).await,
        Err(StoreError::Conflict(_))
    ));
    store
        .commit(vec![
            store.update("notes/b", json!({"node": string_value("n2")})),
        ])
        .await
        .unwrap();

    let doc = store.get("notes/a").await.unwrap().unwrap();
    assert_eq!(doc.id(), "a");
    assert_eq!(doc.string("text").unwrap(), "hello");
    assert!(matches!(doc.string("absent"), Err(StoreError::Corrupt(_))));
    let mut listed: Vec<String> = store
        .list("notes")
        .await
        .unwrap()
        .iter()
        .map(|doc| doc.id().to_owned())
        .collect();
    listed.sort();
    assert_eq!(listed, ["a", "b"]);
    let on_n1 = store.query_equal("notes", "node", "n1").await.unwrap();
    assert_eq!(on_n1.len(), 1);

    let stale = doc.update_time().to_owned();
    store
        .commit(vec![
            store
                .update("notes/a", json!({"text": string_value("edited")}))
                .mask(["text"]),
        ])
        .await
        .unwrap();
    assert!(matches!(
        store
            .commit(vec![store.delete("notes/a").if_unchanged(&stale)])
            .await,
        Err(StoreError::Conflict(_))
    ));
    let doc = store.get("notes/a").await.unwrap().unwrap();
    assert_eq!(doc.string("node").unwrap(), "n1", "the mask kept the rest");
    store
        .commit(vec![
            store.delete("notes/a").if_unchanged(doc.update_time()),
        ])
        .await
        .unwrap();
    assert!(store.get("notes/a").await.unwrap().is_none());
}

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
    let Some(options) = options() else {
        return;
    };
    let loose = FirestoreStore::open(options.clone()).await.unwrap();
    loose.set_admin(&user("alice")).await.unwrap();
    loose.approve(&user("bob"), &user("alice")).await.unwrap();
    loose.set_allowed(&user("carol"), true).await.unwrap();

    let strict = FirestoreStore::open_with(options, Arc::new(OnlyU))
        .await
        .unwrap();
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
async fn an_empty_collection_is_refused() {
    let options = FirestoreOptions {
        emulator_host: Some("127.0.0.1:1".into()),
        ..FirestoreOptions::new(" ")
    };
    assert!(matches!(
        FirestoreStore::open(options).await,
        Err(StoreError::Backend(_))
    ));
}

#[tokio::test]
async fn one_gateway_leads_and_a_release_hands_over_at_once() {
    let Some(store) = open().await else {
        return;
    };
    let ttl = Duration::from_secs(30);
    let (first, second) = (
        FirestoreLeader::new(store.clone(), ttl),
        FirestoreLeader::new(store, ttl),
    );
    let lease = first.acquire("gateway-a").await.unwrap().unwrap();
    assert_eq!(second.acquire("gateway-b").await.unwrap(), None);
    let renewed = first.renew(&lease).await.unwrap().unwrap();
    assert_eq!(second.acquire("gateway-b").await.unwrap(), None);
    first.release(renewed).await.unwrap();
    assert!(second.acquire("gateway-b").await.unwrap().is_some());
}

#[tokio::test]
async fn an_expired_lease_is_taken_over_and_its_old_holder_learns_on_renewal() {
    let Some(store) = open().await else {
        return;
    };
    let ttl = Duration::from_secs(1);
    let (first, second) = (
        FirestoreLeader::new(store.clone(), ttl),
        FirestoreLeader::new(store, ttl),
    );
    let stale = first.acquire("gateway-a").await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let taken = second.acquire("gateway-b").await.unwrap();
    assert!(taken.is_some(), "the expired lease was not taken over");
    assert_eq!(first.renew(&stale).await.unwrap(), None);
}

#[tokio::test]
async fn two_gateways_racing_for_a_free_lease_never_both_win() {
    let Some(store) = open().await else {
        return;
    };
    let ttl = Duration::from_secs(30);
    let mut attempts = tokio::task::JoinSet::new();
    for index in 0..6 {
        let leader = FirestoreLeader::new(store.clone(), ttl);
        attempts.spawn(async move { leader.acquire(&format!("gateway-{index}")).await });
    }
    let won = attempts
        .join_all()
        .await
        .into_iter()
        .filter(|result| matches!(result, Ok(Some(_))))
        .count();
    assert_eq!(won, 1);
}
