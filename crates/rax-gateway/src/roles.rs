//! Who may do what on this gateway, and what each change carries with it. A node token rests on a
//! grant, and a grant on being allowed: minting grants, granting allows, and taking a grant away
//! takes every token resting on it. Allowing and disallowing people, granting, minting and
//! revoking all go through here, from a CLI and an embedder's own screens alike, so they cannot
//! drift.

use std::collections::BTreeSet;

use time::OffsetDateTime;

use crate::settings::Settings;
use crate::store::{Grant, NodeToken, Result, Scope, Store, StoreError, UserId, UserToken};
use crate::token;

/// A token just minted. `token` is the secret, shown once and never stored.
pub struct Minted {
    pub selector: String,
    pub token: String,
    pub owner: UserId,
    pub name: String,
}

/// What [`reconcile`] had to change to make the store hold to the rules.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Reconciled {
    /// Granted people who were not allowed, and now are.
    pub allowed: Vec<UserId>,
    /// Live tokens whose owner held no grant, now revoked.
    pub revoked: Vec<String>,
}

pub async fn allow(store: &dyn Store, user: &UserId) -> Result<()> {
    store.set_allowed(user, true).await
}

/// Takes the person's grant and tokens too, when they hold any. Returns the revoked selectors.
pub async fn disallow(store: &dyn Store, user: &UserId) -> Result<Vec<String>> {
    let (_, revoked) = revoke_grant(store, user).await?;
    store.set_allowed(user, false).await?;
    Ok(revoked)
}

/// Lets a person connect nodes, and allows them on the gateway with it.
pub async fn grant(store: &dyn Store, user: &UserId, by: &UserId) -> Result<Grant> {
    let grant = store.approve(user, by).await?;
    allow(store, user).await?;
    Ok(grant)
}

/// Returns whether there was a grant, and the selectors of the tokens revoked with it. They are
/// revoked for good: granting the person again means minting their nodes new tokens.
pub async fn revoke_grant(store: &dyn Store, user: &UserId) -> Result<(bool, Vec<String>)> {
    let had = store.revoke(user).await?;
    let mut revoked = Vec::new();
    for token in store.node_tokens().await? {
        if &token.owner == user
            && token.revoked_at.is_none()
            && store.revoke_node_token(&token.selector).await?
        {
            revoked.push(token.selector);
        }
    }
    Ok((had, revoked))
}

/// Mints a token for one of `owner`'s nodes, granting them first unless they are the admin, who
/// needs no grant. `by` is who approved the grant, if one was needed.
pub async fn mint(
    store: &dyn Store,
    settings: &Settings,
    owner: &UserId,
    name: &str,
    by: &UserId,
) -> Result<Minted> {
    if store.admin().await?.as_ref() != Some(owner) {
        grant(store, owner, by).await?;
    }
    let minted = token::mint(&settings.node_prefix);
    store
        .add_node_token(&NodeToken {
            selector: minted.selector.clone(),
            secret_hash: minted.secret_hash,
            owner: owner.clone(),
            name: name.to_owned(),
            created_at: OffsetDateTime::now_utc(),
            revoked_at: None,
            disabled_at: None,
        })
        .await?;
    Ok(Minted {
        selector: minted.selector,
        token: minted.token,
        owner: owner.clone(),
        name: name.to_owned(),
    })
}

/// Mints a token for one of `owner`'s own clients, opening the entry points `scopes` names,
/// allowing them first: a client runs nothing, so being allowed is all it rests on. Who may mint
/// for whom is the caller's to check.
pub async fn mint_client(
    store: &dyn Store,
    settings: &Settings,
    owner: &UserId,
    name: &str,
    scopes: BTreeSet<Scope>,
) -> Result<Minted> {
    if scopes.is_empty() {
        return Err(StoreError::Corrupt(
            "a client token must open at least one scope".to_owned(),
        ));
    }
    allow(store, owner).await?;
    let minted = token::mint(&settings.user_prefix);
    store
        .add_user_token(&UserToken {
            selector: minted.selector.clone(),
            secret_hash: minted.secret_hash,
            owner: owner.clone(),
            name: name.to_owned(),
            created_at: OffsetDateTime::now_utc(),
            revoked_at: None,
            scopes,
        })
        .await?;
    Ok(Minted {
        selector: minted.selector,
        token: minted.token,
        owner: owner.clone(),
        name: name.to_owned(),
    })
}

/// Hands the gateway over. The outgoing admin keeps their machines: they needed no grant as the
/// admin, so they are granted one by their successor, which allows them too.
pub async fn set_admin(store: &dyn Store, next: &UserId) -> Result<()> {
    let previous = store.admin().await?;
    store.set_admin(next).await?;
    if let Some(previous) = previous.filter(|previous| previous != next) {
        grant(store, &previous, next).await?;
    }
    Ok(())
}

/// Brings a store written before these rules into line with them: everyone granted is allowed,
/// and a token whose owner lost their grant, which used to lie dormant, is revoked.
pub async fn reconcile(store: &dyn Store) -> Result<Reconciled> {
    let mut reconciled = Reconciled::default();
    let admin = store.admin().await?;
    let grants = store.grants().await?;
    let allowed: Vec<UserId> = store
        .users()
        .await?
        .into_iter()
        .filter(|user| user.allowed)
        .map(|user| user.user)
        .collect();
    for grant in &grants {
        if !allowed.contains(&grant.user) {
            allow(store, &grant.user).await?;
            reconciled.allowed.push(grant.user.clone());
        }
    }
    for token in store.node_tokens().await? {
        let owner_may = admin.as_ref() == Some(&token.owner)
            || grants.iter().any(|grant| grant.user == token.owner);
        if token.revoked_at.is_none()
            && !owner_may
            && store.revoke_node_token(&token.selector).await?
        {
            reconciled.revoked.push(token.selector);
        }
    }
    Ok(reconciled)
}

#[cfg(all(test, feature = "sqlite"))]
#[allow(clippy::unwrap_used)]
mod tests {
    use crate::store::SqliteStore;

    use super::*;

    fn user(raw: &str) -> UserId {
        UserId::parse(raw).unwrap()
    }

    async fn store() -> (tempfile::TempDir, SqliteStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(&dir.path().join("gateway.db")).unwrap();
        store.set_admin(&user("U0ADMIN01")).await.unwrap();
        (dir, store)
    }

    async fn allowed(store: &dyn Store, who: &UserId) -> bool {
        store.user(who).await.unwrap().allowed
    }

    async fn granted(store: &dyn Store, who: &UserId) -> bool {
        store
            .grants()
            .await
            .unwrap()
            .iter()
            .any(|grant| &grant.user == who)
    }

    async fn live(store: &dyn Store, selector: &str) -> bool {
        store
            .node_tokens()
            .await
            .unwrap()
            .iter()
            .any(|token| token.selector == selector && token.revoked_at.is_none())
    }

    #[tokio::test]
    async fn minting_grants_and_granting_allows() {
        let (_dir, store) = store().await;
        let alice = user("U0ALICE01");
        let minted = mint(
            &store,
            &Settings::default(),
            &alice,
            "laptop",
            &user("U0ADMIN01"),
        )
        .await
        .unwrap();
        assert!(granted(&store, &alice).await);
        assert!(allowed(&store, &alice).await);
        assert!(live(&store, &minted.selector).await);
    }

    #[tokio::test]
    async fn the_admin_mints_for_themselves_without_a_grant() {
        let (_dir, store) = store().await;
        let admin = user("U0ADMIN01");
        mint(&store, &Settings::default(), &admin, "desktop", &admin)
            .await
            .unwrap();
        assert!(!granted(&store, &admin).await);
    }

    #[tokio::test]
    async fn revoking_a_node_leaves_the_grant_and_revoking_a_grant_leaves_the_allow() {
        let (_dir, store) = store().await;
        let admin = user("U0ADMIN01");
        let alice = user("U0ALICE01");
        let laptop = mint(&store, &Settings::default(), &alice, "laptop", &admin)
            .await
            .unwrap();
        let desktop = mint(&store, &Settings::default(), &alice, "desktop", &admin)
            .await
            .unwrap();
        store.revoke_node_token(&laptop.selector).await.unwrap();
        assert!(granted(&store, &alice).await);

        let (had, revoked) = revoke_grant(&store, &alice).await.unwrap();
        assert!(had);
        assert_eq!(revoked, std::slice::from_ref(&desktop.selector));
        assert!(!live(&store, &desktop.selector).await);
        assert!(allowed(&store, &alice).await);

        grant(&store, &alice, &admin).await.unwrap();
        assert!(
            !live(&store, &desktop.selector).await,
            "a new grant revived an old token"
        );
    }

    #[tokio::test]
    async fn disallowing_a_node_admin_takes_their_grant_and_tokens() {
        let (_dir, store) = store().await;
        let alice = user("U0ALICE01");
        let laptop = mint(
            &store,
            &Settings::default(),
            &alice,
            "laptop",
            &user("U0ADMIN01"),
        )
        .await
        .unwrap();
        assert_eq!(
            disallow(&store, &alice).await.unwrap(),
            std::slice::from_ref(&laptop.selector)
        );
        assert!(!allowed(&store, &alice).await);
        assert!(!granted(&store, &alice).await);
        assert!(!live(&store, &laptop.selector).await);
    }

    #[tokio::test]
    async fn the_outgoing_admin_keeps_their_machines_as_a_node_admin() {
        let (_dir, store) = store().await;
        let old = user("U0ADMIN01");
        let next = user("U0ALICE01");
        set_admin(&store, &next).await.unwrap();
        assert_eq!(store.admin().await.unwrap(), Some(next.clone()));
        assert!(granted(&store, &old).await && allowed(&store, &old).await);
        assert!(!granted(&store, &next).await);
    }

    #[tokio::test]
    async fn reconciling_allows_the_granted_and_revokes_dormant_tokens() {
        let (_dir, store) = store().await;
        let admin = user("U0ADMIN01");
        let alice = user("U0ALICE01");
        let bob = user("U0BOB0001");
        store.approve(&alice, &admin).await.unwrap();
        let orphan = mint(&store, &Settings::default(), &bob, "old-mac", &admin)
            .await
            .unwrap();
        store.revoke(&bob).await.unwrap();
        let own = mint(&store, &Settings::default(), &admin, "desktop", &admin)
            .await
            .unwrap();

        let reconciled = reconcile(&store).await.unwrap();
        assert_eq!(reconciled.allowed, std::slice::from_ref(&alice));
        assert_eq!(reconciled.revoked, std::slice::from_ref(&orphan.selector));
        assert!(allowed(&store, &alice).await);
        assert!(live(&store, &own.selector).await);
        assert_eq!(reconcile(&store).await.unwrap(), Reconciled::default());
    }
}
