//! Who a node's owner lets talk to their machine, read from the `<name>_access` key of the metadata
//! the node sends, `<name>` being the gateway's own as [`Settings`] gives it. The owner decides and the gateway enforces, because only the gateway
//! knows who is speaking. Nobody is let past it, the gateway's admin included: it is the owner's
//! machine.

use std::collections::HashSet;

use rax::{Metadata, Rejection};
use serde_json::Value;

use crate::principal::Principals;
use crate::settings::Settings;
use crate::store::UserId;

/// The name under the gateway's prefix, so the key a node writes is `<name>_access`.
pub const ACCESS: &str = "access";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeAccess {
    AlwaysAllow,
    AllowList(HashSet<UserId>),
    /// The node's credential may serve its owner alone, such as a Claude subscription, so it said
    /// `owner_only` at `initialize`. Never read from metadata, and nothing in it widens this.
    OwnerOnly,
}

/// A node that says nothing serves its owner alone. Anyone allowed on the gateway may be routed to
/// any node that lets them in, so sharing a machine has to be something its owner said.
impl Default for NodeAccess {
    fn default() -> Self {
        Self::AllowList(HashSet::new())
    }
}

impl NodeAccess {
    /// The owner is let in whether listed or not: an allow list is about other people.
    pub fn admits(&self, owner: &UserId, user: &UserId) -> bool {
        match self {
            Self::AlwaysAllow => true,
            Self::AllowList(people) => user == owner || people.contains(user),
            Self::OwnerOnly => user == owner,
        }
    }

    /// What the node's metadata lets in, unless its credential confines it to its owner.
    pub fn confined(self, owner_only: bool) -> Self {
        if owner_only { Self::OwnerOnly } else { self }
    }
}

/// A node's metadata as this gateway understands it, and the keys it did not.
#[derive(Debug, Default)]
pub struct Read {
    pub access: NodeAccess,
    pub ignored: Vec<String>,
}

/// Strict about the keys this gateway owns and indifferent to every other: a mistyped allow list
/// that fell back to `always_allow` would leave the machine open to everyone, so it is refused.
pub fn read(metadata: &Metadata, settings: &Settings) -> Result<Read, Rejection> {
    let mut read = Read::default();
    for (key, value) in metadata {
        match rax::metadata::owner(key) {
            Some((gateway, ACCESS)) if gateway == settings.name => {
                read.access =
                    access(value, &*settings.principals).map_err(|problem| Rejection {
                        key: Some(key.clone()),
                        message: format!("`{key}` {problem}"),
                    })?;
            }
            _ => read.ignored.push(key.clone()),
        }
    }
    Ok(read)
}

fn access(value: &Value, principals: &dyn Principals) -> Result<NodeAccess, String> {
    let noun = principals.noun();
    let Some(fields) = value.as_object() else {
        return Err("must be a table with a `policy`".to_owned());
    };
    if let Some(unknown) = fields
        .keys()
        .find(|field| !matches!(field.as_str(), "policy" | "people"))
    {
        return Err(format!("has an unknown field `{unknown}`"));
    }
    let people = fields.get("people");
    match fields.get("policy").and_then(Value::as_str) {
        Some("always_allow") if people.is_some() => Err(
            "lists `people`, which `always_allow` would ignore; remove them or use `allow_list`"
                .to_owned(),
        ),
        Some("always_allow") => Ok(NodeAccess::AlwaysAllow),
        Some("allow_list") => {
            let Some(people) = people.and_then(Value::as_array) else {
                return Err(format!(
                    "needs `people`, a list of {noun}s, for `allow_list`"
                ));
            };
            people
                .iter()
                .map(|person| {
                    person
                        .as_str()
                        .and_then(|raw| principals.parse(raw).ok())
                        .ok_or_else(|| format!("lists {person}, which is not a {noun}"))
                })
                .collect::<Result<_, _>>()
                .map(NodeAccess::AllowList)
        }
        Some(other) => Err(format!(
            "has an unknown policy `{other}`; use `always_allow` or `allow_list`"
        )),
        None => Err("needs a `policy`: `always_allow` or `allow_list`".to_owned()),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use serde_json::json;

    use super::*;

    fn user(raw: &str) -> UserId {
        UserId::parse(raw).unwrap()
    }

    fn metadata(value: Value) -> Metadata {
        Metadata::from([("gateway_access".to_owned(), value)])
    }

    fn read(metadata: &Metadata) -> Result<Read, Rejection> {
        super::read(metadata, &Settings::default())
    }

    #[test]
    fn an_owner_only_node_admits_its_owner_alone_whatever_its_metadata_says() {
        let owner = user("U0ALICE01");
        let shared = read(&metadata(json!({"policy": "always_allow"}))).unwrap();
        let confined = shared.access.clone().confined(true);
        assert!(confined.admits(&owner, &owner));
        assert!(!confined.admits(&owner, &user("U0BOB0001")));
        assert_eq!(shared.access.confined(false), NodeAccess::AlwaysAllow);
    }

    #[test]
    fn a_node_that_says_nothing_serves_only_its_owner() {
        let read = read(&Metadata::new()).unwrap();
        let owner = user("U0ALICE01");
        assert!(read.access.admits(&owner, &owner));
        assert!(!read.access.admits(&owner, &user("U0ADMIN01")));
        assert!(read.ignored.is_empty());
    }

    #[test]
    fn an_allow_list_admits_the_listed_and_the_owner_and_nobody_else() {
        let read = read(&metadata(
            json!({"policy": "allow_list", "people": ["U0BOB0001"]}),
        ))
        .unwrap();
        let owner = user("U0ALICE01");
        assert!(read.access.admits(&owner, &owner));
        assert!(read.access.admits(&owner, &user("U0BOB0001")));
        assert!(!read.access.admits(&owner, &user("U0ADMIN01")));
    }

    #[test]
    fn keys_this_gateway_does_not_own_or_know_are_ignored() {
        let metadata = Metadata::from([
            (
                "gateway_access".to_owned(),
                json!({"policy": "always_allow"}),
            ),
            ("gateway_colour".to_owned(), json!("teal")),
            ("other_access".to_owned(), json!(1)),
        ]);
        let read = read(&metadata).unwrap();
        assert_eq!(read.ignored, ["gateway_colour", "other_access"]);
    }

    #[test]
    fn a_malformed_access_key_is_rejected_by_name() {
        for (value, problem) in [
            (json!("everyone"), "must be a table"),
            (json!({"people": []}), "needs a `policy`"),
            (json!({"policy": "everyone"}), "unknown policy `everyone`"),
            (json!({"policy": "allow_list"}), "needs `people`"),
            (
                json!({"policy": "allow_list", "people": "U0BOB0001"}),
                "needs `people`",
            ),
            (
                json!({"policy": "allow_list", "people": ["bob smith"]}),
                "\"bob smith\"",
            ),
            (json!({"policy": "allow_list", "people": [7]}), "lists 7"),
            (
                json!({"policy": "always_allow", "people": []}),
                "would ignore",
            ),
            (
                json!({"policy": "always_allow", "who": 1}),
                "unknown field `who`",
            ),
        ] {
            let rejection = read(&metadata(value)).unwrap_err();
            assert_eq!(rejection.key.as_deref(), Some("gateway_access"));
            assert!(
                rejection.message.contains(problem),
                "{} does not mention {problem}",
                rejection.message
            );
        }
    }

    #[test]
    fn the_key_and_the_ids_are_the_embedders() {
        struct Upper;
        impl Principals for Upper {
            fn noun(&self) -> &str {
                "badge"
            }
            fn parse(&self, raw: &str) -> Result<UserId, crate::store::UserIdError> {
                match raw.bytes().all(|b| b.is_ascii_uppercase()) {
                    true => UserId::parse(raw),
                    false => Err(crate::store::UserIdError::new(raw, "lower")),
                }
            }
        }
        let settings = Settings {
            name: "acme".into(),
            principals: std::sync::Arc::new(Upper),
            ..Settings::default()
        };
        let ours = Metadata::from([(
            "acme_access".to_owned(),
            json!({"policy": "allow_list", "people": ["BOB"]}),
        )]);
        let read = super::read(&ours, &settings).unwrap();
        assert!(read.access.admits(&user("ALICE"), &user("BOB")));
        let gateways = Metadata::from([("gateway_access".to_owned(), json!("anything"))]);
        assert_eq!(
            super::read(&gateways, &settings).unwrap().ignored,
            ["gateway_access"]
        );
        let lower = Metadata::from([(
            "acme_access".to_owned(),
            json!({"policy": "allow_list", "people": ["bob"]}),
        )]);
        let rejection = super::read(&lower, &settings).unwrap_err();
        assert!(rejection.message.contains("which is not a badge"));
    }
}
