//! Credentials: `<prefix><selector>_<secret>`, with one prefix for a node and another for a person's
//! own client, as [`Settings`](crate::Settings) names them. The selector finds the record; only the
//! secret's SHA-256 is stored, so the database never holds anything a node or a client could
//! present.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

const SELECTOR_BYTES: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credential {
    pub selector: String,
    pub secret: String,
}

/// The token is shown once, at minting, and never stored.
pub struct Minted {
    pub token: String,
    pub selector: String,
    pub secret_hash: String,
}

pub fn mint(prefix: &str) -> Minted {
    let selector = hex::encode(rand::random::<[u8; SELECTOR_BYTES]>());
    let secret = URL_SAFE_NO_PAD.encode(rand::random::<[u8; 32]>());
    Minted {
        token: format!("{prefix}{selector}_{secret}"),
        secret_hash: hash(&secret),
        selector,
    }
}

/// Only a credential of the kind `prefix` names: a node's token never opens a client's link, nor
/// the reverse.
pub fn parse(prefix: &str, presented: &str) -> Option<Credential> {
    let rest = presented.trim().strip_prefix(prefix)?;
    let (selector, secret) = rest.split_once('_')?;
    let valid_selector = selector.len() == SELECTOR_BYTES * 2
        && selector.bytes().all(|byte| byte.is_ascii_hexdigit());
    (valid_selector && !secret.is_empty()).then(|| Credential {
        selector: selector.to_owned(),
        secret: secret.to_owned(),
    })
}

pub fn hash(secret: &str) -> String {
    hex::encode(Sha256::digest(secret.as_bytes()))
}

pub fn matches(secret: &str, stored_hash: &str) -> bool {
    hash(secret).as_bytes().ct_eq(stored_hash.as_bytes()).into()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    const NODE_PREFIX: &str = "rax_node_";
    const USER_PREFIX: &str = "rax_user_";

    #[test]
    fn a_minted_token_parses_and_matches_only_its_own_hash() {
        let minted = mint(NODE_PREFIX);
        let credential = parse(NODE_PREFIX, &minted.token).unwrap();
        assert_eq!(credential.selector, minted.selector);
        assert!(matches(&credential.secret, &minted.secret_hash));
        assert!(!matches("another-secret", &minted.secret_hash));
        assert!(!minted.secret_hash.contains(&credential.secret));
    }

    #[test]
    fn malformed_tokens_do_not_parse() {
        for bad in [
            "",
            "rax_node_",
            "rax_node_0123456789abcdef",
            "rax_node_0123456789abcdef_",
            "rax_node_0123_secret",
            "rax_node_0123456789abcdeg_secret",
            "xoxb-0123456789abcdef_secret",
        ] {
            assert!(parse(NODE_PREFIX, bad).is_none(), "{bad:?} parsed");
        }
    }

    #[test]
    fn a_token_parses_only_as_its_own_kind() {
        let user = mint(USER_PREFIX);
        assert!(user.token.starts_with("rax_user_"));
        assert!(parse(USER_PREFIX, &user.token).is_some());
        assert!(parse(NODE_PREFIX, &user.token).is_none());
        assert!(parse(USER_PREFIX, &mint(NODE_PREFIX).token).is_none());
    }
}
