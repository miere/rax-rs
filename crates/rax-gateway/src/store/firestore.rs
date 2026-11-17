//! Firestore over its REST API, behind the `firestore` feature.
//!
//! Everything lives under one document, `<collection>/gateway`, so several gateways can share a
//! database by choosing different collections:
//!
//! | Path, under `<collection>/gateway` | Fields                                                     |
//! |------------------------------------|------------------------------------------------------------|
//! | the document itself                | `admin`                                                    |
//! | `grants/<user>`                    | `user_id`, `approved_by`, `approved_at`                    |
//! | `users/<user>`                     | `user_id`, `allowed`, `tool_mode`, `tool_whitelist`, `tool_denylist` |
//! | `node_tokens/<selector>`           | `selector`, `secret_hash`, `owner`, `name`, `created_at`, `revoked_at`, `disabled_at` |
//! | `user_tokens/<selector>`           | `selector`, `secret_hash`, `owner`, `name`, `created_at`, `revoked_at`, `scopes` |
//! | `leases/leader`                    | `holder`, `lease_seconds`, `released`, `acquired_at`       |
//!
//! An embedder that keeps more beside the gateway's documents puts it in subcollections of its
//! own under the same root, and reaches them through [`FirestoreStore::get`],
//! [`FirestoreStore::list`], [`FirestoreStore::query_equal`] and [`FirestoreStore::commit`].

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Map, Value, json};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::{
    Grant, NodeToken, Result, Scope, Store, StoreError, ToolMode, ToolModeError, UserConfig,
    UserId, UserToken,
};
use crate::principal::{self, Principals};

const SCOPE: &str = "https://www.googleapis.com/auth/datastore";
const PAGE_SIZE: u32 = 300;
/// The project an emulator is addressed by when the options name none.
const EMULATOR_PROJECT: &str = "demo-rax";

/// Where the store lives. Only the collection is required; Google's default credentials and
/// project, and the `(default)` database, fill in the rest.
#[derive(Debug, Clone)]
pub struct FirestoreOptions {
    /// The top-level collection holding this gateway's root document, `<collection>/gateway`.
    pub collection: String,
    /// Defaults to the credentials' project, or `demo-rax` on an emulator.
    pub project_id: Option<String>,
    /// Defaults to `(default)`.
    pub database_id: Option<String>,
    /// A service account key file. Defaults to Google's application default credentials.
    pub credentials_file: Option<PathBuf>,
    /// `host:port` of an emulator, which needs no credentials. Defaults to `FIRESTORE_EMULATOR_HOST`.
    pub emulator_host: Option<String>,
}

impl FirestoreOptions {
    pub fn new(collection: impl Into<String>) -> Self {
        Self {
            collection: collection.into(),
            project_id: None,
            database_id: None,
            credentials_file: None,
            emulator_host: None,
        }
    }
}

enum Auth {
    Emulator,
    Google(Arc<dyn gcp_auth::TokenProvider>),
}

/// The gateway's store in Firestore. Cheap to clone: clones share one HTTP client.
#[derive(Clone)]
pub struct FirestoreStore {
    inner: Arc<Inner>,
}

struct Inner {
    http: reqwest::Client,
    api: String,
    database: String,
    root: String,
    auth: Auth,
    principals: Arc<dyn Principals>,
}

/// A document as read, with its fields still in Firestore's typed encoding, such as
/// `{"stringValue": "…"}`.
#[derive(Debug, Clone)]
pub struct Document {
    id: String,
    fields: Map<String, Value>,
    update_time: String,
}

/// One write of a commit: a Firestore REST `Write`, built by [`FirestoreStore::update`] or
/// [`FirestoreStore::delete`] and narrowed by the methods here.
#[derive(Debug, Clone, PartialEq)]
pub struct Write(Value);

impl FirestoreStore {
    /// Takes any id [`UserId::parse`] takes.
    pub async fn open(options: FirestoreOptions) -> Result<Self> {
        Self::open_with(options, principal::any()).await
    }

    /// Reads back only the ids `principals` takes: a document holding any other is corrupt.
    pub async fn open_with(
        options: FirestoreOptions,
        principals: Arc<dyn Principals>,
    ) -> Result<Self> {
        if options.collection.trim().is_empty() {
            return Err(firestore("the collection is empty"));
        }
        let emulator = options
            .emulator_host
            .clone()
            .or_else(|| std::env::var("FIRESTORE_EMULATOR_HOST").ok())
            .filter(|host| !host.trim().is_empty());
        let (auth, api) = match &emulator {
            Some(host) => (Auth::Emulator, format!("http://{host}/v1")),
            None => {
                let provider: Arc<dyn gcp_auth::TokenProvider> = match &options.credentials_file {
                    Some(path) => Arc::new(
                        gcp_auth::CustomServiceAccount::from_file(path).map_err(auth_error)?,
                    ),
                    None => gcp_auth::provider().await.map_err(auth_error)?,
                };
                (
                    Auth::Google(provider),
                    "https://firestore.googleapis.com/v1".to_owned(),
                )
            }
        };
        let project = match (&options.project_id, &auth) {
            (Some(project), _) => project.clone(),
            (None, Auth::Google(provider)) => {
                provider.project_id().await.map_err(auth_error)?.to_string()
            }
            (None, Auth::Emulator) => EMULATOR_PROJECT.to_owned(),
        };
        let database_id = options
            .database_id
            .clone()
            .unwrap_or_else(|| "(default)".to_owned());
        let collection = &options.collection;
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|err| firestore(err.to_string()))?;
        let database = format!("projects/{project}/databases/{database_id}");
        Ok(Self {
            inner: Arc::new(Inner {
                http,
                root: format!("{database}/documents/{collection}/gateway"),
                api,
                database,
                auth,
                principals,
            }),
        })
    }

    /// An id read from a document, judged as every other backend judges it.
    pub fn user_id(&self, raw: &str) -> Result<UserId> {
        self.inner
            .principals
            .parse(raw)
            .map_err(|err| StoreError::Corrupt(err.to_string()))
    }

    fn user_field(&self, doc: &Document, field: &str) -> Result<UserId> {
        self.user_id(&doc.string(field)?)
    }

    /// The full resource name of a document under the root; `""` is the root itself.
    fn name(&self, path: &str) -> String {
        if path.is_empty() {
            self.inner.root.clone()
        } else {
            format!("{}/{path}", self.inner.root)
        }
    }

    async fn request(
        &self,
        method: reqwest::Method,
        url: &str,
        body: Option<Value>,
    ) -> Result<Option<Value>> {
        let inner = &self.inner;
        let mut request = inner.http.request(method, url);
        request = match &inner.auth {
            Auth::Emulator => request.bearer_auth("owner"),
            Auth::Google(provider) => {
                let token = provider.token(&[SCOPE]).await.map_err(auth_error)?;
                request.bearer_auth(token.as_str())
            }
        };
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request
            .send()
            .await
            .map_err(|err| firestore(err.to_string()))?;
        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let text = response
            .text()
            .await
            .map_err(|err| firestore(err.to_string()))?;
        if !status.is_success() {
            let reason = serde_json::from_str::<Value>(&text)
                .ok()
                .and_then(|body| body["error"]["status"].as_str().map(str::to_owned))
                .unwrap_or_default();
            if matches!(
                reason.as_str(),
                "ALREADY_EXISTS" | "FAILED_PRECONDITION" | "ABORTED"
            ) || status == reqwest::StatusCode::CONFLICT
            {
                return Err(StoreError::Conflict(format!("{status} {reason}")));
            }
            return Err(firestore(format!("{status}: {text}")));
        }
        if text.trim().is_empty() {
            return Ok(Some(Value::Null));
        }
        serde_json::from_str(&text)
            .map(Some)
            .map_err(|err| firestore(format!("undecodable reply: {err}")))
    }

    /// The document at `path` under the root, such as `pins/C0123-1.2`; `""` is the root itself.
    pub async fn get(&self, path: &str) -> Result<Option<Document>> {
        let url = format!("{}/{}", self.inner.api, self.name(path));
        Ok(self
            .request(reqwest::Method::GET, &url, None)
            .await?
            .map(document))
    }

    /// Every document of one subcollection of the root, such as `pins`.
    pub async fn list(&self, collection: &str) -> Result<Vec<Document>> {
        let mut out = Vec::new();
        let mut page: Option<String> = None;
        loop {
            let mut url = format!(
                "{}/{}/{collection}?pageSize={PAGE_SIZE}",
                self.inner.api, self.inner.root
            );
            if let Some(token) = &page {
                url.push_str("&pageToken=");
                url.push_str(token);
            }
            let Some(body) = self.request(reqwest::Method::GET, &url, None).await? else {
                return Ok(out);
            };
            for found in body["documents"].as_array().cloned().unwrap_or_default() {
                out.push(document(found));
            }
            match body["nextPageToken"].as_str() {
                Some(token) if !token.is_empty() => page = Some(token.to_owned()),
                _ => return Ok(out),
            }
        }
    }

    /// The documents of one subcollection of the root whose string `field` equals `value`.
    pub async fn query_equal(
        &self,
        collection: &str,
        field: &str,
        value: &str,
    ) -> Result<Vec<Document>> {
        let url = format!("{}/{}:runQuery", self.inner.api, self.inner.root);
        let query = json!({"structuredQuery": {
            "from": [{"collectionId": collection}],
            "where": {"fieldFilter": {"field": {"fieldPath": field}, "op": "EQUAL", "value": {"stringValue": value}}},
        }});
        let body = self
            .request(reqwest::Method::POST, &url, Some(query))
            .await?
            .unwrap_or(Value::Null);
        Ok(body
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|row| row.get("document").cloned())
            .map(document)
            .collect())
    }

    /// Sets `fields`, Firestore-encoded, on the document at `path`, replacing the others unless
    /// [`Write::mask`] narrows it.
    pub fn update(&self, path: &str, fields: Value) -> Write {
        Write(json!({"update": {"name": self.name(path), "fields": fields}}))
    }

    pub fn delete(&self, path: &str) -> Write {
        Write(json!({"delete": self.name(path)}))
    }

    /// Applies the writes atomically and returns Firestore's reply. A failed precondition, or
    /// any other lost race, is [`StoreError::Conflict`].
    pub async fn commit(&self, writes: Vec<Write>) -> Result<Value> {
        let url = format!(
            "{}/{}/documents:commit",
            self.inner.api, self.inner.database
        );
        let writes: Vec<Value> = writes.into_iter().map(Write::into_json).collect();
        self.request(
            reqwest::Method::POST,
            &url,
            Some(json!({ "writes": writes })),
        )
        .await?
        .ok_or_else(|| firestore("commit answered 404"))
    }

    /// The document and the server's time of the read, which a lease is judged against.
    pub(super) async fn read_with_time(
        &self,
        path: &str,
    ) -> Result<(Option<Document>, OffsetDateTime)> {
        let url = format!(
            "{}/{}/documents:batchGet",
            self.inner.api, self.inner.database
        );
        let body = self
            .request(
                reqwest::Method::POST,
                &url,
                Some(json!({"documents": [self.name(path)]})),
            )
            .await?
            .unwrap_or(Value::Null);
        let row = body
            .as_array()
            .and_then(|rows| rows.first())
            .cloned()
            .unwrap_or(Value::Null);
        let read_time = row["readTime"]
            .as_str()
            .and_then(|raw| OffsetDateTime::parse(raw, &Rfc3339).ok())
            .ok_or_else(|| firestore("batchGet gave no read time"))?;
        Ok((row.get("found").cloned().map(document), read_time))
    }

    /// Writes one field of a person's settings and leaves the others as they are.
    async fn set_user_field(&self, user: &UserId, field: &str, value: Value) -> Result<()> {
        let write = self
            .update(
                &format!("users/{user}"),
                json!({"user_id": string_value(user.as_str()), field: value}),
            )
            .mask(["user_id", field]);
        self.commit(vec![write]).await.map(drop)
    }

    /// An array transform, so two approvals landing together cannot drop each other's rule.
    async fn change_rules(
        &self,
        user: &UserId,
        field: &str,
        tool: &str,
        transform: &str,
    ) -> Result<()> {
        let write = self
            .update(
                &format!("users/{user}"),
                json!({"user_id": string_value(user.as_str())}),
            )
            .mask(["user_id"])
            .transforms(json!([{
                "fieldPath": field,
                transform: {"values": [string_value(tool)]},
            }]));
        self.commit(vec![write]).await.map(drop)
    }

    fn user_of(&self, user: UserId, doc: &Document) -> Result<UserConfig> {
        let tool_mode = match doc
            .fields
            .get("tool_mode")
            .and_then(|v| v["stringValue"].as_str())
        {
            Some(raw) => raw
                .parse()
                .map_err(|err: ToolModeError| StoreError::Corrupt(err.to_string()))?,
            None => ToolMode::default(),
        };
        Ok(UserConfig {
            user,
            allowed: doc.bool("allowed"),
            tool_mode,
            whitelist: rules(doc, "tool_whitelist"),
            denylist: rules(doc, "tool_denylist"),
        })
    }

    fn token_of(&self, doc: &Document) -> Result<NodeToken> {
        Ok(NodeToken {
            selector: doc.string("selector")?,
            secret_hash: doc.string("secret_hash")?,
            owner: self.user_field(doc, "owner")?,
            name: doc.string("name")?,
            created_at: doc.required_time("created_at")?,
            revoked_at: doc.time("revoked_at")?,
            disabled_at: doc.time("disabled_at")?,
        })
    }

    fn user_token_of(&self, doc: &Document) -> Result<UserToken> {
        Ok(UserToken {
            selector: doc.string("selector")?,
            secret_hash: doc.string("secret_hash")?,
            owner: self.user_field(doc, "owner")?,
            name: doc.string("name")?,
            created_at: doc.required_time("created_at")?,
            revoked_at: doc.time("revoked_at")?,
            scopes: scopes_of(doc)?,
        })
    }

    fn grant_of(&self, doc: &Document) -> Result<Grant> {
        Ok(Grant {
            user: self.user_field(doc, "user_id")?,
            approved_by: self.user_field(doc, "approved_by")?,
            approved_at: doc.required_time("approved_at")?,
        })
    }
}

impl Write {
    /// Writes only these fields and leaves the document's others as they are.
    pub fn mask<'a>(mut self, fields: impl IntoIterator<Item = &'a str>) -> Self {
        self.0["updateMask"] = json!({"fieldPaths": fields.into_iter().collect::<Vec<_>>()});
        self
    }

    /// Fails with a conflict if the document exists.
    pub fn if_absent(mut self) -> Self {
        self.0["currentDocument"] = json!({"exists": false});
        self
    }

    /// Fails with a conflict unless the document is as last read, by [`Document::update_time`].
    pub fn if_unchanged(mut self, update_time: &str) -> Self {
        self.0["currentDocument"] = json!({"updateTime": update_time});
        self
    }

    /// Firestore `FieldTransform`s applied after the update, such as
    /// `[{"fieldPath": "at", "setToServerValue": "REQUEST_TIME"}]`.
    pub fn transforms(mut self, transforms: Value) -> Self {
        self.0["updateTransforms"] = transforms;
        self
    }

    pub fn into_json(self) -> Value {
        self.0
    }
}

impl Document {
    /// The last segment of the document's name.
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn fields(&self) -> &Map<String, Value> {
        &self.fields
    }

    pub fn has(&self, field: &str) -> bool {
        self.fields.contains_key(field)
    }

    /// The version a guarded write names in [`Write::if_unchanged`].
    pub fn update_time(&self) -> &str {
        &self.update_time
    }

    /// A string field; absent is corrupt.
    pub fn string(&self, field: &str) -> Result<String> {
        self.fields
            .get(field)
            .and_then(|value| value["stringValue"].as_str())
            .map(str::to_owned)
            .ok_or_else(|| StoreError::Corrupt(format!("field {field} is missing")))
    }

    /// A boolean field; absent is `false`.
    pub fn bool(&self, field: &str) -> bool {
        self.fields
            .get(field)
            .and_then(|value| value["booleanValue"].as_bool())
            .unwrap_or(false)
    }

    /// A timestamp field; absent, or null, is `None`.
    pub fn time(&self, field: &str) -> Result<Option<OffsetDateTime>> {
        match self
            .fields
            .get(field)
            .and_then(|value| value["timestampValue"].as_str())
        {
            None => Ok(None),
            Some(raw) => OffsetDateTime::parse(raw, &Rfc3339)
                .map(Some)
                .map_err(|err| StoreError::Corrupt(format!("{field}: {err}"))),
        }
    }

    /// A timestamp field; absent is corrupt.
    pub fn required_time(&self, field: &str) -> Result<OffsetDateTime> {
        self.time(field)?
            .ok_or_else(|| StoreError::Corrupt(format!("field {field} is missing")))
    }

    /// An integer field, which Firestore sends as a string.
    pub fn integer(&self, field: &str) -> Option<i64> {
        let value = self.fields.get(field)?;
        value["integerValue"]
            .as_str()
            .and_then(|raw| raw.parse().ok())
            .or_else(|| value["integerValue"].as_i64())
    }
}

/// `{"stringValue": value}`.
pub fn string_value(value: &str) -> Value {
    json!({"stringValue": value})
}

/// `{"timestampValue": …}`, in RFC 3339.
pub fn timestamp_value(at: OffsetDateTime) -> Result<Value> {
    Ok(
        json!({"timestampValue": at.format(&Rfc3339).map_err(|err| StoreError::Corrupt(err.to_string()))?}),
    )
}

fn firestore(message: impl std::fmt::Display) -> StoreError {
    StoreError::Backend(format!("firestore: {message}"))
}

fn auth_error(err: gcp_auth::Error) -> StoreError {
    firestore(format!("Google credentials: {err}"))
}

fn document(found: Value) -> Document {
    Document {
        id: found["name"]
            .as_str()
            .and_then(|name| name.rsplit('/').next())
            .unwrap_or_default()
            .to_owned(),
        fields: found["fields"].as_object().cloned().unwrap_or_default(),
        update_time: found["updateTime"].as_str().unwrap_or_default().to_owned(),
    }
}

fn rules(doc: &Document, field: &str) -> BTreeSet<String> {
    doc.fields
        .get(field)
        .and_then(|v| v["arrayValue"]["values"].as_array())
        .map(|values| {
            values
                .iter()
                .filter_map(|v| v["stringValue"].as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

/// Absent on a token minted before scopes existed, which may do what it always could.
fn scopes_of(doc: &Document) -> Result<BTreeSet<Scope>> {
    let Some(values) = doc
        .fields
        .get("scopes")
        .and_then(|v| v["arrayValue"]["values"].as_array())
    else {
        return Ok(Scope::legacy());
    };
    let raw: Vec<&str> = values
        .iter()
        .filter_map(|v| v["stringValue"].as_str())
        .collect();
    Scope::split(&raw.join(",")).map_err(|err| StoreError::Corrupt(err.to_string()))
}

#[async_trait]
impl Store for FirestoreStore {
    async fn admin(&self) -> Result<Option<UserId>> {
        match self.get("").await? {
            Some(doc) if doc.has("admin") => Ok(Some(self.user_field(&doc, "admin")?)),
            _ => Ok(None),
        }
    }

    async fn set_admin(&self, user: &UserId) -> Result<()> {
        let write = self
            .update("", json!({"admin": string_value(user.as_str())}))
            .mask(["admin"]);
        self.commit(vec![write]).await.map(drop)
    }

    async fn grants(&self) -> Result<Vec<Grant>> {
        let mut grants = self
            .list("grants")
            .await?
            .iter()
            .map(|doc| self.grant_of(doc))
            .collect::<Result<Vec<_>>>()?;
        grants.sort_by_key(|grant| grant.approved_at);
        Ok(grants)
    }

    async fn approve(&self, user: &UserId, by: &UserId) -> Result<Grant> {
        let path = format!("grants/{user}");
        let write = self
            .update(
                &path,
                json!({
                    "user_id": string_value(user.as_str()),
                    "approved_by": string_value(by.as_str()),
                    "approved_at": timestamp_value(OffsetDateTime::now_utc())?,
                }),
            )
            .if_absent();
        match self.commit(vec![write]).await {
            Ok(_) | Err(StoreError::Conflict(_)) => {}
            Err(err) => return Err(err),
        }
        let doc = self
            .get(&path)
            .await?
            .ok_or_else(|| StoreError::Corrupt(format!("grant for {user} vanished")))?;
        self.grant_of(&doc)
    }

    async fn revoke(&self, user: &UserId) -> Result<bool> {
        let path = format!("grants/{user}");
        let Some(doc) = self.get(&path).await? else {
            return Ok(false);
        };
        let write = self.delete(&path).if_unchanged(doc.update_time());
        match self.commit(vec![write]).await {
            Ok(_) => Ok(true),
            Err(StoreError::Conflict(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    async fn users(&self) -> Result<Vec<UserConfig>> {
        let mut users = self
            .list("users")
            .await?
            .iter()
            .map(|doc| self.user_of(self.user_field(doc, "user_id")?, doc))
            .collect::<Result<Vec<_>>>()?;
        users.sort_by(|a, b| a.user.cmp(&b.user));
        Ok(users)
    }

    async fn user(&self, user: &UserId) -> Result<UserConfig> {
        match self.get(&format!("users/{user}")).await? {
            Some(doc) => self.user_of(user.clone(), &doc),
            None => Ok(UserConfig::new(user.clone())),
        }
    }

    async fn set_allowed(&self, user: &UserId, allowed: bool) -> Result<()> {
        self.set_user_field(user, "allowed", json!({"booleanValue": allowed}))
            .await
    }

    async fn set_tool_mode(&self, user: &UserId, mode: ToolMode) -> Result<()> {
        self.set_user_field(user, "tool_mode", string_value(mode.as_str()))
            .await
    }

    async fn whitelist_tool(&self, user: &UserId, tool: &str) -> Result<bool> {
        let before = Store::user(self, user).await?.whitelist.contains(tool);
        self.change_rules(user, "tool_whitelist", tool, "appendMissingElements")
            .await?;
        Ok(!before)
    }

    async fn unwhitelist_tool(&self, user: &UserId, tool: &str) -> Result<bool> {
        let before = Store::user(self, user).await?.whitelist.contains(tool);
        self.change_rules(user, "tool_whitelist", tool, "removeAllFromArray")
            .await?;
        Ok(before)
    }

    async fn deny_tool(&self, user: &UserId, tool: &str) -> Result<bool> {
        let before = Store::user(self, user).await?.denylist.contains(tool);
        self.change_rules(user, "tool_denylist", tool, "appendMissingElements")
            .await?;
        Ok(!before)
    }

    async fn undeny_tool(&self, user: &UserId, tool: &str) -> Result<bool> {
        let before = Store::user(self, user).await?.denylist.contains(tool);
        self.change_rules(user, "tool_denylist", tool, "removeAllFromArray")
            .await?;
        Ok(before)
    }

    /// A mode, or a rule list that was ever written, even one emptied since.
    async fn has_tool_rules(&self, user: &UserId) -> Result<bool> {
        Ok(self
            .get(&format!("users/{user}"))
            .await?
            .is_some_and(|doc| {
                ["tool_mode", "tool_whitelist", "tool_denylist"]
                    .iter()
                    .any(|field| doc.has(field))
            }))
    }

    async fn node_tokens(&self) -> Result<Vec<NodeToken>> {
        let mut tokens = self
            .list("node_tokens")
            .await?
            .iter()
            .map(|doc| self.token_of(doc))
            .collect::<Result<Vec<_>>>()?;
        tokens.sort_by_key(|token| token.created_at);
        Ok(tokens)
    }

    async fn add_node_token(&self, token: &NodeToken) -> Result<()> {
        let mut fields = json!({
            "selector": string_value(&token.selector),
            "secret_hash": string_value(&token.secret_hash),
            "owner": string_value(token.owner.as_str()),
            "name": string_value(&token.name),
            "created_at": timestamp_value(token.created_at)?,
        });
        if let Some(revoked_at) = token.revoked_at {
            fields["revoked_at"] = timestamp_value(revoked_at)?;
        }
        if let Some(disabled_at) = token.disabled_at {
            fields["disabled_at"] = timestamp_value(disabled_at)?;
        }
        let write = self
            .update(&format!("node_tokens/{}", token.selector), fields)
            .if_absent();
        self.commit(vec![write]).await.map(drop)
    }

    async fn revoke_node_token(&self, selector: &str) -> Result<bool> {
        let path = format!("node_tokens/{selector}");
        let Some(doc) = self.get(&path).await? else {
            return Ok(false);
        };
        if doc.has("revoked_at") {
            return Ok(false);
        }
        let write = self
            .update(
                &path,
                json!({"revoked_at": timestamp_value(OffsetDateTime::now_utc())?}),
            )
            .mask(["revoked_at"])
            .if_unchanged(doc.update_time());
        match self.commit(vec![write]).await {
            Ok(_) => Ok(true),
            Err(StoreError::Conflict(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    async fn set_node_disabled(&self, selector: &str, disabled: bool) -> Result<()> {
        let value = match disabled {
            true => timestamp_value(OffsetDateTime::now_utc())?,
            false => json!({"nullValue": null}),
        };
        let write = self
            .update(
                &format!("node_tokens/{selector}"),
                json!({"disabled_at": value}),
            )
            .mask(["disabled_at"]);
        self.commit(vec![write]).await.map(drop)
    }

    async fn user_tokens(&self) -> Result<Vec<UserToken>> {
        let mut tokens = self
            .list("user_tokens")
            .await?
            .iter()
            .map(|doc| self.user_token_of(doc))
            .collect::<Result<Vec<_>>>()?;
        tokens.sort_by_key(|token| token.created_at);
        Ok(tokens)
    }

    async fn add_user_token(&self, token: &UserToken) -> Result<()> {
        let mut fields = json!({
            "selector": string_value(&token.selector),
            "secret_hash": string_value(&token.secret_hash),
            "owner": string_value(token.owner.as_str()),
            "name": string_value(&token.name),
            "created_at": timestamp_value(token.created_at)?,
            "scopes": {"arrayValue": {"values": token
                .scopes
                .iter()
                .map(|scope| string_value(scope.as_str()))
                .collect::<Vec<_>>()}},
        });
        if let Some(revoked_at) = token.revoked_at {
            fields["revoked_at"] = timestamp_value(revoked_at)?;
        }
        let write = self
            .update(&format!("user_tokens/{}", token.selector), fields)
            .if_absent();
        self.commit(vec![write]).await.map(drop)
    }

    async fn revoke_user_token(&self, selector: &str) -> Result<bool> {
        let path = format!("user_tokens/{selector}");
        let Some(doc) = self.get(&path).await? else {
            return Ok(false);
        };
        if doc.has("revoked_at") {
            return Ok(false);
        }
        let write = self
            .update(
                &path,
                json!({"revoked_at": timestamp_value(OffsetDateTime::now_utc())?}),
            )
            .mask(["revoked_at"])
            .if_unchanged(doc.update_time());
        match self.commit(vec![write]).await {
            Ok(_) => Ok(true),
            Err(StoreError::Conflict(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }
}
