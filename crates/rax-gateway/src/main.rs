//! A gateway that only speaks RAX: nodes dial in with a node token, clients with a client token,
//! and every client's sessions run on the nodes that let its owner in. Nobody is asked anything,
//! so a call only a node's owner may approve is denied unless their tool rules allow it.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use rax_gateway::access::{Access, Snapshot};
use rax_gateway::fleet::Fleet;
use rax_gateway::lent::Lent;
use rax_gateway::relay::Relay;
use rax_gateway::store::{Leader, Scope, SqliteLeader, SqliteStore, Store, ToolMode, UserId};
use rax_gateway::transfers::Transfers;
use rax_gateway::{Embedding, Settings, hub, roles};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

const DEFAULT_CONFIG: &str = "rax-gateway.toml";
const DEFAULT_LISTEN: &str = "127.0.0.1:7443";
const DEFAULT_DATABASE: &str = "rax-gateway.db";

#[derive(Debug, Parser)]
#[command(
    name = "rax-gateway",
    about = "Serves RAX nodes, and the clients that run sessions on them",
    arg_required_else_help = true
)]
struct Cli {
    /// Configuration file [default: rax-gateway.toml, if there is one]
    #[arg(long, global = true, value_name = "PATH")]
    config: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Serve nodes and clients until stopped
    Serve,
    /// Who administers this gateway
    #[command(subcommand)]
    Admin(AdminCommand),
    /// People allowed to run their own nodes here
    #[command(subcommand)]
    Grant(GrantCommand),
    /// People allowed to use the gateway, on any node whose owner lets them in
    #[command(subcommand)]
    User(UserCommand),
    /// Node credentials
    #[command(subcommand)]
    Node(NodeCommand),
    /// Client credentials, for a person's own editor or agent
    #[command(subcommand)]
    Client(ClientCommand),
    /// How the tools agents use on a person's nodes are ruled on
    #[command(subcommand)]
    Tools(ToolsCommand),
}

#[derive(Debug, Subcommand)]
enum AdminCommand {
    /// Make a person the admin; the outgoing admin keeps their nodes
    Set {
        user: String,
    },
    Show,
}

#[derive(Debug, Subcommand)]
enum GrantCommand {
    /// Let a person run their own nodes, and use the gateway
    Approve {
        user: String,
    },
    /// Take it back; every node token they hold is revoked for good
    Revoke {
        user: String,
    },
    List,
}

#[derive(Debug, Subcommand)]
enum UserCommand {
    Allow {
        user: String,
    },
    /// Take it back, with their grant and every token they hold
    Disallow {
        user: String,
    },
    List,
}

#[derive(Debug, Subcommand)]
enum NodeCommand {
    /// Mint a credential for one of a person's nodes, granting them first if need be
    Mint {
        #[arg(long)]
        owner: String,
        #[arg(long)]
        name: String,
    },
    /// Revoke a credential; its node is disconnected within seconds
    Revoke { selector: String },
    /// Every credential and whether it is revoked
    List,
}

#[derive(Debug, Subcommand)]
enum ClientCommand {
    /// Mint a credential that opens the RAX API, allowing its owner if need be
    Mint {
        #[arg(long)]
        owner: String,
        #[arg(long)]
        name: String,
    },
    /// Revoke a credential; its client is disconnected within seconds
    Revoke {
        selector: String,
    },
    List,
}

#[derive(Debug, Subcommand)]
enum ToolsCommand {
    /// always-allowed (the default), allowed-whitelist or denied. Nobody can be asked here, so
    /// under allowed-whitelist a tool off the whitelist is denied to everyone but the owner's
    /// own clients
    Mode {
        user: String,
        mode: String,
    },
    /// Put a tool on the owner's whitelist
    Allow {
        user: String,
        tool: String,
    },
    /// Take a tool off the owner's whitelist
    Disallow {
        user: String,
        tool: String,
    },
    Show {
        user: String,
    },
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    /// Where nodes and clients dial in.
    listen: Option<String>,
    /// The SQLite database, relative to the configuration file.
    database: Option<PathBuf>,
    /// How long a dropped link is held for its node or client to resume.
    retain_for_secs: Option<u64>,
}

struct Loaded {
    listen: SocketAddr,
    database: PathBuf,
    retain_for: Duration,
}

fn load(path: Option<&Path>) -> Result<Loaded, String> {
    let (path, required) = match path {
        Some(path) => (path.to_path_buf(), true),
        None => (PathBuf::from(DEFAULT_CONFIG), false),
    };
    let config: Config = match std::fs::read_to_string(&path) {
        Ok(raw) => toml::from_str(&raw).map_err(|err| format!("{}: {err}", path.display()))?,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound && !required => Config::default(),
        Err(err) => return Err(format!("{}: {err}", path.display())),
    };
    let listen_raw = config.listen.unwrap_or_else(|| DEFAULT_LISTEN.to_owned());
    let listen = listen_raw.trim().parse().map_err(|err| {
        format!("listen: {listen_raw:?} is not an address like {DEFAULT_LISTEN}: {err}")
    })?;
    let base = path.parent().unwrap_or_else(|| Path::new("."));
    let database = base.join(config.database.unwrap_or_else(|| DEFAULT_DATABASE.into()));
    Ok(Loaded {
        listen,
        database,
        retain_for: config
            .retain_for_secs
            .map(Duration::from_secs)
            .unwrap_or(hub::RETAIN_FOR),
    })
}

fn person(raw: &str) -> Result<UserId, String> {
    UserId::parse(raw).map_err(|err| err.to_string())
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("rax-gateway: {err}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<(), String> {
    let config = load(cli.config.as_deref())?;
    let store = SqliteStore::open(&config.database).map_err(|err| err.to_string())?;
    match cli.command {
        Command::Serve => serve(config, store).await,
        command => {
            let out = admin(&store, command)
                .await
                .map_err(|err| err.to_string())?;
            println!("{out}");
            Ok(())
        }
    }
}

async fn admin(store: &dyn Store, command: Command) -> Result<String, String> {
    let settings = Settings::default();
    let failed = |err: rax_gateway::store::StoreError| err.to_string();
    Ok(match command {
        Command::Serve => return Err("serve runs the gateway; it administers nothing".into()),
        Command::Admin(AdminCommand::Set { user }) => {
            let user = person(&user)?;
            roles::set_admin(store, &user).await.map_err(failed)?;
            format!("{user} is the admin")
        }
        Command::Admin(AdminCommand::Show) => match store.admin().await.map_err(failed)? {
            Some(admin) => admin.to_string(),
            None => "no admin yet; run `rax-gateway admin set <user>`".to_owned(),
        },
        Command::Grant(GrantCommand::Approve { user }) => {
            let user = person(&user)?;
            let admin = store
                .admin()
                .await
                .map_err(failed)?
                .ok_or("set an admin first: they approve every grant")?;
            roles::grant(store, &user, &admin).await.map_err(failed)?;
            format!("{user} may run nodes here")
        }
        Command::Grant(GrantCommand::Revoke { user }) => {
            let user = person(&user)?;
            let (had, revoked) = roles::revoke_grant(store, &user).await.map_err(failed)?;
            match had {
                true => format!(
                    "{user} may no longer run nodes; revoked {} tokens",
                    revoked.len()
                ),
                false => format!("{user} held no grant"),
            }
        }
        Command::Grant(GrantCommand::List) => lines(
            store
                .grants()
                .await
                .map_err(failed)?
                .into_iter()
                .map(|grant| format!("{}\tapproved by {}", grant.user, grant.approved_by)),
        ),
        Command::User(UserCommand::Allow { user }) => {
            let user = person(&user)?;
            roles::allow(store, &user).await.map_err(failed)?;
            format!("{user} may use the gateway")
        }
        Command::User(UserCommand::Disallow { user }) => {
            let user = person(&user)?;
            let revoked = roles::disallow(store, &user).await.map_err(failed)?;
            format!(
                "{user} may no longer use the gateway; revoked {} tokens",
                revoked.len()
            )
        }
        Command::User(UserCommand::List) => lines(
            store
                .users()
                .await
                .map_err(failed)?
                .into_iter()
                .filter(|user| user.allowed)
                .map(|user| user.user.to_string()),
        ),
        Command::Node(NodeCommand::Mint { owner, name }) => {
            let owner = person(&owner)?;
            let admin = store
                .admin()
                .await
                .map_err(failed)?
                .ok_or("set an admin first: they approve every grant")?;
            let minted = roles::mint(store, &settings, &owner, &name, &admin)
                .await
                .map_err(failed)?;
            minted.token
        }
        Command::Node(NodeCommand::Revoke { selector }) => {
            match store.revoke_node_token(&selector).await.map_err(failed)? {
                true => format!("revoked {selector}"),
                false => format!("no live node token {selector}"),
            }
        }
        Command::Node(NodeCommand::List) => lines(
            store
                .node_tokens()
                .await
                .map_err(failed)?
                .into_iter()
                .map(|token| {
                    let state = match (token.revoked_at, token.disabled_at) {
                        (Some(_), _) => "revoked",
                        (None, Some(_)) => "disabled",
                        (None, None) => "live",
                    };
                    format!(
                        "{}\t{}\t{}\t{state}",
                        token.selector, token.owner, token.name
                    )
                }),
        ),
        Command::Client(ClientCommand::Mint { owner, name }) => {
            let owner = person(&owner)?;
            let minted = roles::mint_client(store, &settings, &owner, &name, [Scope::Rax].into())
                .await
                .map_err(failed)?;
            minted.token
        }
        Command::Client(ClientCommand::Revoke { selector }) => {
            match store.revoke_user_token(&selector).await.map_err(failed)? {
                true => format!("revoked {selector}"),
                false => format!("no live client token {selector}"),
            }
        }
        Command::Client(ClientCommand::List) => lines(
            store
                .user_tokens()
                .await
                .map_err(failed)?
                .into_iter()
                .map(|token| {
                    let state = if token.revoked_at.is_some() {
                        "revoked"
                    } else {
                        "live"
                    };
                    format!(
                        "{}\t{}\t{}\t{state}",
                        token.selector, token.owner, token.name
                    )
                }),
        ),
        Command::Tools(ToolsCommand::Mode { user, mode }) => {
            let user = person(&user)?;
            let mode: ToolMode = mode
                .parse()
                .map_err(|err: rax_gateway::store::ToolModeError| err.to_string())?;
            store.set_tool_mode(&user, mode).await.map_err(failed)?;
            format!("{user}'s tools are {mode}")
        }
        Command::Tools(ToolsCommand::Allow { user, tool }) => {
            let user = person(&user)?;
            store.whitelist_tool(&user, &tool).await.map_err(failed)?;
            format!("{tool} is on {user}'s whitelist")
        }
        Command::Tools(ToolsCommand::Disallow { user, tool }) => {
            let user = person(&user)?;
            store.unwhitelist_tool(&user, &tool).await.map_err(failed)?;
            format!("{tool} is off {user}'s whitelist")
        }
        Command::Tools(ToolsCommand::Show { user }) => {
            let config = store.user(&person(&user)?).await.map_err(failed)?;
            let mut out = format!("mode\t{}", config.tool_mode);
            for tool in config.whitelist {
                out.push_str(&format!("\nallowed\t{tool}"));
            }
            out
        }
    })
}

fn lines(rows: impl Iterator<Item = String>) -> String {
    let rows: Vec<String> = rows.collect();
    match rows.is_empty() {
        true => "none".to_owned(),
        false => rows.join("\n"),
    }
}

async fn serve(config: Loaded, store: SqliteStore) -> Result<(), String> {
    let leader = SqliteLeader::new(config.database.clone());
    let holder = format!("rax-gateway-{}", std::process::id());
    let lease = leader
        .acquire(&holder)
        .await
        .map_err(|err| err.to_string())?
        .ok_or_else(|| {
            format!(
                "another gateway already serves {}",
                config.database.display()
            )
        })?;
    let store: Arc<dyn Store> = Arc::new(store);
    let reconciled = roles::reconcile(&*store)
        .await
        .map_err(|err| err.to_string())?;
    if !reconciled.allowed.is_empty() || !reconciled.revoked.is_empty() {
        tracing::info!(allowed = ?reconciled.allowed, revoked = ?reconciled.revoked, "brought access into line");
    }
    let embedding = Embedding::default();
    let snapshot = Snapshot::load(&*store, &embedding.settings)
        .await
        .map_err(|err| err.to_string())?;
    if snapshot.admin().is_none() {
        tracing::warn!("this gateway has no admin yet; run `rax-gateway admin set <user>`");
    }
    let access = Access::reloading(snapshot, store.clone(), embedding.settings.clone());
    let fleet = Fleet::default();
    let lent = Lent::new();
    let transfers = Transfers::default();
    let relay = Relay::new(
        access.clone(),
        fleet.clone(),
        store.clone(),
        embedding.settings.clone(),
        lent.clone(),
        transfers.clone(),
        embedding.owner.clone(),
    );
    let shutdown = CancellationToken::new();
    let http = axum::Router::new().route("/healthz", axum::routing::get(|| async { "ok" }));
    let mut hub = hub::start(
        config.listen,
        config.retain_for,
        access.clone(),
        fleet.clone(),
        transfers,
        lent,
        relay.clone(),
        embedding.clone(),
        http,
        shutdown.clone(),
    )
    .await
    .map_err(|err| format!("cannot listen on {}: {err}", config.listen))?;
    tokio::spawn(hub::refresh(
        store,
        embedding.settings,
        access,
        fleet,
        relay,
        hub::REFRESH,
        shutdown.clone(),
    ));
    tracing::info!(listen = %hub.server.local_addr(), database = %config.database.display(), "rax gateway serving");
    loop {
        tokio::select! {
            change = hub.changes.recv() => match change {
                Some(change) => tracing::debug!(?change, "fleet changed"),
                None => break,
            },
            // Only the relay opens sessions here, and it handles its own.
            background = hub.backgrounds.recv() => if background.is_none() { break },
            _ = tokio::signal::ctrl_c() => break,
        }
    }
    shutdown.cancel();
    leader.release(lease).await.map_err(|err| err.to_string())?;
    tracing::info!("rax gateway stopped");
    Ok(())
}
