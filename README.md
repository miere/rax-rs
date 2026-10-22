# RAX in Rust

The Rust implementation of [RAX](https://github.com/miere/rax-protocol), the protocol a remote
agent node speaks to a chat gateway. The spec lives in that repository; this one is the code.

| Crate | What it is |
|---|---|
| `rax` | The protocol itself: messages, frames and the delivery state machine, with no I/O, so any runtime can drive it. |
| `rax-tokio` | A Tokio runtime for both ends. The dialler connects, redials and resumes; the acceptor authenticates and keeps each link across reconnects. Either role rides on either end: usually a node dials a gateway, but a gateway may dial an acceptor that plays the node. Neither side handles a frame or an acknowledgement itself. |
| `rax-gateway` | An opinionated gateway: nodes dial in with a node token, people's own clients with a client token, and each client's sessions run on the nodes that let its owner in. Whatever is an embedder's own — who a person is, where the data lives, how a node's owner is asked something — sits behind a trait, so a chat gateway like Murtaugh builds on it. Also a `rax-gateway` binary that serves RAX alone. |
| `rax-sim` | A scriptable gateway for testing a node. It records the wire, cuts connections, restarts, revokes tokens and reports timings. In "gateway dials" mode it dials an acceptor that plays the node. |

## Building

```
cargo test --workspace --all-features
```

## Running a gateway on its own

```
cargo install --git https://github.com/miere/rax-rs --tag 0.13.0 rax-gateway --features bin

rax-gateway admin set alice
rax-gateway node mint --owner alice --name laptop     # give this to the node
rax-gateway client mint --owner alice --name editor   # give this to the editor
rax-gateway serve
```

It reads `rax-gateway.toml` from the working directory when there is one, or the file `--config`
names:

```toml
listen = "127.0.0.1:7443"      # where nodes and clients dial in
database = "rax-gateway.db"    # SQLite, relative to this file
retain_for_secs = 300          # how long a dropped link is held for a resume
```

People are plain ids, such as `alice`. A node serves only its owner unless its metadata says
otherwise, under `gateway_access`, exactly as Murtaugh's nodes do under `murtaugh_access`. Nobody
can be asked anything here: a tool call only a node's owner could approve is denied, unless the
owner's tool rules (`rax-gateway tools …`) allow it, and a node cannot be shown a sign-in.

## Embedding the gateway

`rax_gateway::hub::start` serves nodes and clients on one port beside your own HTTP routes. An
[`Embedding`](crates/rax-gateway/src/embed.rs) carries what is yours:

- `Settings`: the token prefixes, the metadata key nodes write their allow list under, and which
  ids are people (`Principals`).
- `OwnerChannel`: how a node's owner is asked to approve a client's call or to sign in.
- `FileSource`: the resources you link in prompts, which nodes read back.
- `Tools`: the tools you lend the sessions you open yourself.

Implement `store::Store` and `store::Leader` to keep the data elsewhere; the SQLite ones sit behind
the default `sqlite` feature, and `SqliteStore::extend` lets you keep tables of your own beside
the gateway's.
