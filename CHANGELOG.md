# Changelog

Every crate in this workspace shares one version, and one tag covers them all. While on 0.x, a
minor bump is a breaking change. Depend on a tag:

```toml
rax = { git = "https://github.com/miere/rax-rs", tag = "0.15.1" }
```

## 0.15.1

- The fleet never assigns a node whose credential is no longer live, even while its link is
  still open, and such a node no longer shadows other people's.

## 0.15.0

- A gateway tool may carry hints for the node's agent in `meta`, which a node passes on as
  the tool's MCP `_meta`.

## 0.14.0

- A plan may list its steps, so a gateway can track them as progress once it is approved.

## 0.13.0

- New crate, `rax-gateway`: Murtaugh's gateway core, with what was Murtaugh's own behind traits.
  It serves nodes and the clients that run sessions on them, and keeps who may do what in a store.
- A `rax-gateway` binary, behind the `bin` feature, that serves RAX alone from a SQLite database.

## 0.12.0

- A link closed while its router is busy flushes what was queued, so a node the gateway rejects
  hears why before the close.
- A node whose credential may serve nobody but its owner declares `owner_only` at `initialize`.

## 0.11.0

- A gateway tool may answer with resources for the node to fetch.

## 0.10.0

- A node reports its activity.

## 0.9.0

- A gateway reads a node's files by identifier.
