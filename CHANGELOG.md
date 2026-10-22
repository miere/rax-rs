# Changelog

Every crate in this workspace shares one version, and one tag covers them all. While on 0.x, a
minor bump is a breaking change. Depend on a tag:

```toml
rax = { git = "https://github.com/miere/rax-rs", tag = "0.12.0" }
```

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
