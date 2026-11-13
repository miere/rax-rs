# Changelog

Every crate in this workspace shares one version, and one tag covers them all. While on 0.x, a
minor bump is a breaking change. Depend on a tag:

```toml
rax = { git = "https://github.com/miere/rax-rs", tag = "0.16.0" }
```

## 0.16.0

- A tool rule may carry a pattern, as in `Bash(git (status|diff))`, matched whole against the
  call's command, path, URL or input. A pattern never allows a chained shell command unless it
  spells the command out.
- Owners keep a denylist beside the whitelist, and a matching deny rule wins.
- `allowed-whitelist` is the default tool mode. `policy::seed` starts an owner with no rules on a
  read-only whitelist, and `Store::has_tool_rules` says whether they have rules.
- Breaking: `policy::rule` takes the `ToolCall`, and `Store` gains `deny_tool`, `undeny_tool`
  and `has_tool_rules`.

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
