# RAX in Rust

The Rust implementation of [RAX](https://github.com/miere/rax-protocol), the protocol a remote
agent node speaks to a chat gateway. The spec lives in that repository; this one is the code.

| Crate | What it is |
|---|---|
| `rax` | The protocol itself: messages, frames and the delivery state machine, with no I/O, so any runtime can drive it. |
| `rax-tokio` | A Tokio runtime for both ends. A node dials, redials and resumes; a gateway authenticates nodes and keeps each link across reconnects. Neither side handles a frame or an acknowledgement itself. |
| `rax-sim` | A scriptable gateway for testing a node. It records the wire, cuts connections, restarts, revokes tokens and reports timings. |

## Building

```
cargo test --workspace
```
