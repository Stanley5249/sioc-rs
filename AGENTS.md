# Agent guidelines

## Build and test

Always use `--workspace --all-targets` for cargo checks and tests. The only exception is the MSRV check, which omits `--all-targets` so dev-dependencies do not gate the supported compiler version.

```sh
just ci
just lint
just lint-py
just lint-js
just test
just test-servers
```

Python and TypeScript test servers share the root `pyproject.toml` and `package.json`; `just test-servers` runs protocol and pressure tests against them.

## Running examples

```sh
RUST_LOG=info cargo run --example glhf
RUST_LOG=glhf=trace,sioc=trace cargo run --example glhf
```

Use `--example <name>` at workspace root; omit `-p`. `sioc=trace` shows all wire packets; `eioc=trace` is library internals only.

## Code style

Re-export public surface through `prelude`. Never put `Result` or `Error` aliases in `prelude` because they shadow `std` and cause ambiguity; import by explicit path (`use eioc::error::{Error, Result}`).

Import types and traits by name. Call a function from another module of the same crate by its full path, such as `crate::manager::session::run(..)`, so a reader sees at first glance that the function is ours. Write that path instead of importing the module with `use crate::websocket;` or `use super::{self, Item};`.

Give a module with child files a `<name>/mod.rs` instead of a `<name>.rs` beside a `<name>/` folder. Keep `mod.rs` to the module docs and `mod` declarations, and put definitions in named child files.

Variable names should be descriptive and avoid unnecessary abbreviations. Prefer `packet` and `event` over `pkt` and `evt`. For short-lived variables, single-letter names like `p` and `e` are acceptable.

Name a channel's ends after the items it carries, such as `frame_tx` and `client_packet_rx`. When two channels carry the same item type, prefix the side that produced the items: `server_` for what the server sent and `client_` for what this client sends. Avoid names that depend on the reader's position, such as `inbound` and `outbound`.

## Async

- Run each direction of a bidirectional pipe as its own loop and `join!` them. A loop that awaits a send to one output stops serving every other input.
- Use `select!` in a loop only when every handler awaits nothing but the loop's one output, or when the arm is a stop signal. Every branch future must also be cancel-safe: tokio channel `recv`, `recv_many`, `FuturesUnordered::next`, and `CancellationToken::cancelled` are. Prove cancel safety before selecting on anything else.
- Shut down by half-close: a task stops by dropping its senders and finishes only when its receivers return `None`. Between our tasks a send never fails during a graceful shutdown, so every send error is a real error. A receiver the caller owns may drop at any time, so delivery to it discards on a closed channel.
- Keep every channel fed by the server bounded, so a flood slows the connection instead of growing memory.
- Lock a `std::sync::Mutex` only inside a synchronous method that never returns the guard, so no lock is held across an `.await`.
- Keep reconnection application-controlled. The application chooses its retry delay and policy, reopens a namespace with `Client::connect` on a live session, and opens a new `Client` after the session ends. Use `tokio::time::sleep` for retry delays instead of adding a backoff dependency.

## Commits

Use Conventional Commits; `cliff.toml` groups release notes by type, so pick the type a reader of the notes expects.

- Scope with the folder or file name the change lives in. Use `deps` for dependency updates and `agents` for agent instructions.
- Mark a change to the public API breaking with `!` and a `BREAKING CHANGE:` footer. A dependency bump counts when the dependency's types appear in the public API.
