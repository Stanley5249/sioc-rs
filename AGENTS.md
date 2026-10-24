# sioc-rs

## Workflow

- Read `README.md`.
- Before changing an area, read its section in `docs/decisions.md` and the docs of the modules you touch. When a fix or a rejected approach teaches a lesson, add it to `docs/decisions.md` in the same commit. When the structure changes, update the module docs in the same commit.
- Run `just --list` or read `justfile` for commands.
- Include `--workspace --all-targets` in `cargo` commands to unify features across the workspace.
- Set `RUST_LOG=sioc=trace` to log every wire packet. Set `RUST_LOG=eioc=trace` to log library internals.
- Write each third-party Cargo requirement up to its leftmost nonzero component, such as `"1"` or `"0.7"`, and each Bun requirement as `bun add` writes it, such as `"^1.4.2"`. The lockfiles pin exact versions.

## Async design

`mpsc` below refers to `tokio::sync::mpsc`.

- Keep each piece of state in one task. A loop that serves several inputs never awaits inside a handler. It waits for each output, such as a channel permit or a timer, as its own `select!` branch beside the inputs, so a full queue holds up only the data that needs it. Prove that every branch future is safe to cancel.
- Run independent I/O directions, such as a transport's read and write halves, in separate loops joined with `tokio::join!` when no state ties them together.
- Shut down through half-close. A task drops its channel senders with `drop` to initiate shutdown, then waits until `mpsc::Receiver::recv` or `mpsc::UnboundedReceiver::recv` returns `None` for every input channel before finishing. Under this protocol, sends between library tasks succeed; treat any send failure as an error. If delivery fails because the caller dropped a receiver, discard the item.
- Use `mpsc::channel` to give every channel fed by the server a bounded capacity. Use `mpsc::unbounded_channel` only if library state limits the number of queued items. Document that limit in a comment on the channel.

## JS parity

`sioc` follows socket.io-client and `eioc` follows engine.io-client, at the version the module docs link to.

- Give every type, field, variant, and public function a JS counterpart, and name that counterpart in its doc comment. A private helper needs only a clear name.
- Add an item without a JS counterpart only when Rust needs what JS gets for free, and name the reason in its doc comment: a channel between tasks (JS calls a method), a bound (JS queues are unbounded), cleanup (JS has GC and a synchronous `close`), or a wait (JS writes at once). An item that fits none of these is a design to remove.
- Keep each entity's state in one owner. A second map with the same key, or a field that repeats another field, belongs in that owner instead.
- Name a symbol in three steps: take the JS name, adapt it to Rust conventions, then compare it with similar names in std, the dependencies, the JS libraries, and this codebase, and pick one that stays distinct.
- Name a module after the concept it holds, as the JS files do, rather than after one of its types.
- Propose each new symbol with its JS counterpart before writing the code.

## Code style

- Re-export the public API through `prelude`. Keep `Result` and `Error` aliases outside `prelude` so they do not shadow the corresponding `std` types.
- Import types and traits by name, or glob-import a `prelude`. To call a function in another module of the same crate, use its full `crate::` path instead of importing that module.
- For a module with child files, use `<name>/mod.rs` as the module file. Keep that file limited to documentation and `mod` declarations.
- Name items after their layer: frame for the transport, message for Engine.IO, and packet for Socket.IO. Engine sessions open and close, namespaces connect and disconnect, and the manager shuts down.
- Name each channel's sender and receiver after the items the channel carries. When two channels carry the same item type, prefix both ends of each channel with `server_` or `client_` to identify the side that produces the items.

## Tests

- **Unit tests:** Put them at the bottom of the source file.
- **Component tests:** Put them in the submodule's `tests.rs`.
- **End-to-end and derive macro tests:** Put them in `sioc/tests/`.

## Keep in sync

- When updating the public API, check the `README.md` examples and the doc tests.
- Keep `justfile` and `.github/workflows/ci.yml` checks functionally equivalent: synchronize tool versions, workspace and target selection, lockfile enforcement, warning policies, test filters, and coverage options when changing either file. Local recipes use quiet output; CI may invoke tools directly with progress output or CI-specific diagnostic formats.
- When updating a `justfile` recipe, check every reference to that recipe.
- Keep the rustfmt nightly toolchain version synchronized across `justfile`, `.github/workflows/ci.yml`, and `.zed/settings.json`.

## Commits

Use Conventional Commits. `cliff.toml` groups release notes by commit type.

- Use the folder or file containing the change as the commit scope. Use `deps` for dependency changes and `agents` for agent instruction changes.
- Mark a public API break with both `!` and a `BREAKING CHANGE:` footer. Apply this rule when bumping a dependency whose types appear in the public API as well.
