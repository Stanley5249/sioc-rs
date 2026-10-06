# sioc-rs

## Workflow

- Read `README.md`.
- Run `just --list` or read `justfile` for commands.
- Include `--workspace --all-targets` in `cargo` commands to unify features across the workspace.
- Set `RUST_LOG=sioc=trace` to log every wire packet. Set `RUST_LOG=eioc=trace` to log library internals.

## Async design

`mpsc` below refers to `tokio::sync::mpsc`.

- Run each direction of a bidirectional pipe in a separate loop. Run both loops together with `tokio::join!`. A loop waiting for one output stops serving its other inputs.
- Use `tokio::select!` in a loop only if each arm handles a stop signal or its handler awaits only the loop's single output. Prove that every branch future is safe to cancel.
- Shut down through half-close. A task drops its channel senders with `drop` to initiate shutdown, then waits until `mpsc::Receiver::recv` or `mpsc::UnboundedReceiver::recv` returns `None` for every input channel before finishing. Under this protocol, sends between library tasks succeed; treat any send failure as an error. If delivery fails because the caller dropped a receiver, discard the item.
- Use `mpsc::channel` to give every channel fed by the server a bounded capacity. Use `mpsc::unbounded_channel` only if library state limits the number of queued items. Document that limit in a comment on the channel.

## Code style

- Re-export the public API through `prelude`. Keep `Result` and `Error` aliases outside `prelude` so they do not shadow the corresponding `std` types.
- Import types and traits by name, or glob-import a `prelude`. To call a function in another module of the same crate, use its full `crate::` path instead of importing that module.
- For a module with child files, use `<name>/mod.rs` as the module file. Keep that file limited to documentation and `mod` declarations.
- Name each channel's sender and receiver after the items the channel carries. When two channels carry the same item type, prefix both ends of each channel with `server_` or `client_` to identify the side that produces the items.

## Tests

- **Unit tests:** Put them at the bottom of the source file.
- **Component tests:** Put them in the submodule's `tests.rs`.
- **End-to-end and derive macro tests:** Put them in `sioc/tests/`.

## Keep in sync

- When updating the public API, check the `README.md` examples and the doc tests.
- When updating a `justfile` recipe, check every reference to that recipe.

## Commits

Use Conventional Commits. `cliff.toml` groups release notes by commit type.

- Use the folder or file containing the change as the commit scope. Use `deps` for dependency changes and `agents` for agent instruction changes.
- Mark a public API break with both `!` and a `BREAKING CHANGE:` footer. Apply this rule when bumping a dependency whose types appear in the public API as well.
