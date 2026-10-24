# Conventions

## Design

`mpsc` below refers to `tokio::sync::mpsc`.

- The core of a protocol layer owns every protocol decision. Its task owns the I/O and turns what the I/O yields into the core's inputs.
- A loop that serves several inputs never awaits inside a handler. The loop waits for each output, such as a channel permit or a timer, as its own `select!` branch beside the inputs, so a full queue holds up only the data that needs it. Prove that every branch future is safe to cancel.
- Run independent I/O directions, such as a transport's read and write halves, in separate loops joined with `tokio::join!` when no state ties them together.
- Shut down through half-close. A task drops its channel senders with `drop` to initiate shutdown, then waits until `mpsc::Receiver::recv` or `mpsc::UnboundedReceiver::recv` returns `None` for every input channel before finishing. Under this protocol, sends between library tasks succeed, so treat any send failure as an error. If delivery fails because the caller dropped a receiver, discard the item.
- Use `mpsc::channel` to give every channel fed by the server a bounded capacity. Use `mpsc::unbounded_channel` only if library state limits the number of queued items. Document that limit in a comment on the channel.

## JS parity and naming

- A private helper needs only a clear name instead of a JS counterpart.
- An item without a JS counterpart names one of four reasons, each something JS gets for free. These are a channel between tasks (JS calls a method), a bound (JS queues are unbounded), cleanup (JS has GC and a synchronous `close`), and a wait (JS writes at once). An item that fits none of these is a design to remove.
- Name a symbol in three steps. Take the JS name, adapt it to Rust conventions, then compare it with similar names in std, the dependencies, the JS libraries, and this codebase, and pick one that stays distinct.
- Name a module after the concept it holds, as the JS files do, rather than after one of its types.
- Use frame for transport data, packet for a wire unit of either protocol, and message for what Engine.IO carries between the two layers. Engine sessions open and close, and namespaces connect and disconnect.
- Name each channel's sender and receiver after the items the channel carries. When two channels carry the same item type, prefix both ends of each channel with `server_` or `client_` to identify the side that produces the items.

## Code style

- Re-export the public API through `prelude`. Keep `Result` and `Error` aliases outside `prelude` so they do not shadow the corresponding `std` types.
- Import types and traits by name, or glob-import a `prelude`. To call a function in another module of the same crate, use its full `crate::` path instead of importing that module.
- For a module with child files, use `<name>/mod.rs` as the module file. Keep that file limited to documentation and `mod` declarations.
- Write each third-party Cargo requirement up to its leftmost nonzero component, such as `"1"` or `"0.7"`, and each Bun requirement as `bun add` writes it, such as `"^1.4.2"`. The lockfiles pin exact versions.

## Tests

- **Unit tests:** Put them at the bottom of the source file.
- **Component tests:** Put them in the submodule's `tests.rs`, or in a `tests/` directory split by scenario group.
- **End-to-end and derive macro tests:** Put them in `sioc/tests/`.

Test each behavior at the layer that decides it:

- **Core tests:** Every protocol rule, such as ordering, acks, reconnection, namespace lifecycle, and binary packets, is a plain `#[test]` on the core. It sends inputs and asserts outputs and queries, using builders only.
- **Task tests:** Cover only what the core cannot see: cancel safety, permits and backpressure, hang-up detection, and cleanup that never waits on the application. Use a small fake peer.
- **End-to-end tests:** Check the wiring against the reference servers, one scenario per feature.

A test that needs time runs on Tokio's paused clock with `#[tokio::test(start_paused = true)]`. An end-to-end test waits for the server's next event instead of a fixed delay.

## Changes that go together

- A fix or a rejected approach that teaches a lesson: `docs/decisions.md`, in the same commit.
- A change of structure: the module docs, in the same commit.
- A public API change: the `README.md` examples and the doc tests.
- `justfile` or `.github/workflows/ci.yml`: keep their checks equivalent in tool versions, workspace and target selection, lockfile enforcement, warning policies, test filters, and coverage options. Local recipes stay quiet, and CI may print progress.
- A renamed or removed `justfile` recipe: every reference to it.
- The rustfmt nightly toolchain version: `justfile`, `.github/workflows/ci.yml`, and `.zed/settings.json`.
