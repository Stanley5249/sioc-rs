# Conventions

## Design

`mpsc` below refers to `tokio::sync::mpsc`.

- Each piece of state has one owner in one task. The protocol core owns every protocol decision and never awaits. Its task owns the I/O and turns what the I/O yields into the core's inputs. The sioc manager uses this design, and the eioc migration is tracked in #13.
- A loop that serves several inputs never awaits inside a handler. The loop waits for each output, such as a channel permit or a timer, as its own `select!` branch beside the inputs, so a full queue holds up only the data that needs it. Prove that every branch future is safe to cancel.
- Run independent I/O directions, such as a transport's read and write halves, in separate loops joined with `tokio::join!` when no state ties them together.
- Shut down through half-close. A task drops its channel senders with `drop` to initiate shutdown, then waits until `mpsc::Receiver::recv` or `mpsc::UnboundedReceiver::recv` returns `None` for every input channel before finishing. Under this protocol, sends between library tasks succeed, so treat any send failure as an error. If delivery fails because the caller dropped a receiver, discard the item.
- Use `mpsc::channel` to give every channel fed by the server a bounded capacity. Use `mpsc::unbounded_channel` only if library state limits the number of queued items. Document that limit in a comment on the channel.

## JS parity and naming

- Every type, field, variant, and public function names its JS counterpart in its doc comment, or the reason Rust needs it when JS does without. Propose each new symbol with its JS counterpart before writing the code.
- A private helper needs only a clear name instead of a JS counterpart.
- An item without a JS counterpart names one of four reasons, each something JS gets for free. These are a channel between tasks (JS calls a method), a bound (JS queues are unbounded), cleanup (JS has GC and a synchronous `close`), and a wait (JS writes at once). An item that fits none of these is a design to remove.
- Name a symbol in three steps. Take the JS name, adapt it to Rust conventions, then compare it with similar names in std, the dependencies, the JS libraries, and this codebase, and pick one that stays distinct.
- Name a module after the concept it holds, as the JS files do, rather than after one of its types.
- Use frame for transport data, packet for a wire unit of either protocol, and message for what Engine.IO carries between the two layers. Engine sessions open and close, and namespaces connect and disconnect.
- Name each channel's sender and receiver after the items the channel carries. When two channels carry the same item type, prefix both ends of each channel with `server_` or `client_` to identify the side that produces the items.

## Code style

- Re-export the public API through `prelude`. Keep `Result` and `Error` aliases outside `prelude` so they do not shadow the corresponding `std` types.
- Import types and traits by name, or glob-import a `prelude`. To call a function in another module of the same crate, use its full `crate::` path instead of importing that module.
- For a module with child files, use `<name>/mod.rs` as the module file. Keep that file limited to documentation, `mod` declarations, and public API re-exports.
- Write each third-party Cargo requirement up to its leftmost nonzero component, such as `"1"` or `"0.7"`, and each Bun requirement as `bun add` writes it, such as `"^1.4.2"`. The lockfiles pin exact versions.

## Tests

- **Unit tests:** Put them at the bottom of the source file.
- **Component tests:** Put them in the submodule's `tests.rs`, or in a `tests/` directory split by scenario group.
- **End-to-end and derive macro tests:** Put them in `sioc/tests/`.

Each test protects a contract and catches a bug you can name. Assert the whole outcome the test controls, including order and the absence of extra outputs, so a regression cannot pass by adding or reordering packets.

Test each behavior at the layer that decides it. Add a test at a higher layer only for what the lower one cannot see.

- **Core tests:** Every protocol rule, such as ordering, acks, reconnection, namespace lifecycle, and binary packets, is a plain `#[test]` on the core. It sends inputs and asserts outputs and queries, using builders only. `sioc/src/manager/tests/state.rs` shows the shape.
- **Task tests:** Cover only what the core cannot see, which is cancel safety, permits and backpressure, hang-up detection, and cleanup that never waits on the application. Use a small fake peer, such as the harness in `sioc/src/manager/tests/actor/`, or bare channels for a client handle, as `sioc/src/client/tests/` does.
- **End-to-end tests:** Check the wiring against the reference servers, one scenario per feature.

Under backpressure, test safety and progress apart. Safety means a full queue loses and reorders nothing, and a core test checks it. Progress means a full queue holds up only the data that needs it, and only a task test can check it, as `emits_flow_while_receiver_is_full` does.

Synchronize a test on an explicit event, such as a message, a channel end, or the virtual clock, because a fixed wait fails on a slow machine, and a check that something did not happen still passes when it happens after the wait.

- A test that needs time runs on Tokio's paused clock with `#[tokio::test(start_paused = true)]`.
- Check that nothing happened in a core test, where an empty output needs no wait.
- An end-to-end test waits for the server's next event. A real-time timeout there only guards against a hang.

Name a test after its condition and expected behavior, such as `dropping_the_ack_handle_keeps_the_event`. When patches have hidden what a test checks, rewrite the test or its group and keep the guarantees it protected.

## Changes that go together

- A fix or a rejected approach that teaches a lesson: `docs/decisions.md`, in the same commit.
- A change of structure: the module docs, in the same commit.
- A public API change: the `README.md` examples and the doc tests.
- `justfile` or `.github/workflows/ci.yml`: keep their checks equivalent in tool versions, workspace and target selection, lockfile enforcement, warning policies, test filters, and coverage options. Local recipes stay quiet, and CI may print progress.
- A renamed or removed `justfile` recipe: every reference to it.
- The rustfmt nightly toolchain version: `justfile`, `.github/workflows/ci.yml`, and `.zed/settings.json`.
