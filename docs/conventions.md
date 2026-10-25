# Conventions

## Design

`mpsc` below refers to `tokio::sync::mpsc`. A component is a unit with its own state and lifecycle, such as the manager, the engine, or a transport.

### Invariants

These protect correctness. A change that breaks one needs a new decision in `docs/decisions.md`.

- Each piece of mutable state has one owner. The owner can be a task, a future inside a task, or a synchronous core that a task drives. Others act on the state through its owner, by a method call or a channel, instead of sharing it.
- Use `mpsc::channel` to give every channel fed by the server a bounded capacity. Use `mpsc::unbounded_channel` only if library state limits the number of queued items. Document that limit in a comment on the channel.
- A `select!` loop that serves several inputs never awaits inside a handler. The loop waits for each output, such as a channel permit or a timer, as its own branch beside the inputs, so a full queue holds up only the data that needs it. Prove that every branch future is safe to cancel. A future that another branch must not cancel, such as an HTTP request in flight, is owned outside the `select!` and polled by reference. Dropping it on purpose, such as to cancel an opening, is cleanup, not cancellation by a branch.
- Shut down through half-close. A component drops its channel senders with `drop` to initiate shutdown, then waits until `mpsc::Receiver::recv` or `mpsc::UnboundedReceiver::recv` returns `None` for every input channel before finishing. Under this protocol, sends between library components succeed, so treat any send failure as an error. If delivery fails because the caller dropped a receiver, discard the item. Discard an item only when its destination has ended, such as a closed session or namespace.
- No two components wait on each other in a cycle. The module docs at each channel boundary state why the two sides cannot wait on each other.

### Preferences

These are defaults. Depart from one when the design is simpler, and say why in the module docs.

- Put protocol rules in a synchronous core that never awaits when their state and branches are worth testing apart from I/O. A core takes and returns plain data. Concurrency that belongs to one component, such as a transport's requests, may stay in that component's async code.
- Run independent I/O directions, such as a transport's read and write halves, in separate loops joined with `tokio::join!` when no state ties them together.
- Inside a component, items waiting in one direction wait in one place. A bounded channel between two components is that boundary's buffer and does not count as a second one.

## JS parity and naming

- Match the observable wire behavior of the JS reference. Internal structure follows Rust ownership and need not mirror the JS objects.
- A type, field, variant, or public function names its JS counterpart in its doc comment when one exists. Otherwise the doc comment states what the item is for.
- Four tags name common purposes that JS gets for free. `Rust channel` is a channel between tasks where JS calls a method, `Rust bound` is a limit where JS queues are unbounded, `Rust cleanup` is a reclamation step where JS has GC and a synchronous `close`, and `Rust wait` is a wait where JS writes at once.
- A private helper needs only a clear name instead of a JS counterpart.
- Name a symbol in three steps. Take the JS name, adapt it to Rust conventions, then compare it with similar names in std, the dependencies, the JS libraries, and this codebase, and pick one that stays distinct.
- Name a module after the concept it holds, as the JS files do, rather than after one of its types.
- Use frame for transport data, packet for a wire unit of either protocol, and message for what Engine.IO carries between the two layers.
- Engine sessions open and close, and namespaces connect and disconnect. Use shutdown for a component's termination, join for awaiting a task's completion, and drain for consuming the items left in a channel. These are vocabulary conventions, not a list of methods every component must expose.
- Prefix Engine.IO message and frame types with `Client` or `Server` for the side that produces them, such as `ClientMessage` and `ServerFrame`. Name each channel's sender and receiver after the values the channel carries, such as `client_message_tx`.

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

- **Core tests:** A rule that a synchronous core decides, such as ordering, acks, reconnection, namespace lifecycle, and binary packets, is a plain `#[test]` on that core. It sends inputs and asserts outputs and queries, using builders only. `sioc/src/manager/tests/state.rs` shows the shape.
- **I/O tests:** Cover what a core cannot see, which is cancel safety, permits and backpressure, hang-up detection, and cleanup that never waits on the application, and the rules of a component without a core, such as a transport. Use a small fake peer, such as the harness in `sioc/src/manager/tests/actor/`, or bare channels for a client handle, as `sioc/src/client/tests/` does.
- **End-to-end tests:** Check the wiring against the reference servers, one scenario per feature.

Under backpressure, test safety and progress apart. Safety means a full queue loses and reorders nothing, and a core test checks it. Progress means a full queue holds up only the data that needs it, and only an I/O test can check it, as `emits_flow_while_receiver_is_full` does.

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
