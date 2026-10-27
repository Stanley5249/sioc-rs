# Conventions

Invariants protect correctness, so a change that breaks one needs a new entry in `docs/decisions.md`. Everything else is a default that gives way when a design is simpler, with the reason in the module docs. `docs/decisions.md` holds the incidents behind these rules, and module docs hold how each component applies them.

## Async invariants

- **Ownership:** Each piece of mutable state has one owner, such as a task or a synchronous core, and others act through it. Handles may share a core through one mutex when each call applies a whole synchronous transition, no lock spans an await or a user connector, and a panic wakes the driver for shutdown.
- **Bounded memory:** Every channel the server can fill is bounded. An unbounded channel needs a library limit on its items, stated in a comment on the channel.
- **Progress:** A full output holds up only the data bound for it, never other inputs, shutdown, or cleanup. A `select!` loop with several data inputs waits for each output, such as a permit or a timer, as its own branch instead of awaiting inside a handler. Every branch future is cancel safe, and a future that must survive, such as an HTTP request in flight, is owned outside the `select!` and polled by reference.
- **Shutdown:** Each component has one way its peers learn that it ended, and its cleanup never waits on the application. An item is discarded only when its destination has ended or the application dropped its receiver, so nothing is lost silently.
- **Wakeups:** Every wait has a wakeup that its condition triggers, and no two components wait on each other. A channel's end hides behind its unread items, so a reader that can stall under backpressure also watches a signal that ends with the sender, such as a value-less `watch` channel.
- **Accounting:** A queued item and its permit have one holder until submission or cancellation. A closing participation stays unusable until its old work is gone.

## Design defaults

- Put protocol rules in a synchronous core that takes and returns plain data when they are worth testing apart from I/O. Concurrency that belongs to one component, such as a transport's requests, may stay in its async code.
- Run independent I/O directions as separate futures, so a stall in one never holds up the other.
- Tasks joined by channels shut down by half-close. A task drops its senders and finishes after every input returns `None`, so a failed send between them is a bug. Handles that share a core end through the core instead.
- Add a task, channel, buffer, lifecycle flag, or manual `poll` only for a responsibility nothing else holds, and say why in a doc comment when it is not obvious.

## Protocol and naming

- Match the observable wire behavior of the JS reference. Internal structure follows Rust ownership.
- Start a name from its JS counterpart and keep it distinct from similar names in std, the dependencies, and this codebase.
- A doc comment says what the item is for and names its JS counterpart when one exists. When an item exists only because Rust lacks what JS has, such as a garbage collector, unbounded queues, or direct calls between objects, say so in plain words. Write a reason that several items share once on their parent instead of on each item.
- Use frame for transport data, packet for a wire unit of either protocol, and message for what Engine.IO carries between the layers. Prefix Engine.IO message and frame types with `Client` or `Server` for the side that produces them, and name a channel's ends after what it carries, such as `client_message_tx`.
- Engine sessions open and close, and namespaces connect and disconnect. Shutdown ends a component, join awaits a task, and drain consumes the items left in a channel.

## Public API

- Each public item has one public path, and `prelude` is the only other re-export. `Result` and `Error` aliases stay out of `prelude` so they do not shadow `std`.
- Make an item public only for callers outside the workspace, such as users, derive expansions, and implementations of public traits. Use `pub(crate)` for a crate-internal item that belongs in a public type's module, such as `PayloadError::new`.
- A diagnostic code reads `crate::component::failure` with stable conceptual components, such as `sioc::parse` or `eioc::transport::websocket`. A transparent wrapper keeps the inner code, and a wrapper that adds context has its own. Users match on codes, so only a breaking release renames one.
- Public error enums are `#[non_exhaustive]`. So is a configuration type or endpoint bundle likely to grow, with a constructor that documents its defaults. A type that mirrors a fixed protocol set, such as a packet, stays exhaustive.
- `Config` holds a subsystem's settings, `Options` holds one operation's parameters, and `Builder` builds a value step by step.

## Style

`rustfmt.toml`, Clippy, and the workspace lints cover formatting, imports, and missing docs. Beyond them:

- Prefer a `mod.rs` that holds only module docs, `mod` declarations, and re-exports.
- Write a Cargo requirement to its leftmost nonzero component, such as `"1"` or `"0.7"`, so downstream crates can share a compatible version. The lockfile pins exact versions.

## Tests

- **Placement:** Unit tests sit at the bottom of their file, component tests in the module's `tests.rs` or a `tests/` directory split by behavior, and end-to-end and derive tests in `sioc/tests/`.
- **Layers:** Test a rule at the layer that decides it. Core tests are plain `#[test]` cases that feed inputs and assert outputs, as in `sioc/src/manager/tests/`. I/O tests cover what a core cannot see, such as wakeups, cancellation, backpressure progress, and cleanup, using a small fake peer like `sioc/src/manager/tests/driver/`. End-to-end tests check one scenario per feature against the reference servers.
- **Outcomes:** Each test protects a contract you can name, after which it is named. Assert the whole outcome, including order and the absence of extra outputs.
- **Time:** Synchronize on an explicit event or Tokio's paused clock, never a fixed wait. A real-time timeout only guards against a hang.
- **Harnesses:** Fake only what lies outside the code under test, never its routing, scheduling, or state. A helper moves to the shared harness once a second file needs it.
- **Changes:** Move tests unchanged before rewriting them, and delete a test only after naming the test that still catches its bug.

## Changes that go together

- A lesson from a fix or a rejected approach: `docs/decisions.md`, in the same commit.
- A change of structure: the module docs, in the same commit.
- A public API change: the `README.md` examples and the doc tests.
- `justfile` and `.github/workflows/ci.yml`: the same tool versions, targets, lockfile enforcement, warning policy, test filters, and coverage options, with quiet local recipes. A renamed or removed recipe: every reference to it.
- The rustfmt nightly version: `justfile`, `.github/workflows/ci.yml`, and `.zed/settings.json`.
- The MSRV: `rust-version` in `Cargo.toml`, the `check-msrv` recipe, and `.github/workflows/ci.yml`.
