# sioc-rs

A Socket.IO client for Rust. `eioc` speaks Engine.IO revision 4, `sioc` speaks Socket.IO revision 5 on top of it, and `sioc-macros` holds the derives. `sioc` follows socket.io-client and `eioc` follows engine.io-client, at the version the module docs link to.

## Read before

- Any task: `README.md`, and `just --list` or the `justfile` for commands.
- Writing or reviewing code, tests, or docs: `docs/conventions.md`.
- Changing an area: its section in `docs/decisions.md`, and the docs of the modules you touch.
- Changing examples: `examples/AGENTS.md`.
- Writing a commit, an issue, or a pull request: `CONTRIBUTING.md`.

## Always

- Each piece of state has one owner in one task. A protocol layer is a synchronous core that never awaits, plus one task that owns its I/O and whose handlers never await. The sioc manager is built this way, and eioc moves to it in #13.
- Every type, field, variant, and public function names its JS counterpart in its doc comment, or the reason Rust needs it when JS does without.
- Propose each new symbol with its JS counterpart before writing the code.
- Include `--workspace --all-targets` in `cargo` commands to unify features across the workspace.
- Set `RUST_LOG=sioc=trace` to log every wire packet, and `RUST_LOG=eioc=trace` to log library internals.
