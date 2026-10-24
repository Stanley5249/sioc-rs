# sioc-rs

A Socket.IO client for Rust. `eioc` speaks Engine.IO revision 4, `sioc` speaks Socket.IO revision 5 on top of it, and `sioc-macros` holds the derives. `sioc` follows socket.io-client and `eioc` follows engine.io-client, at the version the module docs link to.

## Docs

Each fact has one home. Read a file before the task it names.

- `README.md`: usage, compatibility, and development setup. Read before any task, with `just --list` or the `justfile` for commands.
- `sioc/README.md` and `eioc/README.md`: what each crate is and which protocol revision it speaks.
- `CONTRIBUTING.md`: what to check before submitting, titles, and commit conventions. Read before writing a commit, an issue, or a pull request.
- `docs/conventions.md`: how code, tests, and docs are written. Read before writing or reviewing them.
- Module docs: how each part works now, and its invariants. Read the ones you touch.
- `docs/decisions.md`: mistakes and rejected approaches, each with what replaced it. Read the section for an area before changing it.
- `examples/AGENTS.md`: rules for the examples. Read before changing them.

## Always

- Each piece of state has one owner in one task. A protocol layer is a synchronous core that never awaits, plus one task that owns its I/O and whose handlers never await. The sioc manager is built this way, and eioc moves to it in #13.
- Every type, field, variant, and public function names its JS counterpart in its doc comment, or the reason Rust needs it when JS does without.
- Propose each new symbol with its JS counterpart before writing the code.
- Include `--workspace --all-targets` in `cargo` commands to unify features across the workspace.
- Set `RUST_LOG=sioc=trace` to log every wire packet, and `RUST_LOG=eioc=trace` to log library internals.
