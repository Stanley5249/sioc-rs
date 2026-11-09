# sioc-rs

A Socket.IO client for Rust. The crate READMEs describe each layer and its protocol revision, and module docs identify the JavaScript references.

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

- Include `--workspace --all-targets` in `cargo` commands to unify features across the workspace.
- Set `RUST_LOG=sioc=trace` to log every wire packet, and `RUST_LOG=eioc=trace` to log library internals.
