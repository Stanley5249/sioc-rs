# Agent guidelines

## Build and test

Always use `--workspace --all-targets` for cargo checks and tests. The only exception is the MSRV check, which omits `--all-targets` so dev-dependencies do not gate the supported compiler version.

```sh
just ci
just lint
just lint-py
just lint-js
just test
just smoke
```

Python and TypeScript test servers share the root `pyproject.toml` and `package.json`; `just smoke` runs the example clients against them.

## Running examples

```sh
RUST_LOG=info cargo run --example glhf
RUST_LOG=glhf=trace,sioc=trace cargo run --example glhf
```

Use `--example <name>` at workspace root; omit `-p`. `sioc=trace` shows all wire packets; `eioc=trace` is library internals only.

## Code style

Re-export public surface through `prelude`. Never put `Result` or `Error` aliases in `prelude` because they shadow `std` and cause ambiguity; import by explicit path (`use eioc::error::{Error, Result}`).

Variable names should be descriptive and avoid unnecessary abbreviations. Prefer `packet` and `event` over `pkt` and `evt`. For short-lived variables, single-letter names like `p` and `e` are acceptable.

## Commits

Use Conventional Commits; `cliff.toml` groups release notes by type, so pick the type a reader of the notes expects.

- Scope with the folder or file name the change lives in. Use `deps` for dependency updates and `agents` for agent instructions.
- Mark a change to the public API breaking with `!` and a `BREAKING CHANGE:` footer. A dependency bump counts when the dependency's types appear in the public API.
