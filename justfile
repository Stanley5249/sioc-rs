[windows]
set shell := ["pwsh", "-NoLogo", "-NoProfile", "-Command"]

set default-list

# rustfmt.toml uses nightly-only options, so formatting pins one nightly.
# CI installs this toolchain, and Zed formats through _rustfmt-stdin.
rustfmt_toolchain := "nightly-2026-07-20"

# Run the local merge gate.
ci: fmt-check _ci-lint test test-servers doc deny

# The linters read disjoint sources. Cargo work stays in order, because cargo
# serializes on the target directory lock anyway.
[parallel]
_ci-lint: lint lint-py lint-js

# Format all sources.
[parallel]
fmt: _fmt-cargo _fmt-ruff _fmt-oxfmt _fmt-just

# Check formatting of all sources without rewriting them.
[parallel]
fmt-check: (_fmt-cargo "--check") (_fmt-ruff "--check") (_fmt-oxfmt "--check") (_fmt-just "--check")

_fmt-cargo *args:
    cargo +{{ rustfmt_toolchain }} fmt --all {{ args }}

_fmt-ruff *args:
    uv run --locked ruff format -q {{ args }}

_fmt-oxfmt *args:
    bunx oxfmt {{ args }}

_fmt-just *args:
    just --fmt {{ args }}

# Format Rust from stdin to stdout, for rust-analyzer in .zed/settings.json.
_rustfmt-stdin:
    @rustfmt +{{ rustfmt_toolchain }} --edition 2024

# Lint libraries, examples, and tests with warnings denied.
lint *args:
    cargo clippy --quiet --locked --workspace --all-targets {{ args }} -- -D warnings

# Lint and type-check Python test servers.
lint-py:
    uv run --locked ruff check -q
    uv run --locked pyrefly check --summary=none

# Lint TypeScript test servers with type-aware rules.
lint-js *args:
    bunx oxlint {{ args }}

# Run all targets with nextest, which fails hung tests, then documentation examples.
test *args:
    cargo nextest run --locked --workspace --all-targets {{ args }}
    cargo test --locked --workspace --doc {{ args }}

# Run protocol and pressure tests against JavaScript and Python Socket.IO servers.
test-servers *args:
    cargo nextest run --locked --workspace --all-targets --run-ignored only -E 'binary(servers)' {{ args }}

# Run the Rust chat client against a quick-start server.
[env("RUST_LOG", "quick_start=info,sioc=trace")]
quick-start *args:
    cargo run --locked --example quick_start {{ args }}

# Start the TypeScript quick-start server on 127.0.0.1:3000.
quick-start-ts *args:
    bun examples/quick-start/server.ts {{ args }}

# Start the Python quick-start server on 127.0.0.1:3000.
quick-start-py *args:
    uv run --locked python examples/quick-start/server.py {{ args }}

# Check documentation with warnings denied.
[env("RUSTDOCFLAGS", "-D warnings")]
doc *args:
    cargo doc --locked --no-deps --workspace --examples {{ args }}

# Check dependency advisories, licenses, bans, and sources.
deny *args:
    cargo deny --locked check {{ args }}

# Check the minimum supported compiler; dev targets use stable Rust.
[arg("toolchain", long, help="Rust toolchain to check with")]
msrv toolchain="1.88" *args:
    cargo +{{ toolchain }} check --locked --workspace {{ args }}

# Generate an LCOV report; requires cargo-llvm-cov and llvm-tools-preview.
coverage *args:
    cargo llvm-cov --locked --workspace --all-targets --lcov --output-path lcov.info {{ args }}
