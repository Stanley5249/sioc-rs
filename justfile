set default-list
set windows-shell := ["pwsh", "-NoLogo", "-NoProfile", "-Command"]

quick_start := "cargo run --locked -p quick-start --example quick_start"

# Run the local merge gate.
ci: fmt-check lint lint-py lint-js test smoke doc deny

# Format all sources.
fmt: fmt-rs fmt-py fmt-js

# Format Rust sources and this justfile.
fmt-rs:
    cargo fmt --all
    just --fmt

# Format Python test servers.
fmt-py:
    uv run --locked ruff format -q

# Format JS, TS, JSON, YAML, and Markdown.
fmt-js:
    bunx oxfmt

# Check formatting of all sources without rewriting them.
fmt-check: fmt-check-rs fmt-check-py fmt-check-js

# Check Rust and justfile formatting.
fmt-check-rs:
    cargo fmt --all --check
    just --fmt --check

# Check Python formatting.
fmt-check-py:
    uv run --locked ruff format --check -q

# Check JS, TS, JSON, YAML, and Markdown formatting.
fmt-check-js:
    bunx oxfmt --check

# Lint libraries, examples, and tests with warnings denied.
lint:
    cargo clippy --locked --workspace --all-targets -- -D warnings

# Lint and type-check Python test servers.
lint-py:
    uv run --locked ruff check -q
    uv run --locked pyrefly check --summary=none

# Lint TypeScript test servers with type-aware rules.
lint-js:
    bunx oxlint

# Run all targets and documentation examples.
test:
    cargo test --locked --workspace --all-targets
    cargo test --locked --workspace --doc

# Run every smoke test against a real Socket.IO server.
smoke: smoke-py smoke-js

# Run the quick-start client against the python-socketio server.
smoke-py: _build-quick-start
    uv run --locked python examples/quick-start/server.py --client "{{ quick_start }}"

# Run the quick-start client against the reference socket.io server.
smoke-js: _build-quick-start
    bun examples/quick-start/server.ts --client {{ quick_start }}

# Build the client first so the server's startup does not wait on cargo.
_build-quick-start:
    cargo build --locked -p quick-start --example quick_start

# Check documentation with warnings denied.
[env("RUSTDOCFLAGS", "-D warnings")]
doc:
    cargo doc --locked --no-deps --workspace --examples

# Check dependency advisories, licenses, bans, and sources.
deny:
    cargo deny --locked check

# Check the minimum supported compiler; dev targets use stable Rust.
msrv toolchain="1.88":
    cargo +{{ toolchain }} check --locked --workspace

# Generate an LCOV report; requires cargo-llvm-cov and llvm-tools-preview.
coverage:
    cargo llvm-cov --locked --workspace --all-targets --lcov --output-path lcov.info
