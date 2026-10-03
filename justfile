set default-list
set windows-shell := ["pwsh", "-NoLogo", "-NoProfile", "-Command"]

quick_start := "cargo run --locked -p quick-start --example quick_start"

# Format Rust sources and this justfile.
fmt:
    cargo fmt --all
    just --fmt

# Check formatting without rewriting sources.
fmt-check:
    cargo fmt --all --check
    just --fmt --check

# Lint libraries, examples, and tests with warnings denied.
lint:
    cargo clippy --locked --workspace --all-targets -- -D warnings

# Lint, format-check, and type-check Python test servers.
lint-py:
    uv run --locked ruff check
    uv run --locked ruff format --check
    uv run --locked pyrefly check

# Format-check JS, JSON, YAML, and Markdown, and lint TypeScript test servers.
lint-js:
    bunx oxfmt --check
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
