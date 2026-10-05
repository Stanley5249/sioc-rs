[windows]
set shell := ["pwsh", "-NoLogo", "-NoProfile", "-Command"]

set default-list

quick_start := "cargo run --locked -p quick-start --example quick_start"

# Run the local merge gate.
ci: fmt-check _ci-lint test smoke doc deny

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
    cargo fmt --all {{ args }}

_fmt-ruff *args:
    uv run --locked ruff format -q {{ args }}

_fmt-oxfmt *args:
    bunx oxfmt {{ args }}

_fmt-just *args:
    just --fmt {{ args }}

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
