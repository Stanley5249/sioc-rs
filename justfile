set default-list
set windows-shell := ["pwsh", "-NoLogo", "-NoProfile", "-Command"]

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

# Run all targets and documentation examples.
test:
    cargo test --locked --workspace --all-targets
    cargo test --locked --workspace --doc

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

# Run every smoke test against a real Socket.IO server.
smoke: smoke-python

# Run the quick-start client against the python-socketio server.
smoke-python:
    cargo build --locked -p quick-start --example quick_start
    uv run --locked --directory examples/quick-start python server.py --client "cargo run --locked -p quick-start --example quick_start"
