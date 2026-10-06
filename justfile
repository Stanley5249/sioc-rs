[windows]
set shell := ["pwsh", "-NoLogo", "-NoProfile", "-Command"]

set default-list

# Example recipes, such as `just examples::quick-start`.
mod examples

# rustfmt.toml uses nightly-only options, so formatting pins one nightly.
# Keep the CI and Zed toolchain pins in sync with this value.
rustfmt_toolchain := "nightly-2026-07-20"

# Run the local merge gate.
ci: fmt-check _ci-lint test test-e2e doc deny

# The linters read disjoint sources. Cargo work stays in order, because cargo
# serializes on the target directory lock anyway.
[parallel]
_ci-lint: lint lint-js

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
    just --fmt --justfile examples/justfile {{ args }}

# Lint libraries, examples, and tests with warnings denied.
lint *args:
    cargo clippy --quiet --locked --workspace --all-targets {{ args }} -- -D warnings

# Lint and type-check the Python servers; pyrefly warnings fail too.
lint-py:
    uv run --locked ruff check -q
    uv run --locked pyrefly check --summary=none --min-severity warn

# Lint TypeScript test servers with type-aware rules.
lint-js *args:
    bunx oxlint {{ args }}

# Run all targets with nextest, which fails hung tests, then documentation examples.
test *args:
    cargo nextest run --locked --workspace --all-targets {{ args }}
    cargo test --locked --workspace --doc {{ args }}

# Run the end-to-end tests against the TypeScript reference server.
test-e2e *args:
    cargo nextest run --locked --workspace --all-targets --run-ignored only -E 'binary(e2e)' {{ args }}

# Check documentation with warnings denied.
[env("RUSTDOCFLAGS", "-D warnings")]
doc *args:
    cargo doc --locked --no-deps --workspace --examples {{ args }}

# Check dependency advisories, licenses, bans, and sources.
deny *args:
    cargo deny --locked check {{ args }}

# Check every target with the minimum supported compiler.
[arg("toolchain", long, help="Rust toolchain to check with")]
msrv toolchain="1.88" *args:
    cargo +{{ toolchain }} check --locked --workspace --all-targets {{ args }}

# Generate an LCOV report with nextest; requires cargo-llvm-cov and llvm-tools-preview.
coverage *args:
    cargo llvm-cov nextest --locked --workspace --all-targets --lcov --output-path lcov.info {{ args }}
