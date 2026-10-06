[windows]
set shell := ["pwsh", "-NoLogo", "-NoProfile", "-Command"]

set default-list

# Example recipes, such as `just examples::quick-start`.
mod examples

# rustfmt.toml uses nightly-only options, so formatting pins one nightly.
# Keep the CI and Zed toolchain pins in sync with this value.
rustfmt_toolchain := "nightly-2026-07-20"

# CI enables progress output while local runs keep tool summaries quiet.
verbose := env("SIOC_VERBOSE", "0")
quiet := if verbose == "1" { "" } else { "--quiet" }
cargo_quiet := if verbose == "1" { "" } else { "--cargo-quiet" }
pyrefly_output := if verbose == "1" { "--output-format full-text-with-github" } else { "--summary=none" }
deny_output := if verbose == "1" { "" } else { "--hide-inclusion-graph" }

# Check every language and justfile formatting during development.
[group("validation")]
[parallel]
check: check-rust check-py check-js (_fmt-just "--check")

# Check Rust formatting and lint every workspace target.
[group("validation")]
[parallel]
check-rust: (_fmt-cargo "--check") lint

# Check Python formatting, linting, and types.
[group("validation")]
[parallel]
check-py: (_fmt-ruff "--check") lint-py typecheck-py

# Check Oxfmt-managed files and lint the TypeScript servers.
[group("validation")]
[parallel]
check-js: (_fmt-oxfmt "--check") lint-js

# Run the full local merge gate.
[group("validation")]
ci: check _test-all test-doc doc deny

# Format all sources.
[group("formatting")]
[parallel]
fmt: _fmt-cargo _fmt-ruff _fmt-oxfmt _fmt-just

# Check formatting of all sources without rewriting them.
[group("formatting")]
[parallel]
fmt-check: (_fmt-cargo "--check") (_fmt-ruff "--check") (_fmt-oxfmt "--check") (_fmt-just "--check")

_fmt-cargo *args:
    cargo +{{ rustfmt_toolchain }} fmt --all {{ args }}

_fmt-ruff *args:
    uv run --locked ruff format {{ quiet }} {{ args }}

_fmt-oxfmt *args:
    bunx oxfmt {{ args }}

_fmt-just *args:
    just --fmt {{ args }}
    just --fmt --justfile examples/justfile {{ args }}

# Lint libraries, examples, and tests with warnings denied.
[group("validation")]
lint *args:
    cargo clippy {{ quiet }} --locked --workspace --all-targets {{ args }} -- -D warnings

# Lint the Python example with warnings treated as errors.
[group("validation")]
lint-py *args:
    uv run --locked ruff check {{ quiet }} {{ args }}

# Type-check the Python example; pyrefly warnings fail too.
[group("validation")]
typecheck-py *args:
    uv run --locked pyrefly check {{ pyrefly_output }} --min-severity warn {{ args }}

# Lint TypeScript test servers with type-aware rules.
[group("validation")]
lint-js *args:
    bunx oxlint {{ args }}

# Run Rust unit and integration tests, then documentation examples.
[group("tests")]
test: test-rust test-doc

# Run Rust unit and integration tests, skipping ignored E2E tests.
[group("tests")]
test-rust *args: (_test-nextest args)

# Test Rust documentation examples.
[group("tests")]
test-doc *args:
    cargo test {{ quiet }} --locked --workspace --doc {{ args }}

# Run the end-to-end tests against the TypeScript reference server.
[group("tests")]
test-e2e *args: (_test-nextest "--run-ignored only -E 'binary(e2e)'" args)

# CI runs regular and E2E tests in one pass. Review new ignored tests before
# adding them, because this helper enables every ignored test.
_test-all: (_test-nextest "--run-ignored all")

_test-nextest *args:
    cargo nextest run {{ cargo_quiet }} --locked --workspace --all-targets {{ args }}

# Check documentation with warnings denied.
[env("RUSTDOCFLAGS", "-D warnings")]
[group("maintenance")]
doc *args:
    cargo doc {{ quiet }} --locked --no-deps --workspace --examples {{ args }}

# Check dependency advisories, licenses, bans, and sources.
[group("maintenance")]
deny *args:
    cargo deny --locked check {{ deny_output }} {{ args }}

# Check every target with the minimum supported compiler.
[arg("toolchain", long, help="Rust toolchain to check with")]
[group("validation")]
check-msrv toolchain="1.88" *args:
    cargo +{{ toolchain }} check --locked --workspace --all-targets {{ args }}

# Generate lcov.info for CI; requires cargo-llvm-cov and llvm-tools-preview.
[group("tests")]
coverage *args: (_coverage "--lcov --output-path lcov.info" args)

# Generate an HTML coverage report and open it in the browser.
[group("tests")]
coverage-open *args: (_coverage "--open" args)

_coverage *args:
    cargo llvm-cov nextest --locked --workspace --all-targets {{ args }}
