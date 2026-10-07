# Recipe naming:
# 1. Recipes without a suffix cover Rust and shared justfiles.
# 2. The -all variants add Python, Oxfmt-managed files, TypeScript, and
#    Bun-backed E2E tests.
# 3. A few scoped recipes stay public: install-rustfmt, test-doc, and test-e2e.
# 4. Private helpers run one tool each. Run one directly, such as
#    `just _lint-py`, for a targeted run.
# 5. Aggregate recipes take no arguments; pass arguments to a single-tool
#    recipe instead.
#
# Dependency policy, coverage, and MSRV checks are independent.

[windows]
set shell := ["pwsh", "-NoLogo", "-NoProfile", "-Command"]

set default-list

# Example recipes, such as `just examples::quick-start`.
mod examples

# rustfmt.toml uses nightly-only options, so formatting pins one nightly.
# Keep the CI and Zed toolchain pins in sync with this value.
rustfmt_toolchain := "nightly-2026-07-20"

# Set up the pinned nightly formatter for basic Rust development.
[group("installation")]
install: install-rustfmt

# Set up Rust formatting and all JavaScript and Python dependencies.
[group("installation")]
install-all: install _install-js _install-py

# Install Python dependencies and the required Python version.
_install-py *args:
    uv sync --quiet --locked {{ args }}

# Install JavaScript dependencies from the lockfile.
_install-js *args:
    bun install --quiet --frozen-lockfile {{ args }}

# Install the minimal nightly toolchain with rustfmt.
[group("installation")]
install-rustfmt *args:
    rustup toolchain install "{{ rustfmt_toolchain }}" --profile minimal --component rustfmt {{ args }}

# Check Rust and shared justfile formatting, and lint Rust targets.
[group("checks")]
[parallel]
check: (_fmt-cargo "--check") (_fmt-just "--check") lint

# Run format, lint, and type checks for every language.
[group("checks")]
[parallel]
check-all: check _check-py _check-js

# Check Python formatting, linting, and types.
[parallel]
_check-py: (_fmt-ruff "--check") _lint-py _typecheck-py

# Check Oxfmt-managed files and lint the TypeScript servers.
[parallel]
_check-js: (_fmt-oxfmt "--check") _lint-js

# Lint libraries, examples, and tests with warnings denied.
[group("checks")]
lint *args:
    cargo clippy --quiet --locked --workspace --all-targets {{ args }} -- -D warnings

# Lint every language and type-check the Python example.
[group("checks")]
[parallel]
lint-all: lint _lint-py _typecheck-py _lint-js

# Lint the Python example with warnings treated as errors.
_lint-py *args:
    uv run --locked ruff check -q {{ args }}

# Type-check the Python example; pyrefly warnings fail too.
_typecheck-py *args:
    uv run --locked pyrefly check --summary=none --min-severity warn {{ args }}

# Lint TypeScript test servers with type-aware rules.
_lint-js *args:
    bunx oxlint {{ args }}

# Format Rust sources and shared justfiles.
[group("formatting")]
[parallel]
fmt: _fmt-cargo _fmt-just

# Format Rust, Python, Oxfmt-managed files, and shared justfiles.
[group("formatting")]
[parallel]
fmt-all: fmt _fmt-ruff _fmt-oxfmt

_fmt-cargo *args:
    cargo +{{ rustfmt_toolchain }} fmt --all {{ args }}

_fmt-ruff *args:
    uv run --locked ruff format -q {{ args }}

_fmt-oxfmt *args:
    bunx oxfmt {{ args }}

_fmt-just *args:
    just --fmt {{ args }}
    just --fmt --justfile examples/justfile {{ args }}

# Run Rust unit and integration tests, then documentation examples.
[group("tests")]
test: _test-nextest test-doc

# Run regular Rust and Bun-backed E2E tests in one pass, then doctests.
[group("tests")]
test-all: _test-nextest-all test-doc

# Test Rust documentation examples.
[group("tests")]
test-doc *args:
    cargo test --quiet --locked --workspace --doc {{ args }}

# Run the end-to-end tests against the TypeScript reference server.
[group("tests")]
test-e2e *args: (_test-nextest "--run-ignored only -E 'binary(e2e)'" args)

# CI runs regular and E2E tests in one pass. Review new ignored tests before
# adding them, because this helper enables every ignored test.
_test-nextest-all: (_test-nextest "--run-ignored all")

_test-nextest *args:
    cargo nextest run --locked --workspace --all-targets {{ args }}

# Build documentation with warnings denied.
[group("docs")]
doc *args: (_doc args)

# Build documentation with warnings denied and open it in the browser.
[group("docs")]
doc-open *args: (_doc "--open" args)

[env("RUSTDOCFLAGS", "-D warnings")]
_doc *args:
    cargo doc --quiet --locked --no-deps --workspace --examples {{ args }}

# Run the Rust merge gate, including doctests and documentation.
[group("ci")]
ci: check test doc

# Run the full-project merge gate, including Bun-backed E2E tests.
[group("ci")]
ci-all: check-all test-all doc

# Check dependency advisories, licenses, bans, and sources.
[group("extra")]
deny *args:
    cargo deny --locked check --hide-inclusion-graph {{ args }}

# Check every target with the minimum supported compiler.
[arg("toolchain", long, help="Rust toolchain to check with")]
[group("extra")]
check-msrv toolchain="1.88" *args:
    cargo +{{ toolchain }} check --locked --workspace --all-targets {{ args }}

# Generate lcov.info for CI; requires cargo-llvm-cov and llvm-tools-preview.
[group("extra")]
coverage *args: (_coverage "--lcov --output-path lcov.info" args)

# Generate an HTML coverage report and open it in the browser.
[group("extra")]
coverage-open *args: (_coverage "--open" args)

_coverage *args:
    cargo llvm-cov nextest --locked --workspace --all-targets {{ args }}
