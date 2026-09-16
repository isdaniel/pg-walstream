.PHONY: check build format audit test doc-check before-git-push deps-check deps-bump deps-bump-dry

deps-check:
	@echo "=== Checking for outdated dependencies ==="
	@cargo update --dry-run 2>&1 | grep -i "updating\|unchanged\|locking" || echo "All dependencies up to date"

deps-bump:
	@echo "=== Bumping dependencies to latest compatible versions ==="
	cargo update
	@echo "=== Verifying build (main crate) ==="
	cargo check --all-features
	@echo "=== Verifying build (load-tests) ==="
	cargo check --manifest-path load-tests/Cargo.toml --all-features
	@echo "=== Running tests ==="
	cargo test --features libpq
	@echo "Done. Review changes with: git diff Cargo.lock"

deps-bump-dry:
	@echo "=== Dry-run: what would be updated ==="
	@cargo update --dry-run 2>&1

check:
	cargo check

# The three configurations CI gates on. A single `cargo clippy` does not cover
# the libpq backend or the no_std build, both of which CI compiles separately.
lint:
	cargo clippy --workspace --all-targets --features derive -- -D warnings
	cargo clippy --no-default-features --features libpq --all-targets -- -D warnings
	cargo clippy --no-default-features --lib -- -D warnings

# `--features derive` matters: without it the number will not match CI's gate.
coverage:
	cargo llvm-cov --lib --features derive --summary-only

build:
	cargo build

format:
	cargo fmt

audit:
	cargo audit

test:
	cargo test

doc-check:
	cargo doc --no-deps --all-features

before-git-push: check lint build format audit test doc-check
