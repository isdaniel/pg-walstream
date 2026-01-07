.PHONY: check build format audit test doc-check before-git-push

check:
	cargo check
#	cargo clippy -- -D warnings

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

before-git-push: check build format audit test doc-check
