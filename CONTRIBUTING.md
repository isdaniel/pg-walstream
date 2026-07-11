# Contributing to pg_walstream

Thanks for your interest in contributing! This document covers how to get set up,
the standards we hold code to, and how to submit changes.

## Code of Conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). By participating,
you are expected to uphold it.

## Getting started

```bash
git clone https://github.com/isdaniel/pg-walstream.git
cd pg-walstream
cargo build
cargo test --lib
```

`pg_walstream` has two connection backends:

```bash
# Default libpq FFI backend
cargo build

# Pure-Rust rustls-tls backend
cargo build --no-default-features --features rustls-tls
```

## Before you open a pull request

Run the full pre-push pipeline locally — this is required before pushing:

```bash
make before-git-push
```

This runs `check`, `build`, `format`, `audit`, `test`, and `doc-check`. CI also
enforces **code coverage ≥ 90%**, so new logic should come with tests.

### Integration tests

Integration tests under `integration-tests/` need a running PostgreSQL 15+ with
`wal_level = logical`. They are not part of normal development and only run in CI
with the `--ignored` flag. You generally do not need to touch them unless you are
working on connection or streaming features.

## Project layout

See the architecture section in [`.claude/CLAUDE.md`](.claude/CLAUDE.md) and the
`README.md` for a map of the crate. The performance-critical hot path is the WAL
message parsing/streaming pipeline — changes there should be benchmarked:

```bash
cargo bench --bench wal_pipeline
```

## Performance regression CI

Every PR runs the `Benchmarks` workflow (`.github/workflows/benchmarks.yml`),
which executes the `wal_pipeline` and `deserialize` criterion benches under
[CodSpeed](https://codspeed.io) in `simulation` mode. Simulation mode measures
instruction/cache behavior on a simulated CPU, so results are deterministic —
there is no CI-runner noise and no need to re-run for accuracy.

CodSpeed compares the PR against the base branch's last recorded run (baselines
come from the push-to-`main` trigger) and posts a per-benchmark diff as a PR
comment plus a status check. The regression **threshold is configured in the
CodSpeed dashboard**, not in the workflow YAML. To make a regression block
merge, add the CodSpeed status check to the `main` branch-protection rules.

Locally, `cargo bench` is unaffected — `codspeed-criterion-compat` is a
passthrough to criterion when not run under `cargo codspeed`.

**One-time setup (maintainers):** sign in to codspeed.io with the GitHub org and
enable this repository (installs the CodSpeed GitHub App). `CODSPEED_TOKEN` is
optional for this public repo (OIDC via `id-token: write`); add it as a repo
secret only if tokenless auth is insufficient.

## Coding conventions

- Prefer zero-copy `Bytes` over `Vec<u8>` for data flowing through the pipeline.
- Use `#[inline]` on per-message hot-path methods; keep error-path constructors `#[cold]`.
- No `debug!` logging in the DML hot-path parsers (BEGIN/INSERT/UPDATE/DELETE/COMMIT).
- Keep changes focused — one logical change per PR.

## Commit messages & PRs

- Write clear, descriptive commit messages explaining the *why*.
- Reference related issues (e.g. `Closes #123`).
- Fill out the pull request template, including the checklist.
- Keep PRs reasonably small and reviewable.

## Reporting bugs & requesting features

Use the [issue templates](https://github.com/isdaniel/pg-walstream/issues/new/choose).
For security issues, **do not** open a public issue — see [SECURITY.md](SECURITY.md).

## License

By contributing, you agree that your contributions will be licensed under the
[BSD 3-Clause License](LICENSE) that covers this project.
