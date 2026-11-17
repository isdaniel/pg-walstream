# pg-walstream Development Guide

## Build & Test Commands

```bash
# Build (default: pure-Rust rustls-tls backend)
cargo build

# Build (opt-in libpq FFI backend)
cargo build --no-default-features --features libpq

# Run unit tests (no PostgreSQL required)
cargo test --lib

# Run unit tests including the derive-macro layer
cargo test --lib --features derive

# Run benchmarks
cargo bench --bench wal_pipeline

# Format check
cargo fmt --all -- --check

# Lint — CI runs all three; `--all-targets` alone does NOT match it
cargo clippy --workspace --all-targets --features derive -- -D warnings
cargo clippy --no-default-features --features libpq --all-targets -- -D warnings
cargo clippy --no-default-features --lib -- -D warnings          # no_std

# Coverage (requires cargo-llvm-cov)
# `--features derive` is required, or the number won't match CI's gate.
cargo llvm-cov --lib --features derive --summary-only
cargo llvm-cov --lib --features derive --lcov --output-path lcov.info

# Loom: model-check SharedLsnFeedback. Backend-free feature set (tokio changes under
# `--cfg loom`) and the test filter are both required.
RUSTFLAGS='--cfg loom' cargo test --lib --release --no-default-features --features std lsn::loom_tests

# Mutation testing of src/lsn.rs (cargo install cargo-mutants; scope in .cargo/mutants.toml)
cargo mutants
```

Check results by **exit code**, not by grepping for `error`: cargo indents
diagnostics, so `grep '^error'` silently reports success on a failing run.

## Architecture

```
src/
├── lib.rs           # Public re-exports
├── buffer.rs        # Zero-copy BufferReader/BufferWriter (bytes crate)
├── protocol.rs      # WAL message parser (hot path)
├── stream.rs        # High-level LogicalReplicationStream + EventStream
│                    #   + ReplicationStreamConfig builder, EventStream::for_each_event
├── router.rs        # WalRouter: typed async by-table event router (convenience layer)
├── handler.rs       # WalTable trait (bound to #[derive(WalTable)])
├── column_value.rs  # ColumnValue/RowData types
├── deserializer.rs  # serde Deserializer for RowData → user structs
├── types.rs         # Type aliases, CachePadded, ChangeEvent, Lsn
├── error.rs         # ReplicationError enum
├── lsn.rs           # Thread-safe SharedLsnFeedback (atomic CAS)
├── copy_text.rs     # COPY TEXT decoder (crate-private, snapshot only)
├── snapshot/        # Managed initial snapshot (plan.rs / table_copy.rs / rows.rs / events.rs) + aligned.rs (tables added to a running stream)
├── sql_builder.rs   # SQL statement builders (CREATE SLOT, etc.)
├── retry.rs         # Exponential backoff retry logic
└── connection/      # PostgreSQL connection backends
    ├── mod.rs
    ├── libpq.rs     # libpq FFI backend (opt-in)
    └── native/      # Pure-Rust rustls-tls backend (default)
        └── copy_out.rs  # CopyOut state machine (separate from copy.rs)

macros/              # pg-walstream-macros proc-macro crate (opt-in `derive` feature)
└── src/lib.rs       #   #[derive(WalTable)] → impl WalTable { const TABLE }
```

## Client Ergonomics Layer (not hot path)

Convenience API on top of the core streaming primitives, all `Send + 'static`:

- `ReplicationStreamConfig::builder(slot, publication).with_*(...)` — avoids the
  8-arg `new()` (which is retained, non-breaking).
- `EventStream::for_each_event(|ev| async {...})` — consume loop that auto-advances
  the applied LSN after each `Ok` handler; `Cancelled` → graceful exit.
- `WalRouter` — routes by `(table, kind)` to typed async handlers
  (`on_insert/on_update/on_delete/on_default`, `dispatch`, `run`). Its `default`
  is `Option<Handler>`: with no `on_default` registered, unhandled events (unrouted
  DML + all non-DML control events) return `Ok(())` directly — no boxed-future
  allocation. Registered handlers still box one future per event; perf-critical
  consumers use `next_event`/`for_each_event`.
- `#[derive(WalTable)] #[wal(table = "...")]` (opt-in `derive` feature) + the
  `on_insert_of::<T>/on_update_of::<T>/on_delete_of::<T>` router methods infer the
  table from `T::TABLE`. Derive-using tests are `#[cfg(all(test, feature = "derive"))]`.
- `next_event_before(C)` (also `EventStream::next_event_before`) returns `None` before the
  first transaction committed at or past an `AlignedSnapshot`'s consistent point. It holds
  that commit's raw frame (peeked by `frame_commit_lsn`, parser untouched) for the next read,
  or stops on a keepalive `wal_end >= C`. Reconnects and `start()` drop the held frame.

## Hot Path

The performance-critical path is: `get_copy_data_async` → `process_wal_message` → `parse_wal_message_bytes` → `parse_insert/update/delete_message` → `parse_tuple_data`.

Key optimizations in place:
- Zero-copy `Bytes` throughout (no memcpy on WAL data)
- libpq COPY ingress copies each message into a reusable `BytesMut` read buffer
  (`put_slice` + `split().freeze()`), then `PQfreemem`s libpq's allocation
  immediately; the backing allocation is reused after warmup (one memcpy into
  cache-hot memory, bounded peak RSS). A `from_owner` zero-copy variant was
  benchmarked and reverted — no measurable win outside noise.
- `SmallVec<[ColumnData; 16]>` avoids heap alloc for ≤16 columns
- `RelationMap` is a `BTreeMap<Oid, RelationInfo>` — hit once per relation, not per row
- Cache-padded atomics in SharedLsnFeedback (no false sharing); `update_applied_lsn`
  folds the implicit-flush bump into a single `fetch_max` (Relaxed CAS seed)
- SIMD-accelerated `memchr` for null-terminated string scanning
- `Arc<str>` for namespace/relation_name (parsed directly, no String intermediate)
- Feedback syscall throttling (Instant::now() every 128 events)
- No debug! logging in DML hot-path parsers

## Coding Conventions

- Use `#[inline]` on methods called per-message in the hot path
- Use `#[inline(always)]` only for trivial accessors (1-2 instructions)
- Use `#[cold] #[inline(never)]` for error-path constructors
- Prefer `Bytes` over `Vec<u8>` for data that flows through the pipeline
- Use `Arc<str>` for column/table names (shared across rows)
- No `debug!` logging in DML hot-path parsers (BEGIN/INSERT/UPDATE/DELETE/COMMIT)
- `BufferWriter` write methods are infallible (return `()`, not `Result`)

## Initial Snapshot

`LogicalReplicationStream::snapshot()` copies published tables through the slot's
exported snapshot, then hands off to the stream. Three invariants:

- **The stream is moved into the handle** — not because `start()` would clear the
  exported snapshot (the reader imports it into its own `REPEATABLE READ READ ONLY`
  transaction before any handle exists), but because `START_REPLICATION` pins the
  replication connection in CopyBoth and the handoff owns the LSN bookkeeping:
  `finish()` demands a fully consumed snapshot so the following `start(None)`
  resumes at `consistent_point` with no gap. Taking `self` by value makes a
  mid-snapshot `start()` unrepresentable. (The manual `exported_snapshot_name()`
  path *is* still exposed to the server-side clearing — nothing has imported the
  export there.)
- **Failure consumes the handle** — the snapshot is `REPEATABLE READ`; resuming
  against an expired one silently mixes two points in time.
- **COPY TEXT, not BINARY** — `ColumnValue::Binary` is rejected by every scalar
  path in `deserializer.rs`; only TEXT matches pgoutput byte-for-byte.

`connection/native/copy.rs` cannot be reused: it treats `CopyDone` (a COPY's
*success* signal) as `TransientConnection` and discards the trailing
`CommandComplete`/`ReadyForQuery`, desyncing the connection. Hence `copy_out.rs`.

## Benchmarks

CodSpeed builds with `--no-default-features --features std`, under which
`lib.rs` excludes `stream`, `router`, `connection`, `retry`, `snapshot` and
`copy_text` — only the parser and deserializer are benchmarked.

**Never add a `[profile]` section to `Cargo.toml`.** Pinning
`[profile.bench] codegen-units = 1` previously slowed `parse_*` by up to 130% and
read as a CodSpeed regression.

## CI Requirements

- `cargo fmt` clean
- All three clippy configurations clean (default+derive, libpq, no_std)
- Unit tests pass on default, `derive` and `libpq`
- Code coverage ≥ 90% total lines, via `cargo llvm-cov --lib --features derive`
- `cargo doc --no-deps --all-features` warning-free
- Loom models of `SharedLsnFeedback` pass (`loom` job in `ci-checks.yml`); cargo-mutants runs weekly, not per PR
- Integration tests require PostgreSQL 15+ with logical replication enabled
- The 90% gate reads only the default-backend unit pass, so `src/connection/libpq.rs` is invisible to it. Codecov merges both backends' unit passes with the integration suites run on PostgreSQL 18.
- A new `integration-tests/*.rs` file needs a `[[test]]` stanza in `Cargo.toml`

## Integration Tests

Integration tests in `integration-tests/` require a running PostgreSQL instance with `wal_level = logical`. They are NOT run during normal development — only in CI with the `--ignored` flag. Do not modify integration tests unless specifically working on connection/streaming features.
