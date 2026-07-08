# pg-walstream Development Guide

## Build & Test Commands

```bash
# Build (default libpq backend)
cargo build

# Build (pure-Rust rustls-tls backend)
cargo build --no-default-features --features rustls-tls

# Run unit tests (no PostgreSQL required)
cargo test --lib

# Run unit tests including the derive-macro layer
cargo test --lib --features derive

# Run benchmarks
cargo bench --bench wal_pipeline

# Format check
cargo fmt --all -- --check

# Lint
cargo clippy --all-targets

# Coverage (requires cargo-llvm-cov)
cargo llvm-cov --lib --lcov --output-path lcov.info
```

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
├── sql_builder.rs   # SQL statement builders (CREATE SLOT, etc.)
├── retry.rs         # Exponential backoff retry logic
└── connection/      # PostgreSQL connection backends
    ├── mod.rs
    ├── libpq.rs     # libpq FFI backend (default)
    └── native/      # Pure-Rust rustls-tls backend

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
- Identity hasher for OID-keyed RelationMap (no SipHash)
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

## CI Requirements

- `cargo fmt` clean
- `cargo clippy` clean
- All unit tests pass (`cargo test --lib`)
- Code coverage ≥ 90% (checked in CI)
- Integration tests require PostgreSQL 15+ with logical replication enabled

## Integration Tests

Integration tests in `integration-tests/` require a running PostgreSQL instance with `wal_level = logical`. They are NOT run during normal development — only in CI with the `--ignored` flag. Do not modify integration tests unless specifically working on connection/streaming features.
