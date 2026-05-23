# pg-walstream Development Guide

## Build & Test Commands

```bash
# Build (default libpq backend)
cargo build

# Build (pure-Rust rustls-tls backend)
cargo build --no-default-features --features rustls-tls

# Run unit tests (no PostgreSQL required)
cargo test --lib

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
```

## Hot Path

The performance-critical path is: `get_copy_data_async` → `process_wal_message` → `parse_wal_message_bytes` → `parse_insert/update/delete_message` → `parse_tuple_data`.

Key optimizations in place:
- Zero-copy `Bytes` throughout (no memcpy on WAL data)
- `SmallVec<[ColumnData; 16]>` avoids heap alloc for ≤16 columns
- Identity hasher for OID-keyed RelationMap (no SipHash)
- Cache-padded atomics in SharedLsnFeedback (no false sharing)
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
