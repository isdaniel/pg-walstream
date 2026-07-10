# AGENTS.md — pg-walstream

Guidance for AI coding agents working in this repository. `.claude/CLAUDE.md`
holds the same guidance for Claude Code; keep the two in sync when either changes.

pg-walstream is a Rust library that parses the PostgreSQL logical/physical
replication protocol and streams WAL (CDC) to any destination. The core is a
zero-copy, allocation-conscious hot path; a thin ergonomics layer sits on top.

## Build & Test

```bash
cargo build                                              # default: libpq backend
cargo build --no-default-features --features rustls-tls  # pure-Rust TLS backend
cargo test --lib                                         # unit tests (no PostgreSQL)
cargo test --lib --features derive                       # + derive-macro layer
cargo test --lib --no-default-features --features rustls-tls
cargo bench --bench wal_pipeline                         # parsing pipeline benches
cargo fmt --all -- --check                               # format check
cargo clippy --all-targets                               # lint
make before-git-push                                     # check+build+fmt+audit+test+doc-check
```

Integration tests (`integration-tests/`) are `#[ignore]` and need a live
PostgreSQL 15+ with `wal_level = logical`; they only run in CI (`--ignored`).
Do not edit them unless working on connection/streaming behavior.

## Architecture

```
src/
├── buffer.rs        # zero-copy BufferReader/BufferWriter (bytes crate)
├── protocol.rs      # WAL message parser (hot path); RelationInfo, ReplicationState
├── stream.rs        # LogicalReplicationStream + EventStream + config builder
│                    #   next_raw_event → RawXLogData (undecoded pgoutput, BYO decoder)
├── router.rs        # WalRouter: typed async by-table event router (convenience)
├── deserializer.rs  # serde Deserializer: RowData → user structs
├── column_value.rs  # ColumnValue / RowData
├── types.rs         # EventType/ChangeEvent, Lsn, wire encode/decode
├── lsn.rs           # thread-safe SharedLsnFeedback (atomic CAS)
└── connection/      # libpq FFI backend (default) + native/ rustls-tls backend
macros/              # #[derive(WalTable)] (opt-in `derive` feature)
```

## Hot Path

Critical path: `get_copy_data_async` → `process_wal_message` →
`parse_wal_message_bytes` → `parse_insert/update/delete_message` →
`parse_tuple_data`. Optimizations in place:

- Zero-copy `Bytes` throughout — no memcpy on WAL data.
- libpq COPY ingress copies each message into a reusable `BytesMut` read buffer
  (`put_slice` + `split().freeze()`), then `PQfreemem`s libpq's allocation
  immediately. The backing allocation is reused after warmup, so the per-message
  cost is one memcpy into cache-hot memory with a bounded peak RSS. (A `from_owner`
  zero-copy variant was benchmarked against Azure PG and reverted — no measurable
  throughput/CPU win outside noise; see git history.)
- `SmallVec<[ColumnData; 16]>` avoids heap alloc for ≤16 columns.
- Identity hasher for the OID-keyed RelationMap (no SipHash).
- Cache-padded atomics in `SharedLsnFeedback`; `update_applied_lsn` folds the
  implicit-flush bump into one `fetch_max` (with a Relaxed CAS seed load).
- `WalRouter.default` is `Option<Handler>`: with no `on_default`, unhandled events
  return `Ok(())` with no boxed-future allocation.
- SIMD `memchr` for C-string scanning; `Arc<str>` for namespace/relation names;
  feedback syscall throttling (`Instant::now()` every 128 events); no `debug!` in
  DML parsers.

## Coding Conventions

- `#[inline]` on per-message hot-path methods; `#[inline(always)]` only for
  trivial 1–2 instruction accessors; `#[cold] #[inline(never)]` for error-path
  constructors.
- Prefer `Bytes` over `Vec<u8>` for pipeline data; `Arc<str>` for shared
  column/table names.
- `BufferWriter` write methods are infallible (return `()`).
- No `debug!` logging in DML hot-path parsers (BEGIN/INSERT/UPDATE/DELETE/COMMIT).
- New `unsafe` (e.g. FFI buffer ownership) must carry a `// SAFETY:` note and a
  runnable test proving the invariant (see the `Send` impls in `connection/libpq.rs`).

## Public API stability

Changing public types (e.g. `EventType` fields, `ChangeEvent` accessors) or the
binary event wire format (`ChangeEvent::encode`/`decode`) is a breaking change —
bump the minor version (0.x) and add a README "Upgrading" note.

## CI Requirements

`cargo fmt` clean · `cargo clippy` clean · `cargo test --lib` passes · coverage
≥ 90% · `make before-git-push` green.
