# AGENTS.md — pg-walstream

Guidance for AI coding agents working in this repository. `.claude/CLAUDE.md`
holds the same guidance for Claude Code; keep the two in sync when either changes.

pg-walstream is a Rust library that parses the PostgreSQL logical/physical
replication protocol and streams WAL (CDC) to any destination. The core is a
zero-copy, allocation-conscious hot path; a thin ergonomics layer sits on top.

## Build & Test

```bash
cargo build                                              # default: rustls-tls backend
cargo build --no-default-features --features libpq       # opt-in libpq FFI backend
cargo test --lib                                         # unit tests (no PostgreSQL)
cargo test --lib --features derive                       # + derive-macro layer
cargo test --lib --no-default-features --features libpq
cargo bench --bench wal_pipeline                         # parsing pipeline benches
cargo fmt --all -- --check                               # format check

# Lint — CI runs all three; `--all-targets` alone does NOT match it.
cargo clippy --workspace --all-targets --features derive -- -D warnings
cargo clippy --no-default-features --features libpq --all-targets -- -D warnings
cargo clippy --no-default-features --lib -- -D warnings          # no_std

# Coverage — `--features derive` is required or your number won't match CI.
cargo llvm-cov --lib --features derive --summary-only
```

`make before-git-push` does **not** match CI: its clippy line is commented out
and it has no coverage or bench target. Use the commands above.

Check results by **exit code**, not by grepping for `error` — cargo indents
diagnostics, so `grep '^error'` silently reports success on a failing run.

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
├── copy_text.rs     # COPY TEXT decoder (crate-private; snapshot only)
├── snapshot/        # managed initial snapshot — see below
│   ├── plan.rs      #   catalog query, COPY SQL, reader conninfo (all pure)
│   ├── rows.rs      #   SnapshotRows: the one implementation
│   └── events.rs    #   SnapshotEvents: ChangeEvent adapter + EventSource impl
└── connection/      # native/ rustls-tls backend (default) + libpq FFI backend (opt-in)
    └── native/copy_out.rs   # CopyOut state machine (NOT copy.rs — see below)
macros/              # #[derive(WalTable)] (opt-in `derive` feature)
```

## Initial snapshot

`LogicalReplicationStream::snapshot()` copies the published tables through the
slot's exported snapshot, then hands off to the stream. Three invariants an
agent must not "simplify" away:

- **The stream is moved into the handle.** Every replication command runs
  `SnapBuildClearExportedSnapshot` server-side, so `start()` mid-snapshot would
  destroy the snapshot being read. Taking `self` by value makes that
  unrepresentable.
- **Failure consumes the handle.** The exported snapshot is `REPEATABLE READ`;
  resuming against an expired one silently mixes two points in time. `run()`
  returns the stream only on `Ok`; the pull API latches a poison flag.
- **COPY TEXT, not BINARY.** `ColumnValue::Binary` is rejected by every scalar
  path in `deserializer.rs`. Only TEXT yields bytes identical to pgoutput's, which
  is what lets one handler serve both phases.

`connection/native/copy.rs` is **not** reusable for this: it reports `CopyDone`
as `TransientConnection` (for a COPY that means *success*) and its catch-all arm
discards the trailing `CommandComplete`/`ReadyForQuery`, desyncing the
connection. Hence the separate `copy_out.rs`.

Snapshot events are plain `ChangeEvent::insert`s stamped with the slot's
consistent point; nothing on `ChangeEvent` marks them. Which phase you are in is
told by the call site, because the type-state already forces the phases apart.

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
- `RelationMap` is a `BTreeMap<Oid, RelationInfo>` — small, cache-friendly, and
  hit once per relation, not once per row.
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

`EventType`, `ChangeEvent`, `ReplicationError` and `ReplicationStreamConfig` all
have public fields and **no `#[non_exhaustive]`**, so adding a field or a variant
is source-breaking for downstream struct literals and exhaustive matches. Prefer
an approach that avoids it; when unavoidable, bump the minor version (0.x) and add
a README "Upgrading" note.

`ChangeEvent::encode`/`decode` are deprecated since 0.9.0 in favour of a binary
serde codec; do not extend them.

## Benchmarks and the perf gate

CodSpeed runs on every PR in `simulation` mode, building with
`cargo codspeed build --no-default-features --features std`. Under that cfg
`lib.rs` excludes `stream`, `router`, `connection`, `retry`, `snapshot` and
`copy_text` entirely — only the parser and deserializer are benchmarked.

**Never add a `[profile]` section to `Cargo.toml`.** Pinning
`[profile.bench] codegen-units = 1` previously slowed `parse_*` by up to 130% and
read as a CodSpeed regression. If a `parse_*` regression appears, check
`git diff Cargo.toml` before investigating the parser.

## CI Requirements

`cargo fmt` clean · all three clippy configurations clean · `cargo test --lib`
passes on default, `derive` and `libpq` · coverage ≥ 90% total lines
(`cargo llvm-cov --lib --features derive`) · `cargo doc` warning-free.

`src/connection/libpq.rs` is **not compiled** during the coverage run
(`connection/mod.rs` gates it on `libpq` *without* `rustls-tls`), so code added
there is invisible to the gate — it still has to pass the libpq clippy run and be
covered by integration tests.

A new file under `integration-tests/` needs a matching `[[test]]` stanza in
`Cargo.toml`; CI enumerates the directory and fails without it.
