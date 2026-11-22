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

# Loom: model-check SharedLsnFeedback. Backend-free feature set (tokio changes under
# `--cfg loom`) and the test filter are both required.
RUSTFLAGS='--cfg loom' cargo test --lib --release --no-default-features --features std lsn::loom_tests

# Mutation testing of src/lsn.rs (cargo install cargo-mutants; scope in .cargo/mutants.toml)
cargo mutants
```

`make before-git-push` runs check + `make lint` (all three clippy configurations)
+ build + fmt + audit + test + doc-check. It does **not** run coverage
(`make coverage`) or the benchmarks — run those separately before relying on the
numbers.

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
├── types.rs         # EventType/ChangeEvent, Lsn
├── lsn.rs           # thread-safe SharedLsnFeedback (atomic CAS)
├── copy_text.rs     # COPY TEXT decoder (crate-private; snapshot only)
├── snapshot/        # managed initial snapshot — see below
│   ├── plan.rs      #   catalog query, COPY SQL, reader conninfo (all pure)
│   ├── table_copy.rs #  TableCopy: the COPY row loop both handles share
│   ├── rows.rs      #   SnapshotRows: the slot-owning handle
│   ├── events.rs    #   SnapshotEvents: ChangeEvent adapter + EventSource impl
│   └── aligned.rs   #   AlignedSnapshot: tables added to a running stream (side slot)
└── connection/      # native/ rustls-tls backend (default) + libpq FFI backend (opt-in)
    ├── commands.rs          # replication commands both backends share (`replication_commands!`), `server_failure`
    ├── native/error.rs      # ErrorResponse parsing: `error_frame` (whole frame), `parse_error_fields` (payload)
    ├── native/wire.rs       # frame builders + `#[cfg(test)] testing` (frames shared by native tests)
    └── native/copy_out.rs   # CopyOut state machine (NOT copy.rs — see below)
macros/              # #[derive(WalTable)] (opt-in `derive` feature)
integration-tests/   # `#[ignore]` suites; `common/mod.rs` = helpers they share (not a test target)
```

## Shared seams

Logic that both backends, or many tests, need lives in one place. Change it there;
do not add a second copy.

- **Replication commands.** `replication_commands!` (`src/connection/commands.rs`)
  defines `identify_system`, `create_/alter_/drop_/read_replication_slot`,
  `send_standby_status_update` and `send_hot_standby_feedback` (plus `_async`
  forms) once. It is expanded *inside each backend's inherent impl*
  (`connection/libpq.rs`, `connection/native/connection.rs`), so there is no trait
  to import and no per-backend copy to keep in sync. A backend must provide `exec`,
  `exec_async`, `server_version`, `put_copy_data` and a `copy` field with
  `is_replication()`; the macro supplies `ensure_replication_mode`.
  `start_replication`, `start_physical_replication`, `base_backup` and
  `upload_manifest` stay per backend (they differ in how COPY mode is entered).
- **Server errors.** Build the error for a rejected result with
  `commands::server_failure(server_error, message, context)`. In the native backend
  parse an `E`/`N` frame with `error_frame(&msg)` (`native/error.rs`), never
  `parse_error_fields(&msg[5..])`. The `ReplicationError` variant chosen by
  `from_server` decides reconnect vs fatal, so a hand-rolled `format!` that
  bypasses these can silently change recovery behavior.
- **Connection strings (native).** URI and `key=value` both feed `RawOpts::set`,
  and `RawOpts::finish` applies the defaults. A new libpq option is one `set` arm
  plus its default in `finish`, never a branch in one syntax only. Keys not handled
  go through `check_unhandled`; keep `IGNORED_OPTIONS` / `UNSUPPORTED_OPTIONS`
  current (see the maintenance contract above them in `conninfo.rs`).
- **SQL text.** In `sql_builder.rs`: `push_literal` (an optional replication-grammar
  literal), `version_preflight::require` (a "needs PostgreSQL N+" gate) and
  `quote_qualified` (`"ns"."name"`). Replication-command literals use
  `quote_replication_literal` (no `E''`, no backslash escapes), not `quote_literal`.
- **serde.** `deserializer.rs` generates the policy-independent `Deserializer`
  methods for both Strict and Lenient with `shared_deserializer_methods!` (the
  strict impl passes `#[inline]`, the lenient one does not). Add a method there, not
  in each impl.
- **Native worker.** `run_worker`'s idle loop starts `StreamCopy` and
  `CopyOutStream` itself and hands every other `Command` to `handle_stream_cmd`
  through a catch-all arm. A new `Command` that starts a stream needs its own arm
  *before* that catch-all; the compiler will not flag the omission.
- **Test helpers.** Native protocol tests build frames with
  `wire::testing::{frame, build_error_response, discard_request}`; `stream.rs`
  tests use `MockSource`, `test_stream()` and `options_config()`. Prefer a new row
  in an existing table-driven test over a new `#[test]` that repeats the setup.
- **Integration-test helpers.** Every suite starts with `mod common;`, which is
  `integration-tests/common/mod.rs`: `replication_conn_string`,
  `regular_conn_string` / `regular_conn`, `strip_replication_param`, `SlotGuard` /
  `WaitingSlotGuard` / `UnwindSafeSlotGuard`, `drop_slot_best_effort`,
  `ca_cert_path`. Use them; do not paste a private copy into a suite.

## Initial snapshot

`LogicalReplicationStream::snapshot()` copies the published tables through the
slot's exported snapshot, then hands off to the stream. Three invariants an
agent must not "simplify" away:

- **The stream is moved into the handle.** Not because `start()` would clear the
  exported snapshot: the reader imports it into its own `REPEATABLE READ READ
  ONLY` transaction before any handle exists, so the copy no longer depends on
  the exporting transaction. It is moved because `START_REPLICATION` pins the
  replication connection in CopyBoth, and because the handoff owns the LSN
  bookkeeping — `finish()` demands a fully consumed snapshot so the following
  `start(None)` resumes at `consistent_point` with no gap. Taking `self` by value
  makes a mid-snapshot `start()` unrepresentable. (The manual
  `exported_snapshot_name()` path *is* exposed to the server-side clearing.)
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

`EventType`, `ChangeEvent` and `ReplicationError` have public fields/variants and
**no `#[non_exhaustive]`**, so adding a field or a variant is source-breaking for
downstream struct literals and exhaustive matches. Prefer an approach that avoids
it; when unavoidable, bump the minor version (0.x) and add a README "Upgrading"
note.

`ReplicationStreamConfig` is **`#[non_exhaustive]`** (fields still public, so
reads and `with_*` mutation work; literal construction does not). Adding a field
to it is therefore not source-breaking — do not "fix" the attribute away for
consistency with the three above. New public structs get the same treatment —
`SnapshotRow` is `#[non_exhaustive]` for this reason. The documented exception is
`SnapshotOutcome`, an enum that deliberately omits it; the reasoning is on the
type itself in `src/snapshot/mod.rs`, so do not add it there for consistency
either.

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
(`cargo llvm-cov --lib --features derive`) · `cargo doc` warning-free · loom
models of `SharedLsnFeedback` pass (`loom` job in `ci-checks.yml`; cargo-mutants
runs weekly, not per PR).

The 90% gate reads only that default-backend unit pass, so `src/connection/libpq.rs` (gated on `libpq` *without* `rustls-tls` in `connection/mod.rs`) is invisible to it. The shared `commands.rs` is not. Codecov receives the default and libpq unit passes merged, plus the integration suites run instrumented on PostgreSQL 18, and its patch check covers both backends.

A new top-level `integration-tests/*.rs` file needs a matching `[[test]]` stanza in
`Cargo.toml`; CI enumerates those files and fails without it. `common/mod.rs` is a
helper module included with `mod common;`, not a test target, so it needs none.
