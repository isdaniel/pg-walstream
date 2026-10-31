[![Crates.io Version](https://img.shields.io/crates/v/pg_walstream)](https://crates.io/crates/pg_walstream)
[![Crates.io Downloads (recent)](https://img.shields.io/crates/dr/pg_walstream)](https://crates.io/crates/pg_walstream)
[![Crates.io Total Downloads](https://img.shields.io/crates/d/pg_walstream)](https://crates.io/crates/pg_walstream)
[![docs.rs](https://img.shields.io/docsrs/pg_walstream)](https://docs.rs/pg-walstream)
[![codecov](https://codecov.io/gh/isdaniel/pg-walstream/graph/badge.svg?token=e0zmvpOWvz)](https://codecov.io/gh/isdaniel/pg-walstream)
[![CodSpeed](https://img.shields.io/endpoint?url=https://codspeed.io/badge.json)](https://app.codspeed.io/isdaniel/pg-walstream?utm_source=badge)

# pg-walstream

A high-performance Rust library for PostgreSQL logical and physical replication protocol parsing and streaming. Provides a robust, type-safe interface for consuming PostgreSQL Write-Ahead Log (WAL) streams.

## Features

- **Full Logical Replication Support**: Implements PostgreSQL logical replication protocol versions 1-4
- **Physical Replication Support**: Stream raw WAL data for standby servers and PITR
- **Base Backup Support**: Full `BASE_BACKUP` command with progress, compression, and manifest options
- **Pure-Rust Backend (default)**: The default `rustls-tls` backend needs no libpq and no OpenSSL, using `aws-lc-rs` for hardware-accelerated TLS (AES-NI, AVX2, SHA-NI). 
- **TLS/SSL Support**: All PostgreSQL SSL modes (`disable`, `allow`, `prefer`, `require`, `verify-ca`, `verify-full`)
- **Authentication**: Cleartext, MD5, and SCRAM-SHA-256 authentication methods
- **Streaming Transactions**: Support for streaming large transactions (protocol v2+). Each streamed change carries the xid of its subtransaction in `ChangeEvent::xid`, so a `StreamAbort` for a rolled-back savepoint names exactly the changes to drop, except a `Message`, which always carries the top-level xid
- **Two-Phase Commit**: Prepared transaction support (protocol v3+)
- **Parallel Streaming**: Multi-stream parallel replication (protocol v4+)
- **Zero-Copy Operations**: Efficient buffer management using the `bytes` crate with drain-loop batch queue optimization. The libpq backend copies each COPY message into a reusable `BytesMut` buffer, then reference-counts it downstream as `Bytes` (no per-message heap allocation after warmup)
- **Thread-Safe LSN Tracking**: Atomic LSN feedback for producer-consumer patterns
- **Connection Management**: Built-in connection handling with exponential backoff retry logic
- **Type-Safe API**: Strongly typed message parsing with comprehensive error handling
- **Typed Row Deserialization**: Built-in `serde` deserializer maps WAL rows directly into user-defined Rust structs (numerics, `bool`, `String`, `Option<T>`, enums, bytes)
- **High-Level Consumption Ergonomics**: `ReplicationStreamConfig::builder()`, an auto-acking `EventStream::for_each_event`, and a typed by-table `WalRouter` with an optional `#[derive(WalTable)]` layer (opt-in `derive` feature)
- **Managed Initial Snapshot**: `LogicalReplicationStream::snapshot()` copies the published tables through the slot's exported snapshot and hands off to the stream with no gap and no duplicate window. The ordering invariant is enforced by the type system, and snapshot rows arrive as ordinary `ChangeEvent`s, so one set of handlers (or one `WalRouter`) serves both the snapshot and the live stream
- **Aligned Snapshot**: `LogicalReplicationStream::capture_aligned` copies tables added while the stream runs, exact at a temporary side slot's consistent point `C`, and `next_event_before(C)` stops the running stream where that copy applies
- **Bounded Replay**: `ReplicationStreamConfig::with_stop_at_lsn` streams to a target LSN, delivers the crossing transaction in full, then ends cleanly with `ReplicationError::StreamStopped`
- **Raw XLogData Access**: `LogicalReplicationStream::next_raw_event` yields the undecoded pgoutput payload plus WAL positions (`RawXLogData`) for consumers that bring their own decoder — keepalives, feedback, and cancellation still handled, no auto-ack
- **Replication Slot Management**: Create, alter, read, and drop slots with full option support
- **Hot Standby Feedback**: Send hot standby feedback messages for physical replication

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
pg_walstream = "0.9"
```

By default, this uses the pure-Rust `rustls-tls` backend — no libpq and no OpenSSL, only `cmake` + a C compiler at build time (for `aws-lc-rs`).

To use the C **libpq** backend instead (bound by `pq-sys`; requires system libpq):

```toml
[dependencies]
pg_walstream = { version = "0.9", default-features = false, features = ["libpq"] }
```

If both backends are enabled, `rustls-tls` takes priority automatically.

## Unreleased: changes since 0.9.0
<!-- Rename to "Upgrading from 0.9" when the release version is chosen. -->

The next release changes the public API, so code written for 0.9 may not compile unchanged.

- **`ChangeEvent` has a new public field `xid: Option<u32>`.** Build events with `xid: None` and add `..` to struct patterns (`let ChangeEvent { event_type, lsn, .. } = event`). It is `Some` only for a `Relation`, `Type`, `Insert`, `Update`, `Delete`, `Truncate` or `Message` inside a streamed chunk.
- **`ChangeEvent::encode` and `ChangeEvent::decode` (deprecated since 0.9.0) are removed.** Serialize events with `serde_json`; JSON written by 0.9 (no `xid` key) still deserializes, with `xid` as `None`. Bytes written by `encode` are not JSON and cannot be read after the upgrade, so convert them on 0.9 first. bincode and postcard cannot read a `ColumnValue`.
- **`StreamingReplicationMessage::xid`, returned by `LogicalReplicationParser::parse_wal_message`, changes meaning inside a streamed chunk (protocol v2+).** For `Relation`, `Type`, `Insert`, `Update`, `Delete`, `Truncate` and `Message` it is now the xid pgoutput wrote before that change, which is a subtransaction's xid after a savepoint; it used to be the chunk's xid. A consumer that buffers rows by `xid` and flushes on `StreamCommit` must take the top-level xid from `StreamStart`, or it strands the savepoint's rows. A `Message` always carries the top-level xid.
- **The `libpq` feature now also builds `socket2`,** which only `rustls-tls` needed before.
- **`LogicalReplicationStream::new` now runs `IDENTIFY_SYSTEM`, which `start` ran before:** one more round trip at construction. A reply with no usable system identifier and timeline now fails `new` with `ReplicationError::PermanentConnection`, where `start` used to continue without it. `server_identity()` returns what it read.
- **`send_feedback()` before `start()` returns `ReplicationError::Protocol`;** it returned `Ok(())`. After `start()` and before the first message it sends an all-zero status update instead of returning early, and `stop()` of a stream that was never started logs a warning.
- **A cancelled command poisons the connection.** If the future of `start()`, `slot_confirmed_flush_lsn()` or `timeline_history()` is dropped before the reply arrives, every later command on that connection fails with `ReplicationError::PermanentConnection`; create a new stream.
- **`EventStream::next_event` carries a reconnect across a dropped future.** A `timeout` or `select!` that cancels `next_event()` while the stream reconnects no longer restarts the reconnect from its first attempt; the next call resumes it, so a timeout shorter than a reconnect cannot starve it. While such a reconnect is parked, `inner()`, `inner_mut()` and `current_lsn()` panic until the next `next_event()` returns (as they already do after an abandoned `poll_next`); the feedback methods keep working.

## Feature Flags

pg-walstream provides two connection backends plus a `std` toggle, all selected at compile time. `rustls-tls` is the default; `libpq` is opt-in. When both are enabled, `rustls-tls` takes priority:

| Feature | Default | C Dependencies | Description |
|---------|---------|----------------|-------------|
| `std` | Yes | None | Standard library support. Disable with `default-features = false` for a `no_std` plus `alloc` parser-only build (no connection layer) that compiles for `wasm32-unknown-unknown` and embedded targets. |
| `libpq` | No | `libpq-dev` + OpenSSL | Opt-in. PostgreSQL's C client library via FFI, bound by `pq-sys` (pre-generated bindings — no libclang). Battle-tested. Enable with `--no-default-features --features libpq`. Implies `std`. |
| `rustls-tls` | Yes | `cmake`, `gcc` (build-time only) | Default. Pure-Rust implementation using `rustls` with `aws-lc-rs` crypto backend for hardware-accelerated TLS. No libpq, no OpenSSL, no runtime C dependencies. Takes priority when both backends are enabled. Implies `std`. |
| `derive` | No | None | Opt-in proc-macros (pull `syn`/`quote`) that bind a struct to a table — `#[derive(WalTable)] #[wal(table = "...")]` or the one-line attribute form `#[wal_table("...")]` — enabling the `WalRouter::on_insert_of::<T>` / `on_update_of::<T>` / `on_delete_of::<T>` table-inference methods. |

> **Note:** The protocol parser, encoder, and types need no backend. Building with `default-features = false` gives a `no_std` plus `alloc` build of just those, suitable for wasm and embedded. A connection backend (`libpq` or `rustls-tls`) is required only for the live streaming and connection APIs, and pulls in `std`.

## System Dependencies

System dependencies are **only required** for the opt-in `libpq` feature. The default `rustls-tls` backend requires only `cmake` and a C compiler at build time (for the `aws-lc-rs` crypto library), with no runtime dependencies.

### For `libpq` feature (opt-in)

**Ubuntu/Debian:**
```bash
sudo apt-get install libpq-dev libssl-dev
```

**CentOS/RHEL/Fedora:**
```bash
sudo yum install postgresql-devel
# or
sudo dnf install postgresql-devel
```

### For `rustls-tls` feature

Requires `cmake` and a C compiler at build time for `aws-lc-rs` (hardware-accelerated cryptography):

**Ubuntu/Debian:**

```bash
sudo apt-get install cmake gcc
```

Then add to `Cargo.toml`:

```toml
pg_walstream = { version = "0.9", features = ["rustls-tls"] }
```

#### TLS trust store

When `sslmode` is `verify-ca` or `verify-full`, the `rustls-tls` backend builds its root certificate store as follows:

1. If `sslrootcert` is set, it loads **only** those CAs from the PEM file (exclusive).
2. Otherwise, it loads the [Mozilla CA bundle](https://wiki.mozilla.org/CA/Included_Certificates) shipped via `webpki-roots`.

The OS trust store is **not** consulted. If your PostgreSQL server is signed by a corporate/internal CA that is only present in the OS trust store (e.g. `/etc/ssl/certs`), you must point `sslrootcert` at that CA explicitly — for example:

```text
postgresql://user:pass@host/db?sslmode=verify-full&sslrootcert=/etc/ssl/certs/corporate-ca.pem
```

## Quick Start

The [`examples/`](examples/) directory contains runnable examples demonstrating various usage patterns:

| Example | Description |
|---------|-------------|
| [`basic-streaming`](examples/basic-streaming) | High-level `futures::Stream` API with stream combinators (`filter`, `take_while`) |
| [`initial-snapshot`](examples/initial-snapshot) | Copy existing rows, then stream changes with no gap and no duplicate — `stream.snapshot()` |
| [`polling`](examples/polling) | Manual polling loop using `next_event()` for custom integration scenarios |
| [`safe-transaction-consumer`](examples/safe-transaction-consumer) | Production-grade transaction-aware CDC consumer with ordered commits and safe LSN feedback |
| [`rate-limited-streaming`](examples/rate-limited-streaming) | Rate-limited consumption using `tokio_stream::StreamExt::throttle` |
| [`tokio-spawn-streaming`](examples/tokio-spawn-streaming) | Producer/consumer pattern via `tokio::spawn` with `mpsc` channel (demonstrates `Send` safety) |
| [`typed-deserialization`](examples/typed-deserialization) | Map INSERT/UPDATE/DELETE events directly into user-defined Rust structs via `serde` |
| [`derive-router`](examples/derive-router) | `#[derive(WalTable)]` + `WalRouter` table-inference (`on_*_of::<T>`) — the `derive` feature |
| [`pg-basebackup`](examples/pg-basebackup) | Full physical backup tool using `BASE_BACKUP` with tar extraction and progress reporting |
| [`binary-column-access`](examples/binary-column-access) | Lossless zero-copy `BYTEA` access via `PgResult::get_bytes`, contrasted with the lossy `get_value` |
| [`raw-xlogdata`](examples/raw-xlogdata) | Undecoded pgoutput payload via `next_raw_event()` — bring your own decoder |
| [`arbitrary-fuzzing`](examples/arbitrary-fuzzing) | Property-based fuzzing of all protocol types using the `arbitrary` crate |

For more control, you can use the traditional polling approach:

```rust
use pg_walstream::{
    LogicalReplicationStream, ReplicationStreamConfig, RetryConfig, StreamingMode,
    SharedLsnFeedback, CancellationToken,
};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ReplicationStreamConfig::new(
        "my_slot".to_string(),
        "my_publication".to_string(),
        2, StreamingMode::On,
        Duration::from_secs(10),
        Duration::from_secs(30),
        Duration::from_secs(60),
        RetryConfig::default(),
    );

    let mut stream = LogicalReplicationStream::new(
        "postgresql://postgres:password@localhost:5432/mydb?replication=database",
        config,
    ).await?;

    stream.start(None).await?;

    let cancel_token = CancellationToken::new();

    // Traditional polling loop with automatic retry
    loop {
        match stream.next_event_with_retry(&cancel_token).await {
            Ok(event) => {
                println!("Received event: {:?}", event);
                stream.shared_lsn_feedback.update_applied_lsn(event.lsn.value());
            }
            Err(e) if matches!(e, pg_walstream::ReplicationError::Cancelled(_)) => {
                println!("Cancelled, shutting down gracefully");
                break;
            }
            Err(e) => {
                eprintln!("Error: {}", e);
                break;
            }
        }
    }

    Ok(())
}
```

## Mapping Columns to Struct Fields

Rows are keyed by their real PostgreSQL column names. When a struct field name
differs from its column, use serde's `#[serde(rename = "...")]` — no extra
attribute is needed:

```rust
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct User {
    id: i64,
    #[serde(rename = "user_name")] // field `username` ← column `user_name`
    username: String,
    #[serde(rename = "mail")]      // field `email`    ← column `mail`
    email: Option<String>,         // nullable column → Option
}

// let user: User = event.deserialize_insert()?;   // or row.deserialize_into()?
```

With the `derive` feature, add `#[wal_table("...")]` (outermost attribute) to bind the type to its table for `WalRouter`; it composes with the renames above:

```rust,ignore
#[wal_table("typed_deser_users")]
#[derive(Debug, Deserialize)]
struct User { /* fields as above */ }
```

## LSN Tracking

Thread-safe LSN tracking for feedback to PostgreSQL:

```rust
use pg_walstream::SharedLsnFeedback;
use std::sync::Arc;

let feedback = SharedLsnFeedback::new_shared();

// Producer thread: read LSN from feedback
let (flushed_lsn, applied_lsn) = feedback.get_feedback_lsn();

// Consumer thread: update LSN after processing
feedback.update_applied_lsn(commit_lsn);
```

## Initial Snapshot

Streaming alone tells you what *changed*, never what was already there. Enable a managed initial snapshot and the library copies the published tables through the replication slot's exported snapshot, then hands off to the stream:

```rust
use pg_walstream::SnapshotOutcome;
use std::sync::Arc;

let config = ReplicationStreamConfig::builder("my_slot", "my_publication");

let stream = LogicalReplicationStream::new(conn_str, config).await?;
let sink = Arc::new(sink);

let mut stream = match stream.snapshot().await? {
    // The slot already existed, so there is nothing to copy — the normal
    // "resume an existing subscription" path. Not an error.
    SnapshotOutcome::Unavailable(stream) => stream,

    SnapshotOutcome::Available(snapshot) => {
        println!("copying {} table(s) at {}", snapshot.tables().len(), snapshot.consistent_point());
        snapshot
            .events()
            .run(|event| {
                let sink = Arc::clone(&sink);
                async move { sink.apply(event).await }
            })
            .await?
    }
};

// Resumes exactly at the snapshot's consistent point.
stream.start(None).await?;
```

`snapshot()` sets `SNAPSHOT 'export'` on the slot itself — there is no flag to remember and no builder-ordering rule to get wrong.

Snapshot rows arrive as ordinary `ChangeEvent::Insert`s carrying the slot's
consistent point as their LSN, so an existing `WalRouter` works across both
phases with the same handlers:

```rust
let mut router = WalRouter::new();
router.on_insert_of::<User, _>(|user| async move { upsert(user).await });

let mut stream = match stream.snapshot().await? {
    SnapshotOutcome::Unavailable(stream) => stream,
    SnapshotOutcome::Available(snapshot) => {
        let mut events = snapshot.events();
        router.run_snapshot(&mut events).await?;   // snapshot phase
        events.finish().await?
    }
};

stream.start(None).await?;
router.run(&mut stream.into_stream(token)).await?;  // live phase, same handlers
```

> Use `snapshot.rows()` instead of `.events()` to iterate `SnapshotRow`s (`.data`, `.relation`, `.lsn`) without the `ChangeEvent` wrapper. To copy only part of a large publication, bind the snapshot as `mut` and call `snapshot.retain_tables(..)` before `.events()`/`.rows()`.

A complete, runnable version of the above — including a row inserted *during* the handoff window to prove it arrives exactly once — is in [`examples/initial-snapshot`](examples/initial-snapshot).
> **A failed snapshot cannot be resumed.** The exported snapshot is `REPEATABLE READ`; continuing against an expired one would silently mix rows from two points in time. You own the retry policy; the library owns the retry *start point*, which is always the beginning. A failed or dropped snapshot handle makes a best-effort attempt to drop its slot — a failure to drop is logged, not returned — so a retry normally gets a fresh export instead of finding the slot present and silently proceeding with no baseline.

> **Holding a snapshot handle blocks `VACUUM`.** The handle keeps a `REPEATABLE READ` transaction open, pinning the database's `xmin` — no dead tuple newer than the snapshot can be reclaimed, across the whole database, for as long as you hold it. Copy promptly.

### Tables added while the stream runs

`capture_aligned` copies extra tables through a temporary side slot's exported snapshot, exact at its consistent point `C`. Commit the `ALTER PUBLICATION ... ADD TABLE` first. A table added while the capture waits can lose a row written before the `ALTER` committed.

The walsender drops a stream that sends no status update for `wal_sender_timeout`, so keep calling `send_feedback()` while the capture waits and while the copy loads. The capture future borrows nothing from the stream. Do not read the stream until the capture returns, because a read can pass `C`, which `next_event_before` cannot undo:

```rust
let capture = stream.capture_aligned(&[("public", "orders")]);
tokio::pin!(capture);
let mut tick = tokio::time::interval(Duration::from_secs(1));
let mut copy = loop {
    tokio::select! {
        copy = &mut capture => break copy?,
        _ = tick.tick() => stream.send_feedback().await?,
    }
};
let c = copy.consistent_point();
while let Some(event) = stream.next_event_before(c, &cancel).await? {
    let lsn = event.lsn;
    // per transaction, decided at its commit event; true once that transaction is applied
    if sink.buffer(event).await? {
        stream.shared_lsn_feedback.update_applied_lsn(lsn);
    }
}
let mut fed = std::time::Instant::now();
while let Some(row) = copy.next_row().await? {
    sink.load(row).await?;
    if fed.elapsed() >= Duration::from_secs(1) {
        stream.send_feedback().await?;
        fed = std::time::Instant::now();
    }
}
copy.finish()?;
// stream.next_event(&cancel) continues at the first transaction committed at or past c.
```

`next_event_before(C)` returns `None` once every transaction committed below `C` has been delivered. Decide each transaction at its commit event. Below `C`, apply it without the copied tables' rows, which the copy already holds. At or past `C`, apply it in full, after the copy. A transaction that ends in `RollbackPrepared` or in a `StreamAbort` of the whole transaction is dropped. A `StreamAbort` of a subtransaction drops the buffered changes whose `ChangeEvent::xid` equals its `subtransaction_xid`, except a `Message`, which always carries the top-level xid. Streamed chunks, `BeginPrepare` through `Prepare`, and `StreamPrepare` can arrive before the `None` for a transaction that commits at or past `C`, so buffer each transaction until its commit event. `EventStream::next_event_before` does the same with retries.

Acknowledge each transaction you decided below `C` (`update_applied_lsn` with its commit event's `lsn`, or `EventStream::update_applied_lsn`) before the copy loads. A reconnect replays from the acknowledged position, so without it every transaction delivered before the `None` arrives again after the copy and is applied twice: 50 of 50 transactions in a measured idle-database run, none with the acknowledgement. `EventStream::next_event_before` reconnects without telling you, so also start a buffered transaction over when a `Begin`, a `BeginPrepare` or a `StreamStart` with `first_segment` arrives for one you already hold. That replay of a streamed transaction was not reproduced, so treat it as a precaution.

The capture holds a second replication connection and a temporary slot until slot creation ends. `capture_aligned` fails with a permanent error, before creating the slot, if that connection reaches another cluster or timeline than the stream's, which a connection string with several hosts can cause. Slot creation waits for the transactions open when it starts and can also wait for ones that start during the wait, a prepared transaction left open included. `capture` has no deadline of its own: end the blocker, or cancel the walsender from a second connection with `SELECT pg_cancel_backend(active_pid) FROM pg_replication_slots WHERE slot_name LIKE 'pg_walstream_aligned_%'`. Dropping the capture future closes its connections, but a slot creation already sent keeps its walsender waiting on the server, so abandoned captures behind a long transaction can exhaust `max_wal_senders`. `with_cancellation` covers only the copy loop, and `next_row()` is not cancel-safe where a table's `COPY` starts, so do not race it in a `select!` that can drop it.

The copy pins `xmin` until `finish()`, and it holds an `ACCESS SHARE` lock on each copied table from `capture` on. Reads and writes go on, but a `TRUNCATE`, `ALTER TABLE`, `DROP`, `VACUUM FULL` or `CLUSTER` on those tables waits until `finish()` or drop, and writes issued behind a waiting one wait with it (an `ALTER TABLE` waited 2.4 s, an `INSERT` behind it failed with `55P03` at its 2 s `lock_timeout`). Give that DDL a `lock_timeout`, or run it before the capture or after the copy. The lock is what keeps the copy exact: a `COPY` reads the table's current file, so a `TRUNCATE` or rewriting `ALTER TABLE` committed after `C` would otherwise return it empty. If such a command is in flight when `capture` asks for its lock, or committed after `C`, `capture` fails and you capture again.

## Connection Timeouts

Three conninfo options bound three different things, and they are easy to mistake for one another:

| Option | Unit | Bounds |
|---|---|---|
| `connect_timeout` | seconds | Only the TCP connect. On the native backend TLS negotiation and startup/auth are **not** covered; libpq bounds the whole handshake with each address, then tries the next host. |
| `keepalives_idle` / `_interval` / `_count` | seconds | An **idle** connection. Defaults `120`/`10`/`3` — roughly 150 s to notice a peer that vanished while nothing was in flight. |
| `tcp_user_timeout` | **milliseconds** | An unacknowledged **write**. This is the only one that bounds a blocking round-trip — including the `DROP_REPLICATION_SLOT` that snapshot cleanup runs from `Drop`. Linux-family only; `0` (the default) disables it. |

If a stuck destructor or a wedged blocking command matters to you, `tcp_user_timeout` is the knob — keepalives will not do it, because a connection waiting for a reply is not idle. The async stream methods (`new`, `start`, `snapshot`, and reconnects) never block their thread, so an enclosing `tokio::time::timeout` also bounds them. A connection whose command was cancelled refuses further commands and must be dropped.

> Set it deliberately. An aggressive `tcp_user_timeout` tears down healthy long-lived replication connections during ordinary transient stalls, which is usually worse than the delay it prevents.

## PostgreSQL Setup

Before using this library, you need to configure PostgreSQL for replication:

### 1. Configure PostgreSQL

Edit `postgresql.conf`:

```conf
wal_level = logical
max_replication_slots = 4
max_wal_senders = 4
```

Restart PostgreSQL after making these changes.

### 2. Create a Publication

```sql
-- Create a publication for specific tables
CREATE PUBLICATION my_publication FOR TABLE users, orders;

-- Or publish all tables
CREATE PUBLICATION my_publication FOR ALL TABLES;
```

### 3. Create Replication User

```sql
-- Create a user with replication privileges
CREATE USER replication_user WITH REPLICATION PASSWORD 'secure_password';

-- Grant necessary permissions
GRANT SELECT ON ALL TABLES IN SCHEMA public TO replication_user;
GRANT USAGE ON SCHEMA public TO replication_user;
```

### 4. Replication Slot Options

`start()` creates missing slots by default. Set `ReplicationStreamConfig::with_existing_slot_required(true)` to refuse automatic creation on startup and recovery, while explicit `ensure_replication_slot()` and `snapshot()` can still create slots. Slot creation SQL follows the connected PostgreSQL version.
- **PG14**: Legacy positional keyword syntax (`EXPORT_SNAPSHOT`, `NOEXPORT_SNAPSHOT`, `USE_SNAPSHOT`, `TWO_PHASE`, `RESERVE_WAL`)
- **PG15+**: Modern parenthesized options syntax (`(SNAPSHOT 'export', TWO_PHASE true, ...)`)

| Option | Description | PG Version |
|--------|-------------|------------|
| `temporary` | Temporary slot (not persisted to disk, dropped on disconnect) | 14+ |
| `two_phase` | Enable two-phase commit for logical slots | 14+ |
| `reserve_wal` | Reserve WAL immediately for physical slots | 14+ |
| `snapshot` | Snapshot behavior: `"export"`, `"use"`, or `"nothing"` | 14+ |
| `failover` | Enable slot synchronization to standbys for HA | 16+ |

> **Note:** If both `two_phase` and `snapshot` are set, `two_phase` takes priority. An option newer than the connected server is rejected client-side with the required version in the message, before any SQL is sent.

`failover` slots also need the standby configured for slot sync: `sync_replication_slots = on`, `hot_standby_feedback = on`, and a `primary_conninfo` that **includes `dbname`** (`pg_basebackup -R` omits it). After a promotion the consumer only changes its connection string — the slot is already there.

## Message Types

The library supports all PostgreSQL logical replication message types:

### Protocol Version 1 Messages

- **BEGIN**: Transaction start
- **COMMIT**: Transaction commit
- **ORIGIN**: Replication origin
- **RELATION**: Table schema definition
- **TYPE**: Data type definition
- **INSERT**: Row insertion
- **UPDATE**: Row update
- **DELETE**: Row deletion
- **TRUNCATE**: Table truncation
- **MESSAGE**: Generic message

### Protocol Version 2+ Messages (Streaming)

- **STREAM_START**: Streaming transaction start
- **STREAM_STOP**: Streaming transaction segment end
- **STREAM_COMMIT**: Streaming transaction commit
- **STREAM_ABORT**: Streaming transaction abort

### Protocol Version 3+ Messages (Two-Phase Commit)

- **BEGIN_PREPARE**: Prepared transaction start
- **PREPARE**: Transaction prepare
- **COMMIT_PREPARED**: Commit prepared transaction
- **ROLLBACK_PREPARED**: Rollback prepared transaction
- **STREAM_PREPARE**: Stream prepare message

## Architecture

```
┌──────────────────────────────────────────┐
│          Application Layer               │
│  (Your CDC / Replication Logic)          │
└──────────────┬───────────────────────────┘
               │
┌──────────────▼───────────────────────────┐
│    LogicalReplicationStream              │
│  - Connection management & retry         │
│  - Event processing & LSN feedback       │
│  - Managed initial snapshot + handoff    │
└──────────────┬───────────────────────────┘
               │
┌──────────────▼───────────────────────────┐
│  LogicalReplicationParser                │
│  - Protocol v1-v4 parsing                │
│  - Zero-copy message deserialization     │
│  - Streaming transaction support         │
└──────────────┬───────────────────────────┘
               │
┌──────────────▼───────────────────────────┐
│     PgReplicationConnection              │
│  ┌─────────────────┬──────────────────┐  │
│  │  libpq backend  │ rustls-tls       │  │
│  │  (C FFI)        │ (pure Rust)      │  │
│  │                 │                  │  │
│  │  pq-sys         │ rustls +         │  │
│  │                 │ aws-lc-rs +      │  │
│  │                 │ postgres-protocol│  │
│  └─────────────────┴──────────────────┘  │
│  Compile-time feature flag selection     │
└──────────────┬───────────────────────────┘
               │
┌──────────────▼───────────────────────────┐
│     BufferReader / BufferWriter          │
│  - Zero-copy operations (bytes crate)    │
│  - Binary protocol handling              │
│  - Drain-loop batch queue optimization   │
└──────────────────────────────────────────┘
```

## Backend Comparison & Stress Test

Progressive writer concurrency ramp (16 – 192 writers) plus a fixed scenario set, comparing the two connection backends.

- **Backend A**: rustls-tls (aws-lc-rs crypto)
- **Backend B**: libpq (OpenSSL)

**Test environment:** an 8-vCPU Linux VM (TCP-tuned per [Linux VM TCP Tuning](#linux-vm-tcp-tuning-for-production): 64 MB buffers, BBR) streaming from a **remote Azure PostgreSQL Flexible Server 18.6** across regions (~50 ms RTT). Each scenario ran 10 s warmup + 30 s measure. Process CPU/RSS reflect **only the pg-walstream consumer** — the write generator runs as a separate OS process.

> **The `sslmode` you measure under decides the result.** Without TLS the two
> backends are a statistical tie. With TLS, rustls-tls uses **~2.2x less CPU**
> for the same event rate. Always state the sslmode alongside any backend number.

## 1. CPU Efficiency (DML events/sec per 1% CPU)

The primary efficiency metric: DML events processed per 1% of consumer CPU. Higher is better.

| Scenario | rustls plain | libpq plain | rustls TLS | libpq TLS | libpq CPU vs rustls (TLS) |
|----------|----------:|----------:|----------:|----------:|----------:|
| Baseline | 6,670 | 7,135 | 3,759 | 1,739 | 2.13x |
| Batch-100 | 2,080 | 1,940 | 1,633 | 996 | 1.61x |
| Batch-5000 | 7,992 | 7,126 | 4,764 | 1,789 | 2.63x |
| 4-Writers | 6,995 | 7,093 | 4,052 | 2,098 | 1.84x |
| Wide-20col | 2,162 | 2,102 | 1,609 | 1,067 | 1.51x |
| Payload-2KB | 2,279 | 2,165 | 1,611 | 1,104 | 1.46x |
| Mixed-DML | 2,926 | 2,946 | 2,458 | 1,370 | 1.79x |
| Stress-16w | 6,088 | 6,437 | 3,917 | 1,800 | 2.62x |
| Stress-32w | 6,237 | 6,335 | 3,979 | 1,810 | 2.40x |
| Stress-48w | 5,630 | 6,190 | 3,854 | 1,647 | 2.23x |
| Stress-64w | 5,615 | 5,737 | 3,779 | 1,642 | 2.12x |
| Stress-96w | 5,546 | 5,534 | 3,524 | 1,568 | 2.24x |
| Stress-128w | 5,328 | 5,852 | 3,356 | 1,566 | 2.25x |
| Stress-192w | 5,554 | 5,457 | 3,754 | 1,511 | 2.33x |

Median over CPU-bound scenarios:

| `sslmode` | rustls-tls | libpq | Ratio |
|---|---:|---:|---:|
| `disable` (no TLS) | 5,859 | 6,262 | **0.98x — tie** |
| `require` (TLS) | 3,817 | 1,693 | **2.22x rustls-tls** |

Without TLS, per-scenario ratios scatter 0.90x – 1.12x with no directional lean — noise. TLS efficiency retained: **rustls-tls 66%, libpq 28%**.

## 2. Why: OpenSSL per-call overhead, not crypto

`perf` on the consumer during 4-Writers with TLS enabled:

| | libpq | rustls-tls |
|---|---:|---:|
| TLS library share of process CPU | **20.79%** (libcrypto + libssl) | 12.48% (aws-lc-rs, static) |
| ...of which is actual AES-GCM | **0.84%** | most of it |

libpq spends ~20 points of CPU on TLS **bookkeeping** and under 1 point on real
crypto. The hot OpenSSL symbols are all per-call setup — `ERR_clear_error`
(1.67%, which libpq calls before every read), `BIO_ctrl`, `EVP_CIPHER_CTX_ctrl`.
`libpq.so` itself is only 2.39%, so libpq's own code is cheap; the cost is the
OpenSSL call sequence it performs per read. pg-walstream issues one
`PQgetCopyData` per WAL message, so this **scales with message count, not bytes**.

Measured against libpq 18.6 / OpenSSL 3.0.2. A newer OpenSSL would likely narrow this gap but not close it, since the per-read error-stack clear is libpq's own code.

## 3. Throughput and Resources

Throughput here is bounded by the write generator and the ~50 ms link, not by the consumer — so both backends land in the same range. The difference is the CPU spent getting there.

| Metric | rustls-tls | libpq |
|--------|------:|------:|
| Peak DML events/sec (no TLS) | 213,392 | 202,347 |
| Peak DML events/sec (TLS) | 161,151 | 156,813 |
| CPU% at TLS peak | **38.9** | **80.9** |
| Avg RSS (MB) | 15.7 | 17.0 |
| P50 inter-event latency | 1 µs | 1 µs |

RSS and latency show no backend advantage.

## 4. Choosing a backend

- **TLS link (any managed Postgres: Azure, RDS, Cloud SQL)** → prefer the default
  **rustls-tls**; it needs roughly half the consumer CPU.
- **TLS terminated elsewhere** (private network, sidecar/proxy) → the backends are a tie; choose on operational grounds (libpq needs `libpq-dev` at runtime; rustls-tls is pure Rust with native SCRAM).

For a detailed comparison across PostgreSQL 16 and 18 with different optimizations (binary mode, direct TLS, COPY protocol), see the [Load Test Comparison Report](LOAD_TEST_COMPARISON.md).

## Linux VM TCP Tuning for Production

When streaming WAL over high-latency links (e.g., cross-region Azure PostgreSQL), the default Linux TCP buffer sizes can become the throughput bottleneck. The kernel's default `rmem_max` of 208 KB limits the TCP receive window, which — combined with round-trip latency — caps throughput via the **Bandwidth-Delay Product (BDP)**:

### Recommended sysctl Settings

```conf
# --- TCP buffer sizes ---
# Allow up to 64 MB per-socket receive/send buffers (kernel will auto-tune within this ceiling)
net.core.rmem_max = 67108864
net.core.wmem_max = 67108864

# TCP auto-tuning ranges: min / default / max (bytes)
# The kernel dynamically adjusts each socket's buffer within these bounds
net.ipv4.tcp_rmem = 4096 262144 67108864
net.ipv4.tcp_wmem = 4096 262144 67108864

# --- Congestion control ---
# BBR provides significantly better throughput than cubic on high-latency links
net.ipv4.tcp_congestion_control = bbr

# --- Packet backlog ---
# Increase the NIC receive queue (helps at high packet rates)
net.core.netdev_max_backlog = 5000
```

Apply immediately:

```bash
sudo sysctl --system
```

### Why Each Parameter Matters

| Parameter | Default | Recommended | Why |
|-----------|---------|-------------|-----|
| `rmem_max` | 208 KB | 64 MB | Caps TCP receive window; directly limits throughput on high-RTT links |
| `wmem_max` | 208 KB | 64 MB | Caps TCP send window; limits outbound throughput for feedback messages |
| `tcp_rmem` (max) | 6 MB | 64 MB | Per-socket auto-tuned receive buffer ceiling |
| `tcp_wmem` (max) | 4 MB | 64 MB | Per-socket auto-tuned send buffer ceiling |
| `tcp_congestion_control` | cubic | bbr | BBR reacts to actual bandwidth, not packet loss; better on cloud networks |
| `netdev_max_backlog` | 1000 | 5000 | Prevents packet drops under burst traffic at NIC level |

> **Note:** These settings affect all TCP connections on the VM, not just pg-walstream. The kernel auto-tunes actual buffer usage within the configured ceiling, so idle connections do not consume 64 MB each.

## Limitations

- Requires PostgreSQL 14 or later for full protocol support
- Logical replication slot must be created before streaming
- Binary protocol only (no text-based protocol support)
- Requires `replication` permission for the database user
- A reconnect that lands on a different cluster or a new timeline (failover, PITR) is rejected as a permanent error rather than resumed: an LSN is only meaningful within one cluster on one timeline. Compare the confirmed LSN against the switchpoint in `TIMELINE_HISTORY <tli>` to decide whether to resume or re-sync

## Resources

- [PostgreSQL Logical Replication Documentation](https://www.postgresql.org/docs/current/logical-replication.html)
- [Logical Replication Message Formats](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html)
- [Replication Protocol](https://www.postgresql.org/docs/current/protocol-replication.html)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the [BSD 3-Clause License](LICENSE).

## Author

Daniel Shih (dog830228@gmail.com)
