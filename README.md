[![Crates.io Version](https://img.shields.io/crates/v/pg_walstream)](https://crates.io/crates/pg_walstream)
[![Crates.io Downloads (recent)](https://img.shields.io/crates/dr/pg_walstream)](https://crates.io/crates/pg_walstream)
[![Crates.io Total Downloads](https://img.shields.io/crates/d/pg_walstream)](https://crates.io/crates/pg_walstream)
[![docs.rs](https://img.shields.io/docsrs/pg_walstream)](https://docs.rs/pg-walstream)
[![codecov](https://codecov.io/gh/isdaniel/pg-walstream/graph/badge.svg?token=e0zmvpOWvz)](https://codecov.io/gh/isdaniel/pg-walstream)
[![CodSpeed](https://img.shields.io/endpoint?url=https://codspeed.io/badge.json)](https://app.codspeed.io/isdaniel/pg-walstream?utm_source=badge)

# pg-walstream

A high-performance Rust library for PostgreSQL logical and physical replication protocol parsing and streaming. Provides a robust, type-safe interface for consuming PostgreSQL Write-Ahead Log (WAL) streams.

## Features

- **Logical replication**: pgoutput protocol versions 1-4, including streaming of large transactions (v2+), two-phase commit (v3+) and parallel streaming (v4+). Each streamed change carries its subtransaction's xid in `ChangeEvent::xid`.
- **Physical replication**: raw WAL streaming for standbys and PITR, `BASE_BACKUP` with progress, compression and manifest options, and hot standby feedback.
- **Two connection backends**: a pure-Rust default (`rustls-tls`: no libpq, no OpenSSL, `aws-lc-rs` for TLS) and an opt-in `libpq` backend. All SSL modes; cleartext, MD5 and SCRAM-SHA-256 authentication.
- **Typed consumption**: a `serde` deserializer maps rows into your structs, `ReplicationStreamConfig::builder()` configures the stream, `EventStream::for_each_event` acknowledges automatically, and `WalRouter` (with the opt-in `#[derive(WalTable)]`) routes events by table.
- **Managed initial snapshot**: `snapshot()` copies the published tables, then hands off to the stream with no gap and no duplicate window. `capture_aligned` does the same for tables added while the stream runs.
- **Bounded replay and raw access**: `with_stop_at_lsn` streams to a target LSN and ends cleanly; `next_raw_event` yields the undecoded pgoutput payload for your own decoder.
- **Binary serialization**: `ChangeEvent` round-trips through JSON, CBOR, MessagePack, postcard and bincode ([guide](#shipping-events-between-processes)).
- **Reliable feedback and recovery**: thread-safe `SharedLsnFeedback`, exponential-backoff reconnect, replication slot management.
- **Zero-copy parsing** on the `bytes` crate, and a `no_std` + `alloc` parser-only build for wasm and embedded targets.

## Installation

```toml
[dependencies]
pg_walstream = "0.10"
```

The default `rustls-tls` backend is pure Rust: it needs only `cmake` and a C compiler at build time (for `aws-lc-rs`) and nothing at runtime (`sudo apt-get install cmake gcc`). To use the C **libpq** backend instead (bound by `pq-sys`):

```toml
pg_walstream = { version = "0.10", default-features = false, features = ["libpq"] }
```

It needs the system libpq: `sudo apt-get install libpq-dev libssl-dev` (Debian/Ubuntu) or `sudo dnf install postgresql-devel` (RHEL/Fedora). If both backends are enabled, `rustls-tls` takes priority.

| Feature | Default | Description |
|---------|---------|-------------|
| `std` | Yes | Standard library. Without it (`default-features = false`) you get a `no_std` + `alloc` build of the protocol parser, encoder and types, which compiles for wasm and embedded targets. |
| `rustls-tls` | Yes | Pure-Rust backend: `rustls` with `aws-lc-rs`. Implies `std`. |
| `libpq` | No | libpq via FFI, bound by `pq-sys` (pre-generated bindings, no libclang). Implies `std`. |
| `derive` | No | `#[derive(WalTable)] #[wal(table = "...")]` and the one-line `#[wal_table("...")]`, enabling `WalRouter::on_insert_of::<T>` / `on_update_of` / `on_delete_of`. |

The `no_std` build needs a target with pointer-sized atomics (thumbv7m, thumbv7em, thumbv8m and riscv32imac build; thumbv6m and riscv32imc do not). `SharedLsnFeedback` also needs 64-bit atomics, so it is left out on targets without them, such as thumbv7em. A connection backend is required only for the live streaming and connection APIs.

### TLS trust store

When `sslmode` is `verify-ca` or `verify-full`, the `rustls-tls` backend loads **only** the CAs in `sslrootcert` if it is set, and otherwise the [Mozilla CA bundle](https://wiki.mozilla.org/CA/Included_Certificates) shipped via `webpki-roots`. The OS trust store is **not** consulted, so a server signed by a corporate CA that lives only there needs `sslrootcert` pointed at it:

```text
postgresql://user:pass@host/db?sslmode=verify-full&sslrootcert=/etc/ssl/certs/corporate-ca.pem
```

### Client certificates

For a server that authenticates by certificate (`hostssl ... cert`), the `rustls-tls` backend presents the pair named by `sslcert` and `sslkey`, much as libpq does:

```text
postgresql://cdc@host/db?sslmode=verify-full&sslrootcert=/etc/ssl/ca.pem&sslcert=/etc/ssl/cdc.crt&sslkey=/etc/ssl/cdc.key
```

- Both are PEM files and both are required: libpq's default `~/.postgresql/postgresql.{crt,key}` is never read, `sslcert` without `sslkey` is a configuration error, and a lone `sslkey` is ignored. Put intermediate certificates after the leaf in the `sslcert` file and the whole chain is sent.
- The key may be PKCS#8, PKCS#1 (`RSA PRIVATE KEY`) or SEC1 (`EC PRIVATE KEY`) in PEM, not DER; RSA, ECDSA (P-256 and P-384) and Ed25519 keys authenticate. An encrypted key is refused by name (`sslpassword` is not supported): decrypt it first, or use the `libpq` backend.
- The certificate must be X.509 v3. `openssl x509 -req` without `-extfile` issues a v1 certificate, which rustls refuses to present, so the handshake fails with `UnsupportedCertVersion`.
- Both files are read again on every connect, so a rotated pair is used by the next reconnect. A `sslcert` that does not exist means no certificate is sent, as in libpq, so a stale path does not break a connection that never needed one. A certificate that exists but cannot be used, a missing or encrypted key, or a pair that does not match is a connection error under every `sslmode`, `prefer` and `allow` included: it is never answered by retrying in plaintext. These errors are retried like any connection failure (five attempts by default, about 15 s) and then returned with the file and the reason; each attempt logs them too. If `connection_timeout` is shorter than the retries take, the error you get back is a `Timeout` instead. With `sslmode=disable` both options are ignored.
- The pair is offered in every `sslmode`, but `require`, `prefer` and `allow` do not verify the server, so there the certificate goes to whatever answers; use `verify-ca` or `verify-full` to know who receives it.
- `sslcertmode=disable` keeps the pair off the wire. `sslcertmode=require` is refused here, because this backend does not check that the server asked for a certificate.
- Unlike libpq, the key file's permissions are not checked, and the `PGSSLCERT`, `PGSSLKEY` and `PGSSLCERTMODE` environment variables are not read (nor is any environment variable but `PGPASSWORD`).

## Quick Start

```rust
use pg_walstream::{
    CancellationToken, LogicalReplicationStream, ReplicationError, ReplicationStreamConfig,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ReplicationStreamConfig::builder("my_slot", "my_publication");

    let mut stream = LogicalReplicationStream::new(
        "postgresql://postgres:password@localhost:5432/mydb?replication=database",
        config,
    )
    .await?;
    stream.start(None).await?;

    let cancel = CancellationToken::new();
    loop {
        match stream.next_event_with_retry(&cancel).await {
            Ok(event) => {
                println!("{event:?}");
                // Acknowledge only once the event is safely handled.
                stream.shared_lsn_feedback.update_applied_lsn(event.lsn.value());
            }
            Err(ReplicationError::Cancelled(_)) => break,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}
```

`ReplicationStreamConfig` has `with_*` methods for the rest (protocol version, feedback interval, retry policy and more). The [`examples/`](examples/) directory has runnable programs:

| Example | Description |
|---------|-------------|
| [`basic-streaming`](examples/basic-streaming) | `futures::Stream` API with combinators (`filter`, `take_while`) |
| [`polling`](examples/polling) | Manual `next_event()` loop for custom integration |
| [`initial-snapshot`](examples/initial-snapshot) | Copy existing rows, then stream changes with no gap and no duplicate |
| [`safe-transaction-consumer`](examples/safe-transaction-consumer) | Production-grade, transaction-aware consumer with ordered commits and safe LSN feedback |
| [`rate-limited-streaming`](examples/rate-limited-streaming) | Rate-limited consumption with `tokio_stream::StreamExt::throttle` |
| [`tokio-spawn-streaming`](examples/tokio-spawn-streaming) | Producer/consumer across tasks with an `mpsc` channel (`Send` safety) |
| [`typed-deserialization`](examples/typed-deserialization) | INSERT/UPDATE/DELETE events into your own structs via `serde` |
| [`derive-router`](examples/derive-router) | `#[derive(WalTable)]` and `WalRouter` table inference (`derive` feature) |
| [`binary-serde`](examples/binary-serde) | Ship `ChangeEvent`s between processes in five formats (no database needed) |
| [`stream-metrics`](examples/stream-metrics) | Read `stream.metrics()` from other tasks: rates, lag and unconfirmed bytes, reconnects, a Prometheus text endpoint |
| [`raw-xlogdata`](examples/raw-xlogdata) | Undecoded pgoutput payload via `next_raw_event()` |
| [`binary-column-access`](examples/binary-column-access) | Lossless zero-copy `BYTEA` access via `PgResult::get_bytes` |
| [`pg-basebackup`](examples/pg-basebackup) | Physical backup tool using `BASE_BACKUP` with tar extraction and progress |
| [`arbitrary-fuzzing`](examples/arbitrary-fuzzing) | Property-based fuzzing of all protocol types with `arbitrary` |

## Mapping Columns to Struct Fields

Rows are keyed by their real PostgreSQL column names. When a struct field name differs from its column, use serde's `#[serde(rename = "...")]`:

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

## Shipping Events Between Processes

`ChangeEvent` implements serde's `Serialize` and `Deserialize`, so an event can go to another process, a queue or a file in a serde format. JSON, CBOR, MessagePack, postcard and bincode 2 are tested; other formats are not. Use JSON when the bytes must stay readable or outlive a release, and a compact format when size and speed matter. Sizes for one 6-event transaction (BEGIN, INSERT, UPDATE, DELETE, a logical message, COMMIT) from the [`binary-serde`](examples/binary-serde) example:

| Format | Crate | Transaction | Read across versions | Notes |
|--------|-------|------------:|----------------------|-------|
| JSON | `serde_json` | 1174 B (100%) | yes, bytes are unchanged | readable, any language; the one to store long-term |
| CBOR | `ciborium` | 933 B (79%) | no, see [Upgrading](#upgrading-from-09) | self-describing, writes field names |
| MessagePack | `rmp-serde` | 498 B (42%) | no, see [Upgrading](#upgrading-from-09) | `to_vec` writes fields as an array, `to_vec_named` keeps field names |
| postcard | `postcard` | 349 B (30%) | same version on both ends | smallest, `no_std` friendly, Rust to Rust |
| bincode 2 | `bincode` | 361 B (31%) | same version on both ends | Rust to Rust; the crate is unmaintained, so prefer postcard in new code |

```rust,ignore
use pg_walstream::ChangeEvent;

// postcard = { version = "1", features = ["use-std"] }
let bytes: Vec<u8> = postcard::to_allocvec(&event)?;     // producer
let event: ChangeEvent = postcard::from_bytes(&bytes)?;  // consumer
```

The other formats work the same way (`serde_json::to_vec`, `ciborium::into_writer`, `rmp_serde::to_vec`, `bincode::serde::encode_to_vec`). Encode each event as it arrives and call `update_applied_lsn` only after it has been handed off safely. To write many events into one file, put them back to back and read them with `postcard::take_from_bytes` (or bincode's `decode_from_slice`).

- **Same version for postcard and bincode.** They carry no field names and read fields and variants by position, so their bytes are meant to be read by the pg_walstream version that wrote them. MessagePack from `rmp_serde::to_vec` also writes fields by position. Keep events you store long-term as JSON.
- **Untrusted bytes.** Malformed bytes make JSON, CBOR, MessagePack and postcard return an error instead of panicking, and a row reserves room for at most 1600 columns up front whatever length the bytes claim. None of them detects a corrupted value inside an otherwise well-formed event, so add your own checksum if you need that. bincode 2's default configuration allocates whatever length a string claims, which can abort the process: decode bytes you did not write yourself only with a limit above your largest row, `bincode::config::standard().with_limit::<{ 1024 * 1024 }>()`.
- **Your own envelope.** An ordinary enum around a `ChangeEvent` works with every format. `#[serde(tag = "...")]`, `#[serde(untagged)]` and `#[serde(flatten)]` work with JSON, CBOR and MessagePack, never with postcard or bincode (serde's own limit).
- **What changes on the wire.** JSON is exactly what it always was: `null` for NULL, a string for text, `{"$binary":"<hex>"}` for binary data and non-UTF-8 text, and `xid` left out when `None`. Every other format writes a column as `None` for NULL and otherwise as `Some` of `Text` or `Binary` holding the raw bytes, and always writes `xid`.
- **Non-UTF-8 text.** A PostgreSQL text column can hold bytes that are not UTF-8. JSON reads them back as `Binary`; the other formats keep them as `Text`.

The [`binary-serde`](examples/binary-serde) example needs no database. It encodes a small transaction in all five formats, checks that each reads the same events back, and shows framing, envelopes and a decode of hostile bytes (`cd examples/binary-serde && cargo run`).

## LSN Tracking

Thread-safe LSN tracking for feedback to PostgreSQL:

```rust
use pg_walstream::SharedLsnFeedback;

let feedback = SharedLsnFeedback::new_shared();

// Producer thread: read LSN from feedback
let (flushed_lsn, applied_lsn) = feedback.get_feedback_lsn();

// Consumer thread: update LSN after processing
feedback.update_applied_lsn(commit_lsn);
```

The stream sends this to PostgreSQL itself: at the first check after `feedback_interval` while messages flow (the check runs every 128th message), and whenever the server asks for a reply. A check sends only if a position moved; a reply the server asks for is always answered.

### Acknowledging promptly on an idle stream

The stream sends feedback only while it is being read, and an idle stream has no message to trigger a check. An acknowledgement raised while nothing arrives is therefore reported when the server next asks for a reply, every `wal_sender_timeout / 2` (30 s with the default 60 s). Nothing is lost and the connection stays up as long as the stream answers those requests, so this is slow, not unsafe. It matters when something waits on the slot's `confirmed_flush_lsn`:

- a consumer named in `synchronous_standby_names`: other sessions' `COMMIT` waits for its flush position (measured at 28 s per commit);
- a server with `wal_sender_timeout = 0`: it never asks, so an idle stream's acknowledgement is not reported at all;
- a cutover or monitor that waits for the slot to catch up.

To shorten the wait without a code change, shorten `wal_sender_timeout` for the replication connection: the server then asks every `T / 2`. The parameter can be set per session on PostgreSQL 14 to 18 (the versions checked), so any of these works, and the first two override the server-wide value, `0` included:

| Where | How |
|---|---|
| Connection string | `...?replication=database&options=-c%20wal_sender_timeout%3D4s` |
| Role | `ALTER ROLE cdc SET wal_sender_timeout = '4s'` |
| Server | `wal_sender_timeout = '4s'` in `postgresql.conf` |

The trade-off is that the same value is how long the stream may go without answering before the server disconnects it. Choose `T = 2 × the delay you accept`, and keep it above the longest pause your consumer takes between reads (a handler that blocks, a slow downstream write). A disconnected stream is recovered by `next_event_with_retry`, which reconnects and resumes from the last confirmed position, but it costs a reconnect and a replay of the unconfirmed transactions. Measured on PostgreSQL 17 with the native backend: `T` of 4, 10 and 20 s reported an idle acknowledgement after 2, 5 and 10 s, and a 10 s pause between reads disconnected the stream at `T = 4 s` but not at `T = 20 s`.

## Metrics

`stream.metrics()` (also on `EventStream`) returns a cloneable handle any task can read while the stream is polled. `snapshot()` returns a `Copy` struct of totals, so a rate is the difference of two snapshots; counters include replays after a reconnect.

```rust
let metrics = stream.metrics(); // before `into_stream`, or `event_stream.metrics()`
tokio::spawn(async move {
    loop {
        let m = metrics.snapshot();
        println!("{} msgs, {} reconnects, {} B unacked", m.wal_messages, m.reconnects, m.lag_bytes());
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    }
});
```

| Watch | Fields |
|---|---|
| Data flowing | `wal_messages`, `wal_bytes`, `commits`, `keepalives`, `last_server_time` |
| Work in flight (read, not yet acknowledged) | `lag_bytes()` |
| Consumer behind | not visible to the client; on the server, `pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)` from `pg_replication_slots` |
| Server hearing from the consumer | `unconfirmed_bytes()`, `feedback_sent`, `feedback_failures` |
| Connection health | `reconnects`, `reconnect_attempts`, `reconnect_failures`, `recovering`, `last_error` |
| Raw positions and times | `received_lsn`, `flushed_lsn`, `applied_lsn`, `sent_flushed_lsn`, `last_commit_time`, `last_feedback_time` |

- **`lag_bytes()`**: WAL distance from the last data frame to the consumer's `applied_lsn`: the work in flight, such as the open transaction when you acknowledge at each commit. Keepalives are excluded, so an idle stream does not drift. It is the consumer's own progress, not how far behind the consumer is: the stream reads on demand, so a consumer that falls behind leaves its backlog in the socket and on the server, where this does not see it. Watch the message rate and `last_server_time`, and the slot on the server.
- **`unconfirmed_bytes()`**: distance from the server's latest position (keepalives included) to the flushed position the last status update reported. On an idle stream it grows with writes elsewhere in the database; an acknowledgement moves its start to the position the consumer acknowledged, not to the server's latest.
- **`feedback_sent`** standing still is normal on an idle stream: a periodic update is skipped while the positions it would report have not changed. A growing `feedback_failures` is the sign that updates are not getting through.
- **`last_error`** stays set after the stream recovers: alert on it changing, not on it being set, and read it with `reconnects`.
- **`last_server_time`** is the server's clock: compare it with itself over time, not with `now`.

The client cannot see what the server retains for the slot or whether it was invalidated: query `pg_replication_slots` (`wal_status = 'lost'` is final and works on every supported version). Prefer the metrics above to the `*_lag` columns of `pg_stat_replication`, which for a logical slot are sampled only at transaction ends and go stale. Every field is documented on `MetricsSnapshot`; [`examples/stream-metrics`](examples/stream-metrics) adds a Prometheus text endpoint.

## Initial Snapshot

Streaming alone tells you what *changed*, never what was already there. Enable a managed initial snapshot and the library copies the published tables through the replication slot's exported snapshot, then hands off to the stream:

```rust
use pg_walstream::SnapshotOutcome;
use std::sync::Arc;

let config = ReplicationStreamConfig::builder("my_slot", "my_publication");

let stream = LogicalReplicationStream::new(conn_str, config).await?;
let sink = Arc::new(sink);

let mut stream = match stream.snapshot().await? {
    // The slot already existed, so there is nothing to copy: the normal
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

`snapshot()` sets `SNAPSHOT 'export'` on the slot itself, so there is no flag to remember and no builder-ordering rule to get wrong. Snapshot rows arrive as ordinary `ChangeEvent::Insert`s carrying the slot's consistent point as their LSN, so an existing `WalRouter` works across both phases with the same handlers:

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

A complete, runnable version, including a row inserted *during* the handoff window to prove it arrives exactly once, is in [`examples/initial-snapshot`](examples/initial-snapshot).

- Use `snapshot.rows()` instead of `.events()` to iterate `SnapshotRow`s (`.data`, `.relation`, `.lsn`) without the `ChangeEvent` wrapper. To copy only part of a large publication, bind the snapshot as `mut` and call `snapshot.retain_tables(..)` before `.events()` or `.rows()`.
- **A failed snapshot cannot be resumed.** The exported snapshot is `REPEATABLE READ`; continuing against an expired one would silently mix rows from two points in time. You own the retry policy; the retry always starts from the beginning. A failed or dropped snapshot handle makes a best-effort attempt to drop its slot (a failure to drop is logged, not returned), so a retry normally gets a fresh export.
- **Holding a snapshot handle blocks `VACUUM`.** The handle keeps a `REPEATABLE READ` transaction open, pinning the database's `xmin` for as long as you hold it. Copy promptly.

### Tables added while the stream runs

`capture_aligned` copies extra tables through a temporary side slot's exported snapshot, exact at its consistent point `C`, and `next_event_before(C)` returns `None` once every transaction committed below `C` has been delivered. Commit the `ALTER PUBLICATION ... ADD TABLE` first. The walsender drops a stream that sends no status update for `wal_sender_timeout`, so keep calling `send_feedback()` while the capture waits and the copy loads. Do not read the stream until the capture returns, because a read can pass `C`, which `next_event_before` cannot undo:

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

- **Decide each transaction at its commit event.** Below `C`, apply it without the copied tables' rows, which the copy already holds; at or past `C`, apply it in full, after the copy. Buffer each transaction until its commit event, because streamed chunks and prepared transactions can arrive before the `None`.
- **Acknowledge each transaction you decided below `C` before the copy loads.** A reconnect replays from the acknowledged position, so without it every transaction delivered before the `None` is applied twice (50 of 50 in a measured idle-database run). `EventStream::next_event_before` reconnects without telling you, so also start a buffered transaction over when a `Begin`, a `BeginPrepare` or a `StreamStart` with `first_segment` arrives for one you already hold (a precaution: that replay of a streamed transaction was not reproduced).
- **The copy pins `xmin` and holds an `ACCESS SHARE` lock on each copied table until `finish()`.** Reads and writes go on, but a `TRUNCATE`, `ALTER TABLE`, `DROP`, `VACUUM FULL` or `CLUSTER` on those tables waits, and writes behind it wait with it. Give that DDL a `lock_timeout`, or run it before the capture or after the copy.
- **`capture` has no deadline of its own.** Slot creation waits for the transactions open when it starts, so end the blocker or cancel the walsender with `pg_cancel_backend`.

The rustdoc of `LogicalReplicationStream::capture_aligned` has the full list of hazards (connection limits, cancel safety, lock behaviour, abort handling).

## Connection Timeouts

Three conninfo options bound three different things, and they are easy to mistake for one another:

| Option | Unit | Bounds |
|---|---|---|
| `connect_timeout` | seconds | Only the TCP connect. On the native backend TLS negotiation and startup/auth are **not** covered; libpq bounds the whole handshake with each address, then tries the next host. |
| `keepalives_idle` / `_interval` / `_count` | seconds | An **idle** connection. Defaults `120`/`10`/`3`: roughly 150 s to notice a peer that vanished while nothing was in flight. |
| `tcp_user_timeout` | **milliseconds** | An unacknowledged **write**. This is the only one that bounds a blocking round-trip, including the `DROP_REPLICATION_SLOT` that snapshot cleanup runs from `Drop`. Linux-family only; `0` (the default) disables it. |

If a stuck destructor or a wedged blocking command matters to you, `tcp_user_timeout` is the knob, because a connection waiting for a reply is not idle and keepalives will not help. Set it deliberately: an aggressive value tears down healthy long-lived replication connections during ordinary transient stalls. The async stream methods (`new`, `start`, `snapshot`, and reconnects) never block their thread, so an enclosing `tokio::time::timeout` also bounds them. A connection whose command was cancelled refuses further commands and must be dropped.

## Session Parameters

`application_name` and `options` in the connection string reach the server's startup message on both backends:

```text
postgresql://user:pass@host/db?replication=database&application_name=orders-cdc&options=-c%20TimeZone%3DUTC
```

- `application_name` names the walsender in `pg_stat_activity` and `pg_stat_replication`, and is what `synchronous_standby_names` matches (the consumer counts as a synchronous standby once it has acknowledged a position). `fallback_application_name` applies only when it is absent.
- `options` pins GUCs for the session. The text of a decoded `timestamptz`, date or interval follows the walsender's `TimeZone`, `DateStyle` and `IntervalStyle`, so pin them when the same value must render the same wherever it is read. `client_encoding` cannot be changed this way: the stream decodes UTF-8.
- `PGAPPNAME` and `PGOPTIONS` are not read; of the environment variables only `PGPASSWORD` is.

## PostgreSQL Setup

```conf
# postgresql.conf (restart required)
wal_level = logical
max_replication_slots = 4
max_wal_senders = 4
```

```sql
-- A publication for specific tables (or FOR ALL TABLES)
CREATE PUBLICATION my_publication FOR TABLE users, orders;

-- A user with replication privileges
CREATE USER replication_user WITH REPLICATION PASSWORD 'secure_password';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO replication_user;
GRANT USAGE ON SCHEMA public TO replication_user;
```

`start()` creates a missing slot by default. Set `ReplicationStreamConfig::with_existing_slot_required(true)` to refuse automatic creation on startup and recovery, while explicit `ensure_replication_slot()` and `snapshot()` can still create slots. Slot creation SQL follows the connected PostgreSQL version (legacy keyword syntax on PG14, parenthesized options on PG15+).

| Option | Description | PG Version |
|--------|-------------|------------|
| `temporary` | Temporary slot (not persisted to disk, dropped on disconnect) | 14+ |
| `two_phase` | Enable two-phase commit for logical slots | 14+ |
| `reserve_wal` | Reserve WAL immediately for physical slots | 14+ |
| `snapshot` | Snapshot behavior: `"export"`, `"use"`, or `"nothing"` | 14+ |
| `failover` | Enable slot synchronization to standbys for HA | 16+ |

If both `two_phase` and `snapshot` are set, `two_phase` takes priority. An option newer than the connected server is rejected client-side with the required version in the message, before any SQL is sent. `failover` slots also need the standby configured for slot sync: `sync_replication_slots = on`, `hot_standby_feedback = on`, and a `primary_conninfo` that **includes `dbname`** (`pg_basebackup -R` omits it). After a promotion the consumer only changes its connection string, because the slot is already there.

## Message Types

The parser handles every pgoutput message: `BEGIN`, `COMMIT`, `ORIGIN`, `RELATION`, `TYPE`, `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE` and `MESSAGE` (protocol v1); `STREAM_START`, `STREAM_STOP`, `STREAM_COMMIT` and `STREAM_ABORT` (v2+); `BEGIN_PREPARE`, `PREPARE`, `COMMIT_PREPARED`, `ROLLBACK_PREPARED` and `STREAM_PREPARE` (v3+). The formats are in [Logical Replication Message Formats](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html).

## Architecture

From the top down:

- **`LogicalReplicationStream`**: connection management and retry, event processing and LSN feedback, snapshot handoff. `EventStream` and `WalRouter` build on it.
- **`LogicalReplicationParser`**: protocol v1-v4 parsing, zero-copy, streaming transactions.
- **`PgReplicationConnection`**: libpq (C FFI via `pq-sys`) or `rustls-tls` (`rustls` + `aws-lc-rs` + `postgres-protocol`), chosen at compile time.
- **`BufferReader` / `BufferWriter`**: zero-copy binary protocol handling on the `bytes` crate.

## Choosing a Backend

The `sslmode` you measure under decides the result. Without TLS the two backends are a statistical tie; with TLS, `rustls-tls` needs about 2.2x less CPU for the same event rate. DML events per 1% of consumer CPU (higher is better), median over CPU-bound scenarios, streaming from a remote Azure PostgreSQL 18.6 over a ~50 ms link:

| `sslmode` | rustls-tls | libpq | Ratio |
|---|---:|---:|---:|
| `disable` (no TLS) | 5,859 | 6,262 | **0.98x, a tie** |
| `require` (TLS) | 3,817 | 1,693 | **2.22x rustls-tls** |

The gap is OpenSSL's per-call bookkeeping, not crypto: with TLS, libpq spends about 20% of process CPU in OpenSSL, of which 0.84% is AES-GCM, and the cost scales with message count because pg-walstream issues one `PQgetCopyData` per WAL message. So on a TLS link (any managed PostgreSQL) prefer the default `rustls-tls`; with TLS terminated elsewhere the backends tie, so choose on operational grounds (libpq needs `libpq-dev` at runtime; `rustls-tls` is pure Rust with native SCRAM). Per-scenario results, throughput and the host tuning used are in [`load-tests/README.md`](load-tests/README.md); a comparison across PostgreSQL 16 and 18 with different optimizations is in the [Load Test Comparison Report](LOAD_TEST_COMPARISON.md).

## Limitations

- Requires PostgreSQL 14 or later, and the `replication` permission for the database user.
- Decodes the binary `pgoutput` protocol only; other output plugins (`wal2json`, `test_decoding`) are not supported.
- A reconnect that lands on a different cluster or a new timeline (failover, PITR) is rejected as a permanent error rather than resumed: an LSN is only meaningful within one cluster on one timeline. Compare the confirmed LSN against the switchpoint in `TIMELINE_HISTORY <tli>` to decide whether to resume or re-sync.

- **The minimum supported Rust version is 1.88.**

## Resources

- [PostgreSQL Logical Replication Documentation](https://www.postgresql.org/docs/current/logical-replication.html)
- [Logical Replication Message Formats](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html)
- [Replication Protocol](https://www.postgresql.org/docs/current/protocol-replication.html)

## Contributing and License

Contributions are welcome; see [CONTRIBUTING.md](CONTRIBUTING.md). This project is licensed under the [BSD 3-Clause License](LICENSE).

Author: Daniel Shih (dog830228@gmail.com)
