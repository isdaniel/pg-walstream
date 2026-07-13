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
- **Streaming Transactions**: Support for streaming large transactions (protocol v2+)
- **Two-Phase Commit**: Prepared transaction support (protocol v3+)
- **Parallel Streaming**: Multi-stream parallel replication (protocol v4+)
- **Zero-Copy Operations**: Efficient buffer management using the `bytes` crate with drain-loop batch queue optimization. The libpq backend copies each COPY message into a reusable `BytesMut` buffer, then reference-counts it downstream as `Bytes` (no per-message heap allocation after warmup)
- **Thread-Safe LSN Tracking**: Atomic LSN feedback for producer-consumer patterns
- **Connection Management**: Built-in connection handling with exponential backoff retry logic
- **Type-Safe API**: Strongly typed message parsing with comprehensive error handling
- **Typed Row Deserialization**: Built-in `serde` deserializer maps WAL rows directly into user-defined Rust structs (numerics, `bool`, `String`, `Option<T>`, enums, bytes)
- **High-Level Consumption Ergonomics**: `ReplicationStreamConfig::builder()`, an auto-acking `EventStream::for_each_event`, and a typed by-table `WalRouter` with an optional `#[derive(WalTable)]` layer (opt-in `derive` feature)
- **Bounded Replay**: `ReplicationStreamConfig::with_stop_at_lsn` streams to a target LSN, delivers the crossing transaction in full, then ends cleanly with `ReplicationError::StreamStopped`
- **Raw XLogData Access**: `LogicalReplicationStream::next_raw_event` yields the undecoded pgoutput payload plus WAL positions (`RawXLogData`) for consumers that bring their own decoder — keepalives, feedback, and cancellation still handled, no auto-ack
- **Replication Slot Management**: Create, alter, read, and drop slots with full option support
- **Hot Standby Feedback**: Send hot standby feedback messages for physical replication

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
pg_walstream = "0.8"
```

By default, this uses the pure-Rust `rustls-tls` backend — no libpq and no OpenSSL, only `cmake` + a C compiler at build time (for `aws-lc-rs`).

To use the C **libpq** backend instead (bound by `pq-sys`; requires system libpq):

```toml
[dependencies]
pg_walstream = { version = "0.8", default-features = false, features = ["libpq"] }
```

If both backends are enabled, `rustls-tls` takes priority automatically.

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
pg_walstream = { version = "0.8", features = ["rustls-tls"] }
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
| [`polling`](examples/polling) | Manual polling loop using `next_event()` for custom integration scenarios |
| [`safe-transaction-consumer`](examples/safe-transaction-consumer) | Production-grade transaction-aware CDC consumer with ordered commits and safe LSN feedback |
| [`rate-limited-streaming`](examples/rate-limited-streaming) | Rate-limited consumption using `tokio_stream::StreamExt::throttle` |
| [`tokio-spawn-streaming`](examples/tokio-spawn-streaming) | Producer/consumer pattern via `tokio::spawn` with `mpsc` channel (demonstrates `Send` safety) |
| [`typed-deserialization`](examples/typed-deserialization) | Map INSERT/UPDATE/DELETE events directly into user-defined Rust structs via `serde` |
| [`derive-router`](examples/derive-router) | `#[derive(WalTable)]` + `WalRouter` table-inference (`on_*_of::<T>`) — the `derive` feature |
| [`pg-basebackup`](examples/pg-basebackup) | Full physical backup tool using `BASE_BACKUP` with tar extraction and progress reporting |
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

The library provides full control over replication slot creation. The correct SQL syntax is automatically selected based on the connected PostgreSQL version:
- **PG14**: Legacy positional keyword syntax (`EXPORT_SNAPSHOT`, `NOEXPORT_SNAPSHOT`, `USE_SNAPSHOT`, `TWO_PHASE`, `RESERVE_WAL`)
- **PG15+**: Modern parenthesized options syntax (`(SNAPSHOT 'export', TWO_PHASE true, ...)`)

| Option | Description | PG Version |
|--------|-------------|------------|
| `temporary` | Temporary slot (not persisted to disk, dropped on disconnect) | 14+ |
| `two_phase` | Enable two-phase commit for logical slots | 14+ |
| `reserve_wal` | Reserve WAL immediately for physical slots | 14+ |
| `snapshot` | Snapshot behavior: `"export"`, `"use"`, or `"nothing"` | 14+ |
| `failover` | Enable slot synchronization to standbys for HA | 16+ |

> **Note:** If both `two_phase` and `snapshot` are set, `two_phase` takes priority. The `failover` option is not available on PG14 and will return an error.

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
│  - Snapshot export support               │
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

## Stress Test & System Threshold Analysis

Progressive writer concurrency ramp (16 - 192 writers) to find the library's CPU saturation point and throughput ceiling.

- **Backend A**: rustls-tls
- **Backend B**: libpq

**Test environment:** an 8-vCPU Linux VM (TCP-tuned per [Linux VM TCP Tuning](#linux-vm-tcp-tuning-for-production): 64 MB buffers, BBR) streaming from a **remote Azure PostgreSQL Flexible Server 18.4** over TLS (`sslmode=require`). Each scenario ran 10 s warmup + 30 s measure; every configuration was measured **3 times per backend** and the **median** is reported (cross-run CoV ≤ ~6%). Process CPU/RSS reflect **only the pg-walstream consumer** — the write generator runs as a separate OS process.

> **Reading these numbers:** Over a real network link the consumer spends most of its time parked in `epoll`/TLS I/O rather than parsing, so both backends are **network-I/O-bound and converge to within run-to-run noise** on CPU efficiency, CPU%, RSS, and latency. rustls-tls keeps a small (~8%) throughput edge on saturated single-stream ingest. If you are benchmarking over loopback/localhost you may see larger CPU gaps; those do not represent a realistic remote-DB deployment.

## 1. CPU Efficiency (DML events/sec per 1% CPU)

This is the primary efficiency metric: how many DML events each backend processes for every 1% of CPU it consumes. Higher is better.

| Scenario | rustls-tls | libpq | Delta | Winner |
|----------|----------:|----------:|--------:|--------|
| Baseline | 4,338 | 4,250 | +2.1% | ~tie |
| Batch-100 | 2,459 | 2,575 | -4.5% | ~tie |
| Batch-5000 | 4,105 | 4,122 | -0.4% | ~tie |
| 4-Writers | 3,621 | 3,626 | -0.1% | ~tie |
| Wide-20col | 1,856 | 1,960 | -5.3% | ~tie |
| Payload-2KB | 1,212 | 1,218 | -0.5% | ~tie |
| Mixed-DML | 3,293 | 3,301 | -0.2% | ~tie |

Both backends land within ±5% across every scenario — a statistical tie.

## 2. Throughput Comparison

| Scenario | rustls-tls ev/s | libpq ev/s | Delta | rustls-tls DML/s | libpq DML/s | Delta | Winner |
|----------|----------:|----------:|--------:|----------:|----------:|--------:|--------|
| Baseline | 178,812 | 164,585 | +8.6% | 177,414 | 163,299 | +8.6% | **rustls-tls** |
| Batch-100 | 30,909 | 31,069 | -0.5% | 30,303 | 30,460 | -0.5% | ~tie |
| Batch-5000 | 167,095 | 153,424 | +8.9% | 165,801 | 152,236 | +8.9% | **rustls-tls** |
| 4-Writers | 147,377 | 146,476 | +0.6% | 145,273 | 144,404 | +0.6% | ~tie |
| Wide-20col | 27,126 | 27,434 | -1.1% | 26,133 | 26,429 | -1.1% | ~tie |
| Payload-2KB | 23,069 | 21,297 | +8.3% | 21,400 | 19,756 | +8.3% | **rustls-tls** |
| Mixed-DML | 55,180 | 57,506 | -4.0% | 54,526 | 56,824 | -4.0% | **libpq** |

rustls-tls is ~8–9% faster on saturated single-stream ingest (Baseline, Batch-5000, Payload-2KB); everything else is within noise.

## 3. Resource Utilization Comparison

Process CPU and RSS reflect **only the pg-walstream consumer** (generator runs as a separate OS process).

| Scenario | rustls-tls CPU% | libpq CPU% | Delta | rustls-tls RSS MB | libpq RSS MB | Delta | Winner |
|----------|----------:|----------:|--------:|----------:|----------:|--------:|--------|
| Baseline | 38.4 | 38.4 | -0.0% | 14.8 | 14.8 | -0.1% | ~tie |
| Batch-100 | 12.3 | 12.1 | +1.7% | 16.1 | 15.9 | +1.3% | ~tie |
| Batch-5000 | 38.9 | 37.2 | +4.6% | 16.3 | 16.3 | -0.2% | ~tie |
| 4-Writers | 39.6 | 39.7 | -0.3% | 16.5 | 16.5 | +0.4% | ~tie |
| Wide-20col | 14.1 | 14.4 | -2.4% | 16.6 | 16.6 | +0.2% | ~tie |
| Payload-2KB | 17.9 | 16.2 | +10.5% | 16.5 | 16.7 | -0.9% | **libpq** |
| Mixed-DML | 16.5 | 17.7 | -6.4% | 16.7 | 17.6 | -5.1% | **rustls-tls** |

CPU% and RSS are effectively equal; the largest single-scenario deltas (~10%) sit inside the 3-run variance.

## 4. Latency Comparison (inter-event, microseconds)

| Scenario | rustls-tls P50 | libpq P50 | rustls-tls P99 | libpq P99 | Winner |
|----------|----------:|----------:|----------:|----------:|--------|
| Baseline | 1 | 1 | 72 | 66 | ~tie |
| Batch-100 | 1 | 1 | 645 | 599 | ~tie |
| Batch-5000 | 1 | 1 | 64 | 64 | ~tie |
| 4-Writers | 1 | 1 | 150 | 149 | ~tie |
| Wide-20col | 1 | 1 | 227 | 223 | ~tie |
| Payload-2KB | 5 | 5 | 390 | 365 | ~tie |
| Mixed-DML | 1 | 1 | 107 | 109 | ~tie |

P50 is 1 µs for both; P99 tracks within ±10 µs — no backend advantage.

## 5. Stress Ramp Comparison

Progressive writer concurrency ramp — comparing throughput and CPU scaling.

| Writers | rustls-tls DML/s | libpq DML/s | Delta | rustls-tls CPU% | libpq CPU% | rustls-tls eff | libpq eff |
|--------:|----------:|----------:|--------:|----------:|----------:|----------:|----------:|
| 16 | 127,226 | 125,729 | +1.2% | 42.9 | 43.4 | 2,975 | 2,969 |
| 32 | 115,728 | 117,595 | -1.6% | 42.4 | 42.1 | 2,764 | 2,770 |
| 48 | 103,432 | 105,136 | -1.6% | 40.6 | 40.0 | 2,591 | 2,671 |
| 64 | 98,473 | 99,038 | -0.6% | 37.8 | 37.3 | 2,616 | 2,586 |
| 96 | 87,361 | 87,592 | -0.3% | 34.0 | 35.5 | 2,550 | 2,468 |
| 128 | 79,709 | 76,466 | +4.2% | 32.9 | 34.0 | 2,425 | 2,306 |
| 192 | 72,291 | 72,186 | +0.1% | 33.1 | 32.9 | 2,193 | 2,194 |

Throughput and efficiency scale essentially identically for both backends across the ramp.

### Peak Numbers

| Metric | rustls-tls | libpq |
|--------|------:|------:|
| Peak DML events/sec | 177,414 | 163,299 |
| Peak total events/sec | 178,812 | 164,585 |
| Peak CPU efficiency (DML/s per 1% CPU) | 4,338 | 4,250 |
| Peak process CPU% | 51 | 50 |
| Peak RSS (MB) | 18 | 18 |

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
