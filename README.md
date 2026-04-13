[![Crates.io Version](https://img.shields.io/crates/v/pg_walstream)](https://crates.io/crates/pg_walstream)
[![Crates.io Downloads (recent)](https://img.shields.io/crates/dr/pg_walstream)](https://crates.io/crates/pg_walstream)
[![Crates.io Total Downloads](https://img.shields.io/crates/d/pg_walstream)](https://crates.io/crates/pg_walstream)
[![docs.rs](https://img.shields.io/docsrs/pg_walstream)](https://docs.rs/pg-walstream)
[![codecov](https://codecov.io/gh/isdaniel/pg-walstream/graph/badge.svg?token=e0zmvpOWvz)](https://codecov.io/gh/isdaniel/pg-walstream)

# pg-walstream

A high-performance Rust library for PostgreSQL logical and physical replication protocol parsing and streaming. Provides a robust, type-safe interface for consuming PostgreSQL Write-Ahead Log (WAL) streams.

## Features

- **Full Logical Replication Support**: Implements PostgreSQL logical replication protocol versions 1-4
- **Physical Replication Support**: Stream raw WAL data for standby servers and PITR
- **Base Backup Support**: Full `BASE_BACKUP` command with progress, compression, and manifest options
- **Pure-Rust Backend**: Optional `rustls-tls` feature eliminates all C dependencies (no libpq, no libclang), using `aws-lc-rs` for hardware-accelerated TLS (AES-NI, AVX2, SHA-NI)
- **TLS/SSL Support**: All PostgreSQL SSL modes (`disable`, `allow`, `prefer`, `require`, `verify-ca`, `verify-full`)
- **Authentication**: Cleartext, MD5, and SCRAM-SHA-256 authentication methods
- **Streaming Transactions**: Support for streaming large transactions (protocol v2+)
- **Two-Phase Commit**: Prepared transaction support (protocol v3+)
- **Parallel Streaming**: Multi-stream parallel replication (protocol v4+)
- **Zero-Copy Operations**: Efficient buffer management using the `bytes` crate with drain-loop batch queue optimization
- **Thread-Safe LSN Tracking**: Atomic LSN feedback for producer-consumer patterns
- **Connection Management**: Built-in connection handling with exponential backoff retry logic
- **Type-Safe API**: Strongly typed message parsing with comprehensive error handling
- **Replication Slot Management**: Create, alter, read, and drop slots with full option support
- **Hot Standby Feedback**: Send hot standby feedback messages for physical replication
- **`Send`-Safe Streams**: `LogicalReplicationStream` is `Send`, compatible with `tokio::spawn`

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
pg_walstream = "0.5.1"
```

By default, this uses the `libpq` backend (C FFI). For a **pure-Rust** build with no system dependencies:

```toml
[dependencies]
pg_walstream = { version = "0.5.1", default-features = false, features = ["rustls-tls"] }
```

## Feature Flags

pg-walstream provides two mutually exclusive connection backends, selected at compile time:

| Feature | Default | C Dependencies | Description |
|---------|---------|----------------|-------------|
| `libpq` | Yes | `libpq-dev`, `libclang-dev` | Uses PostgreSQL's C client library via FFI. Battle-tested, supports all auth methods natively. |
| `rustls-tls` | No | `cmake`, `gcc` (build-time only) | Pure-Rust implementation using `rustls` with `aws-lc-rs` crypto backend for hardware-accelerated TLS. No runtime C dependencies. |

> **Note:** Enabling both features simultaneously will cause a compile error.

## System Dependencies

System dependencies are **only required** when using the default `libpq` feature. The `rustls-tls` feature requires only `cmake` and a C compiler at build time (for the `aws-lc-rs` crypto library), with no runtime dependencies.

### For `libpq` feature (default)

**Ubuntu/Debian:**
```bash
sudo apt-get install libpq-dev clang libclang-dev
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
pg_walstream = { version = "0.5.1", default-features = false, features = ["rustls-tls"] }
```

## Quick Start

### Logical Replication - Stream API

The Stream API provides an ergonomic, iterator-like interface:

```rust
use pg_walstream::{
    LogicalReplicationStream, ReplicationStreamConfig, ReplicationSlotOptions,
    RetryConfig, StreamingMode, SharedLsnFeedback, CancellationToken,
};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure the replication stream
    let config = ReplicationStreamConfig::new(
        "my_slot".to_string(),           // Replication slot name
        "my_publication".to_string(),     // Publication name
        2,                                // Protocol version
        StreamingMode::On,                // Streaming mode
        Duration::from_secs(10),          // Feedback interval
        Duration::from_secs(30),          // Connection timeout
        Duration::from_secs(60),          // Health check interval
        RetryConfig::default(),           // Retry configuration
    )
    // Optional: configure slot creation options
    .with_slot_options(ReplicationSlotOptions {
        temporary: true,
        snapshot: Some("export".to_string()),
        ..Default::default()
    });

    // Create and initialize the stream
    let mut stream = LogicalReplicationStream::new(
        "postgresql://postgres:password@localhost:5432/mydb?replication=database",
        config,
    ).await?;

    // Step 1: Create the replication slot
    stream.ensure_replication_slot().await?;

    // Step 2: Use the exported snapshot on a SEPARATE regular connection
    // If the slot was created with EXPORT_SNAPSHOT, use the snapshot name
    // on a SEPARATE regular connection to read the initial table state:
    //   BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
    //   SET TRANSACTION SNAPSHOT '<snapshot_name>';
    //   COPY my_table TO STDOUT;   -- or SELECT * FROM my_table
    //   COMMIT;
    if let Some(snapshot_name) = stream.exported_snapshot_name() {
        println!("Exported snapshot: {}", snapshot_name);
    }

    // Step 3: Begin streaming
    stream.start(None).await?;

    // Create cancellation token for graceful shutdown
    let cancel_token = CancellationToken::new();

    // Convert to async Stream - provides iterator-like interface
    let mut event_stream = stream.into_stream(cancel_token);

    // Process events using Stream combinators
    loop {
        match event_stream.next_event().await {
            Ok(event) => {
                println!("Received event: {:?}", event);
                // Update LSN feedback using the convenient method
                event_stream.update_applied_lsn(event.lsn.value());
            }
            Err(e) if matches!(e, pg_walstream::ReplicationError::Cancelled(_)) => {
                println!("Stream cancelled, shutting down gracefully");
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

> **Note:** The exported snapshot is only valid between `ensure_replication_slot()` and `start()`. Once `START_REPLICATION` is issued, PostgreSQL destroys the snapshot. You must read the snapshot on a separate connection **before** calling `start()`.

### Working with Event Data

Events carry row data as [`RowData`] an ordered list of `(Arc<str>, ColumnValue)` pairs.
[`ColumnValue`] is a lightweight enum (`Null | Text(Bytes) | Binary(Bytes)`) that preserves
the raw PostgreSQL wire representation with zero-copy semantics.
Schema, table, and column names are `Arc<str>` (reference-counted, zero-cost cloning):

```rust
use pg_walstream::{EventType, RowData, ColumnValue};

// Pattern match on event types
match &event.event_type {
    EventType::Insert { schema, table, data, .. } => {
        println!("INSERT into {}.{}", schema, table);

        // Access columns by name
        if let Some(id) = data.get("id") {
            println!("  id = {}", id);
        }

        // Iterate over all columns
        for (col_name, value) in data.iter() {
            println!("  {} = {}", col_name, value);
        }
    }
    EventType::Update { old_data, new_data, key_columns, .. } => {
        // key_columns is Vec<Arc<str>>
        println!("Key columns: {:?}", key_columns);
        println!("New data has {} columns", new_data.len());
    }
    EventType::Delete { old_data, .. } => {
        // Convert to HashMap if needed for downstream compatibility
        let map = old_data.clone().into_hash_map();
        println!("Deleted row: {:?}", map);
    }
    _ => {}
}
```

### Using the Polling API

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

## Examples

The [`examples/`](examples/) directory contains runnable examples demonstrating various usage patterns:

| Example | Description |
|---------|-------------|
| [`basic-streaming`](examples/basic-streaming) | High-level `futures::Stream` API with stream combinators (`filter`, `take_while`) |
| [`polling`](examples/polling) | Manual polling loop using `next_event()` for custom integration scenarios |
| [`safe-transaction-consumer`](examples/safe-transaction-consumer) | Production-grade transaction-aware CDC consumer with ordered commits and safe LSN feedback |
| [`rate-limited-streaming`](examples/rate-limited-streaming) | Rate-limited consumption using `tokio_stream::StreamExt::throttle` |
| [`tokio-spawn-streaming`](examples/tokio-spawn-streaming) | Producer/consumer pattern via `tokio::spawn` with `mpsc` channel (demonstrates `Send` safety) |
| [`pg-basebackup`](examples/pg-basebackup) | Full physical backup tool using `BASE_BACKUP` with tar extraction and progress reporting |
| [`arbitrary-fuzzing`](examples/arbitrary-fuzzing) | Property-based fuzzing of all protocol types using the `arbitrary` crate |

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
│  │  libpq-sys      │ rustls +         │  │
│  │  + libclang     │ aws-lc-rs +      │  │
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

### Test Environment

```
CPU: AMD EPYC 7763 64-Core Processor
CPU cores: 8
Memory: 32812624 kB
OS: Ubuntu 22.04.5 LTS
USE TLS/SSL
```

### Stress Ramp: Throughput vs CPU at Increasing Writer Concurrency


```
Writers | DML ev/s    | Proc CPU% | Sys CPU%  | RSS (MB)
--------|-------------|-----------|-----------|--------
    16 |      48,805 |     31.2% |      5.5% | 18.6
    32 |      88,645 |     54.3% |      8.6% | 18.6
    48 |     135,669 |     84.3% |     12.8% | 18.7
    64 |     135,681 |     82.0% |     11.8% | 18.6
    96 |     135,524 |     83.2% |     12.6% | 18.7
    128 |     135,367 |     84.3% |     12.8% | 18.7
    192 |     132,571 |     82.8% |     13.0% | 18.6
```

```
Throughput scaling with writer concurrency:

16w |   48,805 ev/s |██████████████
32w |   88,645 ev/s |██████████████████████████
48w |  135,669 ev/s |████████████████████████████████████████
64w |  135,681 ev/s |████████████████████████████████████████
96w |  135,524 ev/s |████████████████████████████████████████
128w |  135,367 ev/s |████████████████████████████████████████
192w |  132,571 ev/s |███████████████████████████████████████
```


```
CPU usage scaling with writer concurrency:

16w | proc  31.2% sys   5.5% |████████████
32w | proc  54.3% sys   8.6% |██████████████████████
48w | proc  84.3% sys  12.8% |██████████████████████████████████
64w | proc  82.0% sys  11.8% |█████████████████████████████████
96w | proc  83.2% sys  12.6% |█████████████████████████████████
128w | proc  84.3% sys  12.8% |██████████████████████████████████
192w | proc  82.8% sys  13.0% |█████████████████████████████████
Legend: █ = process CPU (pg-walstream), ░ = additional system CPU
```

```
Memory (RSS) scaling with writer concurrency:

16w |   18.6 MB avg /   18.6 MB peak |████████████████████████████████████████
32w |   18.6 MB avg /   18.6 MB peak |████████████████████████████████████████
48w |   18.7 MB avg /   18.7 MB peak |████████████████████████████████████████
64w |   18.6 MB avg /   18.6 MB peak |████████████████████████████████████████
96w |   18.7 MB avg /   18.7 MB peak |████████████████████████████████████████
128w |   18.7 MB avg /   18.7 MB peak |████████████████████████████████████████
192w |   18.6 MB avg /   18.6 MB peak |████████████████████████████████████████
```

**Threshold Analysis:**

- **Peak throughput**: 135,681 DML events/sec at **64w** concurrency
- **Throughput saturation** detected at **64 writers** (throughput gain < 5% or regression)
- **Library CPU moderate**: Process CPU peaked at 88.8% -- approaching but not yet at saturation
- **Memory**: Peak process RSS 18.7 MB / 32044 MB total system (0.06% utilization)
- **CPU efficiency**: ~1,528 DML events/sec per 1% CPU at peak

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
