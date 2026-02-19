[![Crates.io Version](https://img.shields.io/crates/v/pg_walstream)](https://crates.io/crates/pg_walstream)
[![Crates.io Downloads (recent)](https://img.shields.io/crates/dr/pg_walstream)](https://crates.io/crates/pg_walstream)
[![Crates.io Total Downloads](https://img.shields.io/crates/d/pg_walstream)](https://crates.io/crates/pg_walstream)
[![docs.rs](https://img.shields.io/docsrs/pg_walstream)](https://docs.rs/pg-walstream)
[![codecov](https://codecov.io/gh/isdaniel/pg-walstream/graph/badge.svg?token=e0zmvpOWvz)](https://codecov.io/gh/isdaniel/pg-walstream)

# pg-walstream

A high-performance Rust library for PostgreSQL logical and physical replication protocol parsing and streaming. This library provides a robust, type-safe interface for consuming PostgreSQL Write-Ahead Log (WAL) streams.

## Features

- **Full Logical Replication Support**: Implements PostgreSQL logical replication protocol versions 1-4
- **Physical Replication Support**: Stream raw WAL data for standby servers and PITR
- **Streaming Transactions**: Support for streaming large transactions (protocol v2+)
- **Two-Phase Commit**: Prepared transaction support (protocol v3+)
- **Parallel Streaming**: Multi-stream parallel replication (protocol v4+)
- **Zero-Copy Operations**: Efficient buffer management using the `bytes` crate
- **Thread-Safe LSN Tracking**: Atomic LSN feedback for producer-consumer patterns
- **Connection Management**: Built-in connection handling with exponential backoff retry logic
- **Type-Safe API**: Strongly typed message parsing with comprehensive error handling
- **Configurable Slot Options**: Temporary slots, snapshot export, two-phase, and failover support

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
pg_walstream = "0.1.0"
```

## System Dependencies

Make sure you have libpq development libraries installed:

**Ubuntu/Debian:**
```bash
sudo apt-get install libpq-dev \
    clang \
    libclang-dev 
```

**CentOS/RHEL/Fedora:**
```bash
sudo yum install postgresql-devel
# or
sudo dnf install postgresql-devel
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
    
    stream.start(None).await?;

    // If the slot was created with EXPORT_SNAPSHOT, use the snapshot name on a SEPARATE regular connection to read the initial table state:
    //   BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
    //   SET TRANSACTION SNAPSHOT '<snapshot_name>';
    //   COPY my_table TO STDOUT;   -- or SELECT * FROM my_table
    //   COMMIT;
    if let Some(snapshot_name) = stream.exported_snapshot_name() {
        println!("Exported snapshot: {}", snapshot_name);
    }

    // Create cancellation token for graceful shutdown
    let cancel_token = CancellationToken::new();

    // Convert to async Stream - provides iterator-like interface
    let mut event_stream = stream.into_stream(cancel_token);

    // Process events using Stream combinators
    loop {
        match event_stream.next().await {
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

> **Note:** The exported snapshot is only valid while the transaction that created the
> replication slot is still open. You must read the snapshot **before** consuming WAL events
> or closing the replication connection. Temporary slots are automatically recreated on
> connection recovery.

### Working with Event Data

Events carry row data as `RowData` — an ordered list of `(Arc<str>, Value)` pairs.
Schema, table, and column names are `Arc<str>` (reference-counted, zero-cost cloning):

```rust
use pg_walstream::{EventType, RowData};

// Pattern match on event types
match &event.event_type {
    EventType::Insert { schema, table, data, .. } => {
        // schema and table are Arc<str> — Display works directly
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

### 4. Create Replication Slot with Advanced Options

The library provides two methods for creating replication slots:

#### Replication Slot Options

The library automatically selects the correct `CREATE_REPLICATION_SLOT` SQL syntax based on the connected PostgreSQL server version:
- **PG14**: Legacy positional keyword syntax (`EXPORT_SNAPSHOT`, `NOEXPORT_SNAPSHOT`, `USE_SNAPSHOT`, `TWO_PHASE`, `RESERVE_WAL`)
- **PG15+**: Modern parenthesized options syntax (`(SNAPSHOT 'export', TWO_PHASE true, ...)`)

| Option | Description | PG Version |
|--------|-------------|------------|
| `temporary` | Temporary slot (not persisted to disk, dropped on disconnect) | 14+ |
| `two_phase` | Enable two-phase commit for logical slots | 14+  |
| `reserve_wal` | Reserve WAL immediately for physical slots | 14+  |
| `snapshot` | Snapshot behavior: `"export"`, `"use"`, or `"nothing"` | 14+ |
| `failover` | Enable slot synchronization to standbys for HA | 16+ |

> **Note:** : If both `two_phase` and `snapshot` are set, `two_phase` takes priority. The `failover` option is not available on PG14 and will return an error.

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
┌─────────────────────────────────────────┐
│         Application Layer               │
│  (Your CDC / Replication Logic)        │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│    LogicalReplicationStream             │
│  - Connection management                │
│  - Event processing                     │
│  - LSN feedback                         │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│  LogicalReplicationParser               │
│  - Protocol parsing                     │
│  - Message deserialization              │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│     BufferReader / BufferWriter         │
│  - Zero-copy operations                 │
│  - Binary protocol handling             │
└─────────────────────────────────────────┘
```

## Performance Considerations

- **Zero-Copy**: Uses `bytes::Bytes` for efficient buffer management
- **Arc-shared column metadata**: Column names, schema, and table names use `Arc<str>` — cloning is a single atomic increment instead of a heap allocation per event
- **RowData (ordered Vec)**: Row payloads use `RowData` (a `Vec<(Arc<str>, Value)>`) instead of `HashMap<String, Value>`, eliminating per-event hashing overhead and extra allocations
- **Atomic Operations**: Thread-safe LSN tracking with minimal overhead
- **Connection Pooling**: Reusable connection with automatic retry
- **Streaming Support**: Handle large transactions without memory issues
- **Efficient Blocking**: Async I/O with tokio::select eliminates busy-waiting

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

## Author

Daniel Shih (dog830228@gmail.com)
