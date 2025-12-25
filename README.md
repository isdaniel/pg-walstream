# pg-walstream

A high-performance Rust library for PostgreSQL logical replication protocol parsing and streaming. This library provides a robust, type-safe interface for consuming PostgreSQL Write-Ahead Log (WAL) streams with support for Change Data Capture (CDC) use cases.

## Features

- **Full Protocol Support**: Implements PostgreSQL logical replication protocol versions 1-4
- **Streaming Transactions**: Support for streaming large transactions (protocol v2+)
- **Two-Phase Commit**: Prepared transaction support (protocol v3+)
- **Parallel Streaming**: Multi-stream parallel replication (protocol v4+)
- **Zero-Copy Operations**: Efficient buffer management using the `bytes` crate
- **Thread-Safe LSN Tracking**: Atomic LSN feedback for producer-consumer patterns
- **Connection Management**: Built-in connection handling with exponential backoff retry logic
- **Type-Safe API**: Strongly typed message parsing with comprehensive error handling

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
pg_walstream = "0.1.0"
```

## Quick Start

### Basic Message Parsing

```rust
use pg_walstream::{LogicalReplicationParser, LogicalReplicationMessage};

// Create a parser with protocol version 2
let mut parser = LogicalReplicationParser::with_protocol_version(2);

// Parse a WAL message from bytes
let message = parser.parse_wal_message(&wal_data)?;

match message.message {
    LogicalReplicationMessage::Begin { final_lsn, timestamp, xid } => {
        println!("Transaction {} started at LSN {}", xid, final_lsn);
    }
    LogicalReplicationMessage::Insert { relation_id, tuple } => {
        println!("Insert into relation {}", relation_id);
    }
    LogicalReplicationMessage::Commit { commit_lsn, .. } => {
        println!("Transaction committed at LSN {}", commit_lsn);
    }
    _ => {}
}
```

### Complete Replication Stream

```rust
use pg_walstream::{
    LogicalReplicationStream, ReplicationStreamConfig, RetryConfig,
    SharedLsnFeedback, CancellationToken,
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
        true,                             // Enable streaming
        Duration::from_secs(10),          // Feedback interval
        Duration::from_secs(30),          // Connection timeout
        Duration::from_secs(60),          // Health check interval
        RetryConfig::default(),           // Retry configuration
    );

    // Create connection string
    let connection_string = "host=localhost port=5432 dbname=mydb user=myuser password=mypass replication=database";

    // Create and initialize the stream
    let mut stream = LogicalReplicationStream::new(connection_string, config).await?;
    
    // Set up LSN feedback for tracking progress
    let lsn_feedback = SharedLsnFeedback::new_shared();
    stream.set_shared_lsn_feedback(lsn_feedback.clone());

    // Initialize the stream (creates slot if needed)
    stream.initialize().await?;

    // Start replication from a specific LSN (or None for latest)
    stream.start(None).await?;

    // Create cancellation token for graceful shutdown
    let cancel_token = CancellationToken::new();

    // Process events
    loop {
        match stream.next_event(&cancel_token).await? {
            Some(event) => {
                println!("Received event: {:?}", event);
                
                // Update LSN feedback after processing
                if let Some(lsn) = event.lsn {
                    lsn_feedback.update_applied_lsn(lsn.value());
                }
            }
            None => {
                // No event available, continue
            }
        }
    }
}
```

### Working with Change Events

```rust
use pg_walstream::{ChangeEvent, EventType};

fn process_event(event: ChangeEvent) {
    match event.event_type {
        EventType::Insert { schema, table, data, .. } => {
            println!("INSERT into {}.{}", schema, table);
            for (col, value) in data {
                println!("  {}: {:?}", col, value);
            }
        }
        EventType::Update { schema, table, old_data, new_data, .. } => {
            println!("UPDATE {}.{}", schema, table);
            if let Some(old) = old_data {
                println!("  Old values: {:?}", old);
            }
            println!("  New values: {:?}", new_data);
        }
        EventType::Delete { schema, table, old_data, .. } => {
            println!("DELETE from {}.{}", schema, table);
            println!("  Deleted row: {:?}", old_data);
        }
        _ => {}
    }
}
```

## Core Components

### Protocol Parser

The `LogicalReplicationParser` is the core component for parsing WAL messages:

```rust
use pg_walstream::LogicalReplicationParser;

let mut parser = LogicalReplicationParser::with_protocol_version(2);
let message = parser.parse_wal_message(&bytes)?;
```

### LSN Tracking

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

### Buffer Operations

Efficient zero-copy buffer reading and writing:

```rust
use pg_walstream::{BufferReader, BufferWriter};

// Reading
let mut reader = BufferReader::new(&data);
let value = reader.read_u32()?;
let text = reader.read_cstring()?;

// Writing
let mut writer = BufferWriter::new();
writer.write_u32(12345)?;
writer.write_cstring("hello")?;
let bytes = writer.freeze();
```

### Error Handling

Comprehensive error types with retry classification:

```rust
use pg_walstream::ReplicationError;

match error {
    ReplicationError::TransientConnection(_) => {
        // Can retry
        println!("Transient error, will retry");
    }
    ReplicationError::PermanentConnection(_) => {
        // Should not retry
        println!("Permanent error, giving up");
    }
    ReplicationError::Authentication(_) => {
        // Fix credentials
        println!("Authentication failed");
    }
    _ => {}
}
```

## PostgreSQL Setup

Before using this library, you need to configure PostgreSQL for logical replication:

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

### 4. Configure pg_hba.conf

Add replication access:

```conf
# TYPE  DATABASE        USER                ADDRESS         METHOD
host    replication     replication_user    0.0.0.0/0       md5
```

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

## Configuration

### Retry Configuration

```rust
use pg_walstream::RetryConfig;
use std::time::Duration;

let retry_config = RetryConfig {
    max_attempts: 5,
    initial_delay: Duration::from_secs(1),
    max_delay: Duration::from_secs(60),
    multiplier: 2.0,
    max_duration: Duration::from_secs(300),
    jitter: true,
};
```

### Stream Configuration

```rust
use pg_walstream::ReplicationStreamConfig;
use std::time::Duration;

let config = ReplicationStreamConfig::new(
    "my_replication_slot".to_string(),
    "my_publication".to_string(),
    2,                                   // Protocol version
    true,                                // Enable streaming
    Duration::from_secs(10),             // Feedback interval
    Duration::from_secs(30),             // Connection timeout
    Duration::from_secs(60),             // Health check interval
    RetryConfig::default(),
);
```

## Testing

Run the test suite:

```bash
# Run all tests
cargo test

# Run tests with output
cargo test -- --nocapture

# Run specific test module
cargo test buffer::tests

# Run tests with verbose output
cargo test --lib -- --show-output
```

The project includes 95 comprehensive unit tests covering:
- Protocol message parsing
- Buffer operations
- LSN tracking and thread safety
- Error handling
- Retry logic
- Type conversions

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
- **Atomic Operations**: Thread-safe LSN tracking with minimal overhead
- **Connection Pooling**: Reusable connection with automatic retry
- **Streaming Support**: Handle large transactions without memory issues

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
