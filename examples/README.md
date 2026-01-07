# PostgreSQL WAL Streaming Examples

This directory contains examples demonstrating how to use the `pg_walstream` library to stream PostgreSQL Write-Ahead Log (WAL) changes.

## Examples

### 1. basic_streaming.rs

Demonstrates the high-level Stream API for consuming WAL changes.

**Features:**
- Async iterator-like interface
- Automatic event processing with retry logic
- Comprehensive event type handling (Insert, Update, Delete, etc.)
- Graceful shutdown with Ctrl+C

**Run:**
```bash
cargo run --example basic_streaming
```

### 2. polling_example.rs

Shows the lower-level polling API for manual event retrieval.

**Features:**
- Manual event polling with `next_event()`
- More control over the polling loop
- Suitable for custom integration scenarios

**Run:**
```bash
cargo run --example polling_example
```

### 3. safe_transaction_consumer.rs

**NEW**: Advanced example demonstrating safe transaction processing with ordered commits.

**Features:**
- Transaction buffering until commit
- Ordered transaction application
- LSN feedback only after successful application
- Backpressure handling when too many transactions are buffered
- Graceful shutdown with proper cleanup
- Real-time statistics reporting
- Transaction boundary enforcement

**Run:**
```bash
cargo run --example safe_transaction_consumer
```

**Use Cases:**
- Building production-ready replication consumers
- Ensuring data consistency across systems
- Implementing exactly-once processing semantics
- Safe CDC (Change Data Capture) pipelines

## Prerequisites

### 1. PostgreSQL Configuration

Ensure your PostgreSQL server is configured for logical replication. Edit `postgresql.conf`:

```
wal_level = logical
max_replication_slots = 4
max_wal_senders = 4
```

Restart PostgreSQL after making changes.

### 2. Create a Publication

```sql
CREATE PUBLICATION my_publication FOR ALL TABLES;
```

### 3. Set Environment Variable

```bash
export DATABASE_URL="postgresql://postgres:password@localhost:5432/postgres?replication=database"
```

**Important:** The connection string must include `?replication=database` parameter.

## Testing

Start an example:
```bash
cargo run --example basic_streaming
```

In another terminal, make database changes:
```sql
CREATE TABLE test_users (id SERIAL PRIMARY KEY, name TEXT, email TEXT);
INSERT INTO test_users (name, email) VALUES ('Alice', 'alice@example.com');
UPDATE test_users SET email = 'alice.new@example.com' WHERE name = 'Alice';
DELETE FROM test_users WHERE name = 'Alice';
```

You should see the changes appearing in the example output.