# PostgreSQL WAL Streaming Examples

This directory contains examples demonstrating how to use the `pg_walstream` library to stream PostgreSQL Write-Ahead Log (WAL) changes.

Each example is an **independent binary project** with its own `Cargo.toml`, so example-specific dependencies (e.g. `futures`, `flate2`, `tokio-stream`) do not pollute `pg_walstream`'s `[dev-dependencies]`.

## Running an Example

```bash
cd examples/<project-name>
cargo run
```

## Examples

### 1. basic-streaming

Demonstrates the high-level Stream API wrapped with `futures::stream::unfold` for Stream trait compatibility.

**Features:**
- Async iterator-like interface with futures::Stream compatibility
- Automatic event processing with retry logic
- Comprehensive event type handling (Insert, Update, Delete, etc.)
- Stream combinators support (filter, take_while, etc.)
- Graceful shutdown with Ctrl+C

**Run:**
```bash
cd examples/basic-streaming
cargo run
```

### 2. rate-limited-streaming

Demonstrates rate limiting and flow control using futures::Stream combinators.

**Features:**
- Rate limiting to prevent overwhelming downstream systems
- Configurable events per second limit
- Real-time rate statistics and monitoring
- Backpressure handling with automatic throttling
- Practical example of Stream trait usage
- Protection for downstream APIs with rate limits

**Run:**
```bash
cd examples/rate-limited-streaming
cargo run

# Custom rate limit
MAX_EVENTS_PER_SECOND=50 cargo run
```

**Use Cases:**
- Protecting downstream APIs from being overwhelmed
- Complying with third-party service rate limits
- Spreading load over time for cost optimization
- Controlled batch processing
- Testing with realistic production loads

### 3. polling

Shows the lower-level polling API for manual event retrieval.

**Features:**
- Manual event polling with `next_event()`
- More control over the polling loop
- Suitable for custom integration scenarios

**Run:**
```bash
cd examples/polling
cargo run
```

### 4. safe-transaction-consumer

Advanced example demonstrating safe transaction processing with ordered commits.

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
cd examples/safe-transaction-consumer
cargo run
```

**Use Cases:**
- Building production-ready replication consumers
- Ensuring data consistency across systems
- Implementing exactly-once processing semantics
- Safe CDC (Change Data Capture) pipelines

### 5. pg-basebackup

Complete implementation of pg_basebackup functionality for creating physical database backups.

**Features:**
- Full physical backup of PostgreSQL cluster
- Streaming backup data via replication protocol
- Tar archive extraction and file writing
- Progress reporting and verification
- Backup manifest generation with checksums
- Support for all BASE_BACKUP options (compression, WAL inclusion, etc.)
- Production-ready error handling

**Run:**
```bash
cd examples/pg-basebackup
cargo run

# Or with custom backup directory:
BACKUP_DIR="/tmp/pg_basebackup" cargo run
```

**What It Does:**
1. Connects to PostgreSQL in replication mode
2. Initiates BASE_BACKUP with optimal settings
3. Streams backup data as tar archives
4. Extracts and writes files to `./backup/` directory
5. Generates backup manifest for verification

**Use Cases:**
- Setting up physical replication standby servers
- Point-in-time recovery (PITR) setup
- Disaster recovery preparation
- Database migrations and cloning
- Automated backup solutions

### 6. arbitrary-fuzzing

Demonstrates generating arbitrary `pg_walstream` types for fuzzing **without** modifying the library.

**Run:**
```bash
cd examples/arbitrary-fuzzing
cargo run
```

## Prerequisites

### For Logical Replication Examples (basic_streaming, polling_example, safe_transaction_consumer)

#### 1. PostgreSQL Configuration

Ensure your PostgreSQL server is configured for logical replication. Edit `postgresql.conf`:

```
wal_level = logical
max_replication_slots = 4
max_wal_senders = 4
```

Restart PostgreSQL after making changes.

#### 2. Create a Publication

```sql
CREATE PUBLICATION my_publication FOR ALL TABLES;
```

#### 3. Set Environment Variable

```bash
export DATABASE_URL="postgresql://postgres:password@localhost:5432/postgres?replication=database"
```

**Important:** The connection string must include `?replication=database` parameter.

### For Physical Replication Example (pg_basebackup)

#### 1. PostgreSQL Configuration

For base backups and physical replication. Edit `postgresql.conf`:

```
wal_level = replica          # or 'logical' (logical includes replica)
max_wal_senders = 10
max_replication_slots = 10
```

#### 2. Authentication Setup

Edit `pg_hba.conf` to allow replication connections:

```
# TYPE  DATABASE        USER            ADDRESS                 METHOD
host    replication     postgres        127.0.0.1/32            trust
# or for password authentication:
host    replication     postgres        127.0.0.1/32            md5
```

#### 3. Connection String

```bash
export DATABASE_URL="postgresql://postgres:password@localhost:5432/postgres?replication=database"
```

**Note:** Physical replication requires the `replication=database` parameter and appropriate permissions.

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