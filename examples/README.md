# PostgreSQL WAL Streaming Examples

This directory contains examples demonstrating how to use the `pg_walstream` library to stream PostgreSQL Write-Ahead Log (WAL) changes.

## Examples

### 1. basic_streaming.rs

Demonstrates the high-level Stream API wrapped with `futures::stream::unfold` for Stream trait compatibility.

**Features:**
- Async iterator-like interface with futures::Stream compatibility
- Automatic event processing with retry logic
- Comprehensive event type handling (Insert, Update, Delete, etc.)
- Stream combinators support (filter, take_while, etc.)
- Graceful shutdown with Ctrl+C

**Run:**
```bash
cargo run --example basic_streaming
```

### 2. rate_limited_streaming.rs

**NEW**: Demonstrates rate limiting and flow control using futures::Stream combinators.

**Features:**
- Rate limiting to prevent overwhelming downstream systems
- Configurable events per second limit
- Real-time rate statistics and monitoring
- Backpressure handling with automatic throttling
- Practical example of Stream trait usage
- Protection for downstream APIs with rate limits

**Run:**
```bash
# Default: 10 events per second
cargo run --example rate_limited_streaming

# Custom rate limit
MAX_EVENTS_PER_SECOND=50 cargo run --example rate_limited_streaming
```

**Use Cases:**
- Protecting downstream APIs from being overwhelmed
- Complying with third-party service rate limits
- Spreading load over time for cost optimization
- Controlled batch processing
- Testing with realistic production loads

### 3. polling_example.rs

Shows the lower-level polling API for manual event retrieval.

**Features:**
- Manual event polling with `next_event()`
- More control over the polling loop
- Suitable for custom integration scenarios

**Run:**
```bash
cargo run --example polling_example
```

### 4. safe_transaction_consumer.rs

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

### 5. pg_basebackup.rs

**NEW**: Complete implementation of pg_basebackup functionality for creating physical database backups.

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
cargo run --example pg_basebackup
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