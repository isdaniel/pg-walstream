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

### 7. typed-deserialization

Demonstrates deserializing WAL event data directly into user-defined Rust structs using `RowData::deserialize_into()`.

- Automatic text-to-type coercion (PostgreSQL sends all values as text)
- NULL handling with `Option<T>` fields
- `#[serde(rename)]` for column-to-field name mapping
- `#[serde(default)]` for missing/evolving columns
- Enum deserialization from text columns
- `ChangeEvent` convenience methods (`deserialize_insert`, `deserialize_update`, etc.)
- Error handling for type mismatches and NULL violations

**Run:**
```bash
cd examples/typed-deserialization
cargo run
```

**No database required** — this example uses in-memory `RowData` and `ChangeEvent` objects to demonstrate the deserialization API.

### 8. binary-column-access

Demonstrates `PgResult::get_bytes` for lossless, zero-copy access to binary
(`BYTEA`) columns — contrasting it with the lossy `get_value` and showing how a
replication extension reads binary payloads without `encode(data, 'hex')`.

**Run:**
```bash
cd examples/binary-column-access
cargo run
```

### 9. raw-xlogdata

Streams the **undecoded** pgoutput payload via `next_raw_event()` — bring your
own decoder — with manual auto-ack.

**Features:**
- Raw `RawXLogData` (WAL positions + undecoded bytes), no pgoutput parsing
- Advance the applied LSN with `raw.wal_end` after durable processing
- Graceful exit on `Cancelled` / `StreamStopped`

**Run:**
```bash
cd examples/raw-xlogdata
cargo run
```

### 10. initial-snapshot

Copies the rows that **already exist** before streaming starts, then hands off to
the live stream with no gap and no duplicate window.

A CDC stream only reports what *changed*. Without a baseline a downstream sink
can never reach a consistent state — it would be applying `UPDATE ... WHERE id=7`
to a table with no row 7. Snapshotting first loses the changes made in between;
streaming first duplicates rows the snapshot already had. The only correct answer
is PostgreSQL's *exported snapshot*, taken at exactly the slot's
`consistent_point`, and this example wraps that.

**Features:**
- `ReplicationStreamConfig::with_initial_snapshot(true)` — the only new knob
- One `WalRouter` with **one** set of handlers drives both phases; snapshot rows
  arrive as ordinary `ChangeEvent`s
- Proves the handoff rather than claiming it: a row is inserted *after* the
  snapshot is exported but *before* streaming starts — the exact window a naive
  implementation gets wrong — and must arrive on the stream exactly once
- Shows that calling `start()` mid-snapshot is a **compile error**, not a runtime
  one (the stream is moved into the snapshot handle)

**Run:**
```bash
cd examples/initial-snapshot
export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/postgres?replication=database"
export DATABASE_URL_REGULAR="postgresql://postgres:postgres@localhost:5432/postgres"
cargo run
```

The example creates and drops its own table, publication and slot, so it is safe
to re-run. Expected output:

```text
seeded example_snapshot_users with 2 rows that exist BEFORE replication starts
snapshot ready: 1 table(s) at LSN 0/1560818
inserted 'carol' during the handoff window
  [snapshot] id=1 name=alice
  [snapshot] id=2 name=bob
streaming from the snapshot's consistent point
  [stream]   id=3 name=carol-inserted-mid-handoff
snapshot delivered 2 row(s)  (expected 2: alice, bob)
stream   delivered 1 row(s)  (expected 1: carol)
OK — no gap, no duplicate: every row arrived exactly once
```

Requires PostgreSQL 15+.

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