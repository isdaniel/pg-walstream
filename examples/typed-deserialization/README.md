# typed-deserialization

End-to-end example of streaming WAL events from a live PostgreSQL database and
deserializing each change into a user-defined Rust struct via serde.

It exercises the four main convenience methods on `ChangeEvent`:

| Event    | Method                               | Returns                       |
|----------|--------------------------------------|-------------------------------|
| INSERT   | `event.deserialize_insert::<T>()`    | `Result<T>`                   |
| UPDATE   | `event.deserialize_update::<T>()`    | `Result<(Option<T>, T)>` (old, new) |
| DELETE   | `event.deserialize_delete::<T>()`    | `Result<T>`                   |
| any row  | `row.deserialize_into::<T>()`        | `Result<T>`                   |
| lenient  | `row.try_deserialize_into::<T>()`    | `Result<TryDeserializeResult<T>>` — collects per-field errors instead of failing the row |

## Prerequisites

1. PostgreSQL 10+ with `wal_level = logical`, `max_replication_slots >= 1`,
   `max_wal_senders >= 1`.
2. A role that can connect with `replication=database` and create
   publications/slots.

## Run

```bash
cd examples/typed-deserialization
export DATABASE_URL='postgresql://postgres:pass@localhost:5432/postgres?replication=database&sslmode=require'
cargo run
```

The example will:

1. Create table `typed_deser_users`, publication `typed_deser_pub`, and a
   temporary replication slot `typed_deser_slot`.
2. Start the replication stream.
3. Fire an INSERT / UPDATE / DELETE against the table in a background task.
4. Print each decoded `User` struct as events arrive.
5. Drop the slot, publication, and table on exit (or on Ctrl+C).

## How deserialization works

PostgreSQL's pgoutput protocol sends every column value as a **text string**. `pg_walstream`'s serde `Deserializer` parses that text into your target Rust type automatically.

### 1. Define a struct matching the table

```rust
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct User {
    id: i64,
    username: String,
    email: Option<String>,   // nullable column
    score: f64,
    active: bool,
}
```

Field names must match column names. Numeric and boolean types are parsed from the text form PostgreSQL sends (`"true"`/`"t"`, `"42"`, `"3.14"`, etc.).

### 2. Deserialize events

```rust
match &event.event_type {
    EventType::Insert { .. } => {
        let user: User = event.deserialize_insert()?;
    }
    EventType::Update { .. } => {
        let (old, new): (Option<User>, User) = event.deserialize_update()?;
        // `old` is Some only when the table is set to REPLICA IDENTITY FULL
        // (or the primary-key/indexed columns changed).
    }
    EventType::Delete { .. } => {
        let user: User = event.deserialize_delete()?;
    }
    _ => {}
}
```

### 3. Useful serde attributes

```rust
#[derive(Deserialize)]
struct Row {
    #[serde(rename = "user_id")]
    id: i64,

    #[serde(default)]         // column may be missing from the row
    nickname: String,

    active: Option<bool>,     // maps NULL → None
}
```

### 4. Lenient mode — surviving type mismatches

`deserialize_into` fails the whole row on the first bad field. When you need to keep going and collect per-field errors, use `try_deserialize_into`:

```rust
use pg_walstream::TryDeserializeResult;

let result: TryDeserializeResult<User> = row.try_deserialize_into()?;
if !result.is_clean() {
    for err in &result.errors {
        eprintln!("field {}: {}", err.field, err.message);
    }
}
let user = result.value; // numeric/bool/string fields that failed hold their default
```

`try_deserialize_into` substitutes defaults for scalar parse failures (`0` for numbers, `false` for bool, `""` for strings) and records a
`FieldError` per failure. Structural failures (enum variants, nested structs) still return an outer error.

### 5. Using `try_deserialize_into` inside the `next_event()` loop

When you cannot afford to drop a row on the first bad field — e.g. a schema drift between a legacy producer and your newer consumer — plug `try_deserialize_into` into the existing event loop. Pull the `RowData` out of the event's `EventType`, run the lenient deserializer, log any `FieldError`s, and keep going.

```rust
use pg_walstream::{EventType, TryDeserializeResult};

loop {
    let event = event_stream.next_event().await?;

    match &event.event_type {
        // INSERT: one row (`new`) is present.
        EventType::Insert { table, new, .. } => {
            let result: TryDeserializeResult<User> = new.try_deserialize_into()?;
            log_field_errors(table, "INSERT", &result.errors);
            handle_user(result.value);
        }

        // UPDATE: `old` is Some only under REPLICA IDENTITY FULL / key change.
        EventType::Update { table, old, new, .. } => {
            if let Some(old_row) = old {
                let old_res: TryDeserializeResult<User> = old_row.try_deserialize_into()?;
                log_field_errors(table, "UPDATE-old", &old_res.errors);
            }
            let new_res: TryDeserializeResult<User> = new.try_deserialize_into()?;
            log_field_errors(table, "UPDATE-new", &new_res.errors);
            handle_user(new_res.value);
        }

        // DELETE: `old` carries the pre-image (PK only unless REPLICA IDENTITY FULL).
        EventType::Delete { table, old: Some(old_row), .. } => {
            let result: TryDeserializeResult<User> = old_row.try_deserialize_into()?;
            log_field_errors(table, "DELETE", &result.errors);
            handle_user(result.value);
        }

        _ => {}
    }

    event_stream.update_applied_lsn(event.lsn.value());
}

fn log_field_errors(table: &str, op: &str, errors: &[pg_walstream::FieldError]) {
    for e in errors {
        eprintln!("[{op}] {table}.{}: {}", e.field, e.message);
    }
}
```

Prefer `try_deserialize_into` only on fields where a default is semantically
safe. If any field is load-bearing (e.g. an `id` you use as a primary key
downstream), inspect `result.errors` for that field and skip or dead-letter
the event instead of accepting the default.

## Enable REPLICA IDENTITY FULL to get old-row data on UPDATE/DELETE

By default PostgreSQL only includes the primary key in UPDATE/DELETE events.
To see full old rows:

```sql
ALTER TABLE typed_deser_users REPLICA IDENTITY FULL;
```

The example already does this during setup.

## Related

- [`src/deserializer.rs`](../../src/deserializer.rs) — the serde Deserializer
  implementation.
- [Top-level examples README](../README.md) — index of every example in the
  repository.
