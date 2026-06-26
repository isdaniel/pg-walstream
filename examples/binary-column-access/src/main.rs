//! Demonstrates `PgResult::get_bytes` for lossless, zero-copy access to binary
//! columns — the pattern a replication extension uses to consume `BYTEA`
//! without round-tripping queries through `encode(data, 'hex')`.
//!
//! Run against a regular PostgreSQL connection:
//!
//! ```bash
//! export DATABASE_URL_REGULAR="postgresql://postgres:postgres@localhost:5432/postgres"
//! cd examples/binary-column-access
//! cargo run
//! ```

use pg_walstream::PgReplicationConnection;

fn main() {
    let conn_string = std::env::var("DATABASE_URL_REGULAR")
        .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost:5432/postgres".to_string());

    let mut conn =
        PgReplicationConnection::connect(&conn_string).expect("failed to connect to PostgreSQL");

    // A BYTEA value whose bytes are NOT valid UTF-8.
    let result = conn
        .exec("SELECT '\\xDEADBEEF'::bytea")
        .expect("query failed");

    let lossy = result.get_value(0, 0).expect("value present");
    let raw = result.get_bytes(0, 0).expect("value present");

    println!("get_value (String, lossy):    {lossy:?}");
    println!("get_bytes (&[u8], lossless):   {raw:?}");
    println!("byte length via get_bytes:     {}", raw.len());
    println!(
        "\nWith get_bytes the extension reads the column's bytes directly — \
         no `encode(data, 'hex')` wrapper, no UTF-8 corruption."
    );
}
