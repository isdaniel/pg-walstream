use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio_postgres::Client;

/// Configuration for the load generator.
#[derive(Debug, Clone)]
pub struct GeneratorConfig {
    pub conn_string: String,
    pub num_writers: usize,
    pub batch_size: usize,
    pub payload_size: usize,
    pub use_wide_table: bool,
    /// Fraction of INSERTs (vs UPDATE/DELETE). 1.0 = all inserts.
    pub insert_ratio: f64,
    /// Fraction of UPDATEs among non-insert DML.
    pub update_ratio: f64,
}

/// Tracks total rows inserted across all writers.
pub struct GeneratorStats {
    pub rows_written: AtomicU64,
}

/// Create an OpenSSL TLS connector that accepts any certificate.
/// Works for both SSL-required (Azure) and non-SSL (localhost) connections.
pub fn make_tls_connector() -> MakeTlsConnector {
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_verify(SslVerifyMode::NONE);
    MakeTlsConnector::new(builder.build())
}

/// Run the load generator until `stop` is set to true.
pub async fn run_generator(
    config: GeneratorConfig,
    stop: Arc<AtomicBool>,
    stats: Arc<GeneratorStats>,
) {
    let mut handles = Vec::new();
    for writer_id in 0..config.num_writers {
        let config = config.clone();
        let stop = stop.clone();
        let stats = stats.clone();
        handles.push(tokio::spawn(async move {
            writer_loop(writer_id, config, stop, stats).await;
        }));
    }
    for h in handles {
        let _ = h.await;
    }
}

async fn connect_regular(conn_string: &str) -> Client {
    let tls = make_tls_connector();
    let (client, connection) = tokio_postgres::connect(conn_string, tls)
        .await
        .expect("Failed to connect to PostgreSQL for load generation");
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("  generator connection error: {e}");
        }
    });
    client
}

async fn writer_loop(
    writer_id: usize,
    config: GeneratorConfig,
    stop: Arc<AtomicBool>,
    stats: Arc<GeneratorStats>,
) {
    let client = connect_regular(&config.conn_string).await;
    let mut rng = StdRng::seed_from_u64(writer_id as u64 + 42);

    // Pre-generate payload template
    let payload: String = (0..config.payload_size)
        .map(|_| rng.gen_range(b'a'..=b'z') as char)
        .collect();

    while !stop.load(Ordering::Relaxed) {
        if config.use_wide_table {
            write_wide_batch(&client, &config, &payload, &stats).await;
        } else if config.insert_ratio >= 1.0 {
            write_narrow_insert_batch(&client, &config, &payload, &stats).await;
        } else {
            write_mixed_dml_batch(&client, &config, &payload, &mut rng, &stats).await;
        }
    }
}

async fn write_narrow_insert_batch(
    client: &Client,
    config: &GeneratorConfig,
    payload: &str,
    stats: &GeneratorStats,
) {
    // Use a single multi-row INSERT for efficiency
    let mut sql = String::with_capacity(config.batch_size * (config.payload_size + 30));
    sql.push_str("INSERT INTO narrow_events (payload) VALUES ");
    for i in 0..config.batch_size {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str("('");
        sql.push_str(payload);
        sql.push_str("')");
    }

    match client.execute(&sql as &str, &[]).await {
        Ok(_) => {
            stats
                .rows_written
                .fetch_add(config.batch_size as u64, Ordering::Relaxed);
        }
        Err(e) => {
            eprintln!("  insert error: {e}");
        }
    }
}

async fn write_wide_batch(
    client: &Client,
    config: &GeneratorConfig,
    payload: &str,
    stats: &GeneratorStats,
) {
    let cols: Vec<String> = (1..=20).map(|i| format!("col_{i}")).collect();
    let col_list = cols.join(", ");

    let mut sql = String::with_capacity(config.batch_size * (config.payload_size * 20 + 100));
    sql.push_str(&format!("INSERT INTO wide_events ({col_list}) VALUES "));
    for i in 0..config.batch_size {
        if i > 0 {
            sql.push(',');
        }
        sql.push('(');
        for c in 0..20 {
            if c > 0 {
                sql.push(',');
            }
            sql.push('\'');
            sql.push_str(payload);
            sql.push('\'');
        }
        sql.push(')');
    }

    match client.execute(&sql as &str, &[]).await {
        Ok(_) => {
            stats
                .rows_written
                .fetch_add(config.batch_size as u64, Ordering::Relaxed);
        }
        Err(e) => {
            eprintln!("  wide insert error: {e}");
        }
    }
}

/// Batched mixed DML: INSERT a batch, then batch UPDATE and DELETE in single statements.
/// This avoids per-row network round trips which are devastating for remote DB latency.
async fn write_mixed_dml_batch(
    client: &Client,
    config: &GeneratorConfig,
    payload: &str,
    rng: &mut StdRng,
    stats: &GeneratorStats,
) {
    // First, insert a batch to have rows to update/delete
    let mut sql = String::with_capacity(config.batch_size * (config.payload_size + 30));
    sql.push_str("INSERT INTO narrow_events (payload) VALUES ");
    for i in 0..config.batch_size {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str("('");
        sql.push_str(payload);
        sql.push_str("')");
    }
    sql.push_str(" RETURNING id");

    match client.query(&sql as &str, &[]).await {
        Ok(rows) => {
            stats
                .rows_written
                .fetch_add(rows.len() as u64, Ordering::Relaxed);

            // Partition IDs into update and delete sets
            let mut update_ids = Vec::new();
            let mut delete_ids = Vec::new();

            for row in &rows {
                let id: i32 = row.get(0);
                let r: f64 = rng.gen();
                if r < config.insert_ratio {
                    continue; // Already inserted, no further DML
                } else if r
                    < config.insert_ratio + (1.0 - config.insert_ratio) * config.update_ratio
                {
                    update_ids.push(id);
                } else {
                    delete_ids.push(id);
                }
            }

            // Batch UPDATE in a single statement
            if !update_ids.is_empty() {
                let id_list: String = update_ids
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                let update_sql = format!(
                    "UPDATE narrow_events SET payload = '{payload}_upd' WHERE id IN ({id_list})"
                );
                match client.execute(&update_sql as &str, &[]).await {
                    Ok(n) => {
                        stats.rows_written.fetch_add(n, Ordering::Relaxed);
                    }
                    Err(e) => eprintln!("  batch update error: {e}"),
                }
            }

            // Batch DELETE in a single statement
            if !delete_ids.is_empty() {
                let id_list: String = delete_ids
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                let delete_sql =
                    format!("DELETE FROM narrow_events WHERE id IN ({id_list})");
                match client.execute(&delete_sql as &str, &[]).await {
                    Ok(n) => {
                        stats.rows_written.fetch_add(n, Ordering::Relaxed);
                    }
                    Err(e) => eprintln!("  batch delete error: {e}"),
                }
            }
        }
        Err(e) => {
            eprintln!("  mixed DML error: {e}");
        }
    }
}
