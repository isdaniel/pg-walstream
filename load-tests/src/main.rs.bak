mod consumer;
mod metrics;
mod reporter;
mod sampler;

use consumer::{run_consumer, ConsumerConfig};
use pg_walstream_loadtest::generator::GeneratorConfig;
use metrics::{Metrics, ScenarioResult};
use reporter::generate_report;
use sampler::ResourceSampler;

use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

// ─── Configuration ──────────────────────────────────────────────────────────

const DEFAULT_HOST: &str = "localhost";
const DEFAULT_PORT: u16 = 5433;
const DEFAULT_DB: &str = "loadtest";
const DEFAULT_USER: &str = "postgres";
const DEFAULT_PASS: &str = "postgres";
const PUBLICATION: &str = "loadtest_pub";

const WARMUP_SECS: u64 = 10;
const MEASURE_SECS: u64 = 30;

/// Parsed database connection configuration.
#[derive(Clone, Debug)]
struct DbConfig {
    host: String,
    port: u16,
    db: String,
    user: String,
    pass: String,
    sslmode: String,
    is_remote: bool,
}

/// Parse DATABASE_URL env var, or fall back to localhost defaults.
fn parse_db_config() -> DbConfig {
    if let Ok(url) = std::env::var("DATABASE_URL") {
        // Parse URI: postgresql://user:pass@host:port/db?params
        let url = url
            .trim_start_matches("postgresql://")
            .trim_start_matches("postgres://");

        // Split on @ to get credentials and host
        let (creds, rest) = url.split_once('@').unwrap_or(("postgres:postgres", url));
        let (user, pass) = creds.split_once(':').unwrap_or((creds, ""));

        // Split rest on / to get host:port and db?params
        let (host_port, db_params) = rest.split_once('/').unwrap_or((rest, DEFAULT_DB));
        let (host, port) = if host_port.contains(':') {
            let (h, p) = host_port.rsplit_once(':').unwrap();
            (h, p.parse::<u16>().unwrap_or(5432))
        } else {
            (host_port, 5432)
        };

        // Split db from query params
        let (db, params) = db_params.split_once('?').unwrap_or((db_params, ""));

        // Parse sslmode from params
        let mut sslmode = "prefer".to_string();
        for param in params.split('&') {
            if let Some(val) = param.strip_prefix("sslmode=") {
                sslmode = val.to_string();
            }
        }

        let is_remote = host != "localhost" && host != "127.0.0.1";

        println!("  DB Config: host={host}, port={port}, db={db}, user={user}, ssl={sslmode}, remote={is_remote}");

        DbConfig {
            host: host.to_string(),
            port,
            db: db.to_string(),
            user: user.to_string(),
            pass: pass.to_string(),
            sslmode,
            is_remote,
        }
    } else {
        println!("  DB Config: using localhost defaults (set DATABASE_URL to override)");
        DbConfig {
            host: DEFAULT_HOST.to_string(),
            port: DEFAULT_PORT,
            db: DEFAULT_DB.to_string(),
            user: DEFAULT_USER.to_string(),
            pass: DEFAULT_PASS.to_string(),
            sslmode: "prefer".to_string(),
            is_remote: false,
        }
    }
}

fn repl_conn_string(cfg: &DbConfig) -> String {
    format!(
        "postgresql://{}:{}@{}:{}/{}?replication=database&sslmode={}",
        cfg.user, cfg.pass, cfg.host, cfg.port, cfg.db, cfg.sslmode
    )
}

fn regular_conn_string(cfg: &DbConfig) -> String {
    format!(
        "host={} port={} dbname={} user={} password={} sslmode={}",
        cfg.host, cfg.port, cfg.db, cfg.user, cfg.pass, cfg.sslmode
    )
}

// ─── Helpers ────────────────────────────────────────────────────────────────

async fn pg_connect(conn_str: &str) -> tokio_postgres::Client {
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_verify(SslVerifyMode::NONE);
    let tls = MakeTlsConnector::new(builder.build());
    let (client, conn) = tokio_postgres::connect(conn_str, tls)
        .await
        .expect("Failed to connect to PostgreSQL");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
}

/// Create tables and publication on the target database (idempotent).
async fn setup_database(cfg: &DbConfig) {
    println!("  Setting up database schema...");
    let conn_str = regular_conn_string(cfg);
    let client = pg_connect(&conn_str).await;

    // Create tables
    let _ = client
        .execute(
            "CREATE TABLE IF NOT EXISTS narrow_events (
                id SERIAL PRIMARY KEY,
                payload TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT now()
            )",
            &[],
        )
        .await;

    let mut wide_sql = String::from(
        "CREATE TABLE IF NOT EXISTS wide_events (id SERIAL PRIMARY KEY, ",
    );
    for i in 1..=20 {
        wide_sql.push_str(&format!("col_{i} TEXT"));
        if i < 20 {
            wide_sql.push_str(", ");
        }
    }
    wide_sql.push(')');
    let _ = client.execute(&wide_sql as &str, &[]).await;

    // Set REPLICA IDENTITY FULL for narrow_events (needed for UPDATE/DELETE replication)
    let _ = client
        .execute(
            "ALTER TABLE narrow_events REPLICA IDENTITY FULL",
            &[],
        )
        .await;

    // Create publication (ignore error if already exists)
    let pub_exists = client
        .query(
            "SELECT 1 FROM pg_publication WHERE pubname = $1",
            &[&PUBLICATION],
        )
        .await
        .map(|rows| !rows.is_empty())
        .unwrap_or(false);

    if !pub_exists {
        let pub_sql = format!(
            "CREATE PUBLICATION {PUBLICATION} FOR TABLE narrow_events, wide_events"
        );
        match client.execute(&pub_sql as &str, &[]).await {
            Ok(_) => println!("  Created publication: {PUBLICATION}"),
            Err(e) => println!("  Publication creation note: {e}"),
        }
    } else {
        println!("  Publication already exists: {PUBLICATION}");
    }

    println!("  Database setup complete.");
}

async fn cleanup_tables(cfg: &DbConfig) {
    let conn_str = regular_conn_string(cfg);
    let client = pg_connect(&conn_str).await;
    let _ = client
        .execute("TRUNCATE narrow_events RESTART IDENTITY", &[])
        .await;
    let _ = client
        .execute("TRUNCATE wide_events RESTART IDENTITY", &[])
        .await;
}

async fn drop_slot(cfg: &DbConfig, slot_name: &str) {
    let conn_str = regular_conn_string(cfg);
    let client = pg_connect(&conn_str).await;
    let sql = format!(
        "SELECT pg_drop_replication_slot('{slot_name}') \
         WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot_name}')"
    );
    let _ = client.execute(&sql as &str, &[]).await;
}

/// Run a single scenario: generator (child process) + consumer + resource sampler.
///
/// The generator runs as a **separate OS process** so that the `sysinfo` sampler
/// measures only the consumer (pg-walstream library) CPU/RSS.
async fn run_scenario(
    name: &str,
    description: &str,
    gen_config: GeneratorConfig,
    cfg: &DbConfig,
) -> ScenarioResult {
    let slot_name = format!(
        "loadtest_{}",
        name.replace(' ', "_").replace('-', "_").to_lowercase()
    );

    println!();
    println!("============================================================");
    println!("Scenario: {name}");
    println!("  {description}");
    println!("  Slot: {slot_name}");
    println!("============================================================");

    // Cleanup
    drop_slot(cfg, &slot_name).await;
    cleanup_tables(cfg).await;

    // Longer delay for remote DB to settle
    let settle_secs = if cfg.is_remote { 3 } else { 1 };
    tokio::time::sleep(Duration::from_secs(settle_secs)).await;

    let metrics = Metrics::new();
    let stop = Arc::new(AtomicBool::new(false));
    let warmup_done = Arc::new(AtomicBool::new(false));

    // Start resource sampler
    let resource_sampler = ResourceSampler::start(metrics.clone(), warmup_done.clone());

    // Start consumer first (creates slot, starts replication)
    let consumer_config = ConsumerConfig {
        repl_conn_string: repl_conn_string(cfg),
        slot_name: slot_name.clone(),
        publication_name: PUBLICATION.to_string(),
    };

    let consumer_metrics = metrics.clone();
    let consumer_stop = stop.clone();
    let consumer_warmup = warmup_done.clone();
    let warmup_dur = Duration::from_secs(WARMUP_SECS);
    let measure_dur = Duration::from_secs(MEASURE_SECS);
    let consumer_handle = tokio::spawn(async move {
        run_consumer(
            consumer_config,
            consumer_metrics,
            consumer_stop,
            consumer_warmup,
            warmup_dur,
            measure_dur,
        )
        .await
    });

    // Give consumer more time for remote connections
    let consumer_wait = if cfg.is_remote { 5 } else { 2 };
    tokio::time::sleep(Duration::from_secs(consumer_wait)).await;

    // ── Spawn generator as a SEPARATE PROCESS ──
    // This isolates generator CPU from the consumer process so sysinfo
    // measures only pg-walstream library overhead.
    let generator_bin = std::env::current_exe()
        .expect("cannot determine current exe path")
        .parent()
        .expect("exe has no parent dir")
        .join("pg-walstream-generator");

    let gen_args = [
        gen_config.conn_string.as_str(),
        &gen_config.num_writers.to_string(),
        &gen_config.batch_size.to_string(),
        &gen_config.payload_size.to_string(),
        &gen_config.use_wide_table.to_string(),
        &gen_config.insert_ratio.to_string(),
        &gen_config.update_ratio.to_string(),
    ];

    let mut generator_child = tokio::process::Command::new(&generator_bin)
        .args(gen_args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .unwrap_or_else(|e| {
            panic!(
                "Failed to spawn generator process at {}: {e}",
                generator_bin.display()
            )
        });

    println!(
        "  Generator: spawned child process (PID {}, {} writers)",
        generator_child.id().unwrap_or(0),
        gen_config.num_writers
    );

    // Wait for consumer to finish measurement
    let hist = consumer_handle.await.expect("consumer panicked");

    // Signal generator to stop and collect row count
    generator_child.kill().await.ok();
    let gen_output = generator_child.wait_with_output().await.ok();
    let rows_written: u64 = gen_output
        .as_ref()
        .and_then(|o| std::str::from_utf8(&o.stdout).ok())
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0);

    // Collect resource samples
    let resources = resource_sampler.stop().await;

    let duration_secs = MEASURE_SECS as f64;
    let result = ScenarioResult::from_metrics_and_histogram(
        name,
        description,
        &metrics,
        duration_secs,
        &hist,
        resources,
    );

    println!(
        "  Result: {:.0} total events/s, {:.0} DML events/s, {:.2} MB/s",
        result.events_per_sec, result.dml_events_per_sec, result.mb_per_sec
    );
    println!(
        "  Latency: P50={:.0}us P95={:.0}us P99={:.0}us P99.9={:.0}us",
        result.p50_inter_event_us,
        result.p95_inter_event_us,
        result.p99_inter_event_us,
        result.p999_inter_event_us
    );
    println!(
        "  CPU: avg={:.1}% peak={:.1}% (consumer-only), sys avg={:.1}% peak={:.1}%",
        result.resources.avg_process_cpu_pct,
        result.resources.peak_process_cpu_pct,
        result.resources.avg_system_cpu_pct,
        result.resources.peak_system_cpu_pct,
    );
    println!(
        "  Memory: avg={:.1} MB peak={:.1} MB RSS (consumer-only)",
        result.resources.avg_rss_mb, result.resources.peak_rss_mb
    );
    println!("  Generator wrote {rows_written} rows (separate process)");

    // Cleanup slot
    drop_slot(cfg, &slot_name).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    result
}

// ─── Scenarios ──────────────────────────────────────────────────────────────

async fn scenario_baseline(cfg: &DbConfig) -> ScenarioResult {
    let config = GeneratorConfig {
        conn_string: regular_conn_string(cfg),
        num_writers: 1,
        batch_size: 8000,
        payload_size: 100,
        use_wide_table: false,
        insert_ratio: 1.0,
        update_ratio: 0.0,
    };
    run_scenario(
        "Baseline",
        "Single writer, 8000-row batches, 100B payload, narrow table (3 cols)",
        config,
        cfg,
    )
    .await
}

async fn scenario_batch_100(cfg: &DbConfig) -> ScenarioResult {
    let config = GeneratorConfig {
        conn_string: regular_conn_string(cfg),
        num_writers: 1,
        batch_size: 100,
        payload_size: 100,
        use_wide_table: false,
        insert_ratio: 1.0,
        update_ratio: 0.0,
    };
    run_scenario(
        "Batch-100",
        "Single writer, 100-row batches, 100B payload",
        config,
        cfg,
    )
    .await
}

async fn scenario_batch_5000(cfg: &DbConfig) -> ScenarioResult {
    let config = GeneratorConfig {
        conn_string: regular_conn_string(cfg),
        num_writers: 1,
        batch_size: 5000,
        payload_size: 100,
        use_wide_table: false,
        insert_ratio: 1.0,
        update_ratio: 0.0,
    };
    run_scenario(
        "Batch-5000",
        "Single writer, 5000-row batches, 100B payload",
        config,
        cfg,
    )
    .await
}

async fn scenario_concurrent_4(cfg: &DbConfig) -> ScenarioResult {
    let config = GeneratorConfig {
        conn_string: regular_conn_string(cfg),
        num_writers: 4,
        batch_size: 8000,
        payload_size: 100,
        use_wide_table: false,
        insert_ratio: 1.0,
        update_ratio: 0.0,
    };
    run_scenario(
        "4-Writers",
        "4 concurrent writers, 8000-row batches, 100B payload",
        config,
        cfg,
    )
    .await
}

async fn scenario_wide_table(cfg: &DbConfig) -> ScenarioResult {
    let config = GeneratorConfig {
        conn_string: regular_conn_string(cfg),
        num_writers: 1,
        batch_size: 500,
        payload_size: 50,
        use_wide_table: true,
        insert_ratio: 1.0,
        update_ratio: 0.0,
    };
    run_scenario(
        "Wide-20col",
        "Single writer, 500-row batches, 20 columns x 50B payload, wide table",
        config,
        cfg,
    )
    .await
}

async fn scenario_large_payload(cfg: &DbConfig) -> ScenarioResult {
    let config = GeneratorConfig {
        conn_string: regular_conn_string(cfg),
        num_writers: 1,
        batch_size: 500,
        payload_size: 2000,
        use_wide_table: false,
        insert_ratio: 1.0,
        update_ratio: 0.0,
    };
    run_scenario(
        "Payload-2KB",
        "Single writer, 500-row batches, 2KB payload, narrow table",
        config,
        cfg,
    )
    .await
}

async fn scenario_mixed_dml(cfg: &DbConfig) -> ScenarioResult {
    let config = GeneratorConfig {
        conn_string: regular_conn_string(cfg),
        num_writers: 1,
        batch_size: 500,
        payload_size: 100,
        use_wide_table: false,
        insert_ratio: 0.5,
        update_ratio: 0.5,
    };
    run_scenario(
        "Mixed-DML",
        "Single writer, 500-row batches, 50% INSERT + 25% UPDATE + 25% DELETE",
        config,
        cfg,
    )
    .await
}

/// Stress test: progressively increase writer concurrency to find the saturation point.
async fn scenario_stress_ramp(cfg: &DbConfig) -> Vec<ScenarioResult> {
    let writer_counts = [16, 32, 48, 64, 96, 128, 192];
    let mut results = Vec::new();

    for &writers in &writer_counts {
        let config = GeneratorConfig {
            conn_string: regular_conn_string(cfg),
            num_writers: writers,
            batch_size: 10000,
            payload_size: 100,
            use_wide_table: false,
            insert_ratio: 1.0,
            update_ratio: 0.0,
        };
        let name = format!("Stress-{writers}w");
        let desc = format!(
            "Stress ramp: {writers} writer(s), 10000-row batches, 100B payload"
        );
        results.push(run_scenario(&name, &desc, config, cfg).await);
    }

    results
}

// ─── Main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    // Install the ring crypto provider for rustls before any TLS connections.
    // This is needed because the load test links both rustls (via pg_walstream)
    // and openssl (via postgres-openssl), and rustls needs an explicit provider.
    let _ = rustls::crypto::ring::default_provider().install_default();

    println!("============================================================");
    println!("         pg-walstream Load Test Suite                        ");
    println!("============================================================");
    println!();

    let cfg = parse_db_config();

    println!(
        "Config: warmup={}s, measure={}s per scenario",
        WARMUP_SECS, MEASURE_SECS
    );
    println!(
        "PostgreSQL: {}:{}/{}",
        cfg.host, cfg.port, cfg.db
    );
    if cfg.is_remote {
        println!("Mode: REMOTE (DB on separate server — measuring pure library overhead)");
    } else {
        println!("Mode: LOCAL (DB on same machine)");
    }
    println!();

    // Setup database schema on target
    setup_database(&cfg).await;

    let mut results = Vec::new();

    // Run core scenarios
    results.push(scenario_baseline(&cfg).await);
    results.push(scenario_batch_100(&cfg).await);
    results.push(scenario_batch_5000(&cfg).await);
    results.push(scenario_concurrent_4(&cfg).await);
    results.push(scenario_wide_table(&cfg).await);
    results.push(scenario_large_payload(&cfg).await);
    results.push(scenario_mixed_dml(&cfg).await);

    // Run stress ramp scenarios
    let stress_results = scenario_stress_ramp(&cfg).await;
    results.extend(stress_results);

    // Collect VM info
    let vm_info = collect_vm_info(&cfg);

    // Read bench output if available
    let bench_output = std::fs::read_to_string("/tmp/bench_output.txt").unwrap_or_else(|_| {
        "Benchmark output not available. Run: cargo bench --bench wal_pipeline".to_string()
    });

    // Generate report
    let report = generate_report(&results, &bench_output, &vm_info);

    // Write report
    let report_path = std::env::current_dir()
        .unwrap()
        .parent()
        .unwrap_or(&std::env::current_dir().unwrap())
        .join("LOAD_TEST_REPORT.md");
    std::fs::write(&report_path, &report).expect("Failed to write report");

    println!();
    println!("============================================================");
    println!("Report written to: {}", report_path.display());
    println!("============================================================");
}

fn collect_vm_info(cfg: &DbConfig) -> String {
    let mut info = String::new();
    if let Ok(cpuinfo) = std::fs::read_to_string("/proc/cpuinfo") {
        for line in cpuinfo.lines() {
            if line.starts_with("model name") {
                info.push_str(&format!(
                    "CPU: {}\n",
                    line.split(':').nth(1).unwrap_or("unknown").trim()
                ));
                break;
            }
        }
    }
    info.push_str(&format!("CPU cores: {}\n", num_cpus()));
    if let Ok(meminfo) = std::fs::read_to_string("/proc/meminfo") {
        for line in meminfo.lines() {
            if line.starts_with("MemTotal") {
                info.push_str(&format!(
                    "Memory: {}\n",
                    line.split(':').nth(1).unwrap_or("unknown").trim()
                ));
                break;
            }
        }
    }
    if let Ok(os_release) = std::fs::read_to_string("/etc/os-release") {
        for line in os_release.lines() {
            if line.starts_with("PRETTY_NAME") {
                let name = line.split('=').nth(1).unwrap_or("unknown").trim_matches('"');
                info.push_str(&format!("OS: {name}\n"));
                break;
            }
        }
    }
    if let Ok(version) = std::fs::read_to_string("/proc/version") {
        let kernel = version.split_whitespace().nth(2).unwrap_or("unknown");
        info.push_str(&format!("Kernel: {kernel}\n"));
    }
    if cfg.is_remote {
        info.push_str(&format!(
            "PostgreSQL: Remote Azure ({} port {})\n",
            cfg.host, cfg.port
        ));
        info.push_str("Mode: Remote DB (measuring pure library overhead)\n");
    } else {
        info.push_str(&format!(
            "PostgreSQL: localhost port {}\n",
            cfg.port
        ));
    }
    info
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}
