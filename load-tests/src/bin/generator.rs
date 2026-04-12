//! Standalone load generator — runs in a separate process so the consumer's
//! CPU/RSS can be measured in isolation.
//!
//! Usage:
//!   pg-walstream-generator <conn_string> <num_writers> <batch_size> \
//!       <payload_size> <use_wide_table> <insert_ratio> <update_ratio>
//!
//! Writes rows continuously until SIGTERM / SIGINT, then prints the total
//! rows written to stdout and exits.

use pg_walstream_loadtest::generator::{run_generator, GeneratorConfig, GeneratorStats};

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

fn parse_args() -> GeneratorConfig {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 8 {
        eprintln!(
            "Usage: {} <conn_string> <num_writers> <batch_size> \
             <payload_size> <use_wide_table> <insert_ratio> <update_ratio>",
            args[0]
        );
        std::process::exit(1);
    }

    GeneratorConfig {
        conn_string: args[1].clone(),
        num_writers: args[2].parse().expect("invalid num_writers"),
        batch_size: args[3].parse().expect("invalid batch_size"),
        payload_size: args[4].parse().expect("invalid payload_size"),
        use_wide_table: args[5].parse().expect("invalid use_wide_table"),
        insert_ratio: args[6].parse().expect("invalid insert_ratio"),
        update_ratio: args[7].parse().expect("invalid update_ratio"),
    }
}

#[tokio::main]
async fn main() {
    let config = parse_args();

    let stop = Arc::new(AtomicBool::new(false));
    let stats = Arc::new(GeneratorStats {
        rows_written: AtomicU64::new(0),
    });

    // Install SIGTERM / SIGINT handler to gracefully stop
    let stop_signal = stop.clone();
    tokio::spawn(async move {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("failed to install SIGTERM handler");
        let sigint = tokio::signal::ctrl_c();

        tokio::select! {
            _ = sigterm.recv() => {}
            _ = sigint => {}
        }

        stop_signal.store(true, Ordering::Relaxed);
    });

    run_generator(config, stop, stats.clone()).await;

    // Print rows written so the parent can optionally read it
    println!("{}", stats.rows_written.load(Ordering::Relaxed));
}
