use pg_walstream_loadtest::metrics::Metrics;
use hdrhistogram::Histogram;
use pg_walstream::{
    CancellationToken, ColumnValue, EventType, LogicalReplicationStream, ReplicationSlotOptions,
    ReplicationStreamConfig, RetryConfig, StreamingMode,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Configuration for the WAL consumer.
pub struct ConsumerConfig {
    pub repl_conn_string: String,
    pub slot_name: String,
    pub publication_name: String,
}

/// Run the WAL consumer, collecting events until `stop` is set or `duration` elapses.
/// Returns the inter-event latency histogram.
pub async fn run_consumer(
    config: ConsumerConfig,
    metrics: Metrics,
    stop: Arc<AtomicBool>,
    warmup_done_signal: Arc<AtomicBool>,
    warmup_duration: Duration,
    measure_duration: Duration,
) -> Histogram<u64> {
    let stream_config = ReplicationStreamConfig::new(
        config.slot_name.clone(),
        config.publication_name.clone(),
        2,
        StreamingMode::On,
        Duration::from_secs(5),
        Duration::from_secs(30),
        Duration::from_secs(60),
        RetryConfig::default(),
    )
    .with_slot_options(ReplicationSlotOptions {
        temporary: true,
        ..Default::default()
    });

    let mut stream = LogicalReplicationStream::new(&config.repl_conn_string, stream_config)
        .await
        .expect("Failed to create replication stream");

    stream
        .start(None)
        .await
        .expect("Failed to start replication");

    let cancel_token = CancellationToken::new();
    let mut event_stream = stream.into_stream(cancel_token.clone());

    // Inter-event latency histogram (in microseconds, range 1us - 10s)
    let mut hist = Histogram::<u64>::new_with_bounds(1, 10_000_000, 3).unwrap();
    let mut last_event_time: Option<Instant> = None;

    let total_start = Instant::now();
    let mut warmup_done = false;
    let mut measure_start = Instant::now();

    println!(
        "  Consumer: warming up for {}s...",
        warmup_duration.as_secs()
    );

    loop {
        if stop.load(Ordering::Relaxed) && warmup_done {
            // Check if we've been idle too long (no more events coming)
            if let Some(last) = last_event_time {
                if last.elapsed() > Duration::from_secs(3) {
                    break;
                }
            }
        }

        // Check if measurement time is up
        if warmup_done && measure_start.elapsed() >= measure_duration {
            break;
        }

        // Check warmup
        if !warmup_done && total_start.elapsed() >= warmup_duration {
            warmup_done = true;
            warmup_done_signal.store(true, Ordering::Relaxed);
            metrics.reset();
            hist.clear();
            last_event_time = None;
            measure_start = Instant::now();
            println!(
                "  Consumer: warmup complete, measuring for {}s...",
                measure_duration.as_secs()
            );
        }

        match tokio::time::timeout(Duration::from_secs(5), event_stream.next_event()).await {
            Ok(Ok(event)) => {
                let now = Instant::now();

                // Record inter-event latency
                if warmup_done {
                    if let Some(last) = last_event_time {
                        let elapsed_us = now.duration_since(last).as_micros() as u64;
                        let _ = hist.record(elapsed_us.max(1));
                    }
                }
                last_event_time = Some(now);

                let is_dml = matches!(
                    event.event_type,
                    EventType::Insert { .. }
                        | EventType::Update { .. }
                        | EventType::Delete { .. }
                );

                // Estimate event size (rough)
                let event_bytes = estimate_event_size(&event.event_type);

                if warmup_done {
                    metrics.record_event(is_dml, event_bytes);
                }

                event_stream.update_applied_lsn(event.lsn.value());
            }
            Ok(Err(e)) => {
                if matches!(e, pg_walstream::ReplicationError::Cancelled(_)) {
                    break;
                }
                eprintln!("  Consumer error: {e}");
                break;
            }
            Err(_) => {
                // Timeout waiting for event - check if we should stop
                if stop.load(Ordering::Relaxed) {
                    break;
                }
            }
        }
    }

    cancel_token.cancel();
    let _ = event_stream.shutdown().await;
    hist
}

fn column_value_size(v: &ColumnValue) -> usize {
    match v {
        ColumnValue::Null => 0,
        ColumnValue::Text(b) | ColumnValue::Binary(b) => b.len(),
    }
}

fn estimate_event_size(event_type: &EventType) -> u64 {
    match event_type {
        EventType::Insert { data, table, .. } => {
            let row_size: usize = data
                .iter()
                .map(|(k, v)| k.len() + column_value_size(v))
                .sum();
            (row_size + table.len() + 50) as u64
        }
        EventType::Update {
            new_data, table, ..
        } => {
            let row_size: usize = new_data
                .iter()
                .map(|(k, v)| k.len() + column_value_size(v))
                .sum();
            (row_size + table.len() + 80) as u64
        }
        EventType::Delete { table, .. } => (table.len() + 60) as u64,
        EventType::Begin { .. } | EventType::Commit { .. } => 30,
        EventType::Relation { .. } => 100,
        _ => 20,
    }
}
