//! Reading a replication stream's metrics from other tasks.
//!
//! `stream.metrics()` (or `event_stream.metrics()`) returns a [`StreamMetrics`]: a cheap,
//! cloneable, `Send + Sync` handle on the stream's counters. Hand a clone to any task, call
//! [`snapshot`](StreamMetrics::snapshot) whenever you want the numbers, and the stream does not
//! have to be polled for that, nor does it slow down: it only writes a few relaxed atomics.
//!
//! A [`MetricsSnapshot`] is a plain `Copy` struct of totals, so a rate is the difference of two
//! snapshots. `lag_bytes()` is the WAL read off the wire and not acknowledged yet. The stream
//! reads on demand, so a consumer that falls behind leaves its backlog in the socket buffer and
//! on the server, which `lag_bytes()` does not see: it shows the work in flight, here the open
//! transaction, because this example acknowledges at each commit.
//!
//! This example:
//!
//! - consumes the stream and acknowledges each transaction at its `Commit`;
//! - spawns a reporter task that prints rates, unacknowledged bytes and reconnects;
//! - optionally serves the same numbers in the Prometheus text format;
//! - keeps reading the handle after the stream has ended, for a final summary.
//!
//! ## Prerequisites
//!
//! 1. PostgreSQL 14+ with `wal_level = logical`
//! 2. A publication: `CREATE PUBLICATION metrics_example_pub FOR ALL TABLES;`
//!
//! ## Usage
//!
//! ```bash
//! export DATABASE_URL="postgresql://postgres:password@localhost:5432/postgres?replication=database"
//! cargo run
//!
//! # A slow handler (20 ms per event) makes a large transaction visible as the unacknowledged
//! # bytes climbing until its commit, and the endpoint can be scraped meanwhile:
//! HANDLER_DELAY_MS=20 METRICS_ADDR=127.0.0.1:9187 cargo run
//! curl -s http://127.0.0.1:9187/metrics
//! ```
//!
//! Environment: `DATABASE_URL`, `SLOT`, `PUBLICATION`, `REPORT_SECS` (default 5),
//! `HANDLER_DELAY_MS` (default 0), `METRICS_ADDR` (unset: no endpoint).

use pg_walstream::{
    format_lsn, CancellationToken, EventType, LogicalReplicationStream, MetricsSnapshot,
    ReplicationError, ReplicationStreamConfig, StreamMetrics,
};
use std::env;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn env_or(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// The snapshot in the Prometheus text exposition format: totals as counters, positions and lag
/// as gauges.
fn render_prometheus(m: &MetricsSnapshot) -> String {
    let counters = [
        ("wal_messages_total", m.wal_messages),
        ("wal_bytes_total", m.wal_bytes),
        ("keepalives_total", m.keepalives),
        ("feedback_sent_total", m.feedback_sent),
        ("reconnects_total", m.reconnects),
    ];
    let gauges = [
        ("received_lsn", m.received_lsn),
        ("flushed_lsn", m.flushed_lsn),
        ("applied_lsn", m.applied_lsn),
        ("lag_bytes", m.lag_bytes()),
    ];
    let mut out = String::new();
    for (kind, series) in [("counter", &counters[..]), ("gauge", &gauges[..])] {
        for (name, value) in series {
            out.push_str(&format!(
                "# TYPE pg_walstream_{name} {kind}\npg_walstream_{name} {value}\n"
            ));
        }
    }
    out
}

/// Print one line every `every`: totals, rates from the difference of two snapshots, and the
/// unacknowledged bytes.
async fn report(metrics: StreamMetrics, every: Duration, cancel: CancellationToken) {
    let mut previous = metrics.snapshot();
    let mut last = Instant::now();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(every) => {}
        }
        let now = metrics.snapshot();
        let secs = last.elapsed().as_secs_f64();
        last = Instant::now();
        println!(
            "msgs {:>8} ({:>8.1}/s)  {:>10} B ({:>9.1} KiB/s)  unacked {:>8} B  received {}  applied {}  \
             keepalives {}  feedback {}  reconnects {}",
            now.wal_messages,
            (now.wal_messages - previous.wal_messages) as f64 / secs,
            now.wal_bytes,
            (now.wal_bytes - previous.wal_bytes) as f64 / 1024.0 / secs,
            now.lag_bytes(),
            format_lsn(now.received_lsn),
            format_lsn(now.applied_lsn),
            now.keepalives,
            now.feedback_sent,
            now.reconnects,
        );
        previous = now;
    }
}

/// Answer every connection with the current metrics. A real service would route on the path and
/// use its web framework's metrics middleware; the point here is that the handler only needs a
/// clone of the handle.
async fn serve(addr: String, metrics: StreamMetrics) -> std::io::Result<()> {
    let listener = TcpListener::bind(&addr).await?;
    println!("Prometheus metrics on http://{addr}/metrics");
    loop {
        let (mut socket, _) = listener.accept().await?;
        let metrics = metrics.clone();
        tokio::spawn(async move {
            let mut request = [0u8; 1024];
            let _ = socket.read(&mut request).await;
            let body = render_prometheus(&metrics.snapshot());
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
        });
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = env_or(
        "DATABASE_URL",
        "postgresql://postgres:password@localhost:5432/postgres?replication=database",
    );
    let config = ReplicationStreamConfig::builder(
        env_or("SLOT", "metrics_example_slot"),
        env_or("PUBLICATION", "metrics_example_pub"),
    )
    .with_feedback_interval(Duration::from_secs(1));

    let mut stream = LogicalReplicationStream::new(&url, config).await?;
    stream.start(None).await?;

    let cancel = CancellationToken::new();
    let mut events = stream.into_stream(cancel.clone());

    // Take the handle once and clone it per task. `stream.metrics()` before `into_stream`
    // returns the same counters.
    let metrics = events.metrics();
    tokio::spawn(report(
        metrics.clone(),
        Duration::from_secs(env_u64("REPORT_SECS", 5)),
        cancel.clone(),
    ));
    if let Ok(addr) = env::var("METRICS_ADDR") {
        let metrics = metrics.clone();
        tokio::spawn(async move {
            if let Err(e) = serve(addr, metrics).await {
                eprintln!("metrics endpoint stopped: {e}");
            }
        });
    }

    let on_ctrl_c = cancel.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        on_ctrl_c.cancel();
    });

    // Acknowledge at each commit, once every event of the transaction has been handled. A slow
    // handler then shows up as the unacknowledged bytes growing through a transaction and
    // dropping back at its commit.
    let delay = Duration::from_millis(env_u64("HANDLER_DELAY_MS", 0));
    loop {
        match events.next_event().await {
            Ok(event) => {
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                if let EventType::Commit { end_lsn, .. } = event.event_type {
                    events.update_applied_lsn(end_lsn);
                }
            }
            Err(ReplicationError::Cancelled(_)) => break,
            Err(e) => return Err(e.into()),
        }
    }

    // `update_applied_lsn` only moves the shared position; this sends it to the server.
    let _ = events.shutdown().await;

    // The handle outlives the stream.
    let last = metrics.snapshot();
    println!(
        "stopped: {} messages, {} bytes, {} bytes unacknowledged, {} reconnects",
        last.wal_messages,
        last.wal_bytes,
        last.lag_bytes(),
        last.reconnects
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prometheus_text_has_every_series_once() {
        let mut snapshot = MetricsSnapshot::default();
        snapshot.wal_messages = 7;
        snapshot.wal_bytes = 700;
        snapshot.keepalives = 2;
        snapshot.feedback_sent = 3;
        snapshot.reconnects = 1;
        snapshot.received_lsn = 5000;
        snapshot.flushed_lsn = 4000;
        snapshot.applied_lsn = 4000;
        let text = render_prometheus(&snapshot);
        assert!(text.contains("# TYPE pg_walstream_wal_messages_total counter\n"));
        assert!(text.contains("pg_walstream_wal_messages_total 7\n"));
        assert!(text.contains("# TYPE pg_walstream_lag_bytes gauge\n"));
        assert!(text.contains("pg_walstream_lag_bytes 1000\n"));
        assert_eq!(
            text.lines().count(),
            18,
            "9 series, a TYPE line and a value each"
        );
    }
}
