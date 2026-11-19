//! Reading a replication stream's metrics from other tasks.
//!
//! `stream.metrics()` (or `event_stream.metrics()`) returns a [`StreamMetrics`]: a cheap,
//! cloneable, `Send + Sync` handle on the stream's counters. Hand a clone to any task, call
//! [`snapshot`](StreamMetrics::snapshot) whenever you want the numbers, and the stream does not
//! have to be polled for that, nor does it slow down: it only writes a few relaxed atomics.
//!
//! A [`MetricsSnapshot`] is a plain `Copy` struct of totals, so a rate is the difference of two
//! snapshots. `lag_bytes()` is the WAL, as a distance in positions, between the last data frame
//! and what the consumer has marked applied. The stream reads on demand, so a consumer that falls
//! behind leaves its backlog in the socket buffer and on the server, which `lag_bytes()` does not
//! see: it shows the work in flight, here the open transaction, because this example acknowledges
//! at each commit. `unconfirmed_bytes()` also counts the server's keepalive positions, so it grows
//! on an idle stream while the rest of the database writes WAL; what the server retains for the
//! slot is only visible in `pg_replication_slots` on the server (see the root README, "Metrics").
//!
//! This example:
//!
//! - consumes the stream and acknowledges each transaction at its `Commit`;
//! - spawns a reporter task that prints rates, the in-flight lag, unconfirmed bytes and reconnects;
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
//! # A slow handler (20 ms per event) makes a large transaction visible as the lag
//! # climbing until its commit, and the endpoint can be scraped meanwhile:
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

/// PostgreSQL's epoch (2000-01-01) in Unix seconds.
const PG_EPOCH_UNIX_SECS: f64 = 946_684_800.0;

/// A snapshot time (microseconds since 2000-01-01, 0 for "none yet") as Unix seconds, 0 for none.
fn unix_secs(time: i64) -> f64 {
    if time == 0 {
        0.0
    } else {
        time as f64 / 1e6 + PG_EPOCH_UNIX_SECS
    }
}

/// The snapshot in the Prometheus text exposition format: totals as counters, positions, lags and
/// times as gauges, the last error class as an info series, all labelled with the slot.
fn render_prometheus(slot: &str, m: &MetricsSnapshot) -> String {
    let counters = [
        (
            "wal_messages_total",
            "XLogData frames decoded.",
            m.wal_messages,
        ),
        ("wal_bytes_total", "Bytes of those frames.", m.wal_bytes),
        (
            "keepalives_total",
            "Keepalives read from the server.",
            m.keepalives,
        ),
        ("commits_total", "Transaction commits decoded.", m.commits),
        (
            "feedback_sent_total",
            "Standby status updates written without error.",
            m.feedback_sent,
        ),
        (
            "feedback_failures_total",
            "Standby status updates whose write failed.",
            m.feedback_failures,
        ),
        (
            "reconnect_attempts_total",
            "Recoveries started after the connection was lost.",
            m.reconnect_attempts,
        ),
        (
            "reconnects_total",
            "Reconnects that completed.",
            m.reconnects,
        ),
        (
            "reconnect_failures_total",
            "Recovery attempts that failed.",
            m.reconnect_failures,
        ),
    ];
    let gauges = [
        (
            "received_lsn",
            "Highest WAL position the server reported (a byte offset).",
            m.received_lsn.to_string(),
        ),
        (
            "flushed_lsn",
            "The consumer's flushed position (a byte offset).",
            m.flushed_lsn.to_string(),
        ),
        (
            "applied_lsn",
            "The consumer's applied position (a byte offset).",
            m.applied_lsn.to_string(),
        ),
        (
            "sent_flushed_lsn",
            "Flushed position the last status update told the server (a byte offset).",
            m.sent_flushed_lsn.to_string(),
        ),
        (
            "lag_bytes",
            "WAL between the last data frame and the applied position: work read but not yet acknowledged, not the backlog the server holds.",
            m.lag_bytes().to_string(),
        ),
        (
            "unconfirmed_bytes",
            "WAL between the server's latest position and the last status update.",
            m.unconfirmed_bytes().to_string(),
        ),
        (
            "recovering",
            "1 while a reconnect is in progress or has failed and the stream is disconnected.",
            u8::from(m.recovering).to_string(),
        ),
        (
            "last_server_time_seconds",
            "Unix time the server stamped on its last frame or keepalive (the server's clock).",
            format!("{:.6}", unix_secs(m.last_server_time)),
        ),
        (
            "last_commit_time_seconds",
            "Unix commit time of the last decoded transaction.",
            format!("{:.6}", unix_secs(m.last_commit_time)),
        ),
        (
            "last_feedback_time_seconds",
            "Unix time the last status update was written (this host's clock).",
            format!("{:.6}", unix_secs(m.last_feedback_time)),
        ),
    ];
    let mut out = String::new();
    for (name, help, value) in counters {
        out.push_str(&format!(
            "# HELP pg_walstream_{name} {help}\n# TYPE pg_walstream_{name} counter\n\
             pg_walstream_{name}{{slot=\"{slot}\"}} {value}\n"
        ));
    }
    for (name, help, value) in gauges {
        out.push_str(&format!(
            "# HELP pg_walstream_{name} {help}\n# TYPE pg_walstream_{name} gauge\n\
             pg_walstream_{name}{{slot=\"{slot}\"}} {value}\n"
        ));
    }
    // Slot names are lowercase letters, digits and underscores, so the label needs no escaping.
    let class = format!("{:?}", m.last_error).to_lowercase();
    out.push_str(&format!(
        "# HELP pg_walstream_last_error_info Class of the last error the read loop met.\n\
         # TYPE pg_walstream_last_error_info gauge\n\
         pg_walstream_last_error_info{{slot=\"{slot}\",class=\"{class}\"}} 1\n"
    ));
    out
}

/// Print one line every `every`: totals, rates from the difference of two snapshots, and the
/// lag and unconfirmed bytes.
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
            "msgs {:>8} ({:>8.1}/s)  {:>10} B ({:>9.1} KiB/s)  commits {}  in-flight {:>8} B  unconfirmed {:>8} B  \
             received {}  applied {}  keepalives {}  feedback {} ({} failed)  \
             reconnects {} ({} attempts, {} failed{})",
            now.wal_messages,
            (now.wal_messages - previous.wal_messages) as f64 / secs,
            now.wal_bytes,
            (now.wal_bytes - previous.wal_bytes) as f64 / 1024.0 / secs,
            now.commits,
            now.lag_bytes(),
            now.unconfirmed_bytes(),
            format_lsn(now.received_lsn),
            format_lsn(now.applied_lsn),
            now.keepalives,
            now.feedback_sent,
            now.feedback_failures,
            now.reconnects,
            now.reconnect_attempts,
            now.reconnect_failures,
            if now.recovering { ", recovering" } else { "" },
        );
        previous = now;
    }
}

/// Answer every connection with the current metrics. A real service would route on the path and
/// use its web framework's metrics middleware; the point here is that the handler only needs a
/// clone of the handle.
async fn serve(addr: String, slot: String, metrics: StreamMetrics) -> std::io::Result<()> {
    let listener = TcpListener::bind(&addr).await?;
    println!("Prometheus metrics on http://{addr}/metrics");
    loop {
        let (mut socket, _) = listener.accept().await?;
        let metrics = metrics.clone();
        let slot = slot.clone();
        tokio::spawn(async move {
            let mut request = [0u8; 1024];
            let _ = socket.read(&mut request).await;
            let body = render_prometheus(&slot, &metrics.snapshot());
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
    let slot = env_or("SLOT", "metrics_example_slot");
    let config = ReplicationStreamConfig::builder(
        slot.clone(),
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
            if let Err(e) = serve(addr, slot, metrics).await {
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
    // handler then shows up as the lag growing through a transaction and
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
            Err(e) => {
                // The handle outlives the stream: `last_error` says what ended it.
                print_summary(&metrics.snapshot());
                return Err(e.into());
            }
        }
    }

    // `update_applied_lsn` only moves the shared position; this sends it to the server.
    let _ = events.shutdown().await;

    print_summary(&metrics.snapshot());
    Ok(())
}

fn print_summary(last: &MetricsSnapshot) {
    println!(
        "stopped: {} messages, {} bytes, {} bytes unacked, {} reconnects, last error {:?}",
        last.wal_messages,
        last.wal_bytes,
        last.lag_bytes(),
        last.reconnects,
        last.last_error
    );
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
        let text = render_prometheus("s", &snapshot);
        assert!(text.contains("# HELP pg_walstream_wal_messages_total XLogData frames decoded.\n"));
        assert!(text.contains("# TYPE pg_walstream_wal_messages_total counter\n"));
        assert!(text.contains("pg_walstream_wal_messages_total{slot=\"s\"} 7\n"));
        assert!(text.contains("# TYPE pg_walstream_unconfirmed_bytes gauge\n"));
        assert!(text.contains("pg_walstream_received_lsn{slot=\"s\"} 5000\n"));
        assert!(text.contains("pg_walstream_last_error_info{slot=\"s\",class=\"none\"} 1\n"));
        assert_eq!(
            text.lines().count(),
            60,
            "20 series, a HELP line, a TYPE line and a value each"
        );
    }

    #[test]
    fn times_are_unix_seconds_and_zero_means_none() {
        assert_eq!(unix_secs(0), 0.0);
        assert_eq!(unix_secs(1_000_000), PG_EPOCH_UNIX_SECS + 1.0);
        let mut snapshot = MetricsSnapshot::default();
        snapshot.last_server_time = 1_500_000;
        let text = render_prometheus("s", &snapshot);
        assert!(
            text.contains("pg_walstream_last_server_time_seconds{slot=\"s\"} 946684801.500000\n")
        );
        assert!(text.contains("pg_walstream_last_commit_time_seconds{slot=\"s\"} 0.000000\n"));
    }
}
