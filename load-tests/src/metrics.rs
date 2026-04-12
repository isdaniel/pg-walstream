use hdrhistogram::Histogram;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Thread-safe metrics collector for load test scenarios.
#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    event_count: AtomicU64,
    dml_event_count: AtomicU64,
    total_bytes: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MetricsInner {
                event_count: AtomicU64::new(0),
                dml_event_count: AtomicU64::new(0),
                total_bytes: AtomicU64::new(0),
            }),
        }
    }

    pub fn reset(&self) {
        self.inner.event_count.store(0, Ordering::Relaxed);
        self.inner.dml_event_count.store(0, Ordering::Relaxed);
        self.inner.total_bytes.store(0, Ordering::Relaxed);
    }

    pub fn record_event(&self, is_dml: bool, bytes: u64) {
        self.inner.event_count.fetch_add(1, Ordering::Relaxed);
        if is_dml {
            self.inner.dml_event_count.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.total_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn event_count(&self) -> u64 {
        self.inner.event_count.load(Ordering::Relaxed)
    }

    pub fn dml_event_count(&self) -> u64 {
        self.inner.dml_event_count.load(Ordering::Relaxed)
    }

    pub fn total_bytes(&self) -> u64 {
        self.inner.total_bytes.load(Ordering::Relaxed)
    }
}

// ─── Resource monitoring types ──────────────────────────────────────────────

/// A single point-in-time sample of resource usage + throughput.
#[derive(Debug, Clone)]
pub struct ResourceSample {
    /// Seconds elapsed since measurement start.
    pub elapsed_secs: f64,
    /// Instantaneous total events/sec (delta from previous sample).
    pub events_per_sec: f64,
    /// Instantaneous DML events/sec (delta from previous sample).
    pub dml_events_per_sec: f64,
    /// Load-test process CPU usage (0-100% per core, so max = cores*100).
    pub process_cpu_pct: f32,
    /// System-wide CPU usage (0-100%).
    pub system_cpu_pct: f32,
    /// Load-test process RSS in MB.
    pub process_rss_mb: f64,
    /// System-wide used memory in MB.
    pub system_mem_used_mb: f64,
    /// System-wide total memory in MB.
    pub system_mem_total_mb: f64,
}

/// Aggregated resource summary for a scenario.
#[derive(Debug, Clone)]
pub struct ResourceSummary {
    pub avg_process_cpu_pct: f32,
    pub peak_process_cpu_pct: f32,
    pub avg_system_cpu_pct: f32,
    pub peak_system_cpu_pct: f32,
    pub avg_rss_mb: f64,
    pub peak_rss_mb: f64,
    pub system_mem_total_mb: f64,
    pub samples: Vec<ResourceSample>,
}

impl ResourceSummary {
    pub fn from_samples(samples: Vec<ResourceSample>) -> Self {
        if samples.is_empty() {
            return Self {
                avg_process_cpu_pct: 0.0,
                peak_process_cpu_pct: 0.0,
                avg_system_cpu_pct: 0.0,
                peak_system_cpu_pct: 0.0,
                avg_rss_mb: 0.0,
                peak_rss_mb: 0.0,
                system_mem_total_mb: 0.0,
                samples,
            };
        }
        let n = samples.len() as f32;
        let avg_process_cpu = samples.iter().map(|s| s.process_cpu_pct).sum::<f32>() / n;
        let peak_process_cpu = samples
            .iter()
            .map(|s| s.process_cpu_pct)
            .fold(0.0f32, f32::max);
        let avg_sys_cpu = samples.iter().map(|s| s.system_cpu_pct).sum::<f32>() / n;
        let peak_sys_cpu = samples
            .iter()
            .map(|s| s.system_cpu_pct)
            .fold(0.0f32, f32::max);
        let nd = samples.len() as f64;
        let avg_rss = samples.iter().map(|s| s.process_rss_mb).sum::<f64>() / nd;
        let peak_rss = samples
            .iter()
            .map(|s| s.process_rss_mb)
            .fold(0.0f64, f64::max);
        let mem_total = samples.last().map(|s| s.system_mem_total_mb).unwrap_or(0.0);

        Self {
            avg_process_cpu_pct: avg_process_cpu,
            peak_process_cpu_pct: peak_process_cpu,
            avg_system_cpu_pct: avg_sys_cpu,
            peak_system_cpu_pct: peak_sys_cpu,
            avg_rss_mb: avg_rss,
            peak_rss_mb: peak_rss,
            system_mem_total_mb: mem_total,
            samples,
        }
    }
}

/// Snapshot of metrics at a point in time.
#[derive(Debug, Clone)]
pub struct ScenarioResult {
    pub name: String,
    pub duration_secs: f64,
    pub total_events: u64,
    pub dml_events: u64,
    pub total_bytes: u64,
    pub events_per_sec: f64,
    pub dml_events_per_sec: f64,
    pub mb_per_sec: f64,
    pub p50_inter_event_us: f64,
    pub p95_inter_event_us: f64,
    pub p99_inter_event_us: f64,
    pub p999_inter_event_us: f64,
    pub description: String,
    pub resources: ResourceSummary,
}

impl ScenarioResult {
    pub fn from_metrics_and_histogram(
        name: &str,
        description: &str,
        metrics: &Metrics,
        duration_secs: f64,
        hist: &Histogram<u64>,
        resources: ResourceSummary,
    ) -> Self {
        let total_events = metrics.event_count();
        let dml_events = metrics.dml_event_count();
        let total_bytes = metrics.total_bytes();
        Self {
            name: name.to_string(),
            duration_secs,
            total_events,
            dml_events,
            total_bytes,
            events_per_sec: total_events as f64 / duration_secs,
            dml_events_per_sec: dml_events as f64 / duration_secs,
            mb_per_sec: (total_bytes as f64) / (1024.0 * 1024.0) / duration_secs,
            p50_inter_event_us: hist.value_at_quantile(0.50) as f64,
            p95_inter_event_us: hist.value_at_quantile(0.95) as f64,
            p99_inter_event_us: hist.value_at_quantile(0.99) as f64,
            p999_inter_event_us: hist.value_at_quantile(0.999) as f64,
            description: description.to_string(),
            resources,
        }
    }
}
