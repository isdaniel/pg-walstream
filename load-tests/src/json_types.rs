use crate::metrics::ScenarioResult;
use serde::{Deserialize, Serialize};

/// Serializable snapshot of a single scenario's results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonScenarioResult {
    pub name: String,
    pub description: String,
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
    pub avg_process_cpu_pct: f32,
    pub peak_process_cpu_pct: f32,
    pub avg_system_cpu_pct: f32,
    pub peak_system_cpu_pct: f32,
    pub avg_rss_mb: f64,
    pub peak_rss_mb: f64,
    /// DML events/sec per 1% process CPU — the primary efficiency metric.
    pub dml_events_per_cpu_pct: f64,
}

impl From<&ScenarioResult> for JsonScenarioResult {
    fn from(r: &ScenarioResult) -> Self {
        let dml_events_per_cpu_pct = if r.resources.avg_process_cpu_pct > 0.0 {
            r.dml_events_per_sec / r.resources.avg_process_cpu_pct as f64
        } else {
            0.0
        };
        Self {
            name: r.name.clone(),
            description: r.description.clone(),
            duration_secs: r.duration_secs,
            total_events: r.total_events,
            dml_events: r.dml_events,
            total_bytes: r.total_bytes,
            events_per_sec: r.events_per_sec,
            dml_events_per_sec: r.dml_events_per_sec,
            mb_per_sec: r.mb_per_sec,
            p50_inter_event_us: r.p50_inter_event_us,
            p95_inter_event_us: r.p95_inter_event_us,
            p99_inter_event_us: r.p99_inter_event_us,
            p999_inter_event_us: r.p999_inter_event_us,
            avg_process_cpu_pct: r.resources.avg_process_cpu_pct,
            peak_process_cpu_pct: r.resources.peak_process_cpu_pct,
            avg_system_cpu_pct: r.resources.avg_system_cpu_pct,
            peak_system_cpu_pct: r.resources.peak_system_cpu_pct,
            avg_rss_mb: r.resources.avg_rss_mb,
            peak_rss_mb: r.resources.peak_rss_mb,
            dml_events_per_cpu_pct,
        }
    }
}

/// Top-level JSON report containing all scenario results for a single backend run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonReport {
    pub backend: String,
    pub timestamp: String,
    pub vm_info: String,
    pub results: Vec<JsonScenarioResult>,
}
