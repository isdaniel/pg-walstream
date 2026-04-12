use crate::metrics::{Metrics, ResourceSample, ResourceSummary};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use sysinfo::{Pid, ProcessRefreshKind, RefreshKind, System};

/// Background resource sampler that collects CPU, memory, and throughput snapshots
/// at regular intervals.
pub struct ResourceSampler {
    stop: Arc<AtomicBool>,
    handle: Option<tokio::task::JoinHandle<Vec<ResourceSample>>>,
}

impl ResourceSampler {
    /// Start sampling. Call `stop()` to end and collect results.
    ///
    /// `metrics` — the shared counters used by the consumer (for computing per-second deltas).
    /// `warmup_done` — the sampler waits until this is true before recording.
    pub fn start(metrics: Metrics, warmup_done: Arc<AtomicBool>) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = stop.clone();

        let handle = tokio::spawn(async move {
            sampler_loop(metrics, warmup_done, stop_clone).await
        });

        Self {
            stop,
            handle: Some(handle),
        }
    }

    /// Signal the sampler to stop and return collected samples.
    pub async fn stop(mut self) -> ResourceSummary {
        self.stop.store(true, Ordering::Relaxed);
        let samples = if let Some(h) = self.handle.take() {
            h.await.unwrap_or_default()
        } else {
            Vec::new()
        };
        ResourceSummary::from_samples(samples)
    }
}

async fn sampler_loop(
    metrics: Metrics,
    warmup_done: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
) -> Vec<ResourceSample> {
    let pid = Pid::from_u32(std::process::id());

    // Create system with minimal refresh scope
    let mut sys = System::new_with_specifics(
        RefreshKind::nothing()
            .with_cpu(sysinfo::CpuRefreshKind::everything())
            .with_memory(sysinfo::MemoryRefreshKind::everything())
            .with_processes(
                ProcessRefreshKind::nothing()
                    .with_cpu()
                    .with_memory(),
            ),
    );

    // Initial refresh to populate baselines (sysinfo needs two refreshes for CPU delta)
    sys.refresh_cpu_usage();
    sys.refresh_memory();
    sys.refresh_processes_specifics(
        sysinfo::ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing()
            .with_cpu()
            .with_memory(),
    );
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut samples = Vec::with_capacity(30);
    let mut prev_events: u64 = 0;
    let mut prev_dml_events: u64 = 0;
    let mut measure_start: Option<Instant> = None;

    loop {
        if stop.load(Ordering::Relaxed) {
            break;
        }

        tokio::time::sleep(Duration::from_secs(1)).await;

        if stop.load(Ordering::Relaxed) {
            break;
        }

        // Only record after warmup is done
        if !warmup_done.load(Ordering::Relaxed) {
            // Keep refreshing so CPU deltas are ready when measurement starts
            sys.refresh_cpu_usage();
            sys.refresh_processes_specifics(
                sysinfo::ProcessesToUpdate::Some(&[pid]),
                true,
                ProcessRefreshKind::nothing()
                    .with_cpu()
                    .with_memory(),
            );
            prev_events = metrics.event_count();
            prev_dml_events = metrics.dml_event_count();
            continue;
        }

        if measure_start.is_none() {
            measure_start = Some(Instant::now());
            prev_events = metrics.event_count();
            prev_dml_events = metrics.dml_event_count();
        }

        // Refresh system info
        sys.refresh_cpu_usage();
        sys.refresh_memory();
        sys.refresh_processes_specifics(
            sysinfo::ProcessesToUpdate::Some(&[pid]),
            true,
            ProcessRefreshKind::nothing()
                .with_cpu()
                .with_memory(),
        );

        let elapsed = measure_start.unwrap().elapsed().as_secs_f64();
        let cur_events = metrics.event_count();
        let cur_dml = metrics.dml_event_count();

        let delta_events = cur_events.saturating_sub(prev_events);
        let delta_dml = cur_dml.saturating_sub(prev_dml_events);

        // Process-level metrics
        let (process_cpu, process_rss_mb) = if let Some(proc) = sys.process(pid) {
            (proc.cpu_usage(), proc.memory() as f64 / (1024.0 * 1024.0))
        } else {
            (0.0, 0.0)
        };

        // System-level metrics
        let sys_cpu = sys.global_cpu_usage();
        let sys_mem_used_mb = sys.used_memory() as f64 / (1024.0 * 1024.0);
        let sys_mem_total_mb = sys.total_memory() as f64 / (1024.0 * 1024.0);

        samples.push(ResourceSample {
            elapsed_secs: elapsed,
            events_per_sec: delta_events as f64,
            dml_events_per_sec: delta_dml as f64,
            process_cpu_pct: process_cpu,
            system_cpu_pct: sys_cpu,
            process_rss_mb,
            system_mem_used_mb: sys_mem_used_mb,
            system_mem_total_mb: sys_mem_total_mb,
        });

        prev_events = cur_events;
        prev_dml_events = cur_dml;
    }

    samples
}
