//! Comparison report generator — reads two JSON result files (rustls-tls and libpq)
//! and generates a side-by-side markdown comparison report.
//!
//! Usage:
//!   pg-walstream-compare <rustls.json> <libpq.json> [--output report.md]

use pg_walstream_loadtest::json_types::{JsonReport, JsonScenarioResult};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!(
            "Usage: {} <report_a.json> <report_b.json> [--output report.md]",
            args[0]
        );
        std::process::exit(1);
    }

    let file_a = &args[1];
    let file_b = &args[2];

    let output_path = args
        .iter()
        .position(|a| a == "--output")
        .and_then(|i| args.get(i + 1))
        .cloned();

    let report_a: JsonReport = serde_json::from_str(
        &std::fs::read_to_string(file_a).unwrap_or_else(|e| panic!("Cannot read {file_a}: {e}")),
    )
    .unwrap_or_else(|e| panic!("Cannot parse {file_a}: {e}"));

    let report_b: JsonReport = serde_json::from_str(
        &std::fs::read_to_string(file_b).unwrap_or_else(|e| panic!("Cannot read {file_b}: {e}")),
    )
    .unwrap_or_else(|e| panic!("Cannot parse {file_b}: {e}"));

    let md = generate_comparison(&report_a, &report_b);

    if let Some(path) = &output_path {
        std::fs::write(path, &md).unwrap_or_else(|e| panic!("Cannot write {path}: {e}"));
        println!("Comparison report written to: {path}");
    } else {
        print!("{md}");
    }
}

fn fmt_num(n: f64) -> String {
    if n.abs() < 1000.0 {
        return format!("{:.0}", n);
    }
    let s = format!("{:.0}", n);
    let bytes = s.as_bytes();
    let mut result = Vec::new();
    let len = bytes.len();
    for (i, &b) in bytes.iter().enumerate() {
        if i > 0 && (len - i) % 3 == 0 && b != b'-' {
            result.push(b',');
        }
        result.push(b);
    }
    String::from_utf8(result).unwrap()
}

fn pct_delta(a: f64, b: f64) -> String {
    if b == 0.0 {
        return "N/A".to_string();
    }
    let delta = ((a - b) / b) * 100.0;
    if delta > 0.0 {
        format!("+{:.1}%", delta)
    } else {
        format!("{:.1}%", delta)
    }
}

fn winner_tag(a: f64, b: f64, label_a: &str, label_b: &str, higher_is_better: bool) -> String {
    let threshold = 0.02; // 2% difference to declare a winner
    let ratio = if b > 0.0 { (a - b) / b } else { 0.0 };

    if ratio.abs() < threshold {
        "~tie".to_string()
    } else if (higher_is_better && a > b) || (!higher_is_better && a < b) {
        format!("**{}**", label_a)
    } else {
        format!("**{}**", label_b)
    }
}

struct MatchedScenario<'a> {
    a: &'a JsonScenarioResult,
    b: &'a JsonScenarioResult,
}

fn match_scenarios<'a>(
    report_a: &'a JsonReport,
    report_b: &'a JsonReport,
) -> Vec<MatchedScenario<'a>> {
    let mut matched = Vec::new();
    for a in &report_a.results {
        if let Some(b) = report_b.results.iter().find(|b| b.name == a.name) {
            matched.push(MatchedScenario { a, b });
        }
    }
    matched
}

fn generate_comparison(report_a: &JsonReport, report_b: &JsonReport) -> String {
    let label_a = &report_a.backend;
    let label_b = &report_b.backend;
    let matched = match_scenarios(report_a, report_b);

    let core: Vec<&MatchedScenario> = matched
        .iter()
        .filter(|m| !m.a.name.starts_with("Stress-"))
        .collect();
    let stress: Vec<&MatchedScenario> = matched
        .iter()
        .filter(|m| m.a.name.starts_with("Stress-"))
        .collect();

    let mut md = String::with_capacity(16384);

    // Header
    md.push_str(&format!(
        "# pg-walstream Load Test Comparison: {} vs {}\n\n",
        label_a, label_b
    ));
    md.push_str(&format!(
        "**Generated**: {}\n\n",
        chrono::Local::now().format("%Y-%m-%d %H:%M:%S %Z")
    ));

    // Section 1: Test Environment
    md.push_str("## 1. Test Environment\n\n");
    md.push_str("```\n");
    md.push_str(&report_a.vm_info);
    md.push_str("```\n\n");
    md.push_str(&format!(
        "- **Backend A**: {} (run at {})\n",
        label_a, report_a.timestamp
    ));
    md.push_str(&format!(
        "- **Backend B**: {} (run at {})\n",
        label_b, report_b.timestamp
    ));
    md.push_str(&format!(
        "- **Scenarios matched**: {} of {} / {}\n\n",
        matched.len(),
        report_a.results.len(),
        report_b.results.len()
    ));

    // Section 2: CPU Efficiency — the primary metric the user wants
    md.push_str("## 2. CPU Efficiency (DML events/sec per 1% CPU)\n\n");
    md.push_str(
        "This is the primary efficiency metric: how many DML events can each backend process \
         for every 1% of CPU it consumes. Higher is better.\n\n",
    );
    md.push_str(&format!(
        "| Scenario | {} | {} | Delta | Winner |\n",
        label_a, label_b
    ));
    md.push_str("|----------|----------:|----------:|--------:|--------|\n");

    for m in &core {
        let eff_a = m.a.dml_events_per_cpu_pct;
        let eff_b = m.b.dml_events_per_cpu_pct;
        md.push_str(&format!(
            "| {} | {} | {} | {} | {} |\n",
            m.a.name,
            fmt_num(eff_a),
            fmt_num(eff_b),
            pct_delta(eff_a, eff_b),
            winner_tag(eff_a, eff_b, label_a, label_b, true),
        ));
    }
    md.push('\n');

    // Section 3: Throughput Comparison
    md.push_str("## 3. Throughput Comparison\n\n");
    md.push_str(&format!(
        "| Scenario | {} ev/s | {} ev/s | Delta | {} DML/s | {} DML/s | Delta | Winner |\n",
        label_a, label_b, label_a, label_b
    ));
    md.push_str("|----------|----------:|----------:|--------:|----------:|----------:|--------:|--------|\n");

    for m in &core {
        md.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} |\n",
            m.a.name,
            fmt_num(m.a.events_per_sec),
            fmt_num(m.b.events_per_sec),
            pct_delta(m.a.events_per_sec, m.b.events_per_sec),
            fmt_num(m.a.dml_events_per_sec),
            fmt_num(m.b.dml_events_per_sec),
            pct_delta(m.a.dml_events_per_sec, m.b.dml_events_per_sec),
            winner_tag(
                m.a.dml_events_per_sec,
                m.b.dml_events_per_sec,
                label_a,
                label_b,
                true
            ),
        ));
    }
    md.push('\n');

    // Section 4: Resource Utilization
    md.push_str("## 4. Resource Utilization Comparison\n\n");
    md.push_str(
        "Process CPU and RSS reflect **only the pg-walstream consumer** (generator runs \
         as a separate OS process).\n\n",
    );
    md.push_str(&format!(
        "| Scenario | {} CPU% | {} CPU% | Delta | {} RSS MB | {} RSS MB | Delta | Winner |\n",
        label_a, label_b, label_a, label_b
    ));
    md.push_str("|----------|----------:|----------:|--------:|----------:|----------:|--------:|--------|\n");

    for m in &core {
        md.push_str(&format!(
            "| {} | {:.1} | {:.1} | {} | {:.1} | {:.1} | {} | {} |\n",
            m.a.name,
            m.a.avg_process_cpu_pct,
            m.b.avg_process_cpu_pct,
            pct_delta(
                m.a.avg_process_cpu_pct as f64,
                m.b.avg_process_cpu_pct as f64
            ),
            m.a.avg_rss_mb,
            m.b.avg_rss_mb,
            pct_delta(m.a.avg_rss_mb, m.b.avg_rss_mb),
            // Lower CPU and RSS is better
            winner_tag(
                m.a.avg_process_cpu_pct as f64,
                m.b.avg_process_cpu_pct as f64,
                label_a,
                label_b,
                false
            ),
        ));
    }
    md.push('\n');

    // Section 5: Latency Comparison
    md.push_str("## 5. Latency Comparison (inter-event, microseconds)\n\n");
    md.push_str(&format!(
        "| Scenario | {} P50 | {} P50 | {} P99 | {} P99 | Winner |\n",
        label_a, label_b, label_a, label_b
    ));
    md.push_str("|----------|----------:|----------:|----------:|----------:|--------|\n");

    for m in &core {
        md.push_str(&format!(
            "| {} | {:.0} | {:.0} | {:.0} | {:.0} | {} |\n",
            m.a.name,
            m.a.p50_inter_event_us,
            m.b.p50_inter_event_us,
            m.a.p99_inter_event_us,
            m.b.p99_inter_event_us,
            winner_tag(
                m.a.p50_inter_event_us,
                m.b.p50_inter_event_us,
                label_a,
                label_b,
                false
            ),
        ));
    }
    md.push('\n');

    // Section 6: Stress Ramp Comparison
    if !stress.is_empty() {
        md.push_str("## 6. Stress Ramp Comparison\n\n");
        md.push_str(
            "Progressive writer concurrency ramp — comparing throughput and CPU scaling.\n\n",
        );
        md.push_str(&format!(
            "| Writers | {} DML/s | {} DML/s | Delta | {} CPU% | {} CPU% | {} eff | {} eff |\n",
            label_a, label_b, label_a, label_b, label_a, label_b
        ));
        md.push_str("|--------:|----------:|----------:|--------:|----------:|----------:|----------:|----------:|\n");

        for m in &stress {
            let writers = m
                .a
                .name
                .trim_start_matches("Stress-")
                .trim_end_matches('w');
            md.push_str(&format!(
                "| {} | {} | {} | {} | {:.1} | {:.1} | {} | {} |\n",
                writers,
                fmt_num(m.a.dml_events_per_sec),
                fmt_num(m.b.dml_events_per_sec),
                pct_delta(m.a.dml_events_per_sec, m.b.dml_events_per_sec),
                m.a.avg_process_cpu_pct,
                m.b.avg_process_cpu_pct,
                fmt_num(m.a.dml_events_per_cpu_pct),
                fmt_num(m.b.dml_events_per_cpu_pct),
            ));
        }
        md.push('\n');
    }

    // Section 7: Overall Summary
    let summary_section = if stress.is_empty() { "6" } else { "7" };
    md.push_str(&format!("## {}. Overall Summary\n\n", summary_section));

    // Count wins across core scenarios
    let mut throughput_wins_a = 0u32;
    let mut throughput_wins_b = 0u32;
    let mut cpu_eff_wins_a = 0u32;
    let mut cpu_eff_wins_b = 0u32;
    let mut mem_wins_a = 0u32;
    let mut mem_wins_b = 0u32;
    let mut latency_wins_a = 0u32;
    let mut latency_wins_b = 0u32;

    for m in &core {
        // Throughput (higher is better)
        if m.a.dml_events_per_sec > m.b.dml_events_per_sec * 1.02 {
            throughput_wins_a += 1;
        } else if m.b.dml_events_per_sec > m.a.dml_events_per_sec * 1.02 {
            throughput_wins_b += 1;
        }

        // CPU efficiency (higher is better)
        if m.a.dml_events_per_cpu_pct > m.b.dml_events_per_cpu_pct * 1.02 {
            cpu_eff_wins_a += 1;
        } else if m.b.dml_events_per_cpu_pct > m.a.dml_events_per_cpu_pct * 1.02 {
            cpu_eff_wins_b += 1;
        }

        // Memory (lower is better)
        if m.a.avg_rss_mb < m.b.avg_rss_mb * 0.98 {
            mem_wins_a += 1;
        } else if m.b.avg_rss_mb < m.a.avg_rss_mb * 0.98 {
            mem_wins_b += 1;
        }

        // Latency (lower is better)
        if m.a.p50_inter_event_us < m.b.p50_inter_event_us * 0.98 {
            latency_wins_a += 1;
        } else if m.b.p50_inter_event_us < m.a.p50_inter_event_us * 0.98 {
            latency_wins_b += 1;
        }
    }

    let total = core.len() as u32;

    md.push_str(&format!(
        "| Metric | {} wins | {} wins | Ties |\n",
        label_a, label_b
    ));
    md.push_str("|--------|------:|------:|------:|\n");
    md.push_str(&format!(
        "| Throughput (DML/s) | {} | {} | {} |\n",
        throughput_wins_a,
        throughput_wins_b,
        total - throughput_wins_a - throughput_wins_b
    ));
    md.push_str(&format!(
        "| CPU Efficiency (ev/s per 1% CPU) | {} | {} | {} |\n",
        cpu_eff_wins_a,
        cpu_eff_wins_b,
        total - cpu_eff_wins_a - cpu_eff_wins_b
    ));
    md.push_str(&format!(
        "| Memory (lower RSS) | {} | {} | {} |\n",
        mem_wins_a,
        mem_wins_b,
        total - mem_wins_a - mem_wins_b
    ));
    md.push_str(&format!(
        "| Latency (lower P50) | {} | {} | {} |\n",
        latency_wins_a,
        latency_wins_b,
        total - latency_wins_a - latency_wins_b
    ));
    md.push('\n');

    // Peak numbers
    if let (Some(peak_a), Some(peak_b)) = (
        report_a
            .results
            .iter()
            .max_by(|x, y| x.dml_events_per_sec.partial_cmp(&y.dml_events_per_sec).unwrap()),
        report_b
            .results
            .iter()
            .max_by(|x, y| x.dml_events_per_sec.partial_cmp(&y.dml_events_per_sec).unwrap()),
    ) {
        md.push_str("### Peak Numbers\n\n");
        md.push_str(&format!(
            "| Metric | {} | {} |\n",
            label_a, label_b
        ));
        md.push_str("|--------|------:|------:|\n");
        md.push_str(&format!(
            "| Peak DML events/sec | {} | {} |\n",
            fmt_num(peak_a.dml_events_per_sec),
            fmt_num(peak_b.dml_events_per_sec)
        ));
        md.push_str(&format!(
            "| Peak total events/sec | {} | {} |\n",
            fmt_num(peak_a.events_per_sec),
            fmt_num(peak_b.events_per_sec)
        ));
        md.push_str(&format!(
            "| Peak CPU efficiency (DML/s per 1% CPU) | {} | {} |\n",
            fmt_num(peak_a.dml_events_per_cpu_pct),
            fmt_num(peak_b.dml_events_per_cpu_pct)
        ));
        md.push_str(&format!(
            "| Peak process CPU% | {:.1} | {:.1} |\n",
            peak_a.peak_process_cpu_pct, peak_b.peak_process_cpu_pct
        ));
        md.push_str(&format!(
            "| Peak RSS (MB) | {:.1} | {:.1} |\n",
            peak_a.peak_rss_mb, peak_b.peak_rss_mb
        ));
    }
    md.push('\n');

    // Architecture notes
    md.push_str("### Architecture Notes\n\n");
    md.push_str("| Aspect | libpq | rustls-tls |\n");
    md.push_str("|--------|-------|------------|\n");
    md.push_str("| TLS Library | OpenSSL (C) | rustls (pure Rust, aws-lc-rs crypto) |\n");
    md.push_str("| Data Path | triple-copy (SSL_read -> libpq buf -> malloc+copy -> Rust Vec) | zero-copy (read_buf -> BytesMut -> split_to.freeze.slice) |\n");
    md.push_str("| C Dependencies | libpq-dev, libssl-dev | cmake (build only) |\n");
    md.push_str("| Auth | Delegated to libpq | Native SCRAM-SHA-256 with channel binding |\n");
    md.push('\n');

    md
}
