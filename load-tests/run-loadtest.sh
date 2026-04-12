#!/usr/bin/env bash
#
# run-loadtest.sh — Build and run pg-walstream load tests with either backend.
#
# Usage:
#   ./run-loadtest.sh                        # rustls-tls (default)
#   ./run-loadtest.sh rustls-tls             # rustls-tls explicitly
#   ./run-loadtest.sh libpq                  # libpq+OpenSSL
#   ./run-loadtest.sh compare                # run both, generate comparison report
#
# Environment:
#   export DATABASE_URL   PostgreSQL connection string (required)
#
# Examples:
#   export DATABASE_URL='postgresql://user:pass@host:5432/db?sslmode=require' ./run-loadtest.sh
#   export DATABASE_URL='postgresql://user:pass@host:5432/db?sslmode=require' ./run-loadtest.sh compare
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
REPORTS_DIR="$PROJECT_DIR/reports"
CARGO_TOML="$SCRIPT_DIR/Cargo.toml"
MAIN_RS="$SCRIPT_DIR/src/main.rs"

# Timestamp for report filenames
TIMESTAMP="$(date -u +%Y%m%d-%H%M%S)"

# ── Helpers ─────────────────────────────────────────────────────────────────

info()  { printf "\033[1;34m[INFO]\033[0m  %s\n" "$*"; }
ok()    { printf "\033[1;32m[OK]\033[0m    %s\n" "$*"; }
err()   { printf "\033[1;31m[ERROR]\033[0m %s\n" "$*" >&2; }

check_database_url() {
    if [[ -z "${DATABASE_URL:-}" ]]; then
        err "DATABASE_URL is not set."
        echo ""
        echo "  Export it before running:"
        echo "    export DATABASE_URL='postgresql://user:pass@host:5432/db?sslmode=require'"
        echo ""
        exit 1
    fi
    info "DATABASE_URL: ${DATABASE_URL%%@*}@***"
}

ensure_reports_dir() {
    mkdir -p "$REPORTS_DIR"
}

# ── Backend switching ───────────────────────────────────────────────────────

# Save original files so we can restore after libpq runs
save_originals() {
    cp "$CARGO_TOML" "$CARGO_TOML.bak"
    cp "$MAIN_RS" "$MAIN_RS.bak"
}

restore_originals() {
    if [[ -f "$CARGO_TOML.bak" ]]; then
        mv "$CARGO_TOML.bak" "$CARGO_TOML"
    fi
    if [[ -f "$MAIN_RS.bak" ]]; then
        mv "$MAIN_RS.bak" "$MAIN_RS"
    fi
}

switch_to_libpq() {
    info "Switching Cargo.toml to libpq backend..."
    sed -i \
        's|pg_walstream = { path = "..", default-features = false, features = \["rustls-tls"\] }|pg_walstream = { path = "..", features = ["libpq"] }|' \
        "$CARGO_TOML"
    sed -i \
        's|^rustls = |#rustls = |' \
        "$CARGO_TOML"
    # Comment out the rustls crypto provider init
    sed -i \
        's|^\(\s*\)let _ = rustls::crypto::ring::default_provider()\.install_default();|\1// let _ = rustls::crypto::ring::default_provider().install_default();|' \
        "$MAIN_RS"
}

switch_to_rustls() {
    info "Ensuring Cargo.toml uses rustls-tls backend..."
    # These are no-ops if already set to rustls-tls, but ensure correctness
    sed -i \
        's|pg_walstream = { path = "..", features = \["libpq"\] }|pg_walstream = { path = "..", default-features = false, features = ["rustls-tls"] }|' \
        "$CARGO_TOML"
    sed -i \
        's|^#rustls = |rustls = |' \
        "$CARGO_TOML"
    sed -i \
        's|^\(\s*\)// let _ = rustls::crypto::ring::default_provider()\.install_default();|\1let _ = rustls::crypto::ring::default_provider().install_default();|' \
        "$MAIN_RS"
}

# ── Build & Run ─────────────────────────────────────────────────────────────

build_loadtest() {
    local backend="$1"
    info "Building load test ($backend)..."
    cd "$SCRIPT_DIR"
    cargo build --release 2>&1 | tail -5
    ok "Build complete ($backend)"
}

run_loadtest() {
    local backend="$1"
    local report_file="$2"

    info "Running load test ($backend)..."
    info "This takes ~10 minutes (14 scenarios x 40s each)"
    echo ""

    cd "$SCRIPT_DIR"
    cargo run --release --bin pg-walstream-loadtest 2>&1

    local generated="$REPORTS_DIR/LOAD_TEST_REPORT.md"
    if [[ -f "$generated" ]]; then
        cp "$generated" "$report_file"
        ok "Report saved: $report_file"
    else
        err "Expected report not found at $generated"
        return 1
    fi
}

# ── Run modes ───────────────────────────────────────────────────────────────

run_single() {
    local backend="${1:-rustls-tls}"
    local report_file="$REPORTS_DIR/loadtest-${backend}-${TIMESTAMP}.md"

    check_database_url
    ensure_reports_dir

    if [[ "$backend" == "libpq" ]]; then
        save_originals
        trap restore_originals EXIT
        switch_to_libpq
    fi

    build_loadtest "$backend"
    run_loadtest "$backend" "$report_file"

    echo ""
    ok "Done! Report: $report_file"
}


# ── Comparison report generator ─────────────────────────────────────────────

generate_comparison() {
    local rustls_file="$1"
    local libpq_file="$2"
    local output="$3"

    # Extract scenario lines from each report's results table
    # Format: | Scenario | Events/s | DML/s | MB/s | P50 | P95 | P99 | P99.9 |
    local rustls_data libpq_data
    rustls_data=$(grep -E '^\| (Baseline|Batch-|4-Writers|Wide-|Payload-|Mixed-|Stress-)' "$rustls_file" | head -14 || true)
    libpq_data=$(grep -E '^\| (Baseline|Batch-|4-Writers|Wide-|Payload-|Mixed-|Stress-)' "$libpq_file" | head -14 || true)

    # Extract resource lines (second table in the report)
    local rustls_resources libpq_resources
    rustls_resources=$(awk '/Resource Utilization/,/^$/' "$rustls_file" | grep -E '^\| (Baseline|Batch-|4-Writers|Wide-|Payload-|Mixed-|Stress-)' | head -14 || true)
    libpq_resources=$(awk '/Resource Utilization/,/^$/' "$libpq_file" | grep -E '^\| (Baseline|Batch-|4-Writers|Wide-|Payload-|Mixed-|Stress-)' | head -14 || true)

    cat > "$output" << 'HEADER'
# pg-walstream Load Test Report V2 — libpq vs rustls-tls

HEADER

    echo "**Generated**: $(date -u +%Y-%m-%d' '%H:%M:%S' UTC')" >> "$output"
    echo "" >> "$output"

    # Copy environment section from rustls report
    echo "## 1. Test Environment" >> "$output"
    echo "" >> "$output"
    awk '/^```$/,/^```$/' "$rustls_file" | head -20 >> "$output"
    echo "" >> "$output"

    # Throughput table
    cat >> "$output" << 'EOF'
## 2. Throughput Results

### rustls-tls

EOF
    echo "| Scenario | Total Events/s | DML Events/s | MB/s | P50 (us) | P95 (us) | P99 (us) | P99.9 (us) |" >> "$output"
    echo "|----------|---------------:|-------------:|-----:|---------:|---------:|---------:|-----------:|" >> "$output"
    echo "$rustls_data" >> "$output"

    cat >> "$output" << 'EOF'

### libpq+OpenSSL

EOF
    echo "| Scenario | Total Events/s | DML Events/s | MB/s | P50 (us) | P95 (us) | P99 (us) | P99.9 (us) |" >> "$output"
    echo "|----------|---------------:|-------------:|-----:|---------:|---------:|---------:|-----------:|" >> "$output"
    echo "$libpq_data" >> "$output"

    # Resource table
    cat >> "$output" << 'EOF'

## 3. Resource Utilization

### rustls-tls

EOF
    echo "| Scenario | Proc CPU avg% | Proc CPU peak% | Sys CPU avg% | Sys CPU peak% | RSS avg (MB) | RSS peak (MB) |" >> "$output"
    echo "|----------|-------------:|---------------:|------------:|-------------:|-------------:|--------------:|" >> "$output"
    echo "$rustls_resources" >> "$output"

    cat >> "$output" << 'EOF'

### libpq+OpenSSL

EOF
    echo "| Scenario | Proc CPU avg% | Proc CPU peak% | Sys CPU avg% | Sys CPU peak% | RSS avg (MB) | RSS peak (MB) |" >> "$output"
    echo "|----------|-------------:|---------------:|------------:|-------------:|-------------:|--------------:|" >> "$output"
    echo "$libpq_resources" >> "$output"

    cat >> "$output" << 'EOF'

## 4. Summary

See individual reports for detailed per-scenario charts and analysis.

| Metric | libpq+OpenSSL | rustls-tls |
|--------|--------------|------------|
| TLS Library | OpenSSL (C) | rustls (pure Rust) |
| Data Path | triple-copy (SSL_read → libpq buf → malloc+copy → Rust Vec) | zero-copy (read_buf → BytesMut → split_to.freeze.slice) |
| C Dependencies | libpq-dev, libssl-dev | None |
EOF

    ok "Comparison report written: $output"
}

# ── Main ────────────────────────────────────────────────────────────────────

usage() {
    echo "Usage: $0 [rustls-tls|libpq|compare]"
    echo ""
    echo "  rustls-tls   Run load test with rustls-tls backend (default)"
    echo "  libpq        Run load test with libpq+OpenSSL backend"
    echo "  compare      Run both backends and generate comparison report"
    echo ""
    echo "Environment:"
    echo "  DATABASE_URL  PostgreSQL connection string (required)"
    echo ""
    echo "Reports are saved to: $PROJECT_DIR/reports/"
}

case "${1:-rustls-tls}" in
    rustls-tls|rustls)
        run_single "rustls-tls"
        ;;
    libpq)
        run_single "libpq"
        ;;
    -h|--help|help)
        usage
        ;;
    *)
        err "Unknown backend: $1"
        usage
        exit 1
        ;;
esac
