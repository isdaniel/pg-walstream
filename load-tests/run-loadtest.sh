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

# ── Build & Run ─────────────────────────────────────────────────────────────

build_loadtest() {
    local backend="$1"
    info "Building load test ($backend)..."
    cd "$SCRIPT_DIR"
    cargo build --release --no-default-features --features "$backend" 2>&1 | tail -5
    ok "Build complete ($backend)"
}

run_loadtest() {
    local backend="$1"
    local report_file="$2"
    local json_file="${3:-}"

    info "Running load test ($backend)..."
    info "This takes ~10 minutes (14 scenarios x 40s each)"
    echo ""

    cd "$SCRIPT_DIR"
    if [[ -n "$json_file" ]]; then
        cargo run --release --no-default-features --features "$backend" \
            --bin pg-walstream-loadtest -- --json-output "$json_file" 2>&1
    else
        cargo run --release --no-default-features --features "$backend" \
            --bin pg-walstream-loadtest 2>&1
    fi

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

    build_loadtest "$backend"
    run_loadtest "$backend" "$report_file"

    echo ""
    ok "Done! Report: $report_file"
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
    compare)
        check_database_url
        ensure_reports_dir

        RUSTLS_JSON="$REPORTS_DIR/loadtest-rustls-tls-${TIMESTAMP}.json"
        LIBPQ_JSON="$REPORTS_DIR/loadtest-libpq-${TIMESTAMP}.json"
        RUSTLS_REPORT="$REPORTS_DIR/loadtest-rustls-tls-${TIMESTAMP}.md"
        LIBPQ_REPORT="$REPORTS_DIR/loadtest-libpq-${TIMESTAMP}.md"
        COMPARE_REPORT="$REPORTS_DIR/loadtest-comparison-${TIMESTAMP}.md"

        # Run rustls-tls first
        build_loadtest "rustls-tls"
        run_loadtest "rustls-tls" "$RUSTLS_REPORT" "$RUSTLS_JSON"

        # Cooldown between runs to let the DB settle
        info "Cooldown (30s) before libpq run..."
        sleep 30

        # Then libpq
        build_loadtest "libpq"
        run_loadtest "libpq" "$LIBPQ_REPORT" "$LIBPQ_JSON"

        # Generate comparison report using the comparison binary.
        # Build with rustls-tls feature (the compare binary itself doesn't use pg_walstream,
        # but it shares the crate so needs a valid feature selection).
        info "Generating comparison report..."
        cd "$SCRIPT_DIR"
        cargo run --release --no-default-features --features rustls-tls \
            --bin pg-walstream-compare -- \
            "$RUSTLS_JSON" "$LIBPQ_JSON" --output "$COMPARE_REPORT"

        echo ""
        ok "Done! Reports:"
        echo "  rustls-tls:  $RUSTLS_REPORT"
        echo "  libpq:       $LIBPQ_REPORT"
        echo "  comparison:  $COMPARE_REPORT"
        ;;
    -h|--help|help)
        usage
        ;;
    *)
        err "Unknown argument: $1"
        usage
        exit 1
        ;;
esac
