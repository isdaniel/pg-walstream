# pg-walstream Load Tests

Measures consumer-side CPU, throughput, RSS, and latency for both connection
backends. The write generator runs as a **separate OS process**, so reported
CPU/RSS is pure pg-walstream consumer overhead.

## Running

```bash
export DATABASE_URL='postgresql://user:pass@host:5432/db?sslmode=require'

./run-loadtest.sh                # rustls-tls (default)
./run-loadtest.sh libpq          # libpq + OpenSSL
./run-loadtest.sh compare        # both, plus a comparison report
```

14 scenarios x (10 s warmup + 30 s measure) ≈ 10 minutes per backend.
Reports land in `../reports/`.

Requires PostgreSQL with `wal_level = logical` and a role holding the
`REPLICATION` attribute.

## Two traps that silently produce wrong numbers

### 1. Verify the binary actually uses the backend you asked for

`Cargo.toml` declares:

```toml
pg_walstream = { path = "..", default-features = false }
```

**`default-features = false` is load-bearing.** The parent crate defaults to
`rustls-tls`, and `src/connection/mod.rs` gates the libpq backend on
`all(feature = "libpq", not(feature = "rustls-tls"))`. Without it,
`--no-default-features --features libpq` still enables `rustls-tls`, which wins
the `cfg` — you get a rustls binary with a libpq label. No build error, no
warning, just a wrong measurement. This was the case until 2026-09-15, which
invalidated every backend comparison taken before then.

Check before trusting any result:

```bash
ldd target/release/<bin> | grep libpq        # real libpq build: links libpq.so.5
strings target/release/<bin> | grep -c rustls # real libpq build: 0
```

A genuine rustls build is the mirror image: no `libpq.so.5`, ~1,700 rustls strings.

Note that `cargo build` writes both backends to the same
`target/release/pg-walstream-loadtest` path. To run them back to back, copy the
binary aside after each build rather than assuming the path still holds the
backend you last wanted.

### 2. `sslmode` dominates the backend comparison

| `sslmode` | rustls-tls | libpq | Ratio |
|---|---:|---:|---:|
| `disable` | 5,859 ev/1%cpu | 6,262 ev/1%cpu | 0.98x — tie |
| `require` | 3,817 ev/1%cpu | 1,693 ev/1%cpu | 2.22x rustls-tls |

Without TLS the backends are indistinguishable. With TLS, libpq spends ~20% of
process CPU inside OpenSSL — of which only 0.84% is actual AES-GCM; the rest is
per-call bookkeeping (`ERR_clear_error`, `BIO_ctrl`, `EVP_CIPHER_CTX_ctrl`).
That cost scales with **message count, not bytes**, because pg-walstream issues
one `PQgetCopyData` per WAL message.

A result quoted without its `sslmode` is not interpretable.

## Interpreting results over a remote link

Over a high-latency link, low-batch scenarios are bounded by the round trip, not
by the consumer:

- **Network-bound** — throughput approaches `writers x batch_size / RTT`. At 50 ms
  RTT, `Batch-100` (1 writer, 100-row batches) caps near 2,000 ev/s. Its
  events-per-CPU figure measures the link, not the library.
- **Low-rate** — consumer below ~5% CPU. Fixed costs (feedback timer, idle poll)
  dominate the denominator and understate efficiency for both backends.
- **CPU-bound** — the only scenarios where a backend efficiency comparison is
  meaningful.

Tune the host first when testing across regions; see *Linux VM TCP Tuning* in the
[root README](../README.md).

## Layout

| Path | Purpose |
|---|---|
| `src/main.rs` | Scenario definitions and the measurement driver |
| `src/consumer.rs` | The measured pg-walstream consumer |
| `src/generator.rs`, `src/bin/generator.rs` | Write load, spawned as a child process |
| `src/metrics.rs`, `src/sampler.rs` | CPU/RSS sampling and histograms |
| `src/reporter.rs` | Markdown report generation |
| `src/bin/compare.rs` | Diffs two JSON runs into a comparison report |
