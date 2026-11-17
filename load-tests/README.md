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

Tune the host first when testing across regions; see
[Linux VM TCP tuning](#linux-vm-tcp-tuning-for-production) below.

## Reference results

Environment: an 8-vCPU Linux VM (TCP-tuned as below: 64 MB buffers, BBR) streaming
from a **remote Azure PostgreSQL Flexible Server 18.6** across regions (~50 ms RTT).
Each scenario ran 10 s warmup + 30 s measure. Process CPU/RSS reflect **only the
pg-walstream consumer**: the write generator runs as a separate OS process.
Backend A is rustls-tls (aws-lc-rs); backend B is libpq 18.6 with OpenSSL 3.0.2.

### CPU efficiency (DML events/sec per 1% CPU)

The primary efficiency metric: DML events processed per 1% of consumer CPU. Higher is better.

| Scenario | rustls plain | libpq plain | rustls TLS | libpq TLS | libpq CPU vs rustls (TLS) |
|----------|----------:|----------:|----------:|----------:|----------:|
| Baseline | 6,670 | 7,135 | 3,759 | 1,739 | 2.13x |
| Batch-100 | 2,080 | 1,940 | 1,633 | 996 | 1.61x |
| Batch-5000 | 7,992 | 7,126 | 4,764 | 1,789 | 2.63x |
| 4-Writers | 6,995 | 7,093 | 4,052 | 2,098 | 1.84x |
| Wide-20col | 2,162 | 2,102 | 1,609 | 1,067 | 1.51x |
| Payload-2KB | 2,279 | 2,165 | 1,611 | 1,104 | 1.46x |
| Mixed-DML | 2,926 | 2,946 | 2,458 | 1,370 | 1.79x |
| Stress-16w | 6,088 | 6,437 | 3,917 | 1,800 | 2.62x |
| Stress-32w | 6,237 | 6,335 | 3,979 | 1,810 | 2.40x |
| Stress-48w | 5,630 | 6,190 | 3,854 | 1,647 | 2.23x |
| Stress-64w | 5,615 | 5,737 | 3,779 | 1,642 | 2.12x |
| Stress-96w | 5,546 | 5,534 | 3,524 | 1,568 | 2.24x |
| Stress-128w | 5,328 | 5,852 | 3,356 | 1,566 | 2.25x |
| Stress-192w | 5,554 | 5,457 | 3,754 | 1,511 | 2.33x |

Without TLS, per-scenario ratios scatter 0.90x - 1.12x with no directional lean:
noise. TLS efficiency retained: **rustls-tls 66%, libpq 28%**.

### Why: OpenSSL per-call overhead, not crypto

`perf` on the consumer during 4-Writers with TLS enabled:

| | libpq | rustls-tls |
|---|---:|---:|
| TLS library share of process CPU | **20.79%** (libcrypto + libssl) | 12.48% (aws-lc-rs, static) |
| ...of which is actual AES-GCM | **0.84%** | most of it |

libpq spends ~20 points of CPU on TLS **bookkeeping** and under 1 point on real
crypto. The hot OpenSSL symbols are all per-call setup: `ERR_clear_error` (1.67%,
which libpq calls before every read), `BIO_ctrl`, `EVP_CIPHER_CTX_ctrl`. `libpq.so`
itself is only 2.39%, so libpq's own code is cheap; the cost is the OpenSSL call
sequence it performs per read. A newer OpenSSL would likely narrow this gap but not
close it, since the per-read error-stack clear is libpq's own code.

### Throughput and resources

Throughput is bounded by the write generator and the ~50 ms link, not by the
consumer, so both backends land in the same range. The difference is the CPU spent
getting there.

| Metric | rustls-tls | libpq |
|--------|------:|------:|
| Peak DML events/sec (no TLS) | 213,392 | 202,347 |
| Peak DML events/sec (TLS) | 161,151 | 156,813 |
| CPU% at TLS peak | **38.9** | **80.9** |
| Avg RSS (MB) | 15.7 | 17.0 |
| P50 inter-event latency | 1 µs | 1 µs |

RSS and latency show no backend advantage.

## Linux VM TCP tuning for production

When streaming WAL over high-latency links (e.g., cross-region Azure PostgreSQL), the
default Linux TCP buffer sizes can become the throughput bottleneck. The kernel's
default `rmem_max` of 208 KB limits the TCP receive window, which, combined with
round-trip latency, caps throughput via the **Bandwidth-Delay Product (BDP)**:

```conf
# --- TCP buffer sizes ---
# Allow up to 64 MB per-socket receive/send buffers (kernel will auto-tune within this ceiling)
net.core.rmem_max = 67108864
net.core.wmem_max = 67108864

# TCP auto-tuning ranges: min / default / max (bytes)
# The kernel dynamically adjusts each socket's buffer within these bounds
net.ipv4.tcp_rmem = 4096 262144 67108864
net.ipv4.tcp_wmem = 4096 262144 67108864

# --- Congestion control ---
# BBR provides significantly better throughput than cubic on high-latency links
net.ipv4.tcp_congestion_control = bbr

# --- Packet backlog ---
# Increase the NIC receive queue (helps at high packet rates)
net.core.netdev_max_backlog = 5000
```

Apply immediately with `sudo sysctl --system`.

| Parameter | Default | Recommended | Why |
|-----------|---------|-------------|-----|
| `rmem_max` | 208 KB | 64 MB | Caps TCP receive window; directly limits throughput on high-RTT links |
| `wmem_max` | 208 KB | 64 MB | Caps TCP send window; limits outbound throughput for feedback messages |
| `tcp_rmem` (max) | 6 MB | 64 MB | Per-socket auto-tuned receive buffer ceiling |
| `tcp_wmem` (max) | 4 MB | 64 MB | Per-socket auto-tuned send buffer ceiling |
| `tcp_congestion_control` | cubic | bbr | BBR reacts to actual bandwidth, not packet loss; better on cloud networks |
| `netdev_max_backlog` | 1000 | 5000 | Prevents packet drops under burst traffic at NIC level |

> **Note:** These settings affect all TCP connections on the VM, not just pg-walstream.
> The kernel auto-tunes actual buffer usage within the configured ceiling, so idle
> connections do not consume 64 MB each.

## Layout

| Path | Purpose |
|---|---|
| `src/main.rs` | Scenario definitions and the measurement driver |
| `src/consumer.rs` | The measured pg-walstream consumer |
| `src/generator.rs`, `src/bin/generator.rs` | Write load, spawned as a child process |
| `src/metrics.rs`, `src/sampler.rs` | CPU/RSS sampling and histograms |
| `src/reporter.rs` | Markdown report generation |
| `src/bin/compare.rs` | Diffs two JSON runs into a comparison report |
