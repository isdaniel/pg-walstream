# Load Test Comparison Report

**VM**: Intel Xeon Platinum 8370C, 8 cores, 16 GB RAM, Ubuntu 22.04  
**Config**: proto_version=4, streaming=parallel, warmup=10s, measure=30s

## 1. DML Throughput (events/sec)

| Scenario | PG16 proto4+parallel | PG18 +binary+directTLS | PG18 +binary+directTLS +COPY |
|----------|----------|----------|----------|
| Baseline | 148,533 | 168,205 | 209,193 |
| Batch-100 | 22,270 | 141,597 | 199,780 |
| Batch-5000 | 132,623 | 151,820 | 190,687 |
| 4-Writers | 135,036 | 159,233 | 193,354 |
| Wide-20col | 18,917 | 172,772 | 173,283 |
| Payload-2KB | 14,017 | 114,884 | 134,323 |
| Mixed-DML | 42,198 | 176,580 | 186,019 |
| Stress-16w | 125,657 | 130,625 | 185,044 |
| Stress-32w | 111,970 | 133,880 | 184,718 |
| Stress-48w | 101,180 | 129,667 | 181,012 |
| Stress-64w | 103,937 | 125,082 | 182,349 |
| Stress-96w | 96,000 | 119,140 | 177,576 |
| Stress-128w | 87,352 | 109,594 | 160,293 |
| Stress-192w | 71,316 | 98,482 | 171,585 |

## 2. Data Throughput (MB/s)

| Scenario | PG16 proto4+parallel | PG18 +binary+directTLS | PG18 +binary+directTLS +COPY |
|----------|----------|----------|----------|
| Baseline | 30.4 | 31.1 | 38.7 |
| Batch-100 | 4.6 | 26.2 | 37.0 |
| Batch-5000 | 27.2 | 28.1 | 35.3 |
| 4-Writers | 27.7 | 29.5 | 35.8 |
| Wide-20col | 21.3 | 50.7 | 51.5 |
| Payload-2KB | 28.3 | 43.9 | 57.1 |
| Mixed-DML | 7.9 | 32.1 | 33.8 |
| Stress-16w | 25.7 | 24.2 | 34.2 |
| Stress-32w | 22.9 | 24.8 | 34.2 |
| Stress-48w | 20.7 | 24.0 | 33.5 |
| Stress-64w | 21.3 | 23.1 | 33.7 |
| Stress-96w | 19.6 | 22.0 | 32.9 |
| Stress-128w | 17.9 | 20.3 | 29.7 |
| Stress-192w | 14.6 | 18.2 | 31.7 |

## 3. Process Memory RSS (MB)

| Scenario | PG16 Avg | PG16 Peak | PG18 Avg | PG18 Peak | PG18+COPY Avg | PG18+COPY Peak |
|---|---:|---:|---:|---:|---:|---:|
| Baseline | 15.6 | 15.6 | 15.4 | 15.4 | 15.3 | 15.3 |
| Batch-100 | 16.8 | 16.8 | 16.1 | 16.1 | 16.2 | 16.2 |
| Batch-5000 | 16.9 | 16.9 | 16.2 | 16.2 | 16.5 | 16.5 |
| 4-Writers | 17.0 | 17.0 | 16.6 | 16.6 | 16.7 | 16.7 |
| Wide-20col | 17.0 | 17.0 | 16.6 | 16.6 | 16.7 | 16.7 |
| Payload-2KB | 17.1 | 17.1 | 16.7 | 16.7 | 16.8 | 16.8 |
| Mixed-DML | 17.2 | 17.2 | 16.7 | 16.7 | 16.9 | 16.9 |
| Stress-16w | 17.0 | 17.0 | 16.7 | 16.7 | 16.8 | 16.8 |
| Stress-32w | 17.4 | 17.4 | 16.8 | 16.8 | 16.8 | 16.8 |
| Stress-48w | 17.4 | 17.4 | 16.8 | 16.8 | 16.8 | 16.8 |
| Stress-64w | 17.6 | 17.6 | 16.8 | 16.8 | 16.8 | 16.8 |
| Stress-96w | 17.4 | 17.4 | 17.0 | 17.0 | 16.8 | 16.8 |
| Stress-128w | 17.4 | 17.4 | 17.0 | 17.0 | 16.8 | 16.8 |
| Stress-192w | 17.7 | 17.7 | 17.0 | 17.0 | 16.8 | 16.8 |

## 4. CPU Efficiency (DML events/sec per 1% CPU)

| Scenario | PG16 proto4+parallel | PG18 +binary+directTLS | PG18 +binary+directTLS +COPY |
|----------|----------|----------|----------|
| Baseline | 5,689 | 5,637 | 5,920 |
| Batch-100 | 3,966 | 5,572 | 5,693 |
| Batch-5000 | 5,379 | 5,733 | 5,440 |
| 4-Writers | 5,000 | 5,647 | 5,571 |
| Wide-20col | 2,369 | 5,059 | 5,517 |
| Payload-2KB | 2,767 | 5,192 | 5,018 |
| Mixed-DML | 4,408 | 5,495 | 5,254 |
| Stress-16w | 4,968 | 5,071 | 5,507 |
| Stress-32w | 4,886 | 5,188 | 5,411 |
| Stress-48w | 4,688 | 5,040 | 5,281 |
| Stress-64w | 4,656 | 4,846 | 5,289 |
| Stress-96w | 4,480 | 5,168 | 5,220 |
| Stress-128w | 4,681 | 5,084 | 5,232 |
| Stress-192w | 4,807 | 4,982 | 5,406 |

## 5. Stress Ramp: Throughput Scaling

| Writers | PG16 proto4+parallel | PG18 +binary+directTLS | PG18 +binary+directTLS +COPY |
|----------|----------|----------|----------|
| 16 | 125,657 | 130,625 | 185,044 |
| 32 | 111,970 | 133,880 | 184,718 |
| 48 | 101,180 | 129,667 | 181,012 |
| 64 | 103,937 | 125,082 | 182,349 |
| 96 | 96,000 | 119,140 | 177,576 |
| 128 | 87,352 | 109,594 | 160,293 |
| 192 | 71,316 | 98,482 | 171,585 |

## 6. Test Configuration

| Setting | PG16 proto4+parallel | PG18 +binary+directTLS | PG18 +binary+directTLS+COPY |
|---|---|---|---|
| PostgreSQL | 16 | 18.3 | 18.3 |
| proto_version | 4 | 4 | 4 |
| streaming | parallel | parallel | parallel |
| binary | off | **on** | **on** |
| sslnegotiation | postgres | **direct** | **direct** |
| Generator | INSERT | INSERT | **COPY** |
| Backend | rustls-tls | rustls-tls | rustls-tls |
