//! Benchmark: WAL message parsing pipeline — zero-copy vs copy paths
//!
//! Measures the performance difference between:
//!   1. **Old path (copy)**: `Vec<u8>` → `BufferReader::new(&[u8])` → `parse_wal_message(&[u8])`
//!   2. **New path (zero-copy)**: `Bytes` → `BufferReader::from_bytes(Bytes)` → `parse_wal_message_bytes(Bytes)`
//!
//! Benchmark groups:
//!   - `buffer_reader_create` — Creating a BufferReader from &[u8] (copy) vs Bytes (zero-copy)
//!   - `wal_header_parse`     — Parsing the 25-byte WAL XLogData header
//!   - `parse_begin`          — Full parse of a Begin message (header + payload)
//!   - `parse_insert`         — Full parse of an Insert message with relation + tuple data
//!   - `parse_insert_pipeline`— End-to-end: raw bytes → ChangeEvent for an Insert
//!   - `parse_multi_column`   — Insert parsing with varying column counts (5, 10, 20, 50)
//!
//! Run:
//!   cargo bench --bench wal_pipeline

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use pg_walstream::{BufferReader, LogicalReplicationParser};
use std::hint::black_box;

// ---------------------------------------------------------------------------
// Helper: build raw WAL message bytes
// ---------------------------------------------------------------------------

/// Build a WAL XLogData envelope: 'w' + start_lsn(8) + end_lsn(8) + send_time(8) + payload
fn build_wal_envelope(start_lsn: u64, end_lsn: u64, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(25 + payload.len());
    buf.push(b'w');
    buf.extend_from_slice(&start_lsn.to_be_bytes());
    buf.extend_from_slice(&end_lsn.to_be_bytes());
    buf.extend_from_slice(&0i64.to_be_bytes()); // send_time
    buf.extend_from_slice(payload);
    buf
}

/// Build a Begin message payload: 'B' + final_lsn(8) + timestamp(8) + xid(4)
fn build_begin_payload(final_lsn: u64, xid: u32) -> Vec<u8> {
    let mut buf = Vec::with_capacity(21);
    buf.push(b'B');
    buf.extend_from_slice(&final_lsn.to_be_bytes());
    buf.extend_from_slice(&0i64.to_be_bytes()); // timestamp
    buf.extend_from_slice(&xid.to_be_bytes());
    buf
}

/// Build a Commit message payload: 'C' + flags(1) + commit_lsn(8) + end_lsn(8) + timestamp(8)
fn build_commit_payload(commit_lsn: u64, end_lsn: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(26);
    buf.push(b'C');
    buf.push(0u8); // flags
    buf.extend_from_slice(&commit_lsn.to_be_bytes());
    buf.extend_from_slice(&end_lsn.to_be_bytes());
    buf.extend_from_slice(&0i64.to_be_bytes()); // timestamp
    buf
}

/// Build a Relation message payload
fn build_relation_payload(relation_id: u32, n_columns: usize) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(b'R');
    buf.extend_from_slice(&relation_id.to_be_bytes());
    buf.extend_from_slice(b"public\0");
    buf.extend_from_slice(b"bench_table\0");
    buf.push(b'd'); // replica identity: default
    buf.extend_from_slice(&(n_columns as u16).to_be_bytes());
    for i in 0..n_columns {
        buf.push(if i == 0 { 1u8 } else { 0u8 }); // first column is key
        let col_name = format!("col_{i}\0");
        buf.extend_from_slice(col_name.as_bytes());
        buf.extend_from_slice(&25u32.to_be_bytes()); // text OID
        buf.extend_from_slice(&(-1i32).to_be_bytes()); // type modifier
    }
    buf
}

/// Build an Insert message payload with text columns
fn build_insert_payload(relation_id: u32, column_values: &[&str]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(b'I');
    buf.extend_from_slice(&relation_id.to_be_bytes());
    buf.push(b'N'); // new tuple marker
    buf.extend_from_slice(&(column_values.len() as u16).to_be_bytes());
    for val in column_values {
        buf.push(b't'); // text format
        buf.extend_from_slice(&(val.len() as u32).to_be_bytes());
        buf.extend_from_slice(val.as_bytes());
    }
    buf
}

/// Generate column values for N columns
fn generate_column_values(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("value_{i}_data")).collect()
}

/// A column value covering the four `parse_tuple_data` wire variants.
enum Col<'a> {
    Text(&'a str),
    Binary(&'a [u8]),
    Null,
    Unchanged,
}

/// Build a TupleData block: `n_columns(u16)` then per column a type byte and,
/// for `Text`/`Binary`, a `u32` length + data. Mirrors what `parse_tuple_data` reads.
fn build_tuple(cols: &[Col]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(cols.len() as u16).to_be_bytes());
    for c in cols {
        match c {
            Col::Text(s) => {
                buf.push(b't');
                buf.extend_from_slice(&(s.len() as u32).to_be_bytes());
                buf.extend_from_slice(s.as_bytes());
            }
            Col::Binary(data) => {
                buf.push(b'b');
                buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
                buf.extend_from_slice(data);
            }
            Col::Null => buf.push(b'n'),
            Col::Unchanged => buf.push(b'u'),
        }
    }
    buf
}

/// Build an Insert payload from arbitrary column types: `'I'` + relation_id + `'N'` + tuple.
fn build_insert_cols(relation_id: u32, cols: &[Col]) -> Vec<u8> {
    let mut buf = vec![b'I'];
    buf.extend_from_slice(&relation_id.to_be_bytes());
    buf.push(b'N');
    buf.extend_from_slice(&build_tuple(cols));
    buf
}

/// Build an Update payload: `'U'` + relation_id + optional (`'O'` old tuple) + `'N'` new tuple.
/// `old = Some(..)` models REPLICA IDENTITY FULL (the full old row is present).
fn build_update_payload(relation_id: u32, old: Option<&[Col]>, new: &[Col]) -> Vec<u8> {
    let mut buf = vec![b'U'];
    buf.extend_from_slice(&relation_id.to_be_bytes());
    if let Some(old_cols) = old {
        buf.push(b'O');
        buf.extend_from_slice(&build_tuple(old_cols));
    }
    buf.push(b'N');
    buf.extend_from_slice(&build_tuple(new));
    buf
}

/// Build a Delete payload: `'D'` + relation_id + `'O'` old/key tuple.
fn build_delete_payload(relation_id: u32, key: &[Col]) -> Vec<u8> {
    let mut buf = vec![b'D'];
    buf.extend_from_slice(&relation_id.to_be_bytes());
    buf.push(b'O');
    buf.extend_from_slice(&build_tuple(key));
    buf
}

// ---------------------------------------------------------------------------
// 1. BufferReader creation: copy vs zero-copy
// ---------------------------------------------------------------------------

fn bench_buffer_reader_create(c: &mut Criterion) {
    let mut group = c.benchmark_group("buffer_reader_create");

    for size in [64, 256, 1024, 4096] {
        let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        let bytes_data = Bytes::from(data.clone());

        group.bench_with_input(
            BenchmarkId::new("copy_from_slice", size),
            &data,
            |b, data| {
                b.iter(|| {
                    let reader = BufferReader::new(black_box(data));
                    black_box(reader);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("zero_copy_bytes", size),
            &bytes_data,
            |b, data| {
                b.iter(|| {
                    let reader = BufferReader::from_bytes(black_box(data.clone()));
                    black_box(reader);
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 2. WAL header parsing
// ---------------------------------------------------------------------------

fn bench_wal_header_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_header_parse");

    let payload = build_begin_payload(0x2000, 42);
    let envelope = build_wal_envelope(0x1000, 0x1500, &payload);
    let envelope_bytes = Bytes::from(envelope.clone());

    // Old path: &[u8] → BufferReader::new (copies)
    group.bench_function("copy_path", |b| {
        b.iter(|| {
            let mut reader = BufferReader::new(black_box(&envelope));
            let _msg_type = reader.read_u8().unwrap();
            let _start_lsn = reader.read_u64().unwrap();
            let _end_lsn = reader.read_u64().unwrap();
            let _send_time = reader.read_i64().unwrap();
            black_box(reader.remaining());
        });
    });

    // New path: Bytes → BufferReader::from_bytes (zero-copy)
    group.bench_function("zero_copy_path", |b| {
        b.iter(|| {
            let mut reader = BufferReader::from_bytes(black_box(envelope_bytes.clone()));
            let _msg_type = reader.read_u8().unwrap();
            let _start_lsn = reader.read_u64().unwrap();
            let _end_lsn = reader.read_u64().unwrap();
            let _send_time = reader.read_i64().unwrap();
            black_box(reader.remaining());
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 3. Full Begin message parse
// ---------------------------------------------------------------------------

fn bench_parse_begin(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_begin");

    let payload = build_begin_payload(0x2000, 42);
    let payload_bytes = Bytes::from(payload.clone());

    // Old: parse_wal_message(&[u8]) — copies into BufferReader
    group.bench_function("copy_path", |b| {
        let mut parser = LogicalReplicationParser::with_protocol_version(2);
        b.iter(|| {
            let result = parser.parse_wal_message(black_box(&payload)).unwrap();
            black_box(result);
        });
    });

    // New: parse_wal_message_bytes(Bytes) — zero-copy
    group.bench_function("zero_copy_path", |b| {
        let mut parser = LogicalReplicationParser::with_protocol_version(2);
        b.iter(|| {
            let result = parser
                .parse_wal_message_bytes(black_box(payload_bytes.clone()))
                .unwrap();
            black_box(result);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 4. Full Insert message parse (relation + insert)
// ---------------------------------------------------------------------------

fn bench_parse_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_insert");

    let n_cols = 10;
    let relation_payload = build_relation_payload(100, n_cols);
    let values: Vec<String> = generate_column_values(n_cols);
    let val_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
    let insert_payload = build_insert_payload(100, &val_refs);
    let insert_bytes = Bytes::from(insert_payload.clone());

    // Old: parse_wal_message(&[u8])
    group.bench_function("copy_path", |b| {
        let mut parser = LogicalReplicationParser::with_protocol_version(2);
        // Pre-register the relation
        parser.parse_wal_message(&relation_payload).unwrap();
        b.iter(|| {
            let result = parser
                .parse_wal_message(black_box(&insert_payload))
                .unwrap();
            black_box(result);
        });
    });

    // New: parse_wal_message_bytes(Bytes)
    group.bench_function("zero_copy_path", |b| {
        let mut parser = LogicalReplicationParser::with_protocol_version(2);
        parser.parse_wal_message(&relation_payload).unwrap();
        b.iter(|| {
            let result = parser
                .parse_wal_message_bytes(black_box(insert_bytes.clone()))
                .unwrap();
            black_box(result);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 5. End-to-end WAL pipeline: raw bytes → parsed message (header + payload)
// ---------------------------------------------------------------------------

fn bench_parse_insert_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_insert_pipeline");

    let n_cols = 10;
    let relation_payload = build_relation_payload(100, n_cols);
    let values: Vec<String> = generate_column_values(n_cols);
    let val_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
    let insert_payload = build_insert_payload(100, &val_refs);
    let envelope = build_wal_envelope(0x1000, 0x1500, &insert_payload);
    let envelope_bytes = Bytes::from(envelope.clone());

    // Old path: Vec<u8> → BufferReader::new → parse header → parse_wal_message(&[u8])
    group.bench_function("copy_path", |b| {
        let mut parser = LogicalReplicationParser::with_protocol_version(2);
        parser.parse_wal_message(&relation_payload).unwrap();
        b.iter(|| {
            // Simulate the old pipeline: data arrives as Vec<u8>
            let data = black_box(&envelope);
            let mut reader = BufferReader::new(data);
            let _msg_type = reader.read_u8().unwrap();
            let _start_lsn = reader.read_u64().unwrap();
            let _end_lsn = reader.read_u64().unwrap();
            let _send_time = reader.read_i64().unwrap();
            // Old: read remaining as &[u8] (copy), then parse_wal_message (copies again)
            let remaining = reader.remaining();
            let payload_bytes = reader.read_bytes(remaining).unwrap();
            let result = parser.parse_wal_message(&payload_bytes).unwrap();
            black_box(result);
        });
    });

    // New path: Bytes → BufferReader::from_bytes → parse header → read_bytes_buf → parse_wal_message_bytes
    group.bench_function("zero_copy_path", |b| {
        let mut parser = LogicalReplicationParser::with_protocol_version(2);
        parser.parse_wal_message(&relation_payload).unwrap();
        b.iter(|| {
            let data = black_box(envelope_bytes.clone());
            let mut reader = BufferReader::from_bytes(data);
            let _msg_type = reader.read_u8().unwrap();
            let _start_lsn = reader.read_u64().unwrap();
            let _end_lsn = reader.read_u64().unwrap();
            let _send_time = reader.read_i64().unwrap();
            // New: read_bytes_buf returns a Bytes slice (zero-copy)
            let remaining = reader.remaining();
            let payload = reader.read_bytes_buf(remaining).unwrap();
            let result = parser.parse_wal_message_bytes(payload).unwrap();
            black_box(result);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 6. Insert parsing with varying column counts
// ---------------------------------------------------------------------------

fn bench_parse_multi_column(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_multi_column_insert");

    for n_cols in [5, 10, 20, 50] {
        let relation_payload = build_relation_payload(100, n_cols);
        let values: Vec<String> = generate_column_values(n_cols);
        let val_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
        let insert_payload = build_insert_payload(100, &val_refs);
        let envelope = build_wal_envelope(0x1000, 0x1500, &insert_payload);
        let envelope_bytes = Bytes::from(envelope.clone());

        // Old pipeline (two copies)
        group.bench_with_input(
            BenchmarkId::new("copy_path", n_cols),
            &envelope,
            |b, envelope| {
                let mut parser = LogicalReplicationParser::with_protocol_version(2);
                parser.parse_wal_message(&relation_payload).unwrap();
                b.iter(|| {
                    let mut reader = BufferReader::new(black_box(envelope.as_slice()));
                    let _ = reader.read_u8().unwrap();
                    let _ = reader.read_u64().unwrap();
                    let _ = reader.read_u64().unwrap();
                    let _ = reader.read_i64().unwrap();
                    let remaining = reader.remaining();
                    let payload = reader.read_bytes(remaining).unwrap();
                    let result = parser.parse_wal_message(&payload).unwrap();
                    black_box(result);
                });
            },
        );

        // New pipeline (zero-copy)
        group.bench_with_input(
            BenchmarkId::new("zero_copy_path", n_cols),
            &envelope_bytes,
            |b, envelope_bytes| {
                let mut parser = LogicalReplicationParser::with_protocol_version(2);
                parser.parse_wal_message(&relation_payload).unwrap();
                b.iter(|| {
                    let mut reader = BufferReader::from_bytes(black_box(envelope_bytes.clone()));
                    let _ = reader.read_u8().unwrap();
                    let _ = reader.read_u64().unwrap();
                    let _ = reader.read_u64().unwrap();
                    let _ = reader.read_i64().unwrap();
                    let remaining = reader.remaining();
                    let payload = reader.read_bytes_buf(remaining).unwrap();
                    let result = parser.parse_wal_message_bytes(payload).unwrap();
                    black_box(result);
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 7. Commit message parse comparison
// ---------------------------------------------------------------------------

fn bench_parse_commit(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_commit");

    let payload = build_commit_payload(0x2000, 0x2100);
    let payload_bytes = Bytes::from(payload.clone());

    group.bench_function("copy_path", |b| {
        let mut parser = LogicalReplicationParser::with_protocol_version(2);
        b.iter(|| {
            let result = parser.parse_wal_message(black_box(&payload)).unwrap();
            black_box(result);
        });
    });

    group.bench_function("zero_copy_path", |b| {
        let mut parser = LogicalReplicationParser::with_protocol_version(2);
        b.iter(|| {
            let result = parser
                .parse_wal_message_bytes(black_box(payload_bytes.clone()))
                .unwrap();
            black_box(result);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 8. Update message parse — REPLICA IDENTITY DEFAULT (new only) vs FULL (old+new)
// ---------------------------------------------------------------------------

fn bench_parse_update(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_update");

    let new_vals = generate_column_values(10);
    let new_cols: Vec<Col> = new_vals.iter().map(|s| Col::Text(s.as_str())).collect();

    // REPLICA IDENTITY DEFAULT: only the new tuple is sent.
    let default_payload = Bytes::from(build_update_payload(100, None, &new_cols));
    group.bench_function("replica_default", |b| {
        let mut parser = LogicalReplicationParser::with_protocol_version(2);
        b.iter(|| {
            let result = parser
                .parse_wal_message_bytes(black_box(default_payload.clone()))
                .unwrap();
            black_box(result);
        });
    });

    // REPLICA IDENTITY FULL: old tuple + new tuple (two tuples parsed per message).
    let old_vals = generate_column_values(10);
    let old_cols: Vec<Col> = old_vals.iter().map(|s| Col::Text(s.as_str())).collect();
    let full_payload = Bytes::from(build_update_payload(100, Some(&old_cols), &new_cols));
    group.bench_function("replica_full", |b| {
        let mut parser = LogicalReplicationParser::with_protocol_version(2);
        b.iter(|| {
            let result = parser
                .parse_wal_message_bytes(black_box(full_payload.clone()))
                .unwrap();
            black_box(result);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 9. Delete message parse
// ---------------------------------------------------------------------------

fn bench_parse_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_delete");

    let key_vals = generate_column_values(10);
    let key_cols: Vec<Col> = key_vals.iter().map(|s| Col::Text(s.as_str())).collect();
    let payload = Bytes::from(build_delete_payload(100, &key_cols));

    group.bench_function("zero_copy_path", |b| {
        let mut parser = LogicalReplicationParser::with_protocol_version(2);
        b.iter(|| {
            let result = parser
                .parse_wal_message_bytes(black_box(payload.clone()))
                .unwrap();
            black_box(result);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 10. Tuple with mixed column types: text / null / unchanged (TOAST) / binary
// ---------------------------------------------------------------------------

fn bench_parse_tuple_types(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_tuple_types");

    // 12 columns cycling through all four `parse_tuple_data` arms (t/n/u/b).
    let bin = [0xDEu8, 0xAD, 0xBE, 0xEF];
    let cols = vec![
        Col::Text("alpha"),
        Col::Null,
        Col::Unchanged,
        Col::Binary(&bin),
        Col::Text("bravo"),
        Col::Null,
        Col::Unchanged,
        Col::Binary(&bin),
        Col::Text("charlie"),
        Col::Null,
        Col::Unchanged,
        Col::Binary(&bin),
    ];
    let payload = Bytes::from(build_insert_cols(100, &cols));

    group.bench_function("mixed_t_n_u_b", |b| {
        let mut parser = LogicalReplicationParser::with_protocol_version(2);
        b.iter(|| {
            let result = parser
                .parse_wal_message_bytes(black_box(payload.clone()))
                .unwrap();
            black_box(result);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 11. Large column values — guards the zero-copy invariant (time must stay ~flat
// across sizes; a stray copy would make this scale with payload size).
// ---------------------------------------------------------------------------

fn bench_parse_large_values(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_large_values");

    for size in [64usize, 1024, 65536] {
        let big = vec![b'x'; size];
        let big_str = core::str::from_utf8(&big).unwrap();
        // Two large text columns + one large binary column.
        let cols = vec![Col::Text(big_str), Col::Binary(&big), Col::Text(big_str)];
        let payload = Bytes::from(build_insert_cols(100, &cols));

        group.throughput(criterion::Throughput::Bytes((size * 3) as u64));
        group.bench_with_input(
            BenchmarkId::new("zero_copy_path", size),
            &payload,
            |b, payload| {
                let mut parser = LogicalReplicationParser::with_protocol_version(2);
                b.iter(|| {
                    let result = parser
                        .parse_wal_message_bytes(black_box(payload.clone()))
                        .unwrap();
                    black_box(result);
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 12. Realistic transaction stream: BEGIN → INSERT×N → COMMIT parsed in sequence
// on one reused parser (steady-state / state-transition cost).
// ---------------------------------------------------------------------------

fn bench_parse_transaction(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_transaction");

    const N_INSERTS: usize = 64;
    let vals = generate_column_values(10);
    let cols: Vec<Col> = vals.iter().map(|s| Col::Text(s.as_str())).collect();

    let begin = Bytes::from(build_begin_payload(0x2000, 42));
    let insert = Bytes::from(build_insert_cols(100, &cols));
    let commit = Bytes::from(build_commit_payload(0x2000, 0x2100));

    group.bench_function("begin_inserts_commit", |b| {
        let mut parser = LogicalReplicationParser::with_protocol_version(2);
        b.iter(|| {
            black_box(parser.parse_wal_message_bytes(begin.clone()).unwrap());
            for _ in 0..N_INSERTS {
                black_box(parser.parse_wal_message_bytes(insert.clone()).unwrap());
            }
            black_box(parser.parse_wal_message_bytes(commit.clone()).unwrap());
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_buffer_reader_create,
    bench_wal_header_parse,
    bench_parse_begin,
    bench_parse_insert,
    bench_parse_insert_pipeline,
    bench_parse_multi_column,
    bench_parse_commit,
    bench_parse_update,
    bench_parse_delete,
    bench_parse_tuple_types,
    bench_parse_large_values,
    bench_parse_transaction,
);
criterion_main!(benches);
