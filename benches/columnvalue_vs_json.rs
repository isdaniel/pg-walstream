//! Benchmark: JSON serialization (serde_json) vs Binary serialization (ColumnValue encode/decode)
//!
//! Measures ChangeEvent performance across two serialization strategies:
//!
//!   1. **JSON (serde_json)**: `serde_json::to_vec` / `serde_json::from_slice`
//!   2. **Binary (ColumnValue)**: `ChangeEvent::encode` / `ChangeEvent::decode`
//!
//! Benchmark groups:
//!   - `construct`    — Build event: HashMap<String,Value> vs RowData<Arc<str>,ColumnValue>
//!   - `serialize`    — Encode event to bytes: serde_json vs binary
//!   - `deserialize`  — Decode bytes back to event: serde_json vs binary
//!   - `round_trip`   — Full encode → decode cycle
//!   - `pipeline`     — Realistic CDC: construct → clone → lookup → serialize
//!
//! Run:
//!   cargo bench --bench columnvalue_vs_json

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use pg_walstream::types::{ChangeEvent, ColumnValue, EventType, Lsn, RowData};
use serde_json::{self, Value};
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;

/// Old-style event: HashMap<String, serde_json::Value> (pre-ColumnValue approach).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct OldChangeEvent {
    schema: String,
    table: String,
    relation_oid: u32,
    data: HashMap<String, Value>,
    lsn: u64,
}

/// Build an old-style event using HashMap + serde_json::Value.
fn build_old_event(n_columns: usize) -> OldChangeEvent {
    let mut data = HashMap::with_capacity(n_columns);
    for i in 0..n_columns {
        data.insert(format!("column_{i}"), serde_json::json!(i.to_string()));
    }
    OldChangeEvent {
        schema: "public".to_string(),
        table: "users".to_string(),
        relation_oid: 16384,
        data,
        lsn: 0x16B374D848,
    }
}

/// Build a new-style ChangeEvent using RowData + ColumnValue.
/// `shared_names` simulates pre-cached `Arc<str>` column names from RelationInfo.
fn build_new_event(shared_names: &[Arc<str>]) -> ChangeEvent {
    let n = shared_names.len();
    let mut row = RowData::with_capacity(n);
    for (i, name) in shared_names.iter().enumerate() {
        row.push(Arc::clone(name), ColumnValue::text(&i.to_string()));
    }
    ChangeEvent::insert("public", "users", 16384, row, Lsn::new(0x16B374D848))
}

/// Pre-create shared column names (mirrors what RelationInfo holds in production).
fn shared_column_names(n: usize) -> Vec<Arc<str>> {
    (0..n)
        .map(|i| Arc::from(format!("column_{i}").as_str()))
        .collect()
}

const COLUMN_COUNTS: [usize; 4] = [5, 10, 20, 50];

// ---------------------------------------------------------------------------
// 1. Construction: HashMap+Value vs RowData+ColumnValue
// ---------------------------------------------------------------------------

/// Compare event construction cost.
fn bench_construct(c: &mut Criterion) {
    let mut group = c.benchmark_group("construct");

    for n_cols in COLUMN_COUNTS {
        let names = shared_column_names(n_cols);

        group.bench_with_input(
            BenchmarkId::new("json_hashmap", n_cols),
            &n_cols,
            |b, &n| {
                b.iter(|| black_box(build_old_event(n)));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("binary_columnvalue", n_cols),
            &names,
            |b, names| {
                b.iter(|| black_box(build_new_event(names)));
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 2. Serialize: serde_json::to_vec vs ChangeEvent::encode
// ---------------------------------------------------------------------------

/// Compare serialization: JSON vs binary encoding.
fn bench_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("serialize");

    for n_cols in COLUMN_COUNTS {
        let names = shared_column_names(n_cols);
        let new_event = build_new_event(&names);

        // JSON serialize (new ChangeEvent via serde)
        group.bench_with_input(
            BenchmarkId::new("json_serde", n_cols),
            &new_event,
            |b, event| {
                b.iter(|| black_box(serde_json::to_vec(event).unwrap()));
            },
        );

        // Binary encode (ChangeEvent::encode)
        group.bench_with_input(
            BenchmarkId::new("binary_encode", n_cols),
            &new_event,
            |b, event| {
                b.iter(|| {
                    let mut buf = bytes::BytesMut::with_capacity(256);
                    event.encode(&mut buf);
                    black_box(buf);
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 3. Deserialize: serde_json::from_slice vs ChangeEvent::decode
// ---------------------------------------------------------------------------

/// Compare deserialization: JSON vs binary decoding.
fn bench_deserialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("deserialize");

    for n_cols in COLUMN_COUNTS {
        let names = shared_column_names(n_cols);
        let new_event = build_new_event(&names);

        let new_json_bytes = serde_json::to_vec(&new_event).unwrap();
        let mut binary_buf = bytes::BytesMut::with_capacity(256);
        new_event.encode(&mut binary_buf);
        let binary_bytes = binary_buf.freeze();

        // JSON deserialize (new ChangeEvent via serde)
        group.bench_with_input(
            BenchmarkId::new("json_serde", n_cols),
            &new_json_bytes,
            |b, data| {
                b.iter(|| {
                    black_box(serde_json::from_slice::<ChangeEvent>(data).unwrap());
                });
            },
        );

        // Binary decode (ChangeEvent::decode)
        group.bench_with_input(
            BenchmarkId::new("binary_decode", n_cols),
            &binary_bytes,
            |b, data| {
                b.iter(|| {
                    black_box(ChangeEvent::decode(data).unwrap());
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 4. Round-trip: serialize → deserialize
// ---------------------------------------------------------------------------

/// Compare full encode → decode round-trip.
fn bench_round_trip(c: &mut Criterion) {
    let mut group = c.benchmark_group("round_trip");

    for n_cols in COLUMN_COUNTS {
        let names = shared_column_names(n_cols);
        let new_event = build_new_event(&names);

        // JSON round-trip (new ChangeEvent via serde)
        group.bench_with_input(
            BenchmarkId::new("json_serde", n_cols),
            &new_event,
            |b, event| {
                b.iter(|| {
                    let json = serde_json::to_vec(event).unwrap();
                    let decoded: ChangeEvent = serde_json::from_slice(&json).unwrap();
                    black_box(decoded);
                });
            },
        );

        // Binary round-trip (encode → decode)
        group.bench_with_input(
            BenchmarkId::new("binary_encode_decode", n_cols),
            &new_event,
            |b, event| {
                b.iter(|| {
                    let mut buf = bytes::BytesMut::with_capacity(256);
                    event.encode(&mut buf);
                    let decoded = ChangeEvent::decode(&buf).unwrap();
                    black_box(decoded);
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 5. Realistic CDC pipeline: construct → clone → lookup → serialize
// ---------------------------------------------------------------------------

/// End-to-end CDC simulation: construct event, clone it, look up 3 columns,
/// then serialize to the target format.
fn bench_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline");

    for n_cols in COLUMN_COUNTS {
        let names = shared_column_names(n_cols);

        // New path: RowData + ColumnValue → JSON serde
        group.bench_with_input(
            BenchmarkId::new("json_serde", n_cols),
            &names,
            |b, names| {
                b.iter(|| {
                    let event = build_new_event(names);
                    let cloned = event.clone();
                    if let EventType::Insert { ref data, .. } = cloned.event_type {
                        let _ = black_box(data.get("column_0"));
                        let _ = black_box(data.get("column_1"));
                        let _ = black_box(data.get("column_2"));
                    }
                    let out = serde_json::to_vec(&cloned).unwrap();
                    black_box(out);
                });
            },
        );

        // New path: RowData + ColumnValue → binary encode
        group.bench_with_input(
            BenchmarkId::new("binary_encode", n_cols),
            &names,
            |b, names| {
                b.iter(|| {
                    let event = build_new_event(names);
                    let cloned = event.clone();
                    if let EventType::Insert { ref data, .. } = cloned.event_type {
                        let _ = black_box(data.get("column_0"));
                        let _ = black_box(data.get("column_1"));
                        let _ = black_box(data.get("column_2"));
                    }
                    let mut buf = bytes::BytesMut::with_capacity(256);
                    cloned.encode(&mut buf);
                    black_box(buf);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_construct,
    bench_serialize,
    bench_deserialize,
    bench_round_trip,
    bench_pipeline,
);
criterion_main!(benches);
