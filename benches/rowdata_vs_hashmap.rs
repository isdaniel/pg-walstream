//! Benchmark: RowData (Arc<str> + Vec) vs HashMap<String, Value>
//!
//! Compares the old HashMap-based approach with the new RowData/Arc<str> approach
//! across realistic CDC workloads:
//!
//!   - Event construction (simulates convert_to_change_event hot path)
//!   - Column lookup by name
//!   - Cloning events (simulates buffering / sending across channels)
//!   - Serialization to JSON
//!
//! Run:
//!   cargo bench --bench rowdata_vs_hashmap

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use pg_walstream::types::{ChangeEvent, EventType, Lsn, RowData};
use serde_json::{self, Value};
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helpers — simulate the "old" HashMap<String, Value> code path
// ---------------------------------------------------------------------------

/// Old-style event with HashMap<String, Value> and String schema/table.
#[derive(Debug, Clone, serde::Serialize)]
struct OldChangeEvent {
    schema: String,
    table: String,
    relation_oid: u32,
    data: HashMap<String, Value>,
    lsn: u64,
}

/// Build an old-style event the way the old code used to.
fn build_old_event(n_columns: usize) -> OldChangeEvent {
    let mut data = HashMap::with_capacity(n_columns);
    for i in 0..n_columns {
        data.insert(format!("column_{i}"), serde_json::json!(i));
    }
    OldChangeEvent {
        schema: "public".to_string(),
        table: "users".to_string(),
        relation_oid: 16384,
        data,
        lsn: 0x16B374D848,
    }
}

/// Build a new-style ChangeEvent using Arc<str> + RowData.
/// `shared_names` simulates the pre-cached Arc<str> column names
/// held in RelationInfo — this is how production code works.
fn build_new_event(shared_names: &[Arc<str>]) -> ChangeEvent {
    let n = shared_names.len();
    let mut row = RowData::with_capacity(n);
    for (i, name) in shared_names.iter().enumerate() {
        row.push(Arc::clone(name), serde_json::json!(i));
    }
    ChangeEvent::insert("public", "users", 16384, row, Lsn::new(0x16B374D848))
}

/// Pre-create shared column names (what RelationInfo holds in production).
fn shared_column_names(n: usize) -> Vec<Arc<str>> {
    (0..n)
        .map(|i| Arc::from(format!("column_{i}").as_str()))
        .collect()
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

fn bench_event_construction(c: &mut Criterion) {
    let mut group = c.benchmark_group("event_construction");

    for n_cols in [5, 10, 20, 50] {
        let names = shared_column_names(n_cols);

        group.bench_with_input(
            BenchmarkId::new("hashmap_string", n_cols),
            &n_cols,
            |b, &n| {
                b.iter(|| black_box(build_old_event(n)));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("rowdata_arc", n_cols),
            &names,
            |b, names| {
                b.iter(|| black_box(build_new_event(names)));
            },
        );
    }

    group.finish();
}

fn bench_column_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("column_lookup");

    for n_cols in [5, 10, 20, 50] {
        // Build the data structures once
        let old = build_old_event(n_cols);
        let names = shared_column_names(n_cols);
        let new_event = build_new_event(&names);
        let row = match &new_event.event_type {
            EventType::Insert { data, .. } => data,
            _ => unreachable!(),
        };

        // Lookup the LAST column (worst case for linear scan)
        let target = format!("column_{}", n_cols - 1);

        group.bench_with_input(
            BenchmarkId::new("hashmap_get", n_cols),
            &target,
            |b, key| {
                b.iter(|| black_box(old.data.get(key)));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("rowdata_get", n_cols),
            &target,
            |b, key| {
                b.iter(|| black_box(row.get(key)));
            },
        );
    }

    group.finish();
}

fn bench_clone(c: &mut Criterion) {
    let mut group = c.benchmark_group("event_clone");

    for n_cols in [5, 10, 20, 50] {
        let old = build_old_event(n_cols);
        let names = shared_column_names(n_cols);
        let new_event = build_new_event(&names);

        group.bench_with_input(
            BenchmarkId::new("hashmap_string", n_cols),
            &old,
            |b, event| {
                b.iter(|| black_box(event.clone()));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("rowdata_arc", n_cols),
            &new_event,
            |b, event| {
                b.iter(|| black_box(event.clone()));
            },
        );
    }

    group.finish();
}

fn bench_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_serialize");

    for n_cols in [5, 10, 20, 50] {
        let old = build_old_event(n_cols);
        let names = shared_column_names(n_cols);
        let new_event = build_new_event(&names);

        group.bench_with_input(
            BenchmarkId::new("hashmap_string", n_cols),
            &old,
            |b, event| {
                b.iter(|| black_box(serde_json::to_string(event).unwrap()));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("rowdata_arc", n_cols),
            &new_event,
            |b, event| {
                b.iter(|| black_box(serde_json::to_string(event).unwrap()));
            },
        );
    }

    group.finish();
}

/// Simulate a realistic CDC pipeline: construct event → clone → lookup 3 columns → serialize.
fn bench_full_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_pipeline");

    for n_cols in [5, 10, 20, 50] {
        let names = shared_column_names(n_cols);

        group.bench_with_input(
            BenchmarkId::new("hashmap_string", n_cols),
            &n_cols,
            |b, &n| {
                b.iter(|| {
                    let event = build_old_event(n);
                    let cloned = event.clone();
                    // Simulate looking up a few columns
                    let _ = black_box(cloned.data.get("column_0"));
                    let _ = black_box(cloned.data.get("column_1"));
                    let _ = black_box(cloned.data.get("column_2"));
                    let json = serde_json::to_string(&cloned).unwrap();
                    black_box(json);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("rowdata_arc", n_cols),
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
                    let json = serde_json::to_string(&cloned).unwrap();
                    black_box(json);
                });
            },
        );
    }

    group.finish();
}

/// Simulate high-throughput: construct N events from the same relation
/// (e.g., a burst of INSERTs in one transaction).
fn bench_batch_construction(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_100_events");

    for n_cols in [5, 10, 20] {
        let names = shared_column_names(n_cols);

        group.bench_with_input(
            BenchmarkId::new("hashmap_string", n_cols),
            &n_cols,
            |b, &n| {
                b.iter(|| {
                    let events: Vec<_> = (0..100).map(|_| build_old_event(n)).collect();
                    black_box(events);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("rowdata_arc", n_cols),
            &names,
            |b, names| {
                b.iter(|| {
                    let events: Vec<_> = (0..100).map(|_| build_new_event(names)).collect();
                    black_box(events);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_event_construction,
    bench_column_lookup,
    bench_clone,
    bench_serialize,
    bench_full_pipeline,
    bench_batch_construction,
);
criterion_main!(benches);
