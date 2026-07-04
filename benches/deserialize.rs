//! Benchmark: owned vs borrowed row deserialization across many row shapes.
//!
//! This is the before/after for the borrowed-deserialization optimization.
//! For every shape, the SAME row is deserialized two ways:
//!   - `owned`    — text fields are `String` (one heap alloc + memcpy per field)
//!   - `borrowed` — text fields are `&'de str` (zero alloc, zero copy)
//!
//! Shapes cover the axes that matter:
//!   - field count:  narrow (8) vs wide (24)
//!   - value size:   short (~6 B), medium (~32 B), large (~1 KiB)
//!   - mixed types:  ints + strings (only strings benefit from borrowing)
//!
//! Run: cargo bench --bench deserialize

#![allow(dead_code)]

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use pg_walstream::{ColumnValue, RowData};
use serde::Deserialize;
use std::hint::black_box;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Struct shapes — plain fields only (no flatten: flatten buffers into an owned
// serde `Content`, which would defeat borrowing). One owned + one borrowed
// struct per width, generated so field lists stay in sync.
// ---------------------------------------------------------------------------

macro_rules! owned_struct {
    ($name:ident; $($f:ident),+ $(,)?) => {
        #[derive(Deserialize)]
        struct $name {
            id: u32,
            $($f: String),+
        }
    };
}

macro_rules! borrowed_struct {
    ($name:ident; $($f:ident),+ $(,)?) => {
        #[derive(Deserialize)]
        struct $name<'a> {
            id: u32,
            $(#[serde(borrow)] $f: &'a str),+
        }
    };
}

owned_struct!(Narrow8Owned; c0, c1, c2, c3, c4, c5, c6, c7);
borrowed_struct!(Narrow8Borrowed; c0, c1, c2, c3, c4, c5, c6, c7);

owned_struct!(Wide24Owned;
    c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11,
    c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23);
borrowed_struct!(Wide24Borrowed;
    c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11,
    c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23);

// Mixed: half ints, half strings — only the string half can borrow.
#[derive(Deserialize)]
struct MixedOwned {
    a: i64,
    b: i64,
    c: i64,
    d: i64,
    s0: String,
    s1: String,
    s2: String,
    s3: String,
}

#[derive(Deserialize)]
struct MixedBorrowed<'a> {
    a: i64,
    b: i64,
    c: i64,
    d: i64,
    #[serde(borrow)]
    s0: &'a str,
    #[serde(borrow)]
    s1: &'a str,
    #[serde(borrow)]
    s2: &'a str,
    #[serde(borrow)]
    s3: &'a str,
}

// ---------------------------------------------------------------------------
// Row builders — column names are id + c0..c{n-1}
// ---------------------------------------------------------------------------

fn cols_row(n: usize, value_len: usize) -> RowData {
    let val = "x".repeat(value_len);
    let mut row = RowData::with_capacity(n + 1);
    row.push(Arc::from("id"), ColumnValue::text("1234"));
    for i in 0..n {
        let name: Arc<str> = Arc::from(format!("c{i}").as_str());
        row.push(name, ColumnValue::text(&val));
    }
    row
}

fn mixed_row() -> RowData {
    RowData::from_pairs(vec![
        ("a", ColumnValue::text("100")),
        ("b", ColumnValue::text("200")),
        ("c", ColumnValue::text("300")),
        ("d", ColumnValue::text("400")),
        ("s0", ColumnValue::text("some textual payload number zero")),
        ("s1", ColumnValue::text("another textual payload value one")),
        ("s2", ColumnValue::text("yet more textual content in two!!")),
        (
            "s3",
            ColumnValue::text("final textual field carrying three"),
        ),
    ])
}

// ---------------------------------------------------------------------------
// Benches
// ---------------------------------------------------------------------------

fn bench_narrow(c: &mut Criterion) {
    let mut group = c.benchmark_group("narrow8");
    for value_len in [6usize, 32, 1024] {
        let row = cols_row(8, value_len);

        group.bench_with_input(BenchmarkId::new("owned", value_len), &row, |b, row| {
            b.iter(|| {
                let v: Narrow8Owned = black_box(row).deserialize_into().unwrap();
                black_box(v);
            });
        });

        group.bench_with_input(BenchmarkId::new("borrowed", value_len), &row, |b, row| {
            b.iter(|| {
                let v: Narrow8Borrowed = black_box(row).deserialize_borrowed().unwrap();
                black_box(v);
            });
        });
    }
    group.finish();
}

fn bench_wide(c: &mut Criterion) {
    let mut group = c.benchmark_group("wide24");
    for value_len in [6usize, 32, 1024] {
        let row = cols_row(24, value_len);

        group.bench_with_input(BenchmarkId::new("owned", value_len), &row, |b, row| {
            b.iter(|| {
                let v: Wide24Owned = black_box(row).deserialize_into().unwrap();
                black_box(v);
            });
        });

        group.bench_with_input(BenchmarkId::new("borrowed", value_len), &row, |b, row| {
            b.iter(|| {
                let v: Wide24Borrowed = black_box(row).deserialize_borrowed().unwrap();
                black_box(v);
            });
        });
    }
    group.finish();
}

fn bench_mixed(c: &mut Criterion) {
    let row = mixed_row();
    let mut group = c.benchmark_group("mixed_int_str");

    group.bench_function("owned", |b| {
        b.iter(|| {
            let v: MixedOwned = black_box(&row).deserialize_into().unwrap();
            black_box(v);
        });
    });

    group.bench_function("borrowed", |b| {
        b.iter(|| {
            let v: MixedBorrowed = black_box(&row).deserialize_borrowed().unwrap();
            black_box(v);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_narrow, bench_wide, bench_mixed);
criterion_main!(benches);
