//! Shipping `ChangeEvent`s between processes in a binary format.
//!
//! `ChangeEvent` implements serde's `Serialize` and `Deserialize`, so it travels through any serde
//! format. This example builds a small transaction, encodes it with five of them, checks that every
//! one reads the same events back, and shows the four things you need in practice:
//!
//! 1. how big each format is,
//! 2. how to write many events into one buffer or file and read them back one by one,
//! 3. how to wrap an event in your own message type,
//! 4. how to decode bytes you do not trust.
//!
//! No database is needed. Run it with:
//!
//! ```bash
//! cd examples/binary-serde
//! cargo run
//! ```

use std::error::Error;
use std::sync::Arc;

use bytes::Bytes;
use pg_walstream::{
    postgres_timestamp_to_chrono, ChangeEvent, ColumnValue, Lsn, ReplicaIdentity, RowData,
};
use serde::{Deserialize, Serialize};

type BoxError = Box<dyn Error>;

/// The most bytes bincode may claim while decoding one event. bincode 2 trusts every length prefix
/// unless it is given a limit, so set one whenever the bytes come from outside your process.
const MAX_EVENT_BYTES: usize = 1024 * 1024;

fn bincode_config() -> impl bincode::config::Config {
    bincode::config::standard().with_limit::<MAX_EVENT_BYTES>()
}

/// One way of turning a `ChangeEvent` into bytes and back.
struct Codec {
    name: &'static str,
    encode: fn(&ChangeEvent) -> Result<Vec<u8>, BoxError>,
    decode: fn(&[u8]) -> Result<ChangeEvent, BoxError>,
}

const CODECS: [Codec; 5] = [
    Codec {
        name: "json",
        encode: |e| Ok(serde_json::to_vec(e)?),
        decode: |b| Ok(serde_json::from_slice(b)?),
    },
    Codec {
        name: "cbor",
        encode: |e| {
            let mut out = Vec::new();
            ciborium::into_writer(e, &mut out)?;
            Ok(out)
        },
        decode: |b| Ok(ciborium::from_reader(b)?),
    },
    Codec {
        name: "messagepack",
        encode: |e| Ok(rmp_serde::to_vec(e)?),
        decode: |b| Ok(rmp_serde::from_slice(b)?),
    },
    Codec {
        name: "bincode 2",
        encode: |e| Ok(bincode::serde::encode_to_vec(e, bincode_config())?),
        decode: |b| Ok(bincode::serde::decode_from_slice(b, bincode_config())?.0),
    },
    Codec {
        name: "postcard",
        encode: |e| Ok(postcard::to_allocvec(e)?),
        decode: |b| Ok(postcard::from_bytes(b)?),
    },
];

/// `ChangeEvent` has no `PartialEq`, so compare it field by field.
fn same(a: &ChangeEvent, b: &ChangeEvent) -> bool {
    a.event_type == b.event_type && a.lsn == b.lsn && a.metadata == b.metadata && a.xid == b.xid
}

/// A transaction as a consumer sees it: BEGIN, three row changes, a logical message and COMMIT.
fn sample_transaction() -> Vec<ChangeEvent> {
    let at = postgres_timestamp_to_chrono(848_315_200_123_456);
    let lsn = Lsn::new;
    let id = || vec![Arc::from("id")];

    let alice = RowData::from_pairs(vec![
        ("id", ColumnValue::text("1")),
        ("name", ColumnValue::text("Alice")),
        ("bio", ColumnValue::Null),
        (
            "avatar",
            ColumnValue::binary_bytes(Bytes::from_static(&[0x89, b'P', b'N', b'G'])),
        ),
    ]);
    let renamed = RowData::from_pairs(vec![
        ("id", ColumnValue::text("1")),
        ("name", ColumnValue::text("Alicia")),
        ("bio", ColumnValue::text("hello 日本語")),
        (
            "avatar",
            ColumnValue::binary_bytes(Bytes::from_static(&[0x89, b'P', b'N', b'G'])),
        ),
    ]);

    // One INSERT carries an xid, as a change inside a streamed transaction does, so the example covers that field.
    let mut streamed = ChangeEvent::insert("public", "users", 16384, alice.clone(), lsn(0x1010));
    streamed.xid = Some(4242);

    vec![
        ChangeEvent::begin(742, lsn(0x1100), at, lsn(0x1000)),
        streamed,
        ChangeEvent::update(
            "public",
            "users",
            16384,
            Some(alice.clone()),
            renamed,
            ReplicaIdentity::Full,
            id(),
            lsn(0x1020),
        ),
        ChangeEvent::delete(
            "public",
            "users",
            16384,
            alice,
            ReplicaIdentity::Default,
            id(),
            lsn(0x1030),
        ),
        ChangeEvent::message(
            1,
            lsn(0x1040),
            "audit",
            Bytes::from_static(b"\x00\x01done"),
            lsn(0x1040),
        ),
        ChangeEvent::commit(at, lsn(0x1100), lsn(0x1100), lsn(0x1108)),
    ]
}

/// Your own message type around an event. An ordinary enum works with every format.
#[derive(Serialize, Deserialize)]
enum Frame {
    Change(Box<ChangeEvent>),
    Heartbeat { at_ms: u64 },
}

/// The same with an internal tag, which serde can only read back from a self-describing format
/// (JSON, CBOR, MessagePack, but not postcard or bincode).
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind")]
enum TaggedFrame {
    Change(Box<ChangeEvent>),
    Heartbeat { at_ms: u64 },
}

fn main() -> Result<(), BoxError> {
    let events = sample_transaction();

    // 1. Every format carries the same events; they differ in size.
    println!(
        "1. One transaction of {} events, in each format\n",
        events.len()
    );
    println!("   {:<12} {:>8}  {:>8}", "format", "bytes", "vs json");
    let json_total: usize = events
        .iter()
        .map(|e| (CODECS[0].encode)(e).map(|b| b.len()))
        .sum::<Result<_, _>>()?;
    for codec in &CODECS {
        let mut total = 0;
        for event in &events {
            let bytes = (codec.encode)(event)?;
            let back = (codec.decode)(&bytes)?;
            assert!(
                same(&back, event),
                "{}: {:?} did not come back as it went in",
                codec.name,
                event.event_type
            );
            total += bytes.len();
        }
        println!(
            "   {:<12} {:>8}  {:>7.0}%",
            codec.name,
            total,
            total as f64 / json_total as f64 * 100.0
        );
    }

    // 2. Many events in one buffer: postcard values end where they end, so a reader takes them one at a time.
    let mut buffer = Vec::new();
    for event in &events {
        buffer.extend(postcard::to_allocvec(event)?);
    }
    let mut rest = &buffer[..];
    let mut read = 0;
    while !rest.is_empty() {
        let (event, tail) = postcard::take_from_bytes::<ChangeEvent>(rest)?;
        assert!(
            same(&event, &events[read]),
            "event {read} came back different"
        );
        read += 1;
        rest = tail;
    }
    assert_eq!(read, events.len());
    println!(
        "\n2. {} events written back to back into {} bytes, read back one by one",
        read,
        buffer.len()
    );

    // 3. Your own envelope around an event.
    let frame = Frame::Change(Box::new(events[1].clone()));
    let bytes = postcard::to_allocvec(&frame)?;
    match postcard::from_bytes::<Frame>(&bytes)? {
        Frame::Change(back) => assert!(same(&back, &events[1])),
        Frame::Heartbeat { .. } => unreachable!("a change came back as a heartbeat"),
    }
    let mut cbor = Vec::new();
    ciborium::into_writer(&TaggedFrame::Change(Box::new(events[1].clone())), &mut cbor)?;
    match ciborium::from_reader::<TaggedFrame, _>(&cbor[..])? {
        TaggedFrame::Change(back) => assert!(same(&back, &events[1])),
        TaggedFrame::Heartbeat { .. } => unreachable!("a change came back as a heartbeat"),
    }
    println!(
        "3. an event inside your own enum: postcard {} bytes, internally tagged CBOR {} bytes",
        bytes.len(),
        cbor.len()
    );

    // 4. Bytes you do not trust must fail, not crash.
    let whole = postcard::to_allocvec(&events[1])?;
    assert!(postcard::from_bytes::<ChangeEvent>(&whole[..whole.len() / 2]).is_err());
    // An INSERT whose row claims about a billion columns and then ends.
    let hostile_postcard = [0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0x03];
    let postcard_error = postcard::from_bytes::<ChangeEvent>(&hostile_postcard).unwrap_err();
    // A schema name claiming u64::MAX bytes: without the limit in `bincode_config` bincode would try to allocate that much.
    let hostile_bincode = [0x00, 0xfd, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];
    let bincode_error =
        bincode::serde::decode_from_slice::<ChangeEvent, _>(&hostile_bincode, bincode_config())
            .unwrap_err();
    println!("4. truncated postcard: error; hostile postcard: \"{postcard_error}\"; hostile bincode: \"{bincode_error}\"");

    println!("\nAll checks passed.");
    Ok(())
}
