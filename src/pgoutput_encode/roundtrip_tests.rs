//! Cross-cutting property tests: round-trip across all variants and versions,
//! parser robustness on arbitrary bytes, and byte fidelity on spec bytes.

use super::{
    encode_message, encode_message_to_bytes, encode_streaming_message,
    encode_streaming_message_to_bytes,
};
use crate::protocol::{
    message_types, ColumnData, ColumnInfo, LogicalReplicationMessage as M,
    LogicalReplicationParser, StreamingReplicationMessage, TupleData,
};
use bytes::{Bytes, BytesMut};
use proptest::prelude::*;
use proptest::strategy::{BoxedStrategy, Union};
use std::sync::Arc;

fn encode(msg: &M, version: u8) -> Vec<u8> {
    let mut buf = BytesMut::new();
    encode_message(msg, version, &mut buf);
    buf.to_vec()
}

fn arb_cstring() -> impl Strategy<Value = String> {
    any::<String>().prop_map(|s| s.replace('\u{0}', ""))
}

fn arb_column_data(allow_unchanged: bool) -> BoxedStrategy<ColumnData> {
    let payload = || prop::collection::vec(any::<u8>(), 0..24);
    if allow_unchanged {
        prop_oneof![
            Just(ColumnData::null()),
            Just(ColumnData::unchanged()),
            payload().prop_map(ColumnData::text),
            payload().prop_map(ColumnData::binary),
        ]
        .boxed()
    } else {
        prop_oneof![
            Just(ColumnData::null()),
            payload().prop_map(ColumnData::text),
            payload().prop_map(ColumnData::binary),
        ]
        .boxed()
    }
}

fn arb_tuple(allow_unchanged: bool) -> impl Strategy<Value = TupleData> {
    prop::collection::vec(arb_column_data(allow_unchanged), 0..8).prop_map(TupleData::new)
}

fn arb_column_info() -> impl Strategy<Value = ColumnInfo> {
    (any::<u8>(), arb_cstring(), any::<u32>(), any::<i32>()).prop_map(
        |(flags, name, type_id, modifier)| ColumnInfo::new(flags, name, type_id, modifier),
    )
}

fn arb_begin() -> BoxedStrategy<M> {
    (any::<u64>(), any::<i64>(), any::<u32>())
        .prop_map(|(final_lsn, timestamp, xid)| M::Begin {
            final_lsn,
            timestamp,
            xid,
        })
        .boxed()
}

fn arb_commit() -> BoxedStrategy<M> {
    (any::<u8>(), any::<u64>(), any::<u64>(), any::<i64>())
        .prop_map(|(flags, commit_lsn, end_lsn, timestamp)| M::Commit {
            flags,
            commit_lsn,
            end_lsn,
            timestamp,
        })
        .boxed()
}

fn arb_origin() -> BoxedStrategy<M> {
    (any::<u64>(), arb_cstring())
        .prop_map(|(origin_lsn, origin_name)| M::Origin {
            origin_lsn,
            origin_name,
        })
        .boxed()
}

fn arb_relation() -> BoxedStrategy<M> {
    (
        any::<u32>(),
        arb_cstring(),
        arb_cstring(),
        any::<u8>(),
        prop::collection::vec(arb_column_info(), 0..6),
    )
        .prop_map(
            |(relation_id, namespace, relation_name, replica_identity, columns)| M::Relation {
                relation_id,
                namespace: Arc::from(namespace),
                relation_name: Arc::from(relation_name),
                replica_identity,
                columns,
            },
        )
        .boxed()
}

fn arb_type() -> BoxedStrategy<M> {
    (any::<u32>(), arb_cstring(), arb_cstring())
        .prop_map(|(type_id, namespace, type_name)| M::Type {
            type_id,
            namespace,
            type_name,
        })
        .boxed()
}

fn arb_insert() -> BoxedStrategy<M> {
    (any::<u32>(), arb_tuple(false))
        .prop_map(|(relation_id, tuple)| M::Insert { relation_id, tuple })
        .boxed()
}

fn arb_update() -> BoxedStrategy<M> {
    let with_old = (
        any::<u32>(),
        prop_oneof![Just('K'), Just('O')],
        arb_tuple(true),
        arb_tuple(false),
    )
        .prop_map(|(relation_id, key, old, new)| M::Update {
            relation_id,
            old_tuple: Some(old),
            new_tuple: new,
            key_type: Some(key),
        });
    let without_old = (any::<u32>(), arb_tuple(false)).prop_map(|(relation_id, new)| M::Update {
        relation_id,
        old_tuple: None,
        new_tuple: new,
        key_type: None,
    });
    prop_oneof![with_old, without_old].boxed()
}

fn arb_delete() -> BoxedStrategy<M> {
    (
        any::<u32>(),
        prop_oneof![Just('K'), Just('O')],
        arb_tuple(true),
    )
        .prop_map(|(relation_id, key_type, old_tuple)| M::Delete {
            relation_id,
            old_tuple,
            key_type,
        })
        .boxed()
}

fn arb_truncate() -> BoxedStrategy<M> {
    (prop::collection::vec(any::<u32>(), 0..8), any::<u8>())
        .prop_map(|(relation_ids, flags)| M::Truncate {
            relation_ids,
            flags,
        })
        .boxed()
}

fn arb_message() -> BoxedStrategy<M> {
    (
        any::<u8>(),
        any::<u64>(),
        arb_cstring(),
        prop::collection::vec(any::<u8>(), 0..32),
    )
        .prop_map(|(flags, lsn, prefix, content)| M::Message {
            flags,
            lsn,
            prefix,
            content: Bytes::from(content),
        })
        .boxed()
}

fn arb_stream_start() -> BoxedStrategy<M> {
    (any::<u32>(), any::<bool>())
        .prop_map(|(xid, first_segment)| M::StreamStart { xid, first_segment })
        .boxed()
}

fn arb_stream_commit() -> BoxedStrategy<M> {
    (
        any::<u32>(),
        any::<u8>(),
        any::<u64>(),
        any::<u64>(),
        any::<i64>(),
    )
        .prop_map(
            |(xid, flags, commit_lsn, end_lsn, timestamp)| M::StreamCommit {
                xid,
                flags,
                commit_lsn,
                end_lsn,
                timestamp,
            },
        )
        .boxed()
}

fn arb_stream_abort(with_tail: bool) -> BoxedStrategy<M> {
    if with_tail {
        (any::<u32>(), any::<u32>(), any::<u64>(), any::<i64>())
            .prop_map(|(xid, sub, lsn, ts)| M::StreamAbort {
                xid,
                subtransaction_xid: sub,
                abort_lsn: Some(lsn),
                abort_timestamp: Some(ts),
            })
            .boxed()
    } else {
        (any::<u32>(), any::<u32>())
            .prop_map(|(xid, sub)| M::StreamAbort {
                xid,
                subtransaction_xid: sub,
                abort_lsn: None,
                abort_timestamp: None,
            })
            .boxed()
    }
}

fn arb_begin_prepare() -> BoxedStrategy<M> {
    (
        any::<u64>(),
        any::<u64>(),
        any::<i64>(),
        any::<u32>(),
        arb_cstring(),
    )
        .prop_map(
            |(prepare_lsn, end_lsn, timestamp, xid, gid)| M::BeginPrepare {
                prepare_lsn,
                end_lsn,
                timestamp,
                xid,
                gid,
            },
        )
        .boxed()
}

fn arb_prepare() -> BoxedStrategy<M> {
    (
        any::<u8>(),
        any::<u64>(),
        any::<u64>(),
        any::<i64>(),
        any::<u32>(),
        arb_cstring(),
    )
        .prop_map(
            |(flags, prepare_lsn, end_lsn, timestamp, xid, gid)| M::Prepare {
                flags,
                prepare_lsn,
                end_lsn,
                timestamp,
                xid,
                gid,
            },
        )
        .boxed()
}

fn arb_commit_prepared() -> BoxedStrategy<M> {
    (
        any::<u8>(),
        any::<u64>(),
        any::<u64>(),
        any::<i64>(),
        any::<u32>(),
        arb_cstring(),
    )
        .prop_map(
            |(flags, commit_lsn, end_lsn, timestamp, xid, gid)| M::CommitPrepared {
                flags,
                commit_lsn,
                end_lsn,
                timestamp,
                xid,
                gid,
            },
        )
        .boxed()
}

fn arb_rollback_prepared() -> BoxedStrategy<M> {
    (
        any::<u8>(),
        any::<u64>(),
        any::<u64>(),
        any::<i64>(),
        any::<i64>(),
        any::<u32>(),
        arb_cstring(),
    )
        .prop_map(
            |(
                flags,
                prepare_end_lsn,
                rollback_end_lsn,
                prepare_timestamp,
                rollback_timestamp,
                xid,
                gid,
            )| {
                M::RollbackPrepared {
                    flags,
                    prepare_end_lsn,
                    rollback_end_lsn,
                    prepare_timestamp,
                    rollback_timestamp,
                    xid,
                    gid,
                }
            },
        )
        .boxed()
}

fn arb_stream_prepare() -> BoxedStrategy<M> {
    (
        any::<u8>(),
        any::<u64>(),
        any::<u64>(),
        any::<i64>(),
        any::<u32>(),
        arb_cstring(),
    )
        .prop_map(
            |(flags, prepare_lsn, end_lsn, timestamp, xid, gid)| M::StreamPrepare {
                flags,
                prepare_lsn,
                end_lsn,
                timestamp,
                xid,
                gid,
            },
        )
        .boxed()
}

fn arb_message_for_version(v: u8) -> BoxedStrategy<M> {
    let mut options: Vec<BoxedStrategy<M>> = vec![
        arb_begin(),
        arb_commit(),
        arb_origin(),
        arb_relation(),
        arb_type(),
        arb_insert(),
        arb_update(),
        arb_delete(),
        arb_truncate(),
        arb_message(),
    ];
    if v >= 2 {
        options.push(arb_stream_start());
        options.push(Just(M::StreamStop).boxed());
        options.push(arb_stream_commit());
        options.push(arb_stream_abort(false));
    }
    if v >= 3 {
        options.push(arb_begin_prepare());
        options.push(arb_prepare());
        options.push(arb_commit_prepared());
        options.push(arb_rollback_prepared());
        options.push(arb_stream_prepare());
    }
    if v >= 4 {
        options.push(arb_stream_abort(true));
    }
    Union::new(options).boxed()
}

fn arb_message_and_version() -> impl Strategy<Value = (u8, M)> {
    (1u8..=4).prop_flat_map(|v| arb_message_for_version(v).prop_map(move |m| (v, m)))
}

proptest! {
    #[test]
    fn round_trip_pgoutput((version, msg) in arb_message_and_version()) {
        let mut buf = BytesMut::new();
        encode_message(&msg, version, &mut buf);
        let mut parser = LogicalReplicationParser::with_protocol_version(version as u32);
        let parsed = parser
            .parse_wal_message(&buf)
            .expect("parser must accept encoder output");
        // Byte-side loop `encode(parse(b)) == b` (before the move below).
        let mut reencoded = BytesMut::new();
        encode_message(&parsed.message, version, &mut reencoded);
        prop_assert_eq!(reencoded, buf);
        // Message-side loop: `parse(encode(msg)) == msg`.
        prop_assert_eq!(parsed.message, msg);
    }
}

/// Parser input: random bytes, or a valid encoding tagged with its version.
fn arb_version_and_wire_bytes() -> impl Strategy<Value = (u8, Vec<u8>)> {
    let random = (1u8..=4, prop::collection::vec(any::<u8>(), 0..128));
    let structured = arb_message_and_version().prop_map(|(version, msg)| {
        let mut body = BytesMut::new();
        encode_message(&msg, version, &mut body);
        (version, body.to_vec())
    });
    prop_oneof![random, structured]
}

proptest! {
    /// CI analogue of `roundtrip_bytes`: on parse success, assert value
    /// round-trip and encode idempotence.
    #[test]
    fn parser_roundtrip_on_arbitrary_bytes((version, body) in arb_version_and_wire_bytes()) {
        let mut parser = LogicalReplicationParser::with_protocol_version(version as u32);
        if let Ok(parsed) = parser.parse_wal_message(&body) {
            let message = parsed.message;
            let mut encoded = BytesMut::new();
            encode_message(&message, version, &mut encoded);
            let mut verifier = LogicalReplicationParser::with_protocol_version(version as u32);
            let reparsed = verifier
                .parse_wal_message(&encoded)
                .expect("re-encoded bytes must parse")
                .message;
            prop_assert_eq!(&message, &reparsed);
            let mut reencoded = BytesMut::new();
            encode_message(&reparsed, version, &mut reencoded);
            prop_assert_eq!(encoded, reencoded);
        }
    }
}

/// `encode(parse(bytes)) == bytes` on hand-written spec bytes, a third source
/// pinning both parser and encoder.
#[test]
fn reencode_spec_bytes_is_identity() {
    // (protocol_version, full message body including the leading tag byte).
    let cases: Vec<(u8, Vec<u8>)> = vec![
        // Relation: id 100, "public"."t", identity 'd', one key column.
        (1, {
            let mut b = vec![b'R'];
            b.extend_from_slice(&100u32.to_be_bytes());
            b.extend_from_slice(b"public\0");
            b.extend_from_slice(b"t\0");
            b.push(b'd');
            b.extend_from_slice(&1u16.to_be_bytes());
            b.push(1);
            b.extend_from_slice(b"id\0");
            b.extend_from_slice(&23u32.to_be_bytes());
            b.extend_from_slice(&(-1i32).to_be_bytes());
            b
        }),
        // Insert: relation 100, tuple [text "hi", null, binary 00 ff].
        (1, {
            let mut b = vec![b'I'];
            b.extend_from_slice(&100u32.to_be_bytes());
            b.push(b'N');
            b.extend_from_slice(&3u16.to_be_bytes());
            b.push(b't');
            b.extend_from_slice(&2u32.to_be_bytes());
            b.extend_from_slice(b"hi");
            b.push(b'n');
            b.push(b'b');
            b.extend_from_slice(&2u32.to_be_bytes());
            b.extend_from_slice(&[0x00, 0xff]);
            b
        }),
        // Update: 'K' old image (with unchanged-TOAST), then 'N' new tuple.
        (1, {
            let mut b = vec![b'U'];
            b.extend_from_slice(&100u32.to_be_bytes());
            b.push(b'K');
            b.extend_from_slice(&2u16.to_be_bytes());
            b.push(b't');
            b.extend_from_slice(&1u32.to_be_bytes());
            b.extend_from_slice(b"1");
            b.push(b'u');
            b.push(b'N');
            b.extend_from_slice(&2u16.to_be_bytes());
            b.push(b't');
            b.extend_from_slice(&1u32.to_be_bytes());
            b.extend_from_slice(b"2");
            b.push(b'n');
            b
        }),
        // Delete with full identity ('O').
        (1, {
            let mut b = vec![b'D'];
            b.extend_from_slice(&100u32.to_be_bytes());
            b.push(b'O');
            b.extend_from_slice(&1u16.to_be_bytes());
            b.push(b't');
            b.extend_from_slice(&1u32.to_be_bytes());
            b.extend_from_slice(b"1");
            b
        }),
        // Message: content carries embedded nulls (length-prefixed, NOT a
        // CString) and the prefix is a CString.
        (1, {
            let mut b = vec![b'M', 0u8];
            b.extend_from_slice(&0u64.to_be_bytes());
            b.extend_from_slice(b"px\0");
            b.extend_from_slice(&3u32.to_be_bytes());
            b.extend_from_slice(&[0x00, 0x01, 0x00]);
            b
        }),
        // StreamAbort with the protocol-v4 parallel-apply tail.
        (4, {
            let mut b = vec![b'A'];
            b.extend_from_slice(&7u32.to_be_bytes());
            b.extend_from_slice(&8u32.to_be_bytes());
            b.extend_from_slice(&0x0100_0000u64.to_be_bytes());
            b.extend_from_slice(&123i64.to_be_bytes());
            b
        }),
    ];

    for (version, bytes) in cases {
        let mut parser = LogicalReplicationParser::with_protocol_version(version as u32);
        let parsed = parser
            .parse_wal_message(&bytes)
            .expect("spec bytes must parse");
        let mut reencoded = BytesMut::new();
        encode_message(&parsed.message, version, &mut reencoded);
        assert_eq!(
            &reencoded[..],
            bytes.as_slice(),
            "re-encode mismatch for tag '{}'",
            bytes[0] as char
        );
    }
}

/// `encode_message_to_bytes` (which sizes via `capacity_hint`) must match
/// `encode_message` for every variant, exercising each `capacity_hint` arm.
#[test]
fn encode_message_to_bytes_matches_encode_message() {
    let tuple = || TupleData::new(vec![ColumnData::text(b"x".to_vec())]);
    let cases: Vec<(u8, M)> = vec![
        (
            1,
            M::Begin {
                final_lsn: 1,
                timestamp: 2,
                xid: 3,
            },
        ),
        (
            1,
            M::Commit {
                flags: 0,
                commit_lsn: 1,
                end_lsn: 2,
                timestamp: 3,
            },
        ),
        (
            1,
            M::Origin {
                origin_lsn: 1,
                origin_name: "o".into(),
            },
        ),
        (
            1,
            M::Relation {
                relation_id: 1,
                namespace: Arc::from("s"),
                relation_name: Arc::from("t"),
                replica_identity: b'd',
                columns: vec![ColumnInfo::new(1, "c".to_string(), 23, -1)],
            },
        ),
        (
            1,
            M::Type {
                type_id: 1,
                namespace: "s".into(),
                type_name: "ty".into(),
            },
        ),
        (
            1,
            M::Insert {
                relation_id: 1,
                tuple: tuple(),
            },
        ),
        (
            1,
            M::Update {
                relation_id: 1,
                old_tuple: Some(tuple()),
                new_tuple: tuple(),
                key_type: Some('K'),
            },
        ),
        (
            1,
            M::Delete {
                relation_id: 1,
                old_tuple: tuple(),
                key_type: 'K',
            },
        ),
        (
            1,
            M::Truncate {
                relation_ids: vec![1, 2],
                flags: 0,
            },
        ),
        (
            1,
            M::Message {
                flags: 0,
                lsn: 1,
                prefix: "p".into(),
                content: Bytes::from_static(b"ab"),
            },
        ),
        (
            2,
            M::StreamStart {
                xid: 1,
                first_segment: true,
            },
        ),
        (2, M::StreamStop),
        (
            2,
            M::StreamCommit {
                xid: 1,
                flags: 0,
                commit_lsn: 1,
                end_lsn: 2,
                timestamp: 3,
            },
        ),
        (
            4,
            M::StreamAbort {
                xid: 1,
                subtransaction_xid: 2,
                abort_lsn: Some(3),
                abort_timestamp: Some(4),
            },
        ),
        (
            3,
            M::BeginPrepare {
                prepare_lsn: 1,
                end_lsn: 2,
                timestamp: 3,
                xid: 4,
                gid: "g".into(),
            },
        ),
        (
            3,
            M::Prepare {
                flags: 0,
                prepare_lsn: 1,
                end_lsn: 2,
                timestamp: 3,
                xid: 4,
                gid: "g".into(),
            },
        ),
        (
            3,
            M::CommitPrepared {
                flags: 0,
                commit_lsn: 1,
                end_lsn: 2,
                timestamp: 3,
                xid: 4,
                gid: "g".into(),
            },
        ),
        (
            3,
            M::RollbackPrepared {
                flags: 0,
                prepare_end_lsn: 1,
                rollback_end_lsn: 2,
                prepare_timestamp: 3,
                rollback_timestamp: 4,
                xid: 5,
                gid: "g".into(),
            },
        ),
        (
            3,
            M::StreamPrepare {
                flags: 0,
                prepare_lsn: 1,
                end_lsn: 2,
                timestamp: 3,
                xid: 4,
                gid: "g".into(),
            },
        ),
    ];
    for (version, msg) in &cases {
        assert_eq!(
            &encode_message_to_bytes(msg, *version)[..],
            encode(msg, *version).as_slice(),
            "to_bytes mismatch for {msg:?}"
        );
    }
}

/// A parser already in streaming context for `xid` (a StreamStart was fed
/// first), so a subsequent data message is parsed with its in-stream xid prefix.
fn parse_in_stream(xid: u32, proto: u8, bytes: &[u8]) -> StreamingReplicationMessage {
    let mut parser = LogicalReplicationParser::with_protocol_version(proto as u32);
    let mut ss = vec![message_types::STREAM_START];
    ss.extend_from_slice(&xid.to_be_bytes());
    ss.push(1);
    parser.parse_wal_message(&ss).expect("stream start");
    parser
        .parse_wal_message(bytes)
        .expect("data message parsed in stream")
}

fn arb_data_message() -> BoxedStrategy<M> {
    prop_oneof![
        arb_relation(),
        arb_type(),
        arb_insert(),
        arb_update(),
        arb_delete(),
        arb_truncate(),
        arb_message(),
    ]
    .boxed()
}

proptest! {
    /// A streamed data message must encode as its non-streaming bytes with the
    /// Int32 xid inserted right after the tag, and must round-trip through a
    /// parser that is mid-stream. This is the inverse of the parser's in-stream
    /// xid read (protocol v2+).
    #[test]
    fn streaming_data_message_round_trip(
        msg in arb_data_message(),
        xid in any::<u32>(),
        proto in 2u8..=4,
    ) {
        let wrapper = StreamingReplicationMessage::new_streaming(msg.clone(), xid);
        let mut buf = BytesMut::new();
        encode_streaming_message(&wrapper, proto, &mut buf);
        let streamed = buf.to_vec();

        let plain = encode(&msg, proto);
        let mut expected = Vec::with_capacity(plain.len() + 4);
        expected.push(plain[0]);
        expected.extend_from_slice(&xid.to_be_bytes());
        expected.extend_from_slice(&plain[1..]);
        prop_assert_eq!(&streamed, &expected, "in-stream xid prefix missing or misplaced");

        let parsed = parse_in_stream(xid, proto, &streamed);
        prop_assert_eq!(&parsed.message, &msg);
        prop_assert!(parsed.is_streaming);
        prop_assert_eq!(parsed.xid, Some(xid));
    }
}

proptest! {
    /// `encode_streaming_message_to_bytes` matches `encode_streaming_message`.
    #[test]
    fn streaming_to_bytes_matches_encode_streaming(
        msg in arb_data_message(),
        xid in any::<u32>(),
        proto in 2u8..=4,
    ) {
        let wrapper = StreamingReplicationMessage::new_streaming(msg, xid);
        let mut buf = BytesMut::new();
        encode_streaming_message(&wrapper, proto, &mut buf);
        prop_assert_eq!(buf, encode_streaming_message_to_bytes(&wrapper, proto));
    }
}

proptest! {
    /// A non-streaming wrapper (the parser's output outside a stream) encodes
    /// identically to `encode_message`: no in-stream xid prefix is added.
    #[test]
    fn non_streaming_wrapper_matches_encode_message((version, msg) in arb_message_and_version()) {
        let wrapper = StreamingReplicationMessage::new(msg.clone());
        prop_assert_eq!(
            encode_streaming_message_to_bytes(&wrapper, version),
            encode_message_to_bytes(&msg, version)
        );
    }
}
