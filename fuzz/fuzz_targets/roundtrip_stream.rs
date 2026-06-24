#![no_main]
//! Fuzz target: streaming-framing round-trip through a stateful parser.
//!
//! Opens a StreamStart so the parser enters streaming context, then feeds an
//! arbitrary run of generated data messages, each encoded with
//! `encode_streaming_message` (which emits the in-stream xid prefix). Every
//! message must parse mid-stream back to itself and re-encode byte-for-byte.
//! This exercises the streaming-context path the single-message round-trips miss.

use arbitrary::Unstructured;
use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use pg_walstream::pgoutput_encode::encode_streaming_message;
use pg_walstream::protocol::{
    LogicalReplicationMessage as M, LogicalReplicationParser, StreamingReplicationMessage,
};
use pg_walstream_arbitrary_fuzzing::{
    generators::arbitrary_logical_replication_message, parser_producible,
};

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let Ok(version) = u.int_in_range::<u8>(2..=4) else {
        return;
    };
    let Ok(xid) = u.arbitrary::<u32>() else {
        return;
    };

    let mut parser = LogicalReplicationParser::with_protocol_version(version as u32);

    // Enter streaming context.
    let start = StreamingReplicationMessage::new_streaming(
        M::StreamStart {
            xid,
            first_segment: true,
        },
        xid,
    );
    let mut start_bytes = BytesMut::new();
    encode_streaming_message(&start, version, &mut start_bytes);
    parser
        .parse_wal_message(&start_bytes)
        .expect("stream start must parse");

    // Feed an arbitrary run of in-stream data messages.
    for _ in 0..16 {
        if u.is_empty() {
            break;
        }
        let Ok(message) = arbitrary_logical_replication_message(&mut u) else {
            break;
        };
        if !is_streamable_data(&message) || !parser_producible(&message) {
            continue;
        }

        let wrapper = StreamingReplicationMessage::new_streaming(message.clone(), xid);
        let mut bytes = BytesMut::new();
        encode_streaming_message(&wrapper, version, &mut bytes);

        let parsed = parser
            .parse_wal_message(&bytes)
            .expect("streamed data message must parse mid-stream");
        assert_eq!(parsed.message, message, "value round-trip mismatch");
        assert_eq!(parsed.xid, Some(xid), "in-stream xid mismatch");

        let mut reencoded = BytesMut::new();
        encode_streaming_message(&parsed, version, &mut reencoded);
        assert_eq!(bytes, reencoded, "encode is not a fixpoint mid-stream");
    }
});

/// The seven data messages that carry the in-stream xid prefix (protocol v2+).
fn is_streamable_data(msg: &M) -> bool {
    matches!(
        msg,
        M::Relation { .. }
            | M::Type { .. }
            | M::Insert { .. }
            | M::Update { .. }
            | M::Delete { .. }
            | M::Truncate { .. }
            | M::Message { .. }
    )
}
