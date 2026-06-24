#![no_main]
//! Fuzz target: encoder fidelity over generated messages.
//!
//! Generates a message (via the `arbitrary-fuzzing` example), encodes, parses,
//! and re-encodes, asserting `encode` is a fixpoint of `parse`. Values the parser
//! cannot produce are skipped.

use arbitrary::Unstructured;
use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use pg_walstream::pgoutput_encode::encode_message;
use pg_walstream::protocol::LogicalReplicationParser;
use pg_walstream_arbitrary_fuzzing::{
    generators::arbitrary_logical_replication_message, parser_producible,
};

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let Ok(version) = u.int_in_range::<u8>(1..=4) else {
        return;
    };
    let Ok(message) = arbitrary_logical_replication_message(&mut u) else {
        return;
    };
    if !parser_producible(&message) {
        return;
    }

    let mut first = BytesMut::new();
    encode_message(&message, version, &mut first);

    let mut parser = LogicalReplicationParser::with_protocol_version(version as u32);
    let parsed = parser
        .parse_wal_message(&first)
        .expect("encoder output must parse");

    let mut second = BytesMut::new();
    encode_message(&parsed.message, version, &mut second);

    assert_eq!(first, second, "encode is not a fixpoint of parse");
});
