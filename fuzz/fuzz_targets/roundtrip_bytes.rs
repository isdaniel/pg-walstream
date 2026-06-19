#![no_main]
//! Fuzz target: parser robustness + round-trip on arbitrary bytes.
//!
//! On accepted input, asserts value round-trip and encode idempotence. Does not
//! assert byte-equality with the input (the parser normalizes non-canonical
//! bytes PostgreSQL never sends).

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use pg_walstream::pgoutput_encode::encode_message;
use pg_walstream::protocol::LogicalReplicationParser;

fuzz_target!(|data: &[u8]| {
    if data.len() < 2 {
        return;
    }
    // First byte picks the version (1..=4), the rest is the body.
    let version = (data[0] % 4) + 1;
    let body = &data[1..];

    let mut parser = LogicalReplicationParser::with_protocol_version(version as u32);
    let Ok(parsed) = parser.parse_wal_message(body) else {
        return;
    };
    let message = parsed.message;

    // Canonical encoding of the parsed message.
    let mut encoded = BytesMut::new();
    encode_message(&message, version, &mut encoded);

    // 1. Value round-trip: the canonical bytes parse back to an equal message.
    let mut verifier = LogicalReplicationParser::with_protocol_version(version as u32);
    let reparsed = verifier
        .parse_wal_message(&encoded)
        .expect("re-encoded bytes must parse")
        .message;
    assert_eq!(
        message, reparsed,
        "value round-trip mismatch (version={version})"
    );

    // 2. Encode idempotence: re-encoding the re-parsed message is byte-stable.
    let mut reencoded = BytesMut::new();
    encode_message(&reparsed, version, &mut reencoded);
    assert_eq!(
        encoded, reencoded,
        "encode is not idempotent on parser output (version={version})"
    );
});
