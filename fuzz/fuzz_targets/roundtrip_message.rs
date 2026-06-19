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
use pg_walstream::protocol::{LogicalReplicationMessage, LogicalReplicationParser};
use pg_walstream_arbitrary_fuzzing::generators::arbitrary_logical_replication_message;

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

/// Whether `msg` is parser-producible (the encoder's contract). Others are skipped.
fn parser_producible(msg: &LogicalReplicationMessage) -> bool {
    use LogicalReplicationMessage as M;

    fn no_interior_nul(s: &str) -> bool {
        !s.as_bytes().contains(&0)
    }

    match msg {
        M::Relation {
            namespace,
            relation_name,
            columns,
            ..
        } => {
            no_interior_nul(namespace)
                && no_interior_nul(relation_name)
                && columns.iter().all(|c| no_interior_nul(&c.name))
        }
        M::Type {
            namespace,
            type_name,
            ..
        } => no_interior_nul(namespace) && no_interior_nul(type_name),
        M::Origin { origin_name, .. } => no_interior_nul(origin_name),
        M::Message { prefix, .. } => no_interior_nul(prefix),
        // The parser sets `key_type` only after peeking 'K'/'O', and always
        // together with `old_tuple`.
        M::Update {
            old_tuple,
            key_type,
            ..
        } => match (old_tuple, key_type) {
            (Some(_), Some(k)) => *k == 'K' || *k == 'O',
            (None, None) => true,
            _ => false,
        },
        // The parser reads the DELETE key as one byte, so it must fit in one.
        M::Delete { key_type, .. } => (*key_type as u32) <= 0xFF,
        M::BeginPrepare { gid, .. }
        | M::Prepare { gid, .. }
        | M::CommitPrepared { gid, .. }
        | M::RollbackPrepared { gid, .. }
        | M::StreamPrepare { gid, .. } => no_interior_nul(gid),
        _ => true,
    }
}
