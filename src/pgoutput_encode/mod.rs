//! Byte-faithful `pgoutput` encoder: the inverse of [`LogicalReplicationParser`]
//! (`parse(encode(msg)) == msg`), distinct from [`ChangeEvent::encode`] (which
//! writes the crate's own framing, not `pgoutput`).
//!
//! Timestamps are the raw on-wire `i64`, stored verbatim by the parser. Data
//! messages use non-streaming framing (no in-stream xid prefix).
//!
//! [`LogicalReplicationParser`]: crate::protocol::LogicalReplicationParser
//! [`ChangeEvent::encode`]: crate::types::ChangeEvent::encode

mod streaming;
mod two_phase;
mod v1;
mod wire;

#[cfg(test)]
mod roundtrip_tests;

use crate::protocol::{LogicalReplicationMessage, TupleData};
use bytes::BytesMut;

/// Append `msg` as `pgoutput` wire bytes to `buf`. Infallible for any
/// parser-produced value.
pub fn encode_message(msg: &LogicalReplicationMessage, protocol_version: u8, buf: &mut BytesMut) {
    use LogicalReplicationMessage as M;

    match msg {
        M::Begin {
            final_lsn,
            timestamp,
            xid,
        } => v1::encode_begin(buf, *final_lsn, *timestamp, *xid),
        M::Commit {
            flags,
            commit_lsn,
            end_lsn,
            timestamp,
        } => v1::encode_commit(buf, *flags, *commit_lsn, *end_lsn, *timestamp),
        M::Relation {
            relation_id,
            namespace,
            relation_name,
            replica_identity,
            columns,
        } => v1::encode_relation(
            buf,
            *relation_id,
            namespace,
            relation_name,
            *replica_identity,
            columns,
        ),
        M::Insert { relation_id, tuple } => v1::encode_insert(buf, *relation_id, tuple),
        M::Update {
            relation_id,
            old_tuple,
            new_tuple,
            key_type,
        } => v1::encode_update(buf, *relation_id, old_tuple.as_ref(), new_tuple, *key_type),
        M::Delete {
            relation_id,
            old_tuple,
            key_type,
        } => v1::encode_delete(buf, *relation_id, old_tuple, *key_type),
        M::Truncate {
            relation_ids,
            flags,
        } => v1::encode_truncate(buf, relation_ids, *flags),
        M::Type {
            type_id,
            namespace,
            type_name,
        } => v1::encode_type(buf, *type_id, namespace, type_name),
        M::Origin {
            origin_lsn,
            origin_name,
        } => v1::encode_origin(buf, *origin_lsn, origin_name),
        M::Message {
            flags,
            lsn,
            prefix,
            content,
        } => v1::encode_logical_message(buf, *flags, *lsn, prefix, content),
        M::StreamStart { xid, first_segment } => {
            streaming::encode_stream_start(buf, *xid, *first_segment)
        }
        M::StreamStop => streaming::encode_stream_stop(buf),
        M::StreamCommit {
            xid,
            flags,
            commit_lsn,
            end_lsn,
            timestamp,
        } => streaming::encode_stream_commit(buf, *xid, *flags, *commit_lsn, *end_lsn, *timestamp),
        M::StreamAbort {
            xid,
            subtransaction_xid,
            abort_lsn,
            abort_timestamp,
        } => streaming::encode_stream_abort(
            buf,
            protocol_version,
            *xid,
            *subtransaction_xid,
            *abort_lsn,
            *abort_timestamp,
        ),
        M::BeginPrepare {
            prepare_lsn,
            end_lsn,
            timestamp,
            xid,
            gid,
        } => two_phase::encode_begin_prepare(buf, *prepare_lsn, *end_lsn, *timestamp, *xid, gid),
        M::Prepare {
            flags,
            prepare_lsn,
            end_lsn,
            timestamp,
            xid,
            gid,
        } => two_phase::encode_prepare(buf, *flags, *prepare_lsn, *end_lsn, *timestamp, *xid, gid),
        M::CommitPrepared {
            flags,
            commit_lsn,
            end_lsn,
            timestamp,
            xid,
            gid,
        } => two_phase::encode_commit_prepared(
            buf,
            *flags,
            *commit_lsn,
            *end_lsn,
            *timestamp,
            *xid,
            gid,
        ),
        M::RollbackPrepared {
            flags,
            prepare_end_lsn,
            rollback_end_lsn,
            prepare_timestamp,
            rollback_timestamp,
            xid,
            gid,
        } => two_phase::encode_rollback_prepared(
            buf,
            *flags,
            *prepare_end_lsn,
            *rollback_end_lsn,
            *prepare_timestamp,
            *rollback_timestamp,
            *xid,
            gid,
        ),
        M::StreamPrepare {
            flags,
            prepare_lsn,
            end_lsn,
            timestamp,
            xid,
            gid,
        } => two_phase::encode_stream_prepare(
            buf,
            *flags,
            *prepare_lsn,
            *end_lsn,
            *timestamp,
            *xid,
            gid,
        ),
    }
}

/// [`encode_message`] into a fresh [`BytesMut`] pre-sized for the message.
pub fn encode_message_to_bytes(msg: &LogicalReplicationMessage, protocol_version: u8) -> BytesMut {
    let mut buf = BytesMut::with_capacity(capacity_hint(msg));
    encode_message(msg, protocol_version, &mut buf);
    buf
}

/// Best-effort capacity for [`encode_message_to_bytes`] (undersizing just reallocs).
fn capacity_hint(msg: &LogicalReplicationMessage) -> usize {
    use LogicalReplicationMessage as M;

    fn tuple_hint(tuple: &TupleData) -> usize {
        2 + tuple
            .columns
            .iter()
            .map(|c| 1 + 4 + c.as_bytes().len())
            .sum::<usize>()
    }

    match msg {
        M::Begin { .. } => 1 + 8 + 8 + 4,
        M::Commit { .. } => 1 + 1 + 8 + 8 + 8,
        M::Relation {
            namespace,
            relation_name,
            columns,
            ..
        } => {
            let cols: usize = columns.iter().map(|c| 1 + c.name.len() + 1 + 4 + 4).sum();
            1 + 4 + namespace.len() + 1 + relation_name.len() + 1 + 1 + 2 + cols
        }
        M::Insert { tuple, .. } => 1 + 4 + 1 + tuple_hint(tuple),
        M::Update {
            old_tuple,
            new_tuple,
            ..
        } => {
            1 + 4 + old_tuple.as_ref().map_or(0, |t| 1 + tuple_hint(t)) + 1 + tuple_hint(new_tuple)
        }
        M::Delete { old_tuple, .. } => 1 + 4 + 1 + tuple_hint(old_tuple),
        M::Truncate { relation_ids, .. } => 1 + 4 + 1 + relation_ids.len() * 4,
        M::Type {
            namespace,
            type_name,
            ..
        } => 1 + 4 + namespace.len() + 1 + type_name.len() + 1,
        M::Origin { origin_name, .. } => 1 + 8 + origin_name.len() + 1,
        M::Message {
            prefix, content, ..
        } => 1 + 1 + 8 + prefix.len() + 1 + 4 + content.len(),
        M::StreamStart { .. } => 1 + 4 + 1,
        M::StreamStop => 1,
        M::StreamCommit { .. } => 1 + 4 + 1 + 8 + 8 + 8,
        M::StreamAbort { .. } => 1 + 4 + 4 + 8 + 8,
        M::BeginPrepare { gid, .. }
        | M::Prepare { gid, .. }
        | M::CommitPrepared { gid, .. }
        | M::RollbackPrepared { gid, .. }
        | M::StreamPrepare { gid, .. } => 48 + gid.len(),
    }
}
