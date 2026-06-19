//! Encoders for two-phase-commit messages (protocol v3+).

use super::wire::write_cstring;
use crate::protocol::message_types;
use crate::types::{TimestampTz, XLogRecPtr, Xid};
use bytes::{BufMut, BytesMut};

pub(super) fn encode_begin_prepare(
    buf: &mut BytesMut,
    prepare_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
    timestamp: TimestampTz,
    xid: Xid,
    gid: &str,
) {
    buf.put_u8(message_types::BEGIN_PREPARE);
    buf.put_u64(prepare_lsn);
    buf.put_u64(end_lsn);
    buf.put_i64(timestamp);
    buf.put_u32(xid);
    write_cstring(buf, gid);
}

pub(super) fn encode_prepare(
    buf: &mut BytesMut,
    flags: u8,
    prepare_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
    timestamp: TimestampTz,
    xid: Xid,
    gid: &str,
) {
    buf.put_u8(message_types::PREPARE);
    buf.put_u8(flags);
    buf.put_u64(prepare_lsn);
    buf.put_u64(end_lsn);
    buf.put_i64(timestamp);
    buf.put_u32(xid);
    write_cstring(buf, gid);
}

pub(super) fn encode_commit_prepared(
    buf: &mut BytesMut,
    flags: u8,
    commit_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
    timestamp: TimestampTz,
    xid: Xid,
    gid: &str,
) {
    buf.put_u8(message_types::COMMIT_PREPARED);
    buf.put_u8(flags);
    buf.put_u64(commit_lsn);
    buf.put_u64(end_lsn);
    buf.put_i64(timestamp);
    buf.put_u32(xid);
    write_cstring(buf, gid);
}

#[allow(clippy::too_many_arguments)] // mirrors the message's seven fields
pub(super) fn encode_rollback_prepared(
    buf: &mut BytesMut,
    flags: u8,
    prepare_end_lsn: XLogRecPtr,
    rollback_end_lsn: XLogRecPtr,
    prepare_timestamp: TimestampTz,
    rollback_timestamp: TimestampTz,
    xid: Xid,
    gid: &str,
) {
    buf.put_u8(message_types::ROLLBACK_PREPARED);
    buf.put_u8(flags);
    buf.put_u64(prepare_end_lsn);
    buf.put_u64(rollback_end_lsn);
    buf.put_i64(prepare_timestamp);
    buf.put_i64(rollback_timestamp);
    buf.put_u32(xid);
    write_cstring(buf, gid);
}

pub(super) fn encode_stream_prepare(
    buf: &mut BytesMut,
    flags: u8,
    prepare_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
    timestamp: TimestampTz,
    xid: Xid,
    gid: &str,
) {
    buf.put_u8(message_types::STREAM_PREPARE);
    buf.put_u8(flags);
    buf.put_u64(prepare_lsn);
    buf.put_u64(end_lsn);
    buf.put_i64(timestamp);
    buf.put_u32(xid);
    write_cstring(buf, gid);
}

#[cfg(test)]
mod tests {
    use crate::protocol::LogicalReplicationMessage as M;
    use bytes::BytesMut;

    fn encode(msg: &M, version: u8) -> Vec<u8> {
        let mut buf = BytesMut::new();
        crate::pgoutput_encode::encode_message(msg, version, &mut buf);
        buf.to_vec()
    }

    #[test]
    fn encode_begin_prepare_matches_spec_bytes() {
        let msg = M::BeginPrepare {
            prepare_lsn: 0x0000_0000_0100_0000,
            end_lsn: 0x0000_0000_0100_0020,
            timestamp: 1_700_000_000_000_000,
            xid: 55,
            gid: "g1".to_string(),
        };
        let mut expected = vec![b'b'];
        expected.extend_from_slice(&0x0000_0000_0100_0000u64.to_be_bytes());
        expected.extend_from_slice(&0x0000_0000_0100_0020u64.to_be_bytes());
        expected.extend_from_slice(&1_700_000_000_000_000i64.to_be_bytes());
        expected.extend_from_slice(&55u32.to_be_bytes());
        expected.extend_from_slice(b"g1\0");
        assert_eq!(encode(&msg, 3), expected);
    }

    #[test]
    fn encode_prepare_matches_spec_bytes() {
        let msg = M::Prepare {
            flags: 0,
            prepare_lsn: 0x0000_0000_0100_0000,
            end_lsn: 0x0000_0000_0100_0020,
            timestamp: 1_700_000_000_000_000,
            xid: 55,
            gid: "g1".to_string(),
        };
        let mut expected = vec![b'P', 0u8];
        expected.extend_from_slice(&0x0000_0000_0100_0000u64.to_be_bytes());
        expected.extend_from_slice(&0x0000_0000_0100_0020u64.to_be_bytes());
        expected.extend_from_slice(&1_700_000_000_000_000i64.to_be_bytes());
        expected.extend_from_slice(&55u32.to_be_bytes());
        expected.extend_from_slice(b"g1\0");
        assert_eq!(encode(&msg, 3), expected);
    }

    #[test]
    fn encode_commit_prepared_matches_spec_bytes() {
        let msg = M::CommitPrepared {
            flags: 0,
            commit_lsn: 0x0000_0000_0100_0000,
            end_lsn: 0x0000_0000_0100_0020,
            timestamp: 1_700_000_000_000_000,
            xid: 55,
            gid: "g1".to_string(),
        };
        let mut expected = vec![b'K', 0u8];
        expected.extend_from_slice(&0x0000_0000_0100_0000u64.to_be_bytes());
        expected.extend_from_slice(&0x0000_0000_0100_0020u64.to_be_bytes());
        expected.extend_from_slice(&1_700_000_000_000_000i64.to_be_bytes());
        expected.extend_from_slice(&55u32.to_be_bytes());
        expected.extend_from_slice(b"g1\0");
        assert_eq!(encode(&msg, 3), expected);
    }

    #[test]
    fn encode_rollback_prepared_matches_spec_bytes() {
        let msg = M::RollbackPrepared {
            flags: 0,
            prepare_end_lsn: 0x0000_0000_0100_0000,
            rollback_end_lsn: 0x0000_0000_0100_0040,
            prepare_timestamp: 1_700_000_000_000_000,
            rollback_timestamp: 1_700_000_000_500_000,
            xid: 55,
            gid: "g1".to_string(),
        };
        let mut expected = vec![b'r', 0u8];
        expected.extend_from_slice(&0x0000_0000_0100_0000u64.to_be_bytes());
        expected.extend_from_slice(&0x0000_0000_0100_0040u64.to_be_bytes());
        expected.extend_from_slice(&1_700_000_000_000_000i64.to_be_bytes());
        expected.extend_from_slice(&1_700_000_000_500_000i64.to_be_bytes());
        expected.extend_from_slice(&55u32.to_be_bytes());
        expected.extend_from_slice(b"g1\0");
        assert_eq!(encode(&msg, 3), expected);
    }

    #[test]
    fn encode_stream_prepare_matches_spec_bytes() {
        let msg = M::StreamPrepare {
            flags: 0,
            prepare_lsn: 0x0000_0000_0100_0000,
            end_lsn: 0x0000_0000_0100_0020,
            timestamp: 1_700_000_000_000_000,
            xid: 55,
            gid: "g1".to_string(),
        };
        let mut expected = vec![b'p', 0u8];
        expected.extend_from_slice(&0x0000_0000_0100_0000u64.to_be_bytes());
        expected.extend_from_slice(&0x0000_0000_0100_0020u64.to_be_bytes());
        expected.extend_from_slice(&1_700_000_000_000_000i64.to_be_bytes());
        expected.extend_from_slice(&55u32.to_be_bytes());
        expected.extend_from_slice(b"g1\0");
        assert_eq!(encode(&msg, 3), expected);
    }
}
