//! Encoders for streaming-transaction control messages (protocol v2+).

use crate::protocol::message_types;
use crate::types::{TimestampTz, XLogRecPtr, Xid};
use bytes::{BufMut, BytesMut};

pub(super) fn encode_stream_start(buf: &mut BytesMut, xid: Xid, first_segment: bool) {
    buf.put_u8(message_types::STREAM_START);
    buf.put_u32(xid);
    buf.put_u8(u8::from(first_segment));
}

pub(super) fn encode_stream_stop(buf: &mut BytesMut) {
    buf.put_u8(message_types::STREAM_STOP);
}

pub(super) fn encode_stream_commit(
    buf: &mut BytesMut,
    xid: Xid,
    flags: u8,
    commit_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
    timestamp: TimestampTz,
) {
    buf.put_u8(message_types::STREAM_COMMIT);
    buf.put_u32(xid);
    buf.put_u8(flags);
    buf.put_u64(commit_lsn);
    buf.put_u64(end_lsn);
    buf.put_i64(timestamp);
}

pub(super) fn encode_stream_abort(
    buf: &mut BytesMut,
    protocol_version: u8,
    xid: Xid,
    subtransaction_xid: Xid,
    abort_lsn: Option<XLogRecPtr>,
    abort_timestamp: Option<TimestampTz>,
) {
    buf.put_u8(message_types::STREAM_ABORT);
    buf.put_u32(xid);
    buf.put_u32(subtransaction_xid);
    // The abort_lsn / abort_timestamp tail exists only in the protocol-v4
    // parallel-apply variant, matching the parser's version gate.
    if protocol_version >= 4 {
        if let (Some(lsn), Some(timestamp)) = (abort_lsn, abort_timestamp) {
            buf.put_u64(lsn);
            buf.put_i64(timestamp);
        }
    }
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
    fn encode_stream_start_matches_spec_bytes() {
        let msg = M::StreamStart {
            xid: 99,
            first_segment: true,
        };
        let mut expected = vec![b'S'];
        expected.extend_from_slice(&99u32.to_be_bytes());
        expected.push(1);
        assert_eq!(encode(&msg, 2), expected);
    }

    #[test]
    fn encode_stream_stop_matches_spec_bytes() {
        assert_eq!(encode(&M::StreamStop, 2), vec![b'E']);
    }

    #[test]
    fn encode_stream_commit_matches_spec_bytes() {
        let msg = M::StreamCommit {
            xid: 99,
            flags: 0,
            commit_lsn: 0x0000_0000_0100_0000,
            end_lsn: 0x0000_0000_0100_0020,
            timestamp: 1_700_000_000_000_000,
        };
        let mut expected = vec![b'c'];
        expected.extend_from_slice(&99u32.to_be_bytes());
        expected.push(0);
        expected.extend_from_slice(&0x0000_0000_0100_0000u64.to_be_bytes());
        expected.extend_from_slice(&0x0000_0000_0100_0020u64.to_be_bytes());
        expected.extend_from_slice(&1_700_000_000_000_000i64.to_be_bytes());
        assert_eq!(encode(&msg, 2), expected);
    }

    #[test]
    fn encode_stream_abort_without_v4_tail_matches_spec_bytes() {
        let msg = M::StreamAbort {
            xid: 99,
            subtransaction_xid: 100,
            abort_lsn: None,
            abort_timestamp: None,
        };
        let mut expected = vec![b'A'];
        expected.extend_from_slice(&99u32.to_be_bytes());
        expected.extend_from_slice(&100u32.to_be_bytes());
        assert_eq!(encode(&msg, 2), expected);
    }

    #[test]
    fn encode_stream_abort_with_v4_tail_matches_spec_bytes() {
        let msg = M::StreamAbort {
            xid: 99,
            subtransaction_xid: 100,
            abort_lsn: Some(0x0000_0000_0100_0000),
            abort_timestamp: Some(1_700_000_000_000_000),
        };
        let mut expected = vec![b'A'];
        expected.extend_from_slice(&99u32.to_be_bytes());
        expected.extend_from_slice(&100u32.to_be_bytes());
        expected.extend_from_slice(&0x0000_0000_0100_0000u64.to_be_bytes());
        expected.extend_from_slice(&1_700_000_000_000_000i64.to_be_bytes());
        assert_eq!(encode(&msg, 4), expected);
    }

    #[test]
    fn encode_stream_abort_tail_elided_below_v4() {
        // A v4-shaped value encoded at v2/v3 must drop the tail to match the
        // parser, which only reads it at protocol >= 4.
        let msg = M::StreamAbort {
            xid: 99,
            subtransaction_xid: 100,
            abort_lsn: Some(0x0000_0000_0100_0000),
            abort_timestamp: Some(1_700_000_000_000_000),
        };
        let mut expected = vec![b'A'];
        expected.extend_from_slice(&99u32.to_be_bytes());
        expected.extend_from_slice(&100u32.to_be_bytes());
        assert_eq!(encode(&msg, 2), expected);
        assert_eq!(encode(&msg, 3), expected);
    }
}
