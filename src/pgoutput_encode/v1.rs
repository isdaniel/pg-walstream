//! Encoders for the protocol-v1 message set.

use super::wire::{write_cstring, write_tuple_data};
use crate::protocol::{message_types, ColumnInfo, TupleData};
use crate::types::{Oid, TimestampTz, XLogRecPtr, Xid};
use bytes::{BufMut, BytesMut};

pub(super) fn encode_begin(
    buf: &mut BytesMut,
    final_lsn: XLogRecPtr,
    timestamp: TimestampTz,
    xid: Xid,
) {
    buf.put_u8(message_types::BEGIN);
    buf.put_u64(final_lsn);
    buf.put_i64(timestamp);
    buf.put_u32(xid);
}

pub(super) fn encode_commit(
    buf: &mut BytesMut,
    flags: u8,
    commit_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
    timestamp: TimestampTz,
) {
    buf.put_u8(message_types::COMMIT);
    buf.put_u8(flags);
    buf.put_u64(commit_lsn);
    buf.put_u64(end_lsn);
    buf.put_i64(timestamp);
}

pub(super) fn encode_relation(
    buf: &mut BytesMut,
    relation_id: Oid,
    namespace: &str,
    relation_name: &str,
    replica_identity: u8,
    columns: &[ColumnInfo],
) {
    buf.put_u8(message_types::RELATION);
    buf.put_u32(relation_id);
    write_cstring(buf, namespace);
    write_cstring(buf, relation_name);
    buf.put_u8(replica_identity);
    debug_assert!(
        columns.len() <= u16::MAX as usize,
        "relation column count exceeds int16"
    );
    buf.put_u16(columns.len() as u16);
    for col in columns {
        buf.put_u8(col.flags);
        write_cstring(buf, &col.name);
        buf.put_u32(col.type_id);
        buf.put_i32(col.type_modifier);
    }
}

pub(super) fn encode_insert(buf: &mut BytesMut, relation_id: Oid, tuple: &TupleData) {
    buf.put_u8(message_types::INSERT);
    buf.put_u32(relation_id);
    buf.put_u8(b'N');
    write_tuple_data(buf, tuple);
}

pub(super) fn encode_update(
    buf: &mut BytesMut,
    relation_id: Oid,
    old_tuple: Option<&TupleData>,
    new_tuple: &TupleData,
    key_type: Option<char>,
) {
    buf.put_u8(message_types::UPDATE);
    buf.put_u32(relation_id);
    // Old-tuple block only with a key/full image. Byte is 'K' or 'O'.
    if let Some(old) = old_tuple {
        let key = key_type.unwrap_or('K');
        debug_assert!(
            key == 'K' || key == 'O',
            "update key type must be 'K' or 'O'"
        );
        buf.put_u8(key as u8);
        write_tuple_data(buf, old);
    }
    buf.put_u8(b'N');
    write_tuple_data(buf, new_tuple);
}

pub(super) fn encode_delete(
    buf: &mut BytesMut,
    relation_id: Oid,
    old_tuple: &TupleData,
    key_type: char,
) {
    buf.put_u8(message_types::DELETE);
    buf.put_u32(relation_id);
    // Parser accepts any byte here, must fit one byte for the cast.
    debug_assert!(
        (key_type as u32) <= 0xFF,
        "delete key type must fit in a single byte"
    );
    buf.put_u8(key_type as u8);
    write_tuple_data(buf, old_tuple);
}

pub(super) fn encode_truncate(buf: &mut BytesMut, relation_ids: &[Oid], flags: u8) {
    buf.put_u8(message_types::TRUNCATE);
    debug_assert!(
        relation_ids.len() <= u32::MAX as usize,
        "truncate relation count exceeds int32"
    );
    buf.put_u32(relation_ids.len() as u32);
    buf.put_u8(flags);
    for &oid in relation_ids {
        buf.put_u32(oid);
    }
}

pub(super) fn encode_type(buf: &mut BytesMut, type_id: Oid, namespace: &str, type_name: &str) {
    buf.put_u8(message_types::TYPE);
    buf.put_u32(type_id);
    write_cstring(buf, namespace);
    write_cstring(buf, type_name);
}

pub(super) fn encode_origin(buf: &mut BytesMut, origin_lsn: XLogRecPtr, origin_name: &str) {
    buf.put_u8(message_types::ORIGIN);
    buf.put_u64(origin_lsn);
    write_cstring(buf, origin_name);
}

pub(super) fn encode_logical_message(
    buf: &mut BytesMut,
    flags: u8,
    lsn: XLogRecPtr,
    prefix: &str,
    content: &[u8],
) {
    buf.put_u8(message_types::MESSAGE);
    buf.put_u8(flags);
    buf.put_u64(lsn);
    write_cstring(buf, prefix);
    debug_assert!(
        content.len() <= u32::MAX as usize,
        "message content exceeds int32"
    );
    buf.put_u32(content.len() as u32);
    buf.put_slice(content);
}

#[cfg(test)]
mod tests {
    use crate::protocol::{ColumnData, ColumnInfo, LogicalReplicationMessage as M, TupleData};
    use bytes::{Bytes, BytesMut};
    use std::sync::Arc;

    fn encode(msg: &M, version: u8) -> Vec<u8> {
        let mut buf = BytesMut::new();
        crate::pgoutput_encode::encode_message(msg, version, &mut buf);
        buf.to_vec()
    }

    #[test]
    fn encode_begin_matches_spec_bytes() {
        let msg = M::Begin {
            final_lsn: 0x0000_0000_0100_0000,
            timestamp: 1_700_000_000_000_000,
            xid: 42,
        };
        let mut expected = vec![b'B'];
        expected.extend_from_slice(&0x0000_0000_0100_0000u64.to_be_bytes());
        expected.extend_from_slice(&1_700_000_000_000_000i64.to_be_bytes());
        expected.extend_from_slice(&42u32.to_be_bytes());
        assert_eq!(encode(&msg, 1), expected);
    }

    #[test]
    fn encode_commit_matches_spec_bytes() {
        let msg = M::Commit {
            flags: 0,
            commit_lsn: 0x0000_0000_0100_0000,
            end_lsn: 0x0000_0000_0100_0020,
            timestamp: 1_700_000_000_000_000,
        };
        let mut expected = vec![b'C', 0u8];
        expected.extend_from_slice(&0x0000_0000_0100_0000u64.to_be_bytes());
        expected.extend_from_slice(&0x0000_0000_0100_0020u64.to_be_bytes());
        expected.extend_from_slice(&1_700_000_000_000_000i64.to_be_bytes());
        assert_eq!(encode(&msg, 1), expected);
    }

    #[test]
    fn encode_origin_matches_spec_bytes() {
        let msg = M::Origin {
            origin_lsn: 0x0000_0000_1234_5678,
            origin_name: "pg_origin".to_string(),
        };
        let mut expected = vec![b'O'];
        expected.extend_from_slice(&0x0000_0000_1234_5678u64.to_be_bytes());
        expected.extend_from_slice(b"pg_origin\0");
        assert_eq!(encode(&msg, 1), expected);
    }

    #[test]
    fn encode_relation_matches_spec_bytes() {
        let msg = M::Relation {
            relation_id: 0x0001_0203,
            namespace: Arc::from("public"),
            relation_name: Arc::from("users"),
            replica_identity: b'd',
            columns: vec![
                ColumnInfo::new(1, "id".to_string(), 23, -1),
                ColumnInfo::new(0, "name".to_string(), 25, -1),
            ],
        };
        let mut expected = vec![b'R'];
        expected.extend_from_slice(&0x0001_0203u32.to_be_bytes());
        expected.extend_from_slice(b"public\0");
        expected.extend_from_slice(b"users\0");
        expected.push(b'd');
        expected.extend_from_slice(&2u16.to_be_bytes());
        expected.push(1);
        expected.extend_from_slice(b"id\0");
        expected.extend_from_slice(&23u32.to_be_bytes());
        expected.extend_from_slice(&(-1i32).to_be_bytes());
        expected.push(0);
        expected.extend_from_slice(b"name\0");
        expected.extend_from_slice(&25u32.to_be_bytes());
        expected.extend_from_slice(&(-1i32).to_be_bytes());
        assert_eq!(encode(&msg, 1), expected);
    }

    #[test]
    fn encode_type_matches_spec_bytes() {
        let msg = M::Type {
            type_id: 0x0000_4567,
            namespace: "public".to_string(),
            type_name: "mood".to_string(),
        };
        let mut expected = vec![b'Y'];
        expected.extend_from_slice(&0x0000_4567u32.to_be_bytes());
        expected.extend_from_slice(b"public\0");
        expected.extend_from_slice(b"mood\0");
        assert_eq!(encode(&msg, 1), expected);
    }

    #[test]
    fn encode_insert_matches_spec_bytes() {
        let tuple = TupleData::new(vec![ColumnData::text(b"42".to_vec()), ColumnData::null()]);
        let msg = M::Insert {
            relation_id: 0x0000_002A,
            tuple,
        };
        let mut expected = vec![b'I'];
        expected.extend_from_slice(&0x0000_002Au32.to_be_bytes());
        expected.push(b'N');
        expected.extend_from_slice(&2u16.to_be_bytes());
        expected.push(b't');
        expected.extend_from_slice(&2u32.to_be_bytes());
        expected.extend_from_slice(b"42");
        expected.push(b'n');
        assert_eq!(encode(&msg, 1), expected);
    }

    #[test]
    fn encode_update_default_identity_matches_spec_bytes() {
        // 'K' old-tuple block (REPLICA IDENTITY DEFAULT), with an unchanged-TOAST
        // column in the old image, then the 'N' new tuple.
        let old = TupleData::new(vec![
            ColumnData::text(b"1".to_vec()),
            ColumnData::unchanged(),
        ]);
        let new = TupleData::new(vec![
            ColumnData::text(b"1".to_vec()),
            ColumnData::text(b"x".to_vec()),
        ]);
        let msg = M::Update {
            relation_id: 7,
            old_tuple: Some(old),
            new_tuple: new,
            key_type: Some('K'),
        };
        let mut expected = vec![b'U'];
        expected.extend_from_slice(&7u32.to_be_bytes());
        expected.push(b'K');
        expected.extend_from_slice(&2u16.to_be_bytes());
        expected.push(b't');
        expected.extend_from_slice(&1u32.to_be_bytes());
        expected.extend_from_slice(b"1");
        expected.push(b'u');
        expected.push(b'N');
        expected.extend_from_slice(&2u16.to_be_bytes());
        expected.push(b't');
        expected.extend_from_slice(&1u32.to_be_bytes());
        expected.extend_from_slice(b"1");
        expected.push(b't');
        expected.extend_from_slice(&1u32.to_be_bytes());
        expected.extend_from_slice(b"x");
        assert_eq!(encode(&msg, 1), expected);
    }

    #[test]
    fn encode_update_full_identity_matches_spec_bytes() {
        let old = TupleData::new(vec![ColumnData::text(b"1".to_vec())]);
        let new = TupleData::new(vec![ColumnData::text(b"2".to_vec())]);
        let msg = M::Update {
            relation_id: 7,
            old_tuple: Some(old),
            new_tuple: new,
            key_type: Some('O'),
        };
        let mut expected = vec![b'U'];
        expected.extend_from_slice(&7u32.to_be_bytes());
        expected.push(b'O');
        expected.extend_from_slice(&1u16.to_be_bytes());
        expected.push(b't');
        expected.extend_from_slice(&1u32.to_be_bytes());
        expected.extend_from_slice(b"1");
        expected.push(b'N');
        expected.extend_from_slice(&1u16.to_be_bytes());
        expected.push(b't');
        expected.extend_from_slice(&1u32.to_be_bytes());
        expected.extend_from_slice(b"2");
        assert_eq!(encode(&msg, 1), expected);
    }

    #[test]
    fn encode_update_without_old_tuple_matches_spec_bytes() {
        let new = TupleData::new(vec![ColumnData::text(b"9".to_vec())]);
        let msg = M::Update {
            relation_id: 7,
            old_tuple: None,
            new_tuple: new,
            key_type: None,
        };
        let mut expected = vec![b'U'];
        expected.extend_from_slice(&7u32.to_be_bytes());
        expected.push(b'N');
        expected.extend_from_slice(&1u16.to_be_bytes());
        expected.push(b't');
        expected.extend_from_slice(&1u32.to_be_bytes());
        expected.extend_from_slice(b"9");
        assert_eq!(encode(&msg, 1), expected);
    }

    #[test]
    fn encode_delete_default_identity_matches_spec_bytes() {
        let old = TupleData::new(vec![ColumnData::text(b"1".to_vec())]);
        let msg = M::Delete {
            relation_id: 7,
            old_tuple: old,
            key_type: 'K',
        };
        let mut expected = vec![b'D'];
        expected.extend_from_slice(&7u32.to_be_bytes());
        expected.push(b'K');
        expected.extend_from_slice(&1u16.to_be_bytes());
        expected.push(b't');
        expected.extend_from_slice(&1u32.to_be_bytes());
        expected.extend_from_slice(b"1");
        assert_eq!(encode(&msg, 1), expected);
    }

    #[test]
    fn encode_delete_full_identity_matches_spec_bytes() {
        let old = TupleData::new(vec![ColumnData::binary(vec![0xDE, 0xAD])]);
        let msg = M::Delete {
            relation_id: 7,
            old_tuple: old,
            key_type: 'O',
        };
        let mut expected = vec![b'D'];
        expected.extend_from_slice(&7u32.to_be_bytes());
        expected.push(b'O');
        expected.extend_from_slice(&1u16.to_be_bytes());
        expected.push(b'b');
        expected.extend_from_slice(&2u32.to_be_bytes());
        expected.extend_from_slice(&[0xDE, 0xAD]);
        assert_eq!(encode(&msg, 1), expected);
    }

    #[test]
    fn encode_truncate_matches_spec_bytes() {
        let msg = M::Truncate {
            relation_ids: vec![10, 20],
            flags: 0b11, // CASCADE | RESTART IDENTITY
        };
        let mut expected = vec![b'T'];
        expected.extend_from_slice(&2u32.to_be_bytes());
        expected.push(0b11);
        expected.extend_from_slice(&10u32.to_be_bytes());
        expected.extend_from_slice(&20u32.to_be_bytes());
        assert_eq!(encode(&msg, 1), expected);
    }

    #[test]
    fn encode_message_msg_matches_spec_bytes() {
        let msg = M::Message {
            flags: 1,
            lsn: 0x0000_0000_0000_1000,
            prefix: "test".to_string(),
            content: Bytes::from_static(&[0xDE, 0xAD, 0xBE, 0xEF]),
        };
        let mut expected = vec![b'M', 1u8];
        expected.extend_from_slice(&0x0000_0000_0000_1000u64.to_be_bytes());
        expected.extend_from_slice(b"test\0");
        expected.extend_from_slice(&4u32.to_be_bytes());
        expected.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(encode(&msg, 1), expected);
    }

    #[test]
    fn data_message_framing_is_version_independent() {
        // Data messages carry no leading xid prefix in this surface, so the bytes
        // are identical across protocol versions (the in-stream prefix is context,
        // not part of the message value).
        let msg = M::Insert {
            relation_id: 1,
            tuple: TupleData::new(vec![ColumnData::text(b"abc".to_vec())]),
        };
        assert_eq!(encode(&msg, 1), encode(&msg, 2));
        assert_eq!(encode(&msg, 1), encode(&msg, 4));
    }
}
