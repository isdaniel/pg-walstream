//! Low-level `pgoutput` wire primitives shared by the message encoders.

use crate::protocol::TupleData;
use bytes::{BufMut, BytesMut};

/// Append a CString: UTF-8 bytes plus a `0x00` terminator.
#[inline]
pub(super) fn write_cstring(buf: &mut BytesMut, s: &str) {
    debug_assert!(
        !s.as_bytes().contains(&0),
        "pgoutput CString must not contain an interior null byte"
    );
    buf.put_slice(s.as_bytes());
    buf.put_u8(0);
}

/// Append a `TupleData` block: `int16` count, then per column a tag byte plus,
/// for text (`t`) / binary (`b`), an `int32`-length-prefixed payload.
#[inline]
pub(super) fn write_tuple_data(buf: &mut BytesMut, tuple: &TupleData) {
    debug_assert!(
        tuple.columns.len() <= u16::MAX as usize,
        "tuple column count exceeds int16"
    );
    buf.put_u16(tuple.columns.len() as u16);
    for col in &tuple.columns {
        match col.data_type {
            b'n' | b'u' => buf.put_u8(col.data_type),
            tag @ (b't' | b'b') => {
                let data = col.as_bytes();
                buf.put_u8(tag);
                debug_assert!(
                    data.len() <= u32::MAX as usize,
                    "column payload exceeds int32"
                );
                buf.put_u32(data.len() as u32);
                buf.put_slice(data);
            }
            // Defensive: constructors and the parser only yield n/u/t/b, but
            // `data_type` is a public field. Emit an out-of-contract tag as-is
            // rather than panic.
            other => buf.put_u8(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::write_tuple_data;
    use crate::protocol::{ColumnData, TupleData};
    use bytes::BytesMut;

    #[test]
    fn write_tuple_data_unknown_tag_emits_bare_tag() {
        // `data_type` is a public field, so an out-of-contract tag is reachable.
        let mut col = ColumnData::null();
        col.data_type = b'?';
        let tuple = TupleData::new(vec![col]);
        let mut buf = BytesMut::new();
        write_tuple_data(&mut buf, &tuple);
        // int16 count = 1, then the bare unknown tag (no payload).
        assert_eq!(&buf[..], &[0x00, 0x01, b'?']);
    }
}
