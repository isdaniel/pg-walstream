//! Column value types for PostgreSQL logical replication
//!
//! This module provides [`ColumnValue`] and [`RowData`] — the core data types
//! used to represent column-level data from PostgreSQL's logical replication
//! protocol. Both types use zero-copy [`bytes::Bytes`] internally and support
//! a compact binary wire format for efficient serialisation.

use crate::buffer::BufferReader;
use crate::error::{ReplicationError, Result};
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use serde::ser::SerializeMap;

/// Encode a byte slice as lowercase hex string.
pub(crate) fn hex_encode(bytes: &[u8]) -> String {
    const LUT: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(LUT[(byte >> 4) as usize] as char);
        out.push(LUT[(byte & 0x0f) as usize] as char);
    }
    out
}

/// Decode a hex string to bytes. Returns `Err` on invalid hex.
fn hex_decode(hex: &str) -> std::result::Result<Vec<u8>, &'static str> {
    if hex.len() % 2 != 0 {
        return Err("odd hex length");
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    let bytes = hex.as_bytes();
    for chunk in bytes.chunks_exact(2) {
        let high = hex_nibble(chunk[0]).ok_or("invalid hex char")?;
        let low = hex_nibble(chunk[1]).ok_or("invalid hex char")?;
        out.push((high << 4) | low);
    }
    Ok(out)
}

#[inline]
fn hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// ColumnValue
// ---------------------------------------------------------------------------

/// PostgreSQL's logical replication protocol sends column data as either text (UTF-8 encoded) or binary format. This enum preserves the raw representation with zero-copy semantics using [`bytes::Bytes`], avoiding unnecessary  parsing and allocation.
///
/// # Wire Format (binary encode/decode)
///
/// | Tag byte | Meaning                           |
/// |----------|-----------------------------------|
/// | `0x00`   | `Null`                            |
/// | `0x01`   | `Text` — followed by u32-len + data |
/// | `0x02`   | `Binary` — followed by u32-len + data |
///
/// When serialised with [`serde`], `Text` values emit a JSON string,
/// `Binary` values emit a tagged JSON object `{"$binary": "deadbeef"}`,
/// and `Null` emits JSON `null`.
///
/// The tagged-object format is unambiguous: a `Text` value whose content happens to look like hex will always round-trip correctly.
///
/// # Example
///
/// ```
/// use pg_walstream::ColumnValue;
/// use bytes::Bytes;
///
/// let v = ColumnValue::text("hello");
/// assert_eq!(v.as_str(), Some("hello"));
/// assert!(!v.is_null());
///
/// let n = ColumnValue::Null;
/// assert!(n.is_null());
/// ```
#[derive(Debug, Clone)]
pub enum ColumnValue {
    /// SQL NULL
    Null,
    /// Text value from PostgreSQL (UTF-8 encoded string), Uses [`Bytes`] for zero-copy from the protocol buffer.
    Text(Bytes),
    /// Binary data from PostgreSQL (bytea or binary-mode columns), Stored as raw bytes.
    Binary(Bytes),
}

impl ColumnValue {
    /// Wire-format tag bytes
    const TAG_NULL: u8 = 0x00;
    const TAG_TEXT: u8 = 0x01;
    const TAG_BINARY: u8 = 0x02;

    /// Create a `Text` value from a string slice (copies into `Bytes`).
    #[inline]
    pub fn text(s: &str) -> Self {
        Self::Text(Bytes::copy_from_slice(s.as_bytes()))
    }

    /// Create a `Text` value from existing `Bytes` (zero-copy).
    #[inline]
    pub fn text_bytes(b: Bytes) -> Self {
        Self::Text(b)
    }

    /// Create a `Binary` value from existing `Bytes` (zero-copy).
    #[inline]
    pub fn binary_bytes(b: Bytes) -> Self {
        Self::Binary(b)
    }

    /// Returns `true` if this is a `Null` value.
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Get the text content as `&str`.
    ///
    /// Returns `Some` for `Text` values that are valid UTF-8, `None` otherwise.
    #[inline]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Text(b) => std::str::from_utf8(b).ok(),
            _ => None,
        }
    }

    /// Get raw bytes regardless of variant.
    ///
    /// Returns an empty slice for `Null`.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Text(b) | Self::Binary(b) => b,
            Self::Null => &[],
        }
    }

    /// Encode this value into a byte buffer.
    ///
    /// Format: `[1-byte tag]` then for non-null `[4-byte big-endian length][data]`.
    #[inline]
    pub fn encode(&self, buf: &mut BytesMut) {
        match self {
            Self::Null => buf.extend_from_slice(&[Self::TAG_NULL]),
            Self::Text(b) => {
                buf.extend_from_slice(&[Self::TAG_TEXT]);
                buf.extend_from_slice(&(b.len() as u32).to_be_bytes());
                buf.extend_from_slice(b);
            }
            Self::Binary(b) => {
                buf.extend_from_slice(&[Self::TAG_BINARY]);
                buf.extend_from_slice(&(b.len() as u32).to_be_bytes());
                buf.extend_from_slice(b);
            }
        }
    }

    /// Decode a value from a [`BufferReader`].
    ///
    /// Returns an error if the buffer is too short or contains an unknown tag.
    #[inline]
    pub fn decode(reader: &mut BufferReader) -> Result<Self> {
        let tag = reader.read_u8()?;
        match tag {
            Self::TAG_NULL => Ok(Self::Null),
            Self::TAG_TEXT => {
                let len = reader.read_u32()? as usize;
                let data = reader.read_bytes_buf(len)?;
                Ok(Self::Text(data))
            }
            Self::TAG_BINARY => {
                let len = reader.read_u32()? as usize;
                let data = reader.read_bytes_buf(len)?;
                Ok(Self::Binary(data))
            }
            _ => Err(ReplicationError::protocol(format!(
                "Unknown ColumnValue tag: 0x{tag:02x}"
            ))),
        }
    }
}

impl std::fmt::Display for ColumnValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Text(b) => match std::str::from_utf8(b) {
                Ok(s) => write!(f, "{s}"),
                Err(_) => write!(f, "<invalid utf-8: {} bytes>", b.len()),
            },
            Self::Binary(b) => {
                write!(f, "\\x")?;
                for byte in b.iter() {
                    write!(f, "{byte:02x}")?;
                }
                Ok(())
            }
        }
    }
}

impl PartialEq for ColumnValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Text(a), Self::Text(b)) => a == b,
            (Self::Binary(a), Self::Binary(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for ColumnValue {}

/// Allows `column_value == "some_str"` for text comparisons.
impl PartialEq<str> for ColumnValue {
    fn eq(&self, other: &str) -> bool {
        match self {
            Self::Text(b) => b.as_ref() == other.as_bytes(),
            _ => false,
        }
    }
}

/// Allows `column_value == "some_str"` via `&&str`.
impl PartialEq<&str> for ColumnValue {
    fn eq(&self, other: &&str) -> bool {
        self == *other
    }
}

impl Serialize for ColumnValue {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        match self {
            Self::Null => serializer.serialize_none(),
            Self::Text(b) => match std::str::from_utf8(b) {
                Ok(s) => serializer.serialize_str(s),
                Err(_) => {
                    // Non-UTF-8 text cannot be represented as a JSON string, Emit the tagged binary form so the bytes survive round-trip.
                    let mut map = serializer.serialize_map(Some(1))?;
                    map.serialize_entry("$binary", &hex_encode(b))?;
                    map.end()
                }
            },
            Self::Binary(b) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("$binary", &hex_encode(b))?;
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for ColumnValue {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = ColumnValue;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str(r#"a string, null, or {"$binary": "hex..."}"#)
            }

            fn visit_none<E: serde::de::Error>(self) -> std::result::Result<ColumnValue, E> {
                Ok(ColumnValue::Null)
            }

            fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<ColumnValue, E> {
                Ok(ColumnValue::Null)
            }

            fn visit_some<D: serde::Deserializer<'de>>(
                self,
                deserializer: D,
            ) -> std::result::Result<ColumnValue, D::Error> {
                deserializer.deserialize_any(self)
            }

            fn visit_str<E: serde::de::Error>(
                self,
                v: &str,
            ) -> std::result::Result<ColumnValue, E> {
                Ok(ColumnValue::Text(Bytes::copy_from_slice(v.as_bytes())))
            }

            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut map: M,
            ) -> std::result::Result<ColumnValue, M::Error> {
                use serde::de::Error;
                let key: String = map
                    .next_key()?
                    .ok_or_else(|| M::Error::custom("expected \"$binary\" key in tagged object"))?;
                if key != "$binary" {
                    return Err(M::Error::custom(format!(
                        r#"unknown key "{key}", expected "$binary""#
                    )));
                }
                let hex: String = map.next_value()?;
                let bytes = hex_decode(&hex)
                    .map_err(|e| M::Error::custom(format!("invalid hex in $binary: {e}")))?;
                Ok(ColumnValue::Binary(Bytes::from(bytes)))
            }
        }

        deserializer.deserialize_option(Visitor)
    }
}

/// Ordered row data: a list of `(column_name, value)` pairs.
///
/// Column names are `Arc<str>` — zero-cost clones from relation metadata.
/// Values are [`ColumnValue`] — a lightweight enum holding raw `Bytes`
/// from the PostgreSQL wire protocol without intermediate parsing.
///
/// Serialises as a JSON object `{"col": value, …}` for wire-format compatibility.
///
/// # Example
///
/// ```
/// use pg_walstream::{RowData, ColumnValue};
/// use std::sync::Arc;
///
/// let mut row = RowData::with_capacity(2);
/// row.push(Arc::from("id"), ColumnValue::text("1"));
/// row.push(Arc::from("name"), ColumnValue::text("Alice"));
///
/// assert_eq!(row.len(), 2);
/// assert_eq!(row.get("id").and_then(|v| v.as_str()), Some("1"));
/// ```
#[derive(Debug, Clone, Eq)]
pub struct RowData {
    columns: Vec<(Arc<str>, ColumnValue)>,
}

impl RowData {
    /// Create an empty `RowData`.
    #[inline]
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
        }
    }

    /// Create an empty `RowData` with pre-allocated capacity.
    #[inline]
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            columns: Vec::with_capacity(cap),
        }
    }

    /// Append a column.
    #[inline]
    pub fn push(&mut self, name: Arc<str>, value: ColumnValue) {
        self.columns.push((name, value));
    }

    /// Number of columns.
    #[inline]
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Returns `true` when there are no columns.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Look up a value by column name (linear scan — fast for typical column counts).
    #[inline]
    pub fn get(&self, name: &str) -> Option<&ColumnValue> {
        self.columns
            .iter()
            .find(|(k, _)| k.as_ref() == name)
            .map(|(_, v)| v)
    }

    /// Iterate over `(name, value)` pairs.
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = (&Arc<str>, &ColumnValue)> {
        self.columns.iter().map(|(k, v)| (k, v))
    }

    /// Construct from `(&str, ColumnValue)` pairs — handy for tests and literals.
    #[inline]
    pub fn from_pairs(pairs: Vec<(&str, ColumnValue)>) -> Self {
        Self {
            columns: pairs.into_iter().map(|(k, v)| (Arc::from(k), v)).collect(),
        }
    }

    // ---- binary wire format ----

    /// Encode this `RowData` into a byte buffer.
    ///
    /// Format: `[2-byte column count]` then for each column: `[2-byte name length][name bytes][ColumnValue encoding]`.
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.extend_from_slice(&(self.columns.len() as u16).to_be_bytes());
        for (name, value) in &self.columns {
            let name_bytes = name.as_bytes();
            buf.extend_from_slice(&(name_bytes.len() as u16).to_be_bytes());
            buf.extend_from_slice(name_bytes);
            value.encode(buf);
        }
    }

    /// Decode a `RowData` from a [`BufferReader`].
    pub fn decode(reader: &mut BufferReader) -> Result<Self> {
        let count = reader.read_u16()? as usize;
        let mut columns = Vec::with_capacity(count);
        for _ in 0..count {
            let name_len = reader.read_u16()? as usize;
            let name_bytes = reader.read_bytes(name_len)?;
            let name = std::str::from_utf8(&name_bytes)
                .map_err(|e| ReplicationError::protocol(format!("Invalid column name: {e}")))?;
            let name = Arc::from(name);
            let value = ColumnValue::decode(reader)?;
            columns.push((name, value));
        }
        Ok(Self { columns })
    }
}

impl Default for RowData {
    fn default() -> Self {
        Self::new()
    }
}

// Order-sensitive equality (columns must match in the same order).
impl PartialEq for RowData {
    fn eq(&self, other: &Self) -> bool {
        self.columns == other.columns
    }
}

impl Serialize for RowData {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.columns.len()))?;
        for (k, v) in &self.columns {
            map.serialize_entry(k.as_ref(), v)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for RowData {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = RowData;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a map of column names to values")
            }

            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut map: M,
            ) -> std::result::Result<RowData, M::Error> {
                let mut cols = Vec::with_capacity(map.size_hint().unwrap_or(0));
                while let Some((k, v)) = map.next_entry::<String, ColumnValue>()? {
                    cols.push((Arc::from(k), v));
                }
                Ok(RowData { columns: cols })
            }
        }

        deserializer.deserialize_map(Visitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rowdata_default() {
        let row = RowData::default();
        assert!(row.is_empty());
        assert_eq!(row.len(), 0);
    }

    #[test]
    fn test_rowdata_deserialize_invalid_type() {
        // Feeding a non-object type triggers the `expecting()` method.
        let err = serde_json::from_str::<RowData>("42").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("a map"),
            "Error should reference expecting(), got: {msg}"
        );
    }

    #[test]
    fn test_rowdata_deserialize_string_gives_error() {
        let err = serde_json::from_str::<RowData>("\"hello\"").unwrap_err();
        assert!(err.to_string().contains("a map"));
    }

    #[test]
    fn test_rowdata_deserialize_array_gives_error() {
        let err = serde_json::from_str::<RowData>("[1, 2, 3]").unwrap_err();
        assert!(err.to_string().contains("a map"));
    }

    #[test]
    fn test_column_value_text() {
        let v = ColumnValue::text("hello");
        assert_eq!(v.as_str(), Some("hello"));
        assert!(!v.is_null());
        assert_eq!(v.as_bytes(), b"hello");
    }

    #[test]
    fn test_column_value_null() {
        let v = ColumnValue::Null;
        assert!(v.is_null());
        assert_eq!(v.as_str(), None);
        assert_eq!(v.as_bytes(), &[] as &[u8]);
    }

    #[test]
    fn test_column_value_binary() {
        let v = ColumnValue::binary_bytes(Bytes::from_static(&[0xde, 0xad]));
        assert!(!v.is_null());
        assert_eq!(v.as_str(), None);
        assert_eq!(v.as_bytes(), &[0xde, 0xad]);
    }

    #[test]
    fn test_column_value_display() {
        assert_eq!(format!("{}", ColumnValue::Null), "NULL");
        assert_eq!(format!("{}", ColumnValue::text("hi")), "hi");
        assert_eq!(
            format!(
                "{}",
                ColumnValue::binary_bytes(Bytes::from_static(&[0xca, 0xfe]))
            ),
            "\\xcafe"
        );
    }

    #[test]
    fn test_column_value_equality() {
        assert_eq!(ColumnValue::Null, ColumnValue::Null);
        assert_eq!(ColumnValue::text("a"), ColumnValue::text("a"));
        assert_ne!(ColumnValue::text("a"), ColumnValue::text("b"));
        assert_ne!(ColumnValue::text("a"), ColumnValue::Null);
        // Cross-variant never equal
        assert_ne!(
            ColumnValue::text("a"),
            ColumnValue::binary_bytes(Bytes::copy_from_slice(b"a"))
        );
    }

    #[test]
    fn test_column_value_partial_eq_str() {
        let v = ColumnValue::text("hello");
        assert!(v == *"hello");
        assert!(v != *"world");
        assert!(ColumnValue::Null != *"hello");
    }

    #[test]
    fn test_column_value_serde_round_trip() {
        // Text
        let v = ColumnValue::text("hello");
        let json = serde_json::to_string(&v).unwrap();
        let back: ColumnValue = serde_json::from_str(&json).unwrap();
        assert_eq!(v, back);

        // Null
        let v = ColumnValue::Null;
        let json = serde_json::to_string(&v).unwrap();
        let back: ColumnValue = serde_json::from_str(&json).unwrap();
        assert_eq!(v, back);

        // Binary
        let v = ColumnValue::binary_bytes(Bytes::from_static(&[0xde, 0xad]));
        let json = serde_json::to_string(&v).unwrap();
        let back: ColumnValue = serde_json::from_str(&json).unwrap();
        assert_eq!(v, back);
    }

    #[test]
    fn test_column_value_encode_decode_round_trip() {
        use crate::buffer::BufferReader;

        let values = vec![
            ColumnValue::Null,
            ColumnValue::text("hello world"),
            ColumnValue::binary_bytes(Bytes::from_static(&[0x01, 0x02, 0x03])),
        ];

        let mut buf = BytesMut::new();
        for v in &values {
            v.encode(&mut buf);
        }

        let frozen = buf.freeze();
        let mut reader = BufferReader::new(&frozen);

        for expected in &values {
            let decoded = ColumnValue::decode(&mut reader).unwrap();
            assert_eq!(&decoded, expected);
        }
    }

    #[test]
    fn test_rowdata_encode_decode_round_trip() {
        use crate::buffer::BufferReader;

        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("42")),
            ("name", ColumnValue::text("Alice")),
            (
                "data",
                ColumnValue::binary_bytes(Bytes::from_static(&[0xff])),
            ),
            ("empty", ColumnValue::Null),
        ]);

        let mut buf = BytesMut::new();
        row.encode(&mut buf);

        let frozen = buf.freeze();
        let mut reader = BufferReader::new(&frozen);
        let decoded = RowData::decode(&mut reader).unwrap();

        assert_eq!(row, decoded);
    }

    #[test]
    fn test_rowdata_operations() {
        let mut row = RowData::with_capacity(3);
        assert!(row.is_empty());
        assert_eq!(row.len(), 0);

        row.push(Arc::from("id"), ColumnValue::text("1"));
        row.push(Arc::from("name"), ColumnValue::text("Alice"));

        assert!(!row.is_empty());
        assert_eq!(row.len(), 2);
        assert_eq!(row.get("id").unwrap(), "1");
        assert_eq!(row.get("name").unwrap(), "Alice");
        assert!(row.get("missing").is_none());

        let pairs: Vec<_> = row.iter().collect();
        assert_eq!(pairs.len(), 2);
    }

    #[test]
    fn test_rowdata_serde_round_trip() {
        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("name", ColumnValue::text("Alice")),
        ]);
        let json = serde_json::to_string(&row).unwrap();
        let back: RowData = serde_json::from_str(&json).unwrap();
        assert_eq!(row.len(), back.len());

        // Values should match (order may differ in JSON map round-trip)
        assert_eq!(back.get("id").and_then(|v| v.as_str()), Some("1"));
        assert_eq!(back.get("name").and_then(|v| v.as_str()), Some("Alice"));
    }

    #[test]
    fn test_hex_encode() {
        assert_eq!(hex_encode(&[0x00, 0x01, 0x02]), "000102");
        assert_eq!(hex_encode(&[0xff, 0xfe, 0xfd]), "fffefd");
        assert_eq!(hex_encode(&[]), "");
        assert_eq!(hex_encode(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");
    }

    #[test]
    fn test_hex_decode() {
        assert_eq!(hex_decode("000102").unwrap(), vec![0x00, 0x01, 0x02]);
        assert_eq!(
            hex_decode("deadbeef").unwrap(),
            vec![0xde, 0xad, 0xbe, 0xef]
        );
        assert_eq!(hex_decode("").unwrap(), Vec::<u8>::new());
        assert!(hex_decode("0").is_err()); // odd length
        assert!(hex_decode("zz").is_err()); // invalid chars
    }

    // --- Additional coverage tests ---

    #[test]
    fn test_hex_nibble_uppercase() {
        // Exercise the b'A'..=b'F' branch in hex_nibble
        assert_eq!(
            hex_decode("DEADBEEF").unwrap(),
            vec![0xde, 0xad, 0xbe, 0xef]
        );
        assert_eq!(hex_decode("FF00").unwrap(), vec![0xff, 0x00]);
        // Mixed case
        assert_eq!(
            hex_decode("aAbBcCdDeEfF").unwrap(),
            vec![0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff]
        );
    }

    #[test]
    fn test_hex_decode_invalid_second_char() {
        // The second nibble of a pair is invalid — exercises the low nibble error path
        assert!(hex_decode("0z").is_err());
        assert!(hex_decode("a!").is_err());
    }

    #[test]
    fn test_column_value_decode_unknown_tag() {
        use crate::buffer::BufferReader;

        let data = [0xFF]; // Unknown tag byte
        let mut reader = BufferReader::new(&data);
        let result = ColumnValue::decode(&mut reader);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("Unknown ColumnValue tag"),
            "got: {err_msg}"
        );
    }

    #[test]
    fn test_column_value_display_invalid_utf8() {
        // Text variant with invalid UTF-8 bytes exercises the Err(_) Display branch
        let v = ColumnValue::Text(Bytes::from_static(&[0xff, 0xfe, 0xfd]));
        let displayed = format!("{v}");
        assert!(displayed.contains("invalid utf-8"), "got: {displayed}");
        assert!(displayed.contains("3 bytes"), "got: {displayed}");
    }

    #[test]
    fn test_column_value_partial_eq_ref_str() {
        // Exercises the PartialEq<&str> impl (via &&str)
        let v = ColumnValue::text("hello");
        assert!(v == "hello");
        assert!(v != "world");

        let null = ColumnValue::Null;
        assert!(null != "hello");

        let binary = ColumnValue::binary_bytes(Bytes::from_static(b"hello"));
        assert!(binary != "hello");
    }

    #[test]
    fn test_column_value_serialize_non_utf8_text() {
        // Text with invalid UTF-8 falls back to tagged binary object
        let v = ColumnValue::Text(Bytes::from_static(&[0xff, 0xfe]));
        let json = serde_json::to_string(&v).unwrap();
        assert_eq!(json, r#"{"$binary":"fffe"}"#);

        // Round-trip: deserializes back as Binary (raw bytes preserved)
        let back: ColumnValue = serde_json::from_str(&json).unwrap();
        assert_eq!(back.as_bytes(), &[0xff, 0xfe]);
    }

    #[test]
    fn test_column_value_deserialize_invalid_hex() {
        // $binary with invalid hex chars triggers an error
        let json = r#"{"$binary":"ZZZZ"}"#;
        let result = serde_json::from_str::<ColumnValue>(json);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("invalid hex"), "got: {err_msg}");
    }

    #[test]
    fn test_column_value_deserialize_expecting() {
        // Feeding an unexpected type (integer) should trigger the expecting() method
        let result = serde_json::from_str::<ColumnValue>("42");
        assert!(result.is_err());
    }

    #[test]
    fn test_column_value_as_str_binary() {
        // Binary variant returns None from as_str()
        let v = ColumnValue::binary_bytes(Bytes::from_static(b"data"));
        assert_eq!(v.as_str(), None);
    }

    #[test]
    fn test_column_value_as_str_invalid_utf8() {
        // Text with invalid UTF-8 returns None from as_str()
        let v = ColumnValue::Text(Bytes::from_static(&[0xff, 0xfe]));
        assert_eq!(v.as_str(), None);
    }

    #[test]
    fn test_column_value_is_null_all_variants() {
        assert!(ColumnValue::Null.is_null());
        assert!(!ColumnValue::text("x").is_null());
        assert!(!ColumnValue::binary_bytes(Bytes::from_static(b"x")).is_null());
    }

    #[test]
    fn test_column_value_encode_decode_empty_text() {
        use crate::buffer::BufferReader;

        let v = ColumnValue::text("");
        let mut buf = BytesMut::new();
        v.encode(&mut buf);

        let frozen = buf.freeze();
        let mut reader = BufferReader::new(&frozen);
        let decoded = ColumnValue::decode(&mut reader).unwrap();
        assert_eq!(decoded, v);
        assert_eq!(decoded.as_str(), Some(""));
    }

    #[test]
    fn test_column_value_encode_decode_empty_binary() {
        use crate::buffer::BufferReader;

        let v = ColumnValue::binary_bytes(Bytes::new());
        let mut buf = BytesMut::new();
        v.encode(&mut buf);

        let frozen = buf.freeze();
        let mut reader = BufferReader::new(&frozen);
        let decoded = ColumnValue::decode(&mut reader).unwrap();
        assert_eq!(decoded, v);
        assert_eq!(decoded.as_bytes(), &[] as &[u8]);
    }

    #[test]
    fn test_column_value_clone() {
        let original = ColumnValue::text("cloned");
        let cloned = original.clone();
        assert_eq!(original, cloned);

        let original = ColumnValue::binary_bytes(Bytes::from_static(&[1, 2, 3]));
        let cloned = original.clone();
        assert_eq!(original, cloned);

        let original = ColumnValue::Null;
        let cloned = original.clone();
        assert_eq!(original, cloned);
    }

    #[test]
    fn test_column_value_debug() {
        // Exercises the derive(Debug) impl
        let v = ColumnValue::text("debug_test");
        let debug = format!("{v:?}");
        assert!(debug.contains("Text"), "got: {debug}");

        let v = ColumnValue::Null;
        let debug = format!("{v:?}");
        assert!(debug.contains("Null"), "got: {debug}");

        let v = ColumnValue::binary_bytes(Bytes::from_static(&[0xab]));
        let debug = format!("{v:?}");
        assert!(debug.contains("Binary"), "got: {debug}");
    }

    #[test]
    fn test_rowdata_from_pairs_empty() {
        let row = RowData::from_pairs(vec![]);
        assert!(row.is_empty());
        assert_eq!(row.len(), 0);
        assert!(row.get("anything").is_none());
    }

    #[test]
    fn test_rowdata_iter_with_values() {
        let row = RowData::from_pairs(vec![
            ("a", ColumnValue::text("1")),
            ("b", ColumnValue::Null),
            ("c", ColumnValue::binary_bytes(Bytes::from_static(&[0xff]))),
        ]);
        let items: Vec<_> = row.iter().collect();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].0.as_ref(), "a");
        assert_eq!(items[1].0.as_ref(), "b");
        assert!(items[1].1.is_null());
        assert_eq!(items[2].0.as_ref(), "c");
    }

    #[test]
    fn test_rowdata_equality_order_sensitive() {
        let row1 = RowData::from_pairs(vec![
            ("a", ColumnValue::text("1")),
            ("b", ColumnValue::text("2")),
        ]);
        let row2 = RowData::from_pairs(vec![
            ("b", ColumnValue::text("2")),
            ("a", ColumnValue::text("1")),
        ]);
        // Different order → not equal
        assert_ne!(row1, row2);

        // Same order → equal
        let row3 = RowData::from_pairs(vec![
            ("a", ColumnValue::text("1")),
            ("b", ColumnValue::text("2")),
        ]);
        assert_eq!(row1, row3);
    }

    #[test]
    fn test_rowdata_encode_decode_empty() {
        use crate::buffer::BufferReader;

        let row = RowData::new();
        let mut buf = BytesMut::new();
        row.encode(&mut buf);

        let frozen = buf.freeze();
        let mut reader = BufferReader::new(&frozen);
        let decoded = RowData::decode(&mut reader).unwrap();
        assert_eq!(decoded, row);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_rowdata_encode_decode_with_null_values() {
        use crate::buffer::BufferReader;

        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("description", ColumnValue::Null),
            (
                "data",
                ColumnValue::binary_bytes(Bytes::from_static(&[0x01, 0x02])),
            ),
            ("empty_text", ColumnValue::text("")),
        ]);

        let mut buf = BytesMut::new();
        row.encode(&mut buf);

        let frozen = buf.freeze();
        let mut reader = BufferReader::new(&frozen);
        let decoded = RowData::decode(&mut reader).unwrap();
        assert_eq!(decoded, row);
    }

    #[test]
    fn test_rowdata_serde_with_null_and_binary() {
        let row = RowData::from_pairs(vec![
            ("name", ColumnValue::text("Alice")),
            ("middle", ColumnValue::Null),
            (
                "blob",
                ColumnValue::binary_bytes(Bytes::from_static(&[0xca, 0xfe])),
            ),
        ]);
        let json = serde_json::to_string(&row).unwrap();
        let back: RowData = serde_json::from_str(&json).unwrap();
        assert_eq!(back.len(), row.len());
        assert_eq!(back.get("name").and_then(|v| v.as_str()), Some("Alice"));
        assert!(back.get("middle").map(|v| v.is_null()).unwrap_or(false));
        assert_eq!(
            back.get("blob").map(|v| v.as_bytes()),
            Some(&[0xca, 0xfe][..])
        );
    }

    #[test]
    fn test_text_starting_with_backslash_x_round_trips_as_text() {
        // Text that happens to start with literal `\x` followed by valid hex must survive a JSON round-trip as Text, not be silently reinterpreted as Binary.
        let original = ColumnValue::text(r"\x4142");
        let json = serde_json::to_string(&original).unwrap();
        let back: ColumnValue = serde_json::from_str(&json).unwrap();

        // The variant must stay Text, not become Binary
        assert_eq!(
            back.as_str(),
            Some(r"\x4142"),
            "Text was corrupted into Binary on JSON round-trip"
        );
        assert_eq!(original, back);
    }

    #[test]
    fn test_text_with_hex_prefix_and_odd_length_round_trips() {
        // Odd-length hex after `\x` — still valid text content
        let original = ColumnValue::text(r"\xABC");
        let json = serde_json::to_string(&original).unwrap();
        let back: ColumnValue = serde_json::from_str(&json).unwrap();
        assert_eq!(back.as_str(), Some(r"\xABC"));
        assert_eq!(original, back);
    }

    #[test]
    fn test_binary_round_trips_unambiguously() {
        // Binary values must round-trip as Binary, not collide with Text
        let original = ColumnValue::binary_bytes(Bytes::from_static(&[0x41, 0x42]));
        let json = serde_json::to_string(&original).unwrap();
        let back: ColumnValue = serde_json::from_str(&json).unwrap();
        assert_eq!(back.as_bytes(), &[0x41, 0x42]);
        assert_eq!(original, back);
    }

    #[test]
    fn test_binary_and_text_do_not_collide_in_json() {
        // The _serialized_ forms of Binary([0x41, 0x42]) and Text(r"\x4142")
        // must be different JSON values so they decode to the correct variant.
        let binary = ColumnValue::binary_bytes(Bytes::from_static(&[0x41, 0x42]));
        let text = ColumnValue::text(r"\x4142");

        let binary_json = serde_json::to_string(&binary).unwrap();
        let text_json = serde_json::to_string(&text).unwrap();

        assert_ne!(
            binary_json, text_json,
            "Binary and Text produce identical JSON — deserialization will be ambiguous"
        );
    }

    #[test]
    fn test_rowdata_with_hex_like_text_round_trips() {
        // End-to-end: a RowData containing a text column that looks like hex must survive JSON round-trip without corruption.
        let row = RowData::from_pairs(vec![
            ("hash", ColumnValue::text(r"\xdeadbeef")),
            (
                "blob",
                ColumnValue::binary_bytes(Bytes::from_static(&[0xca, 0xfe])),
            ),
            ("name", ColumnValue::text("Alice")),
        ]);
        let json = serde_json::to_string(&row).unwrap();
        let back: RowData = serde_json::from_str(&json).unwrap();

        assert_eq!(
            back.get("hash").and_then(|v| v.as_str()),
            Some(r"\xdeadbeef"),
            "Text column 'hash' was corrupted to Binary"
        );
        assert_eq!(
            back.get("blob").map(|v| v.as_bytes()),
            Some(&[0xca, 0xfe][..])
        );
        assert_eq!(back.get("name").and_then(|v| v.as_str()), Some("Alice"));
    }

    #[test]
    fn test_rowdata_debug() {
        let row = RowData::from_pairs(vec![("x", ColumnValue::text("y"))]);
        let debug = format!("{row:?}");
        assert!(debug.contains("RowData"), "got: {debug}");
    }

    #[test]
    fn test_rowdata_clone() {
        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("val", ColumnValue::Null),
        ]);
        let cloned = row.clone();
        assert_eq!(row, cloned);
    }
}
