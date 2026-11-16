//! Column value types for PostgreSQL logical replication
//!
//! This module provides [`ColumnValue`] and [`RowData`] — the core data types
//! used to represent column-level data from PostgreSQL's logical replication
//! protocol. Both types use zero-copy [`bytes::Bytes`] internally and support
//! a compact binary wire format for efficient serialisation.

use crate::buffer::BufferReader;
use crate::error::{ReplicationError, Result};
use crate::prelude::*;
use bytes::{Bytes, BytesMut};
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

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
pub(crate) fn hex_decode(hex: &str) -> core::result::Result<Vec<u8>, &'static str> {
    if !hex.len().is_multiple_of(2) {
        return Err("odd hex length");
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    let bytes = hex.as_bytes();
    for &[high, low] in bytes.as_chunks::<2>().0 {
        let high = hex_nibble(high).ok_or("invalid hex char")?;
        let low = hex_nibble(low).ok_or("invalid hex char")?;
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
/// A format whose serializer reports `is_human_readable() == false` (CBOR,
/// MessagePack, bincode, postcard) gets a compact form instead: `None` for
/// `Null`, otherwise `Some` of a `Text` or `Binary` variant holding the raw
/// bytes. The JSON form needs `deserialize_any`, which bincode and postcard do
/// not implement; the compact one does not, and `Text` that is not UTF-8
/// stays `Text`.
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
            Self::Text(b) => core::str::from_utf8(b).ok(),
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

impl core::fmt::Display for ColumnValue {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Text(b) => match core::str::from_utf8(b) {
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

/// What a format that is not human-readable sees for a non-null [`ColumnValue`]:
/// borrowed to write, owned to read. `Null` is the `None` around it, so a reader
/// tells the two apart without `deserialize_any`. The variant order is the
/// postcard and bincode wire format.
#[derive(Serialize, Deserialize)]
#[serde(rename = "ColumnValue")]
enum Compact<B> {
    Text(B),
    Binary(B),
}

impl Serialize for ColumnValue {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> core::result::Result<S::Ok, S::Error> {
        if !serializer.is_human_readable() {
            return match self {
                Self::Null => serializer.serialize_none(),
                Self::Text(b) => serializer.serialize_some(&Compact::Text(b)),
                Self::Binary(b) => serializer.serialize_some(&Compact::Binary(b)),
            };
        }
        match self {
            Self::Null => serializer.serialize_none(),
            Self::Text(b) => match core::str::from_utf8(b) {
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
    ) -> core::result::Result<Self, D::Error> {
        if !deserializer.is_human_readable() {
            return Ok(match Option::<Compact<Bytes>>::deserialize(deserializer)? {
                None => ColumnValue::Null,
                Some(Compact::Text(b)) => ColumnValue::Text(b),
                Some(Compact::Binary(b)) => ColumnValue::Binary(b),
            });
        }

        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = ColumnValue;

            fn expecting(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
                f.write_str(r#"a string, null, or {"$binary": "hex..."}"#)
            }

            fn visit_none<E: serde::de::Error>(self) -> core::result::Result<ColumnValue, E> {
                Ok(ColumnValue::Null)
            }

            fn visit_unit<E: serde::de::Error>(self) -> core::result::Result<ColumnValue, E> {
                Ok(ColumnValue::Null)
            }

            fn visit_some<D: serde::Deserializer<'de>>(
                self,
                deserializer: D,
            ) -> core::result::Result<ColumnValue, D::Error> {
                deserializer.deserialize_any(self)
            }

            fn visit_str<E: serde::de::Error>(
                self,
                v: &str,
            ) -> core::result::Result<ColumnValue, E> {
                Ok(ColumnValue::Text(Bytes::copy_from_slice(v.as_bytes())))
            }

            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut map: M,
            ) -> core::result::Result<ColumnValue, M::Error> {
                use serde::de::Error;
                let key: String = map.next_key()?.ok_or_else(|| {
                    M::Error::custom(r#"expected a "$binary", "Text" or "Binary" key"#)
                })?;
                match key.as_str() {
                    "$binary" => {
                        let hex: String = map.next_value()?;
                        let bytes = hex_decode(&hex).map_err(|e| {
                            M::Error::custom(format!("invalid hex in $binary: {e}"))
                        })?;
                        Ok(ColumnValue::Binary(Bytes::from(bytes)))
                    }
                    "Text" => map.next_value().map(ColumnValue::Text),
                    "Binary" => map.next_value().map(ColumnValue::Binary),
                    _ => Err(M::Error::custom(format!(
                        r#"unknown key "{key}", expected "$binary", "Text" or "Binary""#
                    ))),
                }
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
///
/// pgoutput encodes an out-of-line (TOASTed) column that an UPDATE did not
/// touch as `'u'`, which this crate drops from the row entirely. A SQL NULL is
/// always materialised as [`ColumnValue::Null`], so "absent from the row" is an
/// unambiguous encoding of "unchanged — reuse the value already in the target".
///
/// A sink must therefore build partial `UPDATE` statements from what the row
/// actually contains (iterate, or check [`get`](Self::get) for `None`).
/// Substituting NULL for a missing column overwrites good data with nulls.
/// Setting the table's `REPLICA IDENTITY FULL` does not change this; only
/// `ALTER TABLE ... SET (toast_tuple_target = ...)` or keeping the column
/// inline avoids it.
#[derive(Debug, Clone, Eq)]
pub struct RowData {
    columns: SmallVec<[(Arc<str>, ColumnValue); 8]>,
}

impl RowData {
    /// Create an empty `RowData`.
    #[inline]
    pub fn new() -> Self {
        Self {
            columns: SmallVec::new(),
        }
    }

    /// Create an empty `RowData` with pre-allocated capacity.
    #[inline]
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            columns: SmallVec::with_capacity(cap),
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

    /// Access the underlying column slice (crate-internal).
    #[inline]
    pub(crate) fn as_columns(&self) -> &[(Arc<str>, ColumnValue)] {
        &self.columns
    }

    /// Deserialize this row into a user-defined type.
    ///
    /// PostgreSQL sends column values as text strings via the pgoutput plugin.
    /// This method parses those text values into the fields of `T` using serde
    /// deserialization with automatic text-to-type coercion.
    ///
    /// # Type Coercion
    ///
    /// | PostgreSQL text | Rust type |
    /// |---|---|
    /// | `"42"` | `i8`, `i16`, `i32`, `i64`, `u8`, `u16`, `u32`, `u64` |
    /// | `"3.14"` | `f32`, `f64` |
    /// | `"t"` / `"f"` | `bool` |
    /// | `"hello"` | `String` |
    /// | NULL | `Option<T>` (yields `None`) |
    ///
    /// # Example
    ///
    /// ```
    /// use pg_walstream::{RowData, ColumnValue};
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct User {
    ///     id: u32,
    ///     name: String,
    /// }
    ///
    /// let row = RowData::from_pairs(vec![
    ///     ("id", ColumnValue::text("42")),
    ///     ("name", ColumnValue::text("Alice")),
    /// ]);
    ///
    /// let user: User = row.deserialize_into().unwrap();
    /// assert_eq!(user.id, 42);
    /// assert_eq!(user.name, "Alice");
    /// ```
    pub fn deserialize_into<T: serde::de::DeserializeOwned>(&self) -> crate::error::Result<T> {
        T::deserialize(crate::deserializer::RowDataDeserializer::new(self))
    }

    /// Zero-copy deserialize: borrow column data directly from this row.
    ///
    /// Unlike [`deserialize_into`](Self::deserialize_into), the target `T` may
    /// borrow from the row (`&'de str`, `&'de [u8]`, `Cow<'de, str>`), so a
    /// string-heavy row deserializes with zero per-field allocation. The row
    /// (and the underlying protocol buffer it slices) must outlive `T`.
    ///
    /// `String`/owned fields still work — they copy, exactly as with
    /// `deserialize_into`.
    ///
    /// # Caveat: borrowed bytes are raw pgoutput text
    ///
    /// A `&'de [u8]` / `&'de str` borrows the column's *raw* pgoutput bytes. For a `bytea` column (delivered as text like `\x00ff`) that means the escaped text, **not** the decoded binary — decode it yourself, or use an owned type with a deserializer that unescapes. This matches the existing `deserialize_into` semantics; borrowing only removes the copy.
    ///
    /// # Example
    ///
    /// ```
    /// use pg_walstream::{RowData, ColumnValue};
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Row<'a> {
    ///     id: u32,
    ///     name: &'a str,
    /// }
    ///
    /// let row = RowData::from_pairs(vec![
    ///     ("id", ColumnValue::text("42")),
    ///     ("name", ColumnValue::text("Alice")),
    /// ]);
    /// let r: Row = row.deserialize_borrowed().unwrap();
    /// assert_eq!(r.id, 42);
    /// assert_eq!(r.name, "Alice");
    /// ```
    pub fn deserialize_borrowed<'de, T: Deserialize<'de>>(&'de self) -> crate::error::Result<T> {
        T::deserialize(crate::deserializer::RowDataDeserializer::new(self))
    }

    /// Lenient deserialization: returns a [`TryDeserializeResult`] whose `value`
    /// always contains a populated `T` (with per-type defaults substituted for
    /// columns that fail to parse) and whose `errors` lists every failing field.
    ///
    /// Use this when you want to process partial rows — e.g. log bad columns
    /// or quarantine rows that exceed an error threshold, while still getting
    /// the successfully-parsed columns of `T`.
    ///
    /// Substituted defaults on field-level failure:
    /// - numeric types → `0`
    /// - `bool` → `false`
    /// - `String` / `&str` → `""`
    /// - `char` → `'\0'`
    /// - `Option<T>` → `None` when the column is NULL; otherwise inner parse is attempted
    /// - bytes / byte_buf → empty slice
    ///
    /// Structural errors (unsupported shapes like nested structs / sequences)
    /// and enum-variant mismatches still return an outer `Err`.
    ///
    /// [`TryDeserializeResult`]: crate::TryDeserializeResult
    ///
    /// # Example
    ///
    /// ```
    /// use pg_walstream::{RowData, ColumnValue};
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct User { id: u32, score: i32 }
    ///
    /// let row = RowData::from_pairs(vec![
    ///     ("id", ColumnValue::text("42")),
    ///     ("score", ColumnValue::text("not_a_number")),
    /// ]);
    ///
    /// let result = row.try_deserialize_into::<User>().unwrap();
    /// assert_eq!(result.value.id, 42);
    /// assert_eq!(result.value.score, 0); // default substituted
    /// assert_eq!(result.errors.len(), 1);
    /// assert_eq!(result.errors[0].field, "score");
    /// ```
    pub fn try_deserialize_into<T: serde::de::DeserializeOwned>(
        &self,
    ) -> crate::error::Result<crate::deserializer::TryDeserializeResult<T>> {
        crate::deserializer::try_deserialize_row(self)
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
        let mut columns = SmallVec::with_capacity(count);
        for _ in 0..count {
            let name_len = reader.read_u16()? as usize;
            let name_bytes = reader.read_bytes(name_len)?;
            let name = core::str::from_utf8(&name_bytes)
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
    ) -> core::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.columns.len()))?;
        for (k, v) in &self.columns {
            map.serialize_entry(k.as_ref(), v)?;
        }
        map.end()
    }
}

/// The most columns a `RowData` reserves room for before it has read any of them. PostgreSQL allows
/// 1600 per table (`MaxHeapAttributeNumber`), and the length a format reports for a map comes from
/// its input, so it is clamped rather than trusted: a corrupt length must not size the allocation.
const MAX_PREALLOCATED_COLUMNS: usize = 1600;

impl<'de> Deserialize<'de> for RowData {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> core::result::Result<Self, D::Error> {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = RowData;

            fn expecting(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
                f.write_str("a map of column names to values")
            }

            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut map: M,
            ) -> core::result::Result<RowData, M::Error> {
                let hint = map.size_hint().unwrap_or(0).min(MAX_PREALLOCATED_COLUMNS);
                let mut cols = SmallVec::with_capacity(hint);
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

    #[test]
    fn test_rowdata_wide_exceeds_inline_capacity() {
        static NAMES: [&str; 12] = [
            "c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11",
        ];
        let mut row = RowData::with_capacity(12);
        for (i, &name) in NAMES.iter().enumerate() {
            let val = format!("val_{i}");
            row.push(Arc::from(name), ColumnValue::text(&val));
        }
        assert_eq!(row.len(), 12);
        assert_eq!(row.get("c0").unwrap(), "val_0");
        assert_eq!(row.get("c11").unwrap(), "val_11");
    }

    #[test]
    fn test_rowdata_wide_encode_decode_round_trip() {
        use crate::buffer::BufferReader;

        static NAMES: [&str; 12] = [
            "c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11",
        ];
        let mut row = RowData::with_capacity(12);
        for (i, &name) in NAMES.iter().enumerate() {
            let val = if i % 3 == 0 {
                ColumnValue::Null
            } else if i % 3 == 1 {
                let s = format!("text_{i}");
                ColumnValue::text(&s)
            } else {
                ColumnValue::binary_bytes(Bytes::from(vec![i as u8; 4]))
            };
            row.push(Arc::from(name), val);
        }

        let mut buf = BytesMut::new();
        row.encode(&mut buf);

        let frozen = buf.freeze();
        let mut reader = BufferReader::new(&frozen);
        let decoded = RowData::decode(&mut reader).unwrap();

        assert_eq!(row, decoded);
        assert_eq!(decoded.len(), 12);
    }

    #[test]
    fn serde_json_round_trips_null_text_binary() {
        for cv in [
            ColumnValue::Null,
            ColumnValue::text("hello"),
            ColumnValue::Binary(Bytes::from_static(&[0u8, 1, 2, 255])),
        ] {
            let json = serde_json::to_string(&cv).unwrap();
            let back: ColumnValue = serde_json::from_str(&json).unwrap();
            assert_eq!(cv, back, "round trip failed via {json}");
        }
    }

    #[test]
    fn serde_json_reader_accepts_the_compact_map() {
        let text: ColumnValue = serde_json::from_str(r#"{"Text":"abc"}"#).unwrap();
        assert_eq!(text, ColumnValue::text("abc"));

        let binary: ColumnValue = serde_json::from_str(r#"{"Binary":[1,2,255]}"#).unwrap();
        assert_eq!(
            binary,
            ColumnValue::Binary(Bytes::from_static(&[1, 2, 255]))
        );
    }

    #[test]
    fn serde_text_invalid_utf8_round_trips_as_binary() {
        let cv = ColumnValue::Text(Bytes::from_static(&[0xff, 0xfe]));
        let json = serde_json::to_string(&cv).unwrap();
        assert!(json.contains("$binary"));
        let back: ColumnValue = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ColumnValue::Binary(Bytes::from_static(&[0xff, 0xfe])));
    }

    #[test]
    fn serde_column_value_rejects_bad_input() {
        assert!(serde_json::from_str::<ColumnValue>(r#"{"nope":"x"}"#).is_err());
        assert!(serde_json::from_str::<ColumnValue>(r#"{"$binary":"zz"}"#).is_err());
        assert!(serde_json::from_str::<ColumnValue>("42").is_err());
        for bad in [
            r#"{}"#,
            r#"{"Text":5}"#,
            r#"{"Binary":"zz","Text":"a"}"#,
            r#"{"Text":"a","Binary":"b"}"#,
            r#"{"Null":null}"#,
        ] {
            assert!(
                serde_json::from_str::<ColumnValue>(bad).is_err(),
                "accepted {bad}"
            );
        }

        let message = serde_json::from_str::<ColumnValue>(r#"{"Other":"x"}"#)
            .unwrap_err()
            .to_string();
        assert!(
            message.contains("Other") && message.contains("Text") && message.contains("Binary"),
            "{message}"
        );
    }

    #[test]
    fn serde_visit_unit_is_null() {
        use serde::de::value::{Error as ValueError, UnitDeserializer};
        let cv = ColumnValue::deserialize(UnitDeserializer::<ValueError>::new()).unwrap();
        assert_eq!(cv, ColumnValue::Null);
    }

    #[test]
    fn rowdata_serde_json_round_trip() {
        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("name", ColumnValue::text("alice")),
            (
                "blob",
                ColumnValue::Binary(Bytes::from_static(&[1u8, 2, 3])),
            ),
        ]);
        let json = serde_json::to_string(&row).unwrap();
        let back: RowData = serde_json::from_str(&json).unwrap();
        assert_eq!(row, back);
        assert!(serde_json::from_str::<RowData>("42").is_err());
    }

    /// What a single column is in postcard: `None` for NULL, else `Some` of the variant and its bytes.
    #[test]
    fn postcard_layout_of_a_column_value_is_pinned() {
        let cases: [(ColumnValue, &[u8]); 4] = [
            (ColumnValue::Null, &[0x00]),
            (ColumnValue::text("1"), &[0x01, 0x00, 0x01, b'1']),
            (
                ColumnValue::binary_bytes(Bytes::from_static(&[0xde])),
                &[0x01, 0x01, 0x01, 0xde],
            ),
            (
                ColumnValue::text_bytes(Bytes::from_static(&[0xff, 0xfe])),
                &[0x01, 0x00, 0x02, 0xff, 0xfe],
            ),
        ];
        for (value, layout) in cases {
            assert_eq!(postcard::to_stdvec(&value).unwrap(), layout, "{value:?}");
            assert_eq!(postcard::from_bytes::<ColumnValue>(layout).unwrap(), value);
        }
    }

    /// A `MapAccess` that claims to hold `claimed` entries and holds none.
    struct ClaimsEntries {
        claimed: usize,
    }

    impl<'de> serde::de::MapAccess<'de> for ClaimsEntries {
        type Error = serde::de::value::Error;

        fn next_key_seed<K: serde::de::DeserializeSeed<'de>>(
            &mut self,
            _seed: K,
        ) -> core::result::Result<Option<K::Value>, Self::Error> {
            Ok(None)
        }

        fn next_value_seed<V: serde::de::DeserializeSeed<'de>>(
            &mut self,
            _seed: V,
        ) -> core::result::Result<V::Value, Self::Error> {
            unreachable!("there is no key to take a value for")
        }

        fn size_hint(&self) -> Option<usize> {
            Some(self.claimed)
        }
    }

    /// The length a format reports for a map comes from the input, so it must not size an allocation.
    /// `usize::MAX` entries would panic with a capacity overflow, and a smaller claim that no machine can
    /// satisfy would abort the whole process.
    #[test]
    fn rowdata_deserialize_does_not_allocate_for_the_entries_a_map_claims() {
        use serde::de::value::MapAccessDeserializer;

        for claimed in [0, 1, MAX_PREALLOCATED_COLUMNS, usize::MAX] {
            let row = RowData::deserialize(MapAccessDeserializer::new(ClaimsEntries { claimed }))
                .unwrap_or_else(|e| panic!("a map claiming {claimed} entries: {e}"));
            assert!(row.is_empty(), "claimed {claimed}");
        }
    }

    /// The same, from the bytes: postcard writes the number of entries first, here `u64::MAX`.
    #[test]
    fn a_postcard_row_claiming_u64_max_columns_is_an_error() {
        let bytes = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01];
        assert!(postcard::from_bytes::<RowData>(&bytes).is_err());
    }

    /// The cap only limits what is reserved up front: a row with more columns than it keeps all of them,
    /// in order, whether the format reports its length (postcard) or does not (JSON).
    #[test]
    fn rowdata_wider_than_the_preallocation_cap_keeps_every_column() {
        let mut row = RowData::with_capacity(MAX_PREALLOCATED_COLUMNS + 100);
        for i in 0..MAX_PREALLOCATED_COLUMNS + 100 {
            let value = if i % 7 == 0 {
                ColumnValue::Null
            } else {
                ColumnValue::text(&format!("v{i}"))
            };
            row.push(Arc::from(format!("c{i}")), value);
        }

        let back: RowData = postcard::from_bytes(&postcard::to_stdvec(&row).unwrap()).unwrap();
        assert_eq!(back, row, "postcard");
        let back: RowData = serde_json::from_str(&serde_json::to_string(&row).unwrap()).unwrap();
        assert_eq!(back, row, "json");
    }
}
