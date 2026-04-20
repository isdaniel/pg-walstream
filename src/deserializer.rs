//! Custom serde [`Deserializer`][serde::Deserializer] for converting [`RowData`] into user-defined types.
//!
//! PostgreSQL's logical replication protocol (pgoutput) sends column values as
//! text strings. This module provides a [`Deserializer`][serde::Deserializer] that parses those text
//! representations into the target Rust types automatically.
//!
//! # Example
//!
//! ```
//! use pg_walstream::{RowData, ColumnValue};
//! use serde::Deserialize;
//!
//! #[derive(Deserialize, Debug, PartialEq)]
//! struct User {
//!     id: u32,
//!     username: String,
//! }
//!
//! let row = RowData::from_pairs(vec![
//!     ("id", ColumnValue::text("42")),
//!     ("username", ColumnValue::text("alice")),
//! ]);
//!
//! let user: User = row.deserialize_into().unwrap();
//! assert_eq!(user.id, 42);
//! assert_eq!(user.username, "alice");
//! ```

use crate::column_value::{ColumnValue, RowData};
use crate::error::ReplicationError;
use serde::de::{self, DeserializeSeed, MapAccess, Visitor};
use std::sync::Arc;

/// Parse PostgreSQL's boolean text vocabulary.
///
/// PostgreSQL's pgoutput emits `t`/`f`; `BOOL` input also accepts
/// `true`/`false`, `1`/`0`, `on`/`off`, `yes`/`no`. Returns `None` on
/// unrecognized input so callers can surface the offending token.
///
/// Operates on bytes — pgoutput guarantees ASCII for boolean text, so
/// callers can skip UTF-8 validation on the hot path.
#[inline]
fn parse_pg_bool(b: &[u8]) -> Option<bool> {
    if b.len() == 1 {
        return match b[0] {
            b't' | b'1' => Some(true),
            b'f' | b'0' => Some(false),
            _ => None,
        };
    }
    match b {
        b"true" | b"on" | b"yes" => Some(true),
        b"false" | b"off" | b"no" => Some(false),
        _ => None,
    }
}

/// Fast ASCII signed-integer parser with overflow detection.
///
/// Accepts the same syntax as `i64::from_str`: optional `+`/`-`, then one
/// or more ASCII digits. Returns `None` on empty input, non-digit bytes,
/// or out-of-range values for `T`. Skips UTF-8 validation since pgoutput
/// emits ASCII for all integer types.
#[inline]
fn parse_int_signed<T>(b: &[u8]) -> Option<T>
where
    T: TryFrom<i64>,
{
    if b.is_empty() {
        return None;
    }
    let (negative, digits) = match b[0] {
        b'-' => (true, &b[1..]),
        b'+' => (false, &b[1..]),
        _ => (false, b),
    };
    if digits.is_empty() {
        return None;
    }
    let mut acc: i64 = 0;
    if negative {
        for &c in digits {
            let d = c.wrapping_sub(b'0');
            if d > 9 {
                return None;
            }
            acc = acc.checked_mul(10)?.checked_sub(d as i64)?;
        }
    } else {
        for &c in digits {
            let d = c.wrapping_sub(b'0');
            if d > 9 {
                return None;
            }
            acc = acc.checked_mul(10)?.checked_add(d as i64)?;
        }
    }
    T::try_from(acc).ok()
}

/// Fast ASCII unsigned-integer parser with overflow detection.
///
/// Accepts an optional leading `+`. Rejects `-` and non-digits. Uses a
/// `u64` accumulator and narrows to `T`.
#[inline]
fn parse_int_unsigned<T>(b: &[u8]) -> Option<T>
where
    T: TryFrom<u64>,
{
    if b.is_empty() {
        return None;
    }
    let digits = if b[0] == b'+' { &b[1..] } else { b };
    if digits.is_empty() {
        return None;
    }
    let mut acc: u64 = 0;
    for &c in digits {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        acc = acc.checked_mul(10)?.checked_add(d as u64)?;
    }
    T::try_from(acc).ok()
}

/// Render a byte slice as a string for error messages, lossy on non-UTF-8.
#[inline]
fn lossy_token(b: &[u8]) -> std::borrow::Cow<'_, str> {
    String::from_utf8_lossy(b)
}

// ---------------------------------------------------------------------------
// RowDataDeserializer — top-level Deserializer for RowData
// ---------------------------------------------------------------------------

/// A serde [`Deserializer`][serde::Deserializer] that treats a [`RowData`] as a map of column names
/// to values and deserializes it into a user-defined struct.
pub struct RowDataDeserializer<'a> {
    row: &'a RowData,
}

impl<'a> RowDataDeserializer<'a> {
    /// Create a new deserializer from a [`RowData`] reference.
    pub fn new(row: &'a RowData) -> Self {
        Self { row }
    }
}

impl<'de, 'a> de::Deserializer<'de> for RowDataDeserializer<'a> {
    type Error = ReplicationError;

    #[inline]
    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_map(visitor)
    }

    #[inline]
    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        let columns = self.row.as_columns();
        let map = RowDataMapAccess {
            iter: columns.iter(),
            current_value: None,
        };
        visitor.visit_map(map)
    }

    #[inline]
    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.deserialize_map(visitor)
    }

    // Forward all other types to deserialize_any since RowData is fundamentally a map.
    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct enum identifier ignored_any
    }
}

// ---------------------------------------------------------------------------
// RowDataMapAccess — iterates over RowData columns as map entries
// ---------------------------------------------------------------------------

struct RowDataMapAccess<'a> {
    iter: std::slice::Iter<'a, (Arc<str>, ColumnValue)>,
    current_value: Option<&'a ColumnValue>,
}

impl<'de, 'a> MapAccess<'de> for RowDataMapAccess<'a> {
    type Error = ReplicationError;

    #[inline]
    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, Self::Error> {
        match self.iter.next() {
            Some((key, value)) => {
                self.current_value = Some(value);
                let key_de = serde::de::value::StrDeserializer::new(key.as_ref());
                seed.deserialize(key_de).map(Some)
            }
            None => Ok(None),
        }
    }

    #[inline]
    fn next_value_seed<V: DeserializeSeed<'de>>(
        &mut self,
        seed: V,
    ) -> Result<V::Value, Self::Error> {
        let value = self
            .current_value
            .take()
            .expect("next_value_seed called before next_key_seed");
        seed.deserialize(ColumnValueDeserializer { value })
    }

    #[inline]
    fn size_hint(&self) -> Option<usize> {
        let (lower, upper) = self.iter.size_hint();
        Some(upper.unwrap_or(lower))
    }
}

// ---------------------------------------------------------------------------
// ColumnValueDeserializer — deserializes a single ColumnValue
// ---------------------------------------------------------------------------

struct ColumnValueDeserializer<'a> {
    value: &'a ColumnValue,
}

impl<'a> ColumnValueDeserializer<'a> {
    /// Get the text content or return an error for the given target type.
    #[inline]
    fn text_or_err(&self, target: &str) -> Result<&'a str, ReplicationError> {
        match self.value {
            ColumnValue::Text(b) => std::str::from_utf8(b).map_err(|e| {
                ReplicationError::deserialize(format!("invalid UTF-8 for {target}: {e}"))
            }),
            ColumnValue::Null => Err(ReplicationError::deserialize(format!(
                "cannot deserialize NULL as {target} (use Option<{target}>)"
            ))),
            ColumnValue::Binary(_) => Err(ReplicationError::deserialize(format!(
                "cannot deserialize binary column as {target}"
            ))),
        }
    }

    /// Like [`text_or_err`] but returns raw bytes — skips the UTF-8 pass.
    /// Use only for paths where the parser itself rejects non-ASCII bytes
    /// (numeric, bool). String-shaped paths must use `text_or_err`.
    #[inline]
    fn bytes_or_err(&self, target: &str) -> Result<&'a [u8], ReplicationError> {
        match self.value {
            ColumnValue::Text(b) => Ok(b.as_ref()),
            ColumnValue::Null => Err(ReplicationError::deserialize(format!(
                "cannot deserialize NULL as {target} (use Option<{target}>)"
            ))),
            ColumnValue::Binary(_) => Err(ReplicationError::deserialize(format!(
                "cannot deserialize binary column as {target}"
            ))),
        }
    }

    /// Parse text as a numeric type via the std `FromStr` (used for floats).
    #[inline]
    fn parse_text<T: std::str::FromStr>(&self, type_name: &str) -> Result<T, ReplicationError>
    where
        T::Err: std::fmt::Display,
    {
        let s = self.text_or_err(type_name)?;
        s.parse::<T>().map_err(|e| {
            ReplicationError::deserialize(format!(
                "failed to parse '{}' as {}: {}",
                s, type_name, e
            ))
        })
    }

    /// Fast signed-int parse from bytes; emits an error string equivalent
    /// to the std parser (token + type name) so existing tests still match.
    #[inline]
    fn parse_signed<T>(&self, type_name: &str) -> Result<T, ReplicationError>
    where
        T: TryFrom<i64>,
    {
        let b = self.bytes_or_err(type_name)?;
        parse_int_signed::<T>(b).ok_or_else(|| numeric_parse_error(b, type_name))
    }

    /// Fast unsigned-int parse from bytes.
    #[inline]
    fn parse_unsigned<T>(&self, type_name: &str) -> Result<T, ReplicationError>
    where
        T: TryFrom<u64>,
    {
        let b = self.bytes_or_err(type_name)?;
        parse_int_unsigned::<T>(b).ok_or_else(|| numeric_parse_error(b, type_name))
    }
}

/// Build a numeric-parse error. Performs UTF-8 validation only on the cold
/// path so that invalid-UTF-8 inputs still surface the expected diagnostic.
#[cold]
#[inline(never)]
fn numeric_parse_error(b: &[u8], type_name: &str) -> ReplicationError {
    match std::str::from_utf8(b) {
        Ok(s) => ReplicationError::deserialize(format!("failed to parse '{s}' as {type_name}")),
        Err(e) => ReplicationError::deserialize(format!("invalid UTF-8 for {type_name}: {e}")),
    }
}

impl<'de, 'a> de::Deserializer<'de> for ColumnValueDeserializer<'a> {
    type Error = ReplicationError;

    #[inline]
    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.value {
            ColumnValue::Null => visitor.visit_none(),
            ColumnValue::Text(b) => match std::str::from_utf8(b) {
                Ok(s) => visitor.visit_str(s),
                Err(_) => visitor.visit_bytes(b),
            },
            ColumnValue::Binary(b) => visitor.visit_bytes(b),
        }
    }

    #[inline]
    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        let b = self.bytes_or_err("bool")?;
        let val = parse_pg_bool(b).ok_or_else(|| {
            ReplicationError::deserialize(format!(
                "cannot parse '{}' as bool (expected t/f/true/false/1/0/on/off/yes/no)",
                lossy_token(b)
            ))
        })?;
        visitor.visit_bool(val)
    }

    #[inline]
    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_i8(self.parse_signed::<i8>("i8")?)
    }

    #[inline]
    fn deserialize_i16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_i16(self.parse_signed::<i16>("i16")?)
    }

    #[inline]
    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_i32(self.parse_signed::<i32>("i32")?)
    }

    #[inline]
    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_i64(self.parse_signed::<i64>("i64")?)
    }

    #[inline]
    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_u8(self.parse_unsigned::<u8>("u8")?)
    }

    #[inline]
    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_u16(self.parse_unsigned::<u16>("u16")?)
    }

    #[inline]
    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_u32(self.parse_unsigned::<u32>("u32")?)
    }

    #[inline]
    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_u64(self.parse_unsigned::<u64>("u64")?)
    }

    #[inline]
    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_f32(self.parse_text("f32")?)
    }

    #[inline]
    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_f64(self.parse_text("f64")?)
    }

    #[inline]
    fn deserialize_char<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        let s = self.text_or_err("char")?;
        let mut chars = s.chars();
        match (chars.next(), chars.next()) {
            (Some(c), None) => visitor.visit_char(c),
            _ => Err(ReplicationError::deserialize(format!(
                "expected single char, got '{s}'"
            ))),
        }
    }

    #[inline]
    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        let s = self.text_or_err("str")?;
        visitor.visit_str(s)
    }

    #[inline]
    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        let s = self.text_or_err("String")?;
        visitor.visit_str(s)
    }

    #[inline]
    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.value {
            ColumnValue::Binary(b) | ColumnValue::Text(b) => visitor.visit_bytes(b),
            ColumnValue::Null => Err(ReplicationError::deserialize(
                "cannot deserialize NULL as bytes",
            )),
        }
    }

    #[inline]
    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.value {
            ColumnValue::Binary(b) | ColumnValue::Text(b) => visitor.visit_byte_buf(b.to_vec()),
            ColumnValue::Null => Err(ReplicationError::deserialize(
                "cannot deserialize NULL as byte_buf",
            )),
        }
    }

    #[inline]
    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.value {
            ColumnValue::Null => visitor.visit_none(),
            _ => visitor.visit_some(self),
        }
    }

    #[inline]
    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.value {
            ColumnValue::Null => visitor.visit_unit(),
            _ => Err(ReplicationError::deserialize("expected NULL for unit type")),
        }
    }

    #[inline]
    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.deserialize_unit(visitor)
    }

    #[inline]
    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        visitor.visit_newtype_struct(self)
    }

    #[inline]
    fn deserialize_seq<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(ReplicationError::deserialize(
            "sequences are not supported in RowData deserialization",
        ))
    }

    #[inline]
    fn deserialize_tuple<V: Visitor<'de>>(
        self,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(ReplicationError::deserialize(
            "tuples are not supported in RowData deserialization",
        ))
    }

    #[inline]
    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(ReplicationError::deserialize(
            "tuple structs are not supported in RowData deserialization",
        ))
    }

    #[inline]
    fn deserialize_map<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(ReplicationError::deserialize(
            "nested maps are not supported in RowData deserialization",
        ))
    }

    #[inline]
    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(ReplicationError::deserialize(
            "nested structs are not supported in RowData deserialization",
        ))
    }

    #[inline]
    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        let s = self.text_or_err("enum")?;
        visitor.visit_enum(serde::de::value::StrDeserializer::<ReplicationError>::new(
            s,
        ))
    }

    #[inline]
    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_str(visitor)
    }

    #[inline]
    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_unit()
    }
}

// ---------------------------------------------------------------------------
// Lenient (try_*) deserialization
// ---------------------------------------------------------------------------

/// A single field-level deserialization failure, surfaced by  [`RowData::try_deserialize_into`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldError {
    /// Name of the column that failed to parse.
    pub field: String,
    /// Human-readable description of the failure.
    pub message: String,
}

impl std::fmt::Display for FieldError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "field '{}': {}", self.field, self.message)
    }
}

/// Result of a lenient ([`RowData::try_deserialize_into`]) deserialization.
///
/// The `value` is always returned — fields that fail to parse are populated
/// with type-appropriate defaults (numeric 0, empty string, `false`, `None`,
/// empty bytes). The `errors` vector lists every field that triggered a
/// fallback so callers can log, count, or reject the row.
#[derive(Debug)]
pub struct TryDeserializeResult<T> {
    /// Deserialized value (with defaults substituted for failed fields).
    pub value: T,
    /// Per-field errors, in column order.
    pub errors: Vec<FieldError>,
}

impl<T> TryDeserializeResult<T> {
    /// Returns `true` when every field parsed successfully (no errors recorded).
    #[inline]
    pub fn is_clean(&self) -> bool {
        self.errors.is_empty()
    }

    /// Convert into a standard `Result`: `Ok(value)` if no errors, else `Err(errors)`.
    pub fn into_result(self) -> std::result::Result<T, Vec<FieldError>> {
        if self.errors.is_empty() {
            Ok(self.value)
        } else {
            Err(self.errors)
        }
    }
}

/// Shared lenient context passed down through the deserializer chain.
pub(crate) struct LenientCtx {
    errors: std::cell::RefCell<Vec<FieldError>>,
}

impl LenientCtx {
    fn new() -> Self {
        Self {
            errors: std::cell::RefCell::new(Vec::new()),
        }
    }

    fn push(&self, field: &str, message: String) {
        self.errors.borrow_mut().push(FieldError {
            field: field.to_string(),
            message,
        });
    }

    fn into_errors(self) -> Vec<FieldError> {
        self.errors.into_inner()
    }
}

pub(crate) struct LenientRowDataDeserializer<'a> {
    row: &'a RowData,
    ctx: &'a LenientCtx,
}

impl<'a> LenientRowDataDeserializer<'a> {
    pub(crate) fn new(row: &'a RowData, ctx: &'a LenientCtx) -> Self {
        Self { row, ctx }
    }
}

impl<'de, 'a> de::Deserializer<'de> for LenientRowDataDeserializer<'a> {
    type Error = ReplicationError;

    #[inline]
    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_map(visitor)
    }

    #[inline]
    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        let map = LenientMapAccess {
            iter: self.row.as_columns().iter(),
            current: None,
            ctx: self.ctx,
        };
        visitor.visit_map(map)
    }

    #[inline]
    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.deserialize_map(visitor)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct enum identifier ignored_any
    }
}

struct LenientMapAccess<'a> {
    iter: std::slice::Iter<'a, (Arc<str>, ColumnValue)>,
    current: Option<(&'a str, &'a ColumnValue)>,
    ctx: &'a LenientCtx,
}

impl<'de, 'a> MapAccess<'de> for LenientMapAccess<'a> {
    type Error = ReplicationError;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, Self::Error> {
        match self.iter.next() {
            Some((key, value)) => {
                self.current = Some((key.as_ref(), value));
                let key_de = serde::de::value::StrDeserializer::new(key.as_ref());
                seed.deserialize(key_de).map(Some)
            }
            None => Ok(None),
        }
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(
        &mut self,
        seed: V,
    ) -> Result<V::Value, Self::Error> {
        let (field, value) = self
            .current
            .take()
            .expect("next_value_seed called before next_key_seed");
        seed.deserialize(LenientColumnValueDeserializer {
            value,
            field,
            ctx: self.ctx,
        })
    }

    fn size_hint(&self) -> Option<usize> {
        let (lower, upper) = self.iter.size_hint();
        Some(upper.unwrap_or(lower))
    }
}

struct LenientColumnValueDeserializer<'a> {
    value: &'a ColumnValue,
    field: &'a str,
    ctx: &'a LenientCtx,
}

impl<'a> LenientColumnValueDeserializer<'a> {
    /// Get the text content, recording a field error and returning `None` on failure.
    fn try_text(&self, target: &str) -> Option<&'a str> {
        match self.value {
            ColumnValue::Text(b) => match std::str::from_utf8(b) {
                Ok(s) => Some(s),
                Err(e) => {
                    self.ctx
                        .push(self.field, format!("invalid UTF-8 for {target}: {e}"));
                    None
                }
            },
            ColumnValue::Null => {
                self.ctx.push(
                    self.field,
                    format!("cannot deserialize NULL as {target} (use Option<{target}>)"),
                );
                None
            }
            ColumnValue::Binary(_) => {
                self.ctx.push(
                    self.field,
                    format!("cannot deserialize binary column as {target}"),
                );
                None
            }
        }
    }

    /// Parse text into `T`; record an error and return the type's default on failure.
    fn parse_or_default<T>(&self, target: &str) -> T
    where
        T: std::str::FromStr + Default,
        T::Err: std::fmt::Display,
    {
        match self.try_text(target) {
            Some(s) => s.parse::<T>().unwrap_or_else(|e| {
                self.ctx.push(
                    self.field,
                    format!("failed to parse '{s}' as {target}: {e}"),
                );
                T::default()
            }),
            None => T::default(),
        }
    }
}

impl<'de, 'a> de::Deserializer<'de> for LenientColumnValueDeserializer<'a> {
    type Error = ReplicationError;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        // Mirror strict deserialize_any; these paths don't error.
        match self.value {
            ColumnValue::Null => visitor.visit_none(),
            ColumnValue::Text(b) => match std::str::from_utf8(b) {
                Ok(s) => visitor.visit_str(s),
                Err(_) => visitor.visit_bytes(b),
            },
            ColumnValue::Binary(b) => visitor.visit_bytes(b),
        }
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        let v = match self.try_text("bool") {
            Some(s) => parse_pg_bool(s.as_bytes()).unwrap_or_else(|| {
                self.ctx
                    .push(self.field, format!("cannot parse '{s}' as bool"));
                false
            }),
            None => false,
        };
        visitor.visit_bool(v)
    }

    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_i8(self.parse_or_default::<i8>("i8"))
    }
    fn deserialize_i16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_i16(self.parse_or_default::<i16>("i16"))
    }
    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_i32(self.parse_or_default::<i32>("i32"))
    }
    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_i64(self.parse_or_default::<i64>("i64"))
    }
    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_u8(self.parse_or_default::<u8>("u8"))
    }
    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_u16(self.parse_or_default::<u16>("u16"))
    }
    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_u32(self.parse_or_default::<u32>("u32"))
    }
    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_u64(self.parse_or_default::<u64>("u64"))
    }
    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_f32(self.parse_or_default::<f32>("f32"))
    }
    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_f64(self.parse_or_default::<f64>("f64"))
    }

    fn deserialize_char<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        let c = match self.try_text("char") {
            Some(s) => {
                let mut chars = s.chars();
                match (chars.next(), chars.next()) {
                    (Some(c), None) => c,
                    _ => {
                        self.ctx
                            .push(self.field, format!("expected single char, got '{s}'"));
                        '\0'
                    }
                }
            }
            None => '\0',
        };
        visitor.visit_char(c)
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        let s = self.try_text("str").unwrap_or("");
        visitor.visit_str(s)
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        let s = self.try_text("String").unwrap_or("");
        visitor.visit_str(s)
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.value {
            ColumnValue::Binary(b) | ColumnValue::Text(b) => visitor.visit_bytes(b),
            ColumnValue::Null => {
                self.ctx
                    .push(self.field, "cannot deserialize NULL as bytes".to_string());
                visitor.visit_bytes(&[])
            }
        }
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.value {
            ColumnValue::Binary(b) | ColumnValue::Text(b) => visitor.visit_byte_buf(b.to_vec()),
            ColumnValue::Null => {
                self.ctx.push(
                    self.field,
                    "cannot deserialize NULL as byte_buf".to_string(),
                );
                visitor.visit_byte_buf(Vec::new())
            }
        }
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.value {
            ColumnValue::Null => visitor.visit_none(),
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        if !matches!(self.value, ColumnValue::Null) {
            self.ctx
                .push(self.field, "expected NULL for unit type".to_string());
        }
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.deserialize_unit(visitor)
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(ReplicationError::deserialize(
            "sequences are not supported in RowData deserialization",
        ))
    }

    fn deserialize_tuple<V: Visitor<'de>>(
        self,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(ReplicationError::deserialize(
            "tuples are not supported in RowData deserialization",
        ))
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(ReplicationError::deserialize(
            "tuple structs are not supported in RowData deserialization",
        ))
    }

    fn deserialize_map<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(ReplicationError::deserialize(
            "nested maps are not supported in RowData deserialization",
        ))
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(ReplicationError::deserialize(
            "nested structs are not supported in RowData deserialization",
        ))
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        // Enum has no sensible per-field default; propagate strict behavior.
        let s = match self.value {
            ColumnValue::Text(b) => std::str::from_utf8(b).map_err(|e| {
                ReplicationError::deserialize(format!("invalid UTF-8 for enum: {e}"))
            })?,
            ColumnValue::Null => {
                return Err(ReplicationError::deserialize(format!(
                    "cannot deserialize NULL as enum (field '{}')",
                    self.field
                )))
            }
            ColumnValue::Binary(_) => {
                return Err(ReplicationError::deserialize(format!(
                    "cannot deserialize binary as enum (field '{}')",
                    self.field
                )))
            }
        };
        visitor.visit_enum(serde::de::value::StrDeserializer::<ReplicationError>::new(
            s,
        ))
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_unit()
    }
}

pub(crate) fn try_deserialize_row<T: serde::de::DeserializeOwned>(
    row: &RowData,
) -> Result<TryDeserializeResult<T>, ReplicationError> {
    let ctx = LenientCtx::new();
    let de = LenientRowDataDeserializer::new(row, &ctx);
    let value = T::deserialize(de)?;
    Ok(TryDeserializeResult {
        value,
        errors: ctx.into_errors(),
    })
}

#[cfg(test)]
mod tests {
    use crate::column_value::{ColumnValue, RowData};
    use crate::types::{ChangeEvent, Lsn, ReplicaIdentity};
    use bytes::Bytes;
    use serde::Deserialize;
    use std::sync::Arc;

    // -- Basic struct deserialization -----------------------------------------

    #[derive(Debug, Deserialize, PartialEq)]
    struct UserModel {
        id: u32,
        username: String,
    }

    #[test]
    fn test_basic_struct() {
        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("42")),
            ("username", ColumnValue::text("alice")),
        ]);
        let user: UserModel = row.deserialize_into().unwrap();
        assert_eq!(user.id, 42);
        assert_eq!(user.username, "alice");
    }

    // -- All numeric types ---------------------------------------------------

    #[derive(Debug, Deserialize, PartialEq)]
    struct AllNumerics {
        a_i8: i8,
        a_i16: i16,
        a_i32: i32,
        a_i64: i64,
        a_u8: u8,
        a_u16: u16,
        a_u32: u32,
        a_u64: u64,
        a_f32: f32,
        a_f64: f64,
    }

    #[test]
    fn test_all_numeric_types() {
        let row = RowData::from_pairs(vec![
            ("a_i8", ColumnValue::text("-1")),
            ("a_i16", ColumnValue::text("-200")),
            ("a_i32", ColumnValue::text("-300000")),
            ("a_i64", ColumnValue::text("-4000000000")),
            ("a_u8", ColumnValue::text("255")),
            ("a_u16", ColumnValue::text("65535")),
            ("a_u32", ColumnValue::text("4294967295")),
            ("a_u64", ColumnValue::text("18446744073709551615")),
            ("a_f32", ColumnValue::text("1.5")),
            ("a_f64", ColumnValue::text("2.5")),
        ]);
        let v: AllNumerics = row.deserialize_into().unwrap();
        assert_eq!(v.a_i8, -1);
        assert_eq!(v.a_i16, -200);
        assert_eq!(v.a_i32, -300000);
        assert_eq!(v.a_i64, -4000000000);
        assert_eq!(v.a_u8, 255);
        assert_eq!(v.a_u16, 65535);
        assert_eq!(v.a_u32, 4294967295);
        assert_eq!(v.a_u64, 18446744073709551615);
        assert!((v.a_f32 - 1.5).abs() < 0.001);
        assert!((v.a_f64 - 2.5).abs() < 0.0000001);
    }

    // -- Boolean parsing (PostgreSQL sends "t"/"f") --------------------------

    #[derive(Debug, Deserialize, PartialEq)]
    struct Booleans {
        a: bool,
        b: bool,
        c: bool,
        d: bool,
        e: bool,
        f: bool,
    }

    #[test]
    fn test_boolean_parsing() {
        let row = RowData::from_pairs(vec![
            ("a", ColumnValue::text("t")),
            ("b", ColumnValue::text("f")),
            ("c", ColumnValue::text("true")),
            ("d", ColumnValue::text("false")),
            ("e", ColumnValue::text("1")),
            ("f", ColumnValue::text("0")),
        ]);
        let v: Booleans = row.deserialize_into().unwrap();
        assert!(v.a);
        assert!(!v.b);
        assert!(v.c);
        assert!(!v.d);
        assert!(v.e);
        assert!(!v.f);
    }

    #[test]
    fn test_boolean_invalid() {
        let row = RowData::from_pairs(vec![("a", ColumnValue::text("maybe"))]);
        #[derive(Debug, Deserialize)]
        struct B {
            #[allow(dead_code)]
            a: bool,
        }
        let result: crate::error::Result<B> = row.deserialize_into();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("maybe"), "got: {msg}");
    }

    // -- PostgreSQL full boolean input vocabulary --------------------------------

    #[derive(Debug, Deserialize, PartialEq)]
    struct BoolExtended {
        a: bool,
        b: bool,
        c: bool,
        d: bool,
    }

    #[test]
    fn test_boolean_on_off_yes_no() {
        let row = RowData::from_pairs(vec![
            ("a", ColumnValue::text("on")),
            ("b", ColumnValue::text("off")),
            ("c", ColumnValue::text("yes")),
            ("d", ColumnValue::text("no")),
        ]);
        let v: BoolExtended = row.deserialize_into().unwrap();
        assert!(v.a);
        assert!(!v.b);
        assert!(v.c);
        assert!(!v.d);
    }

    // -- Option fields (nullable columns) ------------------------------------

    #[derive(Debug, Deserialize, PartialEq)]
    struct WithOptions {
        id: u32,
        name: Option<String>,
        score: Option<f64>,
    }

    #[test]
    fn test_option_some() {
        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("name", ColumnValue::text("alice")),
            ("score", ColumnValue::text("99.5")),
        ]);
        let v: WithOptions = row.deserialize_into().unwrap();
        assert_eq!(v.id, 1);
        assert_eq!(v.name, Some("alice".to_string()));
        assert_eq!(v.score, Some(99.5));
    }

    #[test]
    fn test_option_none() {
        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("name", ColumnValue::Null),
            ("score", ColumnValue::Null),
        ]);
        let v: WithOptions = row.deserialize_into().unwrap();
        assert_eq!(v.id, 1);
        assert_eq!(v.name, None);
        assert_eq!(v.score, None);
    }

    // -- Error cases ---------------------------------------------------------

    #[test]
    fn test_null_into_non_option() {
        let row = RowData::from_pairs(vec![("id", ColumnValue::Null)]);
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            id: u32,
        }
        let result: crate::error::Result<S> = row.deserialize_into();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("NULL"), "got: {msg}");
    }

    #[test]
    fn test_type_mismatch() {
        let row = RowData::from_pairs(vec![("id", ColumnValue::text("not_a_number"))]);
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            id: u32,
        }
        let result: crate::error::Result<S> = row.deserialize_into();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("not_a_number"), "got: {msg}");
    }

    #[test]
    fn test_overflow() {
        let row = RowData::from_pairs(vec![("val", ColumnValue::text("99999"))]);
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            val: i8,
        }
        let result: crate::error::Result<S> = row.deserialize_into();
        assert!(result.is_err());
    }

    // -- Extra columns ignored -----------------------------------------------

    #[test]
    fn test_extra_columns_ignored() {
        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("username", ColumnValue::text("bob")),
            ("extra_col", ColumnValue::text("ignored")),
        ]);
        let user: UserModel = row.deserialize_into().unwrap();
        assert_eq!(user.id, 1);
        assert_eq!(user.username, "bob");
    }

    // -- Serde attributes ----------------------------------------------------

    #[derive(Debug, Deserialize, PartialEq)]
    struct Renamed {
        #[serde(rename = "user_id")]
        id: u32,
        #[serde(rename = "user_name")]
        name: String,
    }

    #[test]
    fn test_serde_rename() {
        let row = RowData::from_pairs(vec![
            ("user_id", ColumnValue::text("10")),
            ("user_name", ColumnValue::text("charlie")),
        ]);
        let v: Renamed = row.deserialize_into().unwrap();
        assert_eq!(v.id, 10);
        assert_eq!(v.name, "charlie");
    }

    #[derive(Debug, Deserialize, PartialEq)]
    struct WithDefaults {
        id: u32,
        #[serde(default)]
        count: u32,
        #[serde(default = "default_name")]
        name: String,
    }

    fn default_name() -> String {
        "unknown".to_string()
    }

    #[test]
    fn test_serde_default() {
        let row = RowData::from_pairs(vec![("id", ColumnValue::text("5"))]);
        let v: WithDefaults = row.deserialize_into().unwrap();
        assert_eq!(v.id, 5);
        assert_eq!(v.count, 0);
        assert_eq!(v.name, "unknown");
    }

    // -- Char deserialization ------------------------------------------------

    #[test]
    fn test_char_field() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct S {
            c: char,
        }
        let row = RowData::from_pairs(vec![("c", ColumnValue::text("A"))]);
        let v: S = row.deserialize_into().unwrap();
        assert_eq!(v.c, 'A');
    }

    #[test]
    fn test_char_too_long() {
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            c: char,
        }
        let row = RowData::from_pairs(vec![("c", ColumnValue::text("AB"))]);
        let result: crate::error::Result<S> = row.deserialize_into();
        assert!(result.is_err());
    }

    // -- Enum deserialization (simple unit variants) -------------------------

    #[derive(Debug, Deserialize, PartialEq)]
    enum Status {
        Active,
        Inactive,
    }

    #[derive(Debug, Deserialize, PartialEq)]
    struct WithEnum {
        id: u32,
        status: Status,
    }

    #[test]
    fn test_enum_unit_variant() {
        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("status", ColumnValue::text("Active")),
        ]);
        let v: WithEnum = row.deserialize_into().unwrap();
        assert_eq!(v.id, 1);
        assert_eq!(v.status, Status::Active);
    }

    // -- Binary columns ------------------------------------------------------

    #[test]
    fn test_binary_column_to_string_errors() {
        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            (
                "username",
                ColumnValue::binary_bytes(Bytes::from_static(b"binary")),
            ),
        ]);
        let result: crate::error::Result<UserModel> = row.deserialize_into();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("binary"), "got: {msg}");
    }

    // -- ChangeEvent convenience methods -------------------------------------

    #[test]
    fn test_deserialize_insert() {
        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("username", ColumnValue::text("alice")),
        ]);
        let event = ChangeEvent::insert("public", "users", 1, row, Lsn::new(100));
        let user: UserModel = event.deserialize_insert().unwrap();
        assert_eq!(user.id, 1);
        assert_eq!(user.username, "alice");
    }

    #[test]
    fn test_deserialize_insert_wrong_event() {
        let ts = chrono::Utc::now();
        let event = ChangeEvent::begin(1, Lsn::new(100), ts, Lsn::new(100));
        let result: crate::error::Result<UserModel> = event.deserialize_insert();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("begin"), "got: {msg}");
    }

    #[test]
    fn test_deserialize_update_both() {
        let old = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("username", ColumnValue::text("alice")),
        ]);
        let new = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("username", ColumnValue::text("bob")),
        ]);
        let event = ChangeEvent::update(
            "public",
            "users",
            1,
            Some(old),
            new,
            ReplicaIdentity::Full,
            vec![Arc::from("id")],
            Lsn::new(200),
        );
        let (old, new): (Option<UserModel>, UserModel) = event.deserialize_update().unwrap();
        assert_eq!(old.unwrap().username, "alice");
        assert_eq!(new.username, "bob");
    }

    #[test]
    fn test_deserialize_update_both_no_old() {
        let new = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("username", ColumnValue::text("bob")),
        ]);
        let event = ChangeEvent::update(
            "public",
            "users",
            1,
            None,
            new,
            ReplicaIdentity::Default,
            vec![Arc::from("id")],
            Lsn::new(200),
        );
        let (old, new): (Option<UserModel>, UserModel) = event.deserialize_update().unwrap();
        assert!(old.is_none());
        assert_eq!(new.username, "bob");
    }

    #[test]
    fn test_deserialize_update_both_wrong_event() {
        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("username", ColumnValue::text("alice")),
        ]);
        let event = ChangeEvent::insert("public", "users", 1, row, Lsn::new(100));
        let result: crate::error::Result<(Option<UserModel>, UserModel)> =
            event.deserialize_update();
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_delete() {
        let old = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("username", ColumnValue::text("alice")),
        ]);
        let event = ChangeEvent::delete(
            "public",
            "users",
            1,
            old,
            ReplicaIdentity::Full,
            vec![Arc::from("id")],
            Lsn::new(300),
        );
        let user: UserModel = event.deserialize_delete().unwrap();
        assert_eq!(user.id, 1);
        assert_eq!(user.username, "alice");
    }

    #[test]
    fn test_deserialize_data_for_all_dml() {
        // Insert
        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("username", ColumnValue::text("alice")),
        ]);
        let event = ChangeEvent::insert("public", "users", 1, row, Lsn::new(100));
        let user: UserModel = event.deserialize_data().unwrap();
        assert_eq!(user.username, "alice");

        // Update
        let new = RowData::from_pairs(vec![
            ("id", ColumnValue::text("2")),
            ("username", ColumnValue::text("bob")),
        ]);
        let event = ChangeEvent::update(
            "public",
            "users",
            1,
            None,
            new,
            ReplicaIdentity::Default,
            vec![],
            Lsn::new(200),
        );
        let user: UserModel = event.deserialize_data().unwrap();
        assert_eq!(user.username, "bob");

        // Delete
        let old = RowData::from_pairs(vec![
            ("id", ColumnValue::text("3")),
            ("username", ColumnValue::text("charlie")),
        ]);
        let event = ChangeEvent::delete(
            "public",
            "users",
            1,
            old,
            ReplicaIdentity::Full,
            vec![],
            Lsn::new(300),
        );
        let user: UserModel = event.deserialize_data().unwrap();
        assert_eq!(user.username, "charlie");
    }

    #[test]
    fn test_deserialize_data_non_dml_errors() {
        let ts = chrono::Utc::now();
        let event = ChangeEvent::begin(1, Lsn::new(100), ts, Lsn::new(100));
        let result: crate::error::Result<UserModel> = event.deserialize_data();
        assert!(result.is_err());
    }

    // -- Empty RowData -------------------------------------------------------

    #[derive(Debug, Deserialize, PartialEq)]
    struct AllOptional {
        #[serde(default)]
        id: Option<u32>,
        #[serde(default)]
        name: Option<String>,
    }

    #[test]
    fn test_empty_row_with_defaults() {
        let row = RowData::new();
        let v: AllOptional = row.deserialize_into().unwrap();
        assert_eq!(v.id, None);
        assert_eq!(v.name, None);
    }

    // -- Newtype struct ------------------------------------------------------

    #[derive(Debug, Deserialize, PartialEq)]
    struct UserId(u32);

    #[derive(Debug, Deserialize, PartialEq)]
    struct WithNewtype {
        id: UserId,
        name: String,
    }

    #[test]
    fn test_newtype_struct() {
        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("42")),
            ("name", ColumnValue::text("alice")),
        ]);
        let v: WithNewtype = row.deserialize_into().unwrap();
        assert_eq!(v.id, UserId(42));
        assert_eq!(v.name, "alice");
    }

    // -- Empty string --------------------------------------------------------

    #[test]
    fn test_empty_string_field() {
        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("username", ColumnValue::text("")),
        ]);
        let user: UserModel = row.deserialize_into().unwrap();
        assert_eq!(user.id, 1);
        assert_eq!(user.username, "");
    }

    // -- Binary bytes / byte_buf deserialization ------------------------------

    #[test]
    fn test_bytes_from_binary_column() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct S {
            #[serde(with = "serde_bytes")]
            data: Vec<u8>,
        }
        let row = RowData::from_pairs(vec![(
            "data",
            ColumnValue::binary_bytes(Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef])),
        )]);
        let v: S = row.deserialize_into().unwrap();
        assert_eq!(v.data, vec![0xde, 0xad, 0xbe, 0xef]);
    }

    #[test]
    fn test_bytes_from_text_column() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct S {
            #[serde(with = "serde_bytes")]
            data: Vec<u8>,
        }
        let row = RowData::from_pairs(vec![("data", ColumnValue::text("hello"))]);
        let v: S = row.deserialize_into().unwrap();
        assert_eq!(v.data, b"hello");
    }

    #[test]
    fn test_bytes_null_errors() {
        #[derive(Debug, Deserialize)]
        struct S {
            #[serde(with = "serde_bytes")]
            #[allow(dead_code)]
            data: Vec<u8>,
        }
        let row = RowData::from_pairs(vec![("data", ColumnValue::Null)]);
        let result: crate::error::Result<S> = row.deserialize_into();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("NULL"), "got: {msg}");
    }

    // -- deserialize_any paths -----------------------------------------------

    #[test]
    fn test_deserialize_any_null() {
        // deserialize_any on Null visits none, which works for Option<T>
        #[derive(Debug, Deserialize, PartialEq)]
        struct S {
            #[serde(default)]
            val: Option<String>,
        }
        let row = RowData::from_pairs(vec![("val", ColumnValue::Null)]);
        let v: S = row.deserialize_into().unwrap();
        assert_eq!(v.val, None);
    }

    #[test]
    fn test_deserialize_any_binary() {
        // deserialize_any on Binary visits bytes; test the path indirectly
        // through ignored_any (extra columns with binary data are skipped)
        let row = RowData::from_pairs(vec![
            ("a", ColumnValue::text("hello")),
            ("b", ColumnValue::binary_bytes(Bytes::from_static(&[0x01]))),
        ]);
        #[derive(Debug, Deserialize, PartialEq)]
        struct S {
            a: String,
        }
        let v: S = row.deserialize_into().unwrap();
        assert_eq!(v.a, "hello");
    }

    // -- Unit / unit_struct deserialization -----------------------------------

    #[test]
    fn test_unit_from_null() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct S {
            val: (),
        }
        let row = RowData::from_pairs(vec![("val", ColumnValue::Null)]);
        let v: S = row.deserialize_into().unwrap();
        assert_eq!(v.val, ());
    }

    #[test]
    fn test_unit_from_non_null_errors() {
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            val: (),
        }
        let row = RowData::from_pairs(vec![("val", ColumnValue::text("something"))]);
        let result: crate::error::Result<S> = row.deserialize_into();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("NULL"), "got: {msg}");
    }

    // -- Unsupported type errors (seq, tuple, tuple_struct, map, struct on column) --

    #[test]
    fn test_seq_not_supported() {
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            vals: Vec<u32>,
        }
        let row = RowData::from_pairs(vec![("vals", ColumnValue::text("[1,2,3]"))]);
        let result: crate::error::Result<S> = row.deserialize_into();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("sequence"), "got: {msg}");
    }

    #[test]
    fn test_nested_struct_not_supported() {
        #[derive(Debug, Deserialize)]
        struct Inner {
            #[allow(dead_code)]
            x: u32,
        }
        #[derive(Debug, Deserialize)]
        struct Outer {
            #[allow(dead_code)]
            inner: Inner,
        }
        let row = RowData::from_pairs(vec![("inner", ColumnValue::text("{\"x\":1}"))]);
        let result: crate::error::Result<Outer> = row.deserialize_into();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("nested struct"), "got: {msg}");
    }

    // -- text_or_err error paths (NULL for bool, Binary for numeric) ----------

    #[test]
    fn test_null_bool_errors() {
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            flag: bool,
        }
        let row = RowData::from_pairs(vec![("flag", ColumnValue::Null)]);
        let result: crate::error::Result<S> = row.deserialize_into();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("NULL"), "got: {msg}");
    }

    #[test]
    fn test_binary_numeric_errors() {
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            val: i32,
        }
        let row = RowData::from_pairs(vec![(
            "val",
            ColumnValue::binary_bytes(Bytes::from_static(&[0x01, 0x02])),
        )]);
        let result: crate::error::Result<S> = row.deserialize_into();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("binary"), "got: {msg}");
    }

    #[test]
    fn test_binary_bool_errors() {
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            flag: bool,
        }
        let row = RowData::from_pairs(vec![(
            "flag",
            ColumnValue::binary_bytes(Bytes::from_static(&[0x01])),
        )]);
        let result: crate::error::Result<S> = row.deserialize_into();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("binary"), "got: {msg}");
    }

    #[test]
    fn test_binary_string_errors() {
        let row = RowData::from_pairs(vec![(
            "username",
            ColumnValue::binary_bytes(Bytes::from_static(b"data")),
        )]);
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            username: String,
        }
        let result: crate::error::Result<S> = row.deserialize_into();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("binary"), "got: {msg}");
    }

    #[test]
    fn test_null_string_errors() {
        let row = RowData::from_pairs(vec![("name", ColumnValue::Null)]);
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            name: String,
        }
        let result: crate::error::Result<S> = row.deserialize_into();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("NULL"), "got: {msg}");
    }

    // -- Char edge cases -----------------------------------------------------

    #[test]
    fn test_char_empty_string_errors() {
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            c: char,
        }
        let row = RowData::from_pairs(vec![("c", ColumnValue::text(""))]);
        let result: crate::error::Result<S> = row.deserialize_into();
        assert!(result.is_err());
    }

    #[test]
    fn test_char_null_errors() {
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            c: char,
        }
        let row = RowData::from_pairs(vec![("c", ColumnValue::Null)]);
        let result: crate::error::Result<S> = row.deserialize_into();
        assert!(result.is_err());
    }

    // -- Enum edge cases ----------------------------------------------------

    #[test]
    fn test_enum_unknown_variant_errors() {
        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("status", ColumnValue::text("Unknown")),
        ]);
        let result: crate::error::Result<WithEnum> = row.deserialize_into();
        assert!(result.is_err());
    }

    #[test]
    fn test_enum_null_errors() {
        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("status", ColumnValue::Null),
        ]);
        let result: crate::error::Result<WithEnum> = row.deserialize_into();
        assert!(result.is_err());
    }

    // -- ChangeEvent wrong-event-type errors ---------------------------------

    #[test]
    fn test_deserialize_delete_wrong_event() {
        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("username", ColumnValue::text("alice")),
        ]);
        let event = ChangeEvent::insert("public", "users", 1, row, Lsn::new(100));
        let result: crate::error::Result<UserModel> = event.deserialize_delete();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("insert"), "got: {msg}");
    }

    #[test]
    fn test_deserialize_data_truncate_errors() {
        let event = ChangeEvent::truncate(vec![Arc::from("public.users")], Lsn::new(100));
        let result: crate::error::Result<UserModel> = event.deserialize_data();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("truncate"), "got: {msg}");
    }

    // -- Option<bool> and Option<i32> (Option wrapping non-string types) -----

    #[test]
    fn test_option_bool_some() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct S {
            flag: Option<bool>,
        }
        let row = RowData::from_pairs(vec![("flag", ColumnValue::text("t"))]);
        let v: S = row.deserialize_into().unwrap();
        assert_eq!(v.flag, Some(true));
    }

    #[test]
    fn test_option_bool_none() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct S {
            flag: Option<bool>,
        }
        let row = RowData::from_pairs(vec![("flag", ColumnValue::Null)]);
        let v: S = row.deserialize_into().unwrap();
        assert_eq!(v.flag, None);
    }

    #[test]
    fn test_option_i32() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct S {
            val: Option<i32>,
        }
        let row = RowData::from_pairs(vec![("val", ColumnValue::text("-42"))]);
        let v: S = row.deserialize_into().unwrap();
        assert_eq!(v.val, Some(-42));
    }

    // -- Multiple rows deserialization (same struct, different data) ----------

    #[test]
    fn test_multiple_rows() {
        let rows = [
            RowData::from_pairs(vec![
                ("id", ColumnValue::text("1")),
                ("username", ColumnValue::text("alice")),
            ]),
            RowData::from_pairs(vec![
                ("id", ColumnValue::text("2")),
                ("username", ColumnValue::text("bob")),
            ]),
            RowData::from_pairs(vec![
                ("id", ColumnValue::text("3")),
                ("username", ColumnValue::text("charlie")),
            ]),
        ];
        let users: Vec<UserModel> = rows.iter().map(|r| r.deserialize_into().unwrap()).collect();
        assert_eq!(users.len(), 3);
        assert_eq!(users[0].id, 1);
        assert_eq!(users[1].username, "bob");
        assert_eq!(users[2].id, 3);
    }

    // -- Negative floats and special values ----------------------------------

    #[test]
    fn test_negative_float() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct S {
            val: f64,
        }
        let row = RowData::from_pairs(vec![("val", ColumnValue::text("-0.001"))]);
        let v: S = row.deserialize_into().unwrap();
        assert!((v.val - (-0.001)).abs() < f64::EPSILON);
    }

    // -- RowData::as_columns test -------------------------------------------

    #[test]
    fn test_as_columns() {
        let row = RowData::from_pairs(vec![
            ("a", ColumnValue::text("1")),
            ("b", ColumnValue::Null),
        ]);
        let cols = row.as_columns();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].0.as_ref(), "a");
        assert_eq!(cols[1].0.as_ref(), "b");
        assert!(cols[1].1.is_null());
    }

    // -- RowDataDeserializer::new public constructor -------------------------

    #[test]
    fn test_row_data_deserializer_direct_use() {
        use crate::deserializer::RowDataDeserializer;
        use serde::Deserialize;

        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("99")),
            ("username", ColumnValue::text("direct")),
        ]);
        let de = RowDataDeserializer::new(&row);
        let user = UserModel::deserialize(de).unwrap();
        assert_eq!(user.id, 99);
        assert_eq!(user.username, "direct");
    }

    // -- serde(flatten) / HashMap deserialization ----------------------------

    #[test]
    fn test_deserialize_into_hashmap() {
        use std::collections::HashMap;
        let row = RowData::from_pairs(vec![
            ("name", ColumnValue::text("test")),
            ("value", ColumnValue::text("42")),
        ]);
        // RowData can be deserialized into a HashMap<String, String> via deserialize_any
        // because visit_str yields string values for Text columns
        let map: HashMap<String, String> = row.deserialize_into().unwrap();
        assert_eq!(map.get("name").unwrap(), "test");
        assert_eq!(map.get("value").unwrap(), "42");
    }

    // -- deserialize_byte_buf paths -----------------------------------------

    #[test]
    fn test_byte_buf_from_binary() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct S {
            #[serde(with = "serde_bytes")]
            data: Vec<u8>,
        }
        let row = RowData::from_pairs(vec![(
            "data",
            ColumnValue::binary_bytes(Bytes::from_static(&[1, 2, 3])),
        )]);
        let v: S = row.deserialize_into().unwrap();
        assert_eq!(v.data, vec![1, 2, 3]);
    }

    #[test]
    fn test_byte_buf_null_errors() {
        #[derive(Debug, Deserialize)]
        struct S {
            #[serde(with = "serde_bytes")]
            #[allow(dead_code)]
            data: Vec<u8>,
        }
        let row = RowData::from_pairs(vec![("data", ColumnValue::Null)]);
        let result: crate::error::Result<S> = row.deserialize_into();
        assert!(result.is_err());
    }

    // -- unsupported types at column level ------------------------------------

    #[test]
    fn test_tuple_not_supported() {
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            pair: (u32, u32),
        }
        let row = RowData::from_pairs(vec![("pair", ColumnValue::text("1,2"))]);
        let result: crate::error::Result<S> = row.deserialize_into();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("tuple"), "got: {msg}");
    }

    #[test]
    fn test_tuple_struct_not_supported() {
        #[derive(Debug, Deserialize)]
        struct Pair(#[allow(dead_code)] u32, #[allow(dead_code)] u32);
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            p: Pair,
        }
        let row = RowData::from_pairs(vec![("p", ColumnValue::text("1,2"))]);
        let result: crate::error::Result<S> = row.deserialize_into();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("tuple struct"), "got: {msg}");
    }

    #[test]
    fn test_nested_map_not_supported() {
        use std::collections::HashMap;
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            m: HashMap<String, String>,
        }
        let row = RowData::from_pairs(vec![("m", ColumnValue::text("{}"))]);
        let result: crate::error::Result<S> = row.deserialize_into();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("nested map"), "got: {msg}");
    }

    // -- unit_struct from Null -------------------------------------------------

    #[test]
    fn test_unit_struct_from_null() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Marker;
        #[derive(Debug, Deserialize, PartialEq)]
        struct S {
            m: Marker,
        }
        let row = RowData::from_pairs(vec![("m", ColumnValue::Null)]);
        let v: S = row.deserialize_into().unwrap();
        assert_eq!(v.m, Marker);
    }

    // -- identifier / str paths via flatten ------------------------------------

    #[test]
    fn test_str_via_hashmap() {
        use std::collections::HashMap;
        let row = RowData::from_pairs(vec![("k", ColumnValue::text("v"))]);
        let map: HashMap<String, String> = row.deserialize_into().unwrap();
        assert_eq!(map.get("k").unwrap(), "v");
    }

    // -- deserialize_any on Null produces None (via HashMap<String, Option<String>>) --

    #[test]
    fn test_any_null_in_hashmap() {
        use std::collections::HashMap;
        let row = RowData::from_pairs(vec![
            ("a", ColumnValue::text("x")),
            ("b", ColumnValue::Null),
        ]);
        let map: HashMap<String, Option<String>> = row.deserialize_into().unwrap();
        assert_eq!(map.get("a").unwrap(), &Some("x".to_string()));
        assert_eq!(map.get("b").unwrap(), &None);
    }

    // -- boolean every accepted form -------------------------------------------

    #[test]
    fn test_boolean_invalid_single_char() {
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            a: bool,
        }
        let row = RowData::from_pairs(vec![("a", ColumnValue::text("x"))]);
        let result: crate::error::Result<S> = row.deserialize_into();
        assert!(result.is_err());
    }

    // -- i64/u64 boundary ------------------------------------------------------

    #[test]
    fn test_integer_boundaries() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct S {
            i: i64,
            u: u64,
        }
        let row = RowData::from_pairs(vec![
            ("i", ColumnValue::text("-9223372036854775808")),
            ("u", ColumnValue::text("18446744073709551615")),
        ]);
        let v: S = row.deserialize_into().unwrap();
        assert_eq!(v.i, i64::MIN);
        assert_eq!(v.u, u64::MAX);
    }

    // -- RowDataDeserializer::new direct + deserialize_map forwarding ---------

    #[test]
    fn test_deserializer_deserialize_any() {
        use crate::deserializer::RowDataDeserializer;
        use std::collections::HashMap;
        let row = RowData::from_pairs(vec![("k", ColumnValue::text("v"))]);
        let de = RowDataDeserializer::new(&row);
        // HashMap triggers deserialize_map, which goes through deserialize_any fallback path
        let m: HashMap<String, String> = HashMap::deserialize(de).unwrap();
        assert_eq!(m.get("k").unwrap(), "v");
    }

    // -- Invalid UTF-8 in Text column ----------------------------------------

    #[test]
    fn test_invalid_utf8_text_as_string_errors() {
        // Text column containing invalid UTF-8 (lone continuation byte)
        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            (
                "username",
                ColumnValue::Text(Bytes::from_static(&[0xff, 0xfe])),
            ),
        ]);
        let result: crate::error::Result<UserModel> = row.deserialize_into();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("invalid UTF-8"), "got: {msg}");
    }

    #[test]
    fn test_invalid_utf8_text_as_numeric_errors() {
        let row = RowData::from_pairs(vec![(
            "val",
            ColumnValue::Text(Bytes::from_static(&[0xff, 0xfe])),
        )]);
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            val: u32,
        }
        let result: crate::error::Result<S> = row.deserialize_into();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("invalid UTF-8"), "got: {msg}");
    }

    // -- deserialize_any: Binary and invalid-UTF8 Text fallbacks -------------

    #[test]
    fn test_any_binary_via_bytebuf_hashmap() {
        // HashMap<String, serde_bytes::ByteBuf> triggers deserialize_any on columns;
        // Binary column hits the Binary → visit_bytes branch.
        use std::collections::HashMap;
        let row = RowData::from_pairs(vec![(
            "blob",
            ColumnValue::binary_bytes(Bytes::from_static(&[0x01, 0x02, 0x03])),
        )]);
        let m: HashMap<String, serde_bytes::ByteBuf> = row.deserialize_into().unwrap();
        assert_eq!(m.get("blob").unwrap().as_ref(), &[0x01, 0x02, 0x03]);
    }

    #[test]
    fn test_any_invalid_utf8_text_via_bytebuf_hashmap() {
        // Invalid-UTF8 Text column hits the Text → visit_bytes fallback in deserialize_any.
        use std::collections::HashMap;
        let row = RowData::from_pairs(vec![(
            "blob",
            ColumnValue::Text(Bytes::from_static(&[0xff, 0xfe, 0xfd])),
        )]);
        let m: HashMap<String, serde_bytes::ByteBuf> = row.deserialize_into().unwrap();
        assert_eq!(m.get("blob").unwrap().as_ref(), &[0xff, 0xfe, 0xfd]);
    }

    // -- deserialize_byte_buf Text path ---------------------------------------

    #[test]
    fn test_byte_buf_from_text() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct S {
            #[serde(with = "serde_bytes")]
            data: Vec<u8>,
        }
        let row = RowData::from_pairs(vec![("data", ColumnValue::text("hello"))]);
        let v: S = row.deserialize_into().unwrap();
        assert_eq!(v.data, b"hello");
    }

    // -- Non-unit enum variants: hit UnitVariantAccess error paths ------------

    #[test]
    fn test_enum_newtype_variant_not_supported() {
        #[derive(Debug, Deserialize)]
        enum E {
            #[allow(dead_code)]
            V(u32),
        }
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            e: E,
        }
        let row = RowData::from_pairs(vec![("e", ColumnValue::text("V"))]);
        let result: crate::error::Result<S> = row.deserialize_into();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("newtype"), "got: {msg}");
    }

    #[test]
    fn test_enum_tuple_variant_not_supported() {
        #[derive(Debug, Deserialize)]
        enum E {
            #[allow(dead_code)]
            V(u32, u32),
        }
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            e: E,
        }
        let row = RowData::from_pairs(vec![("e", ColumnValue::text("V"))]);
        let result: crate::error::Result<S> = row.deserialize_into();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("tuple"), "got: {msg}");
    }

    #[test]
    fn test_enum_struct_variant_not_supported() {
        #[derive(Debug, Deserialize)]
        enum E {
            #[allow(dead_code)]
            V { x: u32 },
        }
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            e: E,
        }
        let row = RowData::from_pairs(vec![("e", ColumnValue::text("V"))]);
        let result: crate::error::Result<S> = row.deserialize_into();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("struct"), "got: {msg}");
    }

    // -- Multi-byte char -------------------------------------------------------

    #[test]
    fn test_char_multibyte_unicode() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct S {
            c: char,
        }
        let row = RowData::from_pairs(vec![("c", ColumnValue::text("é"))]);
        let v: S = row.deserialize_into().unwrap();
        assert_eq!(v.c, 'é');
    }

    // -- Option wrapping numeric none path ------------------------------------

    #[test]
    fn test_option_u64_none() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct S {
            val: Option<u64>,
        }
        let row = RowData::from_pairs(vec![("val", ColumnValue::Null)]);
        let v: S = row.deserialize_into().unwrap();
        assert_eq!(v.val, None);
    }

    // -- deserialize_str via &str target --------------------------------------

    #[test]
    fn test_str_field() {
        // String already covered; this exercises deserialize_str path explicitly
        // via a type that calls deserialize_str during Visitor expectation.
        #[derive(Debug, Deserialize, PartialEq)]
        struct S {
            name: String,
        }
        let row = RowData::from_pairs(vec![("name", ColumnValue::text("alice"))]);
        let v: S = row.deserialize_into().unwrap();
        assert_eq!(v.name, "alice");
    }

    // -- try_deserialize_into (lenient mode) ---------------------------------

    #[test]
    fn test_try_clean_success() {
        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("42")),
            ("username", ColumnValue::text("alice")),
        ]);
        let r = row.try_deserialize_into::<UserModel>().unwrap();
        assert_eq!(r.errors.len(), 0);
        assert_eq!(r.value.id, 42);
        assert_eq!(r.value.username, "alice");
    }

    #[test]
    fn test_try_numeric_mismatch_uses_default_and_reports() {
        #[derive(Debug, Deserialize)]
        struct S {
            id: u32,
            score: i32,
        }
        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("10")),
            ("score", ColumnValue::text("not_a_number")),
        ]);
        let r = row.try_deserialize_into::<S>().unwrap();
        assert_eq!(r.value.id, 10);
        assert_eq!(r.value.score, 0); // default
        assert_eq!(r.errors.len(), 1);
        assert_eq!(r.errors[0].field, "score");
        assert!(r.errors[0].message.contains("not_a_number"));
    }

    #[test]
    fn test_try_multiple_field_errors_collected() {
        #[derive(Debug, Deserialize)]
        struct S {
            a: u32,
            b: i64,
            c: f64,
        }
        let row = RowData::from_pairs(vec![
            ("a", ColumnValue::text("bad1")),
            ("b", ColumnValue::text("bad2")),
            ("c", ColumnValue::text("bad3")),
        ]);
        let r = row.try_deserialize_into::<S>().unwrap();
        assert_eq!(r.value.a, 0);
        assert_eq!(r.value.b, 0);
        assert_eq!(r.value.c, 0.0);
        assert_eq!(r.errors.len(), 3);
        let fields: Vec<&str> = r.errors.iter().map(|e| e.field.as_str()).collect();
        assert_eq!(fields, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_try_null_for_required_string_uses_empty_and_reports() {
        #[derive(Debug, Deserialize)]
        struct S {
            id: u32,
            name: String,
        }
        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("name", ColumnValue::Null),
        ]);
        let r = row.try_deserialize_into::<S>().unwrap();
        assert_eq!(r.value.id, 1);
        assert_eq!(r.value.name, "");
        assert_eq!(r.errors.len(), 1);
        assert_eq!(r.errors[0].field, "name");
        assert!(r.errors[0].message.contains("NULL"));
    }

    #[test]
    fn test_try_bool_invalid_uses_false_and_reports() {
        #[derive(Debug, Deserialize)]
        struct S {
            flag: bool,
        }
        let row = RowData::from_pairs(vec![("flag", ColumnValue::text("maybe"))]);
        let r = row.try_deserialize_into::<S>().unwrap();
        assert!(!r.value.flag);
        assert_eq!(r.errors.len(), 1);
        assert_eq!(r.errors[0].field, "flag");
    }

    #[test]
    fn test_try_bool_valid_single_and_word_forms() {
        #[derive(Debug, Deserialize)]
        struct S {
            a: bool,
            b: bool,
            c: bool,
            d: bool,
        }
        let row = RowData::from_pairs(vec![
            ("a", ColumnValue::text("t")),
            ("b", ColumnValue::text("0")),
            ("c", ColumnValue::text("true")),
            ("d", ColumnValue::text("off")),
        ]);
        let r = row.try_deserialize_into::<S>().unwrap();
        assert!(r.value.a && !r.value.b && r.value.c && !r.value.d);
    }

    #[test]
    fn test_try_option_null_is_none_no_error() {
        #[derive(Debug, Deserialize)]
        struct S {
            id: u32,
            name: Option<String>,
        }
        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("name", ColumnValue::Null),
        ]);
        let r = row.try_deserialize_into::<S>().unwrap();
        assert_eq!(r.value.id, 1);
        assert_eq!(r.value.name, None);
    }

    #[test]
    fn test_try_binary_for_numeric_reports() {
        #[derive(Debug, Deserialize)]
        struct S {
            val: i32,
        }
        let row = RowData::from_pairs(vec![(
            "val",
            ColumnValue::binary_bytes(Bytes::from_static(&[1, 2])),
        )]);
        let r = row.try_deserialize_into::<S>().unwrap();
        assert_eq!(r.value.val, 0);
        assert_eq!(r.errors.len(), 1);
        assert!(r.errors[0].message.contains("binary"));
    }

    #[test]
    fn test_try_char_invalid_uses_null_char() {
        #[derive(Debug, Deserialize)]
        struct S {
            c: char,
        }
        let row = RowData::from_pairs(vec![("c", ColumnValue::text("AB"))]);
        let r = row.try_deserialize_into::<S>().unwrap();
        assert_eq!(r.value.c, '\0');
        assert_eq!(r.errors.len(), 1);
        assert_eq!(r.errors[0].field, "c");
    }

    #[test]
    fn test_try_invalid_utf8_for_string_uses_empty() {
        #[derive(Debug, Deserialize)]
        struct S {
            name: String,
        }
        let row = RowData::from_pairs(vec![(
            "name",
            ColumnValue::Text(Bytes::from_static(&[0xff, 0xfe])),
        )]);
        let r = row.try_deserialize_into::<S>().unwrap();
        assert_eq!(r.value.name, "");
        assert_eq!(r.errors.len(), 1);
        assert!(r.errors[0].message.contains("invalid UTF-8"));
    }

    #[test]
    fn test_try_bytes_null_uses_empty() {
        #[derive(Debug, Deserialize)]
        struct S {
            #[serde(with = "serde_bytes")]
            data: Vec<u8>,
        }
        let row = RowData::from_pairs(vec![("data", ColumnValue::Null)]);
        let r = row.try_deserialize_into::<S>().unwrap();
        assert_eq!(r.value.data, Vec::<u8>::new());
        assert_eq!(r.errors.len(), 1);
        assert!(r.errors[0].message.contains("NULL"));
    }

    #[test]
    fn test_try_bytes_from_binary_ok() {
        #[derive(Debug, Deserialize)]
        struct S {
            #[serde(with = "serde_bytes")]
            data: Vec<u8>,
        }
        let row = RowData::from_pairs(vec![(
            "data",
            ColumnValue::binary_bytes(Bytes::from_static(&[1, 2, 3])),
        )]);
        let r = row.try_deserialize_into::<S>().unwrap();
        assert_eq!(r.value.data, vec![1, 2, 3]);
    }

    #[test]
    fn test_try_into_result_ok() {
        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("username", ColumnValue::text("x")),
        ]);
        let r = row.try_deserialize_into::<UserModel>().unwrap();
        let v = r.into_result().unwrap();
        assert_eq!(v.id, 1);
    }

    #[test]
    fn test_try_into_result_err() {
        #[derive(Debug, Deserialize)]
        #[allow(dead_code)]
        struct S {
            id: u32,
        }
        let row = RowData::from_pairs(vec![("id", ColumnValue::text("nope"))]);
        let r = row.try_deserialize_into::<S>().unwrap();
        let errs = r.into_result().err().unwrap();
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].field, "id");
    }

    #[test]
    fn test_try_unit_on_non_null_reports_but_succeeds() {
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            marker: (),
        }
        let row = RowData::from_pairs(vec![("marker", ColumnValue::text("x"))]);
        let r = row.try_deserialize_into::<S>().unwrap();
        assert_eq!(r.errors.len(), 1);
        assert_eq!(r.errors[0].field, "marker");
    }

    #[test]
    fn test_try_enum_mismatch_still_errors() {
        // Enum has no safe default; outer result is Err.
        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("status", ColumnValue::text("Unknown")),
        ]);
        let r = row.try_deserialize_into::<WithEnum>();
        assert!(r.is_err());
    }

    #[test]
    fn test_try_enum_null_errors() {
        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("status", ColumnValue::Null),
        ]);
        let r = row.try_deserialize_into::<WithEnum>();
        assert!(r.is_err());
    }

    #[test]
    fn test_field_error_display() {
        use crate::deserializer::FieldError;
        let e = FieldError {
            field: "score".to_string(),
            message: "bad".to_string(),
        };
        assert_eq!(e.to_string(), "field 'score': bad");
    }

    #[test]
    fn test_try_nested_struct_errors_structural() {
        #[derive(Debug, Deserialize)]
        struct Inner {
            #[allow(dead_code)]
            x: u32,
        }
        #[derive(Debug, Deserialize)]
        struct Outer {
            #[allow(dead_code)]
            inner: Inner,
        }
        let row = RowData::from_pairs(vec![("inner", ColumnValue::text("{}"))]);
        let r = row.try_deserialize_into::<Outer>();
        assert!(r.is_err());
    }

    // -- Additional coverage tests --------------------------------------------

    #[test]
    fn test_try_is_clean_true_when_no_errors() {
        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("username", ColumnValue::text("a")),
        ]);
        let r = row.try_deserialize_into::<UserModel>().unwrap();
        assert!(r.is_clean());
    }

    #[test]
    fn test_try_is_clean_false_when_errors_recorded() {
        #[derive(Debug, Deserialize)]
        #[allow(dead_code)]
        struct S {
            v: u32,
        }
        let row = RowData::from_pairs(vec![("v", ColumnValue::text("nope"))]);
        let r = row.try_deserialize_into::<S>().unwrap();
        assert!(!r.is_clean());
    }

    #[test]
    fn test_field_error_clone_eq() {
        let a = crate::deserializer::FieldError {
            field: "f".to_string(),
            message: "m".to_string(),
        };
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn test_try_lenient_newtype_struct() {
        #[derive(Debug, Deserialize)]
        struct Wrap(u32);
        #[derive(Debug, Deserialize)]
        struct S {
            v: Wrap,
        }
        let row = RowData::from_pairs(vec![("v", ColumnValue::text("42"))]);
        let r = row.try_deserialize_into::<S>().unwrap();
        assert_eq!(r.value.v.0, 42);
        assert!(r.is_clean());
    }

    #[test]
    fn test_try_lenient_bytes_from_text_arm() {
        #[derive(Debug, Deserialize)]
        struct S {
            #[serde(with = "serde_bytes")]
            data: Vec<u8>,
        }
        let row = RowData::from_pairs(vec![("data", ColumnValue::text("abc"))]);
        let r = row.try_deserialize_into::<S>().unwrap();
        assert_eq!(r.value.data, b"abc");
        assert!(r.is_clean());
    }

    #[test]
    fn test_try_lenient_tuple_struct_errors() {
        #[derive(Debug, Deserialize)]
        #[allow(dead_code)]
        struct TS(u32, u32);
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            t: TS,
        }
        let row = RowData::from_pairs(vec![("t", ColumnValue::text("1,2"))]);
        let r = row.try_deserialize_into::<S>();
        assert!(r.is_err());
    }

    #[test]
    fn test_try_lenient_seq_errors() {
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            xs: Vec<u32>,
        }
        let row = RowData::from_pairs(vec![("xs", ColumnValue::text("1"))]);
        let r = row.try_deserialize_into::<S>();
        assert!(r.is_err());
    }

    #[test]
    fn test_try_lenient_nested_map_errors() {
        use std::collections::HashMap;
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            m: HashMap<String, String>,
        }
        let row = RowData::from_pairs(vec![("m", ColumnValue::text("{}"))]);
        let r = row.try_deserialize_into::<S>();
        assert!(r.is_err());
    }

    #[test]
    fn test_try_lenient_option_non_null_parses_inner() {
        #[derive(Debug, Deserialize)]
        struct S {
            id: Option<u32>,
        }
        let row = RowData::from_pairs(vec![("id", ColumnValue::text("7"))]);
        let r = row.try_deserialize_into::<S>().unwrap();
        assert_eq!(r.value.id, Some(7));
        assert!(r.is_clean());
    }

    #[test]
    fn test_try_lenient_option_non_null_inner_fails_defaults() {
        #[derive(Debug, Deserialize)]
        struct S {
            id: Option<u32>,
        }
        let row = RowData::from_pairs(vec![("id", ColumnValue::text("bad"))]);
        let r = row.try_deserialize_into::<S>().unwrap();
        // Inner parse fails → u32 default (0) wrapped in Some
        assert_eq!(r.value.id, Some(0));
        assert_eq!(r.errors.len(), 1);
        assert_eq!(r.errors[0].field, "id");
    }

    #[test]
    fn test_try_lenient_ignored_any() {
        #[derive(Debug, Deserialize)]
        struct S {
            id: u32,
            #[serde(skip)]
            _extra: (),
        }
        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("ignored", ColumnValue::text("whatever")),
        ]);
        let r = row.try_deserialize_into::<S>().unwrap();
        assert_eq!(r.value.id, 1);
    }

    #[test]
    fn test_strict_unit_struct_ok_on_null() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Marker;
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            m: Marker,
        }
        let row = RowData::from_pairs(vec![("m", ColumnValue::Null)]);
        let v: S = row.deserialize_into().unwrap();
        let _ = v;
    }

    #[test]
    fn test_strict_newtype_struct() {
        #[derive(Debug, Deserialize)]
        struct Wrap(u32);
        #[derive(Debug, Deserialize)]
        struct S {
            v: Wrap,
        }
        let row = RowData::from_pairs(vec![("v", ColumnValue::text("9"))]);
        let v: S = row.deserialize_into().unwrap();
        assert_eq!(v.v.0, 9);
    }

    #[test]
    fn test_strict_ignored_any() {
        #[derive(Debug, Deserialize)]
        struct S {
            id: u32,
            #[serde(skip)]
            _extra: (),
        }
        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("5")),
            ("other", ColumnValue::text("skip_me")),
        ]);
        let v: S = row.deserialize_into().unwrap();
        assert_eq!(v.id, 5);
    }

    #[test]
    fn test_strict_bytes_from_text_arm() {
        #[derive(Debug, Deserialize)]
        struct S {
            #[serde(with = "serde_bytes")]
            data: Vec<u8>,
        }
        let row = RowData::from_pairs(vec![("data", ColumnValue::text("xyz"))]);
        let v: S = row.deserialize_into().unwrap();
        assert_eq!(v.data, b"xyz");
    }

    #[test]
    fn test_strict_unit_variant_newtype_errors() {
        // Exercise UnitVariantAccess::newtype_variant_seed error path.
        #[derive(Debug, Deserialize)]
        #[allow(dead_code)]
        enum E {
            A(u32),
        }
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            e: E,
        }
        let row = RowData::from_pairs(vec![("e", ColumnValue::text("A"))]);
        let r: crate::error::Result<S> = row.deserialize_into();
        assert!(r.is_err());
    }

    #[test]
    fn test_lenient_map_size_hint_reported() {
        // Simple sanity: a 3-column row should deserialize all 3 fields.
        #[derive(Debug, Deserialize)]
        struct S {
            a: u32,
            b: u32,
            c: u32,
        }
        let row = RowData::from_pairs(vec![
            ("a", ColumnValue::text("1")),
            ("b", ColumnValue::text("2")),
            ("c", ColumnValue::text("3")),
        ]);
        let r = row.try_deserialize_into::<S>().unwrap();
        assert_eq!((r.value.a, r.value.b, r.value.c), (1, 2, 3));
    }

    // -- parse_pg_bool shared helper ------------------------------------------

    #[test]
    fn test_parse_pg_bool_all_accepted_forms() {
        use crate::deserializer::parse_pg_bool;
        for s in ["t", "1", "true", "on", "yes"] {
            assert_eq!(parse_pg_bool(s.as_bytes()), Some(true), "input: {s}");
        }
        for s in ["f", "0", "false", "off", "no"] {
            assert_eq!(parse_pg_bool(s.as_bytes()), Some(false), "input: {s}");
        }
        for s in ["", "x", "TRUE", "FALSE", "maybe", "2"] {
            assert_eq!(parse_pg_bool(s.as_bytes()), None, "input: {s}");
        }
    }

    // -- parse_int_signed edge cases -------------------------------------------

    #[test]
    fn test_parse_int_signed_leading_plus() {
        use crate::deserializer::parse_int_signed;
        assert_eq!(parse_int_signed::<i32>(b"+42"), Some(42));
        assert_eq!(parse_int_signed::<i64>(b"+0"), Some(0));
    }

    #[test]
    fn test_parse_int_signed_bare_sign_rejected() {
        use crate::deserializer::parse_int_signed;
        assert_eq!(parse_int_signed::<i32>(b"+"), None);
        assert_eq!(parse_int_signed::<i32>(b"-"), None);
    }

    #[test]
    fn test_parse_int_signed_empty() {
        use crate::deserializer::parse_int_signed;
        assert_eq!(parse_int_signed::<i32>(b""), None);
    }

    #[test]
    fn test_parse_int_signed_non_digit() {
        use crate::deserializer::parse_int_signed;
        assert_eq!(parse_int_signed::<i32>(b"12a3"), None);
        assert_eq!(parse_int_signed::<i32>(b"abc"), None);
    }

    #[test]
    fn test_parse_int_signed_overflow() {
        use crate::deserializer::parse_int_signed;
        // i8 range is -128..=127
        assert_eq!(parse_int_signed::<i8>(b"128"), None);
        assert_eq!(parse_int_signed::<i8>(b"-129"), None);
        assert_eq!(parse_int_signed::<i8>(b"127"), Some(127));
        assert_eq!(parse_int_signed::<i8>(b"-128"), Some(-128));
    }

    #[test]
    fn test_parse_int_signed_i64_boundaries() {
        use crate::deserializer::parse_int_signed;
        assert_eq!(
            parse_int_signed::<i64>(b"9223372036854775807"),
            Some(i64::MAX)
        );
        assert_eq!(
            parse_int_signed::<i64>(b"-9223372036854775808"),
            Some(i64::MIN)
        );
        // overflow
        assert_eq!(parse_int_signed::<i64>(b"9223372036854775808"), None);
    }

    // -- parse_int_unsigned edge cases -----------------------------------------

    #[test]
    fn test_parse_int_unsigned_leading_plus() {
        use crate::deserializer::parse_int_unsigned;
        assert_eq!(parse_int_unsigned::<u32>(b"+42"), Some(42));
    }

    #[test]
    fn test_parse_int_unsigned_bare_plus_rejected() {
        use crate::deserializer::parse_int_unsigned;
        assert_eq!(parse_int_unsigned::<u32>(b"+"), None);
    }

    #[test]
    fn test_parse_int_unsigned_empty() {
        use crate::deserializer::parse_int_unsigned;
        assert_eq!(parse_int_unsigned::<u32>(b""), None);
    }

    #[test]
    fn test_parse_int_unsigned_negative_rejected() {
        use crate::deserializer::parse_int_unsigned;
        assert_eq!(parse_int_unsigned::<u32>(b"-1"), None);
    }

    #[test]
    fn test_parse_int_unsigned_non_digit() {
        use crate::deserializer::parse_int_unsigned;
        assert_eq!(parse_int_unsigned::<u32>(b"12a3"), None);
    }

    #[test]
    fn test_parse_int_unsigned_overflow() {
        use crate::deserializer::parse_int_unsigned;
        // u8 max = 255
        assert_eq!(parse_int_unsigned::<u8>(b"256"), None);
        assert_eq!(parse_int_unsigned::<u8>(b"255"), Some(255));
        // u64 overflow
        assert_eq!(parse_int_unsigned::<u64>(b"18446744073709551616"), None);
    }

    // -- ChangeEvent::deserialize_data for all non-DML types -------------------

    #[test]
    fn test_deserialize_data_commit_errors() {
        let ts = chrono::Utc::now();
        let event = ChangeEvent::commit(ts, Lsn::new(100), Lsn::new(200), Lsn::new(200));
        let result: crate::error::Result<UserModel> = event.deserialize_data();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("commit"), "got: {msg}");
    }

    #[test]
    fn test_deserialize_data_stream_start_errors() {
        let event = ChangeEvent {
            event_type: crate::types::EventType::StreamStart {
                transaction_id: 1,
                first_segment: true,
            },
            lsn: Lsn::new(100),
            metadata: None,
        };
        let result: crate::error::Result<UserModel> = event.deserialize_data();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("stream_start"), "got: {msg}");
    }

    #[test]
    fn test_deserialize_data_stream_stop_errors() {
        let event = ChangeEvent {
            event_type: crate::types::EventType::StreamStop,
            lsn: Lsn::new(100),
            metadata: None,
        };
        let result: crate::error::Result<UserModel> = event.deserialize_data();
        assert!(result.is_err());
    }

    // -- ChangeEvent::deserialize_update deserialization failures ---------------

    #[test]
    fn test_deserialize_update_old_data_parse_error() {
        let old = RowData::from_pairs(vec![
            ("id", ColumnValue::text("not_a_number")),
            ("username", ColumnValue::text("alice")),
        ]);
        let new = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("username", ColumnValue::text("bob")),
        ]);
        let event = ChangeEvent::update(
            "public",
            "users",
            1,
            Some(old),
            new,
            ReplicaIdentity::Full,
            vec![Arc::from("id")],
            Lsn::new(200),
        );
        let result: crate::error::Result<(Option<UserModel>, UserModel)> =
            event.deserialize_update();
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_update_new_data_parse_error() {
        let new = RowData::from_pairs(vec![
            ("id", ColumnValue::text("not_a_number")),
            ("username", ColumnValue::text("bob")),
        ]);
        let event = ChangeEvent::update(
            "public",
            "users",
            1,
            None,
            new,
            ReplicaIdentity::Default,
            vec![Arc::from("id")],
            Lsn::new(200),
        );
        let result: crate::error::Result<(Option<UserModel>, UserModel)> =
            event.deserialize_update();
        assert!(result.is_err());
    }

    // -- Lenient mode: binary enum errors (structural) -------------------------

    #[test]
    fn test_try_enum_binary_errors() {
        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            (
                "status",
                ColumnValue::binary_bytes(Bytes::from_static(b"Active")),
            ),
        ]);
        let r = row.try_deserialize_into::<WithEnum>();
        assert!(r.is_err());
    }

    // -- Lenient mode: binary bool default false and reports -------------------

    #[test]
    fn test_try_binary_bool_uses_false_and_reports() {
        #[derive(Debug, Deserialize)]
        struct S {
            flag: bool,
        }
        let row = RowData::from_pairs(vec![(
            "flag",
            ColumnValue::binary_bytes(Bytes::from_static(&[1])),
        )]);
        let r = row.try_deserialize_into::<S>().unwrap();
        assert!(!r.value.flag);
        assert_eq!(r.errors.len(), 1);
        assert!(r.errors[0].message.contains("binary"));
    }

    // -- Lenient mode: null bool default false and reports ---------------------

    #[test]
    fn test_try_null_bool_uses_false_and_reports() {
        #[derive(Debug, Deserialize)]
        struct S {
            flag: bool,
        }
        let row = RowData::from_pairs(vec![("flag", ColumnValue::Null)]);
        let r = row.try_deserialize_into::<S>().unwrap();
        assert!(!r.value.flag);
        assert_eq!(r.errors.len(), 1);
        assert!(r.errors[0].message.contains("NULL"));
    }

    // -- Lenient mode: binary char defaults to '\0' and reports ----------------

    #[test]
    fn test_try_binary_char_uses_null_char() {
        #[derive(Debug, Deserialize)]
        struct S {
            c: char,
        }
        let row = RowData::from_pairs(vec![(
            "c",
            ColumnValue::binary_bytes(Bytes::from_static(b"A")),
        )]);
        let r = row.try_deserialize_into::<S>().unwrap();
        assert_eq!(r.value.c, '\0');
        assert_eq!(r.errors.len(), 1);
    }

    // -- Lenient mode: null char defaults to '\0' and reports ------------------

    #[test]
    fn test_try_null_char_uses_null_char() {
        #[derive(Debug, Deserialize)]
        struct S {
            c: char,
        }
        let row = RowData::from_pairs(vec![("c", ColumnValue::Null)]);
        let r = row.try_deserialize_into::<S>().unwrap();
        assert_eq!(r.value.c, '\0');
        assert_eq!(r.errors.len(), 1);
    }

    // -- Lenient mode: overflow defaults to 0 and reports ----------------------

    #[test]
    fn test_try_signed_overflow_defaults_to_zero() {
        #[derive(Debug, Deserialize)]
        struct S {
            v: i8,
        }
        let row = RowData::from_pairs(vec![("v", ColumnValue::text("99999"))]);
        let r = row.try_deserialize_into::<S>().unwrap();
        assert_eq!(r.value.v, 0);
        assert_eq!(r.errors.len(), 1);
    }

    #[test]
    fn test_try_unsigned_overflow_defaults_to_zero() {
        #[derive(Debug, Deserialize)]
        struct S {
            v: u8,
        }
        let row = RowData::from_pairs(vec![("v", ColumnValue::text("999"))]);
        let r = row.try_deserialize_into::<S>().unwrap();
        assert_eq!(r.value.v, 0);
        assert_eq!(r.errors.len(), 1);
    }

    // -- Lenient mode: float invalid default to 0.0 ----------------------------

    #[test]
    fn test_try_float_invalid_defaults_to_zero() {
        #[derive(Debug, Deserialize)]
        struct S {
            a: f32,
            b: f64,
        }
        let row = RowData::from_pairs(vec![
            ("a", ColumnValue::text("not_float")),
            ("b", ColumnValue::text("also_bad")),
        ]);
        let r = row.try_deserialize_into::<S>().unwrap();
        assert_eq!(r.value.a, 0.0);
        assert_eq!(r.value.b, 0.0);
        assert_eq!(r.errors.len(), 2);
    }

    // -- Lenient mode: binary for string defaults to empty ---------------------

    #[test]
    fn test_try_binary_string_uses_empty() {
        #[derive(Debug, Deserialize)]
        struct S {
            name: String,
        }
        let row = RowData::from_pairs(vec![(
            "name",
            ColumnValue::binary_bytes(Bytes::from_static(b"data")),
        )]);
        let r = row.try_deserialize_into::<S>().unwrap();
        assert_eq!(r.value.name, "");
        assert_eq!(r.errors.len(), 1);
        assert!(r.errors[0].message.contains("binary"));
    }

    // -- Lenient mode: unit_struct on non-null reports -------------------------

    #[test]
    fn test_try_unit_struct_on_non_null_reports() {
        #[derive(Debug, Deserialize)]
        struct Marker;
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            m: Marker,
        }
        let row = RowData::from_pairs(vec![("m", ColumnValue::text("x"))]);
        let r = row.try_deserialize_into::<S>().unwrap();
        assert_eq!(r.errors.len(), 1);
        assert_eq!(r.errors[0].field, "m");
    }

    // -- Lenient mode: byte_buf null uses empty --------------------------------

    #[test]
    fn test_try_byte_buf_null_uses_empty() {
        #[derive(Debug, Deserialize)]
        struct S {
            #[serde(with = "serde_bytes")]
            data: Vec<u8>,
        }
        let row = RowData::from_pairs(vec![("data", ColumnValue::Null)]);
        let r = row.try_deserialize_into::<S>().unwrap();
        assert!(r.value.data.is_empty());
        assert_eq!(r.errors.len(), 1);
    }

    // -- Lenient mode: HashMap deserialization (any path) ----------------------

    #[test]
    fn test_try_hashmap_passes_through_any() {
        use std::collections::HashMap;
        let row = RowData::from_pairs(vec![
            ("a", ColumnValue::text("hello")),
            ("b", ColumnValue::Null),
        ]);
        let r = row
            .try_deserialize_into::<HashMap<String, Option<String>>>()
            .unwrap();
        assert_eq!(r.value.get("a").unwrap(), &Some("hello".to_string()));
        assert_eq!(r.value.get("b").unwrap(), &None);
        assert!(r.is_clean());
    }

    // -- RowData from_pairs preserves order ------------------------------------

    #[test]
    fn test_from_pairs_preserves_column_order() {
        let row = RowData::from_pairs(vec![
            ("z", ColumnValue::text("1")),
            ("a", ColumnValue::text("2")),
            ("m", ColumnValue::text("3")),
        ]);
        let names: Vec<&str> = row.iter().map(|(k, _)| k.as_ref()).collect();
        assert_eq!(names, vec!["z", "a", "m"]);
    }

    // -- RowData get returns None for missing column ---------------------------

    #[test]
    fn test_row_data_get_missing_column() {
        let row = RowData::from_pairs(vec![("id", ColumnValue::text("1"))]);
        assert!(row.get("nonexistent").is_none());
    }

    // -- RowData default and is_empty -----------------------------------------

    #[test]
    fn test_row_data_default_empty() {
        let row = RowData::default();
        assert!(row.is_empty());
        assert_eq!(row.len(), 0);
    }

    // -- Leading plus sign in integer fields ----------------------------------

    #[test]
    fn test_positive_sign_prefix_accepted() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct S {
            signed: i32,
            unsigned: u32,
        }
        let row = RowData::from_pairs(vec![
            ("signed", ColumnValue::text("+42")),
            ("unsigned", ColumnValue::text("+100")),
        ]);
        let v: S = row.deserialize_into().unwrap();
        assert_eq!(v.signed, 42);
        assert_eq!(v.unsigned, 100);
    }

    // -- Zero values ----------------------------------------------------------

    #[test]
    fn test_zero_values_all_numeric() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct S {
            a: i32,
            b: u32,
            c: f64,
        }
        let row = RowData::from_pairs(vec![
            ("a", ColumnValue::text("0")),
            ("b", ColumnValue::text("0")),
            ("c", ColumnValue::text("0.0")),
        ]);
        let v: S = row.deserialize_into().unwrap();
        assert_eq!(v.a, 0);
        assert_eq!(v.b, 0);
        assert_eq!(v.c, 0.0);
    }

    // -- Float special values -------------------------------------------------

    #[test]
    fn test_float_inf_and_nan() {
        #[derive(Debug, Deserialize)]
        struct S {
            a: f64,
            b: f64,
            c: f64,
        }
        let row = RowData::from_pairs(vec![
            ("a", ColumnValue::text("inf")),
            ("b", ColumnValue::text("-inf")),
            ("c", ColumnValue::text("NaN")),
        ]);
        let v: S = row.deserialize_into().unwrap();
        assert!(v.a.is_infinite() && v.a > 0.0);
        assert!(v.b.is_infinite() && v.b < 0.0);
        assert!(v.c.is_nan());
    }

    // -- Large column count ---------------------------------------------------

    #[test]
    fn test_large_column_count_struct() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct S {
            #[serde(default)]
            c1: u32,
            #[serde(default)]
            c2: u32,
            #[serde(default)]
            c3: u32,
            #[serde(default)]
            c4: u32,
            #[serde(default)]
            c5: u32,
        }
        let mut names: Vec<String> = Vec::new();
        let mut values: Vec<String> = Vec::new();
        for i in 1..=5 {
            names.push(format!("c{i}"));
            values.push(i.to_string());
        }
        let mut pairs: Vec<(&str, ColumnValue)> = Vec::new();
        for i in 0..5 {
            pairs.push((&names[i], ColumnValue::text(&values[i])));
        }
        // add 20 extra columns that should be ignored
        let extra_names: Vec<String> = (6..=25).map(|i| format!("extra_{i}")).collect();
        for name in &extra_names {
            pairs.push((name, ColumnValue::text("ignored")));
        }
        let row = RowData::from_pairs(pairs);
        let v: S = row.deserialize_into().unwrap();
        assert_eq!((v.c1, v.c2, v.c3, v.c4, v.c5), (1, 2, 3, 4, 5));
    }

    // -- ChangeEvent event_type_str coverage ----------------------------------

    #[test]
    fn test_event_type_str_all_variants() {
        let ts = chrono::Utc::now();

        let insert = ChangeEvent::insert("public", "t", 1, RowData::new(), Lsn::new(1));
        assert_eq!(insert.event_type_str(), "insert");

        let delete = ChangeEvent::delete(
            "public",
            "t",
            1,
            RowData::new(),
            ReplicaIdentity::Full,
            vec![],
            Lsn::new(2),
        );
        assert_eq!(delete.event_type_str(), "delete");

        let update = ChangeEvent::update(
            "public",
            "t",
            1,
            None,
            RowData::new(),
            ReplicaIdentity::Default,
            vec![],
            Lsn::new(3),
        );
        assert_eq!(update.event_type_str(), "update");

        let begin = ChangeEvent::begin(1, Lsn::new(4), ts, Lsn::new(4));
        assert_eq!(begin.event_type_str(), "begin");

        let commit = ChangeEvent::commit(ts, Lsn::new(5), Lsn::new(5), Lsn::new(6));
        assert_eq!(commit.event_type_str(), "commit");

        let truncate = ChangeEvent::truncate(vec![Arc::from("t")], Lsn::new(7));
        assert_eq!(truncate.event_type_str(), "truncate");
    }

    // -- Lenient mode: binary bytes no error -----------------------------------

    #[test]
    fn test_try_binary_bytes_ok_no_error() {
        #[derive(Debug, Deserialize)]
        struct S {
            #[serde(with = "serde_bytes")]
            data: Vec<u8>,
        }
        let row = RowData::from_pairs(vec![(
            "data",
            ColumnValue::binary_bytes(Bytes::from_static(&[0xCA, 0xFE])),
        )]);
        let r = row.try_deserialize_into::<S>().unwrap();
        assert_eq!(r.value.data, vec![0xCA, 0xFE]);
        assert!(r.is_clean());
    }

    // -- Lenient mode: text bytes ok -------------------------------------------

    #[test]
    fn test_try_text_bytes_ok_no_error() {
        #[derive(Debug, Deserialize)]
        struct S {
            #[serde(with = "serde_bytes")]
            data: Vec<u8>,
        }
        let row = RowData::from_pairs(vec![("data", ColumnValue::text("hello"))]);
        let r = row.try_deserialize_into::<S>().unwrap();
        assert_eq!(r.value.data, b"hello");
        assert!(r.is_clean());
    }

    // -- Strict mode: float parse error ----------------------------------------

    #[test]
    fn test_strict_float_parse_error() {
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            v: f64,
        }
        let row = RowData::from_pairs(vec![("v", ColumnValue::text("not_float"))]);
        let result: crate::error::Result<S> = row.deserialize_into();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("not_float"), "got: {msg}");
    }

    // -- Strict mode: binary for char errors -----------------------------------

    #[test]
    fn test_strict_binary_char_errors() {
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            c: char,
        }
        let row = RowData::from_pairs(vec![(
            "c",
            ColumnValue::binary_bytes(Bytes::from_static(b"A")),
        )]);
        let result: crate::error::Result<S> = row.deserialize_into();
        assert!(result.is_err());
    }

    // -- Strict mode: null for char errors -------------------------------------

    #[test]
    fn test_strict_null_char_errors() {
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            c: char,
        }
        let row = RowData::from_pairs(vec![("c", ColumnValue::Null)]);
        let result: crate::error::Result<S> = row.deserialize_into();
        assert!(result.is_err());
    }

    // -- RowData encode/decode round trip --------------------------------------

    #[test]
    fn test_row_data_encode_decode_round_trip() {
        use crate::buffer::BufferReader;
        use bytes::BytesMut;

        let original = RowData::from_pairs(vec![
            ("id", ColumnValue::text("42")),
            ("name", ColumnValue::text("alice")),
            (
                "data",
                ColumnValue::binary_bytes(Bytes::from_static(&[1, 2, 3])),
            ),
            ("empty", ColumnValue::Null),
        ]);

        let mut buf = BytesMut::new();
        original.encode(&mut buf);

        let frozen = buf.freeze();
        let mut reader = BufferReader::new(&frozen);
        let decoded = RowData::decode(&mut reader).unwrap();

        assert_eq!(original, decoded);
    }

    // -- RowData with_capacity and push ----------------------------------------

    #[test]
    fn test_row_data_with_capacity_push() {
        let mut row = RowData::with_capacity(3);
        assert!(row.is_empty());
        row.push(Arc::from("a"), ColumnValue::text("1"));
        row.push(Arc::from("b"), ColumnValue::text("2"));
        assert_eq!(row.len(), 2);
        assert_eq!(row.get("a").and_then(|v| v.as_str()), Some("1"));
        assert_eq!(row.get("b").and_then(|v| v.as_str()), Some("2"));
    }

    // -- deserialize_data works for Update (returns new_data) ------------------

    #[test]
    fn test_deserialize_data_update_returns_new() {
        let old = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("username", ColumnValue::text("old_name")),
        ]);
        let new = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("username", ColumnValue::text("new_name")),
        ]);
        let event = ChangeEvent::update(
            "public",
            "users",
            1,
            Some(old),
            new,
            ReplicaIdentity::Full,
            vec![],
            Lsn::new(200),
        );
        let user: UserModel = event.deserialize_data().unwrap();
        assert_eq!(user.username, "new_name");
    }

    // -- Lenient mode: lenient any with binary/invalid-utf8/null ---------------

    #[test]
    fn test_try_lenient_any_binary_via_bytebuf() {
        use std::collections::HashMap;
        let row = RowData::from_pairs(vec![(
            "blob",
            ColumnValue::binary_bytes(Bytes::from_static(&[0xAA, 0xBB])),
        )]);
        let r = row
            .try_deserialize_into::<HashMap<String, serde_bytes::ByteBuf>>()
            .unwrap();
        assert_eq!(r.value.get("blob").unwrap().as_ref(), &[0xAA, 0xBB]);
        assert!(r.is_clean());
    }

    #[test]
    fn test_try_lenient_any_invalid_utf8_via_bytebuf() {
        use std::collections::HashMap;
        let row = RowData::from_pairs(vec![(
            "blob",
            ColumnValue::Text(Bytes::from_static(&[0xFF, 0xFE])),
        )]);
        let r = row
            .try_deserialize_into::<HashMap<String, serde_bytes::ByteBuf>>()
            .unwrap();
        assert_eq!(r.value.get("blob").unwrap().as_ref(), &[0xFF, 0xFE]);
    }

    #[test]
    fn test_try_lenient_any_null_via_option_hashmap() {
        use std::collections::HashMap;
        let row = RowData::from_pairs(vec![
            ("k", ColumnValue::text("v")),
            ("n", ColumnValue::Null),
        ]);
        let r = row
            .try_deserialize_into::<HashMap<String, Option<String>>>()
            .unwrap();
        assert_eq!(r.value.get("k").unwrap(), &Some("v".to_string()));
        assert_eq!(r.value.get("n").unwrap(), &None);
    }

    // -- Lenient mode: enum with binary produces structural error ---------------

    #[test]
    fn test_try_lenient_enum_binary_structural_error() {
        #[derive(Debug, Deserialize)]
        enum E {
            #[allow(dead_code)]
            A,
        }
        #[derive(Debug, Deserialize)]
        struct S {
            #[allow(dead_code)]
            e: E,
        }
        let row = RowData::from_pairs(vec![(
            "e",
            ColumnValue::binary_bytes(Bytes::from_static(b"A")),
        )]);
        let r = row.try_deserialize_into::<S>();
        assert!(r.is_err());
    }

    // -- serde(deny_unknown_fields) works correctly ----------------------------

    #[test]
    fn test_deny_unknown_fields() {
        #[derive(Debug, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Strict {
            #[allow(dead_code)]
            id: u32,
        }
        let row = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("extra", ColumnValue::text("oops")),
        ]);
        let result: crate::error::Result<Strict> = row.deserialize_into();
        assert!(result.is_err());
    }

    // -- Multiple serde attributes combined ------------------------------------

    #[test]
    fn test_combined_serde_attributes() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct S {
            #[serde(rename = "user_id")]
            id: u32,
            #[serde(default)]
            score: f64,
            name: Option<String>,
        }
        let row = RowData::from_pairs(vec![
            ("user_id", ColumnValue::text("42")),
            ("name", ColumnValue::Null),
        ]);
        let v: S = row.deserialize_into().unwrap();
        assert_eq!(v.id, 42);
        assert_eq!(v.score, 0.0);
        assert_eq!(v.name, None);
    }
}
