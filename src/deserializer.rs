//! Custom serde [`Deserializer`] for converting [`RowData`] into user-defined types.
//!
//! PostgreSQL's logical replication protocol (pgoutput) sends column values as
//! text strings. This module provides a [`Deserializer`] that parses those text
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

// ---------------------------------------------------------------------------
// RowDataDeserializer — top-level Deserializer for RowData
// ---------------------------------------------------------------------------

/// A serde [`Deserializer`] that treats a [`RowData`] as a map of column names
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

    /// Parse text as a numeric type.
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
        let s = self.text_or_err("bool")?;
        // Fast path: PostgreSQL's pgoutput always sends "t"/"f" for booleans.
        // Check single-byte values first (covers ~99% of real WAL traffic),
        // then fall through to full PostgreSQL boolean input vocabulary.
        let val = if s.len() == 1 {
            match s.as_bytes()[0] {
                b't' | b'1' => true,
                b'f' | b'0' => false,
                _ => {
                    return Err(ReplicationError::deserialize(format!(
                        "cannot parse '{s}' as bool (expected t/f/true/false/1/0/on/off/yes/no)"
                    )))
                }
            }
        } else {
            match s {
                "true" | "on" | "yes" => true,
                "false" | "off" | "no" => false,
                _ => {
                    return Err(ReplicationError::deserialize(format!(
                        "cannot parse '{s}' as bool (expected t/f/true/false/1/0/on/off/yes/no)"
                    )))
                }
            }
        };
        visitor.visit_bool(val)
    }

    #[inline]
    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_i8(self.parse_text("i8")?)
    }

    #[inline]
    fn deserialize_i16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_i16(self.parse_text("i16")?)
    }

    #[inline]
    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_i32(self.parse_text("i32")?)
    }

    #[inline]
    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_i64(self.parse_text("i64")?)
    }

    #[inline]
    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_u8(self.parse_text("u8")?)
    }

    #[inline]
    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_u16(self.parse_text("u16")?)
    }

    #[inline]
    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_u32(self.parse_text("u32")?)
    }

    #[inline]
    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_u64(self.parse_text("u64")?)
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
            ColumnValue::Binary(b) => visitor.visit_bytes(b),
            ColumnValue::Text(b) => visitor.visit_bytes(b),
            ColumnValue::Null => Err(ReplicationError::deserialize(
                "cannot deserialize NULL as bytes",
            )),
        }
    }

    #[inline]
    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.value {
            ColumnValue::Binary(b) => visitor.visit_byte_buf(b.to_vec()),
            ColumnValue::Text(b) => visitor.visit_byte_buf(b.to_vec()),
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
        visitor.visit_enum(s.into_deserializer())
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

/// Helper to convert `&str` into a serde enum deserializer.
struct StrEnumAccess<'a>(&'a str);

impl<'de, 'a> de::EnumAccess<'de> for StrEnumAccess<'a> {
    type Error = ReplicationError;
    type Variant = UnitVariantAccess;

    fn variant_seed<V: DeserializeSeed<'de>>(
        self,
        seed: V,
    ) -> Result<(V::Value, Self::Variant), Self::Error> {
        let de: serde::de::value::StrDeserializer<'_, ReplicationError> =
            serde::de::value::StrDeserializer::new(self.0);
        let val = seed.deserialize(de)?;
        Ok((val, UnitVariantAccess))
    }
}

struct UnitVariantAccess;

impl<'de> de::VariantAccess<'de> for UnitVariantAccess {
    type Error = ReplicationError;

    fn unit_variant(self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(
        self,
        _seed: T,
    ) -> Result<T::Value, Self::Error> {
        Err(ReplicationError::deserialize(
            "newtype enum variants are not supported",
        ))
    }

    fn tuple_variant<V: Visitor<'de>>(
        self,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(ReplicationError::deserialize(
            "tuple enum variants are not supported",
        ))
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(ReplicationError::deserialize(
            "struct enum variants are not supported",
        ))
    }
}

/// Trait extension to convert `&str` into an `EnumAccess` deserializer.
trait IntoEnumDeserializer<'a> {
    fn into_deserializer(self) -> StrEnumAccess<'a>;
}

impl<'a> IntoEnumDeserializer<'a> for &'a str {
    fn into_deserializer(self) -> StrEnumAccess<'a> {
        StrEnumAccess(self)
    }
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
}
