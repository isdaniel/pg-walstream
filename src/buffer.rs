//! Buffer utilities for reading and writing replication protocol messages
//!
//! This module provides safe wrappers for reading and writing binary data
//! in PostgreSQL's logical replication protocol format using the bytes crate.

use crate::error::{ReplicationError, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Buffer reader for parsing binary protocol messages
///
/// This implementation uses the `bytes` crate for efficient, zero-copy buffer operations.
/// It provides safe methods for reading various integer types, strings, and byte arrays
/// from PostgreSQL's binary protocol format (network byte order / big-endian).
///
/// # Example
///
/// ```
/// use pg_walstream::BufferReader;
///
/// let data = vec![0x00, 0x01, 0x02, 0x03];
/// let mut reader = BufferReader::new(&data);
///
/// let byte = reader.read_u8().unwrap();
/// assert_eq!(byte, 0x00);
///
/// let remaining = reader.remaining();
/// assert_eq!(remaining, 3);
/// ```
pub struct BufferReader {
    data: Bytes,
}

impl BufferReader {
    /// Create a new buffer reader from a byte slice
    ///
    /// Copies the provided byte slice into an internal Bytes buffer.
    /// For zero-copy operation with existing Bytes, use `from_bytes()` instead.
    ///
    /// # Arguments
    ///
    /// * `data` - Byte slice to read from
    ///
    /// # Example
    ///
    /// ```
    /// use pg_walstream::BufferReader;
    ///
    /// let data = vec![0x01, 0x02, 0x03, 0x04];
    /// let reader = BufferReader::new(&data);
    /// ```
    #[inline]
    pub fn new(data: &[u8]) -> Self {
        Self {
            data: Bytes::copy_from_slice(data),
        }
    }

    /// Create a new buffer reader from Bytes
    #[inline]
    pub fn from_bytes(data: Bytes) -> Self {
        Self { data }
    }

    #[inline]
    pub fn from_vec(data: Vec<u8>) -> Self {
        Self {
            data: Bytes::from(data),
        }
    }

    /// Get remaining bytes in the buffer
    #[inline]
    pub fn remaining(&self) -> usize {
        self.data.remaining()
    }

    /// Check if there are enough bytes remaining
    #[inline]
    fn ensure_bytes(&self, count: usize) -> Result<()> {
        if self.data.remaining() < count {
            return Self::short_buffer_err(count, self.data.remaining());
        }
        Ok(())
    }

    #[cold]
    #[inline(never)]
    fn short_buffer_err(needed: usize, have: usize) -> Result<()> {
        Err(ReplicationError::protocol(format!(
            "Not enough bytes remaining. Need {needed}, have {have}"
        )))
    }

    /// Read a single byte from the buffer.
    #[inline]
    pub fn read_u8(&mut self) -> Result<u8> {
        self.ensure_bytes(1)?;
        Ok(self.data.get_u8())
    }

    /// Read a big-endian u16.
    #[inline]
    pub fn read_u16(&mut self) -> Result<u16> {
        self.ensure_bytes(2)?;
        Ok(self.data.get_u16())
    }

    /// Read a big-endian u32.
    #[inline]
    pub fn read_u32(&mut self) -> Result<u32> {
        self.ensure_bytes(4)?;
        Ok(self.data.get_u32())
    }

    /// Read a big-endian u64.
    #[inline]
    pub fn read_u64(&mut self) -> Result<u64> {
        self.ensure_bytes(8)?;
        Ok(self.data.get_u64())
    }

    /// Read a 16-bit signed integer in network byte order
    #[inline]
    pub fn read_i16(&mut self) -> Result<i16> {
        self.ensure_bytes(2)?;
        Ok(self.data.get_i16())
    }

    /// Read a 32-bit signed integer in network byte order
    #[inline]
    pub fn read_i32(&mut self) -> Result<i32> {
        self.ensure_bytes(4)?;
        Ok(self.data.get_i32())
    }

    /// Read a 64-bit signed integer in network byte order
    #[inline]
    pub fn read_i64(&mut self) -> Result<i64> {
        self.ensure_bytes(8)?;
        Ok(self.data.get_i64())
    }

    /// Read a null-terminated string
    #[inline]
    pub fn read_cstring(&mut self) -> Result<String> {
        let data_slice = self.data.chunk();

        // Find the null terminator using SIMD-accelerated scanning
        let bytes_to_read = memchr::memchr(0, data_slice).ok_or_else(|| {
            ReplicationError::protocol("Unterminated string in buffer".to_string())
        })?;

        // Validate UTF-8 on the borrowed slice (zero-copy), then create one owned String
        let result = std::str::from_utf8(&data_slice[..bytes_to_read])
            .map_err(|e| ReplicationError::protocol(format!("Invalid UTF-8 in string: {e}")))?
            .to_owned();

        // Advance past the string bytes and the null terminator
        self.data.advance(bytes_to_read + 1);

        Ok(result)
    }

    /// Read a null-terminated string directly into `Arc<str>` (avoids intermediate String allocation).
    #[inline]
    pub fn read_cstring_arc(&mut self) -> Result<std::sync::Arc<str>> {
        let data_slice = self.data.chunk();

        let bytes_to_read = memchr::memchr(0, data_slice).ok_or_else(|| {
            ReplicationError::protocol("Unterminated string in buffer".to_string())
        })?;

        let result = std::str::from_utf8(&data_slice[..bytes_to_read])
            .map_err(|e| ReplicationError::protocol(format!("Invalid UTF-8 in string: {e}")))?;

        let arc: std::sync::Arc<str> = std::sync::Arc::from(result);

        self.data.advance(bytes_to_read + 1);

        Ok(arc)
    }

    /// Read a fixed-length string without null terminator
    #[inline]
    pub fn read_string(&mut self, length: usize) -> Result<String> {
        self.ensure_bytes(length)?;
        let result = std::str::from_utf8(&self.data.chunk()[..length])
            .map_err(|e| ReplicationError::protocol(format!("Invalid UTF-8 in string: {e}")))?
            .to_owned();
        self.data.advance(length);
        Ok(result)
    }

    /// Read raw bytes
    #[inline]
    pub fn read_bytes(&mut self, length: usize) -> Result<Vec<u8>> {
        self.ensure_bytes(length)?;
        let mut out = vec![0u8; length];
        self.data.copy_to_slice(&mut out);
        Ok(out)
    }

    /// Get raw bytes as Bytes (zero-copy when possible)
    #[inline]
    pub fn read_bytes_buf(&mut self, length: usize) -> Result<Bytes> {
        self.ensure_bytes(length)?;
        Ok(self.data.copy_to_bytes(length))
    }

    /// Peek at the next byte without advancing position
    #[inline]
    pub fn peek_u8(&self) -> Result<u8> {
        self.ensure_bytes(1)?;
        Ok(self.data.chunk()[0])
    }

    /// Skip n bytes
    #[inline]
    pub fn skip(&mut self, count: usize) -> Result<()> {
        self.ensure_bytes(count)?;
        self.data.advance(count);
        Ok(())
    }
}

/// Buffer writer for creating binary protocol messages
///
/// This implementation uses BytesMut from the bytes crate for efficient buffer operations.
pub struct BufferWriter {
    data: BytesMut,
}

impl BufferWriter {
    /// Create a new buffer writer with initial capacity
    pub fn new() -> Self {
        Self {
            data: BytesMut::new(),
        }
    }

    /// Create a new buffer writer with specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: BytesMut::with_capacity(capacity),
        }
    }

    /// Get current position in the buffer
    pub fn position(&self) -> usize {
        self.data.len()
    }

    /// Get bytes written so far
    pub fn bytes_written(&self) -> usize {
        self.data.len()
    }

    /// Get the data as bytes
    pub fn freeze(self) -> Bytes {
        self.data.freeze()
    }

    /// Get the data as a `Vec<u8>`
    pub fn into_vec(self) -> Vec<u8> {
        self.data.to_vec()
    }

    /// Write a single byte.
    #[inline]
    pub fn write_u8(&mut self, value: u8) {
        self.data.put_u8(value);
    }

    /// Write a big-endian u16.
    #[inline]
    pub fn write_u16(&mut self, value: u16) {
        self.data.put_u16(value);
    }

    /// Write a big-endian i16.
    #[inline]
    pub fn write_i16(&mut self, value: i16) {
        self.data.put_i16(value);
    }

    /// Write a big-endian u32.
    #[inline]
    pub fn write_u32(&mut self, value: u32) {
        self.data.put_u32(value);
    }

    /// Write a big-endian u64.
    #[inline]
    pub fn write_u64(&mut self, value: u64) {
        self.data.put_u64(value);
    }

    /// Write a big-endian i32.
    #[inline]
    pub fn write_i32(&mut self, value: i32) {
        self.data.put_i32(value);
    }

    /// Write a big-endian i64.
    #[inline]
    pub fn write_i64(&mut self, value: i64) {
        self.data.put_i64(value);
    }

    /// Write raw bytes.
    #[inline]
    pub fn write_bytes(&mut self, bytes: &[u8]) {
        self.data.put_slice(bytes);
    }

    /// Write a null-terminated string
    ///
    /// Rejects strings containing embedded null bytes, which would produce
    /// malformed PostgreSQL protocol messages (the null byte is the terminator).
    pub fn write_cstring(&mut self, s: &str) -> Result<()> {
        if s.as_bytes().contains(&0) {
            return Err(ReplicationError::protocol(
                "string contains embedded null byte".to_string(),
            ));
        }
        self.data.put_slice(s.as_bytes());
        self.data.put_u8(0);
        Ok(())
    }

    /// Write a string without null terminator.
    #[inline]
    pub fn write_string(&mut self, s: &str) {
        self.data.put_slice(s.as_bytes());
    }

    /// Reserve capacity for at least additional bytes
    pub fn reserve(&mut self, additional: usize) {
        self.data.reserve(additional);
    }

    /// Clear the buffer, resetting length to 0
    pub fn clear(&mut self) {
        self.data.clear();
    }

    /// Get remaining capacity
    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    /// Get a reference to the internal data  
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }
}

impl AsRef<[u8]> for BufferWriter {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl Default for BufferWriter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_reader_basic() {
        let data = [0x01, 0x02, 0x03, 0x04];
        let mut reader = BufferReader::new(&data);

        assert_eq!(reader.read_u8().unwrap(), 0x01);
        assert_eq!(reader.read_u8().unwrap(), 0x02);
        assert_eq!(reader.remaining(), 2);
    }

    #[test]
    fn test_buffer_reader_u16() {
        let data = [0x01, 0x02];
        let mut reader = BufferReader::new(&data);
        assert_eq!(reader.read_u16().unwrap(), 0x0102);
    }

    #[test]
    fn test_buffer_reader_u32() {
        let data = [0x01, 0x02, 0x03, 0x04];
        let mut reader = BufferReader::new(&data);
        assert_eq!(reader.read_u32().unwrap(), 0x01020304);
    }

    #[test]
    fn test_buffer_writer_basic() {
        let mut writer = BufferWriter::new();

        writer.write_u8(0x01);
        writer.write_u16(0x0203);
        writer.write_u32(0x04050607);

        assert_eq!(writer.bytes_written(), 7);

        let data = writer.freeze();
        assert_eq!(&data[..], &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07]);
    }

    #[test]
    fn test_buffer_reader_strings() {
        let data = b"hello\x00world";
        let mut reader = BufferReader::new(data);

        let s = reader.read_cstring().unwrap();
        assert_eq!(s, "hello");

        let s2 = reader.read_string(5).unwrap();
        assert_eq!(s2, "world");
    }

    #[test]
    fn test_buffer_reader_read_cstring_arc() {
        let data = b"hello\x00rest";
        let mut reader = BufferReader::new(data);
        let arc = reader.read_cstring_arc().unwrap();
        assert_eq!(&*arc, "hello");
        assert_eq!(reader.remaining(), 4);
    }

    #[test]
    fn test_buffer_reader_read_cstring_arc_empty() {
        let data = [0x00, 0x01];
        let mut reader = BufferReader::new(&data);
        let arc = reader.read_cstring_arc().unwrap();
        assert_eq!(&*arc, "");
        assert_eq!(reader.remaining(), 1);
    }

    #[test]
    fn test_buffer_reader_read_cstring_arc_unterminated() {
        let data = b"no null";
        let mut reader = BufferReader::new(data);
        assert!(reader.read_cstring_arc().is_err());
    }

    #[test]
    fn test_buffer_writer_strings() {
        let mut writer = BufferWriter::new();

        writer.write_cstring("hello").unwrap();
        writer.write_string("world");

        let data = writer.freeze();
        assert_eq!(&data[..], b"hello\x00world");
    }

    #[test]
    fn test_buffer_reader_zero_copy() {
        let data = vec![0x01, 0x02, 0x03, 0x04, 0x05];
        let bytes = Bytes::from(data);
        let mut reader = BufferReader::from_bytes(bytes);

        let chunk = reader.read_bytes_buf(3).unwrap();
        assert_eq!(&chunk[..], &[0x01, 0x02, 0x03]);
        assert_eq!(reader.remaining(), 2);
    }

    #[test]
    fn test_buffer_reader_signed_integers() {
        // Test i16
        let data = [0xFF, 0xFE]; // -2 in two's complement
        let mut reader = BufferReader::new(&data);
        assert_eq!(reader.read_i16().unwrap(), -2);

        // Test i32
        let data = [0xFF, 0xFF, 0xFF, 0xFE]; // -2 in two's complement
        let mut reader = BufferReader::new(&data);
        assert_eq!(reader.read_i32().unwrap(), -2);

        // Test i64
        let data = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE]; // -2
        let mut reader = BufferReader::new(&data);
        assert_eq!(reader.read_i64().unwrap(), -2);
    }

    #[test]
    fn test_buffer_reader_peek() {
        let data = [0x01, 0x02, 0x03];
        let mut reader = BufferReader::new(&data);

        // Peek should not advance position
        assert_eq!(reader.peek_u8().unwrap(), 0x01);
        assert_eq!(reader.remaining(), 3);
        assert_eq!(reader.peek_u8().unwrap(), 0x01);

        // Read should advance position
        assert_eq!(reader.read_u8().unwrap(), 0x01);
        assert_eq!(reader.remaining(), 2);
        assert_eq!(reader.peek_u8().unwrap(), 0x02);
    }

    #[test]
    fn test_buffer_reader_skip() {
        let data = [0x01, 0x02, 0x03, 0x04, 0x05];
        let mut reader = BufferReader::new(&data);

        reader.skip(2).unwrap();
        assert_eq!(reader.remaining(), 3);
        assert_eq!(reader.read_u8().unwrap(), 0x03);

        reader.skip(1).unwrap();
        assert_eq!(reader.read_u8().unwrap(), 0x05);
    }

    #[test]
    fn test_buffer_reader_errors() {
        let data = [0x01, 0x02];
        let mut reader = BufferReader::new(&data);

        // Try to read more bytes than available
        assert!(reader.read_u32().is_err());
        assert!(reader.read_u64().is_err());

        // After error, buffer should still be valid
        assert_eq!(reader.read_u8().unwrap(), 0x01);
    }

    #[test]
    fn test_buffer_reader_unterminated_string() {
        let data = b"hello world"; // No null terminator
        let mut reader = BufferReader::new(data);

        let result = reader.read_cstring();
        assert!(result.is_err());
    }

    #[test]
    fn test_buffer_reader_invalid_utf8() {
        let data = [0xFF, 0xFE, 0x00]; // Invalid UTF-8 followed by null
        let mut reader = BufferReader::new(&data);

        let result = reader.read_cstring();
        assert!(result.is_err());
    }

    #[test]
    fn test_buffer_reader_cstring_empty_string() {
        // Null byte at start → empty string
        let data = [0x00, 0x01, 0x02];
        let mut reader = BufferReader::new(&data);
        let s = reader.read_cstring().unwrap();
        assert_eq!(s, "");
        assert_eq!(reader.remaining(), 2); // consumed 1 byte (the null)
    }

    #[test]
    fn test_buffer_reader_cstring_consecutive() {
        // Two back-to-back C strings
        let data = b"hello\x00world\x00";
        let mut reader = BufferReader::new(data);
        assert_eq!(reader.read_cstring().unwrap(), "hello");
        assert_eq!(reader.read_cstring().unwrap(), "world");
        assert_eq!(reader.remaining(), 0);
    }

    #[test]
    fn test_buffer_reader_cstring_long_string() {
        // 256-byte string — tests memchr SIMD vectorization beyond a single 16/32-byte lane
        let mut data = vec![b'A'; 256];
        data.push(0x00);
        let mut reader = BufferReader::new(&data);
        let s = reader.read_cstring().unwrap();
        assert_eq!(s.len(), 256);
        assert!(s.chars().all(|c| c == 'A'));
        assert_eq!(reader.remaining(), 0);
    }

    #[test]
    fn test_buffer_reader_cstring_from_bytes_zero_copy() {
        // Verify read_cstring works correctly via from_bytes (zero-copy path)
        let data = Bytes::from_static(b"test\x00rest");
        let mut reader = BufferReader::from_bytes(data);
        assert_eq!(reader.read_cstring().unwrap(), "test");
        assert_eq!(reader.remaining(), 4); // "rest"
    }

    #[test]
    fn test_buffer_reader_from_vec() {
        let data = vec![0x01, 0x02, 0x03];
        let mut reader = BufferReader::from_vec(data);

        assert_eq!(reader.read_u8().unwrap(), 0x01);
        assert_eq!(reader.remaining(), 2);
    }

    #[test]
    fn test_buffer_writer_signed_integers() {
        let mut writer = BufferWriter::new();

        writer.write_i16(-2);
        writer.write_i32(-2);
        writer.write_i64(-2);

        let data = writer.freeze();
        assert_eq!(data.len(), 2 + 4 + 8);

        let mut reader = BufferReader::from_bytes(data);
        assert_eq!(reader.read_i16().unwrap(), -2);
        assert_eq!(reader.read_i32().unwrap(), -2);
        assert_eq!(reader.read_i64().unwrap(), -2);
    }

    #[test]
    fn test_buffer_writer_with_capacity() {
        let mut writer = BufferWriter::with_capacity(100);
        writer.write_u64(0x0102030405060708);
        assert_eq!(writer.bytes_written(), 8);
    }

    #[test]
    fn test_buffer_writer_bytes() {
        let mut writer = BufferWriter::new();
        writer.write_bytes(&[0x01, 0x02, 0x03]);
        writer.write_bytes(&[0x04, 0x05]);

        let data = writer.freeze();
        assert_eq!(&data[..], &[0x01, 0x02, 0x03, 0x04, 0x05]);
    }

    #[test]
    fn test_buffer_reader_read_bytes() {
        let data = [0x01, 0x02, 0x03, 0x04, 0x05];
        let mut reader = BufferReader::new(&data);

        let bytes = reader.read_bytes(3).unwrap();
        assert_eq!(bytes, vec![0x01, 0x02, 0x03]);
        assert_eq!(reader.remaining(), 2);
    }

    #[test]
    fn test_buffer_empty() {
        let data: &[u8] = &[];
        let mut reader = BufferReader::new(data);

        assert_eq!(reader.remaining(), 0);
        assert!(reader.read_u8().is_err());
        assert!(reader.peek_u8().is_err());
    }

    #[test]
    fn test_buffer_reader_from_bytes() {
        let bytes = Bytes::from_static(&[0x01, 0x02, 0x03]);
        let mut reader = BufferReader::from_bytes(bytes);
        assert_eq!(reader.read_u8().unwrap(), 0x01);
        assert_eq!(reader.remaining(), 2);
    }

    #[test]
    fn test_buffer_reader_from_vec_full() {
        let data = vec![0xAA, 0xBB, 0xCC, 0xDD];
        let mut reader = BufferReader::from_vec(data);
        assert_eq!(reader.read_u32().unwrap(), 0xAABBCCDD);
        assert_eq!(reader.remaining(), 0);
    }

    #[test]
    fn test_buffer_writer_write_i16() {
        let mut writer = BufferWriter::new();
        writer.write_i16(0x1234);
        let data = writer.freeze();
        assert_eq!(&data[..], &[0x12, 0x34]);
    }

    #[test]
    fn test_buffer_writer_write_i32() {
        let mut writer = BufferWriter::new();
        writer.write_i32(0x12345678);
        let data = writer.freeze();
        assert_eq!(&data[..], &[0x12, 0x34, 0x56, 0x78]);
    }

    #[test]
    fn test_buffer_writer_write_i64() {
        let mut writer = BufferWriter::new();
        writer.write_i64(0x0102030405060708);
        let data = writer.freeze();
        assert_eq!(&data[..], &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
    }

    #[test]
    fn test_buffer_writer_write_cstring() {
        let mut writer = BufferWriter::new();
        writer.write_cstring("test").unwrap();
        let data = writer.freeze();
        assert_eq!(&data[..], b"test\x00");
    }

    #[test]
    fn test_buffer_writer_write_cstring_rejects_embedded_null() {
        let mut writer = BufferWriter::new();
        let result = writer.write_cstring("hello\x00world");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("embedded null"),
            "Expected embedded null error, got: {err}"
        );
    }

    #[test]
    fn test_buffer_writer_write_string() {
        let mut writer = BufferWriter::new();
        writer.write_string("hello");
        let data = writer.freeze();
        assert_eq!(&data[..], b"hello");
    }

    #[test]
    fn test_buffer_writer_reserve() {
        let mut writer = BufferWriter::new();
        writer.reserve(1024);
        assert!(writer.capacity() >= 1024);
    }

    #[test]
    fn test_buffer_writer_clear() {
        let mut writer = BufferWriter::new();
        writer.write_u8(0x01);
        writer.write_u8(0x02);
        assert_eq!(writer.bytes_written(), 2);

        writer.clear();
        assert_eq!(writer.bytes_written(), 0);
        assert_eq!(writer.position(), 0);
    }

    #[test]
    fn test_buffer_writer_capacity() {
        let writer = BufferWriter::with_capacity(256);
        assert!(writer.capacity() >= 256);
    }

    #[test]
    fn test_buffer_writer_as_bytes() {
        let mut writer = BufferWriter::new();
        writer.write_u8(0xAA);
        writer.write_u8(0xBB);
        assert_eq!(writer.as_bytes(), &[0xAA, 0xBB]);
    }

    #[test]
    fn test_buffer_writer_as_ref() {
        let mut writer = BufferWriter::new();
        writer.write_u8(0x01);
        let slice: &[u8] = writer.as_ref();
        assert_eq!(slice, &[0x01]);
    }

    #[test]
    fn test_buffer_writer_default() {
        let mut writer = BufferWriter::default();
        writer.write_u8(0xFF);
        assert_eq!(writer.bytes_written(), 1);
    }

    #[test]
    fn test_buffer_writer_into_vec() {
        let mut writer = BufferWriter::new();
        writer.write_u8(0x01);
        writer.write_u16(0x0203);
        let vec = writer.into_vec();
        assert_eq!(vec, vec![0x01, 0x02, 0x03]);
    }

    #[test]
    fn test_buffer_reader_u64() {
        let data = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        let mut reader = BufferReader::new(&data);
        assert_eq!(reader.read_u64().unwrap(), 0x0102030405060708);
    }

    #[test]
    fn test_buffer_reader_remaining_initial() {
        let data = [0x01, 0x02, 0x03, 0x04];
        let reader = BufferReader::new(&data);
        assert_eq!(reader.remaining(), 4);
    }

    #[test]
    fn test_buffer_writer_position() {
        let mut writer = BufferWriter::new();
        assert_eq!(writer.position(), 0);
        writer.write_u32(0x12345678);
        assert_eq!(writer.position(), 4);
    }

    #[test]
    fn test_buffer_roundtrip_complex() {
        let mut writer = BufferWriter::new();
        writer.write_u8(0x42);
        writer.write_u16(0x1234);
        writer.write_u32(0xDEADBEEF);
        writer.write_u64(0xCAFEBABE12345678);
        writer.write_i16(-100);
        writer.write_i32(-200);
        writer.write_i64(-300);
        writer.write_cstring("hello").unwrap();
        writer.write_bytes(&[0x01, 0x02]);

        let data = writer.freeze();
        let mut reader = BufferReader::from_bytes(data);

        assert_eq!(reader.read_u8().unwrap(), 0x42);
        assert_eq!(reader.read_u16().unwrap(), 0x1234);
        assert_eq!(reader.read_u32().unwrap(), 0xDEADBEEF);
        assert_eq!(reader.read_u64().unwrap(), 0xCAFEBABE12345678);
        assert_eq!(reader.read_i16().unwrap(), -100);
        assert_eq!(reader.read_i32().unwrap(), -200);
        assert_eq!(reader.read_i64().unwrap(), -300);
        assert_eq!(reader.read_cstring().unwrap(), "hello");
        assert_eq!(reader.read_bytes(2).unwrap(), vec![0x01, 0x02]);
        assert_eq!(reader.remaining(), 0);
    }

    #[test]
    fn test_buffer_reader_skip_insufficient() {
        let data = [0x01, 0x02];
        let mut reader = BufferReader::new(&data);
        assert!(reader.skip(5).is_err());
    }

    #[test]
    fn test_buffer_reader_read_string_insufficient() {
        let data = [0x01, 0x02];
        let mut reader = BufferReader::new(&data);
        assert!(reader.read_string(5).is_err());
    }

    #[test]
    fn test_buffer_reader_read_bytes_insufficient() {
        let data = [0x01];
        let mut reader = BufferReader::new(&data);
        assert!(reader.read_bytes(5).is_err());
    }

    #[test]
    fn test_buffer_reader_read_bytes_buf_insufficient() {
        let data = [0x01];
        let mut reader = BufferReader::new(&data);
        assert!(reader.read_bytes_buf(5).is_err());
    }

    /// Pin the error message format produced by the `#[cold]`
    /// `short_buffer_err` helper. Several layers above (parser, stream)
    /// surface this string in logs, so format regressions would silently
    /// degrade diagnostics.
    #[test]
    fn test_buffer_reader_short_buffer_err_message_format() {
        let data = [0x01, 0x02];
        let mut reader = BufferReader::new(&data);
        let err = reader.read_bytes_buf(5).unwrap_err();
        let s = err.to_string();
        assert!(
            s.contains("Not enough bytes remaining"),
            "expected 'Not enough bytes remaining' in error, got: {s}"
        );
        assert!(s.contains("Need 5"), "expected 'Need 5', got: {s}");
        assert!(s.contains("have 2"), "expected 'have 2', got: {s}");
    }
}
