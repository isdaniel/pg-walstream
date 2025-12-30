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

    /// Get current position in the buffer
    #[inline]
    pub fn position(&self) -> usize {
        self.data.len()
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
            return Err(ReplicationError::protocol(format!(
                "Not enough bytes remaining. Need {}, have {}",
                count,
                self.data.remaining()
            )));
        }
        Ok(())
    }

    /// Skip the message type byte and return current position
    #[inline]
    pub fn skip_message_type(&mut self) -> Result<usize> {
        self.ensure_bytes(1)?;
        self.data.advance(1);
        Ok(self.data.len())
    }

    /// Read a single byte
    ///
    /// # Returns
    ///
    /// Returns the next byte from the buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if there are insufficient bytes remaining in the buffer.
    #[inline]
    pub fn read_u8(&mut self) -> Result<u8> {
        self.ensure_bytes(1)?;
        Ok(self.data.get_u8())
    }

    /// Read a 16-bit unsigned integer in network byte order
    ///
    /// Reads 2 bytes and interprets them as a big-endian u16.
    ///
    /// # Returns
    ///
    /// Returns the next 16-bit unsigned integer.
    ///
    /// # Errors
    ///
    /// Returns an error if there are insufficient bytes remaining.
    #[inline]
    pub fn read_u16(&mut self) -> Result<u16> {
        self.ensure_bytes(2)?;
        Ok(self.data.get_u16())
    }

    /// Read a 32-bit unsigned integer in network byte order
    ///
    /// Reads 4 bytes and interprets them as a big-endian u32.
    ///
    /// # Returns
    ///
    /// Returns the next 32-bit unsigned integer.
    ///
    /// # Errors
    ///
    /// Returns an error if there are insufficient bytes remaining.
    #[inline]
    pub fn read_u32(&mut self) -> Result<u32> {
        self.ensure_bytes(4)?;
        Ok(self.data.get_u32())
    }

    /// Read a 64-bit unsigned integer in network byte order
    ///
    /// Reads 8 bytes and interprets them as a big-endian u64.
    /// This is commonly used for reading LSN values.
    ///
    /// # Returns
    ///
    /// Returns the next 64-bit unsigned integer.
    ///
    /// # Errors
    ///
    /// Returns an error if there are insufficient bytes remaining.
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

        // Find the null terminator
        let bytes_to_read = data_slice.iter().position(|&b| b == 0).ok_or_else(|| {
            ReplicationError::protocol("Unterminated string in buffer".to_string())
        })?;

        // Read the string bytes
        let string_bytes = self.data.copy_to_bytes(bytes_to_read);
        let result = String::from_utf8(string_bytes.to_vec())
            .map_err(|e| ReplicationError::protocol(format!("Invalid UTF-8 in string: {e}")))?;

        // Skip null terminator
        self.data.advance(1);

        Ok(result)
    }

    /// Read a fixed-length string without null terminator
    #[inline]
    pub fn read_string(&mut self, length: usize) -> Result<String> {
        self.ensure_bytes(length)?;
        let string_bytes = self.data.copy_to_bytes(length);
        let result = String::from_utf8(string_bytes.to_vec())
            .map_err(|e| ReplicationError::protocol(format!("Invalid UTF-8 in string: {e}")))?;
        Ok(result)
    }

    /// Read raw bytes
    #[inline]
    pub fn read_bytes(&mut self, length: usize) -> Result<Vec<u8>> {
        self.ensure_bytes(length)?;
        let bytes = self.data.copy_to_bytes(length);
        Ok(bytes.to_vec())
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

    /// Write a single byte
    pub fn write_u8(&mut self, value: u8) -> Result<()> {
        self.data.put_u8(value);
        Ok(())
    }

    /// Write a 16-bit unsigned integer in network byte order
    pub fn write_u16(&mut self, value: u16) -> Result<()> {
        self.data.put_u16(value);
        Ok(())
    }

    /// Write a 16-bit signed integer in network byte order
    pub fn write_i16(&mut self, value: i16) -> Result<()> {
        self.data.put_i16(value);
        Ok(())
    }

    /// Write a 32-bit unsigned integer in network byte order
    pub fn write_u32(&mut self, value: u32) -> Result<()> {
        self.data.put_u32(value);
        Ok(())
    }

    /// Write a 64-bit unsigned integer in network byte order
    pub fn write_u64(&mut self, value: u64) -> Result<()> {
        self.data.put_u64(value);
        Ok(())
    }

    /// Write a 32-bit signed integer in network byte order
    pub fn write_i32(&mut self, value: i32) -> Result<()> {
        self.data.put_i32(value);
        Ok(())
    }

    /// Write a 64-bit signed integer in network byte order
    pub fn write_i64(&mut self, value: i64) -> Result<()> {
        self.data.put_i64(value);
        Ok(())
    }

    /// Write raw bytes
    pub fn write_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.data.put_slice(bytes);
        Ok(())
    }

    /// Write a null-terminated string
    pub fn write_cstring(&mut self, s: &str) -> Result<()> {
        self.data.put_slice(s.as_bytes());
        self.data.put_u8(0);
        Ok(())
    }

    /// Write a string without null terminator
    pub fn write_string(&mut self, s: &str) -> Result<()> {
        self.data.put_slice(s.as_bytes());
        Ok(())
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

        writer.write_u8(0x01).unwrap();
        writer.write_u16(0x0203).unwrap();
        writer.write_u32(0x04050607).unwrap();

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
    fn test_buffer_writer_strings() {
        let mut writer = BufferWriter::new();

        writer.write_cstring("hello").unwrap();
        writer.write_string("world").unwrap();

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
    fn test_buffer_reader_from_vec() {
        let data = vec![0x01, 0x02, 0x03];
        let mut reader = BufferReader::from_vec(data);

        assert_eq!(reader.read_u8().unwrap(), 0x01);
        assert_eq!(reader.remaining(), 2);
    }

    #[test]
    fn test_buffer_reader_skip_message_type() {
        let data = [0x42, 0x01, 0x02, 0x03]; // 'B' message type
        let mut reader = BufferReader::new(&data);

        reader.skip_message_type().unwrap();
        assert_eq!(reader.read_u8().unwrap(), 0x01);
    }

    #[test]
    fn test_buffer_writer_signed_integers() {
        let mut writer = BufferWriter::new();

        writer.write_i16(-2).unwrap();
        writer.write_i32(-2).unwrap();
        writer.write_i64(-2).unwrap();

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
        writer.write_u64(0x0102030405060708).unwrap();
        assert_eq!(writer.bytes_written(), 8);
    }

    #[test]
    fn test_buffer_writer_bytes() {
        let mut writer = BufferWriter::new();
        writer.write_bytes(&[0x01, 0x02, 0x03]).unwrap();
        writer.write_bytes(&[0x04, 0x05]).unwrap();

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
}
