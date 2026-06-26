//! Pure-Rust replacement for libpq's `PGresult`.
//!
//! Provides the same public API as the libpq `PgResult` wrapper so that
//! `stream.rs` works unchanged regardless of backend.

use super::wire;

/// Result status matching the libpq `ExecStatusType` values used by callers.
#[derive(Debug, Clone, PartialEq)]
pub enum NativeResultStatus {
    CommandOk,
    TuplesOk,
    CopyBoth,
    CopyOut,
    FatalError,
    Empty,
}

/// Pure-Rust result set from a PostgreSQL simple-query response.
#[derive(Debug)]
pub struct NativePgResult {
    pub(crate) status: NativeResultStatus,
    pub(crate) columns: Vec<String>,
    pub(crate) rows: Vec<Vec<Option<Vec<u8>>>>,
    pub(crate) error_msg: Option<String>,
}

impl NativePgResult {
    pub(crate) fn new() -> Self {
        Self {
            status: NativeResultStatus::Empty,
            columns: Vec::new(),
            rows: Vec::new(),
            error_msg: None,
        }
    }

    // ── Public API (matching libpq PgResult) ─────────────────────────────

    /// Get the execution status.
    /// Returns the internal status, but callers only check via `is_ok()`.
    pub fn status(&self) -> &NativeResultStatus {
        &self.status
    }

    /// Check if the result is OK.
    pub fn is_ok(&self) -> bool {
        matches!(
            self.status,
            NativeResultStatus::CommandOk
                | NativeResultStatus::TuplesOk
                | NativeResultStatus::CopyBoth
                | NativeResultStatus::CopyOut
        )
    }

    /// Get number of tuples (rows).
    pub fn ntuples(&self) -> i32 {
        self.rows.len() as i32
    }

    /// Get number of fields (columns).
    pub fn nfields(&self) -> i32 {
        self.columns.len() as i32
    }

    /// Get a field value as string. Lossy for non-UTF-8 data — use [`get_bytes`](Self::get_bytes) for lossless access.
    pub fn get_value(&self, row: i32, col: i32) -> Option<String> {
        let bytes = self.get_bytes(row, col)?;
        Some(String::from_utf8_lossy(bytes).into_owned())
    }

    /// Get a field value as raw bytes, zero-copy and lossless.
    ///
    /// Returns `None` for SQL NULL and for out-of-bounds `row`/`col`.
    /// Unlike [`get_value`](Self::get_value) this never mangles non-UTF-8 data.
    pub fn get_bytes(&self, row: i32, col: i32) -> Option<&[u8]> {
        if row < 0 || col < 0 {
            return None;
        }
        self.rows.get(row as usize)?.get(col as usize)?.as_deref()
    }

    /// Get error message if any.
    pub fn error_message(&self) -> Option<String> {
        self.error_msg.clone()
    }

    // ── Internal parsing helpers ─────────────────────────────────────────

    /// Parse a RowDescription ('T') message payload (after the 5-byte header).
    pub(crate) fn parse_row_description(&mut self, payload: &[u8]) {
        if payload.len() < 2 {
            return;
        }
        let nfields = i16::from_be_bytes(payload[0..2].try_into().unwrap()) as usize;
        self.columns.clear();

        let mut pos = 2;
        for _ in 0..nfields {
            if pos >= payload.len() {
                break;
            }
            let (name, consumed) = wire::read_cstring(&payload[pos..]);
            self.columns.push(name.to_string());
            pos += consumed;
            // Skip: table OID (4) + attr num (2) + type OID (4) + type size (2)
            //        + type modifier (4) + format code (2) = 18 bytes
            pos += 18;
        }

        if self.status == NativeResultStatus::Empty {
            self.status = NativeResultStatus::TuplesOk;
        }
    }

    /// Parse a DataRow ('D') message payload (after the 5-byte header).
    pub(crate) fn parse_data_row(&mut self, payload: &[u8]) {
        if payload.len() < 2 {
            return;
        }
        let ncols = i16::from_be_bytes(payload[0..2].try_into().unwrap()) as usize;
        let mut row = Vec::with_capacity(ncols);

        let mut pos = 2;
        for _ in 0..ncols {
            if pos + 4 > payload.len() {
                break;
            }
            let col_len = i32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap());
            pos += 4;

            if col_len < 0 {
                // -1 is SQL NULL; any other negative is malformed wire data, treat it as absent rather than sign-extending into a huge usize.
                row.push(None);
            } else {
                let len = col_len as usize;
                match pos.checked_add(len) {
                    Some(end) if end <= payload.len() => {
                        row.push(Some(payload[pos..end].to_vec()));
                        pos = end;
                    }
                    // Truncated or overflowing length: stop parsing this row.
                    _ => {
                        row.push(None);
                        break;
                    }
                }
            }
        }

        self.rows.push(row);
    }
}

// Implement Send — NativePgResult is just data, no raw pointers.
// (This matches the `unsafe impl Send for PgResult` on the libpq side.)

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_native_result_empty() {
        let result = NativePgResult::new();
        assert!(!result.is_ok());
        assert_eq!(result.ntuples(), 0);
        assert_eq!(result.nfields(), 0);
    }

    #[test]
    fn test_parse_row_description() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&2i16.to_be_bytes()); // 2 fields
                                                        // Field 1: "systemid"
        payload.extend_from_slice(b"systemid\0");
        payload.extend_from_slice(&[0u8; 18]); // metadata
                                               // Field 2: "timeline"
        payload.extend_from_slice(b"timeline\0");
        payload.extend_from_slice(&[0u8; 18]); // metadata

        let mut result = NativePgResult::new();
        result.parse_row_description(&payload);
        assert_eq!(result.nfields(), 2);
        assert_eq!(result.columns[0], "systemid");
        assert_eq!(result.columns[1], "timeline");
        assert_eq!(result.status, NativeResultStatus::TuplesOk);
    }

    #[test]
    fn test_parse_data_row() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&2i16.to_be_bytes()); // 2 columns
                                                        // Col 0: "abc"
        payload.extend_from_slice(&3i32.to_be_bytes());
        payload.extend_from_slice(b"abc");
        // Col 1: NULL
        payload.extend_from_slice(&(-1i32).to_be_bytes());

        let mut result = NativePgResult::new();
        result.columns = vec!["a".into(), "b".into()];
        result.parse_data_row(&payload);
        assert_eq!(result.get_value(0, 0), Some("abc".to_string()));
        assert_eq!(result.get_value(0, 1), None);
    }

    #[test]
    fn test_is_ok_variants() {
        let mut r = NativePgResult::new();
        r.status = NativeResultStatus::CommandOk;
        assert!(r.is_ok());
        r.status = NativeResultStatus::TuplesOk;
        assert!(r.is_ok());
        r.status = NativeResultStatus::CopyBoth;
        assert!(r.is_ok());
        r.status = NativeResultStatus::FatalError;
        assert!(!r.is_ok());
    }

    #[test]
    fn test_error_message_some() {
        let mut result = NativePgResult::new();
        result.error_msg = Some("something went wrong".to_string());
        assert_eq!(
            result.error_message(),
            Some("something went wrong".to_string())
        );
    }

    #[test]
    fn test_error_message_none() {
        let result = NativePgResult::new();
        assert_eq!(result.error_message(), None);
    }

    #[test]
    fn test_get_value_out_of_bounds_row() {
        let result = NativePgResult::new();
        assert_eq!(result.get_value(0, 0), None);
        assert_eq!(result.get_value(99, 0), None);
    }

    #[test]
    fn test_get_value_out_of_bounds_col() {
        let mut result = NativePgResult::new();
        // Add a RowDescription with 1 column
        let mut rd_payload = Vec::new();
        rd_payload.extend_from_slice(&1i16.to_be_bytes()); // 1 field
        rd_payload.extend_from_slice(b"col1\0"); // name
        rd_payload.extend_from_slice(&0i32.to_be_bytes()); // table OID
        rd_payload.extend_from_slice(&0i16.to_be_bytes()); // col number
        rd_payload.extend_from_slice(&25i32.to_be_bytes()); // type OID (text)
        rd_payload.extend_from_slice(&(-1i16).to_be_bytes()); // type size
        rd_payload.extend_from_slice(&0i32.to_be_bytes()); // type modifier
        rd_payload.extend_from_slice(&0i16.to_be_bytes()); // format code
        result.parse_row_description(&rd_payload);

        // Add a DataRow with 1 column value "hello"
        let mut dr_payload = Vec::new();
        dr_payload.extend_from_slice(&1i16.to_be_bytes()); // 1 column
        let val = b"hello";
        dr_payload.extend_from_slice(&(val.len() as i32).to_be_bytes());
        dr_payload.extend_from_slice(val);
        result.parse_data_row(&dr_payload);

        // Valid access
        assert_eq!(result.get_value(0, 0), Some("hello".to_string()));
        // Out of bounds column
        assert_eq!(result.get_value(0, 1), None);
        assert_eq!(result.get_value(0, 99), None);
    }

    #[test]
    fn test_parse_data_row_truncated_payload() {
        let mut result = NativePgResult::new();
        // Payload says 5 columns but has no data
        let payload = 5i16.to_be_bytes();
        result.parse_data_row(&payload);
        // Should not panic — gracefully handle truncated data
        assert_eq!(result.ntuples(), 1);
    }

    #[test]
    fn test_parse_data_row_negative_length_is_not_null() {
        // A column length of -2 (anything negative but -1) is malformed wire
        // data. It must NOT sign-extend into a huge usize and panic on overflow.
        let mut payload = Vec::new();
        payload.extend_from_slice(&1i16.to_be_bytes()); // 1 column
        payload.extend_from_slice(&(-2i32).to_be_bytes()); // malformed length

        let mut result = NativePgResult::new();
        result.columns = vec!["b".into()];
        result.parse_data_row(&payload); // must not panic
        assert_eq!(result.ntuples(), 1);
        assert_eq!(result.get_bytes(0, 0), None);
    }

    #[test]
    fn test_get_bytes_lossless_non_utf8() {
        // A column holding bytes that are NOT valid UTF-8.
        let mut payload = Vec::new();
        payload.extend_from_slice(&1i16.to_be_bytes()); // 1 column
        let val = [0xffu8, 0x00, 0xfe];
        payload.extend_from_slice(&(val.len() as i32).to_be_bytes());
        payload.extend_from_slice(&val);

        let mut result = NativePgResult::new();
        result.columns = vec!["b".into()];
        result.parse_data_row(&payload);

        // get_bytes is exact and lossless.
        assert_eq!(result.get_bytes(0, 0), Some(&val[..]));
        // get_value mangles the same bytes through from_utf8_lossy (the win).
        assert_ne!(
            result.get_value(0, 0).unwrap().as_bytes(),
            &val[..],
            "get_value is expected to be lossy for non-UTF-8 data"
        );
    }

    #[test]
    fn test_get_bytes_null_is_none() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&1i16.to_be_bytes()); // 1 column
        payload.extend_from_slice(&(-1i32).to_be_bytes()); // SQL NULL

        let mut result = NativePgResult::new();
        result.columns = vec!["b".into()];
        result.parse_data_row(&payload);

        assert_eq!(result.get_bytes(0, 0), None);
    }

    #[test]
    fn test_get_bytes_empty_non_null() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&1i16.to_be_bytes()); // 1 column
        payload.extend_from_slice(&0i32.to_be_bytes()); // zero-length, non-NULL

        let mut result = NativePgResult::new();
        result.columns = vec!["b".into()];
        result.parse_data_row(&payload);

        assert_eq!(result.get_bytes(0, 0), Some(&[][..]));
    }

    #[test]
    fn test_get_bytes_out_of_bounds() {
        let result = NativePgResult::new();
        assert_eq!(result.get_bytes(0, 0), None);
        assert_eq!(result.get_bytes(99, 0), None);
        assert_eq!(result.get_bytes(-1, 0), None);
        assert_eq!(result.get_bytes(0, -1), None);
    }

    #[test]
    fn test_parse_row_description_zero_fields() {
        let mut result = NativePgResult::new();
        let payload = 0i16.to_be_bytes();
        result.parse_row_description(&payload);
        assert_eq!(result.nfields(), 0);
    }
}
