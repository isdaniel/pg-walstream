//! PostgreSQL `COPY ... TO STDOUT` TEXT-format decoder.
//!
//! The initial-snapshot helper reads table contents with `COPY <table> TO STDOUT`
//! and must produce values that are **byte-identical** to what `pgoutput` emits in
//! text mode, so that a snapshot row and a streamed row deserialize through the
//! exact same path ([`crate::deserializer`], [`crate::handler::WalTable`]).
//!
//! The two wire formats differ in exactly one respect:
//!
//! | | framing | escaping |
//! |---|---|---|
//! | `pgoutput` text tuple | length-prefixed | none |
//! | `COPY` TEXT | tab/newline delimited | backslash escapes |
//!
//! So this module is the unescaper `pgoutput` never needed. Everything downstream
//! ([`TupleData::into_row_data`](crate::protocol::TupleData::into_row_data)) is
//! shared verbatim.
//!
//! # Why a raw newline is unambiguously a row terminator
//!
//! `COPY ... TO STDOUT` in TEXT format escapes every newline in the data as `\n`
//! (two bytes: `0x5C 0x6A`) and every tab as `\t`. A backslash in the data is
//! emitted as `\\`. Therefore a raw `0x0A` can only be a row terminator and a raw
//! `0x09` can only be a field separator — no escape-state tracking is needed when
//! splitting. This is what makes the zero-copy fast path below correct.
//!
//! # Zero-copy
//!
//! A field with no backslash in it (the overwhelming majority) is returned as a
//! [`Bytes`] slice of the original `CopyData` frame, with no allocation and no
//! copy. Only an escaped field allocates.
//!

use crate::error::{ReplicationError, Result};
use crate::prelude::*;
use crate::protocol::ColumnData;
use alloc::collections::VecDeque;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use smallvec::SmallVec;

/// The two-byte sequence PostgreSQL uses for SQL NULL in COPY TEXT format.
const NULL_MARKER: &[u8] = b"\\N";

/// The `\.` line PostgreSQL uses to terminate `COPY FROM STDIN`. It is never sent
/// for `COPY TO STDOUT`, but is recognised defensively so it can never be mistaken
/// for a data row.
const END_OF_DATA_MARKER: &[u8] = b"\\.";

/// Accumulates `CopyData` frames and yields complete, still-escaped rows.
///
/// The COPY sub-protocol makes no guarantee about the relationship between
/// `CopyData` frames and rows: a frame may carry a partial row, exactly one row,
/// or several. This decoder buffers across that boundary.
#[derive(Debug, Default)]
pub(crate) struct TextRowDecoder {
    /// Frames pushed but not yet fully consumed.
    queue: VecDeque<Bytes>,
    /// A partial row carried over from earlier frames. Empty on the fast path.
    carry: BytesMut,
}

impl TextRowDecoder {
    /// Create an empty decoder.
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            carry: BytesMut::new(),
        }
    }

    /// Hand a `CopyData` payload to the decoder. Empty frames are dropped.
    #[inline]
    pub(crate) fn push_frame(&mut self, frame: Bytes) {
        if !frame.is_empty() {
            self.queue.push_back(frame);
        }
    }

    /// The next complete line, with the terminator (and any trailing `\r`)
    /// stripped.
    ///
    /// Returns `None` when more frames are needed. A `None` return leaves the
    /// frame queue empty; anything left over lives in the carry buffer and is
    /// reported by [`finish`](Self::finish).
    pub(crate) fn next_line(&mut self) -> Option<Bytes> {
        loop {
            // Look before mutating so the queue borrow ends before `pop_front`.
            let terminator = memchr::memchr(b'\n', self.queue.front()?);

            let Some(idx) = terminator else {
                // No terminator in this frame: the row spans frames, so it has to
                // be copied into the carry buffer. Off the fast path by definition.
                let frame = self.queue.pop_front()?;
                self.carry.extend_from_slice(&frame);
                continue;
            };

            let front = self
                .queue
                .front_mut()
                .expect("front() succeeded immediately above");

            let line = if self.carry.is_empty() {
                // Fast path: the whole row is inside this frame, so it is a slice
                // of it — no copy, one refcount bump.
                let line = front.slice(..idx);
                front.advance(idx + 1);
                line
            } else {
                self.carry.extend_from_slice(&front[..idx]);
                front.advance(idx + 1);
                self.carry.split().freeze()
            };

            if front.is_empty() {
                self.queue.pop_front();
            }

            let line = strip_trailing_cr(line);

            // `COPY TO STDOUT` never emits the end-of-data marker, but a stray one
            // must not be decoded as a row of data.
            if line == END_OF_DATA_MARKER {
                tracing::debug!("ignoring end-of-data marker in COPY TEXT stream");
                continue;
            }

            return Some(line);
        }
    }

    /// Assert the stream ended on a row boundary.
    ///
    /// Call after the server's `CopyDone`. A partial row here means the COPY was
    /// truncated, which would otherwise be silently dropped.
    pub(crate) fn finish(&self) -> Result<()> {
        if self.carry.is_empty() {
            Ok(())
        } else {
            Err(ReplicationError::protocol(format!(
                "COPY stream ended mid-row: {} trailing bytes with no row terminator",
                self.carry.len()
            )))
        }
    }
}

/// Strip one trailing `\r`, if present.
///
/// Safe for the same reason raw newlines are unambiguous: a `\r` in the *data* is
/// escaped as `\r`, so a raw `0x0D` immediately before the terminator can only be
/// a line-ending artefact.
#[inline]
fn strip_trailing_cr(line: Bytes) -> Bytes {
    match line.last() {
        Some(&b'\r') => line.slice(..line.len() - 1),
        _ => line,
    }
}

/// Split one decoded line into exactly `column_count` columns.
///
/// Fields are slices of `line`, so an escape-free row allocates nothing beyond the
/// returned `SmallVec` (which is inline for the ≤ 16 columns that cover almost
/// every table).
pub(crate) fn decode_line(line: &Bytes, column_count: usize) -> Result<SmallVec<[ColumnData; 16]>> {
    // A zero-column table is legal in PostgreSQL and copies as empty lines.
    if column_count == 0 {
        return if line.is_empty() {
            Ok(SmallVec::new())
        } else {
            Err(column_count_err(0, 1))
        };
    }

    let mut columns: SmallVec<[ColumnData; 16]> = SmallVec::with_capacity(column_count);
    let mut start = 0usize;

    loop {
        match memchr::memchr(b'\t', &line[start..]) {
            Some(offset) => {
                let end = start + offset;
                columns.push(decode_field(line.slice(start..end)));
                start = end + 1;
                // Bail as soon as the row is too wide rather than walking a
                // malformed line to its end.
                if columns.len() > column_count {
                    return Err(column_count_err(column_count, columns.len()));
                }
            }
            None => {
                columns.push(decode_field(line.slice(start..)));
                break;
            }
        }
    }

    if columns.len() != column_count {
        return Err(column_count_err(column_count, columns.len()));
    }

    Ok(columns)
}

#[cold]
#[inline(never)]
fn column_count_err(expected: usize, got: usize) -> ReplicationError {
    ReplicationError::protocol(format!("COPY row has {got} columns, expected {expected}"))
}

/// Decode one raw (still-escaped) field into a [`ColumnData`].
///
/// The NULL test runs on the **raw** bytes, before unescaping: only a field that is
/// exactly `\N` is SQL NULL. A literal `\N` in the data arrives on the wire as
/// `\\N` and therefore decodes to the two-character *text* `\N`. A zero-length
/// field is the empty *string*, never NULL.
#[inline]
fn decode_field(raw: Bytes) -> ColumnData {
    if raw == NULL_MARKER {
        ColumnData::null()
    } else {
        ColumnData::text_bytes(unescape_field(raw))
    }
}

/// Reverse PostgreSQL's COPY TEXT escaping.
///
/// Follows `CopyReadAttributesText` in `src/backend/commands/copyfromparse.c` for
/// every escape sequence, with one deliberate divergence on the last row:
///
/// | input | output |
/// |---|---|
/// | `\b` `\f` `\n` `\r` `\t` `\v` | `0x08` `0x0C` `0x0A` `0x0D` `0x09` `0x0B` |
/// | `\\` | `\` |
/// | `\` + 1–3 octal digits | that byte, wrapping (`\400` → `0x00`) |
/// | `\x` + 1–2 hex digits | that byte |
/// | `\x` not followed by a hex digit | literal `x` |
/// | `\` + any other character | that character verbatim |
/// | lone `\` at end of line | `\` — PostgreSQL **drops** it (see below) |
///
/// `CopyReadAttributesText` leaves its scan loop as soon as a backslash is the
/// final byte of the *line*, which happens before the loop appends the pending
/// byte to the output, so the backslash is discarded. This decoder emits it
/// literally rather than silently losing a byte. Mid-line the two differ in kind
/// rather than in degree: this decoder splits on raw delimiters before
/// unescaping, so a backslash before a delimiter ends a field here, whereas
/// PostgreSQL reads the delimiter as escaped and merges the two fields. Both
/// differences are unreachable for well-formed server output:
/// `CopyAttributeOutText` escapes every backslash in the data as `\\`, so
/// `COPY ... TO STDOUT` never emits a lone one.
///
/// Returns `raw` **unchanged** — no allocation, no copy — when it contains no
/// backslash.
fn unescape_field(raw: Bytes) -> Bytes {
    let Some(first) = memchr::memchr(b'\\', &raw) else {
        // Fast path: nothing to unescape.
        return raw;
    };

    let src = &raw[..];
    let mut out = BytesMut::with_capacity(raw.len());
    out.extend_from_slice(&src[..first]);

    let mut pos = first;
    while pos < src.len() {
        if src[pos] != b'\\' {
            out.put_u8(src[pos]);
            pos += 1;
            continue;
        }

        pos += 1;
        if pos >= src.len() {
            // Trailing lone backslash: emit it literally rather than dropping it
            // the way `CopyReadAttributesText` does — see this function's docs.
            out.put_u8(b'\\');
            break;
        }

        let escaped = src[pos];
        pos += 1;

        match escaped {
            b'b' => out.put_u8(0x08),
            b'f' => out.put_u8(0x0C),
            b'n' => out.put_u8(0x0A),
            b'r' => out.put_u8(0x0D),
            b't' => out.put_u8(0x09),
            b'v' => out.put_u8(0x0B),
            b'\\' => out.put_u8(b'\\'),
            b'0'..=b'7' => {
                // Up to three octal digits. Accumulating in a `u8` reproduces
                // PostgreSQL's truncation to one byte, so `\400` yields `0x00`.
                let mut value = escaped - b'0';
                let mut digits = 1;
                while digits < 3 && pos < src.len() && src[pos].is_ascii_digit() && src[pos] < b'8'
                {
                    value = value.wrapping_mul(8).wrapping_add(src[pos] - b'0');
                    pos += 1;
                    digits += 1;
                }
                out.put_u8(value);
            }
            b'x' => {
                // `hex_value` *is* the digit test, so there is no separate classification step that could drift out of sync with it.
                if let Some(hi) = src.get(pos).copied().and_then(hex_value) {
                    pos += 1;
                    let mut value = hi;
                    if let Some(lo) = src.get(pos).copied().and_then(hex_value) {
                        value = value.wrapping_mul(16).wrapping_add(lo);
                        pos += 1;
                    }
                    out.put_u8(value);
                } else {
                    // PostgreSQL falls through to emitting the character that
                    // followed the backslash.
                    out.put_u8(b'x');
                }
            }
            other => out.put_u8(other),
        }
    }

    out.freeze()
}

/// Value of one ASCII hex digit, or `None` if `byte` is not one.
///
/// Returning `Option` keeps the range check and the subtraction in the same
/// `match`: every arm subtracts a bound it has just tested, so no caller can make
/// this underflow by forgetting to classify the byte first.
#[inline]
fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    fn text_of(col: &ColumnData) -> Vec<u8> {
        assert_eq!(col.data_type, b't', "expected a text column");
        col.as_bytes().to_vec()
    }

    // ---- unescape_field ----------------------------------------------------

    #[test]
    fn unescape_without_backslash_is_zero_copy() {
        let raw = bytes("hello world");
        let ptr = raw.as_ptr();
        let out = unescape_field(raw);
        assert_eq!(out.as_ptr(), ptr, "escape-free field must not be copied");
        assert_eq!(&out[..], b"hello world");
    }

    #[test]
    fn unescape_empty_is_zero_copy() {
        let out = unescape_field(Bytes::new());
        assert!(out.is_empty());
    }

    #[test]
    fn unescape_single_char_escapes() {
        let out = unescape_field(bytes(r"\b\f\n\r\t\v\\"));
        assert_eq!(&out[..], &[0x08, 0x0C, 0x0A, 0x0D, 0x09, 0x0B, 0x5C]);
    }

    #[test]
    fn unescape_leading_text_is_preserved() {
        let out = unescape_field(bytes(r"abc\tdef"));
        assert_eq!(&out[..], b"abc\tdef");
    }

    #[test]
    fn unescape_octal_one_two_three_digits() {
        assert_eq!(&unescape_field(bytes(r"\1"))[..], &[0o1]);
        assert_eq!(&unescape_field(bytes(r"\12"))[..], &[0o12]);
        assert_eq!(&unescape_field(bytes(r"\101"))[..], b"A");
        assert_eq!(&unescape_field(bytes(r"\377"))[..], &[0xFF]);
    }

    #[test]
    fn unescape_octal_stops_at_three_digits() {
        // `\1011` is `\101` ("A") followed by a literal '1'.
        assert_eq!(&unescape_field(bytes(r"\1011"))[..], b"A1");
    }

    #[test]
    fn unescape_octal_stops_at_non_octal_digit() {
        // '8' and '9' are not octal digits.
        assert_eq!(&unescape_field(bytes(r"\18"))[..], &[0o1, b'8']);
        assert_eq!(&unescape_field(bytes(r"\79"))[..], &[0o7, b'9']);
    }

    #[test]
    fn unescape_octal_overflow_wraps_like_pg() {
        // PostgreSQL truncates the accumulated value to one byte.
        assert_eq!(&unescape_field(bytes(r"\400"))[..], &[0x00]);
    }

    #[test]
    fn unescape_hex_one_and_two_digits() {
        assert_eq!(&unescape_field(bytes(r"\x4"))[..], &[0x04]);
        assert_eq!(&unescape_field(bytes(r"\x41"))[..], b"A");
        assert_eq!(&unescape_field(bytes(r"\xFF"))[..], &[0xFF]);
        assert_eq!(&unescape_field(bytes(r"\xaB"))[..], &[0xAB]);
    }

    #[test]
    fn unescape_hex_stops_at_two_digits() {
        assert_eq!(&unescape_field(bytes(r"\x414"))[..], b"A4");
    }

    #[test]
    fn unescape_x_without_hex_digit_yields_literal_x() {
        assert_eq!(&unescape_field(bytes(r"\xZ"))[..], b"xZ");
        assert_eq!(&unescape_field(bytes(r"\x"))[..], b"x");
    }

    #[test]
    fn unescape_unknown_escape_yields_the_char() {
        assert_eq!(&unescape_field(bytes(r"\q"))[..], b"q");
        assert_eq!(&unescape_field(bytes(r"\'"))[..], b"'");
        assert_eq!(&unescape_field(bytes(r"\N"))[..], b"N");
    }

    #[test]
    fn hex_value_accepts_both_cases_and_rejects_the_rest() {
        assert_eq!(hex_value(b'0'), Some(0));
        assert_eq!(hex_value(b'9'), Some(9));
        assert_eq!(hex_value(b'a'), Some(10));
        assert_eq!(hex_value(b'F'), Some(15));
        // Not a regression test: the call site already classified the byte, so
        // these were unreachable. They pin the contract for any future caller.
        assert_eq!(hex_value(b'/'), None);
        assert_eq!(hex_value(0x00), None);
        assert_eq!(hex_value(b'g'), None);
        assert_eq!(hex_value(b'Z'), None);
    }

    #[test]
    fn unescape_trailing_lone_backslash() {
        // Deliberate divergence from `CopyReadAttributesText`, which drops it.
        // Unreachable for real server output, which escapes backslashes as `\\`.
        assert_eq!(&unescape_field(bytes(r"abc\"))[..], b"abc\\");
    }

    #[test]
    fn bytea_hex_survives_byte_identical() {
        // pgoutput text mode emits bytea as `\x48656c6c6f`. COPY TEXT escapes the
        // backslash, so the wire bytes are `\\x48656c6c6f`. Unescaping must give
        // back exactly what pgoutput would have produced — this is the whole
        // premise of decoding COPY in TEXT rather than BINARY format.
        let out = unescape_field(bytes(r"\\x48656c6c6f"));
        assert_eq!(&out[..], br"\x48656c6c6f");
    }

    // ---- decode_field ------------------------------------------------------

    #[test]
    fn null_marker_is_exactly_backslash_n() {
        assert_eq!(decode_field(bytes(r"\N")).data_type, b'n');
        // A literal `\N` in the data arrives escaped, and is text.
        assert_eq!(text_of(&decode_field(bytes(r"\\N"))), br"\N".to_vec());
        // `\NX` is not the NULL marker; the unknown escape yields 'N'.
        assert_eq!(text_of(&decode_field(bytes(r"\NX"))), b"NX".to_vec());
    }

    #[test]
    fn empty_field_is_empty_string_not_null() {
        let col = decode_field(Bytes::new());
        assert_eq!(col.data_type, b't');
        assert!(col.as_bytes().is_empty());
    }

    // ---- decode_line -------------------------------------------------------

    #[test]
    fn decode_line_splits_on_raw_tabs() {
        let line = bytes("1\tAlice\t30");
        let cols = decode_line(&line, 3).unwrap();
        assert_eq!(text_of(&cols[0]), b"1".to_vec());
        assert_eq!(text_of(&cols[1]), b"Alice".to_vec());
        assert_eq!(text_of(&cols[2]), b"30".to_vec());
    }

    #[test]
    fn decode_line_keeps_leading_and_trailing_empty_fields() {
        let line = bytes("\tmid\t");
        let cols = decode_line(&line, 3).unwrap();
        assert!(cols[0].as_bytes().is_empty());
        assert_eq!(text_of(&cols[1]), b"mid".to_vec());
        assert!(cols[2].as_bytes().is_empty());
    }

    #[test]
    fn decode_line_single_column() {
        let line = bytes("only");
        assert_eq!(
            text_of(&decode_line(&line, 1).unwrap()[0]),
            b"only".to_vec()
        );
    }

    #[test]
    fn decode_line_field_slices_are_zero_copy() {
        let line = bytes("abc\tdef");
        let base = line.as_ptr();
        let cols = decode_line(&line, 2).unwrap();
        assert_eq!(cols[0].as_bytes().as_ptr(), base);
        assert_eq!(cols[1].as_bytes().as_ptr(), unsafe { base.add(4) });
    }

    #[test]
    fn decode_line_rejects_too_few_columns() {
        let err = decode_line(&bytes("a\tb"), 3).unwrap_err();
        assert!(format!("{err}").contains("expected 3"), "{err}");
    }

    #[test]
    fn decode_line_rejects_too_many_columns() {
        let err = decode_line(&bytes("a\tb\tc\td"), 2).unwrap_err();
        assert!(format!("{err}").contains("expected 2"), "{err}");
    }

    #[test]
    fn decode_line_zero_columns() {
        assert!(decode_line(&Bytes::new(), 0).unwrap().is_empty());
        assert!(decode_line(&bytes("x"), 0).is_err());
    }

    #[test]
    fn decode_line_unescapes_embedded_delimiters() {
        // A tab and a newline inside a value arrive escaped, so they must not
        // split the row, and must decode back to the raw control bytes.
        let line = bytes(r"a\tb\nc");
        let cols = decode_line(&line, 1).unwrap();
        assert_eq!(text_of(&cols[0]), b"a\tb\nc".to_vec());
    }

    // ---- TextRowDecoder ----------------------------------------------------

    fn collect_lines(frames: &[&[u8]]) -> Vec<Vec<u8>> {
        let mut dec = TextRowDecoder::new();
        let mut out = Vec::new();
        for f in frames {
            dec.push_frame(Bytes::copy_from_slice(f));
            while let Some(line) = dec.next_line() {
                out.push(line.to_vec());
            }
        }
        out
    }

    #[test]
    fn decoder_one_row_per_frame() {
        let lines = collect_lines(&[b"a\tb\n", b"c\td\n"]);
        assert_eq!(lines, vec![b"a\tb".to_vec(), b"c\td".to_vec()]);
    }

    #[test]
    fn decoder_many_rows_in_one_frame() {
        let lines = collect_lines(&[b"1\n2\n3\n"]);
        assert_eq!(lines, vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]);
    }

    #[test]
    fn decoder_row_split_across_frames_mid_field() {
        let lines = collect_lines(&[b"ab", b"cd\n"]);
        assert_eq!(lines, vec![b"abcd".to_vec()]);
    }

    #[test]
    fn decoder_row_split_across_frames_mid_escape() {
        // The split lands between the backslash and the escaped character.
        let lines = collect_lines(&[b"a\\", b"tb\n"]);
        assert_eq!(lines, vec![b"a\\tb".to_vec()]);
        let cols = decode_line(&Bytes::copy_from_slice(&lines[0]), 1).unwrap();
        assert_eq!(text_of(&cols[0]), b"a\tb".to_vec());
    }

    #[test]
    fn decoder_row_split_exactly_at_terminator() {
        let lines = collect_lines(&[b"ab", b"\n"]);
        assert_eq!(lines, vec![b"ab".to_vec()]);
    }

    #[test]
    fn decoder_row_spanning_three_frames() {
        let lines = collect_lines(&[b"a", b"b", b"c\n"]);
        assert_eq!(lines, vec![b"abc".to_vec()]);
    }

    #[test]
    fn decoder_strips_trailing_cr() {
        let lines = collect_lines(&[b"a\tb\r\n"]);
        assert_eq!(lines, vec![b"a\tb".to_vec()]);
    }

    #[test]
    fn decoder_empty_line_is_yielded() {
        let lines = collect_lines(&[b"\n"]);
        assert_eq!(lines, vec![Vec::<u8>::new()]);
    }

    #[test]
    fn decoder_empty_frames_are_dropped() {
        let mut dec = TextRowDecoder::new();
        dec.push_frame(Bytes::new());
        assert!(dec.next_line().is_none());
        dec.finish().unwrap();
    }

    #[test]
    fn decoder_ignores_backslash_dot_line() {
        let lines = collect_lines(&[b"a\n\\.\nb\n"]);
        assert_eq!(lines, vec![b"a".to_vec(), b"b".to_vec()]);
    }

    #[test]
    fn decoder_finish_ok_on_empty_copy() {
        let dec = TextRowDecoder::new();
        dec.finish().unwrap();
    }

    #[test]
    fn decoder_finish_errors_on_partial_row() {
        let mut dec = TextRowDecoder::new();
        dec.push_frame(Bytes::from_static(b"unterminated"));
        assert!(dec.next_line().is_none());
        let err = dec.finish().unwrap_err();
        assert!(format!("{err}").contains("mid-row"), "{err}");
    }

    #[test]
    fn decoder_fast_path_line_is_a_slice_of_the_frame() {
        let frame = Bytes::from_static(b"abc\n");
        let base = frame.as_ptr();
        let mut dec = TextRowDecoder::new();
        dec.push_frame(frame);
        let line = dec.next_line().unwrap();
        assert_eq!(line.as_ptr(), base, "single-frame row must not be copied");
    }

    #[test]
    fn decoder_default_matches_new() {
        let mut dec = TextRowDecoder::default();
        dec.push_frame(Bytes::from_static(b"x\n"));
        assert_eq!(&dec.next_line().unwrap()[..], b"x");
    }

    // ---- end-to-end into RowData ------------------------------------------

    #[test]
    fn tuple_into_row_data_matches_pgoutput_shape() {
        use crate::protocol::{ColumnInfo, RelationInfo, TupleData};

        let relation = RelationInfo::new(
            42,
            "public".to_string(),
            "users".to_string(),
            b'd',
            vec![
                ColumnInfo::new(0, "id".to_string(), 23, -1),
                ColumnInfo::new(0, "name".to_string(), 25, -1),
                ColumnInfo::new(0, "note".to_string(), 25, -1),
            ],
        );

        let line = bytes("7\tAlice\t\\N");
        let cols = decode_line(&line, 3).unwrap();
        let row = TupleData::from_smallvec(cols).into_row_data(&relation);

        assert_eq!(row.get("id").unwrap().as_str().unwrap(), "7");
        assert_eq!(row.get("name").unwrap().as_str().unwrap(), "Alice");
        assert!(matches!(
            row.get("note").unwrap(),
            crate::column_value::ColumnValue::Null
        ));
    }

    // ---- property ----------------------------------------------------------

    /// Escape a byte string the way `COPY ... TO STDOUT` does.
    fn escape_field(raw: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(raw.len());
        for &b in raw {
            match b {
                0x5C => out.extend_from_slice(br"\\"),
                0x08 => out.extend_from_slice(br"\b"),
                0x0C => out.extend_from_slice(br"\f"),
                0x0A => out.extend_from_slice(br"\n"),
                0x0D => out.extend_from_slice(br"\r"),
                0x09 => out.extend_from_slice(br"\t"),
                0x0B => out.extend_from_slice(br"\v"),
                other => out.push(other),
            }
        }
        out
    }

    proptest::proptest! {
        #[test]
        fn proptest_unescape_roundtrip(raw in proptest::collection::vec(proptest::num::u8::ANY, 0..256)) {
            let escaped = escape_field(&raw);
            let decoded = unescape_field(Bytes::from(escaped));
            proptest::prop_assert_eq!(&decoded[..], &raw[..]);
        }

        #[test]
        fn proptest_decode_line_roundtrip(
            fields in proptest::collection::vec(
                proptest::collection::vec(proptest::num::u8::ANY, 0..32),
                1..8,
            )
        ) {
            // A field that is exactly `\N` after escaping can only come from the
            // NULL marker, and raw bytes never escape to it, so every field here
            // round-trips as text.
            let escaped: Vec<Vec<u8>> = fields.iter().map(|f| escape_field(f)).collect();
            let line = Bytes::from(escaped.join(&b'\t'));
            let cols = decode_line(&line, fields.len()).unwrap();
            for (col, expected) in cols.iter().zip(&fields) {
                proptest::prop_assert_eq!(col.data_type, b't');
                proptest::prop_assert_eq!(col.as_bytes(), &expected[..]);
            }
        }
    }
}
