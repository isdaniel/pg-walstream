//! PostgreSQL ErrorResponse parsing.

use crate::error::ServerError;

/// Parse the payload of an ErrorResponse ('E') or NoticeResponse ('N') message.
///
/// Wire format: sequence of `[Byte1(field_type) String(value)\0]` terminated by `\0`.
/// A `C` field that is not a valid SQLSTATE leaves [`ServerError::code`] `None`.
pub fn parse_error_fields(payload: &[u8]) -> ServerError {
    let mut fields = ServerError::default();

    let mut pos = 0;
    while pos < payload.len() {
        let field_type = payload[pos];
        if field_type == 0 {
            break; // terminator
        }
        pos += 1;

        let end = payload[pos..]
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(payload.len() - pos);
        let raw = &payload[pos..pos + end];
        pos += end + 1; // skip null terminator

        let value = || String::from_utf8_lossy(raw).into_owned();
        match field_type {
            b'S' | b'V' => fields.severity = value(),
            b'C' => {
                fields.code = <[u8; 5]>::try_from(raw)
                    .ok()
                    .and_then(sqlstate::SqlState::new)
            }
            b'M' => fields.message = value(),
            b'D' => fields.detail = Some(value()),
            b'H' => fields.hint = Some(value()),
            b'R' => fields.routine = Some(value()),
            _ => {} // skip unknown field types
        }
    }

    fields
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_error_fields() {
        let mut payload = Vec::new();
        payload.push(b'S');
        payload.extend_from_slice(b"ERROR\0");
        payload.push(b'C');
        payload.extend_from_slice(b"42P01\0");
        payload.push(b'M');
        payload.extend_from_slice(b"relation \"foo\" does not exist\0");
        payload.push(0); // terminator

        let fields = parse_error_fields(&payload);
        assert_eq!(fields.severity, "ERROR");
        assert_eq!(fields.code, Some(sqlstate::pg::UNDEFINED_TABLE));
        assert!(fields.message.contains("foo"));
        assert!(fields.detail.is_none());
    }

    /// A peer that sends a malformed `C` field has sent no SQLSTATE, and the
    /// fields after it still parse.
    #[test]
    fn parse_error_fields_with_an_invalid_code() {
        for code in [&b"4201\0"[..], b"42p01\0", b"423456\0", b"\0"] {
            let mut payload = b"SERROR\0C".to_vec();
            payload.extend_from_slice(code);
            payload.extend_from_slice(b"Mstill here\0\0");

            let fields = parse_error_fields(&payload);
            assert_eq!(fields.code, None, "{code:?}");
            assert_eq!(fields.message, "still here", "{code:?}");
        }
    }

    #[test]
    fn test_parse_error_fields_with_detail() {
        let mut payload = Vec::new();
        payload.push(b'S');
        payload.extend_from_slice(b"ERROR\0");
        payload.push(b'C');
        payload.extend_from_slice(b"23505\0");
        payload.push(b'M');
        payload.extend_from_slice(b"duplicate key\0");
        payload.push(b'D');
        payload.extend_from_slice(b"Key already exists\0");
        payload.push(b'H');
        payload.extend_from_slice(b"Try a different key\0");
        payload.push(0);

        let fields = parse_error_fields(&payload);
        assert_eq!(fields.detail, Some("Key already exists".to_string()));
        assert_eq!(fields.hint, Some("Try a different key".to_string()));
    }

    #[test]
    fn test_pg_error_fields_display_basic() {
        let fields = ServerError {
            severity: "ERROR".to_string(),
            code: Some(sqlstate::pg::UNDEFINED_TABLE),
            message: "relation does not exist".to_string(),
            ..Default::default()
        };
        assert_eq!(
            fields.to_string(),
            "ERROR: relation does not exist (SQLSTATE 42P01)"
        );
    }

    #[test]
    fn test_pg_error_fields_display_with_detail_and_hint() {
        let fields = ServerError {
            severity: "ERROR".to_string(),
            code: Some(sqlstate::pg::UNIQUE_VIOLATION),
            message: "duplicate key".to_string(),
            detail: Some("Key (id)=(1) already exists.".to_string()),
            hint: Some("Use ON CONFLICT to handle duplicates.".to_string()),
            ..Default::default()
        };
        assert_eq!(
            fields.to_string(),
            "ERROR: duplicate key (SQLSTATE 23505)\n\
             DETAIL: Key (id)=(1) already exists.\n\
             HINT: Use ON CONFLICT to handle duplicates."
        );
    }

    /// `'R'` is the only ErrorResponse field the backend never passes through
    /// `gettext`, so it is the only stable discriminator between two errors that
    /// share a SQLSTATE. It used to be dropped by the catch-all arm.
    #[test]
    fn parse_error_fields_captures_source_function() {
        let mut payload = Vec::new();
        payload.push(b'S');
        payload.extend_from_slice(b"ERROR\0");
        payload.push(b'C');
        payload.extend_from_slice(b"55000\0");
        payload.push(b'M');
        payload.extend_from_slice(b"cannot use replication slot \"s1\" for logical decoding\0");
        payload.push(b'D');
        payload.extend_from_slice(
            b"This replication slot is being synchronized from the primary server.\0",
        );
        payload.push(b'R');
        payload.extend_from_slice(b"CreateDecodingContext\0");
        // 'F'/'L' must keep falling through the catch-all without desynchronising.
        payload.push(b'F');
        payload.extend_from_slice(b"logical.c\0");
        payload.push(b'L');
        payload.extend_from_slice(b"609\0");
        payload.push(0);

        let fields = parse_error_fields(&payload);
        assert_eq!(fields.routine.as_deref(), Some("CreateDecodingContext"));
        assert_eq!(
            fields.code,
            Some(sqlstate::pg::OBJECT_NOT_IN_PREREQUISITE_STATE)
        );
        assert!(fields.detail.is_some());
        assert!(format!("{fields}").contains("ROUTINE: CreateDecodingContext"));
    }

    /// A pooler-synthesised error, or any peer that omits `'R'`, must still parse.
    #[test]
    fn parse_error_fields_without_source_function() {
        let mut payload = Vec::new();
        payload.push(b'S');
        payload.extend_from_slice(b"ERROR\0");
        payload.push(b'C');
        payload.extend_from_slice(b"42704\0");
        payload.push(b'M');
        payload.extend_from_slice(b"replication slot \"s1\" does not exist\0");
        payload.push(0);

        let fields = parse_error_fields(&payload);
        assert!(fields.routine.is_none());
        assert_eq!(fields.code, Some(sqlstate::pg::UNDEFINED_OBJECT));
        assert!(!format!("{fields}").contains("ROUTINE"));
    }

    /// Locale guard for the classification we deliberately do NOT change.
    ///
    /// `errmsg`/`errdetail` reach the client already translated (`dgettext`
    /// under `lc_messages`, which is `PGC_SUSET` so a replication role cannot
    /// force `C`). Any future carve-out that discriminates 55000/42704 on
    /// message text would be a silent no-op on a localised cluster — worse than
    /// the status quo. This test fails the moment someone tries.
    #[test]
    fn classification_is_independent_of_lc_messages() {
        use crate::error::ReplicationError;
        let classify = |message: &str, detail: &str| {
            let fields = ServerError {
                severity: "ERROR".to_string(),
                code: Some(sqlstate::pg::OBJECT_NOT_IN_PREREQUISITE_STATE),
                message: message.to_string(),
                detail: Some(detail.to_string()),
                ..Default::default()
            };
            let msg = fields.to_string();
            ReplicationError::from_server(Some(fields), msg)
        };
        let en = classify(
            "cannot use replication slot \"s1\" for logical decoding",
            "This replication slot is being synchronized from the primary server.",
        );
        let de = classify(
            "Replikations-Slot »s1« kann nicht für logisches Dekodieren verwendet werden",
            "Dieser Replikations-Slot wird vom Primärserver synchronisiert.",
        );
        assert_eq!(
            core::mem::discriminant(&en),
            core::mem::discriminant(&de),
            "classification must not depend on the server's lc_messages"
        );
    }

    #[test]
    fn test_parse_error_fields_from_framed_message() {
        // A full ErrorResponse frame: tag 'E' + i32 length + field payload.
        // Verifies fields parse out of the framed form (payload starts at msg[5..]).
        let mut payload = Vec::new();
        payload.push(b'S');
        payload.extend_from_slice(b"FATAL\0");
        payload.push(b'C');
        payload.extend_from_slice(b"57P01\0");
        payload.push(b'M');
        payload.extend_from_slice(b"terminating connection\0");
        payload.push(0); // terminator

        let mut msg = vec![b'E'];
        let len = (4 + payload.len()) as i32;
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(&payload);

        let fields = parse_error_fields(&msg[5..]);
        assert_eq!(fields.severity, "FATAL");
        assert_eq!(fields.code, Some(sqlstate::pg::ADMIN_SHUTDOWN));
        assert!(fields.message.contains("terminating"));
    }

    #[test]
    fn test_parse_error_fields_empty_payload() {
        // Just a null terminator
        let payload = [0u8];
        let fields = parse_error_fields(&payload);
        assert!(fields.message.is_empty() || fields.severity.is_empty());
    }
}
