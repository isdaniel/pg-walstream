//! PostgreSQL ErrorResponse parsing.

/// Parsed error/notice fields from a PostgreSQL ErrorResponse or NoticeResponse.
#[derive(Debug, Clone)]
pub struct PgErrorFields {
    pub severity: String,
    pub code: String,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
}

impl std::fmt::Display for PgErrorFields {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {} (SQLSTATE {})",
            self.severity, self.message, self.code
        )?;
        if let Some(ref detail) = self.detail {
            write!(f, "\nDETAIL: {detail}")?;
        }
        if let Some(ref hint) = self.hint {
            write!(f, "\nHINT: {hint}")?;
        }
        Ok(())
    }
}

/// Parse the payload of an ErrorResponse ('E') or NoticeResponse ('N') message.
///
/// Wire format: sequence of `[Byte1(field_type) String(value)\0]` terminated by `\0`.
pub fn parse_error_fields(payload: &[u8]) -> PgErrorFields {
    let mut fields = PgErrorFields {
        severity: String::new(),
        code: String::new(),
        message: String::new(),
        detail: None,
        hint: None,
    };

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
        let value = String::from_utf8_lossy(&payload[pos..pos + end]).to_string();
        pos += end + 1; // skip null terminator

        match field_type {
            b'S' | b'V' => fields.severity = value,
            b'C' => fields.code = value,
            b'M' => fields.message = value,
            b'D' => fields.detail = Some(value),
            b'H' => fields.hint = Some(value),
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
        assert_eq!(fields.code, "42P01");
        assert!(fields.message.contains("foo"));
        assert!(fields.detail.is_none());
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
        let fields = PgErrorFields {
            severity: "ERROR".to_string(),
            code: "42P01".to_string(),
            message: "relation does not exist".to_string(),
            detail: None,
            hint: None,
        };
        let display = format!("{}", fields);
        assert!(display.contains("ERROR"));
        assert!(display.contains("relation does not exist"));
        assert!(display.contains("42P01"));
    }

    #[test]
    fn test_pg_error_fields_display_with_detail_and_hint() {
        let fields = PgErrorFields {
            severity: "ERROR".to_string(),
            code: "23505".to_string(),
            message: "duplicate key".to_string(),
            detail: Some("Key (id)=(1) already exists.".to_string()),
            hint: Some("Use ON CONFLICT to handle duplicates.".to_string()),
        };
        let display = format!("{}", fields);
        assert!(display.contains("duplicate key"));
        assert!(display.contains("Key (id)=(1) already exists"));
        assert!(display.contains("Use ON CONFLICT"));
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
        assert_eq!(fields.code, "57P01");
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
