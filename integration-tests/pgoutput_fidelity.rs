//! Docker-gated fidelity tests: the encoder reproduces real PostgreSQL `pgoutput` bytes.
//!
//! Each test runs deterministic DML, captures the raw `pgoutput` message bodies
//! PostgreSQL emits (via `pg_logical_slot_get_binary_changes`, which yields bare
//! message bodies with no `XLogData` wrapper, exactly what `encode_message`
//! produces), parses each, and asserts `encode_message(parse(bytes)) == bytes`
//! against PostgreSQL's output. The text-DML test additionally pins decoded
//! values to what was inserted, so a compensating parser+encoder bug cannot hide.
//!
//! Coverage against real PG in this file: messages B/C/R/Y/I/U/D/T/M/b/P/K/r/O
//! and column tags t/n/b/u. Streaming (S/E/c/A/p and the in-stream xid prefix) is
//! not reachable through the SQL capture function (streaming is in-progress-only
//! over the replication protocol), so it is anchored against real PG in
//! `streaming_decode.rs` instead.
//!
//! Prerequisites: PostgreSQL 15+ with `wal_level = logical` and, for the
//! two-phase test, `max_prepared_transactions > 0`. Run serially (each test owns
//! a slot that decodes the whole database):
//!
//! ```bash
//! export DATABASE_URL_REGULAR="postgresql://postgres:postgres@localhost:5432/postgres"
//! cargo test --test pgoutput_fidelity -- --ignored --test-threads=1 --nocapture
//! ```

use pg_walstream::{
    encode_message_to_bytes, LogicalReplicationMessage as M, LogicalReplicationParser,
    PgReplicationConnection,
};
use std::collections::{BTreeSet, HashMap};

fn conn_string() -> String {
    std::env::var("DATABASE_URL_REGULAR")
        .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost:5432/postgres".to_string())
}

fn connect() -> PgReplicationConnection {
    PgReplicationConnection::connect(&conn_string()).expect("connect to PostgreSQL")
}

fn hex_decode(s: &str) -> Vec<u8> {
    let b = s.as_bytes();
    assert_eq!(b.len() % 2, 0, "odd hex length");
    (0..b.len() / 2)
        .map(|i| {
            let hi = (b[2 * i] as char).to_digit(16).expect("hex digit") as u8;
            let lo = (b[2 * i + 1] as char).to_digit(16).expect("hex digit") as u8;
            (hi << 4) | lo
        })
        .collect()
}

/// Capture the raw pgoutput message bodies for `slot`, filtered to `publication`.
fn capture(
    c: &mut PgReplicationConnection,
    slot: &str,
    publication: &str,
    proto: &str,
    binary: bool,
) -> Vec<Vec<u8>> {
    let binary_opt = if binary { ", 'binary', 'true'" } else { "" };
    let res = c
        .exec(&format!(
            "SELECT encode(data, 'hex') FROM pg_logical_slot_get_binary_changes(\
             '{slot}', NULL, NULL, \
             'proto_version', '{proto}', 'publication_names', '{publication}', \
             'messages', 'true'{binary_opt})"
        ))
        .expect("get binary changes");
    (0..res.ntuples())
        .map(|r| hex_decode(&res.get_value(r, 0).expect("hex value")))
        .collect()
}

/// Parse and re-encode every message, asserting byte fidelity against PostgreSQL,
/// and return the parsed messages for further inspection.
fn roundtrip(raws: &[Vec<u8>], proto: u8) -> Vec<M> {
    raws.iter()
        .map(|raw| {
            assert!(!raw.is_empty(), "empty message body");
            let parsed = LogicalReplicationParser::with_protocol_version(proto as u32)
                .parse_wal_message(raw)
                .unwrap_or_else(|e| panic!("parse failed for tag '{}': {e}", raw[0] as char))
                .message;
            let reencoded = encode_message_to_bytes(&parsed, proto);
            assert_eq!(
                &reencoded[..],
                raw.as_slice(),
                "re-encode != PostgreSQL bytes for tag '{}'",
                raw[0] as char
            );
            parsed
        })
        .collect()
}

fn tags(raws: &[Vec<u8>]) -> BTreeSet<u8> {
    raws.iter().map(|r| r[0]).collect()
}

#[test]
#[ignore = "requires PostgreSQL with wal_level=logical; run with --ignored --test-threads=1"]
fn encoder_reproduces_text_dml() {
    let mut c = connect();

    let _ = c.exec(
        "SELECT pg_drop_replication_slot('fid_slot') \
         WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'fid_slot')",
    );
    let _ = c.exec("DROP PUBLICATION IF EXISTS fid_pub");
    let _ = c.exec("DROP TABLE IF EXISTS fid_a");
    let _ = c.exec("DROP TABLE IF EXISTS fid_b");
    let _ = c.exec("DROP TYPE IF EXISTS fid_mood");

    // An enum column forces a Type ('Y') message; fid_b uses REPLICA IDENTITY FULL
    // so its UPDATE carries an 'O' old tuple (vs 'K' for fid_a's primary key).
    c.exec("CREATE TYPE fid_mood AS ENUM ('happy', 'sad')")
        .expect("create type");
    c.exec("CREATE TABLE fid_a (id int PRIMARY KEY, name text, mood fid_mood)")
        .expect("create fid_a");
    c.exec("CREATE TABLE fid_b (id int PRIMARY KEY, label text)")
        .expect("create fid_b");
    c.exec("ALTER TABLE fid_b REPLICA IDENTITY FULL")
        .expect("set replica identity full");
    c.exec("CREATE PUBLICATION fid_pub FOR TABLE fid_a, fid_b")
        .expect("create publication");
    c.exec("SELECT pg_create_logical_replication_slot('fid_slot', 'pgoutput', true)")
        .expect("create logical slot");

    // Deterministic DML covering B, C, R, Y, I, U('K'), U('O'), D('K'), T, M.
    c.exec("INSERT INTO fid_a (id, name, mood) VALUES (1, 'alice', 'happy')")
        .expect("insert fid_a");
    c.exec("INSERT INTO fid_b (id, label) VALUES (1, 'x')")
        .expect("insert fid_b");
    // Change the primary key so default identity emits a 'K' old tuple.
    c.exec("UPDATE fid_a SET id = 2, name = 'alice2' WHERE id = 1")
        .expect("update fid_a");
    c.exec("UPDATE fid_b SET label = 'y' WHERE id = 1")
        .expect("update fid_b");
    c.exec("DELETE FROM fid_a WHERE id = 2")
        .expect("delete fid_a");
    c.exec("TRUNCATE fid_a").expect("truncate fid_a");
    c.exec("SELECT pg_logical_emit_message(true, 'fidpfx', 'hello')")
        .expect("emit logical message");

    let raws = capture(&mut c, "fid_slot", "fid_pub", "1", false);
    assert!(!raws.is_empty(), "expected pgoutput messages");
    let msgs = roundtrip(&raws, 1);

    // Encoder teeth: pin decoded content to what we inserted.
    let mut relations: HashMap<u32, (String, String, Vec<String>)> = HashMap::new();
    let (mut checked_insert, mut saw_update_k, mut saw_update_o) = (false, false, false);
    for m in &msgs {
        match m {
            M::Relation {
                relation_id,
                namespace,
                relation_name,
                columns,
                ..
            } => {
                relations.insert(
                    *relation_id,
                    (
                        namespace.to_string(),
                        relation_name.to_string(),
                        columns.iter().map(|col| col.name.to_string()).collect(),
                    ),
                );
            }
            M::Insert { relation_id, tuple } => {
                if let Some((ns, name, cols)) = relations.get(relation_id) {
                    if name == "fid_a" {
                        assert_eq!(ns, "public");
                        assert_eq!(cols, &["id", "name", "mood"]);
                        let vals: Vec<String> = tuple
                            .columns
                            .iter()
                            .map(|col| col.as_str().map(|s| s.into_owned()).unwrap_or_default())
                            .collect();
                        assert_eq!(vals, ["1", "alice", "happy"], "fid_a insert values");
                        checked_insert = true;
                    }
                }
            }
            M::Update {
                key_type: Some('K'),
                ..
            } => saw_update_k = true,
            M::Update {
                key_type: Some('O'),
                old_tuple,
                ..
            } => {
                saw_update_o = true;
                assert!(
                    old_tuple.is_some(),
                    "full-identity update carries old tuple"
                );
            }
            _ => {}
        }
    }

    let seen = tags(&raws);
    for tag in b"BCRYIUDTM" {
        assert!(
            seen.contains(tag),
            "expected PostgreSQL to emit a '{}' message",
            *tag as char
        );
    }
    assert!(checked_insert, "fid_a insert values were verified");
    assert!(
        saw_update_k,
        "default-identity update produced a 'K' old tuple"
    );
    assert!(
        saw_update_o,
        "full-identity update produced an 'O' old tuple"
    );

    let _ = c.exec("DROP PUBLICATION IF EXISTS fid_pub");
    let _ = c.exec("DROP TABLE IF EXISTS fid_a");
    let _ = c.exec("DROP TABLE IF EXISTS fid_b");
    let _ = c.exec("DROP TYPE IF EXISTS fid_mood");
}

#[test]
#[ignore = "requires PostgreSQL with wal_level=logical; run with --ignored --test-threads=1"]
fn encoder_reproduces_binary_format_columns() {
    let mut c = connect();

    let _ = c.exec(
        "SELECT pg_drop_replication_slot('fid_bin_slot') \
         WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'fid_bin_slot')",
    );
    let _ = c.exec("DROP PUBLICATION IF EXISTS fid_bin_pub");
    let _ = c.exec("DROP TABLE IF EXISTS fid_bin");

    c.exec("CREATE TABLE fid_bin (id int PRIMARY KEY, n int, f float8, flag boolean, t text)")
        .expect("create table");
    c.exec("CREATE PUBLICATION fid_bin_pub FOR TABLE fid_bin")
        .expect("create publication");
    c.exec("SELECT pg_create_logical_replication_slot('fid_bin_slot', 'pgoutput', true)")
        .expect("create slot");
    c.exec("INSERT INTO fid_bin VALUES (1, 42, 2.5, true, 'hi')")
        .expect("insert");

    // `binary 'true'` makes pgoutput send column values in binary format ('b').
    let raws = capture(&mut c, "fid_bin_slot", "fid_bin_pub", "1", true);
    let msgs = roundtrip(&raws, 1);

    let saw_binary = msgs.iter().any(|m| {
        matches!(m, M::Insert { tuple, .. }
            if tuple.columns.iter().any(|col| col.data_type == b'b'))
    });
    assert!(
        saw_binary,
        "expected at least one binary-format ('b') column from PostgreSQL"
    );

    let _ = c.exec("DROP PUBLICATION IF EXISTS fid_bin_pub");
    let _ = c.exec("DROP TABLE IF EXISTS fid_bin");
}

#[test]
#[ignore = "requires PostgreSQL with wal_level=logical; run with --ignored --test-threads=1"]
fn encoder_reproduces_unchanged_toast() {
    let mut c = connect();

    let _ = c.exec(
        "SELECT pg_drop_replication_slot('fid_toast_slot') \
         WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'fid_toast_slot')",
    );
    let _ = c.exec("DROP PUBLICATION IF EXISTS fid_toast_pub");
    let _ = c.exec("DROP TABLE IF EXISTS fid_toast");

    c.exec("CREATE TABLE fid_toast (id int PRIMARY KEY, big text, small int)")
        .expect("create table");
    // EXTERNAL storage forces the large value out-of-line (TOASTed, uncompressed).
    c.exec("ALTER TABLE fid_toast ALTER COLUMN big SET STORAGE EXTERNAL")
        .expect("set storage external");
    c.exec("CREATE PUBLICATION fid_toast_pub FOR TABLE fid_toast")
        .expect("create publication");
    c.exec("SELECT pg_create_logical_replication_slot('fid_toast_slot', 'pgoutput', true)")
        .expect("create slot");

    c.exec("INSERT INTO fid_toast (id, big, small) VALUES (1, repeat('x', 8000), 1)")
        .expect("insert big value");
    // `big` is unchanged and TOASTed, so its new-tuple slot is sent as 'u'.
    c.exec("UPDATE fid_toast SET small = 2 WHERE id = 1")
        .expect("update small column");

    let raws = capture(&mut c, "fid_toast_slot", "fid_toast_pub", "1", false);
    let msgs = roundtrip(&raws, 1);

    let saw_unchanged = msgs.iter().any(|m| {
        matches!(m, M::Update { new_tuple, .. }
            if new_tuple.columns.iter().any(|col| col.data_type == b'u'))
    });
    assert!(
        saw_unchanged,
        "expected an unchanged-TOAST ('u') column in the UPDATE new tuple"
    );

    let _ = c.exec("DROP PUBLICATION IF EXISTS fid_toast_pub");
    let _ = c.exec("DROP TABLE IF EXISTS fid_toast");
}

#[test]
#[ignore = "requires PostgreSQL 15+ with wal_level=logical and max_prepared_transactions>0; \
            run with --ignored --test-threads=1"]
fn encoder_reproduces_two_phase_commit() {
    let mut c = connect();

    // Roll back any prepared transactions left by a previous failed run.
    let _ = c.exec("ROLLBACK PREPARED 'fid_gid1'");
    let _ = c.exec("ROLLBACK PREPARED 'fid_gid2'");
    let _ = c.exec(
        "SELECT pg_drop_replication_slot('fid_tp_slot') \
         WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'fid_tp_slot')",
    );
    let _ = c.exec("DROP PUBLICATION IF EXISTS fid_tp_pub");
    let _ = c.exec("DROP TABLE IF EXISTS fid_tp");

    c.exec("CREATE TABLE fid_tp (id int PRIMARY KEY, v text)")
        .expect("create table");
    c.exec("CREATE PUBLICATION fid_tp_pub FOR TABLE fid_tp")
        .expect("create publication");
    // Fourth argument enables two-phase decoding on the slot.
    c.exec("SELECT pg_create_logical_replication_slot('fid_tp_slot', 'pgoutput', true, true)")
        .expect("create two-phase slot");

    // A prepared transaction that commits: BeginPrepare ('b'), Prepare ('P'),
    // CommitPrepared ('K').
    c.exec("BEGIN").expect("begin 1");
    c.exec("INSERT INTO fid_tp VALUES (1, 'a')")
        .expect("insert 1");
    c.exec("PREPARE TRANSACTION 'fid_gid1'").expect("prepare 1");
    c.exec("COMMIT PREPARED 'fid_gid1'")
        .expect("commit prepared");

    // A prepared transaction that rolls back: RollbackPrepared ('r').
    c.exec("BEGIN").expect("begin 2");
    c.exec("INSERT INTO fid_tp VALUES (2, 'b')")
        .expect("insert 2");
    c.exec("PREPARE TRANSACTION 'fid_gid2'").expect("prepare 2");
    c.exec("ROLLBACK PREPARED 'fid_gid2'")
        .expect("rollback prepared");

    // proto_version 3 is required for the two-phase message set.
    let raws = capture(&mut c, "fid_tp_slot", "fid_tp_pub", "3", false);
    let _ = roundtrip(&raws, 3);

    let seen = tags(&raws);
    for tag in b"bPKr" {
        assert!(
            seen.contains(tag),
            "expected PostgreSQL to emit a two-phase '{}' message",
            *tag as char
        );
    }

    let _ = c.exec("DROP PUBLICATION IF EXISTS fid_tp_pub");
    let _ = c.exec("DROP TABLE IF EXISTS fid_tp");
}

#[test]
#[ignore = "requires PostgreSQL 16+ with wal_level=logical; run with --ignored --test-threads=1"]
fn encoder_reproduces_origin() {
    let mut c = connect();

    // The pgoutput `origin` option was added in PostgreSQL 16; skip on older servers.
    let ver: i32 = c
        .exec("SHOW server_version_num")
        .ok()
        .and_then(|r| r.get_value(0, 0).and_then(|v| v.parse().ok()))
        .unwrap_or(0);
    if ver < 160000 {
        eprintln!("skipping encoder_reproduces_origin: requires PG 16+ (server_version_num={ver})");
        return;
    }

    // Clean up leftovers from a prior run.
    let _ = c.exec("SELECT pg_replication_origin_session_reset()");
    let _ = c.exec(
        "SELECT pg_drop_replication_slot('fid_origin_slot') \
         WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'fid_origin_slot')",
    );
    let _ = c.exec(
        "SELECT pg_replication_origin_drop('fid_origin') \
         WHERE EXISTS (SELECT 1 FROM pg_replication_origin WHERE roname = 'fid_origin')",
    );
    let _ = c.exec("DROP PUBLICATION IF EXISTS fid_origin_pub");
    let _ = c.exec("DROP TABLE IF EXISTS fid_origin_t");

    c.exec("CREATE TABLE fid_origin_t (id int PRIMARY KEY)")
        .expect("create table");
    c.exec("CREATE PUBLICATION fid_origin_pub FOR TABLE fid_origin_t")
        .expect("create publication");
    c.exec("SELECT pg_create_logical_replication_slot('fid_origin_slot', 'pgoutput', true)")
        .expect("create slot");

    // Attribute a transaction to a replication origin so pgoutput prefixes its
    // first change with an ORIGIN ('O') message naming the origin.
    c.exec("SELECT pg_replication_origin_create('fid_origin')")
        .expect("create origin");
    c.exec("SELECT pg_replication_origin_session_setup('fid_origin')")
        .expect("origin session setup");
    c.exec("BEGIN").expect("begin");
    c.exec("SELECT pg_replication_origin_xact_setup('0/AABBCCDD', now())")
        .expect("origin xact setup");
    c.exec("INSERT INTO fid_origin_t (id) VALUES (1)")
        .expect("insert");
    c.exec("COMMIT").expect("commit");
    c.exec("SELECT pg_replication_origin_session_reset()")
        .expect("origin session reset");

    // Capture with origin='any' so origin-tagged changes (and the O message) are sent.
    let res = c
        .exec(
            "SELECT encode(data, 'hex') FROM pg_logical_slot_get_binary_changes(\
             'fid_origin_slot', NULL, NULL, \
             'proto_version', '1', 'publication_names', 'fid_origin_pub', 'origin', 'any')",
        )
        .expect("get binary changes");
    let raws: Vec<Vec<u8>> = (0..res.ntuples())
        .map(|r| hex_decode(&res.get_value(r, 0).expect("hex value")))
        .collect();

    // Cleanup before asserting so a failure does not leak the slot/origin.
    let _ = c.exec("SELECT pg_drop_replication_slot('fid_origin_slot')");
    let _ = c.exec("SELECT pg_replication_origin_drop('fid_origin')");

    assert!(!raws.is_empty(), "expected pgoutput messages");
    let msgs = roundtrip(&raws, 1);
    assert!(
        tags(&raws).contains(&b'O'),
        "expected an Origin ('O') message, got tags {:?}",
        tags(&raws)
    );

    // Encoder teeth: pin the decoded origin name to the one we created.
    let origin_name = msgs.iter().find_map(|m| match m {
        M::Origin { origin_name, .. } => Some(origin_name.to_string()),
        _ => None,
    });
    assert_eq!(
        origin_name.as_deref(),
        Some("fid_origin"),
        "origin name pinned to what was created"
    );
}
