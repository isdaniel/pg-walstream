//! # pg_walstream External Arbitrary / Fuzzing Demo
//!
//! ## Run
//!
//! ```bash
//! cd tools/arbitrary-fuzzing
//! cargo run
//! ```

use arbitrary::Unstructured;
use pg_walstream::protocol::LogicalReplicationMessage;
use pg_walstream_arbitrary_fuzzing::generators;

fn main() {
    println!("=== pg_walstream External Arbitrary / Fuzzing Demo ===\n");
    println!("This is an INDEPENDENT binary project. The `arbitrary` crate");
    println!("is NOT a dependency of pg_walstream — only of this tool.\n");

    // Deterministic LCG-based entropy for reproducible output.
    let entropy: Vec<u8> = {
        let mut v = Vec::with_capacity(65536);
        let mut state: u64 = 0xDEAD_BEEF_CAFE_BABE;
        for _ in 0..65536 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            v.push((state >> 33) as u8);
        }
        v
    };
    let mut u = Unstructured::new(&entropy);

    // --- MessageType ---
    println!("--- MessageType ---");
    let mut seen_types = std::collections::HashSet::new();
    for _ in 0..50 {
        if let Ok(mt) = generators::arbitrary_message_type(&mut u) {
            seen_types.insert(mt as u8);
        }
    }
    println!(
        "Generated {} distinct MessageType variants out of 19",
        seen_types.len()
    );

    // --- ReplicaIdentity ---
    println!("\n--- ReplicaIdentity ---");
    let mut seen_ri = std::collections::HashSet::new();
    for _ in 0..20 {
        if let Ok(ri) = generators::arbitrary_replica_identity(&mut u) {
            let byte = ri.to_byte();
            assert!(
                byte == b'd' || byte == b'n' || byte == b'f' || byte == b'i',
                "Invalid ReplicaIdentity byte: {}",
                byte
            );
            seen_ri.insert(byte);
        }
    }
    println!(
        "Generated {} distinct ReplicaIdentity variants out of 4",
        seen_ri.len()
    );

    // --- CachePadded ---
    println!("\n--- CachePadded<u64> ---");
    for i in 0..3 {
        if let Ok(padded) = generators::arbitrary_cache_padded_u64(&mut u) {
            println!("  [{i}] CachePadded value = {}", *padded);
        }
    }

    // --- ColumnData ---
    println!("\n--- ColumnData ---");
    let mut saw_null = false;
    let mut saw_text = false;
    let mut saw_binary = false;
    let mut saw_unchanged = false;
    for _ in 0..30 {
        if let Ok(cd) = generators::arbitrary_column_data(&mut u) {
            if cd.is_null() {
                saw_null = true;
            }
            if cd.is_text() {
                saw_text = true;
            }
            if cd.is_binary() {
                saw_binary = true;
            }
            if cd.is_unchanged() {
                saw_unchanged = true;
            }
            let _bytes = cd.as_bytes();
            if cd.is_text() || cd.is_binary() {
                let _s = cd.as_str();
            }
        }
    }
    println!(
        "  null={saw_null}  text={saw_text}  binary={saw_binary}  unchanged={saw_unchanged}"
    );
    assert!(
        saw_null || saw_text || saw_binary || saw_unchanged,
        "Should produce at least one ColumnData variant"
    );

    // --- ColumnInfo ---
    println!("\n--- ColumnInfo ---");
    for i in 0..3 {
        if let Ok(ci) = generators::arbitrary_column_info(&mut u) {
            println!(
                "  [{i}] name={:?}  type_id={}  is_key={}",
                ci.name,
                ci.type_id,
                ci.is_key()
            );
        }
    }

    // --- TupleData ---
    println!("\n--- TupleData ---");
    for i in 0..3 {
        if let Ok(td) = generators::arbitrary_tuple_data(&mut u) {
            println!("  [{i}] column_count={}", td.column_count());
            for j in 0..td.column_count() {
                let _col = td.get_column(j);
            }
        }
    }

    // --- RelationInfo ---
    println!("\n--- RelationInfo ---");
    for i in 0..3 {
        if let Ok(ri) = generators::arbitrary_relation_info(&mut u) {
            println!(
                "  [{i}] full_name={:?}  cols={}  key_cols={}",
                ri.full_name(),
                ri.columns.len(),
                ri.get_key_columns().len()
            );
        }
    }

    // --- LogicalReplicationMessage ---
    println!("\n--- LogicalReplicationMessage ---");
    let mut variant_names = std::collections::HashSet::new();
    for _ in 0..100 {
        if let Ok(msg) = generators::arbitrary_logical_replication_message(&mut u) {
            let name = match msg {
                LogicalReplicationMessage::Begin { .. } => "Begin",
                LogicalReplicationMessage::Commit { .. } => "Commit",
                LogicalReplicationMessage::Relation { .. } => "Relation",
                LogicalReplicationMessage::Insert { .. } => "Insert",
                LogicalReplicationMessage::Update { .. } => "Update",
                LogicalReplicationMessage::Delete { .. } => "Delete",
                LogicalReplicationMessage::Truncate { .. } => "Truncate",
                LogicalReplicationMessage::Type { .. } => "Type",
                LogicalReplicationMessage::Origin { .. } => "Origin",
                LogicalReplicationMessage::Message { .. } => "Message",
                LogicalReplicationMessage::StreamStart { .. } => "StreamStart",
                LogicalReplicationMessage::StreamStop => "StreamStop",
                LogicalReplicationMessage::StreamCommit { .. } => "StreamCommit",
                LogicalReplicationMessage::StreamAbort { .. } => "StreamAbort",
                LogicalReplicationMessage::BeginPrepare { .. } => "BeginPrepare",
                LogicalReplicationMessage::Prepare { .. } => "Prepare",
                LogicalReplicationMessage::CommitPrepared { .. } => "CommitPrepared",
                LogicalReplicationMessage::RollbackPrepared { .. } => "RollbackPrepared",
                LogicalReplicationMessage::StreamPrepare { .. } => "StreamPrepare",
            };
            variant_names.insert(name);
        }
    }
    println!(
        "Generated {} distinct message variants out of 19: {:?}",
        variant_names.len(),
        variant_names
    );
    assert!(
        variant_names.len() > 1,
        "Should produce diverse LogicalReplicationMessage variants"
    );

    // --- TupleData.to_row_data roundtrip ---
    println!("\n--- TupleData.to_row_data roundtrip ---");
    if let (Ok(ri), Ok(td)) = (
        generators::arbitrary_relation_info(&mut u),
        generators::arbitrary_tuple_data(&mut u),
    ) {
        let row = td.to_row_data(&ri);
        println!("  RowData entries: {}", row.len());
    }

    println!("All pg_walstream types can be fuzz-generated externally.");
}
