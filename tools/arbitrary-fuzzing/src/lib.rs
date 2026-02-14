//! # External Arbitrary Generators for pg_walstream
//!
//! Construct arbitrary `pg_walstream` types for fuzzing without modifying the library. This crate provides standalone generator functions that produce random instances of `pg_walstream` types using the `arbitrary` crate.
//!
//! ## How to use with `cargo-fuzz`
//!
//! ```ignore
//! use arbitrary::Unstructured;
//! use pg_walstream_arbitrary_fuzzing::generators;
//!
//! libfuzzer_sys::fuzz_target!(|data: &[u8]| {
//!     let mut u = Unstructured::new(data);
//!     if let Ok(msg) = generators::arbitrary_logical_replication_message(&mut u) {
//!         // exercise your code with `msg`
//!     }
//! });
//! ```

pub mod generators {
    use arbitrary::{self, Unstructured};
    use pg_walstream::protocol::{
        ColumnData, ColumnInfo, LogicalReplicationMessage, MessageType, RelationInfo, TupleData,
    };
    use pg_walstream::types::{CachePadded, ReplicaIdentity};

    /// All `MessageType` variants in a static slice for uniform selection.
    const ALL_MESSAGE_TYPES: &[MessageType] = &[
        MessageType::Begin,
        MessageType::Commit,
        MessageType::Origin,
        MessageType::Relation,
        MessageType::Type,
        MessageType::Insert,
        MessageType::Update,
        MessageType::Delete,
        MessageType::Truncate,
        MessageType::Message,
        MessageType::StreamStart,
        MessageType::StreamStop,
        MessageType::StreamCommit,
        MessageType::StreamAbort,
        MessageType::BeginPrepare,
        MessageType::Prepare,
        MessageType::CommitPrepared,
        MessageType::RollbackPrepared,
        MessageType::StreamPrepare,
    ];

    /// Generate an arbitrary `MessageType`.
    pub fn arbitrary_message_type(u: &mut Unstructured) -> arbitrary::Result<MessageType> {
        u.choose(ALL_MESSAGE_TYPES).copied()
    }

    /// Generate an arbitrary `ReplicaIdentity`.
    pub fn arbitrary_replica_identity(
        u: &mut Unstructured,
    ) -> arbitrary::Result<ReplicaIdentity> {
        const VARIANTS: &[ReplicaIdentity] = &[
            ReplicaIdentity::Default,
            ReplicaIdentity::Nothing,
            ReplicaIdentity::Full,
            ReplicaIdentity::Index,
        ];
        u.choose(VARIANTS).cloned()
    }

    /// Generate an arbitrary `CachePadded<T>` wrapping a `u64`.
    pub fn arbitrary_cache_padded_u64(u: &mut Unstructured) -> arbitrary::Result<CachePadded<u64>> {
        let val: u64 = u.arbitrary()?;
        Ok(CachePadded::new(val))
    }

    /// Generate an arbitrary `ColumnData`.
    pub fn arbitrary_column_data(u: &mut Unstructured) -> arbitrary::Result<ColumnData> {
        let variant: u8 = u.int_in_range(0..=3)?;
        Ok(match variant {
            0 => ColumnData::null(),
            1 => ColumnData::unchanged(),
            2 => {
                let data: Vec<u8> = u.arbitrary()?;
                ColumnData::text(data)
            }
            _ => {
                let data: Vec<u8> = u.arbitrary()?;
                ColumnData::binary(data)
            }
        })
    }

    /// Generate an arbitrary `ColumnInfo`.
    pub fn arbitrary_column_info(u: &mut Unstructured) -> arbitrary::Result<ColumnInfo> {
        let flags: u8 = u.arbitrary()?;
        let name: String = u.arbitrary()?;
        let type_id: u32 = u.arbitrary()?;
        let type_modifier: i32 = u.arbitrary()?;
        Ok(ColumnInfo::new(flags, name, type_id, type_modifier))
    }

    /// Generate an arbitrary `TupleData`.
    pub fn arbitrary_tuple_data(u: &mut Unstructured) -> arbitrary::Result<TupleData> {
        let len = u.int_in_range(0..=10)?;
        let mut columns = Vec::with_capacity(len);
        for _ in 0..len {
            columns.push(arbitrary_column_data(u)?);
        }
        Ok(TupleData::new(columns))
    }

    /// Generate an arbitrary `RelationInfo`.
    pub fn arbitrary_relation_info(u: &mut Unstructured) -> arbitrary::Result<RelationInfo> {
        let relation_id: u32 = u.arbitrary()?;
        let namespace: String = u.arbitrary()?;
        let relation_name: String = u.arbitrary()?;
        let replica_identity: u8 = u.arbitrary()?;

        let col_count = u.int_in_range(0..=8)?;
        let mut columns = Vec::with_capacity(col_count);
        for _ in 0..col_count {
            columns.push(arbitrary_column_info(u)?);
        }
        Ok(RelationInfo::new(
            relation_id,
            namespace,
            relation_name,
            replica_identity,
            columns,
        ))
    }

    /// Generate an arbitrary `LogicalReplicationMessage`.
    ///
    /// Covers all 19 enum variants including streaming (v2) and two-phase commit (v3).
    pub fn arbitrary_logical_replication_message(
        u: &mut Unstructured,
    ) -> arbitrary::Result<LogicalReplicationMessage> {
        let variant: u8 = u.int_in_range(0..=18)?;
        Ok(match variant {
            0 => LogicalReplicationMessage::Begin {
                final_lsn: u.arbitrary()?,
                timestamp: u.arbitrary()?,
                xid: u.arbitrary()?,
            },
            1 => LogicalReplicationMessage::Commit {
                flags: u.arbitrary()?,
                commit_lsn: u.arbitrary()?,
                end_lsn: u.arbitrary()?,
                timestamp: u.arbitrary()?,
            },
            2 => {
                let col_count: usize = u.int_in_range(0..=8)?;
                let mut columns = Vec::with_capacity(col_count);
                for _ in 0..col_count {
                    columns.push(arbitrary_column_info(u)?);
                }
                LogicalReplicationMessage::Relation {
                    relation_id: u.arbitrary()?,
                    namespace: u.arbitrary()?,
                    relation_name: u.arbitrary()?,
                    replica_identity: u.arbitrary()?,
                    columns,
                }
            }
            3 => LogicalReplicationMessage::Insert {
                relation_id: u.arbitrary()?,
                tuple: arbitrary_tuple_data(u)?,
            },
            4 => {
                let has_old: bool = u.arbitrary()?;
                LogicalReplicationMessage::Update {
                    relation_id: u.arbitrary()?,
                    old_tuple: if has_old {
                        Some(arbitrary_tuple_data(u)?)
                    } else {
                        None
                    },
                    new_tuple: arbitrary_tuple_data(u)?,
                    key_type: if has_old {
                        Some(u.arbitrary()?)
                    } else {
                        None
                    },
                }
            }
            5 => LogicalReplicationMessage::Delete {
                relation_id: u.arbitrary()?,
                old_tuple: arbitrary_tuple_data(u)?,
                key_type: u.arbitrary()?,
            },
            6 => {
                let id_count: usize = u.int_in_range(0..=5)?;
                let mut relation_ids = Vec::with_capacity(id_count);
                for _ in 0..id_count {
                    relation_ids.push(u.arbitrary()?);
                }
                LogicalReplicationMessage::Truncate {
                    relation_ids,
                    flags: u.arbitrary()?,
                }
            }
            7 => LogicalReplicationMessage::Type {
                type_id: u.arbitrary()?,
                namespace: u.arbitrary()?,
                type_name: u.arbitrary()?,
            },
            8 => LogicalReplicationMessage::Origin {
                origin_lsn: u.arbitrary()?,
                origin_name: u.arbitrary()?,
            },
            9 => {
                let content: Vec<u8> = u.arbitrary()?;
                LogicalReplicationMessage::Message {
                    flags: u.arbitrary()?,
                    lsn: u.arbitrary()?,
                    prefix: u.arbitrary()?,
                    content,
                }
            }
            10 => LogicalReplicationMessage::StreamStart {
                xid: u.arbitrary()?,
                first_segment: u.arbitrary()?,
            },
            11 => LogicalReplicationMessage::StreamStop,
            12 => LogicalReplicationMessage::StreamCommit {
                xid: u.arbitrary()?,
                flags: u.arbitrary()?,
                commit_lsn: u.arbitrary()?,
                end_lsn: u.arbitrary()?,
                timestamp: u.arbitrary()?,
            },
            13 => {
                let has_v4: bool = u.arbitrary()?;
                LogicalReplicationMessage::StreamAbort {
                    xid: u.arbitrary()?,
                    subtransaction_xid: u.arbitrary()?,
                    abort_lsn: if has_v4 {
                        Some(u.arbitrary()?)
                    } else {
                        None
                    },
                    abort_timestamp: if has_v4 {
                        Some(u.arbitrary()?)
                    } else {
                        None
                    },
                }
            }
            14 => LogicalReplicationMessage::BeginPrepare {
                prepare_lsn: u.arbitrary()?,
                end_lsn: u.arbitrary()?,
                timestamp: u.arbitrary()?,
                xid: u.arbitrary()?,
                gid: u.arbitrary()?,
            },
            15 => LogicalReplicationMessage::Prepare {
                flags: u.arbitrary()?,
                prepare_lsn: u.arbitrary()?,
                end_lsn: u.arbitrary()?,
                timestamp: u.arbitrary()?,
                xid: u.arbitrary()?,
                gid: u.arbitrary()?,
            },
            16 => LogicalReplicationMessage::CommitPrepared {
                flags: u.arbitrary()?,
                commit_lsn: u.arbitrary()?,
                end_lsn: u.arbitrary()?,
                timestamp: u.arbitrary()?,
                xid: u.arbitrary()?,
                gid: u.arbitrary()?,
            },
            17 => LogicalReplicationMessage::RollbackPrepared {
                flags: u.arbitrary()?,
                prepare_end_lsn: u.arbitrary()?,
                rollback_end_lsn: u.arbitrary()?,
                prepare_timestamp: u.arbitrary()?,
                rollback_timestamp: u.arbitrary()?,
                xid: u.arbitrary()?,
                gid: u.arbitrary()?,
            },
            _ => LogicalReplicationMessage::StreamPrepare {
                flags: u.arbitrary()?,
                prepare_lsn: u.arbitrary()?,
                end_lsn: u.arbitrary()?,
                timestamp: u.arbitrary()?,
                xid: u.arbitrary()?,
                gid: u.arbitrary()?,
            },
        })
    }
}
