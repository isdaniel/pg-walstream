//! Table-binding trait for the derive-macro layer.
//!
//! `WalTable` is provided by `#[derive(WalTable)]`.

/// A row type bound to a PostgreSQL table name.
///
/// Provided by `#[derive(WalTable)]` with `#[wal(table = "...")]`:
///
/// ```ignore
/// #[derive(serde::Deserialize, pg_walstream::WalTable)]
/// #[wal(table = "users")]
/// struct User { id: i64 }
/// assert_eq!(<User as pg_walstream::WalTable>::TABLE, "users");
/// ```
pub trait WalTable {
    /// The PostgreSQL table name this type maps to.
    const TABLE: &'static str;
}

#[cfg(all(test, feature = "derive"))]
mod derive_tests {
    use crate::{wal_table, WalTable};

    #[derive(WalTable)]
    #[wal(table = "widgets")]
    struct Widget {
        #[allow(dead_code)]
        id: i64,
    }

    #[test]
    fn derive_sets_table_const() {
        assert_eq!(<Widget as WalTable>::TABLE, "widgets");
    }

    // Attribute form: a single annotation, name inline. Equivalent to the derive.
    #[wal_table("gadgets")]
    struct Gadget {
        #[allow(dead_code)]
        id: i64,
    }

    #[test]
    fn wal_table_attr_sets_table_const() {
        assert_eq!(<Gadget as WalTable>::TABLE, "gadgets");
    }
}
