#![cfg(all(feature = "libpq", not(feature = "rustls-tls")))]

//! Live check for `PgResult::get_bytes`. `exec` is a text-format query, so this
//! covers the two text-mode wins over `get_value`: byte-exact access and
//! telling SQL `NULL` from empty. Raw binary recovery needs a binary-format
//! query; the unit tests cover that path on binary cells.

use pg_walstream::PgReplicationConnection;

fn conn_string() -> String {
    std::env::var("DATABASE_URL_REGULAR")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost:5432/postgres".to_string())
}

#[test]
#[ignore = "requires live PostgreSQL"]
fn get_bytes_against_live_result() {
    let mut conn = PgReplicationConnection::connect(&conn_string()).expect("connect");
    conn.exec("DROP TABLE IF EXISTS get_bytes_it").unwrap();
    conn.exec("CREATE TABLE get_bytes_it (id int, t text)")
        .unwrap();
    conn.exec("INSERT INTO get_bytes_it VALUES (1, 'hello'), (2, ''), (3, NULL)")
        .unwrap();

    let res = conn
        .exec("SELECT t FROM get_bytes_it ORDER BY id")
        .expect("select");
    assert_eq!(res.ntuples(), 3);
    assert_eq!(res.get_bytes(0, 0), Some(&b"hello"[..]));
    assert_eq!(
        res.get_bytes(1, 0),
        Some(&[][..]),
        "empty text is Some(&[])"
    );
    assert_eq!(res.get_bytes(2, 0), None, "SQL NULL is None");
    assert_eq!(res.get_bytes_owned(0, 0), Some(b"hello".to_vec()));

    // get_value delegates to get_bytes: empty text is Some(""), SQL NULL is None.
    assert_eq!(res.get_value(1, 0), Some(String::new()));
    assert_eq!(res.get_value(2, 0), None);

    conn.exec("DROP TABLE get_bytes_it").unwrap();
}
