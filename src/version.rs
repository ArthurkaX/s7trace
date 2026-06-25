use rusqlite::Connection;

pub const PACKAGE_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const API_VERSION: u64 = 1;

pub fn schema_version(conn: &Connection) -> Option<String> {
    conn.query_row(
        "SELECT value FROM schema_meta WHERE key = 'schema_version'",
        [],
        |row| row.get(0),
    )
    .ok()
}

pub fn status_json(conn: &Connection) -> serde_json::Value {
    serde_json::json!({
        "package": PACKAGE_VERSION,
        "schema": schema_version(conn),
        "api": API_VERSION,
    })
}
