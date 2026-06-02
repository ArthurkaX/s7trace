/// Initial schema and migration runner.

use anyhow::{Context, Result};
use log::info;
use rusqlite::Connection;

const SCHEMA_SQL: &str = include_str!("../../docs/schema.sql");

/// Open or create the database, run pending migrations.
pub fn open(path: &str) -> Result<Connection> {
    let conn = Connection::open(path)
        .with_context(|| format!("opening database at {path}"))?;

    // Safe defaults per schema.sql
    conn.execute_batch("PRAGMA journal_mode = WAL;")?;
    conn.execute_batch("PRAGMA synchronous = NORMAL;")?;
    conn.execute_batch("PRAGMA busy_timeout = 5000;")?;
    conn.execute_batch("PRAGMA foreign_keys = ON;")?;

    let version: String = conn
        .query_row(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .unwrap_or_default();

    match version.as_str() {
        "" => {
            info!("fresh database, applying initial schema");
            apply_initial(&conn)?;
        }
        "1" => {
            info!("database at schema version 1");
        }
        _ => {
            anyhow::bail!(
                "database schema version '{version}' is newer than this binary (max 1). \
                 Upgrade the binary first."
            );
        }
    }

    // Ensure time_unit is set
    let tu: String = conn
        .query_row(
            "SELECT value FROM schema_meta WHERE key = 'time_unit'",
            [],
            |row| row.get(0),
        )
        .unwrap_or_default();
    if tu != "unix_epoch_us" {
        anyhow::bail!("expected time_unit = 'unix_epoch_us', got '{tu}'");
    }

    Ok(conn)
}

fn apply_initial(conn: &Connection) -> Result<()> {
    conn.execute_batch(SCHEMA_SQL)
        .context("applying initial schema from docs/schema.sql")?;
    info!("initial schema applied");
    Ok(())
}

/// Create an in-memory database for testing.
pub fn open_memory() -> Result<Connection> {
    let conn = Connection::open_in_memory()?;
    conn.execute_batch("PRAGMA foreign_keys = ON;")?;
    apply_initial(&conn)?;
    Ok(conn)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_applies_cleanly() {
        let conn = open_memory().unwrap();
        let version: String = conn
            .query_row(
                "SELECT value FROM schema_meta WHERE key = 'schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, "1");
    }
}
