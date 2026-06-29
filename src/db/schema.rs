/// Initial schema and migration runner.
use anyhow::{Context, Result};
use log::info;
use rusqlite::Connection;

/// Migration file 0001 — must be byte-identical to the SQL kept in
/// `docs/schema.sql` (used by out-of-band tools and CI).
const MIGRATION_0001: &str = include_str!("../../migrations/0001_initial.sql");

/// Open or create the database, run pending migrations.
pub fn open(path: &str) -> Result<Connection> {
    let conn = Connection::open(path).with_context(|| format!("opening database at {path}"))?;

    // Safe defaults per schema.sql
    conn.execute_batch("PRAGMA journal_mode = WAL;")?;
    conn.execute_batch("PRAGMA synchronous = NORMAL;")?;
    conn.execute_batch("PRAGMA busy_timeout = 5000;")?;
    conn.execute_batch("PRAGMA foreign_keys = ON;")?;

    // The schema is applied as a single embedded migration; it brings
    // `schema_meta` with it. We do not pre-create it here, otherwise
    // re-opening a freshly-created DB would fail on the duplicate
    // CREATE TABLE.

    // Embedded migrations applied in order. Real migrations live under
    // `migrations/*.sql` (loaded via include_str!). For now only 0001 is
    // embedded; future versions add 0002, 0003, ...
    const EMBEDDED: &[(&str, &str)] = &[("0001_initial", MIGRATION_0001)];

    let version: String = conn
        .query_row(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .unwrap_or_default();

    if version.is_empty() {
        info!("fresh database, applying initial schema");
        apply_migrations(&conn, EMBEDDED)?;
    } else {
        info!("database at schema version {version}");
        // Future: detect version < current and apply pending migrations.
        // For now only version 1 exists.
        if version != "1" {
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

/// Apply all embedded migrations that haven't been recorded as applied
/// yet. The migration files are expected to be idempotent
/// (`CREATE TABLE IF NOT EXISTS` style); this is a deliberate choice so
/// that opening an existing database is safe even if it was bootstrapped
/// from `docs/schema.sql` directly.
fn apply_migrations(conn: &Connection, migrations: &[(&str, &str)]) -> Result<()> {
    for (name, sql) in migrations {
        info!("applying migration {name}");
        conn.execute_batch(sql)
            .with_context(|| format!("applying migration {name}"))?;
    }
    Ok(())
}

/// Create an in-memory database for testing.
pub fn open_memory() -> Result<Connection> {
    let conn = Connection::open_in_memory()?;
    conn.execute_batch("PRAGMA foreign_keys = ON;")?;
    // `schema_meta` is part of the embedded schema (see schema.sql / 0001
    // migration) so we don't pre-create it here.
    apply_migrations(&conn, &[("0001_initial", MIGRATION_0001)])?;
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

    #[test]
    fn test_migration_file_matches_docs() {
        // `migrations/0001_initial.sql` and `docs/schema.sql` must stay
        // logically identical so that re-opening a database is always safe.
        // If you change the schema, update both files.
        let docs_sql = std::fs::read_to_string("docs/schema.sql").expect("read docs/schema.sql");
        let docs_sql = docs_sql.replace("\r\n", "\n");
        assert_eq!(
            MIGRATION_0001, docs_sql,
            "migrations/0001_initial.sql and docs/schema.sql diverged"
        );
    }
}
