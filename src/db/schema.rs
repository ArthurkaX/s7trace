/// Initial schema and migration runner.
use anyhow::{Context, Result};
use rusqlite::Connection;
use tracing::info;

/// Initial schema, embedded in the binary at compile time.
const MIGRATION_0001: &str = include_str!("../../migrations/0001_initial.sql");
/// poll_cycle retention config and the time-range indexes it needs.
const MIGRATION_0002: &str = include_str!("../../migrations/0002_poll_cycle_retention.sql");

/// Embedded migrations, ordered. The first field is the schema version the
/// migration brings the database *to*; each file ends by writing that value
/// into `schema_meta.schema_version`.
///
/// A migration is applied only when the stored version is below its target, so
/// migrations need not be idempotent — `ALTER TABLE ADD COLUMN`, which SQLite
/// has no `IF NOT EXISTS` form for, is fine here.
const EMBEDDED: &[(i64, &str, &str)] = &[
    (1, "0001_initial", MIGRATION_0001),
    (2, "0002_poll_cycle_retention", MIGRATION_0002),
];

/// Schema version this binary expects after all embedded migrations run.
pub const CURRENT_SCHEMA_VERSION: i64 = 2;

/// Open or create the database, run pending migrations.
pub fn open(path: &str) -> Result<Connection> {
    let conn = Connection::open(path).with_context(|| format!("opening database at {path}"))?;

    // Safe database defaults.
    conn.execute_batch("PRAGMA journal_mode = WAL;")?;
    conn.execute_batch("PRAGMA synchronous = NORMAL;")?;
    conn.execute_batch("PRAGMA busy_timeout = 5000;")?;
    conn.execute_batch("PRAGMA foreign_keys = ON;")?;

    // The schema is applied as a single embedded migration; it brings
    // `schema_meta` with it. We do not pre-create it here, otherwise
    // re-opening a freshly-created DB would fail on the duplicate
    // CREATE TABLE.

    let version: String = conn
        .query_row(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .unwrap_or_default();

    let current: i64 = if version.is_empty() {
        info!("fresh database, applying initial schema");
        0
    } else {
        let n: i64 = version.parse().with_context(|| {
            format!("database schema version '{version}' is not a number — database is corrupt")
        })?;
        if n > CURRENT_SCHEMA_VERSION {
            anyhow::bail!(
                "database schema version '{version}' is newer than this binary \
                 (max {CURRENT_SCHEMA_VERSION}). Upgrade the binary first."
            );
        }
        if n < CURRENT_SCHEMA_VERSION {
            info!("database at schema version {n}, upgrading to {CURRENT_SCHEMA_VERSION}");
        } else {
            info!("database at schema version {n}");
        }
        n
    };

    apply_migrations(&conn, current)?;

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

/// Apply every embedded migration whose target version is above `from`.
///
/// Transactions are the migration file's own business, not the runner's: the
/// initial schema opens with `PRAGMA journal_mode`/`synchronous`, which SQLite
/// refuses to run inside a transaction. Files that can be wrapped (everything
/// after 0001) open with `BEGIN` and close with `COMMIT` themselves, so a
/// failure mid-file leaves the database at the previous version rather than
/// half-upgraded.
fn apply_migrations(conn: &Connection, from: i64) -> Result<()> {
    for (version, name, sql) in EMBEDDED {
        if *version <= from {
            continue;
        }
        info!("applying migration {name}");
        if let Err(e) = conn
            .execute_batch(sql)
            .with_context(|| format!("applying migration {name}"))
        {
            // No-op when the file was not transactional or already committed.
            let _ = conn.execute_batch("ROLLBACK");
            return Err(e);
        }
    }
    Ok(())
}

/// Create an in-memory database for testing.
pub fn open_memory() -> Result<Connection> {
    let conn = Connection::open_in_memory()?;
    conn.execute_batch("PRAGMA foreign_keys = ON;")?;
    // `schema_meta` is part of the initial migration, so we don't pre-create
    // it here.
    apply_migrations(&conn, 0)?;
    Ok(conn)
}

/// The current global retention policy, as read back from the database:
/// `(sample_cutoff_days, cycle_cutoff_days, failed_cycle_cutoff_days,
/// keep_failed_cycles, delete_batch_rows, enabled)`.
pub type HistoryPolicy = (Option<i64>, Option<i64>, Option<i64>, bool, i64, bool);

/// Apply a history-retention policy change (if any) against the global row and
/// read the resulting policy back.
///
/// The single source of truth for the write+clamp rules, shared by the local
/// CLI handler (`handlers::history::handle_history_policy`) and the unified
/// `Op::HistoryPolicy` execution arm so the two never drift apart.
///
/// Returns [`HistoryPolicy`].
pub fn apply_history_policy(
    conn: &Connection,
    cycle_days: Option<i64>,
    sample_days: Option<i64>,
    failed_cycle_days: Option<i64>,
    keep_failed: Option<bool>,
    batch_rows: Option<i64>,
) -> Result<HistoryPolicy> {
    // The global row (poll_group_id IS NULL) is the one the collector's sweep
    // reads; per-group rows are not consulted for cycle retention.
    if cycle_days.is_some()
        || sample_days.is_some()
        || failed_cycle_days.is_some()
        || keep_failed.is_some()
        || batch_rows.is_some()
    {
        if let Some(d) = cycle_days {
            // 0 means "keep everything", stored as NULL so the sweep's own
            // "unset" and the operator's "off" are the same state.
            let v = if d > 0 { Some(d) } else { None };
            conn.execute(
                "UPDATE history_cleanup_policy SET cycle_cutoff_days = ?1
                 WHERE poll_group_id IS NULL",
                rusqlite::params![v],
            )?;
        }
        if let Some(d) = sample_days {
            let v = if d > 0 { Some(d) } else { None };
            conn.execute(
                "UPDATE history_cleanup_policy SET cutoff_days = ?1
                 WHERE poll_group_id IS NULL",
                rusqlite::params![v],
            )?;
        }
        if let Some(d) = failed_cycle_days {
            // 0 means "keep failed cycles forever" when keep_failed is true.
            let v = if d > 0 { Some(d) } else { None };
            conn.execute(
                "UPDATE history_cleanup_policy SET failed_cycle_cutoff_days = ?1
                 WHERE poll_group_id IS NULL",
                rusqlite::params![v],
            )?;
        }
        if let Some(k) = keep_failed {
            conn.execute(
                "UPDATE history_cleanup_policy SET keep_failed_cycles = ?1
                 WHERE poll_group_id IS NULL",
                rusqlite::params![i64::from(k)],
            )?;
        }
        if let Some(b) = batch_rows {
            conn.execute(
                "UPDATE history_cleanup_policy SET delete_batch_rows = ?1
                 WHERE poll_group_id IS NULL",
                rusqlite::params![b.clamp(100, 100_000)],
            )?;
        }
    }

    let row = conn.query_row(
        "SELECT cutoff_days, cycle_cutoff_days, failed_cycle_cutoff_days,
                keep_failed_cycles, delete_batch_rows, enabled
         FROM history_cleanup_policy WHERE poll_group_id IS NULL",
        [],
        |r| {
            Ok((
                r.get::<_, Option<i64>>(0)?,
                r.get::<_, Option<i64>>(1)?,
                r.get::<_, Option<i64>>(2)?,
                r.get::<_, i64>(3)? != 0,
                r.get::<_, i64>(4)?,
                r.get::<_, i64>(5)? != 0,
            ))
        },
    );
    row.map_err(|_| anyhow::anyhow!("no global retention policy row; database predates schema 2"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema_version(conn: &Connection) -> String {
        conn.query_row(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .unwrap()
    }

    #[test]
    fn test_schema_applies_cleanly() {
        let conn = open_memory().unwrap();
        assert_eq!(schema_version(&conn), CURRENT_SCHEMA_VERSION.to_string());
    }

    #[test]
    fn test_upgrade_from_v1_applies_only_pending() {
        // Simulate a database created by an older binary: apply 0001 only.
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        conn.execute_batch(MIGRATION_0001).unwrap();
        assert_eq!(schema_version(&conn), "1");

        apply_migrations(&conn, 1).unwrap();
        assert_eq!(schema_version(&conn), "2");

        // The v2 additions are present...
        let idx: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'index' AND name = 'ix_poll_cycle_finished_ts'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(idx, 1);
        let days: Option<i64> = conn
            .query_row(
                "SELECT cycle_cutoff_days FROM history_cleanup_policy
                 WHERE poll_group_id IS NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(days, Some(14));
        let failed_days: Option<i64> = conn
            .query_row(
                "SELECT failed_cycle_cutoff_days FROM history_cleanup_policy
                 WHERE poll_group_id IS NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(failed_days, Some(30));

        // ...and re-running the runner is a no-op rather than an error.
        // `ALTER TABLE ADD COLUMN` would fail if 0002 were applied twice.
        apply_migrations(&conn, 2).unwrap();
        assert_eq!(schema_version(&conn), "2");
    }
}
