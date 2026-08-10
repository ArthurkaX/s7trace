//! Retention for the `poll_cycle` audit log.
//!
//! `history-clean` deletes a PLC's history, but only with collection stopped —
//! and the field instance ran 22 days without a stop. Over that time
//! `poll_cycle` reached 12.7M rows against 1.4M samples: nine audit rows per
//! row of collected data, most of them failed cycles that produced nothing.
//!
//! This sweep runs from the monitor loop while collection is active. It is
//! bounded twice over — a row limit per statement and a batch limit per pass —
//! so it never holds a write transaction long enough to stall the sample
//! writer, and it only ever removes cycles nothing else references, leaving the
//! sample retention policy to decide when the referenced ones may go.

use rusqlite::Connection;
use tracing::{debug, warn};

/// How the sweep is configured, as read from `history_cleanup_policy`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CyclePolicy {
    /// Cycles finished more than this many days ago are eligible.
    pub cutoff_days: i64,
    /// Retain `ok = 0` rows past the cutoff, so the record of an outage
    /// outlives the routine cycles around it.
    pub keep_failed: bool,
    /// Rows per DELETE statement.
    pub batch_rows: i64,
}

/// The global cycle policy, or `None` when retention is off.
///
/// Only the global (`poll_group_id IS NULL`) row is consulted: cycle rows are
/// diagnostics for the whole collector, and a per-group cutoff would leave the
/// table's size depending on which group happened to fail most.
pub fn load_policy(conn: &Connection) -> Option<CyclePolicy> {
    conn.query_row(
        "SELECT cycle_cutoff_days, keep_failed_cycles, delete_batch_rows
         FROM history_cleanup_policy
         WHERE poll_group_id IS NULL AND enabled = 1",
        [],
        |r| {
            Ok((
                r.get::<_, Option<i64>>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
            ))
        },
    )
    .ok()
    .and_then(|(days, keep_failed, batch)| {
        // A NULL or non-positive cutoff means "keep everything" — the setting
        // is absent, not zero days.
        days.filter(|d| *d > 0).map(|cutoff_days| CyclePolicy {
            cutoff_days,
            keep_failed: keep_failed != 0,
            batch_rows: batch.clamp(100, 100_000),
        })
    })
}

/// Delete up to `max_batches` batches of expired cycles, returning the count.
///
/// Stops early when a batch comes back empty. Returning before the table is
/// clean is intended: the caller runs this every few minutes, and a sweep that
/// ran until done would, on first contact with a 12.7M-row backlog, hold the
/// database against the writer for as long as that took.
pub fn sweep(conn: &Connection, policy: CyclePolicy, now_us: i64, max_batches: u32) -> i64 {
    let cutoff = now_us - policy.cutoff_days * 86_400_000_000;
    let keep_failed = i64::from(policy.keep_failed);
    let mut deleted = 0;

    for _ in 0..max_batches {
        // Only unreferenced cycles. A cycle whose samples are still within the
        // sample retention window is not expired — it is the provenance of data
        // the operator still has, and the FK would refuse the delete anyway.
        let n = conn.execute(
            "DELETE FROM poll_cycle
             WHERE rowid IN (
               SELECT rowid FROM poll_cycle
               WHERE finished_ts < ?1
                 AND (?2 = 0 OR ok = 1)
                 AND NOT EXISTS (SELECT 1 FROM sample s WHERE s.cycle_id = poll_cycle.id)
                 AND NOT EXISTS (SELECT 1 FROM event_marker em WHERE em.cycle_id = poll_cycle.id)
                 AND NOT EXISTS (SELECT 1 FROM plc_time_sample pts WHERE pts.cycle_id = poll_cycle.id)
               LIMIT ?3
             )",
            rusqlite::params![cutoff, keep_failed, policy.batch_rows],
        );
        match n {
            Ok(0) => break,
            Ok(n) => deleted += n as i64,
            Err(e) => {
                warn!("cycle retention: {e}");
                break;
            }
        }
    }

    if deleted > 0 {
        debug!(
            "cycle retention: deleted {deleted} poll_cycle rows older than {} days",
            policy.cutoff_days
        );
    }
    deleted
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::schema;

    /// A database with one PLC, one poll group, one dataset, and nothing else.
    fn fixture() -> Connection {
        let conn = schema::open_memory().unwrap();
        conn.execute_batch(
            "INSERT INTO plc (id, name, host, rack, slot, enabled)
                 VALUES (1, 'plc1', '10.0.0.1', 0, 1, 1);
             INSERT INTO poll_group (id, name, period_ms)
                 VALUES (1, 'fast', 20);
             INSERT INTO dataset (id, plc_id, name, state, created_ts, updated_ts)
                 VALUES (1, 1, 'ds1', 'active', 0, 0);",
        )
        .unwrap();
        conn
    }

    fn add_cycle(conn: &Connection, id: i64, finished_ts: i64, ok: bool) {
        conn.execute(
            "INSERT INTO poll_cycle (id, dataset_id, plc_id, poll_group_id, seq,
                                     started_ts, finished_ts, target_ms, actual_ms, ok)
             VALUES (?1, 1, 1, 1, ?1, ?2, ?2, 20, 12.0, ?3)",
            rusqlite::params![id, finished_ts, i64::from(ok)],
        )
        .unwrap();
    }

    fn cycle_ids(conn: &Connection) -> Vec<i64> {
        let mut stmt = conn
            .prepare("SELECT id FROM poll_cycle ORDER BY id")
            .unwrap();
        let ids = stmt
            .query_map([], |r| r.get(0))
            .unwrap()
            .filter_map(|x| x.ok())
            .collect();
        ids
    }

    const DAY: i64 = 86_400_000_000;

    #[test]
    fn the_migration_ships_a_usable_default_policy() {
        let conn = fixture();
        let p = load_policy(&conn).expect("0002 seeds a global policy");
        assert_eq!(p.cutoff_days, 14);
        assert!(p.keep_failed);
        assert_eq!(p.batch_rows, 10_000);
    }

    #[test]
    fn no_cutoff_means_retention_is_off() {
        let conn = fixture();
        conn.execute_batch("UPDATE history_cleanup_policy SET cycle_cutoff_days = NULL")
            .unwrap();
        assert_eq!(load_policy(&conn), None);
    }

    #[test]
    fn expired_cycles_go_and_recent_ones_stay() {
        let conn = fixture();
        let now = 100 * DAY;
        add_cycle(&conn, 1, now - 30 * DAY, true); // expired
        add_cycle(&conn, 2, now - DAY, true); // recent
        let policy = CyclePolicy {
            cutoff_days: 14,
            keep_failed: false,
            batch_rows: 1000,
        };
        assert_eq!(sweep(&conn, policy, now, 10), 1);
        assert_eq!(cycle_ids(&conn), vec![2]);
    }

    /// The 22-day outage left 30,835 failed cycles in a day. Those are the rows
    /// an operator reconstructs the outage from, so they outlive the cutoff.
    #[test]
    fn failed_cycles_survive_the_cutoff_when_asked() {
        let conn = fixture();
        let now = 100 * DAY;
        add_cycle(&conn, 1, now - 30 * DAY, false);
        add_cycle(&conn, 2, now - 30 * DAY, true);
        let policy = CyclePolicy {
            cutoff_days: 14,
            keep_failed: true,
            batch_rows: 1000,
        };
        assert_eq!(sweep(&conn, policy, now, 10), 1);
        assert_eq!(cycle_ids(&conn), vec![1]);
    }

    #[test]
    fn a_cycle_that_still_has_samples_is_not_expired() {
        let conn = fixture();
        let now = 100 * DAY;
        add_cycle(&conn, 1, now - 30 * DAY, true);
        conn.execute_batch(
            "INSERT INTO tag (id, plc_id, name, address, db_num, byte_offset,
                              byte_size, data_type, poll_group_id, created_ts, updated_ts)
                 VALUES (1, 1, 't1', 'DB1.DBW0', 1, 0, 2, 'u16', 1, 0, 0);
             INSERT INTO sample (dataset_id, cycle_id, tag_id, ts, vi)
                 VALUES (1, 1, 1, 1, 42);",
        )
        .unwrap();
        let policy = CyclePolicy {
            cutoff_days: 14,
            keep_failed: false,
            batch_rows: 1000,
        };
        assert_eq!(sweep(&conn, policy, now, 10), 0);
        assert_eq!(cycle_ids(&conn), vec![1]);
    }

    /// Each pass is bounded so a first contact with a large backlog cannot hold
    /// the database against the sample writer.
    #[test]
    fn a_pass_deletes_at_most_its_batch_budget() {
        let conn = fixture();
        let now = 100 * DAY;
        for id in 1..=10 {
            add_cycle(&conn, id, now - 30 * DAY, true);
        }
        let policy = CyclePolicy {
            cutoff_days: 14,
            keep_failed: false,
            batch_rows: 2,
        };
        assert_eq!(sweep(&conn, policy, now, 3), 6);
        assert_eq!(cycle_ids(&conn).len(), 4);
    }
}
