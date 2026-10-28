//! Retention for collected history: samples past the sample cutoff, then the
//! `poll_cycle` audit log, then read spans nothing references any more.
//!
//! `history-clean` deletes a PLC's history, but only with collection stopped —
//! and the field instance ran 22 days without a stop. Over that time
//! `poll_cycle` reached 12.7M rows against 1.4M samples: nine audit rows per
//! row of collected data, most of them failed cycles that produced nothing.
//!
//! Retention runs on its own thread while collection is active: one pass a
//! minute after startup, then one a day. A pass works until nothing is left to
//! delete, but one bounded batch at a time with a pause between batches, so it
//! never holds a write transaction long enough to stall the sample writer. It
//! only ever removes cycles nothing else references, leaving the sample
//! retention policy to decide when the referenced ones may go.
//!
//! Sample retention runs the same way, for the same reason: a database whose
//! collection never stopped grew without bound, and a set `--sample-days`
//! cutoff did nothing until a collection stop allowed `history-clean`. Samples go first in each pass, so
//! the cycles and spans they referenced become eligible in the same pass.
//!
//! Before deleting anything, each pass writes the daily `poll_cycle_daily`
//! summary (see [`crate::engine::cycle_daily`]), so thinned cycle history still
//! says how much polling there was.
//!
//! Deleted rows leave free pages that new samples reuse; the file only shrinks
//! through [`vacuum_if_worthwhile`] at the next server start.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use rusqlite::Connection;
use tracing::{debug, info, warn};

/// Delay from server start to the first pass, so startup work settles first.
const FIRST_PASS_DELAY: Duration = Duration::from_secs(60);
/// Interval between passes.
const PASS_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
/// Pause between batches, which leaves the write lock to the sample writer.
const BATCH_PAUSE: Duration = Duration::from_millis(100);

/// Startup VACUUM runs when free pages are at least this share of the file...
const VACUUM_MIN_FREE_SHARE: f64 = 0.25;
/// ...and at least this many bytes.
const VACUUM_MIN_FREE_BYTES: i64 = 64 * 1024 * 1024;

/// How the sweep is configured, as read from `history_cleanup_policy`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CyclePolicy {
    /// Cycles finished more than this many days ago are eligible.
    pub cutoff_days: i64,
    /// Separate retention window for failed cycles. `None` deliberately means
    /// forever, but the fresh-database default is bounded.
    pub failed_cutoff_days: Option<i64>,
    /// Use the separate failed-cycle cutoff instead of the normal cutoff.
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
        "SELECT cycle_cutoff_days, failed_cycle_cutoff_days,
                keep_failed_cycles, delete_batch_rows
         FROM history_cleanup_policy
         WHERE poll_group_id IS NULL AND enabled = 1",
        [],
        |r| {
            Ok((
                r.get::<_, Option<i64>>(0)?,
                r.get::<_, Option<i64>>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, i64>(3)?,
            ))
        },
    )
    .ok()
    .and_then(|(days, failed_days, keep_failed, batch)| {
        // A NULL or non-positive cutoff means "keep everything" — the setting
        // is absent, not zero days.
        days.filter(|d| *d > 0).map(|cutoff_days| CyclePolicy {
            cutoff_days,
            failed_cutoff_days: failed_days.filter(|d| *d > 0),
            keep_failed: keep_failed != 0,
            batch_rows: batch.clamp(100, 100_000),
        })
    })
}

/// How long samples are kept, as read from `history_cleanup_policy`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SamplePolicy {
    /// Samples taken more than this many days ago are eligible.
    pub cutoff_days: i64,
    /// Rows per DELETE statement.
    pub batch_rows: i64,
}

/// The global sample policy, or `None` when samples are kept forever.
///
/// Like the cycle policy, only the global row is consulted.
pub fn load_sample_policy(conn: &Connection) -> Option<SamplePolicy> {
    conn.query_row(
        "SELECT cutoff_days, delete_batch_rows
         FROM history_cleanup_policy
         WHERE poll_group_id IS NULL AND enabled = 1",
        [],
        |r| Ok((r.get::<_, Option<i64>>(0)?, r.get::<_, i64>(1)?)),
    )
    .ok()
    .and_then(|(days, batch)| {
        days.filter(|d| *d > 0).map(|cutoff_days| SamplePolicy {
            cutoff_days,
            batch_rows: batch.clamp(100, 100_000),
        })
    })
}

/// Delete up to `max_batches` batches of expired samples, returning the count.
///
/// Walks one dataset at a time so every DELETE is a range on
/// `ix_sample_dataset_ts`: a bare `ts < cutoff` has no index and would scan the
/// whole table on each pass once the backlog is gone. Active datasets are
/// included — collection that never stops is the case this exists for.
pub fn sweep_samples(
    conn: &Connection,
    policy: SamplePolicy,
    now_us: i64,
    max_batches: u32,
) -> i64 {
    let cutoff = now_us - policy.cutoff_days * 86_400_000_000;
    let datasets: Vec<i64> = match conn
        .prepare("SELECT id FROM dataset ORDER BY id")
        .and_then(|mut stmt| stmt.query_map([], |r| r.get(0))?.collect())
    {
        Ok(ids) => ids,
        Err(e) => {
            warn!("sample retention: {e}");
            return 0;
        }
    };
    let mut deleted = 0;
    let mut batches = 0;

    // Only batches that deleted something count against the budget: an empty
    // dataset costs one index probe, and charging it would leave datasets past
    // the first `max_batches` never swept.
    'datasets: for dataset_id in datasets {
        loop {
            if batches >= max_batches {
                break 'datasets;
            }
            let n = match conn.execute(
                "DELETE FROM sample
                 WHERE rowid IN (
                   SELECT rowid FROM sample
                   WHERE dataset_id = ?1 AND ts < ?2
                   LIMIT ?3
                 )",
                rusqlite::params![dataset_id, cutoff, policy.batch_rows],
            ) {
                Ok(n) => n as i64,
                Err(e) => {
                    warn!("sample retention: {e}");
                    break 'datasets;
                }
            };
            if n > 0 {
                batches += 1;
                deleted += n;
            }
            if n < policy.batch_rows {
                continue 'datasets;
            }
        }
    }

    if deleted > 0 {
        debug!(
            "sample retention: deleted {deleted} samples older than {} days",
            policy.cutoff_days
        );
    }
    deleted
}

/// Delete up to `max_batches` batches of expired cycles, returning the count.
///
/// Stops early when a batch comes back empty. The bound lets the caller pause
/// between batches: a sweep that ran until done would, on first contact with a
/// 12.7M-row backlog, hold the database against the writer for that long.
pub fn sweep(conn: &Connection, policy: CyclePolicy, now_us: i64, max_batches: u32) -> i64 {
    let cutoff = now_us - policy.cutoff_days * 86_400_000_000;
    let failed_cutoff = policy
        .failed_cutoff_days
        .filter(|d| *d > 0)
        .map(|days| now_us - days * 86_400_000_000);
    let use_failed_cutoff = i64::from(policy.keep_failed);
    let mut deleted = 0;

    for _ in 0..max_batches {
        // Only unreferenced cycles. A cycle whose samples are still within the
        // sample retention window is not expired — it is the provenance of data
        // the operator still has, and the FK would refuse the delete anyway.
        let n = select_then_delete(
            conn,
            "SELECT rowid FROM poll_cycle
             WHERE (
                 (ok = 1 AND finished_ts < ?1)
                 OR (ok = 0 AND (
                     (?2 = 0 AND finished_ts < ?1)
                     OR (?2 = 1 AND ?3 IS NOT NULL AND finished_ts < ?3)
                 ))
             )
               AND NOT EXISTS (SELECT 1 FROM sample s WHERE s.cycle_id = poll_cycle.id)
               AND NOT EXISTS (SELECT 1 FROM event_marker em WHERE em.cycle_id = poll_cycle.id)
               AND NOT EXISTS (SELECT 1 FROM plc_time_sample pts WHERE pts.cycle_id = poll_cycle.id)
             LIMIT ?4",
            rusqlite::params![cutoff, use_failed_cutoff, failed_cutoff, policy.batch_rows],
            "DELETE FROM poll_cycle
             WHERE rowid IN (SELECT value FROM json_each(?1))
               AND NOT EXISTS (SELECT 1 FROM sample s WHERE s.cycle_id = poll_cycle.id)
               AND NOT EXISTS (SELECT 1 FROM event_marker em WHERE em.cycle_id = poll_cycle.id)
               AND NOT EXISTS (SELECT 1 FROM plc_time_sample pts WHERE pts.cycle_id = poll_cycle.id)",
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

/// Delete spans that no remaining sample references. Spans are metadata for a
/// stored sample, not an audit log of every failed physical read; keeping
/// unreferenced rows made `read_span` larger than `poll_cycle` in the field.
pub fn sweep_orphan_spans(conn: &Connection, batch_rows: i64, max_batches: u32) -> i64 {
    let batch_rows = batch_rows.clamp(100, 100_000);
    let mut deleted = 0;
    for _ in 0..max_batches {
        match select_then_delete(
            conn,
            "SELECT rs.rowid FROM read_span rs
             WHERE NOT EXISTS (SELECT 1 FROM sample s WHERE s.span_id = rs.id)
             LIMIT ?1",
            rusqlite::params![batch_rows],
            "DELETE FROM read_span
             WHERE rowid IN (SELECT value FROM json_each(?1))
               AND NOT EXISTS (SELECT 1 FROM sample s WHERE s.span_id = read_span.id)",
        ) {
            Ok(0) => break,
            Ok(n) => deleted += n as i64,
            Err(e) => {
                warn!("read_span retention: {e}");
                break;
            }
        }
    }
    deleted
}

/// Rows deleted by one retention pass.
#[derive(Debug, Default, PartialEq)]
pub struct PassTotals {
    /// Days written to `poll_cycle_daily`.
    pub days: i64,
    pub samples: i64,
    pub cycles: i64,
    pub spans: i64,
}

/// The retention thread: a pass shortly after start, then one a day, until
/// `running` clears.
pub fn retention_worker(conn: &Connection, running: &AtomicBool) {
    let mut next = Instant::now() + FIRST_PASS_DELAY;
    while running.load(Ordering::Relaxed) {
        if Instant::now() < next {
            std::thread::sleep(Duration::from_secs(1));
            continue;
        }
        let started = Instant::now();
        let t = run_pass(conn, running, crate::types::now_us(), BATCH_PAUSE);
        info!(
            "history retention: summarised {} day(s), deleted {} samples, {} poll_cycle rows and {} orphan read_span rows in {:.1} s; next pass in 24 h",
            t.days,
            t.samples,
            t.cycles,
            t.spans,
            started.elapsed().as_secs_f64()
        );
        next = Instant::now() + PASS_INTERVAL;
    }
}

/// Delete everything the policy has expired as of `now_us`, one batch at a
/// time with `pause` between batches. Stops early when `running` clears.
///
/// The daily cycle summary comes first, while the cycles it counts are all
/// still there; then samples, so the cycles and spans they referenced go in the
/// same pass.
pub fn run_pass(
    conn: &Connection,
    running: &AtomicBool,
    now_us: i64,
    pause: Duration,
) -> PassTotals {
    let mut t = PassTotals::default();
    let batches = |step: &mut dyn FnMut() -> i64| -> i64 {
        let mut total = 0;
        while running.load(Ordering::Relaxed) {
            let n = step();
            if n == 0 {
                break;
            }
            total += n;
            std::thread::sleep(pause);
        }
        total
    };
    let cycles = load_policy(conn);
    t.days = crate::engine::cycle_daily::summarise_days(
        conn,
        running,
        now_us,
        cycles.map(|p| p.cutoff_days),
        pause,
    );
    if let Some(p) = load_sample_policy(conn) {
        t.samples = batches(&mut || sweep_samples(conn, p, now_us, 1));
    }
    if let Some(p) = cycles {
        t.cycles = batches(&mut || sweep(conn, p, now_us, 1));
    }
    // Span cleanup is independent of cycle retention: a user may keep every
    // cycle forever and still expect metadata orphaned by sample deletion to go.
    let batch_rows = cycles.map(|p| p.batch_rows).unwrap_or(10_000);
    t.spans = batches(&mut || sweep_orphan_spans(conn, batch_rows, 1));
    t
}

/// VACUUM the database when retention has left enough free pages to be worth
/// rewriting the file; returns whether it ran.
///
/// Meant for server startup, before collection: VACUUM rewrites the whole file
/// under an exclusive lock and needs free disk space about the size of the
/// result. A failure (typically a full disk) is logged and startup continues on
/// the unchanged database.
pub fn vacuum_if_worthwhile(conn: &Connection) -> bool {
    let pages = |pragma: &str| conn.query_row(pragma, [], |r| r.get::<_, i64>(0));
    let (total, free, page_size) = match (
        pages("PRAGMA page_count"),
        pages("PRAGMA freelist_count"),
        pages("PRAGMA page_size"),
    ) {
        (Ok(t), Ok(f), Ok(s)) => (t, f, s),
        _ => return false,
    };
    let free_bytes = free * page_size;
    let mb = |pages: i64| (pages * page_size) as f64 / 1_048_576.0;
    if total == 0
        || (free as f64) < VACUUM_MIN_FREE_SHARE * total as f64
        || free_bytes < VACUUM_MIN_FREE_BYTES
    {
        debug!(
            "startup vacuum: skipped, {:.0} MB free of {:.0} MB",
            mb(free),
            mb(total)
        );
        return false;
    }
    info!(
        "startup vacuum: compacting, {:.0} MB free of {:.0} MB; collection starts when it finishes",
        mb(free),
        mb(total)
    );
    let started = Instant::now();
    let result = conn.execute_batch("VACUUM");
    // Under WAL the rewritten file passes through the log; truncate it so the
    // space comes back now rather than at some later checkpoint.
    let _ = conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)");
    match result {
        Ok(()) => {
            let after = pages("PRAGMA page_count").unwrap_or(0);
            info!(
                "startup vacuum: done in {:.1} s, {:.0} MB -> {:.0} MB",
                started.elapsed().as_secs_f64(),
                mb(total),
                mb(after)
            );
            true
        }
        Err(e) => {
            warn!("startup vacuum failed, continuing without it: {e}");
            false
        }
    }
}

/// Find up to a batch of rowids with `select`, then delete them with `delete`,
/// which receives them as a JSON array in `?1`.
///
/// Finding is the expensive half: when nothing has expired, the cycle and span
/// queries probe every old row and find none — seconds on the field database.
/// As a plain read under WAL that search runs beside the sample writer instead
/// of holding the write lock against it; only the delete of rows already found
/// takes the lock. `delete` repeats the reference checks, so a row that gained
/// a reference in between stays.
fn select_then_delete(
    conn: &Connection,
    select: &str,
    params: impl rusqlite::Params,
    delete: &str,
) -> rusqlite::Result<usize> {
    let ids: Vec<i64> = conn
        .prepare_cached(select)?
        .query_map(params, |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    if ids.is_empty() {
        return Ok(0);
    }
    let ids = serde_json::to_string(&ids).expect("a list of integers serializes");
    conn.prepare_cached(delete)?.execute([ids])
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
        assert_eq!(p.failed_cutoff_days, Some(30));
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
            failed_cutoff_days: Some(30),
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
            failed_cutoff_days: Some(30),
            keep_failed: true,
            batch_rows: 1000,
        };
        assert_eq!(sweep(&conn, policy, now, 10), 1);
        assert_eq!(cycle_ids(&conn), vec![1]);
    }

    #[test]
    fn failed_cycles_are_bounded_by_their_own_cutoff() {
        let conn = fixture();
        let now = 100 * DAY;
        add_cycle(&conn, 1, now - 31 * DAY, false); // failed retention expired
        add_cycle(&conn, 2, now - 29 * DAY, false); // failed retention remains
        let policy = CyclePolicy {
            cutoff_days: 14,
            failed_cutoff_days: Some(30),
            keep_failed: true,
            batch_rows: 1000,
        };
        assert_eq!(sweep(&conn, policy, now, 10), 1);
        assert_eq!(cycle_ids(&conn), vec![2]);
    }

    #[test]
    fn orphan_spans_are_removed_but_sample_spans_remain() {
        let conn = fixture();
        conn.execute_batch(
            "INSERT INTO read_span (id, plc_id, poll_group_id, db_num, byte_offset, byte_len)
             VALUES (1, 1, 1, 1, 0, 2), (2, 1, 1, 1, 2, 2);
             INSERT INTO tag (id, plc_id, name, address, db_num, byte_offset,
                              byte_size, data_type, poll_group_id, created_ts, updated_ts)
             VALUES (1, 1, 't1', 'DB1.DBW0', 1, 0, 2, 'u16', 1, 0, 0);
             INSERT INTO sample (dataset_id, cycle_id, tag_id, span_id, ts, vi)
             VALUES (1, NULL, 1, 2, 1, 42);",
        )
        .unwrap();

        assert_eq!(sweep_orphan_spans(&conn, 1000, 10), 1);
        let remaining: Vec<i64> = conn
            .prepare("SELECT id FROM read_span ORDER BY id")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(remaining, vec![2]);
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
            failed_cutoff_days: Some(30),
            keep_failed: false,
            batch_rows: 1000,
        };
        assert_eq!(sweep(&conn, policy, now, 10), 0);
        assert_eq!(cycle_ids(&conn), vec![1]);
    }

    fn add_sample(conn: &Connection, dataset_id: i64, cycle_id: Option<i64>, ts: i64) {
        conn.execute(
            "INSERT INTO sample (dataset_id, cycle_id, tag_id, ts, vi) VALUES (?1, ?2, 1, ?3, 0)",
            rusqlite::params![dataset_id, cycle_id, ts],
        )
        .unwrap();
    }

    fn sample_ts(conn: &Connection) -> Vec<i64> {
        conn.prepare("SELECT ts FROM sample ORDER BY ts")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    }

    fn with_tag(conn: &Connection) {
        conn.execute_batch(
            "INSERT INTO tag (id, plc_id, name, address, db_num, byte_offset,
                              byte_size, data_type, poll_group_id, created_ts, updated_ts)
                 VALUES (1, 1, 't1', 'DB1.DBW0', 1, 0, 2, 'u16', 1, 0, 0);",
        )
        .unwrap();
    }

    #[test]
    fn samples_are_kept_forever_by_default() {
        let conn = fixture();
        assert_eq!(load_sample_policy(&conn), None);
        conn.execute_batch("UPDATE history_cleanup_policy SET cutoff_days = 30")
            .unwrap();
        assert_eq!(
            load_sample_policy(&conn),
            Some(SamplePolicy {
                cutoff_days: 30,
                batch_rows: 10_000
            })
        );
    }

    /// The field case: collection never stopped, so expiry must work on an
    /// active dataset, and across every dataset of the database.
    #[test]
    fn expired_samples_go_from_active_and_stopped_datasets() {
        let conn = fixture();
        with_tag(&conn);
        conn.execute_batch(
            "INSERT INTO dataset (id, plc_id, name, state, created_ts, updated_ts)
                 VALUES (2, 1, 'ds2', 'stopped', 0, 0);",
        )
        .unwrap();
        let now = 100 * DAY;
        add_sample(&conn, 1, None, now - 40 * DAY);
        add_sample(&conn, 1, None, now - DAY);
        add_sample(&conn, 2, None, now - 50 * DAY);
        let policy = SamplePolicy {
            cutoff_days: 30,
            batch_rows: 1000,
        };
        assert_eq!(sweep_samples(&conn, policy, now, 10), 2);
        assert_eq!(sample_ts(&conn), vec![now - DAY]);
    }

    /// Samples pin their cycles; once they expire, the cycle sweep that follows
    /// in the same pass can take the cycles too.
    #[test]
    fn expiring_samples_releases_their_cycles() {
        let conn = fixture();
        with_tag(&conn);
        let now = 100 * DAY;
        add_cycle(&conn, 1, now - 40 * DAY, true);
        add_sample(&conn, 1, Some(1), now - 40 * DAY);
        let cycles = CyclePolicy {
            cutoff_days: 14,
            failed_cutoff_days: Some(30),
            keep_failed: true,
            batch_rows: 1000,
        };
        assert_eq!(sweep(&conn, cycles, now, 10), 0);
        let samples = SamplePolicy {
            cutoff_days: 30,
            batch_rows: 1000,
        };
        assert_eq!(sweep_samples(&conn, samples, now, 10), 1);
        assert_eq!(sweep(&conn, cycles, now, 10), 1);
        assert!(cycle_ids(&conn).is_empty());
    }

    #[test]
    fn empty_datasets_do_not_use_up_the_sample_budget() {
        let conn = fixture();
        with_tag(&conn);
        for id in 2..=5 {
            conn.execute(
                "INSERT INTO dataset (id, plc_id, name, state, created_ts, updated_ts)
                 VALUES (?1, 1, ?2, 'stopped', 0, 0)",
                rusqlite::params![id, format!("ds{id}")],
            )
            .unwrap();
        }
        let now = 100 * DAY;
        add_sample(&conn, 5, None, now - 40 * DAY);
        let policy = SamplePolicy {
            cutoff_days: 30,
            batch_rows: 1000,
        };
        assert_eq!(sweep_samples(&conn, policy, now, 1), 1);
    }

    #[test]
    fn a_sample_pass_deletes_at_most_its_batch_budget() {
        let conn = fixture();
        with_tag(&conn);
        let now = 100 * DAY;
        for i in 0..10 {
            add_sample(&conn, 1, None, now - 40 * DAY + i);
        }
        let policy = SamplePolicy {
            cutoff_days: 30,
            batch_rows: 3,
        };
        assert_eq!(sweep_samples(&conn, policy, now, 2), 6);
        assert_eq!(sample_ts(&conn).len(), 4);
    }

    #[test]
    fn a_pass_runs_until_nothing_has_expired() {
        let conn = fixture();
        with_tag(&conn);
        conn.execute_batch(
            "UPDATE history_cleanup_policy SET cutoff_days = 30, delete_batch_rows = 100",
        )
        .unwrap();
        let now = 100 * DAY;
        for id in 1..=250 {
            add_cycle(&conn, id, now - 40 * DAY, true);
            add_sample(&conn, 1, Some(id), now - 40 * DAY);
        }
        add_cycle(&conn, 251, now - DAY, true);
        add_sample(&conn, 1, Some(251), now - DAY);
        let running = AtomicBool::new(true);
        let t = run_pass(&conn, &running, now, Duration::ZERO);
        assert_eq!(
            t,
            PassTotals {
                // Only the day with a cycle: the fixture's tag is in no
                // enabled set, so its cycle-less days get no summary.
                days: 1,
                samples: 250,
                cycles: 250,
                spans: 0
            }
        );
        assert_eq!(cycle_ids(&conn), vec![251]);
        assert_eq!(sample_ts(&conn), vec![now - DAY]);
    }

    #[test]
    fn a_pass_stops_when_the_server_does() {
        let conn = fixture();
        with_tag(&conn);
        conn.execute_batch("UPDATE history_cleanup_policy SET cutoff_days = 30")
            .unwrap();
        add_sample(&conn, 1, None, 0);
        let running = AtomicBool::new(false);
        assert_eq!(
            run_pass(&conn, &running, 100 * DAY, Duration::ZERO),
            PassTotals::default()
        );
        assert_eq!(sample_ts(&conn).len(), 1);
    }

    #[test]
    fn vacuum_runs_only_when_enough_space_is_free() {
        let dir = std::env::temp_dir().join(format!("s7trace-vacuum-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("v.db");
        let _ = std::fs::remove_file(&path);
        let conn = crate::db::schema::open(path.to_str().unwrap()).unwrap();
        // A fresh database has nothing to reclaim.
        assert!(!vacuum_if_worthwhile(&conn));
        conn.execute_batch(
            "CREATE TABLE filler (b BLOB);
             WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM c WHERE i < 20000)
             INSERT INTO filler SELECT randomblob(4096) FROM c;
             DELETE FROM filler;
             PRAGMA wal_checkpoint(TRUNCATE);",
        )
        .unwrap();
        let pages = |p: &str| conn.query_row(p, [], |r| r.get::<_, i64>(0)).unwrap();
        let before = pages("PRAGMA page_count");
        assert!(vacuum_if_worthwhile(&conn));
        assert!(pages("PRAGMA page_count") < before / 4);
        assert_eq!(pages("PRAGMA freelist_count"), 0);
        drop(conn);
        let _ = std::fs::remove_dir_all(&dir);
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
            failed_cutoff_days: Some(30),
            keep_failed: false,
            batch_rows: 2,
        };
        assert_eq!(sweep(&conn, policy, now, 3), 6);
        assert_eq!(cycle_ids(&conn).len(), 4);
    }
}
