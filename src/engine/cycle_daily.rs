//! Daily `poll_cycle` summaries that outlive cycle retention.
//!
//! Cycle retention keeps only the cycles samples still reference once they are
//! older than `cycle_cutoff_days`. After that, a quiet stretch with nothing
//! changing and a real polling outage look the same: a hole between referenced
//! cycles. Each retention pass first writes one `poll_cycle_daily` row per
//! complete UTC day, PLC and poll group while that day's cycles are all still
//! there: cycle and failure counts, overruns, latency, and the longest gap.
//!
//! A day is summarised once. Days older than the cycle cutoff are skipped, since
//! their cycles may already be thinned and the counts would be wrong.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use rusqlite::{params, Connection};
use tracing::warn;

const DAY_US: i64 = 86_400_000_000;
/// With cycles kept forever, how far back the first pass goes.
const MAX_BACKFILL_DAYS: i64 = 400;

/// One day of one poll group.
#[derive(Debug, Default, PartialEq)]
struct DaySummary {
    cycles: i64,
    failed: i64,
    overruns: i64,
    first_ts: Option<i64>,
    last_ts: Option<i64>,
    max_gap_ms: f64,
    max_gap_end_ts: i64,
    avg_actual_ms: Option<f64>,
    max_actual_ms: Option<f64>,
}

/// Summarise one `[day_ts, day_ts + 1 day)` of one poll group.
///
/// Gaps run between consecutive finished cycles. The first one starts at the
/// previous finished cycle when it is at or after `intact_from` (older cycles
/// may be thinned, and a deleted neighbour would inflate the gap), otherwise
/// at midnight. The end of the day is the last end point, so an outage still
/// running at midnight counts in the day it started too.
fn summarise_day(
    conn: &Connection,
    plc_id: i64,
    poll_group_id: i64,
    day_ts: i64,
    intact_from: i64,
) -> rusqlite::Result<DaySummary> {
    let day_end = day_ts + DAY_US;
    let prev: Option<i64> = conn
        .prepare_cached(
            "SELECT MAX(finished_ts) FROM poll_cycle
             WHERE plc_id = ?1 AND poll_group_id = ?2 AND finished_ts < ?3",
        )?
        .query_row(params![plc_id, poll_group_id, day_ts], |r| r.get(0))?;
    let mut last_point = prev.filter(|ts| *ts >= intact_from).unwrap_or(day_ts);

    let mut s = DaySummary::default();
    let gap = |s: &mut DaySummary, from: i64, to: i64| {
        let ms = (to - from) as f64 / 1000.0;
        if ms > s.max_gap_ms {
            s.max_gap_ms = ms;
            s.max_gap_end_ts = to;
        }
    };
    let mut actual_sum = 0.0;
    let mut stmt = conn.prepare_cached(
        "SELECT finished_ts, ok, actual_ms, target_ms FROM poll_cycle
         WHERE plc_id = ?1 AND poll_group_id = ?2 AND finished_ts >= ?3 AND finished_ts < ?4
         ORDER BY finished_ts",
    )?;
    let mut rows = stmt.query(params![plc_id, poll_group_id, day_ts, day_end])?;
    while let Some(r) = rows.next()? {
        let ts: i64 = r.get(0)?;
        let ok: i64 = r.get(1)?;
        let actual: f64 = r.get(2)?;
        let target: i64 = r.get(3)?;
        s.cycles += 1;
        if ok == 0 {
            s.failed += 1;
        }
        if actual > target as f64 {
            s.overruns += 1;
        }
        actual_sum += actual;
        s.max_actual_ms = Some(s.max_actual_ms.map_or(actual, |m| m.max(actual)));
        s.first_ts.get_or_insert(ts);
        s.last_ts = Some(ts);
        gap(&mut s, last_point, ts);
        last_point = ts;
    }
    gap(&mut s, last_point, day_end);
    if s.cycles > 0 {
        s.avg_actual_ms = Some(actual_sum / s.cycles as f64);
    }
    Ok(s)
}

/// Write summaries for every complete day not summarised yet, oldest first,
/// pausing between days. Returns the number of days written.
///
/// `cycle_cutoff_days` is the cycle retention cutoff (`None`: kept forever).
/// A day is only summarised while it is entirely within it.
pub fn summarise_days(
    conn: &Connection,
    running: &AtomicBool,
    now_us: i64,
    cycle_cutoff_days: Option<i64>,
    pause: Duration,
) -> i64 {
    match summarise_days_inner(conn, running, now_us, cycle_cutoff_days, pause) {
        Ok(n) => n,
        Err(e) => {
            warn!("poll_cycle daily summary: {e}");
            0
        }
    }
}

fn summarise_days_inner(
    conn: &Connection,
    running: &AtomicBool,
    now_us: i64,
    cycle_cutoff_days: Option<i64>,
    pause: Duration,
) -> rusqlite::Result<i64> {
    let today = now_us.div_euclid(DAY_US) * DAY_US;
    let intact_from = match cycle_cutoff_days {
        Some(days) => now_us - days * DAY_US,
        None => i64::MIN,
    };
    // The first whole day at or after `intact_from`, and no earlier than the
    // oldest cycle there is.
    let oldest_cycle: Option<i64> =
        conn.query_row("SELECT MIN(finished_ts) FROM poll_cycle", [], |r| r.get(0))?;
    let Some(oldest_cycle) = oldest_cycle else {
        return Ok(0);
    };
    let first_day = [
        intact_from.saturating_add(DAY_US - 1).div_euclid(DAY_US) * DAY_US,
        oldest_cycle.div_euclid(DAY_US) * DAY_US,
        today - MAX_BACKFILL_DAYS * DAY_US,
    ]
    .into_iter()
    .max()
    .unwrap_or(today);

    let done: std::collections::HashSet<i64> = conn
        .prepare("SELECT DISTINCT day_ts FROM poll_cycle_daily WHERE day_ts >= ?1")?
        .query_map([first_day], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    // Every PLC/poll group pair a tag names. A pair that is meant to be
    // collected (an enabled, healthy tag in an enabled set) gets a row even for
    // a day without a single cycle: that is the outage record. A pair nothing
    // asks to collect gets one only on days it had cycles.
    let pairs: Vec<(i64, i64, bool)> = conn
        .prepare(
            "SELECT t.plc_id, t.poll_group_id,
                    MAX(t.enabled = 1 AND t.bad = 0 AND EXISTS (
                        SELECT 1 FROM metric_set_tag mst
                        JOIN metric_set ms ON ms.id = mst.metric_set_id
                        WHERE mst.tag_id = t.id AND ms.enabled = 1))
             FROM tag t
             GROUP BY t.plc_id, t.poll_group_id ORDER BY t.plc_id, t.poll_group_id",
        )?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get::<_, i64>(2)? != 0)))?
        .collect::<rusqlite::Result<_>>()?;

    let mut written = 0;
    let mut day = first_day;
    while day + DAY_US <= today && running.load(Ordering::Relaxed) {
        if !done.contains(&day) {
            let mut rows = Vec::new();
            for &(plc_id, group_id, wanted) in &pairs {
                let s = summarise_day(conn, plc_id, group_id, day, intact_from)?;
                if s.cycles > 0 || wanted {
                    rows.push((plc_id, group_id, s));
                }
            }
            if !rows.is_empty() {
                let tx = conn.unchecked_transaction()?;
                for (plc_id, group_id, s) in &rows {
                    tx.prepare_cached(
                        "INSERT OR REPLACE INTO poll_cycle_daily
                            (day_ts, plc_id, poll_group_id, cycles, failed, overruns,
                             first_ts, last_ts, max_gap_ms, max_gap_end_ts,
                             avg_actual_ms, max_actual_ms, computed_ts)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                    )?
                    .execute(params![
                        day,
                        plc_id,
                        group_id,
                        s.cycles,
                        s.failed,
                        s.overruns,
                        s.first_ts,
                        s.last_ts,
                        s.max_gap_ms,
                        s.max_gap_end_ts,
                        s.avg_actual_ms,
                        s.max_actual_ms,
                        now_us,
                    ])?;
                }
                tx.commit()?;
                written += 1;
                std::thread::sleep(pause);
            }
        }
        day += DAY_US;
    }
    Ok(written)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIN: i64 = 60_000_000;

    fn fixture() -> Connection {
        let conn = crate::db::schema::open_memory().unwrap();
        conn.execute_batch(
            "INSERT INTO plc (id, name, host) VALUES (1, 'p', 'h');
             INSERT INTO poll_group (id, name, period_ms) VALUES (1, 'fast', 100), (2, 'idle', 100);
             INSERT INTO dataset (id, plc_id, name, state, created_ts, updated_ts)
             VALUES (1, 1, 'd', 'active', 0, 0);
             INSERT INTO tag (id, plc_id, name, address, db_num, byte_offset, byte_size,
                              data_type, poll_group_id, enabled, created_ts, updated_ts)
             VALUES (1, 1, 'a', 'DB1.DBW0', 1, 0, 2, 'i16', 1, 1, 0, 0),
                    (2, 1, 'b', 'DB1.DBW2', 1, 2, 2, 'i16', 2, 1, 0, 0);
             INSERT INTO metric_set (id, plc_id, name, enabled, created_ts, updated_ts)
             VALUES (1, 1, 'on', 1, 0, 0), (2, 1, 'off', 0, 0, 0);
             INSERT INTO metric_set_tag (metric_set_id, tag_id, plc_id, created_ts)
             VALUES (1, 1, 1, 0), (2, 2, 1, 0);",
        )
        .unwrap();
        conn
    }

    fn cycle(conn: &Connection, seq: i64, finished: i64, ok: bool, actual: f64) {
        conn.execute(
            "INSERT INTO poll_cycle (dataset_id, plc_id, poll_group_id, seq, started_ts,
                                     finished_ts, target_ms, actual_ms, ok)
             VALUES (1, 1, 1, ?1, ?2, ?2, 100, ?3, ?4)",
            params![seq, finished, actual, i64::from(ok)],
        )
        .unwrap();
    }

    type Row = (i64, i64, i64, i64, f64, i64, Option<f64>);

    fn rows(conn: &Connection) -> Vec<Row> {
        conn.prepare(
            "SELECT day_ts, poll_group_id, cycles, failed, max_gap_ms, max_gap_end_ts, max_actual_ms
             FROM poll_cycle_daily ORDER BY day_ts, poll_group_id",
        )
        .unwrap()
        .query_map([], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?))
        })
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
    }

    #[test]
    fn complete_days_are_summarised_once() {
        let conn = fixture();
        let d0 = 100 * DAY_US;
        // Day 0: a cycle at 00:01, a failed one at 00:02, nothing until 06:00
        // (the longest gap), then one every four hours and a last at 23:59.
        cycle(&conn, 1, d0 + MIN, true, 10.0);
        cycle(&conn, 2, d0 + 2 * MIN, false, 150.0);
        for (seq, minute) in [
            (3, 360),
            (4, 600),
            (5, 840),
            (6, 1080),
            (7, 1320),
            (8, 1439),
        ] {
            cycle(&conn, seq, d0 + minute * MIN, true, 20.0);
        }
        // Day 1: nothing at all. Day 2: today, incomplete.
        cycle(&conn, 9, d0 + 2 * DAY_US + MIN, true, 10.0);
        let running = AtomicBool::new(true);
        let now = d0 + 2 * DAY_US + 5 * MIN;

        assert_eq!(
            summarise_days(&conn, &running, now, Some(14), Duration::ZERO),
            2
        );
        let r = rows(&conn);
        // Group 2's tag is enabled but only in a disabled set: nothing asks
        // for it to be collected, so its cycle-less days get no rows.
        assert_eq!(r.len(), 2, "{r:?}");
        let (day, group, cycles, failed, gap_ms, gap_end, max_actual) = r[0];
        assert_eq!((day, group, cycles, failed), (d0, 1, 8, 1));
        assert_eq!((gap_ms, gap_end), (358.0 * 60_000.0, d0 + 360 * MIN));
        assert_eq!(max_actual, Some(150.0));
        // The empty day: no cycles, and the gap runs from 23:59 the day
        // before to midnight.
        let (day, _, cycles, failed, gap_ms, gap_end, max_actual) = r[1];
        assert_eq!((day, cycles, failed, max_actual), (d0 + DAY_US, 0, 0, None));
        assert_eq!(gap_end, d0 + 2 * DAY_US);
        assert_eq!(gap_ms, (DAY_US + MIN) as f64 / 1000.0);

        let overruns: i64 = conn
            .query_row(
                "SELECT overruns FROM poll_cycle_daily WHERE day_ts = ?1",
                [d0],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(overruns, 1);

        // A second pass has nothing left to do; a day later, only the day
        // that has completed since.
        assert_eq!(
            summarise_days(&conn, &running, now, Some(14), Duration::ZERO),
            0
        );
        assert_eq!(
            summarise_days(&conn, &running, now + DAY_US, Some(14), Duration::ZERO),
            1
        );
    }

    #[test]
    fn days_past_the_cycle_cutoff_are_not_summarised() {
        let conn = fixture();
        let now = 100 * DAY_US + 12 * 60 * MIN;
        for (seq, days_ago) in [(1, 20), (2, 3), (3, 1)] {
            cycle(&conn, seq, now - days_ago * DAY_US, true, 10.0);
        }
        let running = AtomicBool::new(true);
        summarise_days(&conn, &running, now, Some(3), Duration::ZERO);
        let days: Vec<i64> = rows(&conn).iter().map(|r| r.0).collect();
        // Cutoff at day 97 12:00: day 97 is partly gone, 98 and 99 are whole.
        assert_eq!(days, vec![98 * DAY_US, 99 * DAY_US]);
    }

    #[test]
    fn a_thinned_neighbour_does_not_inflate_the_first_gap() {
        let conn = fixture();
        let now = 100 * DAY_US + 12 * 60 * MIN;
        // Older than the cutoff: the cycles around it were thinned away.
        cycle(&conn, 1, now - 10 * DAY_US, true, 10.0);
        cycle(&conn, 2, 98 * DAY_US + 30 * MIN, true, 10.0);
        let running = AtomicBool::new(true);
        summarise_days(&conn, &running, now, Some(2), Duration::ZERO);
        let r = rows(&conn);
        assert_eq!(r[0].0, 99 * DAY_US);
        // Day 99 has no cycles. The day-98 cycle precedes the cutoff (98 12:00)
        // and may have lost its neighbours, so the gap starts at midnight.
        assert_eq!(r[0].4, DAY_US as f64 / 1000.0);
    }

    #[test]
    fn nothing_is_written_when_stopped() {
        let conn = fixture();
        cycle(&conn, 1, 10 * DAY_US, true, 10.0);
        let running = AtomicBool::new(false);
        assert_eq!(
            summarise_days(&conn, &running, 12 * DAY_US, None, Duration::ZERO),
            0
        );
        assert!(rows(&conn).is_empty());
    }
}
