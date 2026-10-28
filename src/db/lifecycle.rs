//! Lifecycle markers in `event_marker`: server start/stop and collection
//! start/stop.
//!
//! Restarts and pauses used to show up only as holes in `poll_cycle`, which
//! retention later thins, so they could not be told from quiet periods. Each lifecycle change now leaves a row:
//!
//! | kind | when | details_json |
//! |---|---|---|
//! | `server_start` | the server starts | `version`, `previous_stop_ts`, `previous_stop_clean`, `down_ms` |
//! | `server_stop` | clean shutdown | `clean: true` |
//! | `server_stop` | written at the next start when the previous run ended without one (crash, killed process, power loss) | `clean: false`, `reconstructed: true`; `ts` is the last poll cycle the previous run finished |
//! | `collection_start` | `collection-start` | `dataset`, `sets` |
//! | `collection_stop` | `collection-stop` | `dataset` |
//!
//! `event_marker.plc_id` is mandatory, so the server markers are written once
//! per enabled PLC, tagged with that PLC's active dataset if it has one.

use rusqlite::{params, Connection, OptionalExtension};

pub const SERVER_START: &str = "server_start";
pub const SERVER_STOP: &str = "server_stop";
pub const COLLECTION_START: &str = "collection_start";
pub const COLLECTION_STOP: &str = "collection_stop";

fn insert(
    conn: &Connection,
    plc_id: i64,
    dataset_id: Option<i64>,
    ts: i64,
    kind: &str,
    severity: &str,
    details: &serde_json::Value,
) -> rusqlite::Result<()> {
    conn.prepare_cached(
        "INSERT INTO event_marker (dataset_id, plc_id, ts, kind, severity, details_json)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
    )?
    .execute(params![
        dataset_id,
        plc_id,
        ts,
        kind,
        severity,
        details.to_string()
    ])?;
    Ok(())
}

/// Enabled PLCs with their active dataset, if any.
fn enabled_plcs(conn: &Connection) -> rusqlite::Result<Vec<(i64, Option<i64>)>> {
    conn.prepare(
        "SELECT p.id, d.id FROM plc p
         LEFT JOIN dataset d ON d.plc_id = p.id AND d.state = 'active'
         WHERE p.enabled = 1 ORDER BY p.id",
    )?
    .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
    .collect()
}

/// The newest `server_start`/`server_stop` marker of a PLC: `(kind, ts)`.
fn last_server_marker(conn: &Connection, plc_id: i64) -> rusqlite::Result<Option<(String, i64)>> {
    // Two lookups on ix_event_marker_kind_ts rather than one `kind IN (...)`
    // ordered by ts, which would sort every lifecycle row of the PLC.
    let newest = |kind: &str| -> rusqlite::Result<Option<i64>> {
        conn.query_row(
            "SELECT MAX(ts) FROM event_marker WHERE kind = ?1 AND plc_id = ?2",
            params![kind, plc_id],
            |r| r.get(0),
        )
    };
    Ok(match (newest(SERVER_START)?, newest(SERVER_STOP)?) {
        (None, None) => None,
        (Some(start), Some(stop)) if stop >= start => Some((SERVER_STOP.into(), stop)),
        (Some(start), _) => Some((SERVER_START.into(), start)),
        (None, Some(stop)) => Some((SERVER_STOP.into(), stop)),
    })
}

/// The last poll cycle a PLC finished, after `after_ts`. One index lookup per
/// poll group on ix_poll_cycle_plc_group_finished; a plain `MAX` over the PLC
/// would walk every cycle it has.
fn last_cycle_ts(conn: &Connection, plc_id: i64, after_ts: i64) -> rusqlite::Result<Option<i64>> {
    let groups: Vec<i64> = conn
        .prepare("SELECT id FROM poll_group")?
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    let mut last: Option<i64> = None;
    for g in groups {
        let ts: Option<i64> = conn.query_row(
            "SELECT MAX(finished_ts) FROM poll_cycle
             WHERE plc_id = ?1 AND poll_group_id = ?2 AND finished_ts > ?3",
            params![plc_id, g, after_ts],
            |r| r.get(0),
        )?;
        last = last.max(ts);
    }
    Ok(last)
}

/// The newest `collection_start`/`collection_stop` of a PLC after `after_ts`.
fn last_collection_marker_ts(
    conn: &Connection,
    plc_id: i64,
    after_ts: i64,
) -> rusqlite::Result<Option<i64>> {
    let mut last = None;
    for kind in [COLLECTION_START, COLLECTION_STOP] {
        let ts: Option<i64> = conn.query_row(
            "SELECT MAX(ts) FROM event_marker WHERE kind = ?1 AND plc_id = ?2 AND ts > ?3",
            params![kind, plc_id, after_ts],
            |r| r.get(0),
        )?;
        last = last.max(ts);
    }
    Ok(last)
}

/// Record that the server started, first reconstructing the previous run's
/// `server_stop` when it ended without writing one.
pub fn record_server_start(conn: &Connection, now: i64, version: &str) -> rusqlite::Result<()> {
    for (plc_id, dataset_id) in enabled_plcs(conn)? {
        let (previous_stop_ts, previous_stop_clean) = match last_server_marker(conn, plc_id)? {
            Some((kind, ts)) if kind == SERVER_STOP => (Some(ts), Some(true)),
            Some((_, started)) => {
                // The previous run started but never said it stopped. The
                // latest thing it is known to have done, a finished cycle or a
                // collection start/stop, is the best evidence of when it did.
                let stopped = last_cycle_ts(conn, plc_id, started)?
                    .max(last_collection_marker_ts(conn, plc_id, started)?);
                if let Some(ts) = stopped {
                    insert(
                        conn,
                        plc_id,
                        dataset_id,
                        ts,
                        SERVER_STOP,
                        "warning",
                        &serde_json::json!({ "clean": false, "reconstructed": true }),
                    )?;
                }
                (stopped.or(Some(started)), Some(false))
            }
            None => (None, None),
        };
        insert(
            conn,
            plc_id,
            dataset_id,
            now,
            SERVER_START,
            "info",
            &serde_json::json!({
                "version": version,
                "previous_stop_ts": previous_stop_ts,
                "previous_stop_clean": previous_stop_clean,
                "down_ms": previous_stop_ts.map(|ts| (now - ts) / 1000),
            }),
        )?;
    }
    Ok(())
}

/// Record a clean shutdown.
pub fn record_server_stop(conn: &Connection, now: i64) -> rusqlite::Result<()> {
    for (plc_id, dataset_id) in enabled_plcs(conn)? {
        insert(
            conn,
            plc_id,
            dataset_id,
            now,
            SERVER_STOP,
            "info",
            &serde_json::json!({ "clean": true }),
        )?;
    }
    Ok(())
}

/// Record `collection-start` of `dataset_id`.
pub fn record_collection_start(
    conn: &Connection,
    plc_id: i64,
    dataset_id: i64,
    dataset: &str,
    sets: &[String],
    now: i64,
) -> rusqlite::Result<()> {
    insert(
        conn,
        plc_id,
        Some(dataset_id),
        now,
        COLLECTION_START,
        "info",
        &serde_json::json!({ "dataset": dataset, "sets": sets }),
    )
}

/// Stop the PLC's active dataset: set `stopped_ts` and record
/// `collection_stop`. Returns the stopped dataset's name, or `None` when the
/// PLC was not collecting.
pub fn stop_active_dataset(
    conn: &Connection,
    plc_id: i64,
    now: i64,
) -> rusqlite::Result<Option<String>> {
    let active: Option<(i64, String)> = conn
        .query_row(
            "SELECT id, name FROM dataset WHERE plc_id = ?1 AND state = 'active'",
            params![plc_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let Some((dataset_id, name)) = active else {
        return Ok(None);
    };
    conn.execute(
        "UPDATE dataset SET state = 'stopped', stopped_ts = ?1, updated_ts = ?1 WHERE id = ?2",
        params![now, dataset_id],
    )?;
    insert(
        conn,
        plc_id,
        Some(dataset_id),
        now,
        COLLECTION_STOP,
        "info",
        &serde_json::json!({ "dataset": name }),
    )?;
    Ok(Some(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn db() -> Connection {
        let conn = crate::db::schema::open_memory().unwrap();
        conn.execute_batch(
            "INSERT INTO plc (id, name, host) VALUES (1, 'p1', 'h'), (2, 'p2', 'h');
             INSERT INTO plc (id, name, host, enabled) VALUES (3, 'off', 'h', 0);
             INSERT INTO poll_group (id, name, period_ms) VALUES (1, 'fast', 100), (2, 'slow', 1000);
             INSERT INTO dataset (id, plc_id, name, state, started_ts, created_ts, updated_ts)
             VALUES (1, 1, 'run1', 'active', 10, 10, 10);",
        )
        .unwrap();
        conn
    }

    fn markers(
        conn: &Connection,
        plc_id: i64,
    ) -> Vec<(String, i64, Option<i64>, serde_json::Value)> {
        conn.prepare(
            "SELECT kind, ts, dataset_id, details_json FROM event_marker
             WHERE plc_id = ?1 ORDER BY ts, id",
        )
        .unwrap()
        .query_map([plc_id], |r| {
            let d: String = r.get(3)?;
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                serde_json::from_str(&d).unwrap(),
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
    }

    fn cycle(conn: &Connection, plc: i64, group: i64, seq: i64, finished: i64) {
        conn.execute(
            "INSERT INTO poll_cycle (dataset_id, plc_id, poll_group_id, seq, started_ts,
                                     finished_ts, target_ms, actual_ms, ok)
             VALUES (1, ?1, ?2, ?3, ?4, ?4, 100, 1.0, 1)",
            params![plc, group, seq, finished],
        )
        .unwrap();
    }

    #[test]
    fn first_start_has_no_previous_stop() {
        let conn = db();
        record_server_start(&conn, 1_000, "1.2.3").unwrap();
        let m = markers(&conn, 1);
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].0, SERVER_START);
        assert_eq!(m[0].2, Some(1), "tagged with the active dataset");
        assert_eq!(m[0].3["version"], "1.2.3");
        assert!(m[0].3["previous_stop_ts"].is_null());
        assert_eq!(markers(&conn, 2)[0].2, None, "no active dataset");
        assert!(markers(&conn, 3).is_empty(), "disabled PLCs get no markers");
    }

    #[test]
    fn clean_restart_reports_the_downtime() {
        let conn = db();
        record_server_start(&conn, 1_000, "v").unwrap();
        record_server_stop(&conn, 5_000_000).unwrap();
        record_server_start(&conn, 65_000_000, "v").unwrap();
        let m = markers(&conn, 1);
        let kinds: Vec<&str> = m.iter().map(|x| x.0.as_str()).collect();
        assert_eq!(kinds, [SERVER_START, SERVER_STOP, SERVER_START]);
        assert_eq!(m[1].3["clean"], true);
        assert_eq!(m[2].3["previous_stop_ts"], 5_000_000);
        assert_eq!(m[2].3["previous_stop_clean"], true);
        assert_eq!(m[2].3["down_ms"], 60_000);
    }

    #[test]
    fn unclean_stop_is_reconstructed_from_the_last_cycle() {
        let conn = db();
        record_server_start(&conn, 1_000, "v").unwrap();
        cycle(&conn, 1, 1, 1, 2_000_000);
        cycle(&conn, 1, 2, 1, 3_000_000);
        cycle(&conn, 1, 1, 2, 2_500_000);
        // The process dies here: no server_stop.
        record_server_start(&conn, 10_000_000, "v").unwrap();

        let m = markers(&conn, 1);
        let kinds: Vec<&str> = m.iter().map(|x| x.0.as_str()).collect();
        assert_eq!(kinds, [SERVER_START, SERVER_STOP, SERVER_START]);
        assert_eq!(m[1].1, 3_000_000, "the last cycle of any group");
        assert_eq!(m[1].3["clean"], false);
        assert_eq!(m[1].3["reconstructed"], true);
        assert_eq!(m[2].3["previous_stop_ts"], 3_000_000);
        assert_eq!(m[2].3["previous_stop_clean"], false);
        assert_eq!(m[2].3["down_ms"], 7_000);

        // PLC 2 never ran a cycle: no reconstructed stop, the previous start
        // stands in for it.
        let m2 = markers(&conn, 2);
        let kinds: Vec<&str> = m2.iter().map(|x| x.0.as_str()).collect();
        assert_eq!(kinds, [SERVER_START, SERVER_START]);
        assert_eq!(m2[1].3["previous_stop_ts"], 1_000);
        assert_eq!(m2[1].3["previous_stop_clean"], false);
    }

    #[test]
    fn a_collection_stop_after_the_last_cycle_dates_the_unclean_stop() {
        let conn = db();
        record_server_start(&conn, 1_000, "v").unwrap();
        cycle(&conn, 1, 1, 1, 2_000_000);
        stop_active_dataset(&conn, 1, 4_000_000).unwrap();
        record_server_start(&conn, 10_000_000, "v").unwrap();
        let m = markers(&conn, 1);
        let kinds: Vec<&str> = m.iter().map(|x| x.0.as_str()).collect();
        assert_eq!(
            kinds,
            [SERVER_START, COLLECTION_STOP, SERVER_STOP, SERVER_START]
        );
        assert_eq!(m[2].1, 4_000_000);
    }

    #[test]
    fn stopping_collection_records_the_time() {
        let conn = db();
        assert_eq!(stop_active_dataset(&conn, 2, 50).unwrap(), None);
        assert!(markers(&conn, 2).is_empty());

        assert_eq!(
            stop_active_dataset(&conn, 1, 50).unwrap().as_deref(),
            Some("run1")
        );
        let (state, stopped): (String, Option<i64>) = conn
            .query_row(
                "SELECT state, stopped_ts FROM dataset WHERE id = 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!((state.as_str(), stopped), ("stopped", Some(50)));
        let m = markers(&conn, 1);
        assert_eq!(m.len(), 1);
        assert_eq!(
            (m[0].0.as_str(), m[0].1, m[0].2),
            (COLLECTION_STOP, 50, Some(1))
        );
        assert_eq!(m[0].3["dataset"], "run1");
    }
}
