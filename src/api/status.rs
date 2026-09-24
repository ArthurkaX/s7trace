use crate::engine::health::{GroupHealth, LatencySummary};
use crate::types::now_us;
use crate::version;
use std::sync::Arc;

use super::{ApiState, Response};

/// Staleness threshold used when no collector health is available (the API
/// served over a database alone). The live path scales the threshold to each
/// poll group's own period instead — see [`GroupHealth::stale_after_us`].
const FALLBACK_STALE_AFTER_US: i64 = 60_000_000; // 60s

pub(crate) fn handle_status(state: &ApiState) -> Response {
    let health = state.health.as_ref();
    let conn = state.db.lock();
    let now = now_us();

    let plcs: Vec<serde_json::Value> = conn
        .prepare("SELECT id, name, host, enabled, last_ok_ts, last_error, connectivity FROM plc")
        .ok()
        .and_then(|mut s| {
            s.query_map([], |r| {
                let plc_id: i64 = r.get(0)?;
                let plc_name: String = r.get(1)?;

                // Per-PLC: the poll groups this PLC actually has tags in.
                // Configuration comes from the tables; the runtime numbers
                // (latency, overruns, liveness) come from the collector's own
                // counters. Deriving them from `poll_cycle` meant a GROUP BY
                // over a 12.7M-row table on every status request.
                let pg_rows: Vec<(i64, String, i64)> = conn
                    .prepare(
                        "SELECT pg.id, pg.name, pg.period_ms
                         FROM poll_group pg
                         JOIN tag t ON t.poll_group_id = pg.id
                         WHERE t.plc_id = ?1 AND t.enabled = 1
                         GROUP BY pg.id
                         ORDER BY pg.id",
                    )
                    .ok()
                    .and_then(|mut stmt| {
                        stmt.query_map(rusqlite::params![plc_id], |row| {
                            Ok((
                                row.get::<_, i64>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, i64>(2)?,
                            ))
                        })
                        .ok()
                        .map(|it| it.filter_map(|x| x.ok()).collect())
                    })
                    .unwrap_or_default();

                let groups: Vec<(i64, String, i64, Option<Arc<GroupHealth>>)> = pg_rows
                    .into_iter()
                    .map(|(pg_id, name, period_ms)| {
                        let g = health.and_then(|h| h.get_group(plc_id, pg_id));
                        (pg_id, name, period_ms, g)
                    })
                    .collect();

                let poll_groups: Vec<serde_json::Value> = groups
                    .iter()
                    .map(|(_, name, period_ms, g)| {
                        let hour = g.as_ref().map(|g| g.last_hour()).unwrap_or_default();
                        serde_json::json!({
                            "name": name,
                            "target_ms": period_ms,
                            "actual_ms": hour.latency_avg_ms,
                            "overrun_count": hour.overruns,
                            // `null` when the collector is not in this process
                            // (an API served over a database alone).
                            "running": g.as_ref().map(|g| g.is_alive()),
                            "restarts": g.as_ref().map(|g| g.restarts()),
                            "cycles_ok": g.as_ref().map(|g| g.cycles_ok()),
                            "cycles_failed": g.as_ref().map(|g| g.cycles_failed()),
                            "last_cycle_ts": g.as_ref().and_then(|g| g.last_cycle_ts()),
                            "last_ok_ts": g.as_ref().and_then(|g| g.last_ok_ts()),
                            "last_error": g.as_ref().and_then(|g| g.last_error()),
                        })
                    })
                    .collect();

                // Per-PLC: last poll cycle finished timestamp. From memory when
                // the collector is here — the equivalent `MAX(finished_ts)` was
                // another full scan, and a stopped collector cannot update the
                // row it would have been read from.
                let last_poll_cycle_finished_ts: Option<i64> = match health {
                    Some(h) => h.plc_last_cycle_ts(plc_id),
                    None => conn
                        .query_row(
                            "SELECT MAX(finished_ts) FROM poll_cycle WHERE plc_id = ?1",
                            rusqlite::params![plc_id],
                            |r| r.get(0),
                        )
                        .unwrap_or(None),
                };

                // Per-PLC: metric sets with tag counts and effective poll info
                let ms_rows: Vec<(i64, String, bool, i64, i64, i64)> = conn
                    .prepare(
                        "SELECT ms.id, ms.name, ms.enabled,
                            (SELECT COUNT(*) FROM metric_set_tag mst
                             JOIN tag t ON t.id = mst.tag_id
                             WHERE mst.metric_set_id = ms.id) as tags_total,
                            (SELECT COUNT(*) FROM metric_set_tag mst
                             JOIN tag t ON t.id = mst.tag_id
                             WHERE mst.metric_set_id = ms.id AND t.enabled = 1 AND t.bad = 0) as tags_active,
                            (SELECT COUNT(*) FROM metric_set_tag mst
                             JOIN tag t ON t.id = mst.tag_id
                             WHERE mst.metric_set_id = ms.id AND t.bad = 1) as tags_bad
                         FROM metric_set ms
                         WHERE ms.plc_id = ?1
                         ORDER BY ms.id",
                    )
                    .ok()
                    .and_then(|mut s| {
                        s.query_map(rusqlite::params![plc_id], |row| {
                            Ok((
                                row.get::<_, i64>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, i64>(2)? != 0,
                                row.get::<_, i64>(3)?,
                                row.get::<_, i64>(4)?,
                                row.get::<_, i64>(5)?,
                            ))
                        })
                        .ok()
                        .map(|it| it.filter_map(|x| x.ok()).collect())
                    })
                    .unwrap_or_default();

                let mut metric_sets: Vec<serde_json::Value> = Vec::new();
                for (ms_id, ms_name, ms_enabled, tags_total, tags_active, tags_bad) in ms_rows {
                    // Determine effective_poll for this metric set
                    let enabled_in_set: i64 = conn
                        .query_row(
                            "SELECT COUNT(*) FROM metric_set_tag mst
                             JOIN tag t ON t.id = mst.tag_id
                             WHERE mst.metric_set_id = ?1 AND t.enabled = 1",
                            rusqlite::params![ms_id],
                            |r| r.get(0),
                        )
                        .unwrap_or(0);

                    let effective_poll = if enabled_in_set == 0 {
                        serde_json::json!({
                            "mode": "none",
                            "poll_group_id": null,
                            "poll_group_name": null,
                            "period_ms": null,
                            "rate_hz": null,
                        })
                    } else {
                        let groups: Vec<(i64, String, i64)> = conn
                            .prepare(
                                "SELECT DISTINCT t.poll_group_id, pg.name, pg.period_ms
                                 FROM metric_set_tag mst
                                 JOIN tag t ON t.id = mst.tag_id AND t.enabled = 1
                                 JOIN poll_group pg ON pg.id = t.poll_group_id
                                 WHERE mst.metric_set_id = ?1
                                 ORDER BY t.poll_group_id",
                            )
                            .ok()
                            .and_then(|mut s| {
                                s.query_map(rusqlite::params![ms_id], |row| {
                                    Ok((
                                        row.get::<_, i64>(0)?,
                                        row.get::<_, String>(1)?,
                                        row.get::<_, i64>(2)?,
                                    ))
                                })
                                .ok()
                                .map(|it| it.filter_map(|x| x.ok()).collect())
                            })
                            .unwrap_or_default();

                        if groups.is_empty() {
                            serde_json::json!({
                                "mode": "none",
                                "poll_group_id": null,
                                "poll_group_name": null,
                                "period_ms": null,
                                "rate_hz": null,
                            })
                        } else if groups.len() == 1 {
                            let (pg_id, ref pg_name, period_ms) = groups[0];
                            let rate_hz =
                                (1000.0 / period_ms as f64 * 10000.0).round() / 10000.0;
                            serde_json::json!({
                                "mode": "single",
                                "poll_group_id": pg_id,
                                "poll_group_name": pg_name,
                                "period_ms": period_ms,
                                "rate_hz": rate_hz,
                            })
                        } else {
                            serde_json::json!({
                                "mode": "mixed",
                                "poll_group_id": null,
                                "poll_group_name": null,
                                "period_ms": null,
                                "rate_hz": null,
                            })
                        }
                    };

                    metric_sets.push(serde_json::json!({
                        "name": ms_name,
                        "enabled": ms_enabled,
                        "tags_total": tags_total,
                        "tags_active": tags_active,
                        "tags_bad": tags_bad,
                        "effective_poll": effective_poll,
                    }));
                }

                // Per-PLC: bad tags
                let bad_tags: Vec<serde_json::Value> = conn
                    .prepare(
                        "SELECT t.id, t.name, t.address, t.bad_class, t.bad_detail, t.last_error_ts
                         FROM tag t
                         WHERE t.plc_id = ?1 AND t.bad = 1
                         ORDER BY t.id",
                    )
                    .ok()
                    .and_then(|mut stmt| {
                        stmt.query_map(rusqlite::params![plc_id], |row| {
                            Ok(serde_json::json!({
                                "id": row.get::<_, i64>(0)?,
                                "name": row.get::<_, String>(1)?,
                                "address": row.get::<_, String>(2)?,
                                "bad_class": row.get::<_, Option<String>>(3)?,
                                "bad_detail": row.get::<_, Option<String>>(4)?,
                                "last_error_ts": row.get::<_, Option<i64>>(5)?,
                            }))
                        })
                        .ok()
                        .map(|it| it.filter_map(|x| x.ok()).collect())
                    })
                    .unwrap_or_default();

                let enabled = r.get::<_, i64>(3)? != 0;
                let db_last_ok_ts = r.get::<_, Option<i64>>(4)?;
                let db_last_error = r.get::<_, Option<String>>(5)?;
                let connectivity = r.get::<_, String>(6)?;

                // Derived health, surfaced so operators can see at a glance
                // which PLCs are degraded.
                //
                // The `plc` table can only say what the collector last managed
                // to write, so when the collector stops the row freezes at
                // whatever was true at that moment — in the field that meant 22
                // days of `online` with nothing being collected. With the
                // collector in this process, every judgement below comes from
                // its live counters instead, and the staleness window is each
                // group's own period rather than a fixed 60 s (which reported
                // both PLCs `stale` while 850 ms cycles were landing normally).
                let (health_str, connected, last_ok_ts, last_error) = match health {
                    Some(_) if !enabled => ("disabled", false, db_last_ok_ts, db_last_error),
                    Some(h) => {
                        let known: Vec<&Arc<GroupHealth>> =
                            groups.iter().filter_map(|(_, _, _, g)| g.as_ref()).collect();
                        let alive = known.iter().filter(|g| g.is_alive()).count();
                        // "Connected" has to mean a running poller is getting
                        // data. A group that stopped seconds ago is not stale
                        // yet, but nothing is asking the PLC anything.
                        let fresh = alive > 0 && known.iter().any(|g| !g.is_stale(now));
                        let plc_last_ok = h.plc_last_ok_ts(plc_id);
                        let last_error = known
                            .iter()
                            .filter(|g| !g.is_alive() || g.is_stale(now))
                            .find_map(|g| g.last_error())
                            .or(db_last_error);
                        let s = if groups.is_empty() {
                            // Configured, enabled, nothing to poll.
                            "idle"
                        } else if alive == 0 {
                            // The state that went unreported for 22 days.
                            "stopped"
                        } else if fresh {
                            "ok"
                        } else if connectivity != "online" {
                            "unavailable"
                        } else {
                            "stale"
                        };
                        (s, fresh, plc_last_ok.or(db_last_ok_ts), last_error)
                    }
                    // No collector in this process: the database is all we have.
                    None => {
                        let stale = match db_last_ok_ts {
                            Some(ts) => now - ts > FALLBACK_STALE_AFTER_US,
                            None => true,
                        };
                        let s = if !enabled {
                            "disabled"
                        } else if connectivity != "online" {
                            "unavailable"
                        } else if stale {
                            "stale"
                        } else {
                            "ok"
                        };
                        (s, connectivity == "online", db_last_ok_ts, db_last_error)
                    }
                };

                Ok(serde_json::json!({
                    "name": plc_name,
                    "host": r.get::<_, String>(2)?,
                    "enabled": enabled,
                    "connected": connected,
                    "health": health_str,
                    "last_ok_ts": last_ok_ts,
                    "last_error": last_error,
                    // Kept as both `conn` and `connectivity`: the CLI status
                    // view reads `conn`, and printed a blank column for years
                    // because only the longer name was ever emitted.
                    "conn": connectivity,
                    "connectivity": connectivity,
                    "last_poll_cycle_finished_ts": last_poll_cycle_finished_ts,
                    "poll_groups": poll_groups,
                    "metric_sets": metric_sets,
                    "bad_tags": bad_tags,
                }))
            })
            .ok()
            .map(|it| it.filter_map(|x| x.ok()).collect())
        })
        .unwrap_or_default();

    let bad_tags_total: i64 = conn
        .query_row("SELECT COUNT(*) FROM tag WHERE bad = 1", [], |r| r.get(0))
        .unwrap_or(0);
    let active_tags: i64 = conn
        .query_row("SELECT COUNT(*) FROM v_active_tag", [], |r| r.get(0))
        .unwrap_or(0);
    let pending_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM command_request WHERE state = 'pending'",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);
    let running_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM command_request WHERE state = 'running'",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);
    let config_apply_pending: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM config_apply WHERE state IN ('pending','applying')",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);

    // Runtime metrics.
    //
    // `COUNT(*)` on `sample` and `poll_cycle` is a full scan in SQLite; at the
    // sizes this collector reaches (12.7M cycles in the field) those two counts
    // were most of a 12 s status request, for numbers nobody needs to the row.
    // When the collector is in this process they come from a snapshot the
    // monitor loop refreshes once a minute on its own connection, stamped with
    // `as_of_ts` so the age is visible rather than implied. The last-hour
    // summary comes from the collector's rolling window — the same question the
    // `WHERE finished_ts > ?` aggregate answered, in constant time.
    let (samples_written, poll_cycles_total, totals_as_of_ts) = match health {
        Some(h) => {
            let t = h.db_totals();
            (t.samples, t.poll_cycles, t.as_of_ts)
        }
        None => (
            conn.query_row("SELECT COUNT(*) FROM sample", [], |r| r.get(0))
                .unwrap_or(0),
            conn.query_row("SELECT COUNT(*) FROM poll_cycle", [], |r| r.get(0))
                .unwrap_or(0),
            Some(now),
        ),
    };

    let hour: LatencySummary = match health {
        Some(h) => h.last_hour(),
        None => {
            let hour_cutoff = now - 3_600_000_000; // 1h in microseconds
            conn.query_row(
                "SELECT
                    COALESCE(SUM(CASE WHEN ok = 1 THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN ok = 0 THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN actual_ms > target_ms THEN 1 ELSE 0 END), 0),
                    COALESCE(AVG(actual_ms), 0.0),
                    COALESCE(MAX(actual_ms), 0.0)
                 FROM poll_cycle WHERE finished_ts > ?1",
                rusqlite::params![hour_cutoff],
                |r| {
                    Ok(LatencySummary {
                        cycles_ok: r.get::<_, i64>(0)? as u64,
                        cycles_failed: r.get::<_, i64>(1)? as u64,
                        overruns: r.get::<_, i64>(2)? as u64,
                        latency_avg_ms: r.get(3)?,
                        latency_max_ms: r.get(4)?,
                    })
                },
            )
            .unwrap_or_default()
        }
    };

    // Collector liveness, so an operator can tell "nothing is being collected"
    // from "everything is fine" without correlating timestamps by hand.
    let collector = match health {
        Some(h) => {
            let groups = h.groups();
            let alive = groups.iter().filter(|g| g.is_alive()).count();
            serde_json::json!({
                "present": true,
                "writer_alive": h.writer_alive(),
                "writer_restarts": h.writer_restarts(),
                "writer_last_write_ts": h.writer_last_write_ts(),
                "pollers_alive": alive,
                "pollers_total": groups.len(),
                "poller_restarts": groups.iter().map(|g| g.restarts()).sum::<u64>(),
            })
        }
        None => serde_json::json!({ "present": false }),
    };

    Response::json(serde_json::json!({
        "service": "s7trace",
        "version": version::status_json(&conn),
        "plcs": plcs,
        "tags": { "active": active_tags, "bad": bad_tags_total },
        "command_queue": { "pending": pending_count, "running": running_count },
        "config_apply": { "pending": config_apply_pending },
        "collector": collector,
        "metrics": {
            "samples_written": samples_written,
            "poll_cycles_total": poll_cycles_total,
            "totals_as_of_ts": totals_as_of_ts,
            "last_hour": {
                "cycles_ok": hour.cycles_ok,
                "cycles_failed": hour.cycles_failed,
                "overruns": hour.overruns,
                "poll_latency_ms": {
                    "avg": hour.latency_avg_ms,
                    "max": hour.latency_max_ms,
                },
            },
        },
    }))
}
