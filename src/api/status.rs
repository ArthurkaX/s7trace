use crate::types::now_us;
use crate::version;
use parking_lot::Mutex;
use rusqlite::Connection;
use std::sync::Arc;

use super::Response;

pub(crate) fn handle_status(db: &Arc<Mutex<Connection>>) -> Response {
    let conn = db.lock();

    let plcs: Vec<serde_json::Value> = conn
        .prepare("SELECT id, name, host, enabled, last_ok_ts, last_error, connectivity FROM plc")
        .ok()
        .and_then(|mut s| {
            s.query_map([], |r| {
                let plc_id: i64 = r.get(0)?;
                let plc_name: String = r.get(1)?;

                // Per-PLC: poll groups with latest cycle stats
                let poll_groups: Vec<serde_json::Value> = conn
                    .prepare(
                        "SELECT pg.name, pg.period_ms,
                            COALESCE(latest.actual_ms, 0),
                            COALESCE(latest.overrun_count, 0)
                         FROM poll_group pg
                         JOIN tag t ON t.poll_group_id = pg.id
                         LEFT JOIN (
                            SELECT pc.poll_group_id,
                                AVG(pc.actual_ms) as actual_ms,
                                COUNT(CASE WHEN pc.actual_ms > pc.target_ms THEN 1 END) as overrun_count
                            FROM poll_cycle pc
                            WHERE pc.plc_id = ?1
                              AND pc.finished_ts > ?2
                            GROUP BY pc.poll_group_id
                         ) latest ON latest.poll_group_id = pg.id
                         WHERE t.plc_id = ?1 AND t.enabled = 1
                         GROUP BY pg.id
                         ORDER BY pg.id",
                    )
                    .ok()
                    .and_then(|mut stmt| {
                        let cutoff = now_us() - 3_600_000_000; // last hour
                        stmt.query_map(rusqlite::params![plc_id, cutoff], |row| {
                            Ok(serde_json::json!({
                                "name": row.get::<_, String>(0)?,
                                "target_ms": row.get::<_, i64>(1)?,
                                "actual_ms": row.get::<_, f64>(2)?,
                                "overrun_count": row.get::<_, i64>(3)?,
                            }))
                        })
                        .ok()
                        .map(|it| it.filter_map(|x| x.ok()).collect())
                    })
                    .unwrap_or_default();

                // Per-PLC: last poll cycle finished timestamp
                let last_poll_cycle_finished_ts: Option<i64> = conn
                    .query_row(
                        "SELECT MAX(finished_ts) FROM poll_cycle WHERE plc_id = ?1",
                        rusqlite::params![plc_id],
                        |r| r.get(0),
                    )
                    .unwrap_or(None);

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
                let last_ok_ts = r.get::<_, Option<i64>>(4)?;
                let last_error = r.get::<_, Option<String>>(5)?;
                let connectivity = r.get::<_, String>(6)?;

                // Derived health, surfaced so operators can see at a glance
                // which PLCs are degraded without parsing connectivity/last_ok.
                // "stale" = nominally online but no successful poll recently.
                const STALE_AFTER_US: i64 = 60_000_000; // 60s
                let stale = match last_ok_ts {
                    Some(ts) => now_us() - ts > STALE_AFTER_US,
                    None => true,
                };
                let health = if !enabled {
                    "disabled"
                } else if connectivity != "online" {
                    "unavailable"
                } else if stale {
                    "stale"
                } else {
                    "ok"
                };

                Ok(serde_json::json!({
                    "name": plc_name,
                    "host": r.get::<_, String>(2)?,
                    "enabled": enabled,
                    "connected": connectivity == "online",
                    "health": health,
                    "last_ok_ts": last_ok_ts,
                    "last_error": last_error,
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

    // Lightweight runtime metrics. Totals are cheap COUNTs; the latency/health
    // summary is over the last hour of poll cycles so it reflects current
    // behavior rather than all history. (Per-PLC reconnect counts are tracked
    // separately and surfaced once the plc table carries them.)
    let samples_written: i64 = conn
        .query_row("SELECT COUNT(*) FROM sample", [], |r| r.get(0))
        .unwrap_or(0);
    let poll_cycles_total: i64 = conn
        .query_row("SELECT COUNT(*) FROM poll_cycle", [], |r| r.get(0))
        .unwrap_or(0);
    let hour_cutoff = now_us() - 3_600_000_000; // 1h in microseconds
    let (cycles_ok, cycles_failed, lat_avg, lat_max): (i64, i64, f64, f64) = conn
        .query_row(
            "SELECT
                COALESCE(SUM(CASE WHEN ok = 1 THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN ok = 0 THEN 1 ELSE 0 END), 0),
                COALESCE(AVG(actual_ms), 0.0),
                COALESCE(MAX(actual_ms), 0.0)
             FROM poll_cycle WHERE finished_ts > ?1",
            rusqlite::params![hour_cutoff],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .unwrap_or((0, 0, 0.0, 0.0));

    Response::json(serde_json::json!({
        "service": "s7trace",
        "version": version::status_json(&conn),
        "plcs": plcs,
        "tags": { "active": active_tags, "bad": bad_tags_total },
        "command_queue": { "pending": pending_count, "running": running_count },
        "config_apply": { "pending": config_apply_pending },
        "metrics": {
            "samples_written": samples_written,
            "poll_cycles_total": poll_cycles_total,
            "last_hour": {
                "cycles_ok": cycles_ok,
                "cycles_failed": cycles_failed,
                "poll_latency_ms": { "avg": lat_avg, "max": lat_max },
            },
        },
    }))
}
