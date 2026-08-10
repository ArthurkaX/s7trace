//! `status` command handler (read-only status report).

use anyhow::Result;
use rusqlite::Connection;

use crate::cli::args::Cli;
use crate::cli::format::Format;
use crate::server_base;
use crate::types::*;
use crate::{cli, db, version};

/// Staleness threshold for the local (database-only) view: with no collector
/// to ask, a fixed window is all there is. The server's own `/status` scales it
/// to each poll group's period instead.
const STALE_AFTER_US: i64 = 60_000_000; // 60s

pub(crate) fn handle_status(cli: &Cli, conn: &Connection, fmt: &str) -> Result<ExitCode> {
    // Remote mode: fetch JSON from server and pretty-print.
    if let Some(base) = server_base(cli) {
        let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);
        let v = cli::remote::get_status(&base, timeout)?;
        if fmt == "json" || fmt == "jsonl" {
            println!("{}", serde_json::to_string_pretty(&v)?);
        } else {
            // text mode: human-readable summary from the API JSON.
            println!("(remote server: {base})");
            println!(
                "s7trace version={} schema={} api={}",
                v["version"]["package"].as_str().unwrap_or("?"),
                v["version"]["schema"].as_str().unwrap_or("?"),
                v["version"]["api"].as_i64().unwrap_or(0)
            );
            println!(
                "tags: active={} bad={}",
                v["tags"]["active"], v["tags"]["bad"]
            );
            println!(
                "command_queue: pending={} running={}",
                v["command_queue"]["pending"], v["command_queue"]["running"]
            );
            println!("PLCs:");
            for plc in v["plcs"].as_array().unwrap_or(&Vec::new()) {
                let pg_array = plc["poll_groups"].as_array().cloned().unwrap_or_default();
                let pg_parts: Vec<String> = pg_array
                    .iter()
                    .map(|pg| {
                        let target = pg["target_ms"].as_i64().unwrap_or(0);
                        let act = pg["actual_ms"].as_f64().unwrap_or(-1.0);
                        let actual = if act >= 0.0 {
                            format!("{act:.2}")
                        } else {
                            "-".into()
                        };
                        let utilization = if act > 0.0 && target > 0 {
                            let pct = (act / target as f64) * 100.0;
                            if pct > 90.0 {
                                format!(" {pct:.0}% HIGH")
                            } else if pct > 50.0 {
                                format!(" {pct:.0}% mod")
                            } else {
                                format!(" {pct:.0}% low")
                            }
                        } else {
                            String::new()
                        };
                        format!(
                            "{} target={}ms actual={}ms{utilization} overruns={}",
                            pg["name"], target, actual, pg["overrun_count"]
                        )
                    })
                    .collect();
                println!(
                    "  {} (host={}, health={}, conn={}) last_ok_ts={:?}",
                    plc["name"],
                    plc["host"],
                    plc["health"].as_str().unwrap_or("?"),
                    plc["connectivity"],
                    plc["last_ok_ts"]
                );
                for pg_line in pg_parts {
                    println!("    poll_group: {pg_line}");
                }
                if let Some(err) = plc["last_error"].as_str() {
                    if !err.is_empty() {
                        println!("    last_error: {err}");
                    }
                }
                for ms in plc["metric_sets"].as_array().unwrap_or(&Vec::new()) {
                    let ep = &ms["effective_poll"];
                    let poll_desc = match ep["mode"].as_str() {
                        Some("single") => {
                            let pgn = ep["poll_group_name"].as_str().unwrap_or("?");
                            let per = ep["period_ms"].as_i64().unwrap_or(0);
                            let hz = ep["rate_hz"].as_f64().unwrap_or(0.0);
                            format!("{pgn} {per}ms / {hz:.1}Hz")
                        }
                        _ => "off".into(),
                    };
                    println!(
                        "    set {} enabled={} tags={}/{} poll={}",
                        ms["name"], ms["enabled"], ms["tags_healthy"], ms["tags_total"], poll_desc
                    );
                }
            }
        }
        return Ok(ExitCode::Ok);
    }
    let out_fmt = Format::from_str(fmt);

    // Gather PLCs
    let mut stmt = conn
        .prepare("SELECT id, name, host, enabled, last_ok_ts, last_error, connectivity FROM plc")?;
    let plcs: Vec<serde_json::Value> = stmt
        .query_map([], |row| {
            let id: i64 = row.get(0)?;
            let name: String = row.get(1)?;
            let host: String = row.get(2)?;
            let enabled: bool = row.get::<_, i64>(3)? != 0;
            let last_ok_ts: Option<i64> = row.get(4)?;
            let last_error: Option<String> = row.get(5)?;
            let connectivity: String = row.get(6)?;

            // Active datasets for this PLC
            let mut ds_stmt = conn.prepare(
                "SELECT name, started_ts FROM dataset WHERE plc_id = ?1 AND state = 'active'",
            )?;
            let datasets: Vec<serde_json::Value> = ds_stmt
                .query_map(rusqlite::params![id], |r| {
                    let n: String = r.get(0)?;
                    let s: Option<i64> = r.get(1)?;
                    Ok(serde_json::json!({"name": n, "started_ts": s}))
                })?
                .filter_map(|x| x.ok())
                .collect();

            // Per-(plc, poll_group) recent cycle metrics.
            //
            // Both subqueries are bounded to the last hour and driven by
            // ix_poll_cycle_plc_group_finished. The unbounded form counted
            // overruns across all history — a full scan of a table that reached
            // 12.7M rows in the field, per poll group, per `status` call.
            let mut pg_stmt = conn.prepare(
                "SELECT DISTINCT pg.id, pg.name, pg.period_ms,
                        (SELECT pc.actual_ms FROM poll_cycle pc
                         WHERE pc.plc_id = ?1 AND pc.poll_group_id = pg.id
                         ORDER BY pc.finished_ts DESC LIMIT 1),
                        (SELECT COUNT(*) FROM poll_cycle pc
                         WHERE pc.plc_id = ?1 AND pc.poll_group_id = pg.id
                           AND pc.finished_ts > ?2
                           AND pc.actual_ms > pg.period_ms),
                        (SELECT MAX(pc.finished_ts) FROM poll_cycle pc
                         WHERE pc.plc_id = ?1 AND pc.poll_group_id = pg.id)
                 FROM poll_group pg
                 JOIN tag t ON t.poll_group_id = pg.id
                 WHERE t.plc_id = ?1
                 ORDER BY pg.id",
            )?;
            let hour_cutoff = now_us() - 3_600_000_000;
            let pg_rows: Vec<(serde_json::Value, Option<i64>)> = pg_stmt
                .query_map(rusqlite::params![id, hour_cutoff], |r| {
                    let n: String = r.get(1)?;
                    let target: i64 = r.get(2)?;
                    let actual: Option<f64> = r.get(3)?;
                    let overruns: i64 = r.get(4)?;
                    let last_finished: Option<i64> = r.get(5)?;
                    Ok((
                        serde_json::json!({
                            "name": n,
                            "target_ms": target,
                            "actual_ms": actual,
                            "overrun_count": overruns,
                            "last_cycle_ts": last_finished,
                        }),
                        last_finished,
                    ))
                })?
                .filter_map(|x| x.ok())
                .collect();

            // Last poll cycle for this PLC: the newest of its groups', rather
            // than a `MAX(finished_ts) WHERE plc_id = ?` that no index can
            // answer without scanning every cycle the PLC ever ran.
            let last_poll_cycle_finished_ts: Option<i64> =
                pg_rows.iter().filter_map(|(_, ts)| *ts).max();
            let poll_groups: Vec<serde_json::Value> = pg_rows.into_iter().map(|(v, _)| v).collect();

            // Metric sets with member counts
            let mut ms_stmt = conn.prepare(
                "SELECT ms.id, ms.name, ms.enabled,
                        (SELECT COUNT(*) FROM metric_set_tag mst WHERE mst.metric_set_id = ms.id),
                        (SELECT COUNT(*) FROM metric_set_tag mst
                         JOIN tag t ON t.id = mst.tag_id
                         WHERE mst.metric_set_id = ms.id AND t.bad = 0 AND t.enabled = 1)
                 FROM metric_set ms
                 WHERE ms.plc_id = ?1
                 ORDER BY ms.id",
            )?;
            let ms_rows: Vec<(i64, String, bool, i64, i64)> = ms_stmt
                .query_map(rusqlite::params![id], |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, i64>(2)? != 0,
                        r.get::<_, i64>(3)?,
                        r.get::<_, i64>(4)?,
                    ))
                })?
                .filter_map(|x| x.ok())
                .collect();

            let mut metric_sets: Vec<serde_json::Value> = Vec::new();
            for (ms_id, ms_name, ms_enabled, tags_total, tags_healthy) in ms_rows {
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
                    let mut pg_stmt_eff = conn.prepare(
                        "SELECT DISTINCT t.poll_group_id, pg.name, pg.period_ms
                         FROM metric_set_tag mst
                         JOIN tag t ON t.id = mst.tag_id AND t.enabled = 1
                         JOIN poll_group pg ON pg.id = t.poll_group_id
                         WHERE mst.metric_set_id = ?1
                         ORDER BY t.poll_group_id",
                    )?;
                    let groups: Vec<(i64, String, i64)> = pg_stmt_eff
                        .query_map(rusqlite::params![ms_id], |row| {
                            Ok((
                                row.get::<_, i64>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, i64>(2)?,
                            ))
                        })?
                        .filter_map(|x| x.ok())
                        .collect();

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
                        let rate_hz = (1000.0 / period_ms as f64 * 10000.0).round() / 10000.0;
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
                    "tags_healthy": tags_healthy,
                    "effective_poll": effective_poll,
                }));
            }

            // Derived health, matching the field the HTTP API reports so the
            // local and remote views of the same server agree. Read from the
            // database, this can only describe what the collector last wrote:
            // "ok" here means the last cycle it stored was recent, not that a
            // poller is running now. Ask a running server (`-r`) for that.
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
                "name": name,
                "host": host,
                "enabled": enabled,
                "health": health,
                "last_ok_ts": last_ok_ts,
                "last_error": last_error,
                // Both spellings: the text view has always labelled this
                // column `conn`, so that is the name operators look for in the
                // JSON, where only `connectivity` existed.
                "conn": connectivity,
                "connectivity": connectivity,
                "last_poll_cycle_finished_ts": last_poll_cycle_finished_ts,
                "datasets": datasets,
                "poll_groups": poll_groups,
                "metric_sets": metric_sets,
            }))
        })?
        .filter_map(|r| r.ok())
        .collect();

    let bad_tags: i64 = conn
        .query_row("SELECT COUNT(*) FROM tag WHERE bad = 1", [], |row| {
            row.get(0)
        })
        .unwrap_or(0);

    let active_tags: i64 = conn
        .query_row("SELECT COUNT(*) FROM v_active_tag", [], |row| row.get(0))
        .unwrap_or(0);

    let pending_count = db::config_apply::pending_count(conn).unwrap_or(0);
    let command_pending: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM command_request WHERE state = 'pending'",
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);
    let command_running: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM command_request WHERE state = 'running'",
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);

    let status = serde_json::json!({
        "service": "s7trace",
        "version": version::status_json(conn),
        "plcs": plcs,
        "tags": {
            "active": active_tags,
            "bad": bad_tags,
        },
        "command_queue": {
            "pending": command_pending,
            "running": command_running,
        },
        "config_apply": {
            "pending": pending_count,
        },
    });

    match out_fmt {
        Format::Table => {
            let schema_version = version::schema_version(conn).unwrap_or_else(|| "unknown".into());
            println!(
                "s7trace version={} schema={} api={}",
                version::PACKAGE_VERSION,
                schema_version,
                version::API_VERSION
            );
            println!("PLCs:");
            for plc in &plcs {
                println!(
                    "  {} (host={}, enabled={}, health={}, conn={}) last_ok={:?}",
                    plc["name"],
                    plc["host"],
                    plc["enabled"],
                    plc["health"].as_str().unwrap_or("?"),
                    plc["connectivity"],
                    plc["last_ok_ts"]
                );
                if let Some(err) = plc["last_error"].as_str() {
                    if !err.is_empty() {
                        println!("    last_error: {err}");
                    }
                }
                for pg in plc["poll_groups"].as_array().unwrap_or(&Vec::new()) {
                    let act = pg["actual_ms"].as_f64().unwrap_or(-1.0);
                    let target = pg["target_ms"].as_i64().unwrap_or(0);
                    let actual = if act >= 0.0 {
                        format!("{act:.2}")
                    } else {
                        "-".into()
                    };
                    let utilization = if act > 0.0 && target > 0 {
                        let pct = (act / target as f64) * 100.0;
                        if pct > 90.0 {
                            format!(" {pct:.0}% HIGH")
                        } else if pct > 50.0 {
                            format!(" {pct:.0}% mod")
                        } else {
                            format!(" {pct:.0}% low")
                        }
                    } else {
                        String::new()
                    };
                    println!(
                        "    poll_group {}: target={}ms actual={}ms{utilization} overruns={}",
                        pg["name"], target, actual, pg["overrun_count"]
                    );
                }
                for ms in plc["metric_sets"].as_array().unwrap_or(&Vec::new()) {
                    let poll_str = match ms["effective_poll"]["mode"].as_str() {
                        Some("single") => {
                            let ms_pg_name = ms["effective_poll"]["poll_group_name"]
                                .as_str()
                                .unwrap_or("?");
                            let period = ms["effective_poll"]["period_ms"].as_i64().unwrap_or(0);
                            let hz = ms["effective_poll"]["rate_hz"].as_f64().unwrap_or(0.0);
                            format!("{ms_pg_name} {period}ms / {hz:.1}Hz")
                        }
                        Some("mixed") => "mixed".to_string(),
                        Some("none") => "none".to_string(),
                        _ => "?".to_string(),
                    };
                    println!(
                        "    metric_set {} enabled={} tags_total={} tags_healthy={} poll={}",
                        ms["name"], ms["enabled"], ms["tags_total"], ms["tags_healthy"], poll_str
                    );
                }
                for ds in plc["datasets"].as_array().unwrap_or(&Vec::new()) {
                    println!("    dataset {} started={:?}", ds["name"], ds["started_ts"]);
                }
            }
            println!("Tags: active={active_tags}, bad={bad_tags}");
            println!("Command queue pending={command_pending} running={command_running}");
            println!("Config apply pending: {pending_count}");
        }
        _ => {
            println!(
                "{}",
                serde_json::to_string_pretty(&status).unwrap_or_default()
            );
        }
    }

    // Show problems (bad tags)
    let mut pstmt = conn.prepare(
        "SELECT t.name, t.address, t.bad_class, t.bad_detail
         FROM tag t WHERE t.bad = 1 LIMIT 20",
    )?;
    let has_problems = pstmt.exists([])?;
    if has_problems {
        println!("\nProblems:");
        let problems = pstmt.query_map([], |row| {
            let name: String = row.get(0)?;
            let address: String = row.get(1)?;
            let bc: Option<String> = row.get(2)?;
            let bd: Option<String> = row.get(3)?;
            Ok((name, address, bc, bd))
        })?;
        for (name, addr, bc, bd) in problems.flatten() {
            let detail = bd.unwrap_or_default();
            println!("  bad_tag: {name} ({addr}) class={bc:?} detail=\"{detail}\"");
        }
    }

    Ok(ExitCode::Ok)
}
