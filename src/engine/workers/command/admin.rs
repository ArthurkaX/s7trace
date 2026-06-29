//! Admin command handler.

use rusqlite::Connection;

use crate::engine::server::PollerRegistry;

pub(super) fn execute_admin(
    conn: &Connection,
    pollers: &PollerRegistry,
    req: &serde_json::Value,
) -> Result<(bool, String), anyhow::Error> {
    let action = req["action"].as_str().unwrap_or("");
    match action {
        "delete_dataset" => {
            let dataset = req["dataset"].as_str().unwrap_or("");
            // Refuse if dataset is still active (retention.md D009)
            let state: Option<String> = conn
                .query_row(
                    "SELECT state FROM dataset WHERE name = ?1",
                    rusqlite::params![dataset],
                    |r| r.get(0),
                )
                .ok();
            if let Some(s) = state {
                if s == "active" {
                    return Ok((
                        false,
                        format!("dataset '{dataset}' is active; stop it first"),
                    ));
                }
            } else {
                return Ok((false, format!("dataset '{dataset}' not found")));
            }
            // ON DELETE CASCADE handles dependent rows.
            let n = conn.execute(
                "DELETE FROM dataset WHERE name = ?1",
                rusqlite::params![dataset],
            )?;
            Ok((true, format!("dataset '{dataset}' deleted rows={n}")))
        }
        "history_clean" => {
            let plc_id = req["plc_id"].as_i64();
            let cutoff = req["cutoff_ts"].as_i64();
            let batch_size = req["batch_size"].as_u64().unwrap_or(10_000) as i64;

            if plc_id.is_none() || cutoff.is_none() {
                return Ok((false, "missing plc_id or cutoff_ts".into()));
            }
            let plc_id = plc_id.unwrap();
            let cutoff = cutoff.unwrap();

            // Refuse if any active dataset for this PLC (retention.md)
            let active: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM dataset WHERE plc_id = ?1 AND state = 'active'",
                    rusqlite::params![plc_id],
                    |r| r.get(0),
                )
                .unwrap_or(0);
            if active > 0 {
                return Ok((
                    false,
                    format!("PLC has {active} active dataset(s); stop collection first"),
                ));
            }

            // Refuse if any poller is still running for this PLC
            {
                let reg = pollers.lock();
                let has_poller = reg.keys().any(|(pid, _)| *pid == plc_id);
                if has_poller {
                    return Ok((
                        false,
                        format!("PLC {plc_id} has running pollers; stop collection first"),
                    ));
                }
            }

            let mut total_deleted: i64 = 0;
            // 1. sample
            loop {
                let n = conn.execute(
                    "DELETE FROM sample
                     WHERE rowid IN (
                       SELECT rowid FROM sample
                       WHERE ts < ?1 AND dataset_id IN (
                         SELECT id FROM dataset WHERE plc_id = ?2
                       )
                       LIMIT ?3
                     )",
                    rusqlite::params![cutoff, plc_id, batch_size],
                )?;
                if n == 0 {
                    break;
                }
                total_deleted += n as i64;
            }
            // 2. sample_rollup
            loop {
                let n = conn.execute(
                    "DELETE FROM sample_rollup
                     WHERE rowid IN (
                       SELECT rowid FROM sample_rollup
                       WHERE window_end_ts < ?1 AND plc_id = ?2
                       LIMIT ?3
                     )",
                    rusqlite::params![cutoff, plc_id, batch_size],
                )?;
                if n == 0 {
                    break;
                }
                total_deleted += n as i64;
            }
            // 3. event_marker
            loop {
                let n = conn.execute(
                    "DELETE FROM event_marker
                     WHERE rowid IN (
                       SELECT rowid FROM event_marker
                       WHERE ts < ?1 AND plc_id = ?2
                       LIMIT ?3
                     )",
                    rusqlite::params![cutoff, plc_id, batch_size],
                )?;
                if n == 0 {
                    break;
                }
                total_deleted += n as i64;
            }
            // 4. plc_time_sample
            loop {
                let n = conn.execute(
                    "DELETE FROM plc_time_sample
                     WHERE rowid IN (
                       SELECT rowid FROM plc_time_sample
                       WHERE host_before_ts < ?1 AND plc_id = ?2
                       LIMIT ?3
                     )",
                    rusqlite::params![cutoff, plc_id, batch_size],
                )?;
                if n == 0 {
                    break;
                }
                total_deleted += n as i64;
            }
            // 5. raw_probe
            loop {
                let n = conn.execute(
                    "DELETE FROM raw_probe
                     WHERE rowid IN (
                       SELECT rowid FROM raw_probe
                       WHERE ts < ?1 AND plc_id = ?2
                       LIMIT ?3
                     )",
                    rusqlite::params![cutoff, plc_id, batch_size],
                )?;
                if n == 0 {
                    break;
                }
                total_deleted += n as i64;
            }
            // 6. orphaned poll_cycle (no remaining samples/events/time referencing them)
            loop {
                let n = conn.execute(
                    "DELETE FROM poll_cycle
                     WHERE rowid IN (
                       SELECT rowid FROM poll_cycle
                       WHERE finished_ts < ?1 AND plc_id = ?2
                         AND NOT EXISTS (SELECT 1 FROM sample s WHERE s.cycle_id = poll_cycle.id)
                         AND NOT EXISTS (SELECT 1 FROM event_marker em WHERE em.cycle_id = poll_cycle.id)
                         AND NOT EXISTS (SELECT 1 FROM plc_time_sample pts WHERE pts.cycle_id = poll_cycle.id)
                       LIMIT ?3
                     )",
                    rusqlite::params![cutoff, plc_id, batch_size],
                )?;
                if n == 0 {
                    break;
                }
                total_deleted += n as i64;
            }
            // 7. inactive read_span no longer referenced by sample
            loop {
                let n = conn.execute(
                    "DELETE FROM read_span
                     WHERE rowid IN (
                       SELECT rowid FROM read_span
                       WHERE plc_id = ?1
                         AND NOT EXISTS (SELECT 1 FROM sample s WHERE s.span_id = read_span.id)
                       LIMIT ?2
                     )",
                    rusqlite::params![plc_id, batch_size],
                )?;
                if n == 0 {
                    break;
                }
                total_deleted += n as i64;
            }

            Ok((
                true,
                format!("history_clean plc={plc_id} cutoff={cutoff} deleted={total_deleted}"),
            ))
        }
        other => Ok((false, format!("unknown admin action '{other}'"))),
    }
}
