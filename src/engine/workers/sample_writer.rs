//! Sample-writer worker: drains the poll-result channel, persists samples,
//! and updates the in-memory snapshot.

use parking_lot::Mutex;
use rusqlite::Connection;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use crate::engine::server::SampleBatch;
use crate::engine::snapshot::Snapshot;
use crate::types::*;

fn write_samples(conn: &Connection, batch: &SampleBatch) -> Result<(), anyhow::Error> {
    let ci = &batch.cycle_info;

    // Update PLC runtime status (last_ok_ts / last_error / connectivity)
    if ci.ok {
        conn.execute(
            "UPDATE plc SET last_ok_ts = ?1, last_error = NULL,
                            connectivity = 'online'
             WHERE id = ?2",
            rusqlite::params![ci.finished_ts, ci.plc_id],
        )?;
    } else if let Some(ref err) = ci.error {
        conn.execute(
            "UPDATE plc SET last_error = ?1, connectivity = 'offline'
             WHERE id = ?2",
            rusqlite::params![err, ci.plc_id],
        )?;
    }

    // Find the active dataset for this PLC. Datasets are not directly
    // linked to poll groups in the schema; the active dataset for the
    // PLC is the one to attach new cycles to. If there is no active
    // dataset, the sample-writer skips the write (pollers only produce
    // data while a collection is running).
    let dataset_id_opt: Option<i64> = conn
        .query_row(
            "SELECT id FROM dataset
             WHERE plc_id = ?1 AND state = 'active'
             ORDER BY id DESC LIMIT 1",
            rusqlite::params![ci.plc_id],
            |row| row.get(0),
        )
        .ok();
    let dataset_id = match dataset_id_opt {
        Some(id) => id,
        None => {
            debug!(
                "no active dataset for plc_id={} poll_group_id={}, skipping cycle write",
                ci.plc_id, ci.poll_group_id
            );
            return Ok(());
        }
    };

    // The poller's in-memory `seq` resets to 0 whenever it is respawned (e.g.
    // after a config-apply). poll_cycle has UNIQUE(dataset_id, poll_group_id,
    // seq), so reusing the poller seq collides across restarts within the same
    // dataset. Allocate a monotonic seq per (dataset, poll_group) at write time
    // instead — safe because a single sample-writer thread serializes inserts.
    let seq: i64 = conn
        .query_row(
            "SELECT COALESCE(MAX(seq), 0) + 1 FROM poll_cycle
             WHERE dataset_id = ?1 AND poll_group_id = ?2",
            rusqlite::params![dataset_id, ci.poll_group_id],
            |r| r.get(0),
        )
        .unwrap_or(ci.seq as i64);

    conn.execute(
        "INSERT INTO poll_cycle
            (plc_id, poll_group_id, seq, started_ts, finished_ts,
             target_ms, actual_ms, ok, error, dataset_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        rusqlite::params![
            ci.plc_id,
            ci.poll_group_id,
            seq,
            ci.started_ts,
            ci.finished_ts,
            ci.target_ms as i64,
            ci.actual_ms,
            ci.ok as i32,
            ci.error,
            dataset_id,
        ],
    )?;
    let cycle_id = conn.last_insert_rowid();

    // Insert one read_span per physical S7 read, and remember the rowid
    // for each cycle-local span id.
    let mut span_rowids: std::collections::HashMap<i64, i64> = std::collections::HashMap::new();
    for s in &batch.spans {
        // read_span has (plc_id, poll_group_id, db_num, byte_offset,
        // byte_len, active) per docs/schema.sql. `ok` is implicit in
        // the parent poll_cycle row.
        conn.execute(
            "INSERT INTO read_span
                (plc_id, poll_group_id, db_num, byte_offset, byte_len, active)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                ci.plc_id,
                ci.poll_group_id,
                s.db_num as i64,
                s.byte_offset as i64,
                s.byte_len as i64,
                1i32,
            ],
        )?;
        span_rowids.insert(s.span_id, conn.last_insert_rowid());
    }

    if !batch.samples.is_empty() {
        let mut stmt = conn.prepare(
            "INSERT INTO sample
                (ts, tag_id, vi, vf, span_id, dataset_id, cycle_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )?;
        for s in &batch.samples {
            let span_rowid = s.span_id.and_then(|sid| span_rowids.get(&sid).copied());
            stmt.execute(rusqlite::params![
                s.ts, s.tag_id, s.vi, s.vf, span_rowid, dataset_id, cycle_id,
            ])?;
        }
    }

    // Rollup rows from min_max_rollup / delta_rollup hooks
    if !batch.rollups.is_empty() {
        let mut stmt = conn.prepare(
            "INSERT INTO sample_rollup
                (dataset_id, tag_id, plc_id, poll_group_id,
                 window_start_ts, window_end_ts, sample_count,
                 first_vi, last_vi, min_vi, max_vi, delta_vi,
                 first_vf, last_vf, min_vf, max_vf, delta_vf)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
        )?;
        for r in &batch.rollups {
            let p = &r.payload;
            stmt.execute(rusqlite::params![
                dataset_id,
                r.tag_id,
                ci.plc_id,
                ci.poll_group_id,
                p.window_start_ts,
                p.window_end_ts,
                p.sample_count,
                p.first_vi,
                p.last_vi,
                p.min_vi,
                p.max_vi,
                p.delta_vi,
                p.first_vf,
                p.last_vf,
                p.min_vf,
                p.max_vf,
                p.delta_vf,
            ])?;
        }
    }

    // State intervals from state_duration hook
    if !batch.state_intervals.is_empty() {
        let mut stmt = conn.prepare(
            "INSERT INTO event_marker
                (dataset_id, plc_id, cycle_id, ts, kind, severity, tag_id, value_i, details_json)
             VALUES (?1, ?2, ?3, ?4, 'state_duration', 'info', ?5, ?6, ?7)",
        )?;
        for iv in &batch.state_intervals {
            let details = serde_json::json!({
                "start_ts": iv.start_ts,
                "end_ts": iv.end_ts,
                "duration_ms": iv.duration_ms,
            });
            stmt.execute(rusqlite::params![
                dataset_id,
                ci.plc_id,
                cycle_id,
                iv.end_ts,
                iv.tag_id,
                iv.state,
                details.to_string(),
            ])?;
        }
    }

    // Mark localized bad tags (architecture.md: tag.bad = 1 with class+detail)
    // Then schedule a config apply so the plan is rebuilt without them.
    if !batch.bad_tags.is_empty() {
        for b in &batch.bad_tags {
            conn.execute(
                "UPDATE tag SET bad = 1, bad_class = ?1, bad_detail = ?2, last_error_ts = ?3
                 WHERE id = ?4",
                rusqlite::params![b.bad_class.to_string(), b.detail, ci.finished_ts, b.tag_id],
            )?;
            // Schedule plan rebuild for this tag's PLC + poll group
            let _ = crate::db::config_apply::schedule(
                conn,
                ApplyScope::Tag,
                Some(ci.plc_id),
                None,
                None,
                Some(b.tag_id),
                &format!("bad_tag id={}", b.tag_id),
                crate::db::config_apply::PLAN_COOLDOWN_MS,
                Some(
                    &serde_json::json!({
                        "bad_class": b.bad_class.to_string(),
                        "detail": b.detail,
                    })
                    .to_string(),
                ),
            );
        }
    }

    // Derived event markers
    if !batch.event_markers.is_empty() {
        let mut stmt = conn.prepare(
            "INSERT INTO event_marker
                (dataset_id, plc_id, cycle_id, ts, kind, severity, tag_id, value_i, details_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        )?;
        for em in &batch.event_markers {
            stmt.execute(rusqlite::params![
                dataset_id,
                ci.plc_id,
                cycle_id,
                em.ts,
                em.kind,
                em.severity,
                em.tag_id,
                em.value_i,
                em.details_json,
            ])?;
        }
    }

    for s in &batch.samples {
        conn.execute(
            "UPDATE tag SET last_value_i = COALESCE(?1, last_value_i),
                            last_value_f = COALESCE(?2, last_value_f),
                            last_ts = COALESCE(?3, last_ts)
             WHERE id = ?4",
            rusqlite::params![s.vi, s.vf, s.ts, s.tag_id],
        )?;
    }

    Ok(())
}

pub(crate) fn sample_writer(
    db: Arc<Mutex<Connection>>,
    rx: Receiver<SampleBatch>,
    running: Arc<AtomicBool>,
    snapshot: Arc<Snapshot>,
) {
    info!("sample-writer started");

    // Cache tag metadata so we can update the in-memory snapshot without
    // hitting SQLite on every poll cycle. Populated lazily from the DB.
    // Map: tag_id -> (plc_id, name, address, data_type)
    use std::collections::HashMap;
    let mut tag_cache: HashMap<i64, (i64, String, String, DataType)> = HashMap::new();

    while running.load(Ordering::Relaxed) {
        match rx.recv_timeout(Duration::from_millis(500)) {
            Ok(batch) => {
                let conn = db.lock();
                if let Err(e) = write_samples(&conn, &batch) {
                    error!("sample-writer: write error: {e}");
                }
                // Update snapshot from every successful poll cycle, regardless
                // of whether the samples were stored (the snapshot is the live
                // view for status/API reads — per architecture.md D002).
                update_snapshot_from_batch(&snapshot, &conn, &mut tag_cache, &batch);
            }
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                warn!("sample-writer: channel closed, stopping");
                break;
            }
        }
    }

    while let Ok(batch) = rx.try_recv() {
        let conn = db.lock();
        if let Err(e) = write_samples(&conn, &batch) {
            error!("sample-writer: drain write error: {e}");
        }
        update_snapshot_from_batch(&snapshot, &conn, &mut tag_cache, &batch);
    }

    info!("sample-writer stopped");
}

/// Update the in-memory snapshot with tag values from a poll batch.
/// Populates the tag metadata cache lazily from the database.
fn update_snapshot_from_batch(
    snapshot: &Arc<Snapshot>,
    conn: &Connection,
    tag_cache: &mut HashMap<i64, (i64, String, String, DataType)>,
    batch: &SampleBatch,
) {
    let plc_id = batch.plc_id;
    for sample in &batch.samples {
        // Lazy-load tag metadata into cache
        if let std::collections::hash_map::Entry::Vacant(e) = tag_cache.entry(sample.tag_id) {
            if let Ok((name, address, dt_str)) = conn.query_row(
                "SELECT name, address, data_type FROM tag WHERE id = ?1",
                rusqlite::params![sample.tag_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            ) {
                if let Ok(dt) = dt_str.parse::<DataType>() {
                    e.insert((plc_id, name, address, dt));
                }
            }
        }
        if let Some((plc_id, name, address, dt)) = tag_cache.get(&sample.tag_id).cloned() {
            snapshot.update(
                sample.tag_id,
                plc_id,
                &name,
                &address,
                dt,
                sample.vi,
                sample.vf,
                sample.ts,
            );
        }
    }
}
