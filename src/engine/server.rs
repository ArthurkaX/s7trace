/// s7trace server/daemon mode.
///
/// Orchestrates poller threads, command queue worker, config apply worker,
/// and sample writer. Runs until shutdown is requested.
///
/// ## Pipeline
///
/// ```text
/// CLI --enqueue--> command_request --claim--> cmd-worker --exec--> PLC / DB / Snapshot
///                                          +---> command_result
/// Poller --send--> sample-writer --batch--> SQLite
/// ```

use log::{debug, error, info, warn};
use parking_lot::Mutex;
use rusqlite::Connection;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::time::Duration;

use crate::db;
use crate::engine::plan::build_plan;
use crate::engine::poller::{PollerHandle, PollResult, spawn_poller};
use crate::engine::snapshot::Snapshot;
use crate::plc::client::PlcClient;
use crate::plc::create_plc_client;
use crate::types::*;
use crate::api;

/// Batch of samples from one poll cycle, sent through the channel.
pub type SampleBatch = PollResult;

/// Registry of PLC clients, keyed by plc_id.
type PlcRegistry = Arc<Mutex<HashMap<i64, Arc<Mutex<dyn PlcClient + Send>>>>>;

/// Registry of running poller handles, keyed by (plc_id, poll_group_id).
type PollerRegistry = Arc<Mutex<HashMap<(i64, i64), PollerHandle>>>;

/// Per-PLC PDU length cache. Populated on first successful connect.
type PduCache = Arc<Mutex<HashMap<i64, usize>>>;

// ---------------------------------------------------------------------------
// Config apply worker
// ---------------------------------------------------------------------------

fn apply_worker(
    db: Arc<Mutex<Connection>>,
    running: Arc<AtomicBool>,
    plcs: PlcRegistry,
    pollers: PollerRegistry,
    pdu_cache: PduCache,
    sample_tx: Sender<SampleBatch>,
) {
    info!("apply-worker started");

    while running.load(Ordering::Relaxed) {
        let due = {
            let conn = db.lock();
            match db::config_apply::claim_due(&conn) {
                Ok(Some(row)) => Some(row),
                Ok(None) => None,
                Err(e) => {
                    error!("apply-worker: claim_due error: {e}");
                    None
                }
            }
        };

        if let Some(row) = due {
            info!("apply-worker: executing apply id={} reason={:?}", row.id, row.reason);
            let conn = db.lock();
            use crate::types::ApplyScope;
            let mut affected: Vec<(i64, i64)> = Vec::new(); // (plc_id, pg_id)
            match row.scope {
                ApplyScope::Tag => {
                    if let Some(tag_id) = row.tag_id {
                        let _ = conn.execute(
                            "UPDATE tag SET applied_version = desired_version, apply_state = 'applied'
                             WHERE id = ?1",
                            rusqlite::params![tag_id],
                        );
                        // Determine affected (plc, pg) for a plan rebuild
                        let plc_pg: Option<(i64, i64)> = conn.query_row(
                            "SELECT plc_id, COALESCE(poll_group_id, 0) FROM tag WHERE id = ?1",
                            rusqlite::params![tag_id],
                            |r| Ok((r.get(0)?, r.get(1)?)),
                        ).ok();
                        if let Some(pp) = plc_pg {
                            affected.push(pp);
                        }
                    }
                }
                ApplyScope::Plc => {
                    if let Some(plc_id) = row.plc_id {
                        let _ = conn.execute(
                            "UPDATE tag SET applied_version = desired_version, apply_state = 'applied'
                             WHERE plc_id = ?1",
                            rusqlite::params![plc_id],
                        );
                        // Rebuild all poll groups for this PLC
                        let pgs: Vec<i64> = conn.prepare(
                            "SELECT DISTINCT COALESCE(poll_group_id, 0)
                             FROM tag WHERE plc_id = ?1 AND enabled = 1 AND bad = 0"
                        ).ok().and_then(|mut s| {
                            s.query_map(rusqlite::params![plc_id], |r| r.get::<_, i64>(0))
                                .ok().map(|it| it.filter_map(|x| x.ok()).collect())
                        }).unwrap_or_default();
                        for pg in pgs {
                            affected.push((plc_id, pg));
                        }
                    }
                }
                ApplyScope::PollGroup => {
                    if let (Some(plc_id), Some(pg_id)) = (row.plc_id, row.poll_group_id) {
                        let _ = conn.execute(
                            "UPDATE tag SET applied_version = desired_version, apply_state = 'applied'
                             WHERE plc_id = ?1 AND poll_group_id = ?2",
                            rusqlite::params![plc_id, pg_id],
                        );
                        affected.push((plc_id, pg_id));
                    }
                }
                ApplyScope::MetricSet => {
                    if let Some(plc_id) = row.plc_id {
                        let _ = conn.execute(
                            "UPDATE tag SET apply_state = 'applied'
                             WHERE plc_id = ?1
                               AND id IN (
                                 SELECT mst.tag_id FROM metric_set_tag mst
                                 WHERE mst.metric_set_id = ?2
                               )",
                            rusqlite::params![plc_id, row.metric_set_id],
                        );
                        // Rebuild all poll groups for this PLC that are
                        // affected by this metric set.
                        let pgs: Vec<i64> = conn.prepare(
                            "SELECT DISTINCT COALESCE(t.poll_group_id, 0)
                             FROM tag t
                             JOIN metric_set_tag mst ON mst.tag_id = t.id
                             WHERE mst.metric_set_id = ?1 AND t.plc_id = ?2
                               AND t.enabled = 1 AND t.bad = 0"
                        ).ok().and_then(|mut s| {
                            s.query_map(rusqlite::params![row.metric_set_id, plc_id], |r| r.get::<_, i64>(0))
                                .ok().map(|it| it.filter_map(|x| x.ok()).collect())
                        }).unwrap_or_default();
                        for pg in pgs {
                            affected.push((plc_id, pg));
                        }
                    }
                }
                _ => {}
            }

            // Rebuild running pollers for affected (plc, pg) pairs.
            // Only do this if there is an active dataset (collection running).
            for (plc_id, pg_id) in &affected {
                let has_active_dataset: bool = conn.query_row(
                    "SELECT COUNT(*) FROM dataset WHERE plc_id = ?1 AND state = 'active'",
                    rusqlite::params![plc_id],
                    |r| r.get::<_, i64>(0),
                ).unwrap_or(0) > 0;
                if !has_active_dataset {
                    continue;
                }

                // Stop the existing poller if running
                {
                    let mut reg = pollers.lock();
                    if let Some(handle) = reg.remove(&(*plc_id, *pg_id)) {
                        handle.stop();
                        info!("apply-worker: stopped poller for PLC {} group {}", plc_id, pg_id);
                    }
                }

                // Look up plc name
                let plc_name: String = match conn.query_row(
                    "SELECT name FROM plc WHERE id = ?1",
                    rusqlite::params![plc_id],
                    |r| r.get(0),
                ) {
                    Ok(n) => n,
                    Err(_) => continue,
                };

                // Spawn a fresh poller with the updated plan
                let period_ms: u64 = conn.query_row(
                    "SELECT period_ms FROM poll_group WHERE id = ?1",
                    rusqlite::params![pg_id],
                    |r| r.get(0),
                ).unwrap_or(1000);

                if let Err(e) = spawn_one_poller(
                    &plcs, &pollers, &pdu_cache, &sample_tx, &conn,
                    *plc_id, &plc_name, *pg_id, period_ms,
                ) {
                    error!("apply-worker: spawn_one_poller failed for PLC {} group {}: {}", plc_id, pg_id, e);
                }
            }

            if let Err(e) = db::config_apply::finish_apply(&conn, row.id) {
                error!("apply-worker: finish_apply id={} failed: {e}", row.id);
            }
        } else {
            std::thread::sleep(Duration::from_millis(500));
        }
    }

    info!("apply-worker stopped");
}

// ---------------------------------------------------------------------------
// Command worker
// ---------------------------------------------------------------------------

/// Spawn a poller for one (plc, poll_group) pair.
fn spawn_one_poller(
    plcs: &PlcRegistry,
    pollers: &PollerRegistry,
    pdu_cache: &PduCache,
    sample_tx: &Sender<SampleBatch>,
    conn: &Connection,
    plc_id: i64,
    plc_name: &str,
    pg_id: i64,
    period_ms: u64,
) -> Result<(), anyhow::Error> {
    // Build plan
    let tags = load_active_tags(conn, plc_id, pg_id)?;
    if tags.is_empty() {
        debug!("PLC {} group {}: no active tags, skipping", plc_name, pg_id);
        return Ok(());
    }
    let pdu_payload = pdu_cache.lock().get(&plc_id).copied().unwrap_or(240);
    let max_gap = 8;
    let plan = build_plan(plc_id, pg_id, &tags, pdu_payload, max_gap);

    // Get client
    let client = {
        let reg = plcs.lock();
        reg.get(&plc_id).cloned()
        .ok_or_else(|| anyhow::anyhow!("PLC {plc_id} not in registry"))?
    };

    // Spawn
    let handle = spawn_poller(
        plc_id,
        plc_name.to_string(),
        period_ms,
        plan,
        tags,
        client,
        sample_tx.clone(),
    );

    // Store
    pollers.lock().insert((plc_id, pg_id), handle);
    info!("poller started for PLC {} group {} (period={}ms)", plc_name, pg_id, period_ms);
    Ok(())
}

/// Load enabled, non-bad tags for a PLC + poll group from enabled metric sets.
fn load_active_tags(conn: &Connection, plc_id: i64, pg_id: i64) -> Result<Vec<TagRow>, anyhow::Error> {
    let mut stmt = conn.prepare(
        "SELECT DISTINCT t.id, t.plc_id, t.name, t.address,
                t.db_num, t.byte_offset, t.bit_offset,
                t.byte_size, t.data_type, t.poll_group_id,
                t.store_mode, t.storage_hook, t.hook_params_json,
                t.enabled, t.bad, t.bad_class, t.bad_detail
         FROM tag t
         JOIN metric_set_tag mst ON mst.tag_id = t.id
         JOIN metric_set ms ON ms.id = mst.metric_set_id
         WHERE t.plc_id = ?1
           AND t.poll_group_id = ?2
           AND t.enabled = 1
           AND t.bad = 0
           AND ms.enabled = 1
         ORDER BY t.db_num, t.byte_offset",
    )?;

    let tags: Vec<TagRow> = stmt.query_map(rusqlite::params![plc_id, pg_id], |row| {
        let data_type_str: String = row.get(8)?;
        let store_mode_str: String = row.get(10)?;
        let storage_hook_str: String = row.get(11)?;
        let bad_class_str: Option<String> = row.get(15)?;

        Ok(TagRow {
            id: row.get(0)?,
            plc_id: row.get(1)?,
            name: row.get(2)?,
            address: row.get(3)?,
            db_num: row.get(4)?,
            byte_offset: row.get(5)?,
            bit_offset: row.get(6)?,
            byte_size: row.get(7)?,
            data_type: data_type_str.parse::<DataType>()
                .map_err(|e| rusqlite::Error::FromSqlConversionFailure(
                    8, rusqlite::types::Type::Text, std::io::Error::new(std::io::ErrorKind::InvalidData, e).into(),
                ))?,
            poll_group_id: row.get(9)?,
            store_mode: store_mode_str.parse::<StoreMode>()
                .map_err(|e| rusqlite::Error::FromSqlConversionFailure(
                    10, rusqlite::types::Type::Text, std::io::Error::new(std::io::ErrorKind::InvalidData, e).into(),
                ))?,
            storage_hook: storage_hook_str.parse::<StorageHook>()
                .map_err(|e| rusqlite::Error::FromSqlConversionFailure(
                    11, rusqlite::types::Type::Text, std::io::Error::new(std::io::ErrorKind::InvalidData, e).into(),
                ))?,
            hook_params_json: row.get(12)?,
            enabled: row.get(13)?,
            bad: row.get(14)?,
            bad_class: bad_class_str.map(|s| s.parse::<BadClass>().ok()).flatten(),
            bad_detail: row.get(16)?,
        })
    })?.filter_map(|r| r.ok()).collect();

    Ok(tags)
}

/// Execute a PLC read command.
fn execute_command(
    conn: &Connection,
    plcs: &PlcRegistry,
    pollers: &PollerRegistry,
    pdu_cache: &PduCache,
    snapshot: &Arc<Snapshot>,
    sample_tx: &Sender<SampleBatch>,
    cmd_id: i64,
    kind: CommandKind,
    request_json: &str,
) -> Result<(bool, String), anyhow::Error> {
    use CommandKind::*;

    let req: serde_json::Value = serde_json::from_str(request_json)?;

    match kind {
        Admin => {
            let action = req["action"].as_str().unwrap_or("");
            match action {
                "delete_dataset" => {
                    let dataset = req["dataset"].as_str().unwrap_or("");
                    // Refuse if dataset is still active (retention.md D009)
                    let state: Option<String> = conn.query_row(
                        "SELECT state FROM dataset WHERE name = ?1",
                        rusqlite::params![dataset],
                        |r| r.get(0),
                    ).ok();
                    if let Some(s) = state {
                        if s == "active" {
                            return Ok((false, format!("dataset '{dataset}' is active; stop it first")));
                        }
                    } else {
                        return Ok((false, format!("dataset '{dataset}' not found")));
                    }
                    // ON DELETE CASCADE handles dependent rows.
                    let n = conn.execute("DELETE FROM dataset WHERE name = ?1", rusqlite::params![dataset])?;
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
                    let active: i64 = conn.query_row(
                        "SELECT COUNT(*) FROM dataset WHERE plc_id = ?1 AND state = 'active'",
                        rusqlite::params![plc_id], |r| r.get(0),
                    ).unwrap_or(0);
                    if active > 0 {
                        return Ok((false, format!("PLC has {active} active dataset(s); stop collection first")));
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
                        if n == 0 { break; }
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
                        if n == 0 { break; }
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
                        if n == 0 { break; }
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
                        if n == 0 { break; }
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
                        if n == 0 { break; }
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
                        if n == 0 { break; }
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
                        if n == 0 { break; }
                        total_deleted += n as i64;
                    }

                    Ok((true, format!("history_clean plc={plc_id} cutoff={cutoff} deleted={total_deleted}")))
                }
                other => Ok((false, format!("unknown admin action '{other}'")))
            }
        }
        PlcRead => {
            let plc_name = req["plc"].as_str().unwrap_or("");
            let address = req["address"].as_str().unwrap_or("");
            let data_type = req["data_type"].as_str().unwrap_or("");

            let dt: DataType = data_type.parse()
                .map_err(|e: String| anyhow::anyhow!("invalid data type: {e}"))?;
            let addr = S7Address::parse(address)
                .map_err(|e| anyhow::anyhow!("invalid address: {e}"))?;

            // Get plc_id from name
            let plc_id: i64 = conn.query_row(
                "SELECT id FROM plc WHERE name = ?1",
                rusqlite::params![plc_name],
                |row| row.get(0),
            ).map_err(|_| anyhow::anyhow!("PLC '{plc_name}' not found"))?;

            // Get client from registry
            let client_arc = {
                let reg = plcs.lock();
                reg.get(&plc_id).cloned()
                    .ok_or_else(|| anyhow::anyhow!("PLC {plc_id} client not registered"))?
            };

            let mut client = client_arc.lock();
            let data = client.read_db(addr.db_num, addr.byte_offset, dt.byte_size())?;

            // Decode
            let (vi, vf) = crate::engine::storage::decode_value(&data, dt, None);

            // Update snapshot (negative tag_id = ephemeral one-off read)
            snapshot.update(
                -cmd_id, plc_id, "", address, dt, vi, vf, now_us(),
            );

            let json_result = serde_json::json!({
                "command_id": cmd_id,
                "ok": true,
                "value_i": vi,
                "value_f": vf,
                "ts_us": now_us(),
            });

            // Save raw probe if requested
            if req.get("save_probe").and_then(|v| v.as_bool()).unwrap_or(false) {
                let ds_id: Option<i64> = conn.query_row(
                    "SELECT id FROM dataset WHERE plc_id = ?1 AND state = 'active' LIMIT 1",
                    rusqlite::params![plc_id], |row| row.get(0),
                ).ok();
                conn.execute(
                    "INSERT INTO raw_probe (plc_id, db_num, byte_offset, byte_len, data, dataset_id)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    rusqlite::params![
                        plc_id, addr.db_num, addr.byte_offset, dt.byte_size() as i64,
                        data, ds_id,
                    ],
                )?;
            }

            Ok((true, json_result.to_string()))
        }
        Control => {
            let action = req["action"].as_str().unwrap_or("");
            match action {
                "tag_add_full" => {
                    // Full remote tag add: PLC + name + address + dt + ... — server
                    // does INSERT into tag + metric_set_tag, validates via PLC
                    // client, schedules apply. Returns tag_id, ok, value.
                    let plc_name = req["plc"].as_str()
                        .ok_or_else(|| anyhow::anyhow!("missing plc"))?;
                    let name = req["name"].as_str()
                        .ok_or_else(|| anyhow::anyhow!("missing name"))?;
                    let address_str = req["address"].as_str()
                        .ok_or_else(|| anyhow::anyhow!("missing address"))?;
                    let data_type_str = req["data_type"].as_str()
                        .ok_or_else(|| anyhow::anyhow!("missing data_type"))?;
                    let poll_group = req["poll_group"].as_str().unwrap_or("slow");
                    let store_mode = req["store_mode"].as_str().unwrap_or("on_change");
                    let storage_hook = req["storage_hook"].as_str().unwrap_or("on_change");
                    let value_role = req["value_role"].as_str().unwrap_or("state");
                    let metric_set = req["metric_set"].as_str();

                    let dt: DataType = data_type_str.parse()
                        .map_err(|e: String| anyhow::anyhow!("invalid data_type: {e}"))?;
                    let _sm: StoreMode = store_mode.parse()
                        .map_err(|_e: String| anyhow::anyhow!("invalid store_mode: {store_mode}"))?;
                    let _sh: StorageHook = storage_hook.parse()
                        .map_err(|_e: String| anyhow::anyhow!("invalid storage_hook: {storage_hook}"))?;
                    let addr = S7Address::parse(address_str)
                        .map_err(|e| anyhow::anyhow!("invalid address: {e}"))?;

                    let inserted = db::validation::insert_tag(
                        conn, plc_name, name, &addr, dt, poll_group, store_mode,
                        storage_hook, value_role, metric_set,
                    )?;
                    let tag_id = inserted.tag_id;

                    // Validation
                    let client_arc = {
                        let reg = plcs.lock();
                        reg.get(&inserted.plc_id).cloned()
                            .ok_or_else(|| anyhow::anyhow!("PLC client not available for {}", inserted.plc_id))?
                    };
                    let result = db::validation::validate_tag(conn, &client_arc, tag_id)?;

                    if result.ok {
                        snapshot.update(
                            tag_id, inserted.plc_id, name, address_str, dt,
                            result.value_i, result.value_f, result.ts_us,
                        );
                    }

                    let _ = db::config_apply::schedule(
                        conn, ApplyScope::Tag, Some(inserted.plc_id), None, None, Some(tag_id),
                        &format!("tag_add_full id={tag_id}"),
                        db::config_apply::PLAN_COOLDOWN_MS, None,
                    );

                    let val_str = if let Some(v) = result.value_i {
                        if let Some(f) = result.value_f {
                            format!("value={v} float={f}")
                        } else {
                            format!("value={v}")
                        }
                    } else if let Some(f) = result.value_f {
                        format!("value={f}")
                    } else {
                        "value=null".into()
                    };

                    if result.ok {
                        Ok((true, format!("{val_str} apply_state=pending tag_id={tag_id}")))
                    } else {
                        let bc = result.bad_class.as_ref().map(|b| b.to_string()).unwrap_or_default();
                        let det = result.detail.as_deref().unwrap_or("");
                        Ok((false, format!("bad bad_class={bc} detail=\"{det}\" tag_id={tag_id}")))
                    }
                }
                "tag_add" => {
                    let plc_id = req["plc_id"].as_i64()
                        .ok_or_else(|| anyhow::anyhow!("missing plc_id"))?;
                    let tag_id = req["tag_id"].as_i64()
                        .ok_or_else(|| anyhow::anyhow!("missing tag_id"))?;

                    // Read tag metadata (only name+address+dt needed for snapshot)
                    let (name, address, data_type_str): (String, String, String) = conn.query_row(
                        "SELECT name, address, data_type FROM tag WHERE id = ?1",
                        rusqlite::params![tag_id],
                        |row| Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        )),
                    )?;
                    let dt: DataType = data_type_str.parse().unwrap_or(DataType::I16);

                    let client_arc = {
                        let reg = plcs.lock();
                        reg.get(&plc_id).cloned()
                            .ok_or_else(|| anyhow::anyhow!("PLC client not available for {plc_id}"))?
                    };

                    let result = db::validation::validate_tag(conn, &client_arc, tag_id)?;

                    if result.ok {
                        snapshot.update(
                            tag_id, plc_id, &name, &address, dt,
                            result.value_i, result.value_f, result.ts_us,
                        );
                    }

                    // Schedule config apply
                    let _ = db::config_apply::schedule(
                        conn, ApplyScope::Tag, Some(plc_id), None, None, Some(tag_id),
                        &format!("tag_add id={tag_id}"),
                        db::config_apply::PLAN_COOLDOWN_MS, None,
                    );

                    if result.ok {
                        let val_str = if let Some(v) = result.value_i {
                            if let Some(f) = result.value_f {
                                format!("value={v} float={f}")
                            } else {
                                format!("value={v}")
                            }
                        } else if let Some(f) = result.value_f {
                            format!("value={f}")
                        } else {
                            "value=null".into()
                        };
                        Ok((true, format!("{val_str} apply_state=pending tag_id={tag_id}")))
                    } else {
                        let bc = result.bad_class.as_ref().map(|b| b.to_string()).unwrap_or_default();
                        let det = result.detail.as_deref().unwrap_or("");
                        Ok((false, format!("bad bad_class={bc} detail=\"{det}\" tag_id={tag_id}")))
                    }
                }

                "tag_disable" => {
                    if let Some(tag_id) = req["tag_id"].as_i64() {
                        conn.execute(
                            "UPDATE tag SET enabled = 0 WHERE id = ?1",
                            rusqlite::params![tag_id],
                        )?;
                        let reason = format!("tag_disable id={tag_id}");
                        let _ = db::config_apply::schedule(
                            conn, ApplyScope::Tag, Some(tag_id), None, None, None,
                            &reason, db::config_apply::PLAN_COOLDOWN_MS, None,
                        );
                        Ok((true, format!("tag {tag_id} disabled, apply scheduled")))
                    } else {
                        Ok((false, "missing tag_id".into()))
                    }
                }

                "tag_revalidate" => {
                    let tag_id = req["tag_id"].as_i64()
                        .ok_or_else(|| anyhow::anyhow!("missing tag_id"))?;

                    let (plc_id, name, address, data_type_str): (i64, String, String, String) =
                        conn.query_row(
                            "SELECT plc_id, name, address, data_type FROM tag WHERE id = ?1",
                            rusqlite::params![tag_id],
                            |row| Ok((
                                row.get::<_, i64>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, String>(3)?,
                            )),
                        )?;
                    let dt: DataType = data_type_str.parse().unwrap_or(DataType::I16);

                    let client_arc = {
                        let reg = plcs.lock();
                        reg.get(&plc_id).cloned()
                            .ok_or_else(|| anyhow::anyhow!("PLC client not available"))?
                    };

                    let result = db::validation::validate_tag(conn, &client_arc, tag_id)?;

                    if result.ok {
                        snapshot.update(
                            tag_id, plc_id, &name, &address, dt,
                            result.value_i, result.value_f, result.ts_us,
                        );
                    }

                    let _ = db::config_apply::schedule(
                        conn, ApplyScope::Tag, Some(plc_id), None, None, Some(tag_id),
                        &format!("tag_revalidate id={tag_id}"),
                        db::config_apply::PLAN_COOLDOWN_MS, None,
                    );

                    if result.ok {
                        Ok((true, format!("tag {tag_id} revalidated OK value={:?}", result.value_i)))
                    } else {
                        let det = result.detail.as_deref().unwrap_or("");
                        Ok((false, format!("revalidate failed: {det}")))
                    }
                }

                "collection_start" => {
                    let plc_id = req["plc_id"].as_i64()
                        .ok_or_else(|| anyhow::anyhow!("missing plc_id"))?;
                    let plc_name = req["plc"].as_str().unwrap_or("?");
                    let dataset = req["dataset"].as_str()
                        .ok_or_else(|| anyhow::anyhow!("missing dataset"))?;

                    // Check no active dataset
                    let active: i64 = conn.query_row(
                        "SELECT COUNT(*) FROM dataset WHERE plc_id = ?1 AND state = 'active'",
                        rusqlite::params![plc_id], |row| row.get(0),
                    )?;
                    if active > 0 {
                        return Ok((false, format!("PLC {plc_name} already has an active dataset")));
                    }

                    // Create dataset
                    let now = now_us();
                    let n = conn.execute(
                        "INSERT INTO dataset (plc_id, name, state, created_ts, updated_ts)
                         VALUES (?1, ?2, 'active', ?3, ?3)",
                        rusqlite::params![plc_id, dataset, now],
                    )?;
                    info!(
                        "collection_start: inserted dataset id=? plc_id={} name='{}' rows_affected={}",
                        plc_id, dataset, n
                    );

                    // Find poll groups for this PLC
                    let pg_rows: Vec<(i64, String, u64)> = {
                        let mut stmt = conn.prepare(
                            "SELECT DISTINCT pg.id, pg.name, pg.period_ms
                             FROM poll_group pg
                             JOIN tag t ON t.poll_group_id = pg.id
                             JOIN metric_set_tag mst ON mst.tag_id = t.id
                             JOIN metric_set ms ON ms.id = mst.metric_set_id
                             WHERE t.plc_id = ?1
                               AND t.enabled = 1
                               AND t.bad = 0
                               AND ms.enabled = 1
                             ORDER BY pg.id",
                        )?;
                        let rows = stmt.query_map(rusqlite::params![plc_id], |row| {
                            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                        })?;
                        rows.filter_map(|r| r.ok()).collect()
                    };

                    info!("found {} active poll groups for PLC {}", pg_rows.len(), plc_name);

                    if pg_rows.is_empty() {
                        warn!("collection_start {dataset}: no active poll groups for PLC {plc_name}");
                    }

                    for (pg_id, _pg_name, period_ms) in &pg_rows {
                        spawn_one_poller(
                            plcs, pollers, pdu_cache, sample_tx, conn,
                            plc_id, plc_name, *pg_id, *period_ms,
                        )?;
                    }

                    Ok((true, format!("collection started dataset={dataset} plc={plc_name}")))
                }

                "collection_stop" => {
                    let plc_id = req["plc_id"].as_i64()
                        .ok_or_else(|| anyhow::anyhow!("missing plc_id"))?;
                    let plc_name = req["plc"].as_str().unwrap_or("?");

                    // Stop all pollers for this PLC
                    {
                        let mut reg = pollers.lock();
                        let keys: Vec<(i64, i64)> = reg.keys()
                            .filter(|(pid, _)| *pid == plc_id)
                            .copied()
                            .collect();
                        for key in &keys {
                            if let Some(handle) = reg.remove(key) {
                                handle.stop();
                                info!("poller stopped for PLC {} group {}", plc_name, key.1);
                            }
                        }
                    }

                    // Mark dataset as stopped
                    conn.execute(
                        "UPDATE dataset SET state = 'stopped' WHERE plc_id = ?1 AND state = 'active'",
                        rusqlite::params![plc_id],
                    )?;

                    Ok((true, format!("collection stopped for PLC {plc_name}")))
                }

                other => Ok((false, format!("unknown control action '{other}'")))
            }
        }

        Query => {
            Ok((false, "server cannot execute SQL queries, use CLI directly".into()))
        }
    }
}

/// Runs in a dedicated thread: claims pending commands, dispatches to
/// `execute_command`, writes results.
fn command_worker(
    db: Arc<Mutex<Connection>>,
    running: Arc<AtomicBool>,
    snapshot: Arc<Snapshot>,
    plcs: PlcRegistry,
    pollers: PollerRegistry,
    pdu_cache: PduCache,
    sample_tx: Sender<SampleBatch>,
) {
    info!("cmd-worker started");

    while running.load(Ordering::Relaxed) {
        let claimed = {
            let conn = db.lock();
            db::command_queue::claim_next(&conn)
        };

        let cmd = match claimed {
            Ok(Some(cmd)) => cmd,
            Ok(None) => {
                std::thread::sleep(Duration::from_millis(200));
                continue;
            }
            Err(e) => {
                error!("cmd-worker: claim error: {e}");
                std::thread::sleep(Duration::from_millis(1000));
                continue;
            }
        };

        debug!("cmd-worker: claimed command id={} kind={:?}", cmd.id, cmd.kind);

        let result = {
            let conn = db.lock();
            execute_command(
                &conn, &plcs, &pollers, &pdu_cache, &snapshot, &sample_tx,
                cmd.id, cmd.kind, &cmd.request_json,
            )
        };

        match result {
            Ok((ok, msg)) => {
                if ok {
                    let conn = db.lock();
                    if let Err(e) = db::command_queue::finish_ok(&conn, cmd.id, &msg) {
                        error!("cmd-worker: finish_ok id={} failed: {e}", cmd.id);
                    }
                } else {
                    let conn = db.lock();
                    if let Err(e) = db::command_queue::finish_bad(&conn, cmd.id, &msg) {
                        error!("cmd-worker: finish_bad id={} failed: {e}", cmd.id);
                    }
                }
            }
            Err(e) => {
                error!("cmd-worker: command id={} execution error: {e}", cmd.id);
                let conn = db.lock();
                if let Err(e2) = db::command_queue::finish_bad(&conn, cmd.id, &e.to_string()) {
                    error!("cmd-worker: finish_bad id={} failed: {e2}", cmd.id);
                }
            }
        }
    }

    info!("cmd-worker stopped");
}

// ---------------------------------------------------------------------------
// Sample writer
// ---------------------------------------------------------------------------

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

    conn.execute(
        "INSERT INTO poll_cycle
            (plc_id, poll_group_id, seq, started_ts, finished_ts,
             target_ms, actual_ms, ok, error, dataset_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        rusqlite::params![
            ci.plc_id, ci.poll_group_id, ci.seq as i64, ci.started_ts, ci.finished_ts,
            ci.target_ms as i64, ci.actual_ms, ci.ok as i32, ci.error, dataset_id,
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
                ci.plc_id, ci.poll_group_id,
                s.db_num as i64, s.byte_offset as i64, s.byte_len as i64,
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
                dataset_id, r.tag_id, ci.plc_id, ci.poll_group_id,
                p.window_start_ts, p.window_end_ts, p.sample_count,
                p.first_vi, p.last_vi, p.min_vi, p.max_vi, p.delta_vi,
                p.first_vf, p.last_vf, p.min_vf, p.max_vf, p.delta_vf,
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
                dataset_id, ci.plc_id, cycle_id,
                iv.end_ts, iv.tag_id, iv.state, details.to_string(),
            ])?;
        }
    }

    // Mark localized bad tags (architecture.md: tag.bad = 1 with class+detail)
    if !batch.bad_tags.is_empty() {
        for b in &batch.bad_tags {
            conn.execute(
                "UPDATE tag SET bad = 1, bad_class = ?1, bad_detail = ?2, last_error_ts = ?3
                 WHERE id = ?4",
                rusqlite::params![b.bad_class.to_string(), b.detail, ci.finished_ts, b.tag_id],
            )?;
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
                dataset_id, ci.plc_id, cycle_id,
                em.ts, em.kind, em.severity, em.tag_id, em.value_i, em.details_json,
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

fn sample_writer(
    db: Arc<Mutex<Connection>>,
    rx: Receiver<SampleBatch>,
    running: Arc<AtomicBool>,
) {
    info!("sample-writer started");

    while running.load(Ordering::Relaxed) {
        match rx.recv_timeout(Duration::from_millis(500)) {
            Ok(batch) => {
                let conn = db.lock();
                if let Err(e) = write_samples(&conn, &batch) {
                    error!("sample-writer: write error: {e}");
                }
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
    }

    info!("sample-writer stopped");
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

/// Server configuration.
pub struct ServerConfig {
    pub db_path: String,
    /// If set, start the HTTP API on this bind address (e.g. "127.0.0.1:8080").
    pub http_bind: Option<String>,
    /// If true, open the platform-native control window (Windows only).
    pub control_window: bool,
}

/// The main s7trace server. Orchestrates all workers and pollers.
pub struct Server {
    db: Arc<Mutex<Connection>>,
    snapshot: Arc<Snapshot>,
    running: Arc<AtomicBool>,
    plcs: PlcRegistry,
    pollers: PollerRegistry,
    pdu_cache: PduCache,
    http_bind: Option<String>,
    control_window: bool,
}

impl Server {
    /// Create a new server instance.
    pub fn new(config: ServerConfig) -> Result<Self, anyhow::Error> {
        let conn = db::schema::open(&config.db_path)
            .map_err(|e| anyhow::anyhow!("opening database: {e}"))?;
        let db = Arc::new(Mutex::new(conn));
        let snapshot = Arc::new(Snapshot::new());
        let running = Arc::new(AtomicBool::new(true));

        Ok(Server {
            db,
            snapshot,
            running,
            plcs: Arc::new(Mutex::new(HashMap::new())),
            pollers: Arc::new(Mutex::new(HashMap::new())),
            pdu_cache: Arc::new(Mutex::new(HashMap::new())),
            http_bind: config.http_bind,
            control_window: config.control_window,
        })
    }

    /// Initialize PLC clients for all configured PLCs. Attempts to
    /// connect and discover the negotiated PDU length; falls back to a
    /// safe default (240) if the PLC is not reachable at startup.
    fn init_plc_clients(&self) -> Result<(), anyhow::Error> {
        let conn = self.db.lock();
        let mut stmt = conn.prepare("SELECT id, name, host, COALESCE(rack, 0), COALESCE(slot, 0) FROM plc ORDER BY id")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?))
        })?;

        let mut reg = self.plcs.lock();
        let mut pdu = self.pdu_cache.lock();
        for row in rows {
            let (plc_id, plc_name, host, rack, slot): (i64, String, String, i64, i64) = row?;
            info!("initializing PLC '{}' (id={}) at {} (rack={}, slot={})", plc_name, plc_id, host, rack, slot);
            let client = create_plc_client(&plc_name, &host, rack as u16, slot as u16);

            // Try to connect and discover PDU length. Failure is non-fatal
            // (PLC may come online later).
            {
                let mut c = client.lock();
                if c.connect().is_ok() {
                    match c.pdu_len() {
                        Ok(n) => {
                            info!("PLC {}: PDU length = {} bytes", plc_name, n);
                            pdu.insert(plc_id, n);
                        }
                        Err(e) => warn!("PLC {}: pdu_len() failed: {} (using 240)", plc_name, e),
                    }
                } else {
                    warn!("PLC {}: connect failed at startup; will retry in poller", plc_name);
                    pdu.insert(plc_id, 240);
                }
            }
            reg.insert(plc_id, client);
        }

        info!("initialized {} PLC client(s)", reg.len());
        Ok(())
    }

    /// Run the server main loop. Blocks until shutdown.
    pub fn run(&mut self) -> Result<(), anyhow::Error> {
        info!("s7trace server starting");

        // Init PLC clients
        self.init_plc_clients()?;

        // Channel for poller samples
        let (sample_tx, sample_rx) = mpsc::channel::<SampleBatch>();

        // Start sample writer thread
        let db_writer = self.db.clone();
        let running_writer = self.running.clone();
        std::thread::Builder::new()
            .name("sample-writer".into())
            .spawn(move || sample_writer(db_writer, sample_rx, running_writer))?;

        // Start command worker thread (with shared registries)
        let db_cmd = self.db.clone();
        let running_cmd = self.running.clone();
        let snapshot_cmd = self.snapshot.clone();
        let plcs_cmd = self.plcs.clone();
        let pollers_cmd = self.pollers.clone();
        let pdu_cache_cmd = self.pdu_cache.clone();
        let sample_tx_cmd = sample_tx.clone();
        std::thread::Builder::new()
            .name("cmd-worker".into())
            .spawn(move || {
                command_worker(db_cmd, running_cmd, snapshot_cmd, plcs_cmd, pollers_cmd, pdu_cache_cmd, sample_tx_cmd);
            })?;

        // Start config apply worker thread (now with registries for plan rebuild)
        let db_apply = self.db.clone();
        let running_apply = self.running.clone();
        let plcs_apply = self.plcs.clone();
        let pollers_apply = self.pollers.clone();
        let pdu_cache_apply = self.pdu_cache.clone();
        let sample_tx_apply = sample_tx.clone();
        std::thread::Builder::new()
            .name("apply-worker".into())
            .spawn(move || {
                apply_worker(db_apply, running_apply, plcs_apply, pollers_apply, pdu_cache_apply, sample_tx_apply);
            })?;

        // Optionally start HTTP API
        if let Some(bind) = self.http_bind.clone() {
            let db_http = self.db.clone();
            std::thread::Builder::new()
                .name("http-api".into())
                .spawn(move || {
                    if let Err(e) = api::serve(db_http, bind) {
                        error!("HTTP API server failed: {e}");
                    }
                })?;
        }

        // Optionally open the platform control window (Windows only).
        #[cfg(windows)]
        if self.control_window {
            let db_win = self.db.clone();
            let running_win = self.running.clone();
            std::thread::Builder::new()
                .name("control-window".into())
                .spawn(move || {
                    if let Err(e) = crate::platform::run_control_window(db_win) {
                        error!("control window exited with error: {e}");
                    }
                    // Window returning means user chose Exit.
                    running_win.store(false, Ordering::Relaxed);
                })?;
        }

        info!("all workers started, monitoring...");

        // Monitor loop
        let mut tick: u64 = 0;
        while self.running.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_millis(5000));
            tick += 1;

            if tick % 6 == 0 {
                let conn = self.db.lock();
                let tag_count: i64 = conn
                    .query_row("SELECT COUNT(*) FROM tag WHERE enabled = 1 AND bad = 0", [], |r| r.get(0))
                    .unwrap_or(0);
                let bad_count: i64 = conn
                    .query_row("SELECT COUNT(*) FROM tag WHERE bad = 1", [], |r| r.get(0))
                    .unwrap_or(0);
                let queue_count: i64 = conn
                    .query_row("SELECT COUNT(*) FROM command_request WHERE state = 'pending'", [], |r| r.get(0))
                    .unwrap_or(0);
                let active_ds: i64 = conn
                    .query_row("SELECT COUNT(*) FROM dataset WHERE state = 'active'", [], |r| r.get(0))
                    .unwrap_or(0);
                let poller_count = self.pollers.lock().len();
                drop(conn);

                info!(
                    "status: {} PLC(s), {} active tag(s), {} bad, {} pending cmd(s), {} active dataset(s), {} poller(s)",
                    self.plcs.lock().len(), tag_count, bad_count, queue_count, active_ds, poller_count,
                );
            }
        }

        // Clean shutdown
        info!("shutting down...");
        // Stop all pollers
        let handles: Vec<PollerHandle> = self.pollers.lock().drain().map(|(_, h)| h).collect();
        for h in &handles {
            h.stop();
        }
        std::thread::sleep(Duration::from_millis(2000));

        info!("s7trace server stopped");
        Ok(())
    }

    /// Request graceful shutdown.
    #[allow(dead_code)]
    pub fn shutdown(&self) {
        info!("shutdown requested");
        self.running.store(false, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// Top-level entry point
// ---------------------------------------------------------------------------

/// Run the server. Called from `main.rs`.
/// Start the server with explicit configuration (used when the CLI passes
/// extra options such as `--http-bind`).
pub fn run_server_with_config(config: ServerConfig) -> Result<(), anyhow::Error> {
    let mut server = Server::new(config)?;
    server.run()?;
    Ok(())
}
