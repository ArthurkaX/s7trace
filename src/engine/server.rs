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
use crate::plc::snap7::create_plc_client;
use crate::types::*;

/// Batch of samples from one poll cycle, sent through the channel.
pub type SampleBatch = PollResult;

/// Registry of PLC clients, keyed by plc_id.
type PlcRegistry = Arc<Mutex<HashMap<i64, Arc<Mutex<dyn PlcClient + Send>>>>>;

/// Registry of running poller handles, keyed by (plc_id, poll_group_id).
type PollerRegistry = Arc<Mutex<HashMap<(i64, i64), PollerHandle>>>;

// ---------------------------------------------------------------------------
// Config apply worker
// ---------------------------------------------------------------------------

fn apply_worker(db: Arc<Mutex<Connection>>, running: Arc<AtomicBool>) {
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
    snapshot: &Arc<Snapshot>,
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
    let pdu_payload = 240;
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
        client,
        snapshot.clone(),
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
                    conn.execute("DELETE FROM dataset WHERE name = ?1", rusqlite::params![dataset])?;
                    Ok((true, format!("dataset '{dataset}' deleted")))
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
            let (vi, vf) = crate::engine::storage::decode_value(&data, dt);

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
                "tag_add" => {
                    let plc_id = req["plc_id"].as_i64()
                        .ok_or_else(|| anyhow::anyhow!("missing plc_id"))?;
                    let tag_id = req["tag_id"].as_i64()
                        .ok_or_else(|| anyhow::anyhow!("missing tag_id"))?;

                    // Read tag from DB
                    let tag: TagRow = conn.query_row(
                        "SELECT id, plc_id, name, address, db_num, byte_offset, bit_offset,
                                byte_size, data_type, poll_group_id, store_mode, storage_hook,
                                hook_params_json, enabled, bad, bad_class, bad_detail
                         FROM tag WHERE id = ?1",
                        rusqlite::params![tag_id],
                        |row| {
                            let dt_str: String = row.get(8)?;
                            let sm_str: String = row.get(10)?;
                            let sh_str: String = row.get(11)?;
                            let bc_str: Option<String> = row.get(15)?;
                            Ok(TagRow {
                                id: row.get(0)?, plc_id: row.get(1)?, name: row.get(2)?,
                                address: row.get(3)?, db_num: row.get(4)?,
                                byte_offset: row.get(5)?, bit_offset: row.get(6)?,
                                byte_size: row.get(7)?,
                                data_type: dt_str.parse::<DataType>().unwrap_or(DataType::I16),
                                poll_group_id: row.get(9)?,
                                store_mode: sm_str.parse::<StoreMode>().unwrap_or(StoreMode::OnChange),
                                storage_hook: sh_str.parse::<StorageHook>().unwrap_or(StorageHook::OnChange),
                                hook_params_json: row.get(12)?,
                                enabled: row.get(13)?, bad: row.get(14)?,
                                bad_class: bc_str.map(|s| s.parse::<BadClass>().ok()).flatten(),
                                bad_detail: row.get(16)?,
                            })
                        },
                    )?;

                    // Validate by reading from PLC
                    let client_arc = {
                        let reg = plcs.lock();
                        reg.get(&plc_id).cloned()
                            .ok_or_else(|| anyhow::anyhow!("PLC client not available for {plc_id}"))?
                    };

                    let mut client = client_arc.lock();
                    let read_result = client.read_db(
                        tag.db_num as u16,
                        tag.byte_offset as usize,
                        tag.byte_size as usize,
                    );

                    match read_result {
                        Ok(data) => {
                            let (vi, vf) = crate::engine::storage::decode_value(&data, tag.data_type);
                            let ts = now_us();
                            snapshot.update(tag_id, plc_id, &tag.name, &tag.address,
                                            tag.data_type, vi, vf, ts);

                            // Mark tag as good
                            conn.execute(
                                "UPDATE tag SET bad = 0, bad_class = NULL, bad_detail = NULL,
                                 last_value_i = COALESCE(?1, last_value_i),
                                 last_value_f = COALESCE(?2, last_value_f),
                                 last_ts = ?3 WHERE id = ?4",
                                rusqlite::params![vi, vf, ts, tag_id],
                            )?;

                            // Schedule config apply
                            let _ = db::config_apply::schedule(
                                conn, ApplyScope::PollGroup, Some(plc_id),
                                Some(tag.poll_group_id), None, None,
                                &format!("tag_add id={tag_id}"),
                                db::config_apply::PLAN_COOLDOWN_MS, None,
                            );

                            let val_str = if let Some(v) = vi {
                                if let Some(f) = vf {
                                    format!("value={v} float={f}")
                                } else {
                                    format!("value={v}")
                                }
                            } else if let Some(f) = vf {
                                format!("value={f}")
                            } else {
                                "value=null".into()
                            };

                            Ok((true, format!("{val_str} apply_state=pending tag_id={tag_id}")))
                        }
                        Err(e) => {
                            let bad_class = e.bad_class();
                            let detail = e.detail().to_string();
                            conn.execute(
                                "UPDATE tag SET bad = 1, bad_class = ?1, bad_detail = ?2, last_error_ts = ?3
                                 WHERE id = ?4",
                                rusqlite::params![bad_class.to_string(), detail, now_us(), tag_id],
                            )?;
                            Ok((false, format!("bad bad_class={bad_class} detail=\"{detail}\" tag_id={tag_id}")))
                        }
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

                    let tag: TagRow = conn.query_row(
                        "SELECT id, plc_id, name, address, db_num, byte_offset, bit_offset,
                                byte_size, data_type, poll_group_id, store_mode, storage_hook,
                                hook_params_json, enabled, bad, bad_class, bad_detail
                         FROM tag WHERE id = ?1", rusqlite::params![tag_id],
                        |row| {
                            let dt_str: String = row.get(8)?;
                            let sm_str: String = row.get(10)?;
                            let sh_str: String = row.get(11)?;
                            let bc_str: Option<String> = row.get(15)?;
                            Ok(TagRow {
                                id: row.get(0)?, plc_id: row.get(1)?, name: row.get(2)?,
                                address: row.get(3)?, db_num: row.get(4)?,
                                byte_offset: row.get(5)?, bit_offset: row.get(6)?,
                                byte_size: row.get(7)?,
                                data_type: dt_str.parse::<DataType>().unwrap_or(DataType::I16),
                                poll_group_id: row.get(9)?,
                                store_mode: sm_str.parse::<StoreMode>().unwrap_or(StoreMode::OnChange),
                                storage_hook: sh_str.parse::<StorageHook>().unwrap_or(StorageHook::OnChange),
                                hook_params_json: row.get(12)?,
                                enabled: row.get(13)?, bad: row.get(14)?,
                                bad_class: bc_str.map(|s| s.parse::<BadClass>().ok()).flatten(),
                                bad_detail: row.get(16)?,
                            })
                        },
                    )?;

                    let client_arc = {
                        let reg = plcs.lock();
                        reg.get(&tag.plc_id).cloned()
                            .ok_or_else(|| anyhow::anyhow!("PLC client not available"))?
                    };

                    let mut client = client_arc.lock();
                    match client.read_db(tag.db_num as u16, tag.byte_offset as usize, tag.byte_size as usize) {
                        Ok(data) => {
                            let (vi, vf) = crate::engine::storage::decode_value(&data, tag.data_type);
                            let ts = now_us();
                            snapshot.update(tag_id, tag.plc_id, &tag.name, &tag.address,
                                            tag.data_type, vi, vf, ts);
                            conn.execute(
                                "UPDATE tag SET bad = 0, bad_class = NULL, bad_detail = NULL,
                                 last_value_i = COALESCE(?1, last_value_i),
                                 last_value_f = COALESCE(?2, last_value_f),
                                 last_ts = ?3 WHERE id = ?4",
                                rusqlite::params![vi, vf, ts, tag_id],
                            )?;
                            let _ = db::config_apply::schedule(
                                conn, ApplyScope::Tag, Some(tag_id), None, None, None,
                                &format!("tag_revalidate id={tag_id}"),
                                db::config_apply::PLAN_COOLDOWN_MS, None,
                            );
                            Ok((true, format!("tag {tag_id} revalidated OK")))
                        }
                        Err(e) => {
                            conn.execute(
                                "UPDATE tag SET bad = 1, bad_class = ?1, bad_detail = ?2,
                                 last_error_ts = ?3 WHERE id = ?4",
                                rusqlite::params![e.bad_class().to_string(), e.detail(), now_us(), tag_id],
                            )?;
                            Ok((false, format!("revalidate failed: {e}")))
                        }
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
                    conn.execute(
                        "INSERT INTO dataset (plc_id, name, state, created_ts)
                         VALUES (?1, ?2, 'active', ?3)",
                        rusqlite::params![plc_id, dataset, now_us()],
                    )?;

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
                            plcs, pollers, snapshot, sample_tx, conn,
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
                &conn, &plcs, &pollers, &snapshot, &sample_tx,
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
    let dataset_id: Option<i64> = conn
        .query_row(
            "SELECT ds.id FROM dataset ds
             JOIN poll_group pg ON pg.plc_id = ds.plc_id
             WHERE ds.plc_id = ?1 AND ds.state = 'active'
             AND pg.id = ?2
             LIMIT 1",
            rusqlite::params![batch.plc_id, batch.poll_group_id],
            |row| row.get(0),
        )
        .ok();

    let ci = &batch.cycle_info;
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

    conn.execute(
        "INSERT INTO read_span
            (plc_id, poll_group_id, seq, db_num, byte_offset, byte_len, ok, dataset_id)
         VALUES (?1, ?2, ?3, 0, 0, 0, 1, ?4)",
        rusqlite::params![batch.plc_id, batch.poll_group_id, ci.seq as i64, dataset_id],
    )?;

    if !batch.samples.is_empty() {
        let mut stmt = conn.prepare(
            "INSERT INTO sample
                (ts, tag_id, vi, vf, span_id, dataset_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )?;
        for s in &batch.samples {
            stmt.execute(rusqlite::params![
                s.ts, s.tag_id, s.vi, s.vf, s.span_id, dataset_id,
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
}

/// The main s7trace server. Orchestrates all workers and pollers.
pub struct Server {
    db: Arc<Mutex<Connection>>,
    snapshot: Arc<Snapshot>,
    running: Arc<AtomicBool>,
    plcs: PlcRegistry,
    pollers: PollerRegistry,
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
        })
    }

    /// Initialize PLC clients for all configured PLCs.
    fn init_plc_clients(&self) -> Result<(), anyhow::Error> {
        let conn = self.db.lock();
        let mut stmt = conn.prepare("SELECT id, name, host FROM plc ORDER BY id")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;

        let mut reg = self.plcs.lock();
        for row in rows {
            let (plc_id, plc_name, host): (i64, String, String) = row?;
            info!("initializing PLC '{}' (id={}) at {}", plc_name, plc_id, host);
            let client = create_plc_client(&plc_name, &host);
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
        let sample_tx_cmd = sample_tx.clone();
        std::thread::Builder::new()
            .name("cmd-worker".into())
            .spawn(move || {
                command_worker(db_cmd, running_cmd, snapshot_cmd, plcs_cmd, pollers_cmd, sample_tx_cmd);
            })?;

        // Start config apply worker thread
        let db_apply = self.db.clone();
        let running_apply = self.running.clone();
        std::thread::Builder::new()
            .name("apply-worker".into())
            .spawn(move || apply_worker(db_apply, running_apply))?;

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
pub fn run_server(db_path: &str) -> Result<(), anyhow::Error> {
    let mut server = Server::new(ServerConfig {
        db_path: db_path.to_string(),
    })?;
    server.run()?;
    Ok(())
}
