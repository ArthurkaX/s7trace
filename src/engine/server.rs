/// s7trace server/daemon mode.
///
/// Orchestrates poller threads, command queue worker, config apply worker,
/// and sample writer. Runs until shutdown is requested.

use log::{debug, error, info, warn};
use parking_lot::Mutex;
use rusqlite::Connection;
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

// ---------------------------------------------------------------------------
// Config apply worker
// ---------------------------------------------------------------------------

/// Runs in a dedicated thread: polls `config_apply` for due rows, executes
/// apply by rebuilding the affected polling plans.
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

/// Execute a queued command in the server context.
fn execute_command(
    conn: &Connection,
    _cmd_id: i64,
    kind: CommandKind,
    request_json: &str,
    _snapshot: &Snapshot,
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
                other => {
                    warn!("cmd-worker: unknown admin action '{other}'");
                    Ok((false, format!("unknown admin action '{other}'")))
                }
            }
        }
        Control => {
            let action = req["action"].as_str().unwrap_or("");

            match action {
                "tag_disable" => {
                    if let Some(tag_id) = req["tag_id"].as_i64() {
                        conn.execute(
                            "UPDATE tag SET enabled = 0 WHERE id = ?1",
                            rusqlite::params![tag_id],
                        )?;
                        let reason = format!("tag_disable id={tag_id}");
                        let _ = db::config_apply::schedule(
                            conn,
                            ApplyScope::Tag,
                            Some(tag_id),
                            None, None, None,
                            &reason,
                            db::config_apply::PLAN_COOLDOWN_MS,
                            None,
                        );
                        Ok((true, format!("tag {tag_id} disabled, apply scheduled")))
                    } else {
                        Ok((false, "missing tag_id".into()))
                    }
                }
                "collection_stop" => {
                    let plc_id = req["plc_id"].as_i64();
                    let dataset = req["dataset"].as_str();
                    if let (Some(plc_id), Some(dataset)) = (plc_id, dataset) {
                        conn.execute(
                            "UPDATE dataset SET state = 'stopped' WHERE plc_id = ?1 AND name = ?2",
                            rusqlite::params![plc_id, dataset],
                        )?;
                        Ok((true, format!("collection '{dataset}' stopped")))
                    } else {
                        Ok((false, "missing plc_id or dataset".into()))
                    }
                }
                other => {
                    warn!("cmd-worker: unknown control action '{other}'");
                    Ok((false, format!("unknown control action '{other}'")))
                }
            }
        }
        PlcRead | Query => {
            warn!("cmd-worker: PLC-touching commands not yet supported in server mode");
            Ok((false, "server cannot execute PLC reads yet".into()))
        }
    }
}

/// Runs in a dedicated thread: claims pending commands, executes them,
/// writes results.
fn command_worker(
    db: Arc<Mutex<Connection>>,
    running: Arc<AtomicBool>,
    snapshot: Arc<Snapshot>,
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
            execute_command(&conn, cmd.id, cmd.kind, &cmd.request_json, &snapshot)
        };

        match result {
            Ok((ok, msg)) => {
                let result_json = if ok {
                    serde_json::json!({"ok": true, "message": msg}).to_string()
                } else {
                    serde_json::json!({"ok": false, "message": msg}).to_string()
                };
                if ok {
                    let conn = db.lock();
                    if let Err(e) = db::command_queue::finish_ok(&conn, cmd.id, &result_json) {
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

/// Batch-write samples into the database.
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

    // Write poll_cycle
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

    // Insert a single placeholder read_span per cycle
    conn.execute(
        "INSERT INTO read_span
            (plc_id, poll_group_id, seq, db_num, byte_offset, byte_len, ok, dataset_id)
         VALUES (?1, ?2, ?3, 0, 0, 0, 1, ?4)",
        rusqlite::params![batch.plc_id, batch.poll_group_id, ci.seq as i64, dataset_id],
    )?;

    // Write samples
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

    // Update tag.last_* in DB metadata
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

/// Runs in a dedicated thread: receives sample batches and writes to SQLite.
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

    // Drain remaining samples
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

/// Per-PLC runtime state.
struct PlcRuntime {
    plc_id: i64,
    _plc_name: String,
    _host: String,
    poller_handle: Option<PollerHandle>,
    _client: Arc<Mutex<dyn PlcClient + Send>>,
}

/// Server configuration.
pub struct ServerConfig {
    pub db_path: String,
}

/// The main s7trace server.
pub struct Server {
    db: Arc<Mutex<Connection>>,
    snapshot: Arc<Snapshot>,
    running: Arc<AtomicBool>,
    _plcs: Vec<PlcRuntime>,
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
            _plcs: Vec::new(),
        })
    }

    /// Load PLCs from the database.
    fn load_plcs(&self) -> Result<Vec<(i64, String, String)>, anyhow::Error> {
        let conn = self.db.lock();
        let mut stmt = conn.prepare("SELECT id, name, host FROM plc ORDER BY id")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;
        let mut plcs = Vec::new();
        for row in rows {
            plcs.push(row?);
        }
        Ok(plcs)
    }

    /// Load enabled tags for a PLC + poll group, build a read plan.
    fn build_plan_for_poll_group(
        &self,
        plc_id: i64,
        pg_id: i64,
    ) -> Result<ReadPlan, anyhow::Error> {
        let conn = self.db.lock();

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
                        8, rusqlite::types::Type::Text, e.into(),
                    ))?,
                poll_group_id: row.get(9)?,
                store_mode: store_mode_str.parse::<StoreMode>()
                    .map_err(|e| rusqlite::Error::FromSqlConversionFailure(
                        10, rusqlite::types::Type::Text, e.into(),
                    ))?,
                storage_hook: storage_hook_str.parse::<StorageHook>()
                    .map_err(|e| rusqlite::Error::FromSqlConversionFailure(
                        11, rusqlite::types::Type::Text, e.into(),
                    ))?,
                hook_params_json: row.get(12)?,
                enabled: row.get(13)?,
                bad: row.get(14)?,
                bad_class: bad_class_str.map(|s| s.parse::<BadClass>().ok()).flatten(),
                bad_detail: row.get(16)?,
            })
        })?.filter_map(|r| r.ok()).collect();

        if tags.is_empty() {
            return Ok(ReadPlan {
                plc_id,
                poll_group_id: pg_id,
                spans: Vec::new(),
            });
        }

        let pdu_payload = 240;
        let max_gap = 8;

        let plan = build_plan(plc_id, pg_id, &tags, pdu_payload, max_gap);
        Ok(plan)
    }

    /// Start pollers for all configured PLCs and poll groups.
    fn start_pollers(
        &mut self,
        sample_tx: &Sender<SampleBatch>,
    ) -> Result<(), anyhow::Error> {
        let plcs = self.load_plcs()?;

        // Create PLC runtimes
        for (plc_id, plc_name, host) in &plcs {
            info!("setting up PLC '{}' (id={}) at {}", plc_name, plc_id, host);
            let client = create_plc_client(plc_name, host);

            self._plcs.push(PlcRuntime {
                plc_id: *plc_id,
                _plc_name: plc_name.clone(),
                _host: host.clone(),
                poller_handle: None,
                _client: client,
            });
        }

        // Load poll groups
        let pg_rows: Vec<(i64, String, u64)> = {
            let conn = self.db.lock();
            let mut stmt = conn.prepare(
                "SELECT pg.id, pg.name, pg.period_ms FROM poll_group pg ORDER BY pg.id",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?.filter_map(|r| r.ok()).collect();
            rows
        };

        // For each (plc, pg) pair that has enabled tags, build plan + spawn poller
        for (plc_id, plc_name, _) in &plcs {
            for (pg_id, _pg_name, period_ms) in &pg_rows {
                let plan = self.build_plan_for_poll_group(*plc_id, *pg_id)?;
                if plan.spans.is_empty() {
                    continue;
                }

                let handle = spawn_poller(
                    *plc_id,
                    plc_name.clone(),
                    *period_ms,
                    plan,
                    // Find the client for this PLC
                    {
                        let rt = self._plcs.iter().find(|p| p.plc_id == *plc_id)
                            .expect("PLC runtime exists");
                        rt._client.clone()
                    },
                    self.snapshot.clone(),
                    sample_tx.clone(),
                );

                // Store handle
                if let Some(rt) = self._plcs.iter_mut().find(|p| p.plc_id == *plc_id) {
                    rt.poller_handle = Some(handle);
                }
            }
        }

        Ok(())
    }

    /// Run the server main loop. Blocks until shutdown.
    pub fn run(&mut self) -> Result<(), anyhow::Error> {
        info!("s7trace server starting");

        let (sample_tx, sample_rx) = mpsc::channel::<SampleBatch>();

        // Start pollers
        self.start_pollers(&sample_tx)?;

        // Start sample writer thread
        let db_writer = self.db.clone();
        let running_writer = self.running.clone();
        std::thread::Builder::new()
            .name("sample-writer".into())
            .spawn(move || {
                sample_writer(db_writer, sample_rx, running_writer);
            })?;

        // Start command worker thread
        let db_cmd = self.db.clone();
        let running_cmd = self.running.clone();
        let snapshot_cmd = self.snapshot.clone();
        std::thread::Builder::new()
            .name("cmd-worker".into())
            .spawn(move || {
                command_worker(db_cmd, running_cmd, snapshot_cmd);
            })?;

        // Start config apply worker thread
        let db_apply = self.db.clone();
        let running_apply = self.running.clone();
        std::thread::Builder::new()
            .name("apply-worker".into())
            .spawn(move || {
                apply_worker(db_apply, running_apply);
            })?;

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
                drop(conn);

                info!(
                    "status: {} PLC(s), {} active tag(s), {} bad, {} pending cmd(s), {} active dataset(s)",
                    self._plcs.len(), tag_count, bad_count, queue_count, active_ds,
                );
            }
        }

        // Clean shutdown
        info!("shutting down...");
        for rt in &self._plcs {
            if let Some(ref h) = rt.poller_handle {
                h.stop();
            }
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
