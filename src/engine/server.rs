use parking_lot::Mutex;
use rusqlite::Connection;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;
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
use tracing::{error, info, warn};

use crate::api;
use crate::db;
use crate::engine::poller::{OverrunMode, PollResult, PollerHandle};
use crate::engine::snapshot::Snapshot;
use crate::plc::client::PlcClient;
use crate::plc::{create_plc_handle, PlcHandle};
use crate::types::*;

/// Batch of samples from one poll cycle, sent through the channel.
pub type SampleBatch = PollResult;

/// Registry of PLC connection handles, keyed by plc_id. Each value is a cheap,
/// cloneable [`PlcHandle`] to a per-PLC actor thread that owns the connection
/// (see `plc::actor`); cloning a handle per consumer is what lets concurrent
/// poll groups read the same PLC without blocking each other on a shared mutex.
pub(crate) type PlcRegistry = Arc<Mutex<HashMap<i64, PlcHandle>>>;

/// Registry of running poller handles, keyed by (plc_id, poll_group_id).
pub(crate) type PollerRegistry = Arc<Mutex<HashMap<(i64, i64), PollerHandle>>>;

/// Per-PLC PDU length cache. Populated on first successful connect.
pub(crate) type PduCache = Arc<Mutex<HashMap<i64, usize>>>;

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
    /// Overrun behavior: SkipMissedTicks (default) or Strict.
    pub overrun_mode: OverrunMode,
}

/// The main s7trace server. Orchestrates all workers and pollers.
pub struct Server {
    db: Arc<Mutex<Connection>>,
    /// Database path, kept so read-mostly subsystems (the HTTP API) can open
    /// their *own* connection instead of sharing the writer's mutex. Under WAL
    /// that lets `/status` & friends read concurrently with the sample-writer
    /// rather than queueing behind every poll-cycle write.
    db_path: String,
    snapshot: Arc<Snapshot>,
    running: Arc<AtomicBool>,
    plcs: PlcRegistry,
    pollers: PollerRegistry,
    pdu_cache: PduCache,
    http_bind: Option<String>,
    control_window: bool,
    overrun_mode: OverrunMode,
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
            db_path: config.db_path,
            snapshot,
            running,
            plcs: Arc::new(Mutex::new(HashMap::new())),
            pollers: Arc::new(Mutex::new(HashMap::new())),
            pdu_cache: Arc::new(Mutex::new(HashMap::new())),
            http_bind: config.http_bind,
            control_window: config.control_window,
            overrun_mode: config.overrun_mode,
        })
    }

    /// Initialize PLC clients for all configured PLCs. Attempts to
    /// connect and discover the negotiated PDU length; falls back to a
    /// safe default (240) if the PLC is not reachable at startup.
    fn init_plc_clients(&self) -> Result<(), anyhow::Error> {
        let conn = self.db.lock();
        let mut stmt = conn.prepare(
            "SELECT id, name, host, COALESCE(rack, 0), COALESCE(slot, 0) FROM plc ORDER BY id",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })?;

        let mut reg = self.plcs.lock();
        let mut pdu = self.pdu_cache.lock();
        for row in rows {
            let (plc_id, plc_name, host, rack, slot): (i64, String, String, i64, i64) = row?;
            info!(
                "initializing PLC '{}' (id={}) at {} (rack={}, slot={})",
                plc_name, plc_id, host, rack, slot
            );
            let mut handle = create_plc_handle(&plc_name, &host, rack as u16, slot as u16);

            // Try to connect and discover PDU length. Failure is non-fatal
            // (PLC may come online later).
            if handle.connect().is_ok() {
                match handle.pdu_len() {
                    Ok(n) => {
                        info!("PLC {}: PDU length = {} bytes", plc_name, n);
                        pdu.insert(plc_id, n);
                    }
                    Err(e) => warn!("PLC {}: pdu_len() failed: {} (using 240)", plc_name, e),
                }
            } else {
                warn!(
                    "PLC {}: connect failed at startup; will retry in poller",
                    plc_name
                );
                pdu.insert(plc_id, 240);
            }
            reg.insert(plc_id, handle);
        }

        info!("initialized {} PLC client(s)", reg.len());
        Ok(())
    }

    /// Run the server main loop. Blocks until shutdown.
    pub fn run(&mut self) -> Result<(), anyhow::Error> {
        info!("s7trace server starting");

        // Init PLC clients
        self.init_plc_clients()?;

        // Startup revalidation of bad tags (per architecture.md:195)
        {
            let conn = self.db.lock();
            let bad_tags: Vec<(i64, i64)> = conn
                .prepare("SELECT id, plc_id FROM tag WHERE bad = 1")
                .ok()
                .and_then(|mut s| {
                    s.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))
                        .ok()
                        .map(|it| it.filter_map(|x| x.ok()).collect())
                })
                .unwrap_or_default();
            if !bad_tags.is_empty() {
                info!("startup: revalidating {} bad tag(s)", bad_tags.len());
                let reg = self.plcs.lock();
                for (tag_id, plc_id) in &bad_tags {
                    if let Some(handle) = reg.get(plc_id) {
                        let mut client = handle.with_priority(crate::plc::PRIORITY_INTERACTIVE);
                        match db::validation::validate_tag(&conn, &mut client, *tag_id) {
                            Ok(result) => {
                                if result.ok {
                                    info!(
                                        "startup: tag {} revalidated OK, clearing bad flag",
                                        tag_id
                                    );
                                    let _ = db::config_apply::schedule(
                                        &conn,
                                        ApplyScope::Tag,
                                        Some(*plc_id),
                                        None,
                                        None,
                                        Some(*tag_id),
                                        &format!("startup_revalidate tag_id={tag_id}"),
                                        db::config_apply::PLAN_COOLDOWN_MS,
                                        None,
                                    );
                                } else {
                                    info!(
                                        "startup: tag {} still bad (class={:?} detail={:?})",
                                        tag_id, result.bad_class, result.detail
                                    );
                                }
                            }
                            Err(e) => {
                                warn!("startup: validate tag {} failed: {e}", tag_id);
                            }
                        }
                    } else {
                        warn!(
                            "startup: no PLC client for plc_id={} tag_id={}, skipping revalidation",
                            plc_id, tag_id
                        );
                    }
                }
                drop(reg);
            } else {
                info!("startup: no bad tags to revalidate");
            }
            drop(conn);
        }

        // Channel for poller samples
        let (sample_tx, sample_rx) = mpsc::channel::<SampleBatch>();

        // Start sample writer thread (with snapshot for live value updates)
        let db_writer = self.db.clone();
        let running_writer = self.running.clone();
        let snapshot_writer = self.snapshot.clone();
        std::thread::Builder::new()
            .name("sample-writer".into())
            .spawn(move || {
                crate::engine::workers::sample_writer::sample_writer(
                    db_writer,
                    sample_rx,
                    running_writer,
                    snapshot_writer,
                )
            })?;

        // Start command worker thread (with shared registries)
        let db_cmd = self.db.clone();
        let running_cmd = self.running.clone();
        let snapshot_cmd = self.snapshot.clone();
        let plcs_cmd = self.plcs.clone();
        let pollers_cmd = self.pollers.clone();
        let pdu_cache_cmd = self.pdu_cache.clone();
        let sample_tx_cmd = sample_tx.clone();
        let overrun_cmd = self.overrun_mode;
        std::thread::Builder::new()
            .name("cmd-worker".into())
            .spawn(move || {
                crate::engine::workers::command::command_worker(
                    db_cmd,
                    running_cmd,
                    snapshot_cmd,
                    plcs_cmd,
                    pollers_cmd,
                    pdu_cache_cmd,
                    sample_tx_cmd,
                    overrun_cmd,
                );
            })?;

        // Start config apply worker thread (now with registries for plan rebuild)
        let db_apply = self.db.clone();
        let running_apply = self.running.clone();
        let plcs_apply = self.plcs.clone();
        let pollers_apply = self.pollers.clone();
        let pdu_cache_apply = self.pdu_cache.clone();
        let sample_tx_apply = sample_tx.clone();
        let overrun_apply = self.overrun_mode;
        std::thread::Builder::new()
            .name("apply-worker".into())
            .spawn(move || {
                crate::engine::workers::apply::apply_worker(
                    db_apply,
                    running_apply,
                    plcs_apply,
                    pollers_apply,
                    pdu_cache_apply,
                    sample_tx_apply,
                    overrun_apply,
                );
            })?;

        // Resume pollers for any active datasets (survive server restart).
        {
            let conn = self.db.lock();
            let mut stmt = conn.prepare(
                "SELECT DISTINCT d.plc_id, p.name, pg.id, pg.period_ms
                 FROM dataset d
                 JOIN plc p              ON p.id = d.plc_id
                 JOIN tag t             ON t.plc_id = d.plc_id
                 JOIN poll_group pg     ON pg.id = t.poll_group_id
                 JOIN metric_set_tag mst ON mst.tag_id = t.id
                 JOIN metric_set ms     ON ms.id = mst.metric_set_id
                 WHERE d.state = 'active'
                   AND t.enabled = 1
                   AND t.bad = 0
                   AND ms.enabled = 1
                 ORDER BY d.plc_id, pg.id",
            )?;
            let rows: Vec<(i64, String, i64, u64)> = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, u64>(3)?,
                    ))
                })?
                .filter_map(|r| r.ok())
                .collect();
            drop(stmt);

            let count = rows.len();
            for (plc_id, plc_name, pg_id, period_ms) in &rows {
                if let Err(e) = crate::engine::workers::spawn::spawn_one_poller(
                    &self.plcs,
                    &self.pollers,
                    &self.pdu_cache,
                    &sample_tx,
                    &conn,
                    *plc_id,
                    plc_name,
                    *pg_id,
                    *period_ms,
                    self.overrun_mode,
                ) {
                    error!(
                        "resume poller failed for PLC {} group {}: {e}",
                        plc_id, pg_id
                    );
                }
            }
            info!("resumed {count} poller(s) for active dataset(s)");
        }

        // Optionally start HTTP API on a dedicated connection (see `db_path`):
        // the API is read-mostly, so giving it its own WAL connection keeps
        // `/status` responsive while the sample-writer holds the writer mutex.
        if let Some(bind) = self.http_bind.clone() {
            let db_path = self.db_path.clone();
            std::thread::Builder::new()
                .name("http-api".into())
                .spawn(move || {
                    let conn = match db::schema::open(&db_path) {
                        Ok(c) => c,
                        Err(e) => {
                            error!("HTTP API: opening read connection at {db_path}: {e}");
                            return;
                        }
                    };
                    let db_http = Arc::new(Mutex::new(conn));
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

            if tick.is_multiple_of(6) {
                let conn = self.db.lock();
                let tag_count: i64 = conn
                    .query_row(
                        "SELECT COUNT(*) FROM tag WHERE enabled = 1 AND bad = 0",
                        [],
                        |r| r.get(0),
                    )
                    .unwrap_or(0);
                let bad_count: i64 = conn
                    .query_row("SELECT COUNT(*) FROM tag WHERE bad = 1", [], |r| r.get(0))
                    .unwrap_or(0);
                let queue_count: i64 = conn
                    .query_row(
                        "SELECT COUNT(*) FROM command_request WHERE state = 'pending'",
                        [],
                        |r| r.get(0),
                    )
                    .unwrap_or(0);
                let active_ds: i64 = conn
                    .query_row(
                        "SELECT COUNT(*) FROM dataset WHERE state = 'active'",
                        [],
                        |r| r.get(0),
                    )
                    .unwrap_or(0);
                let poller_count = self.pollers.lock().len();
                drop(conn);

                info!(
                    "status: {} PLC(s), {} active tag(s), {} bad, {} pending cmd(s), {} active dataset(s), {} poller(s)",
                    self.plcs.lock().len(), tag_count, bad_count, queue_count, active_ds, poller_count,
                );
            }

            // Expire stale pending commands every 12 ticks (60 s)
            if tick.is_multiple_of(12) {
                let conn = self.db.lock();
                let expired = db::command_queue::expire_stale_pending(
                    &conn,
                    db::command_queue::PENDING_CMD_TTL_US,
                )
                .unwrap_or(0);
                if expired > 0 {
                    info!("expired {expired} stale pending command(s)");
                }
                drop(conn);
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
        // Stop all PLC actor threads (after pollers, so no in-flight reads).
        for handle in self.plcs.lock().values() {
            handle.stop();
        }

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
    install_panic_logger();
    let mut server = Server::new(config)?;
    server.run()?;
    Ok(())
}

/// Install a panic hook that logs the thread name, source location, and message
/// of any panic before the default hook runs. Without this, a panic in a
/// background thread (poller, cmd-worker, sample-writer, apply-worker) just
/// kills that thread silently — from the outside the process appears to "stop
/// doing work" with nothing in the log. (Bug B-7)
fn install_panic_logger() {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let thread = std::thread::current();
        let name = thread.name().unwrap_or("<unnamed>");
        let location = info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_else(|| "<unknown>".to_string());
        let msg = if let Some(s) = info.payload().downcast_ref::<&str>() {
            (*s).to_string()
        } else if let Some(s) = info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "<non-string panic payload>".to_string()
        };
        error!("PANIC in thread '{name}' at {location}: {msg}");
        default_hook(info);
    }));
}
