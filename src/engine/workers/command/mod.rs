//! Command-queue worker: claims, dispatches, and settles pending commands.

mod admin;
mod control;
mod plc_read;

use parking_lot::Mutex;
use rusqlite::Connection;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

use crate::db;
use crate::engine::health::CollectorHealth;
use crate::engine::poller::OverrunMode;
use crate::engine::server::{PduCache, PlcRegistry, PollerRegistry, SampleBatch};
use crate::engine::snapshot::Snapshot;
use crate::types::*;

/// Execute a PLC read command.
#[allow(clippy::too_many_arguments)]
fn execute_command(
    conn: &Connection,
    plcs: &PlcRegistry,
    pollers: &PollerRegistry,
    pdu_cache: &PduCache,
    snapshot: &Arc<Snapshot>,
    sample_tx: &Sender<SampleBatch>,
    health: &Arc<CollectorHealth>,
    cmd_id: i64,
    kind: CommandKind,
    request_json: &str,
    overrun_mode: OverrunMode,
) -> Result<(bool, String), anyhow::Error> {
    use CommandKind::*;

    let req: serde_json::Value = serde_json::from_str(request_json)?;

    // Unified core: actions migrated to `ops::execute` are handled here,
    // bypassing the legacy match below. The CLI sends a serialized `Op`.
    let migrated_action = req["action"].as_str().unwrap_or("");
    if crate::engine::ops::MIGRATED_ACTIONS.contains(&migrated_action) {
        let op: crate::engine::ops::Op = serde_json::from_str(request_json)?;
        let ctx = crate::engine::ops::ExecCtx::server(
            conn,
            plcs,
            pollers,
            pdu_cache,
            snapshot,
            sample_tx,
            overrun_mode,
        );
        let out = crate::engine::ops::execute(&ctx, op)?;
        return Ok((out.ok, out.message));
    }

    match kind {
        Admin => admin::execute_admin(conn, pollers, &req),
        PlcRead => plc_read::execute_plc_read(conn, plcs, snapshot, cmd_id, &req),
        Control => control::execute_control(
            conn,
            plcs,
            pollers,
            pdu_cache,
            snapshot,
            sample_tx,
            health,
            overrun_mode,
            &req,
        ),
        Query => Ok((
            false,
            "server cannot execute SQL queries, use CLI directly".into(),
        )),
    }
}

/// Runs in a dedicated thread: claims pending commands, dispatches to
/// `execute_command`, writes results.
#[allow(clippy::too_many_arguments)]
pub(crate) fn command_worker(
    db: Arc<Mutex<Connection>>,
    running: Arc<AtomicBool>,
    snapshot: Arc<Snapshot>,
    plcs: PlcRegistry,
    pollers: PollerRegistry,
    pdu_cache: PduCache,
    sample_tx: Sender<SampleBatch>,
    health: Arc<CollectorHealth>,
    overrun_mode: OverrunMode,
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

        debug!(
            "cmd-worker: claimed command id={} kind={:?}",
            cmd.id, cmd.kind
        );

        let cmd_id = cmd.id;
        let kind = cmd.kind;
        let _span = tracing::info_span!("command", cmd_id, kind = ?kind).entered();

        // Execute under catch_unwind so a panic in one command (decode bug,
        // backend assertion, etc.) is converted to a failed command instead of
        // silently killing the worker thread and freezing the queue. (Bug B-7)
        // parking_lot mutexes don't poison, so the db lock stays usable after
        // an unwind; AssertUnwindSafe is sound here.
        let result = {
            let conn = db.lock();
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                execute_command(
                    &conn,
                    &plcs,
                    &pollers,
                    &pdu_cache,
                    &snapshot,
                    &sample_tx,
                    &health,
                    cmd.id,
                    cmd.kind,
                    &cmd.request_json,
                    overrun_mode,
                )
            }))
        };

        match result {
            Ok(Ok((ok, msg))) => {
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
            Ok(Err(e)) => {
                error!("cmd-worker: command id={} execution error: {e}", cmd.id);
                let conn = db.lock();
                if let Err(e2) = db::command_queue::finish_bad(&conn, cmd.id, &e.to_string()) {
                    error!("cmd-worker: finish_bad id={} failed: {e2}", cmd.id);
                }
            }
            Err(panic) => {
                let msg = if let Some(s) = panic.downcast_ref::<&str>() {
                    (*s).to_string()
                } else if let Some(s) = panic.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "<non-string panic payload>".to_string()
                };
                error!("cmd-worker: command id={} PANICKED: {msg}", cmd.id);
                let conn = db.lock();
                if let Err(e2) =
                    db::command_queue::finish_bad(&conn, cmd.id, &format!("panic: {msg}"))
                {
                    error!("cmd-worker: finish_bad id={} failed: {e2}", cmd.id);
                }
            }
        }
    }

    info!("cmd-worker stopped");
}
