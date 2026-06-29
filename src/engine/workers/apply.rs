//! Config-apply worker: claims pending apply rows and rebuilds running pollers.

use parking_lot::Mutex;
use rusqlite::Connection;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

use crate::db;
use crate::engine::poller::OverrunMode;
use crate::engine::server::{PduCache, PlcRegistry, PollerRegistry, SampleBatch};
use crate::types::*;

pub(crate) fn apply_worker(
    db: Arc<Mutex<Connection>>,
    running: Arc<AtomicBool>,
    plcs: PlcRegistry,
    pollers: PollerRegistry,
    pdu_cache: PduCache,
    sample_tx: Sender<SampleBatch>,
    overrun_mode: OverrunMode,
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
            info!(
                "apply-worker: executing apply id={} reason={:?}",
                row.id, row.reason
            );
            let conn = db.lock();
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
                        let plc_pg: Option<(i64, i64)> = conn
                            .query_row(
                                "SELECT plc_id, COALESCE(poll_group_id, 0) FROM tag WHERE id = ?1",
                                rusqlite::params![tag_id],
                                |r| Ok((r.get(0)?, r.get(1)?)),
                            )
                            .ok();
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
                        let pgs: Vec<i64> = conn
                            .prepare(
                                "SELECT DISTINCT COALESCE(poll_group_id, 0)
                             FROM tag WHERE plc_id = ?1 AND enabled = 1 AND bad = 0",
                            )
                            .ok()
                            .and_then(|mut s| {
                                s.query_map(rusqlite::params![plc_id], |r| r.get::<_, i64>(0))
                                    .ok()
                                    .map(|it| it.filter_map(|x| x.ok()).collect())
                            })
                            .unwrap_or_default();
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
                        let pgs: Vec<i64> = conn
                            .prepare(
                                "SELECT DISTINCT COALESCE(t.poll_group_id, 0)
                             FROM tag t
                             JOIN metric_set_tag mst ON mst.tag_id = t.id
                             WHERE mst.metric_set_id = ?1 AND t.plc_id = ?2
                               AND t.enabled = 1 AND t.bad = 0",
                            )
                            .ok()
                            .and_then(|mut s| {
                                s.query_map(rusqlite::params![row.metric_set_id, plc_id], |r| {
                                    r.get::<_, i64>(0)
                                })
                                .ok()
                                .map(|it| it.filter_map(|x| x.ok()).collect())
                            })
                            .unwrap_or_default();
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
                let has_active_dataset: bool = conn
                    .query_row(
                        "SELECT COUNT(*) FROM dataset WHERE plc_id = ?1 AND state = 'active'",
                        rusqlite::params![plc_id],
                        |r| r.get::<_, i64>(0),
                    )
                    .unwrap_or(0)
                    > 0;
                if !has_active_dataset {
                    continue;
                }

                // Stop the existing poller if running
                {
                    let mut reg = pollers.lock();
                    if let Some(handle) = reg.remove(&(*plc_id, *pg_id)) {
                        handle.stop();
                        info!(
                            "apply-worker: stopped poller for PLC {} group {}",
                            plc_id, pg_id
                        );
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
                let period_ms: u64 = conn
                    .query_row(
                        "SELECT period_ms FROM poll_group WHERE id = ?1",
                        rusqlite::params![pg_id],
                        |r| r.get(0),
                    )
                    .unwrap_or(1000);

                if let Err(e) = crate::engine::workers::spawn::spawn_one_poller(
                    &plcs,
                    &pollers,
                    &pdu_cache,
                    &sample_tx,
                    &conn,
                    *plc_id,
                    &plc_name,
                    *pg_id,
                    period_ms,
                    overrun_mode,
                ) {
                    error!(
                        "apply-worker: spawn_one_poller failed for PLC {} group {}: {}",
                        plc_id, pg_id, e
                    );
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
