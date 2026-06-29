//! Poller spawn helpers: plan-building and per-(plc,poll_group) poller launch.

use log::{debug, info};
use rusqlite::Connection;
use std::sync::mpsc::Sender;

use crate::engine::plan::build_plan;
use crate::engine::poller::{spawn_poller, OverrunMode};
use crate::engine::server::{PduCache, PlcRegistry, PollerRegistry, SampleBatch};
use crate::types::*;

/// Spawn a poller for one (plc, poll_group) pair.
pub(crate) fn spawn_one_poller(
    plcs: &PlcRegistry,
    pollers: &PollerRegistry,
    pdu_cache: &PduCache,
    sample_tx: &Sender<SampleBatch>,
    conn: &Connection,
    plc_id: i64,
    plc_name: &str,
    pg_id: i64,
    period_ms: u64,
    overrun_mode: OverrunMode,
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
        reg.get(&plc_id)
            .cloned()
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
        overrun_mode,
    );

    // Store
    pollers.lock().insert((plc_id, pg_id), handle);
    info!(
        "poller started for PLC {} group {} (period={}ms)",
        plc_name, pg_id, period_ms
    );
    Ok(())
}

/// Load enabled, non-bad tags for a PLC + poll group from enabled metric sets.
fn load_active_tags(
    conn: &Connection,
    plc_id: i64,
    pg_id: i64,
) -> Result<Vec<TagRow>, anyhow::Error> {
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

    let tags: Vec<TagRow> = stmt
        .query_map(rusqlite::params![plc_id, pg_id], |row| {
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
                data_type: data_type_str.parse::<DataType>().map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        8,
                        rusqlite::types::Type::Text,
                        std::io::Error::new(std::io::ErrorKind::InvalidData, e).into(),
                    )
                })?,
                poll_group_id: row.get(9)?,
                store_mode: store_mode_str.parse::<StoreMode>().map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        10,
                        rusqlite::types::Type::Text,
                        std::io::Error::new(std::io::ErrorKind::InvalidData, e).into(),
                    )
                })?,
                storage_hook: storage_hook_str.parse::<StorageHook>().map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        11,
                        rusqlite::types::Type::Text,
                        std::io::Error::new(std::io::ErrorKind::InvalidData, e).into(),
                    )
                })?,
                hook_params_json: row.get(12)?,
                enabled: row.get(13)?,
                bad: row.get(14)?,
                bad_class: bad_class_str.and_then(|s| s.parse::<BadClass>().ok()),
                bad_detail: row.get(16)?,
            })
        })?
        .filter_map(|r| r.ok())
        .collect();

    Ok(tags)
}
