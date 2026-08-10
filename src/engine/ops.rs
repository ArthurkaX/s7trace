//! Unified command core.
//!
//! Every mutating operation in s7trace is described by a single typed [`Op`]
//! and executed by exactly one function, [`execute`]. Both entry points share
//! it:
//!
//! - the **server** command-worker deserializes the queued `request_json` into
//!   an `Op` and runs `execute(&ExecCtx::server(..), op)`;
//! - the **CLI** in local mode runs `execute(&ExecCtx::local(conn), op)`
//!   directly; in remote mode it serializes the same `Op` and enqueues it.
//!
//! This removes the historical local/remote duplication where each operation
//! was implemented twice (once as a `handle_*` function in `main.rs`, once as a
//! string-dispatched arm in `server::execute_command`).
//!
//! Operations are migrated group-by-group. Variants whose logic has not been
//! moved here yet fall through to `bail!` and continue to be served by the
//! legacy `execute_command` path (see the migrated-action routing in
//! `server.rs`).

use std::sync::mpsc::Sender;
use std::sync::Arc;

use rusqlite::Connection;
use serde::{Deserialize, Serialize};

use anyhow::anyhow;

use crate::engine::poller::OverrunMode;
use crate::engine::server::{PduCache, PlcRegistry, PollerRegistry, SampleBatch};
use crate::engine::snapshot::Snapshot;
use crate::plc::client::PlcClient;
use crate::plc::{create_plc_handle, PRIORITY_INTERACTIVE};
use crate::types::*;
use crate::{cli, db};

/// Actions whose logic has been migrated into [`execute`]. The server's
/// `execute_command` routes these through the unified core; everything else
/// still uses the legacy match. Grows group-by-group.
pub const MIGRATED_ACTIONS: &[&str] = &[
    "tag_add_full",
    "tag_update",
    "tag_disable",
    "tag_delete",
    "tag_enable",
    "tag_revalidate",
    "set_create",
    "set_add_tag",
    "set_remove_tag",
    "set_enable",
    "set_disable",
    "set_delete",
    "plc_add",
    "poll_group_add",
    "poll_group_set_period",
    "history_policy",
];

/// A single mutating operation. The `action` tag matches the string the legacy
/// queue payloads used, so a serialized `Op` is the queue's `request_json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum Op {
    // --- tags ---
    TagAddFull {
        plc: String,
        name: String,
        address: String,
        data_type: String,
        poll_group: String,
        store_mode: String,
        storage_hook: String,
        value_role: String,
        metric_set: Option<String>,
    },
    TagUpdate {
        tag_id: Option<i64>,
        plc: Option<String>,
        name: Option<String>,
        address: Option<String>,
        data_type: Option<String>,
        poll_group: Option<String>,
        store_mode: Option<String>,
        storage_hook: Option<String>,
    },
    TagDisable {
        tag_id: Option<i64>,
        plc: Option<String>,
        name: Option<String>,
    },
    TagDelete {
        tag_id: Option<i64>,
        plc: Option<String>,
        name: Option<String>,
    },
    TagEnable {
        tag_id: Option<i64>,
        plc: Option<String>,
        name: Option<String>,
    },
    TagRevalidate {
        tag_id: Option<i64>,
        plc: Option<String>,
        name: Option<String>,
    },

    // --- metric sets ---
    SetCreate {
        plc: String,
        name: String,
        description: Option<String>,
    },
    SetAddTag {
        set_name: String,
        tag: Option<String>,
        from_sql: Option<String>,
        from_file: Option<String>,
    },
    SetRemoveTag {
        set_name: String,
        tag: Option<String>,
        from_sql: Option<String>,
    },
    SetEnable {
        set_name: String,
    },
    SetDisable {
        set_name: String,
    },
    SetDelete {
        set_name: String,
    },

    // --- plc / poll groups ---
    PlcAdd {
        name: String,
        host: String,
        rack: i64,
        slot: i64,
        disabled: bool,
    },
    PollGroupAdd {
        name: String,
        period_ms: i64,
    },
    PollGroupSetPeriod {
        poll_group_id: Option<i64>,
        poll_group_name: Option<String>,
        period_ms: i64,
    },

    // --- collection ---
    CollectionStart {
        plc: String,
        dataset: String,
        sets: Vec<String>,
    },
    CollectionStop {
        plc: String,
    },

    // --- history ---
    HistoryClean {
        plc: String,
        before_ts: Option<i64>,
        before: Option<String>,
        batch_size: u64,
    },
    HistoryPolicy {
        cycle_days: Option<i64>,
        sample_days: Option<i64>,
        keep_failed: Option<bool>,
        batch_rows: Option<i64>,
    },
    HistoryDeleteDataset {
        dataset: String,
    },
    HistoryVacuum {
        force: bool,
    },

    // --- events / reads / import ---
    EventAdd {
        text: String,
        kind: String,
        severity: String,
        plc: Option<String>,
        dataset: Option<String>,
        cycle_id: Option<i64>,
    },
    TagImport {
        plc: String,
        file: String,
        set: Option<String>,
        poll_group: String,
        store_mode: String,
        storage_hook: String,
        value_role: String,
        validate: bool,
    },
    ReadOnce {
        plc: String,
        address: String,
        data_type: String,
    },
    ReadRaw {
        plc: String,
        db: u16,
        start: usize,
        len: usize,
        save_probe: bool,
    },
}

impl Op {
    /// The `action` tag for this op (matches the serde tag), used for the
    /// migrated-action routing in `server::execute_command`.
    pub fn action(&self) -> &'static str {
        match self {
            Op::TagAddFull { .. } => "tag_add_full",
            Op::TagUpdate { .. } => "tag_update",
            Op::TagDisable { .. } => "tag_disable",
            Op::TagDelete { .. } => "tag_delete",
            Op::TagEnable { .. } => "tag_enable",
            Op::TagRevalidate { .. } => "tag_revalidate",
            Op::SetCreate { .. } => "set_create",
            Op::SetAddTag { .. } => "set_add_tag",
            Op::SetRemoveTag { .. } => "set_remove_tag",
            Op::SetEnable { .. } => "set_enable",
            Op::SetDisable { .. } => "set_disable",
            Op::SetDelete { .. } => "set_delete",
            Op::PlcAdd { .. } => "plc_add",
            Op::PollGroupAdd { .. } => "poll_group_add",
            Op::PollGroupSetPeriod { .. } => "poll_group_set_period",
            Op::CollectionStart { .. } => "collection_start",
            Op::CollectionStop { .. } => "collection_stop",
            Op::HistoryClean { .. } => "history_clean",
            Op::HistoryPolicy { .. } => "history_policy",
            Op::HistoryDeleteDataset { .. } => "history_delete_dataset",
            Op::HistoryVacuum { .. } => "history_vacuum",
            Op::EventAdd { .. } => "event_add",
            Op::TagImport { .. } => "tag_import",
            Op::ReadOnce { .. } => "read_once",
            Op::ReadRaw { .. } => "read_raw",
        }
    }
}

/// Result of executing an [`Op`]. Serialized into the command_result payload
/// (server path) or printed directly (local path).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpOutput {
    pub ok: bool,
    pub message: String,
    #[serde(default)]
    pub data: serde_json::Value,
}

impl OpOutput {
    pub fn ok(message: impl Into<String>) -> Self {
        Self {
            ok: true,
            message: message.into(),
            data: serde_json::Value::Null,
        }
    }

    pub fn ok_data(message: impl Into<String>, data: serde_json::Value) -> Self {
        Self {
            ok: true,
            message: message.into(),
            data,
        }
    }

    pub fn fail(message: impl Into<String>) -> Self {
        Self {
            ok: false,
            message: message.into(),
            data: serde_json::Value::Null,
        }
    }
}

/// Execution context. Server handles are present only on the server path;
/// in local CLI mode they are `None` and operations behave as the historical
/// local handlers did (e.g. schedule a config-apply instead of touching live
/// pollers).
pub struct ExecCtx<'a> {
    pub conn: &'a Connection,
    pub plcs: Option<&'a PlcRegistry>,
    pub pollers: Option<&'a PollerRegistry>,
    pub pdu_cache: Option<&'a PduCache>,
    pub snapshot: Option<&'a Arc<Snapshot>>,
    pub sample_tx: Option<&'a Sender<SampleBatch>>,
    pub overrun_mode: OverrunMode,
}

impl<'a> ExecCtx<'a> {
    /// Local (CLI) context: no live server handles.
    pub fn local(conn: &'a Connection) -> Self {
        Self {
            conn,
            plcs: None,
            pollers: None,
            pdu_cache: None,
            snapshot: None,
            sample_tx: None,
            overrun_mode: OverrunMode::SkipMissedTicks,
        }
    }

    /// Server context: full access to the live registries.
    pub fn server(
        conn: &'a Connection,
        plcs: &'a PlcRegistry,
        pollers: &'a PollerRegistry,
        pdu_cache: &'a PduCache,
        snapshot: &'a Arc<Snapshot>,
        sample_tx: &'a Sender<SampleBatch>,
        overrun_mode: OverrunMode,
    ) -> Self {
        Self {
            conn,
            plcs: Some(plcs),
            pollers: Some(pollers),
            pdu_cache: Some(pdu_cache),
            snapshot: Some(snapshot),
            sample_tx: Some(sample_tx),
            overrun_mode,
        }
    }

    /// Whether this context has live server handles.
    pub fn is_server(&self) -> bool {
        self.plcs.is_some()
    }
}

/// Resolve a tag id from an explicit id, or a (plc, name) pair.
fn resolve_tag_id(
    conn: &Connection,
    tag_id: Option<i64>,
    plc: Option<&str>,
    name: Option<&str>,
) -> anyhow::Result<i64> {
    if let Some(id) = tag_id {
        return Ok(id);
    }
    match (plc, name) {
        (Some(plc), Some(name)) => conn
            .query_row(
                "SELECT t.id FROM tag t JOIN plc p ON p.id = t.plc_id \
                 WHERE p.name = ?1 AND t.name = ?2",
                rusqlite::params![plc, name],
                |row| row.get::<_, i64>(0),
            )
            .map_err(|_| anyhow!("tag '{name}' not found on PLC '{plc}'")),
        _ => Err(anyhow!("provide --id or both --plc and --name")),
    }
}

/// The single implementation of every mutating operation.
///
/// Operations are migrated into this `match` group-by-group. Until an op's
/// logic is moved here, it returns an error and the legacy `execute_command`
/// path continues to serve it (the migrated-action set [`MIGRATED_ACTIONS`]
/// gates which actions reach this function on the server).
pub fn execute(ctx: &ExecCtx, op: Op) -> anyhow::Result<OpOutput> {
    let conn = ctx.conn;
    match op {
        // ---- tags ----------------------------------------------------------
        Op::TagAddFull {
            plc,
            name,
            address,
            data_type,
            poll_group,
            store_mode,
            storage_hook,
            value_role,
            metric_set,
        } => {
            if name.trim().is_empty() {
                return Ok(OpOutput::fail("--name cannot be empty"));
            }
            let dt: DataType = data_type.parse().map_err(|e: String| anyhow!("{e}"))?;
            let _sm: StoreMode = store_mode
                .parse()
                .map_err(|e: String| anyhow!("invalid store_mode: {e}"))?;
            let _sh: StorageHook = storage_hook
                .parse()
                .map_err(|e: String| anyhow!("invalid storage_hook: {e}"))?;
            let addr = S7Address::parse(&address).map_err(|e| anyhow!("invalid address: {e}"))?;

            let inserted = db::validation::insert_tag(
                conn,
                &plc,
                &name,
                &addr,
                dt,
                &poll_group,
                &store_mode,
                &storage_hook,
                &value_role,
                metric_set.as_deref(),
            )?;
            let tag_id = inserted.tag_id;
            let set_name = metric_set.clone().unwrap_or_else(|| "default".to_string());

            let _ = db::config_apply::schedule(
                conn,
                ApplyScope::Tag,
                Some(inserted.plc_id),
                None,
                None,
                Some(tag_id),
                &format!("tag_add id={tag_id}"),
                db::config_apply::PLAN_COOLDOWN_MS,
                None,
            );

            // PLC client: from the live registry on the server, or built
            // directly from the PLC row in local CLI mode.
            let mut client = if let Some(plcs) = ctx.plcs {
                let reg = plcs.lock();
                reg.get(&inserted.plc_id)
                    .map(|h| h.with_priority(PRIORITY_INTERACTIVE))
                    .ok_or_else(|| anyhow!("PLC client not available for {}", inserted.plc_id))?
            } else {
                let (host, plc_name, rack, slot): (String, String, i64, i64) = conn
                    .query_row(
                        "SELECT host, name, rack, slot FROM plc WHERE id = ?1",
                        rusqlite::params![inserted.plc_id],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                    )
                    .map_err(|_| anyhow!("PLC '{plc}' not found"))?;
                create_plc_handle(&plc_name, &host, rack as u16, slot as u16)
            };

            let result = db::validation::validate_tag(conn, &mut client, tag_id)?;
            if result.ok {
                if let Some(snap) = ctx.snapshot {
                    snap.update(
                        tag_id,
                        inserted.plc_id,
                        &name,
                        &address,
                        dt,
                        result.value_i,
                        result.value_f,
                        result.ts_us,
                    );
                }
                let val_str = cli::format::format_value(result.value_i, result.value_f);
                Ok(OpOutput::ok_data(
                    format!("apply_state=pending tag_id={tag_id}"),
                    serde_json::json!({
                        "value": val_str,
                        "apply_state": "pending",
                        "tag_id": tag_id,
                        "set": set_name,
                    }),
                ))
            } else {
                let bc = result
                    .bad_class
                    .as_ref()
                    .map(|b| b.to_string())
                    .unwrap_or_default();
                let det = result.detail.as_deref().unwrap_or("");
                Ok(OpOutput {
                    ok: false,
                    message: format!("bad bad_class={bc} detail=\"{det}\" tag_id={tag_id}"),
                    data: serde_json::json!({"bad_class": bc, "detail": det, "tag_id": tag_id}),
                })
            }
        }

        Op::TagUpdate {
            tag_id,
            plc,
            name,
            address,
            data_type,
            poll_group,
            store_mode,
            storage_hook,
        } => {
            let tag_id = resolve_tag_id(conn, tag_id, plc.as_deref(), name.as_deref())?;
            let now = now_us();
            if let Some(addr_str) = &address {
                let addr =
                    S7Address::parse(addr_str).map_err(|e| anyhow!("invalid address: {e}"))?;
                conn.execute(
                    "UPDATE tag SET address = ?1, db_num = ?2, byte_offset = ?3, updated_ts = ?4, desired_version = desired_version + 1 WHERE id = ?5",
                    rusqlite::params![addr_str, addr.db_num, addr.byte_offset, now, tag_id],
                )?;
            }
            if let Some(dt) = &data_type {
                let byte_size = dt
                    .parse::<DataType>()
                    .map_err(|e: String| anyhow!("{e}"))?
                    .byte_size() as i64;
                conn.execute(
                    "UPDATE tag SET data_type = ?1, byte_size = ?2, updated_ts = ?3, desired_version = desired_version + 1 WHERE id = ?4",
                    rusqlite::params![dt, byte_size, now, tag_id],
                )?;
            }
            if let Some(pg) = &poll_group {
                let pg_id: i64 = conn
                    .query_row(
                        "SELECT id FROM poll_group WHERE name = ?1",
                        rusqlite::params![pg],
                        |row| row.get(0),
                    )
                    .map_err(|_| anyhow!("poll group '{pg}' not found"))?;
                conn.execute(
                    "UPDATE tag SET poll_group_id = ?1, updated_ts = ?2, desired_version = desired_version + 1 WHERE id = ?3",
                    rusqlite::params![pg_id, now, tag_id],
                )?;
            }
            // An update can produce the same signed/unsigned mismatch an insert
            // can — either half of the binding may have just moved.
            if address.is_some() || data_type.is_some() {
                if let Ok((addr_str, dt)) = conn.query_row(
                    "SELECT address, data_type FROM tag WHERE id = ?1",
                    rusqlite::params![tag_id],
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
                ) {
                    if let Ok(dt) = dt.parse::<DataType>() {
                        crate::db::validation::warn_on_questionable_binding(
                            name.as_deref().unwrap_or("?"),
                            &addr_str,
                            dt,
                        );
                    }
                }
            }

            if let Some(sm) = &store_mode {
                conn.execute(
                    "UPDATE tag SET store_mode = ?1, updated_ts = ?2, desired_version = desired_version + 1 WHERE id = ?3",
                    rusqlite::params![sm, now, tag_id],
                )?;
            }
            if let Some(sh) = &storage_hook {
                conn.execute(
                    "UPDATE tag SET storage_hook = ?1, updated_ts = ?2, desired_version = desired_version + 1 WHERE id = ?3",
                    rusqlite::params![sh, now, tag_id],
                )?;
            }
            db::config_apply::schedule(
                conn,
                ApplyScope::Tag,
                None,
                None,
                None,
                Some(tag_id),
                "tag_updated",
                db::config_apply::PLAN_COOLDOWN_MS,
                None,
            )?;
            Ok(OpOutput::ok_data(
                format!("tag_id={tag_id} apply_state=pending"),
                serde_json::json!({"tag_id": tag_id, "apply_state": "pending"}),
            ))
        }

        Op::TagDisable { tag_id, plc, name } => {
            let tag_id = resolve_tag_id(conn, tag_id, plc.as_deref(), name.as_deref())?;
            let plc_id: Option<i64> = conn
                .query_row(
                    "SELECT plc_id FROM tag WHERE id = ?1",
                    rusqlite::params![tag_id],
                    |row| row.get(0),
                )
                .ok();
            conn.execute(
                "UPDATE tag SET enabled = 0, updated_ts = ?1 WHERE id = ?2",
                rusqlite::params![now_us(), tag_id],
            )?;
            if conn.changes() == 0 {
                return Ok(OpOutput::fail(format!("tag {tag_id}: not found")));
            }
            let _ = db::config_apply::schedule(
                conn,
                ApplyScope::Tag,
                plc_id,
                None,
                None,
                Some(tag_id),
                &format!("tag_disable id={tag_id}"),
                db::config_apply::PLAN_COOLDOWN_MS,
                None,
            );
            Ok(OpOutput::ok_data(
                format!("tag_id={tag_id} enabled=0 apply_state=pending"),
                serde_json::json!({"tag_id": tag_id, "enabled": 0, "apply_state": "pending"}),
            ))
        }

        Op::TagDelete { tag_id, plc, name } => {
            let tag_id = resolve_tag_id(conn, tag_id, plc.as_deref(), name.as_deref())?;
            let plc_id: Option<i64> = conn
                .query_row(
                    "SELECT plc_id FROM tag WHERE id = ?1",
                    rusqlite::params![tag_id],
                    |row| row.get(0),
                )
                .ok();
            if plc_id.is_none() {
                return Ok(OpOutput::fail(format!("tag {tag_id}: not found")));
            }
            // Remove the tag and every row that references it. `metric_set_tag`
            // is ON DELETE CASCADE, but `sample`, `sample_rollup`,
            // `event_marker`, and `config_apply` are not, so clear them first
            // (in one transaction) or the foreign-key check would block the
            // delete.
            let tx = conn.unchecked_transaction()?;
            tx.execute(
                "DELETE FROM sample WHERE tag_id = ?1",
                rusqlite::params![tag_id],
            )?;
            tx.execute(
                "DELETE FROM sample_rollup WHERE tag_id = ?1",
                rusqlite::params![tag_id],
            )?;
            tx.execute(
                "DELETE FROM event_marker WHERE tag_id = ?1",
                rusqlite::params![tag_id],
            )?;
            tx.execute(
                "DELETE FROM config_apply WHERE tag_id = ?1",
                rusqlite::params![tag_id],
            )?;
            tx.execute("DELETE FROM tag WHERE id = ?1", rusqlite::params![tag_id])?;
            tx.commit()?;
            // Rebuild this PLC's plans so a deleted *active* tag leaves the poll
            // set (no-op for already-disabled tags). Scoped to the PLC because
            // the tag row — and any Tag-scoped apply referencing it — is gone.
            if let Some(pid) = plc_id {
                let _ = db::config_apply::schedule(
                    conn,
                    ApplyScope::Plc,
                    Some(pid),
                    None,
                    None,
                    None,
                    &format!("tag_delete id={tag_id}"),
                    db::config_apply::PLAN_COOLDOWN_MS,
                    None,
                );
            }
            Ok(OpOutput::ok_data(
                format!("tag_id={tag_id} deleted"),
                serde_json::json!({"tag_id": tag_id, "deleted": true}),
            ))
        }

        Op::TagEnable { tag_id, plc, name } => {
            let tag_id = resolve_tag_id(conn, tag_id, plc.as_deref(), name.as_deref())?;
            let plc_id: Option<i64> = conn
                .query_row(
                    "SELECT plc_id FROM tag WHERE id = ?1",
                    rusqlite::params![tag_id],
                    |row| row.get(0),
                )
                .ok();
            conn.execute(
                "UPDATE tag SET enabled = 1, updated_ts = ?1 WHERE id = ?2",
                rusqlite::params![now_us(), tag_id],
            )?;
            if conn.changes() == 0 {
                return Ok(OpOutput::fail(format!("tag {tag_id}: not found")));
            }
            let _ = db::config_apply::schedule(
                conn,
                ApplyScope::Tag,
                plc_id,
                None,
                None,
                Some(tag_id),
                &format!("tag_enable id={tag_id}"),
                db::config_apply::PLAN_COOLDOWN_MS,
                None,
            );
            Ok(OpOutput::ok_data(
                format!("tag_id={tag_id} enabled=1 apply_state=pending"),
                serde_json::json!({"tag_id": tag_id, "enabled": 1, "apply_state": "pending"}),
            ))
        }

        Op::TagRevalidate { tag_id, plc, name } => {
            let tag_id = resolve_tag_id(conn, tag_id, plc.as_deref(), name.as_deref())?;

            // Server: perform the actual PLC revalidation now. Local CLI: there
            // is no live PLC connection, so enqueue a command for the server to
            // process (matching historical local behavior).
            let plcs = match ctx.plcs {
                Some(p) => p,
                None => {
                    let cmd_id = db::command_queue::enqueue(
                        conn,
                        CommandKind::Control,
                        &serde_json::to_string(&Op::TagRevalidate {
                            tag_id: Some(tag_id),
                            plc: None,
                            name: None,
                        })?,
                        None,
                        None,
                        None,
                    )?;
                    return Ok(OpOutput::ok_data(
                        format!("queued command_id={cmd_id}"),
                        serde_json::json!({"command_id": cmd_id}),
                    ));
                }
            };

            let (plc_id, tname, taddress, data_type_str): (i64, String, String, String) = conn
                .query_row(
                    "SELECT plc_id, name, address, data_type FROM tag WHERE id = ?1",
                    rusqlite::params![tag_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )?;
            let dt: DataType = data_type_str.parse().unwrap_or(DataType::I16);

            let mut client = {
                let reg = plcs.lock();
                reg.get(&plc_id)
                    .map(|h| h.with_priority(PRIORITY_INTERACTIVE))
                    .ok_or_else(|| anyhow!("PLC client not available"))?
            };
            let result = db::validation::validate_tag(conn, &mut client, tag_id)?;
            if result.ok {
                if let Some(snap) = ctx.snapshot {
                    snap.update(
                        tag_id,
                        plc_id,
                        &tname,
                        &taddress,
                        dt,
                        result.value_i,
                        result.value_f,
                        result.ts_us,
                    );
                }
            }
            let _ = db::config_apply::schedule(
                conn,
                ApplyScope::Tag,
                Some(plc_id),
                None,
                None,
                Some(tag_id),
                &format!("tag_revalidate id={tag_id}"),
                db::config_apply::PLAN_COOLDOWN_MS,
                None,
            );
            if result.ok {
                Ok(OpOutput::ok_data(
                    format!("tag {tag_id} revalidated OK value={:?}", result.value_i),
                    serde_json::json!({"tag_id": tag_id, "value": result.value_i}),
                ))
            } else {
                let det = result.detail.as_deref().unwrap_or("");
                Ok(OpOutput::fail(format!("revalidate failed: {det}")))
            }
        }

        // ---- metric sets ---------------------------------------------------
        Op::SetCreate {
            plc,
            name,
            description,
        } => {
            let plc_id: i64 = conn
                .query_row(
                    "SELECT id FROM plc WHERE name = ?1",
                    rusqlite::params![plc],
                    |row| row.get(0),
                )
                .map_err(|_| anyhow!("PLC '{plc}' not found"))?;
            let now = now_us();
            conn.execute(
                "INSERT INTO metric_set (plc_id, name, enabled, description, created_ts, updated_ts)
                 VALUES (?1, ?2, 0, ?3, ?4, ?4)",
                rusqlite::params![plc_id, name, description, now],
            )?;
            let id = conn.last_insert_rowid();
            Ok(OpOutput::ok_data(
                format!("set_id={id} name={name} plc={plc}"),
                serde_json::json!({"set_id": id, "name": name, "plc": plc}),
            ))
        }

        Op::SetAddTag {
            set_name,
            tag,
            from_sql,
            from_file,
        } => {
            let set: (i64, i64) = conn
                .query_row(
                    "SELECT ms.id, ms.plc_id FROM metric_set ms WHERE ms.name = ?1",
                    rusqlite::params![set_name],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(|_| anyhow!("metric set '{set_name}' not found"))?;
            let (set_id, plc_id) = set;
            let now = now_us();

            // Build the list of tag IDs to add, in priority order: file > sql > tag.
            let mut tag_ids: Vec<i64> = Vec::new();
            if let Some(path) = &from_file {
                let content =
                    std::fs::read_to_string(path).map_err(|e| anyhow!("reading {path}: {e}"))?;
                for line in content.lines() {
                    let name = line.trim();
                    if name.is_empty() || name.starts_with('#') {
                        continue;
                    }
                    let id: i64 = conn
                        .query_row(
                            "SELECT id FROM tag WHERE plc_id = ?1 AND name = ?2",
                            rusqlite::params![plc_id, name],
                            |row| row.get(0),
                        )
                        .map_err(|_| anyhow!("tag '{name}' not found"))?;
                    tag_ids.push(id);
                }
            } else if let Some(sql) = &from_sql {
                let sql_upper = sql.trim().to_uppercase();
                if !sql_upper.starts_with("SELECT") {
                    return Err(anyhow!("--from-sql must be a SELECT query"));
                }
                let mut stmt = conn.prepare(sql)?;
                let rows = stmt.query_map([], |row| row.get::<_, i64>(0))?;
                for row in rows {
                    tag_ids.push(row?);
                }
            } else if let Some(tag_spec) = &tag {
                for piece in tag_spec.split(',') {
                    let piece = piece.trim();
                    if piece.is_empty() {
                        continue;
                    }
                    let id: i64 = if let Ok(id) = piece.parse::<i64>() {
                        id
                    } else {
                        conn.query_row(
                            "SELECT id FROM tag WHERE plc_id = ?1 AND name = ?2",
                            rusqlite::params![plc_id, piece],
                            |row| row.get(0),
                        )
                        .map_err(|_| anyhow!("tag '{piece}' not found"))?
                    };
                    tag_ids.push(id);
                }
            } else {
                return Err(anyhow!("provide --tag, --from-sql, or --from-file"));
            }

            let mut added = 0usize;
            for tag_id in &tag_ids {
                let n = conn.execute(
                    "INSERT OR IGNORE INTO metric_set_tag (metric_set_id, tag_id, plc_id, created_ts)
                     VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![set_id, tag_id, plc_id, now],
                )?;
                added += n;
            }
            let requested = tag_ids.len();
            Ok(OpOutput::ok_data(
                format!(
                    "set='{set_name}' requested={requested} added={added} already_member={}",
                    requested - added
                ),
                serde_json::json!({
                    "set": set_name,
                    "requested": requested,
                    "added": added,
                    "already_member": requested - added,
                }),
            ))
        }

        Op::SetRemoveTag {
            set_name,
            tag,
            from_sql,
        } => {
            let set_id: i64 = conn
                .query_row(
                    "SELECT id FROM metric_set WHERE name = ?1",
                    rusqlite::params![set_name],
                    |row| row.get(0),
                )
                .map_err(|_| anyhow!("metric set '{set_name}' not found"))?;
            if let Some(sql) = &from_sql {
                let mut stmt = conn.prepare(sql)?;
                let rows = stmt.query_map([], |row| row.get::<_, i64>(0))?;
                let mut count = 0;
                for row in rows {
                    let tag_id = row?;
                    conn.execute(
                        "DELETE FROM metric_set_tag WHERE metric_set_id = ?1 AND tag_id = ?2",
                        rusqlite::params![set_id, tag_id],
                    )?;
                    count += 1;
                }
                Ok(OpOutput::ok_data(
                    format!("removed={count} tags from set '{set_name}'"),
                    serde_json::json!({"set": set_name, "removed": count}),
                ))
            } else if let Some(tag_str) = &tag {
                if let Ok(tag_id) = tag_str.parse::<i64>() {
                    conn.execute(
                        "DELETE FROM metric_set_tag WHERE metric_set_id = ?1 AND tag_id = ?2",
                        rusqlite::params![set_id, tag_id],
                    )?;
                } else {
                    conn.execute(
                        "DELETE FROM metric_set_tag WHERE metric_set_id = ?1 AND tag_id IN (SELECT id FROM tag WHERE name = ?2)",
                        rusqlite::params![set_id, tag_str],
                    )?;
                }
                Ok(OpOutput::ok_data(
                    format!("tag removed from set '{set_name}'"),
                    serde_json::json!({"set": set_name}),
                ))
            } else {
                Err(anyhow!("provide --tag or --from-sql"))
            }
        }

        Op::SetEnable { set_name } => {
            let now = now_us();
            conn.execute(
                "UPDATE metric_set SET enabled = 1, updated_ts = ?1 WHERE name = ?2",
                rusqlite::params![now, set_name],
            )?;
            if conn.changes() == 0 {
                return Ok(OpOutput::fail(format!("metric set '{set_name}' not found")));
            }
            let set_id: i64 = conn.query_row(
                "SELECT id FROM metric_set WHERE name = ?1",
                rusqlite::params![set_name],
                |row| row.get(0),
            )?;
            db::config_apply::schedule(
                conn,
                ApplyScope::MetricSet,
                None,
                None,
                Some(set_id),
                None,
                "metric_set_enabled",
                db::config_apply::PLAN_COOLDOWN_MS,
                None,
            )?;
            Ok(OpOutput::ok_data(
                format!("set={set_name} enabled=1 apply_state=pending"),
                serde_json::json!({"set": set_name, "enabled": 1, "apply_state": "pending"}),
            ))
        }

        Op::SetDisable { set_name } => {
            let now = now_us();
            conn.execute(
                "UPDATE metric_set SET enabled = 0, updated_ts = ?1 WHERE name = ?2",
                rusqlite::params![now, set_name],
            )?;
            if conn.changes() == 0 {
                return Ok(OpOutput::fail(format!("metric set '{set_name}' not found")));
            }
            let set_id: i64 = conn.query_row(
                "SELECT id FROM metric_set WHERE name = ?1",
                rusqlite::params![set_name],
                |row| row.get(0),
            )?;
            db::config_apply::schedule(
                conn,
                ApplyScope::MetricSet,
                None,
                None,
                Some(set_id),
                None,
                "metric_set_disabled",
                db::config_apply::PLAN_COOLDOWN_MS,
                None,
            )?;
            Ok(OpOutput::ok_data(
                format!("set={set_name} enabled=0 apply_state=pending"),
                serde_json::json!({"set": set_name, "enabled": 0, "apply_state": "pending"}),
            ))
        }

        Op::SetDelete { set_name } => {
            let set_id: i64 = match conn.query_row(
                "SELECT id FROM metric_set WHERE name = ?1",
                rusqlite::params![set_name],
                |row| row.get(0),
            ) {
                Ok(id) => id,
                Err(_) => return Ok(OpOutput::fail(format!("metric set '{set_name}' not found"))),
            };
            conn.execute(
                "DELETE FROM dataset_metric_set WHERE metric_set_id = ?1",
                rusqlite::params![set_id],
            )?;
            conn.execute(
                "DELETE FROM config_apply WHERE metric_set_id = ?1",
                rusqlite::params![set_id],
            )?;
            conn.execute(
                "DELETE FROM metric_set_tag WHERE metric_set_id = ?1",
                rusqlite::params![set_id],
            )?;
            conn.execute(
                "DELETE FROM metric_set WHERE id = ?1",
                rusqlite::params![set_id],
            )?;
            Ok(OpOutput::ok_data(
                format!("deleted metric set '{set_name}'"),
                serde_json::json!({"set": set_name, "deleted": true}),
            ))
        }

        // ---- plc / poll groups --------------------------------------------
        Op::PlcAdd {
            name,
            host,
            rack,
            slot,
            disabled,
        } => {
            if name.trim().is_empty() {
                return Ok(OpOutput::fail("--name cannot be empty"));
            }
            let exists: bool = conn
                .query_row(
                    "SELECT COUNT(*) FROM plc WHERE name = ?1",
                    rusqlite::params![name],
                    |row| row.get::<_, i64>(0),
                )
                .map(|c| c > 0)
                .unwrap_or(false);
            if exists {
                return Ok(OpOutput::fail(format!("PLC '{name}' already exists")));
            }
            let enabled = if disabled { 0 } else { 1 };
            conn.execute(
                "INSERT INTO plc (name, host, rack, slot, enabled) VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![name, host, rack, slot, enabled],
            )?;
            let id = conn.last_insert_rowid();

            // On the server, register the client in the live registry so the
            // PLC is usable immediately (pollers, reads, tag validation)
            // without restarting the server. Mirrors `init_plc_clients`:
            // try to connect (non-fatal) and cache the negotiated PDU length.
            if let Some(plcs) = ctx.plcs {
                let mut handle = create_plc_handle(&name, &host, rack as u16, slot as u16);
                let pdu = if handle.connect().is_ok() {
                    handle.pdu_len().unwrap_or(240)
                } else {
                    240
                };
                if let Some(cache) = ctx.pdu_cache {
                    cache.lock().insert(id, pdu);
                }
                plcs.lock().insert(id, handle);
                tracing::info!("plc-add: registered live client for PLC {id} ({name})");
            }

            Ok(OpOutput::ok_data(
                format!("plc_id={id} name={name} host={host}"),
                serde_json::json!({"plc_id": id, "name": name, "host": host}),
            ))
        }

        Op::PollGroupAdd { name, period_ms } => {
            if name.trim().is_empty() {
                return Ok(OpOutput::fail("--name cannot be empty"));
            }
            if period_ms < 1 {
                return Ok(OpOutput::fail("--period-ms must be at least 1"));
            }
            let exists: bool = conn
                .query_row(
                    "SELECT COUNT(*) FROM poll_group WHERE name = ?1",
                    rusqlite::params![name],
                    |row| row.get::<_, i64>(0),
                )
                .map(|c| c > 0)
                .unwrap_or(false);
            if exists {
                return Ok(OpOutput::fail(format!(
                    "poll group '{name}' already exists"
                )));
            }
            conn.execute(
                "INSERT INTO poll_group (name, period_ms) VALUES (?1, ?2)",
                rusqlite::params![name, period_ms],
            )?;
            Ok(OpOutput::ok_data(
                format!("poll_group={name} period_ms={period_ms}"),
                serde_json::json!({"poll_group": name, "period_ms": period_ms}),
            ))
        }

        Op::PollGroupSetPeriod {
            poll_group_id,
            poll_group_name,
            period_ms,
        } => {
            if period_ms < 50 {
                return Ok(OpOutput::fail("--period-ms must be at least 50"));
            }
            let pg_id = match (poll_group_id, poll_group_name.as_deref()) {
                (Some(id), _) => id,
                (None, Some(name)) => conn
                    .query_row(
                        "SELECT id FROM poll_group WHERE name = ?1",
                        rusqlite::params![name],
                        |row| row.get::<_, i64>(0),
                    )
                    .map_err(|_| anyhow!("poll group '{name}' not found"))?,
                _ => return Ok(OpOutput::fail("provide --id or --name")),
            };
            let plc_id: Option<i64> = conn
                .query_row(
                    "SELECT DISTINCT t.plc_id FROM tag t WHERE t.poll_group_id = ?1 LIMIT 1",
                    rusqlite::params![pg_id],
                    |row| row.get::<_, i64>(0),
                )
                .ok();
            let pg_name: String = conn
                .query_row(
                    "SELECT name FROM poll_group WHERE id = ?1",
                    rusqlite::params![pg_id],
                    |row| row.get(0),
                )
                .unwrap_or_else(|_| format!("id={pg_id}"));
            conn.execute(
                "UPDATE poll_group SET period_ms = ?1 WHERE id = ?2",
                rusqlite::params![period_ms, pg_id],
            )?;
            if conn.changes() == 0 {
                return Ok(OpOutput::fail("poll group not found"));
            }
            let _ = db::config_apply::schedule(
                conn,
                ApplyScope::PollGroup,
                plc_id,
                Some(pg_id),
                None,
                None,
                &format!("poll_group_update_period id={pg_id}"),
                db::config_apply::PLAN_COOLDOWN_MS,
                None,
            );
            Ok(OpOutput::ok_data(
                format!("poll_group={pg_name} period_ms={period_ms}"),
                serde_json::json!({"poll_group": pg_name, "period_ms": period_ms}),
            ))
        }

        Op::HistoryPolicy {
            cycle_days,
            sample_days,
            keep_failed,
            batch_rows,
        } => {
            let (cutoff, cycle_cutoff, keep_failed, batch, enabled) =
                db::schema::apply_history_policy(
                    conn,
                    cycle_days,
                    sample_days,
                    keep_failed,
                    batch_rows,
                )?;
            let fmt = |d: Option<i64>| match d {
                Some(d) => format!("{d} days"),
                None => "keep forever".into(),
            };
            let summary = format!(
                "enabled={enabled} sample_history={} cycle_history={} keep_failed_cycles={keep_failed} delete_batch_rows={batch}",
                fmt(cutoff),
                fmt(cycle_cutoff)
            );
            Ok(OpOutput::ok_data(
                summary,
                serde_json::json!({
                    "cycle_cutoff_days": cycle_cutoff,
                    "cutoff_days": cutoff,
                    "keep_failed_cycles": keep_failed,
                    "delete_batch_rows": batch,
                    "enabled": enabled,
                }),
            ))
        }

        // ---- not yet migrated ---------------------------------------------
        other => anyhow::bail!("op not yet migrated to unified core: {}", other.action()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The unified core guarantees that an `Op` produces the same effect
    /// whether it is executed directly (local CLI mode) or routed through the
    /// queue (CLI remote -> server). The queue path is exactly
    /// serialize -> enqueue -> deserialize -> execute, so this test exercises
    /// the serde round-trip plus `execute` and asserts identical DB effects.
    #[test]
    fn op_round_trips_and_executes_identically() {
        let conn = crate::db::schema::open_memory().unwrap();

        // Direct local execution.
        let direct = Op::PlcAdd {
            name: "p_direct".into(),
            host: "10.0.0.1".into(),
            rack: 0,
            slot: 2,
            disabled: false,
        };
        let out1 = execute(&ExecCtx::local(&conn), direct).unwrap();
        assert!(out1.ok, "direct execute failed: {}", out1.message);

        // Queue path: serialize -> (enqueue) -> deserialize -> execute.
        let queued = Op::PlcAdd {
            name: "p_queued".into(),
            host: "10.0.0.2".into(),
            rack: 0,
            slot: 2,
            disabled: false,
        };
        let json = serde_json::to_string(&queued).unwrap();
        // The serde tag is the migrated action string the server routes on.
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["action"], "plc_add");
        assert!(MIGRATED_ACTIONS.contains(&"plc_add"));
        let parsed: Op = serde_json::from_str(&json).unwrap();
        let out2 = execute(&ExecCtx::local(&conn), parsed).unwrap();
        assert!(out2.ok, "queued execute failed: {}", out2.message);

        // Both paths persisted a row.
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM plc", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 2);
    }

    fn mem() -> Connection {
        crate::db::schema::open_memory().unwrap()
    }

    fn run(conn: &Connection, op: Op) -> anyhow::Result<OpOutput> {
        execute(&ExecCtx::local(conn), op)
    }

    /// Seed a PLC, poll group and a single tag with the given enabled flag.
    fn seed_tag(conn: &Connection, enabled: i64) {
        conn.execute(
            "INSERT INTO plc (name, host) VALUES ('plc1','127.0.0.1')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO poll_group (name, period_ms) VALUES ('g1',1000)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tag (plc_id,name,address,data_type,poll_group_id,db_num,byte_offset,byte_size,enabled,created_ts,updated_ts)
             VALUES (1,'mytag','DB1.DBW0','i16',1,1,0,2,?1,1000000,1000000)",
            rusqlite::params![enabled],
        )
        .unwrap();
    }

    #[test]
    fn op_set_create_inserts_row() {
        let conn = mem();
        conn.execute(
            "INSERT INTO plc (name, host) VALUES ('test-plc','127.0.0.1')",
            [],
        )
        .unwrap();
        let out = run(
            &conn,
            Op::SetCreate {
                plc: "test-plc".into(),
                name: "my_set".into(),
                description: None,
            },
        )
        .unwrap();
        assert!(out.ok);
        let id: i64 = conn
            .query_row("SELECT id FROM metric_set WHERE name='my_set'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert!(id > 0);
    }

    #[test]
    fn op_tag_disable_unknown_id_fails() {
        let conn = mem();
        let out = run(
            &conn,
            Op::TagDisable {
                tag_id: Some(999),
                plc: None,
                name: None,
            },
        )
        .unwrap();
        assert!(!out.ok);
    }

    #[test]
    fn op_tag_enable_by_id_and_unknown_and_missing() {
        let conn = mem();
        seed_tag(&conn, 0);
        // enable existing id 1
        let out = run(
            &conn,
            Op::TagEnable {
                tag_id: Some(1),
                plc: None,
                name: None,
            },
        )
        .unwrap();
        assert!(out.ok);
        let en: i64 = conn
            .query_row("SELECT enabled FROM tag WHERE id=1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(en, 1);
        // unknown id -> not ok
        assert!(
            !run(
                &conn,
                Op::TagEnable {
                    tag_id: Some(999),
                    plc: None,
                    name: None
                }
            )
            .unwrap()
            .ok
        );
        // missing selectors -> error
        assert!(run(
            &conn,
            Op::TagEnable {
                tag_id: None,
                plc: None,
                name: None
            }
        )
        .is_err());
    }

    #[test]
    fn op_tag_delete_removes_tag_and_non_cascade_children() {
        let conn = mem();
        seed_tag(&conn, 1);
        // Disabling schedules a `config_apply` row referencing the tag.
        // `config_apply` has no ON DELETE CASCADE, so deleting the tag must
        // clear it first or the FK check blocks the delete.
        run(
            &conn,
            Op::TagDisable {
                tag_id: Some(1),
                plc: None,
                name: None,
            },
        )
        .unwrap();
        let ca_before: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM config_apply WHERE tag_id=1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(ca_before >= 1, "expected a config_apply row for the tag");

        let out = run(
            &conn,
            Op::TagDelete {
                tag_id: Some(1),
                plc: None,
                name: None,
            },
        )
        .unwrap();
        assert!(out.ok);

        let tags: i64 = conn
            .query_row("SELECT COUNT(*) FROM tag WHERE id=1", [], |r| r.get(0))
            .unwrap();
        let ca_after: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM config_apply WHERE tag_id=1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(tags, 0, "tag row should be gone");
        assert_eq!(ca_after, 0, "config_apply rows should be gone");

        // Unknown id -> not ok (no panic, no FK error).
        assert!(
            !run(
                &conn,
                Op::TagDelete {
                    tag_id: Some(999),
                    plc: None,
                    name: None
                }
            )
            .unwrap()
            .ok
        );
    }

    #[test]
    fn op_set_add_tag_csv_and_file() {
        let conn = mem();
        conn.execute(
            "INSERT INTO plc (name, host) VALUES ('plc1','127.0.0.1')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO poll_group (name, period_ms) VALUES ('g1',1000)",
            [],
        )
        .unwrap();
        for n in ["t1", "t2", "t3"] {
            conn.execute(
                "INSERT INTO tag (plc_id,name,address,data_type,poll_group_id,db_num,byte_offset,byte_size,created_ts,updated_ts)
                 VALUES (1,?1,'DB1.DBW0','i16',1,1,0,2,1000000,1000000)",
                rusqlite::params![n],
            )
            .unwrap();
        }
        run(
            &conn,
            Op::SetCreate {
                plc: "plc1".into(),
                name: "my_set".into(),
                description: None,
            },
        )
        .unwrap();
        let out = run(
            &conn,
            Op::SetAddTag {
                set_name: "my_set".into(),
                tag: Some("t1,t2,t3".into()),
                from_sql: None,
                from_file: None,
            },
        )
        .unwrap();
        assert!(out.ok);
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM metric_set_tag WHERE metric_set_id=1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 3);

        // from_file (comments and blank lines ignored)
        let path = std::env::temp_dir().join(format!("s7trace_ops_set_{}.txt", std::process::id()));
        std::fs::write(&path, "# comment\nt1\n\nt2\n").unwrap();
        run(
            &conn,
            Op::SetCreate {
                plc: "plc1".into(),
                name: "s2".into(),
                description: None,
            },
        )
        .unwrap();
        let out = run(
            &conn,
            Op::SetAddTag {
                set_name: "s2".into(),
                tag: None,
                from_sql: None,
                from_file: Some(path.to_string_lossy().into_owned()),
            },
        )
        .unwrap();
        assert!(out.ok);
        let set2: i64 = conn
            .query_row("SELECT id FROM metric_set WHERE name='s2'", [], |r| {
                r.get(0)
            })
            .unwrap();
        let n2: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM metric_set_tag WHERE metric_set_id=?1",
                rusqlite::params![set2],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n2, 2);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn op_plc_add_and_duplicate() {
        let conn = mem();
        let out = run(
            &conn,
            Op::PlcAdd {
                name: "myplc".into(),
                host: "10.0.0.1".into(),
                rack: 0,
                slot: 2,
                disabled: false,
            },
        )
        .unwrap();
        assert!(out.ok);
        let dup = run(
            &conn,
            Op::PlcAdd {
                name: "myplc".into(),
                host: "10.0.0.2".into(),
                rack: 0,
                slot: 2,
                disabled: false,
            },
        )
        .unwrap();
        assert!(!dup.ok);
    }

    #[test]
    fn op_poll_group_add_and_duplicate() {
        let conn = mem();
        assert!(
            run(
                &conn,
                Op::PollGroupAdd {
                    name: "fast".into(),
                    period_ms: 40
                }
            )
            .unwrap()
            .ok
        );
        assert!(
            !run(
                &conn,
                Op::PollGroupAdd {
                    name: "fast".into(),
                    period_ms: 100
                }
            )
            .unwrap()
            .ok
        );
    }

    #[test]
    fn op_poll_group_set_period_variants() {
        let conn = mem();
        conn.execute(
            "INSERT INTO poll_group (name, period_ms) VALUES ('g1',1000)",
            [],
        )
        .unwrap();
        // ok by name
        let out = run(
            &conn,
            Op::PollGroupSetPeriod {
                poll_group_id: None,
                poll_group_name: Some("g1".into()),
                period_ms: 50,
            },
        )
        .unwrap();
        assert!(out.ok);
        let p: i64 = conn
            .query_row(
                "SELECT period_ms FROM poll_group WHERE name='g1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(p, 50);
        // too low / unknown id / missing args all -> not ok
        for op in [
            Op::PollGroupSetPeriod {
                poll_group_id: Some(1),
                poll_group_name: None,
                period_ms: 49,
            },
            Op::PollGroupSetPeriod {
                poll_group_id: Some(999),
                poll_group_name: None,
                period_ms: 100,
            },
            Op::PollGroupSetPeriod {
                poll_group_id: None,
                poll_group_name: None,
                period_ms: 100,
            },
        ] {
            assert!(!run(&conn, op).unwrap().ok);
        }
    }

    #[test]
    fn op_history_policy_zero_clamps_and_roundtrips() {
        let conn = mem();
        // 0 stores NULL ("keep forever") for cycle retention; batch_rows is
        // clamped to the 100..=100_000 range; the result round-trips.
        let out = run(
            &conn,
            Op::HistoryPolicy {
                cycle_days: Some(0),
                sample_days: None,
                keep_failed: Some(false),
                batch_rows: Some(999_999_999),
            },
        )
        .unwrap();
        assert!(out.ok, "execute failed: {}", out.message);
        assert_eq!(out.data["cycle_cutoff_days"], serde_json::Value::Null);
        assert_eq!(
            out.data["keep_failed_cycles"],
            serde_json::Value::Bool(false)
        );
        assert_eq!(out.data["delete_batch_rows"], 100_000);
        // Values read back from the DB directly.
        let (ccd, kfc, dbr): (Option<i64>, i64, i64) = conn
            .query_row(
                "SELECT cycle_cutoff_days, keep_failed_cycles, delete_batch_rows
                 FROM history_cleanup_policy WHERE poll_group_id IS NULL",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(ccd, None, "--cycle-days 0 must store NULL");
        assert_eq!(kfc, 0, "keep_failed=false must store 0");
        assert_eq!(dbr, 100_000, "batch_rows above max must clamp to 100000");

        // A positive cycle retention round-trips.
        let out = run(
            &conn,
            Op::HistoryPolicy {
                cycle_days: Some(21),
                sample_days: None,
                keep_failed: None,
                batch_rows: None,
            },
        )
        .unwrap();
        assert!(out.ok);
        assert_eq!(out.data["cycle_cutoff_days"], 21);
    }
}
