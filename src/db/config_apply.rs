/// Delayed configuration apply with cooldown/debounce support.

use crate::types::*;
use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};

/// Default cooldown for poll plan rebuilds (ms).
pub const PLAN_COOLDOWN_MS: i64 = 1500;

/// Default cooldown for storage hook changes (ms).
pub const HOOK_COOLDOWN_MS: i64 = 3000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigApplyRow {
    pub id: i64,
    pub created_ts: TimestampUs,
    pub updated_ts: TimestampUs,
    pub apply_after_ts: TimestampUs,
    pub applied_ts: Option<TimestampUs>,
    pub state: ConfigApplyState,
    pub scope: ApplyScope,
    pub plc_id: Option<i64>,
    pub poll_group_id: Option<i64>,
    pub metric_set_id: Option<i64>,
    pub tag_id: Option<i64>,
    pub reason: Option<String>,
    pub cooldown_ms: i64,
    pub details_json: Option<String>,
}

/// Schedule a config apply with cooldown. If a pending apply for the same
/// scope already exists, it is updated (debounced) rather than duplicated.
pub fn schedule(
    conn: &Connection,
    scope: ApplyScope,
    plc_id: Option<i64>,
    poll_group_id: Option<i64>,
    metric_set_id: Option<i64>,
    tag_id: Option<i64>,
    reason: &str,
    cooldown_ms: i64,
    details_json: Option<&str>,
) -> Result<i64> {
    let now = now_us();
    let apply_after = now + cooldown_ms * 1000;

    let scope_str = serde_json::to_string(&scope)?;
    let scope_clean = scope_str.trim_matches('"').to_string();

    // Debounce: update existing pending row for same scope instead of inserting new
    let updated = conn.execute(
        "UPDATE config_apply
         SET updated_ts = ?1, apply_after_ts = ?2, reason = ?3,
             details_json = COALESCE(?4, details_json), cooldown_ms = ?5
         WHERE state = 'pending'
           AND scope = ?6
           AND (plc_id IS NOT DISTINCT FROM ?7)
           AND (poll_group_id IS NOT DISTINCT FROM ?8)
           AND (metric_set_id IS NOT DISTINCT FROM ?9)
           AND (tag_id IS NOT DISTINCT FROM ?10)",
        params![
            now, apply_after, reason, details_json, cooldown_ms,
            scope_clean, plc_id, poll_group_id, metric_set_id, tag_id,
        ],
    )?;

    if updated > 0 {
        return Ok(conn
            .query_row(
                "SELECT id FROM config_apply
                 WHERE scope = ?1 AND state = 'pending'
                   AND (plc_id IS NOT DISTINCT FROM ?2)
                   AND (poll_group_id IS NOT DISTINCT FROM ?3)
                   AND (metric_set_id IS NOT DISTINCT FROM ?4)
                   AND (tag_id IS NOT DISTINCT FROM ?5)
                 ORDER BY id DESC LIMIT 1",
                params![
                    scope_clean, plc_id, poll_group_id, metric_set_id, tag_id,
                ],
                |row| row.get(0),
            )
            .unwrap());
    }

    conn.execute(
        "INSERT INTO config_apply
            (created_ts, updated_ts, apply_after_ts, state, scope,
             plc_id, poll_group_id, metric_set_id, tag_id, reason, cooldown_ms, details_json)
         VALUES (?1, ?1, ?2, 'pending', ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            now, apply_after, scope_clean,
            plc_id, poll_group_id, metric_set_id, tag_id,
            reason, cooldown_ms, details_json,
        ],
    )
    .context("schedule config apply")?;

    Ok(conn.last_insert_rowid())
}

/// Claim next due config apply.
pub fn claim_due(conn: &Connection) -> Result<Option<ConfigApplyRow>> {
    let now = now_us();

    conn.execute(
        "UPDATE config_apply
         SET state = 'applying'
         WHERE id = (
             SELECT id FROM config_apply
             WHERE state = 'pending' AND apply_after_ts <= ?1
             ORDER BY apply_after_ts, id
             LIMIT 1
         )",
        params![now],
    )?;

    if conn.changes() == 0 {
        return Ok(None);
    }

    let row = conn.query_row(
        "SELECT id, created_ts, updated_ts, apply_after_ts, applied_ts,
                state, scope, plc_id, poll_group_id, metric_set_id, tag_id,
                reason, cooldown_ms, details_json
         FROM config_apply
         WHERE state = 'applying'
         ORDER BY id DESC LIMIT 1",
        [],
        |row| {
            let state_str: String = row.get(5)?;
            let scope_str: String = row.get(6)?;
            Ok(ConfigApplyRow {
                id: row.get(0)?,
                created_ts: row.get(1)?,
                updated_ts: row.get(2)?,
                apply_after_ts: row.get(3)?,
                applied_ts: row.get(4)?,
                state: serde_json::from_str(&format!("\"{state_str}\"")).unwrap_or(ConfigApplyState::Pending),
                scope: serde_json::from_str(&format!("\"{scope_str}\"")).unwrap_or(ApplyScope::Tag),
                plc_id: row.get(7)?,
                poll_group_id: row.get(8)?,
                metric_set_id: row.get(9)?,
                tag_id: row.get(10)?,
                reason: row.get(11)?,
                cooldown_ms: row.get(12)?,
                details_json: row.get(13)?,
            })
        },
    )?;

    Ok(Some(row))
}

/// Mark a config apply as done.
pub fn finish_apply(conn: &Connection, apply_id: i64) -> Result<()> {
    let now = now_us();
    conn.execute(
        "UPDATE config_apply
         SET state = 'done', applied_ts = ?1
         WHERE id = ?2",
        params![now, apply_id],
    )?;
    Ok(())
}

/// Mark a config apply as failed.
pub fn fail_apply(conn: &Connection, apply_id: i64, error: &str) -> Result<()> {
    conn.execute(
        "UPDATE config_apply
         SET state = 'bad', details_json = ?1
         WHERE id = ?2",
        params![Some(error), apply_id],
    )?;
    Ok(())
}

/// Count pending applies.
pub fn pending_count(conn: &Connection) -> Result<i64> {
    conn.query_row(
        "SELECT COUNT(*) FROM config_apply WHERE state = 'pending'",
        [],
        |row| row.get(0),
    )
    .context("count pending applies")
}

/// List pending applies.
pub fn list_pending(conn: &Connection) -> Result<Vec<ConfigApplyRow>> {
    let mut stmt = conn.prepare(
        "SELECT id, created_ts, updated_ts, apply_after_ts, applied_ts,
                state, scope, plc_id, poll_group_id, metric_set_id, tag_id,
                reason, cooldown_ms, details_json
         FROM config_apply
         WHERE state = 'pending'
         ORDER BY apply_after_ts, id",
    )?;

    let rows = stmt.query_map([], |row| {
        let state_str: String = row.get(5)?;
        let scope_str: String = row.get(6)?;
        Ok(ConfigApplyRow {
            id: row.get(0)?,
            created_ts: row.get(1)?,
            updated_ts: row.get(2)?,
            apply_after_ts: row.get(3)?,
            applied_ts: row.get(4)?,
            state: serde_json::from_str(&format!("\"{state_str}\"")).unwrap_or(ConfigApplyState::Pending),
            scope: serde_json::from_str(&format!("\"{scope_str}\"")).unwrap_or(ApplyScope::Tag),
            plc_id: row.get(7)?,
            poll_group_id: row.get(8)?,
            metric_set_id: row.get(9)?,
            tag_id: row.get(10)?,
            reason: row.get(11)?,
            cooldown_ms: row.get(12)?,
            details_json: row.get(13)?,
        })
    })?;

    let mut items = Vec::new();
    for row in rows {
        items.push(row?);
    }
    Ok(items)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::schema::open_memory;

    #[test]
    fn test_schedule_and_claim() {
        let conn = open_memory().unwrap();
        // Insert required FK rows
        conn.execute("INSERT INTO plc (id, name, host) VALUES (1, 'test-plc', '127.0.0.1')", []).unwrap();
        conn.execute("INSERT INTO poll_group (id, name, period_ms) VALUES (2, 'fast', 40)", []).unwrap();
        let id = schedule(
            &conn, ApplyScope::PollGroup, Some(1), Some(2), None, None,
            "poll_group_changed", PLAN_COOLDOWN_MS, None,
        ).unwrap();
        assert!(id > 0);
    }

    #[test]
    fn test_debounce() {
        let conn = open_memory().unwrap();
        conn.execute("INSERT INTO plc (id, name, host) VALUES (1, 'test-plc', '127.0.0.1')", []).unwrap();
        conn.execute("INSERT INTO poll_group (id, name, period_ms) VALUES (2, 'fast', 40)", []).unwrap();

        let id1 = schedule(
            &conn, ApplyScope::PollGroup, Some(1), Some(2), None, None,
            "first", PLAN_COOLDOWN_MS, None,
        ).unwrap();

        let id2 = schedule(
            &conn, ApplyScope::PollGroup, Some(1), Some(2), None, None,
            "debounced", PLAN_COOLDOWN_MS, None,
        ).unwrap();

        // Should be same row (debounced)
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_apply_tag_version_flip() {
        // Simulates what apply_worker does: claim_due + apply + finish_apply
        // should reflect in tag.applied_version == desired_version.
        let conn = open_memory().unwrap();
        conn.execute("INSERT INTO plc (id, name, host) VALUES (1, 'p', 'h')", []).unwrap();
        conn.execute("INSERT INTO poll_group (id, name, period_ms) VALUES (1, 'fast', 40)", []).unwrap();
        conn.execute(
            "INSERT INTO tag (id, plc_id, name, address, db_num, byte_offset, byte_size,
                              data_type, poll_group_id, store_mode, storage_hook,
                              enabled, created_ts, updated_ts, desired_version, applied_version, apply_state)
             VALUES (10, 1, 't', 'DB1.DBW0', 1, 0, 2, 'i16', 1, 'on_change', 'on_change',
                     1, 0, 0, 5, 0, 'pending')",
            [],
        ).unwrap();

        let id = schedule(
            &conn, ApplyScope::Tag, Some(1), Some(1), None, Some(10),
            "test", 0, None,
        ).unwrap();
        assert!(id > 0);

        let row = claim_due(&conn).unwrap().unwrap();
        assert_eq!(row.id, id);

        // Mimic apply_worker Tag-scope update
        conn.execute(
            "UPDATE tag SET applied_version = desired_version, apply_state = 'applied' WHERE id = ?1",
            rusqlite::params![row.tag_id],
        ).unwrap();
        finish_apply(&conn, row.id).unwrap();

        let (applied, desired, state): (i64, i64, String) = conn.query_row(
            "SELECT applied_version, desired_version, apply_state FROM tag WHERE id = 10",
            [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        ).unwrap();
        assert_eq!(applied, 5);
        assert_eq!(desired, 5);
        assert_eq!(state, "applied");
    }
}
