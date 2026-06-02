/// Durable command queue backed by SQLite with lease-based claiming.
///
/// Per D003, commands have leases so a crashed worker cannot leave
/// commands in `running` forever.

use crate::types::*;
use anyhow::{Context, Result};
use log::warn;
use rusqlite::{params, Connection};

/// Default lease duration (30 s).
pub const DEFAULT_LEASE_MS: i64 = 30_000;

/// Default heartbeat interval (5 s).
pub const DEFAULT_HEARTBEAT_MS: i64 = 5_000;

/// Maximum number of reclaim attempts before a command is marked bad.
pub const MAX_ATTEMPTS: i64 = 3;

/// Enqueue a new command request.
pub fn enqueue(
    conn: &Connection,
    kind: CommandKind,
    request_json: &str,
    plc_id: Option<i64>,
    requested_by: Option<&str>,
    idempotency_key: Option<&str>,
) -> Result<i64> {
    let now = now_us();
    let kind_str = serde_json::to_string(&kind)?;
    let kind_clean = kind_str.trim_matches('"'); // serde adds quotes for simple enums

    conn.execute(
        "INSERT INTO command_request
            (created_ts, state, kind, request_json, requested_by, idempotency_key, plc_id)
         VALUES (?1, 'pending', ?2, ?3, ?4, ?5, ?6)",
        params![now, kind_clean, request_json, requested_by, idempotency_key, plc_id],
    )
    .context("enqueue command")?;

    Ok(conn.last_insert_rowid())
}

/// Claim the next pending command. Returns `None` if no pending commands.
///
/// Claims one command at a time. Also reclaims expired leases from crashed workers.
pub fn claim_next(conn: &Connection) -> Result<Option<CommandRequest>> {
    let now = now_us();

    // First, try to reclaim expired leases
    let reclaimed = conn.execute(
        "UPDATE command_request
         SET state = 'pending', claimed_ts = NULL, lease_expires_ts = NULL,
             heartbeat_ts = NULL, attempt_count = attempt_count + 1,
             error = CASE WHEN attempt_count >= ?1 THEN 'max_attempts_exceeded' ELSE error END
         WHERE state = 'running'
           AND lease_expires_ts < ?2
           AND attempt_count < ?1",
        params![MAX_ATTEMPTS, now],
    )?;
    if reclaimed > 0 {
        warn!("reclaimed {reclaimed} expired command lease(s)");
    }

    // Mark commands with too many attempts as bad
    conn.execute(
        "UPDATE command_request
         SET state = 'bad', finished_ts = ?1
         WHERE state IN ('pending', 'running')
           AND attempt_count >= ?2",
        params![now, MAX_ATTEMPTS],
    )?;

    // Claim next pending
    conn.execute(
        "UPDATE command_request
         SET state = 'running',
             claimed_ts = ?1,
             lease_expires_ts = ?2,
             heartbeat_ts = ?1,
             attempt_count = attempt_count + 1
         WHERE id = (
             SELECT id FROM command_request
             WHERE state = 'pending'
             ORDER BY priority ASC, id ASC
             LIMIT 1
         )",
        params![now, now + DEFAULT_LEASE_MS * 1000],
    )?;

    if conn.changes() == 0 {
        return Ok(None);
    }

    let row = conn.query_row(
        "SELECT id, created_ts, state, kind, request_json, plc_id
         FROM command_request
         WHERE state = 'running'
         ORDER BY id DESC
         LIMIT 1",
        [],
        |row| {
            let state_str: String = row.get(2)?;
            let kind_str: String = row.get(3)?;
            Ok(CommandRequest {
                id: row.get(0)?,
                created_ts: row.get(1)?,
                state: serde_json::from_str(&format!("\"{state_str}\"")).unwrap_or(CommandState::Pending),
                kind: serde_json::from_str(&format!("\"{kind_str}\"")).unwrap_or(CommandKind::Query),
                request_json: row.get(4)?,
                plc_id: row.get(5)?,
            })
        },
    )?;

    Ok(Some(row))
}

/// Renew the lease on a claimed command (heartbeat).
pub fn heartbeat(conn: &Connection, command_id: i64) -> Result<()> {
    let now = now_us();
    conn.execute(
        "UPDATE command_request
         SET heartbeat_ts = ?1,
             lease_expires_ts = ?2
         WHERE id = ?3 AND state = 'running'",
        params![now, now + DEFAULT_LEASE_MS * 1000, command_id],
    )?;
    Ok(())
}

/// Finish a command with success.
pub fn finish_ok(conn: &Connection, command_id: i64, result_json: &str) -> Result<()> {
    let now = now_us();
    conn.execute(
        "UPDATE command_request
         SET state = 'done', finished_ts = ?1
         WHERE id = ?2",
        params![now, command_id],
    )?;

    conn.execute(
        "INSERT INTO command_result (command_id, ok, result_json)
         VALUES (?1, 1, ?2)
         ON CONFLICT(command_id) DO UPDATE SET ok = 1, result_json = ?2",
        params![command_id, result_json],
    )?;

    Ok(())
}

/// Finish a command with failure.
pub fn finish_bad(conn: &Connection, command_id: i64, error: &str) -> Result<()> {
    let now = now_us();
    conn.execute(
        "UPDATE command_request
         SET state = 'bad', finished_ts = ?1, error = ?2
         WHERE id = ?3",
        params![now, error, command_id],
    )?;

    conn.execute(
        "INSERT INTO command_result (command_id, ok, result_json, error)
         VALUES (?1, 0, NULL, ?2)
         ON CONFLICT(command_id) DO UPDATE SET ok = 0, error = ?2",
        params![command_id, error],
    )?;

    Ok(())
}

/// Get command result.
pub fn get_result(conn: &Connection, command_id: i64) -> Result<Option<CommandResult>> {
    let mut stmt = conn.prepare(
        "SELECT cr.command_id, cr.ok, cr.result_json, cr.error
         FROM command_result cr
         WHERE cr.command_id = ?1",
    )?;

    let mut rows = stmt.query(params![command_id])?;
    match rows.next()? {
        Some(row) => Ok(Some(CommandResult {
            command_id: row.get(0)?,
            ok: row.get(1)?,
            result_json: row.get(2)?,
            error: row.get(3)?,
        })),
        None => Ok(None),
    }
}

/// List pending commands.
pub fn list_pending(conn: &Connection, state_filter: Option<&str>) -> Result<Vec<CommandRequest>> {
    let query = match state_filter {
        Some(_) => "SELECT id, created_ts, state, kind, request_json, plc_id
                     FROM command_request WHERE state = ?1
                     ORDER BY id DESC LIMIT 100",
        None => "SELECT id, created_ts, state, kind, request_json, plc_id
                 FROM command_request
                 ORDER BY id DESC LIMIT 100",
    };

    let mut stmt = conn.prepare(query)?;

    let row_mapper = |row: &rusqlite::Row| {
        let state_str: String = row.get(2)?;
        let kind_str: String = row.get(3)?;
        Ok(CommandRequest {
            id: row.get(0)?,
            created_ts: row.get(1)?,
            state: serde_json::from_str(&format!("\"{state_str}\"")).unwrap_or(CommandState::Pending),
            kind: serde_json::from_str(&format!("\"{kind_str}\"")).unwrap_or(CommandKind::Query),
            request_json: row.get(4)?,
            plc_id: row.get(5)?,
        })
    };

    let row_iter = if let Some(sf) = state_filter {
        stmt.query_map(params![sf], row_mapper)?
    } else {
        stmt.query_map([], row_mapper)?
    };

    let mut commands = Vec::new();
    for row in row_iter {
        commands.push(row?);
    }
    Ok(commands)

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::schema::open_memory;

    #[test]
    fn test_enqueue_and_claim() {
        let conn = open_memory().unwrap();
        let id = enqueue(&conn, CommandKind::PlcRead, r#"{"plc":"line1"}"#, None, None, None).unwrap();
        assert!(id > 0);

        let claimed = claim_next(&conn).unwrap().unwrap();
        assert_eq!(claimed.id, id);
        assert_eq!(claimed.state as i32, CommandState::Running as i32);
    }

    #[test]
    fn test_finish_ok() {
        let conn = open_memory().unwrap();
        let id = enqueue(&conn, CommandKind::PlcRead, r#"{}"#, None, None, None).unwrap();
        claim_next(&conn).unwrap();
        finish_ok(&conn, id, r#"{"value":42}"#).unwrap();

        let result = get_result(&conn, id).unwrap().unwrap();
        assert!(result.ok);
        assert_eq!(result.result_json.unwrap(), r#"{"value":42}"#);
    }

    #[test]
    fn test_finish_bad() {
        let conn = open_memory().unwrap();
        let id = enqueue(&conn, CommandKind::PlcRead, r#"{}"#, None, None, None).unwrap();
        claim_next(&conn).unwrap();
        finish_bad(&conn, id, "PLC unavailable").unwrap();

        let result = get_result(&conn, id).unwrap().unwrap();
        assert!(!result.ok);
        assert_eq!(result.error.unwrap(), "PLC unavailable");
    }

    #[test]
    fn test_lease_reclaim() {
        let conn = open_memory().unwrap();
        let id = enqueue(&conn, CommandKind::PlcRead, r#"{}"#, None, None, None).unwrap();

        // Claim it
        claim_next(&conn).unwrap();

        // Force the lease to expire by setting lease_expires_ts in the past
        conn.execute(
            "UPDATE command_request SET lease_expires_ts = 0 WHERE id = ?1",
            params![id],
        ).unwrap();

        // Should reclaim
        let reclaimed = claim_next(&conn).unwrap().unwrap();
        assert_eq!(reclaimed.id, id);
    }
}
