//! `history-clean` / `-delete-dataset` / `-vacuum` handlers.

use anyhow::Result;
use rusqlite::Connection;

use crate::cli::args::Cli;
use crate::db;
use crate::types::*;

pub(crate) fn handle_history_clean(
    conn: &Connection,
    _cli: &Cli,
    plc: &str,
    before_ts: Option<i64>,
    before: Option<&str>,
    batch_size: u64,
) -> Result<ExitCode> {
    let plc_id: i64 = conn
        .query_row(
            "SELECT id FROM plc WHERE name = ?1",
            rusqlite::params![plc],
            |row| row.get(0),
        )
        .map_err(|_| anyhow::anyhow!("PLC '{plc}' not found"))?;

    // Check no active dataset
    let active: i64 = conn.query_row(
        "SELECT COUNT(*) FROM dataset WHERE plc_id = ?1 AND state = 'active'",
        rusqlite::params![plc_id],
        |row| row.get(0),
    )?;
    if active > 0 {
        eprintln!("error: PLC '{plc}' has active datasets. Stop collection first.");
        return Ok(ExitCode::BadInput);
    }

    let cutoff = match (before_ts, before) {
        (Some(ts), _) => ts,
        (_, Some(iso)) => {
            // Parse ISO 8601 to epoch microseconds
            let dt =
                chrono::NaiveDateTime::parse_from_str(&iso.replace('T', " "), "%Y-%m-%d %H:%M:%S")
                    .map_err(|e| anyhow::anyhow!("invalid datetime '{iso}': {e}"))?;
            dt.and_utc().timestamp_micros()
        }
        (None, None) => {
            eprintln!("error: specify --before-ts or --before");
            return Ok(ExitCode::BadInput);
        }
    };

    let req = serde_json::json!({
        "action": "history_clean",
        "plc_id": plc_id,
        "cutoff_ts": cutoff,
        "batch_size": batch_size,
    });
    let cmd_id = db::command_queue::enqueue(
        conn,
        CommandKind::Admin,
        &req.to_string(),
        Some(plc_id),
        None,
        None,
    )?;
    println!("queued command_id={cmd_id}");
    Ok(ExitCode::Ok)
}

pub(crate) fn handle_history_delete_dataset(
    conn: &Connection,
    _cli: &Cli,
    dataset: &str,
) -> Result<ExitCode> {
    // Check dataset exists and is stopped
    let state: String = conn
        .query_row(
            "SELECT state FROM dataset WHERE name = ?1",
            rusqlite::params![dataset],
            |row| row.get(0),
        )
        .map_err(|_| anyhow::anyhow!("dataset '{dataset}' not found"))?;

    if state == "active" {
        eprintln!("error: dataset '{dataset}' is active. Stop collection first.");
        return Ok(ExitCode::BadInput);
    }

    let req = serde_json::json!({
        "action": "delete_dataset",
        "dataset": dataset,
    });
    let cmd_id =
        db::command_queue::enqueue(conn, CommandKind::Admin, &req.to_string(), None, None, None)?;
    println!("queued command_id={cmd_id}");
    Ok(ExitCode::Ok)
}

/// `history-policy`: show or change the retention policy.
///
/// The `poll_cycle` cutoff was previously fixed in the schema with no way to
/// read or change it from the CLI, which made "configurable retention" true
/// only for someone willing to open the database by hand.
pub(crate) fn handle_history_policy(
    conn: &Connection,
    cycle_days: Option<i64>,
    sample_days: Option<i64>,
    keep_failed: Option<bool>,
    batch_rows: Option<i64>,
) -> Result<ExitCode> {
    match db::schema::apply_history_policy(conn, cycle_days, sample_days, keep_failed, batch_rows) {
        Ok((cutoff, cycle_cutoff, keep_failed, batch, enabled)) => {
            let fmt = |d: Option<i64>| match d {
                Some(d) => format!("{d} days"),
                None => "keep forever".into(),
            };
            println!("retention policy (global):");
            println!("  enabled           = {enabled}");
            println!("  sample history    = {}", fmt(cutoff));
            println!("  poll_cycle history= {}", fmt(cycle_cutoff));
            println!("  keep failed cycles= {keep_failed}");
            println!("  delete batch rows = {batch}");
            Ok(ExitCode::Ok)
        }
        Err(e) => {
            eprintln!("error: {e}");
            Ok(ExitCode::BadInput)
        }
    }
}

pub(crate) fn handle_history_vacuum(conn: &Connection, force: bool) -> Result<ExitCode> {
    if !force {
        // Check for active datasets
        let active: i64 = conn.query_row(
            "SELECT COUNT(*) FROM dataset WHERE state = 'active'",
            [],
            |row| row.get(0),
        )?;
        if active > 0 {
            eprintln!("warning: {active} active dataset(s). VACUUM may block polling.");
            eprintln!("Use --force to proceed anyway.");
            return Ok(ExitCode::BadInput);
        }
    }

    conn.execute_batch("VACUUM")?;
    println!("success vacuum completed");
    Ok(ExitCode::Ok)
}
