//! `history-clean` / `-delete-dataset` / `-vacuum` handlers.

use anyhow::Result;
use rusqlite::Connection;

use crate::cli::args::Cli;
use crate::cli::output::{self, fail};
use crate::db;
use crate::types::*;

pub(crate) fn handle_history_clean(
    conn: &Connection,
    cli: &Cli,
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
        .map_err(|_| fail(ErrorCode::NotFound, format!("PLC '{plc}' not found")))?;

    // Check no active dataset
    let active: i64 = conn.query_row(
        "SELECT COUNT(*) FROM dataset WHERE plc_id = ?1 AND state = 'active'",
        rusqlite::params![plc_id],
        |row| row.get(0),
    )?;
    if active > 0 {
        return Err(fail(
            ErrorCode::Conflict,
            format!("PLC '{plc}' has active datasets. Stop collection first."),
        ));
    }

    let cutoff = match (before_ts, before) {
        (Some(ts), _) => ts,
        (_, Some(iso)) => {
            // Parse ISO 8601 to epoch microseconds
            let dt =
                chrono::NaiveDateTime::parse_from_str(&iso.replace('T', " "), "%Y-%m-%d %H:%M:%S")
                    .map_err(|e| {
                        fail(
                            ErrorCode::InvalidArgument,
                            format!("invalid datetime '{iso}': {e}"),
                        )
                    })?;
            dt.and_utc().timestamp_micros()
        }
        (None, None) => {
            return Err(fail(
                ErrorCode::InvalidArgument,
                "specify --before-ts or --before",
            ));
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
    print_queued(cli, cmd_id);
    Ok(ExitCode::Ok)
}

/// The history commands only queue work for the server's admin worker; the
/// result is the queued command, followed with `command-wait`.
fn print_queued(cli: &Cli, cmd_id: i64) {
    output::emit(
        cli,
        serde_json::json!({ "state": "pending" }),
        Some(cmd_id),
        || println!("queued command_id={cmd_id}"),
    );
}

pub(crate) fn handle_history_delete_dataset(
    conn: &Connection,
    cli: &Cli,
    dataset: &str,
) -> Result<ExitCode> {
    // Check dataset exists and is stopped
    let state: String = conn
        .query_row(
            "SELECT state FROM dataset WHERE name = ?1",
            rusqlite::params![dataset],
            |row| row.get(0),
        )
        .map_err(|_| {
            fail(
                ErrorCode::NotFound,
                format!("dataset '{dataset}' not found"),
            )
        })?;

    if state == "active" {
        return Err(fail(
            ErrorCode::Conflict,
            format!("dataset '{dataset}' is active. Stop collection first."),
        ));
    }

    let req = serde_json::json!({
        "action": "delete_dataset",
        "dataset": dataset,
    });
    let cmd_id =
        db::command_queue::enqueue(conn, CommandKind::Admin, &req.to_string(), None, None, None)?;
    print_queued(cli, cmd_id);
    Ok(ExitCode::Ok)
}

/// `history-policy`: show or change the retention policy.
///
/// The `poll_cycle` cutoff was previously fixed in the schema with no way to
/// read or change it from the CLI, which made "configurable retention" true
/// only for someone willing to open the database by hand.
pub(crate) fn handle_history_policy(
    cli: &Cli,
    conn: &Connection,
    cycle_days: Option<i64>,
    sample_days: Option<i64>,
    failed_cycle_days: Option<i64>,
    keep_failed: Option<bool>,
    batch_rows: Option<i64>,
) -> Result<ExitCode> {
    let (cutoff, cycle_cutoff, failed_cycle_cutoff, keep_failed, batch, enabled) =
        db::schema::apply_history_policy(
            conn,
            cycle_days,
            sample_days,
            failed_cycle_days,
            keep_failed,
            batch_rows,
        )
        .map_err(|e| fail(ErrorCode::InvalidArgument, e.to_string()))?;
    let result = serde_json::json!({
        "enabled": enabled,
        "sample_days": cutoff,
        "cycle_days": cycle_cutoff,
        "failed_cycle_days": failed_cycle_cutoff,
        "keep_failed": keep_failed,
        "batch_rows": batch,
    });
    output::emit(cli, result, None, || {
        let fmt = |d: Option<i64>| match d {
            Some(d) => format!("{d} days"),
            None => "keep forever".into(),
        };
        println!("retention policy (global):");
        println!("  enabled           = {enabled}");
        println!("  sample history    = {}", fmt(cutoff));
        println!("  poll_cycle history= {}", fmt(cycle_cutoff));
        println!("  failed cycles    = {}", fmt(failed_cycle_cutoff));
        println!("  use failed cutoff= {keep_failed}");
        println!("  delete batch rows = {batch}");
    });
    Ok(ExitCode::Ok)
}

pub(crate) fn handle_history_vacuum(cli: &Cli, conn: &Connection, force: bool) -> Result<ExitCode> {
    if !force {
        // Check for active datasets
        let active: i64 = conn.query_row(
            "SELECT COUNT(*) FROM dataset WHERE state = 'active'",
            [],
            |row| row.get(0),
        )?;
        if active > 0 {
            return Err(fail(
                ErrorCode::Conflict,
                format!(
                    "{active} active dataset(s); VACUUM may block polling. Use --force to proceed anyway."
                ),
            ));
        }
    }

    conn.execute_batch("VACUUM")?;
    output::emit(cli, serde_json::json!({ "vacuumed": true }), None, || {
        println!("success vacuum completed")
    });
    Ok(ExitCode::Ok)
}
