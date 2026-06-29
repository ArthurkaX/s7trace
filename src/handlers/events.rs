//! `event` command handler.

use anyhow::Result;
use rusqlite::Connection;

use crate::cli::args::{Cli, EventAction};
use crate::types::*;

pub(crate) fn handle_event_add(
    conn: &Connection,
    cli: &Cli,
    action: &EventAction,
) -> Result<ExitCode> {
    let EventAction::Add {
        text,
        kind,
        severity,
        plc,
        dataset,
        cycle_id,
    } = action;

    // Remote mode not supported yet — event markers are local-only.
    if cli.remote_server.is_some() {
        eprintln!("error: event markers are not supported via remote server yet");
        return Ok(ExitCode::BadInput);
    }

    if text.trim().is_empty() {
        eprintln!("error: event text cannot be empty");
        return Ok(ExitCode::BadInput);
    }

    // Resolve plc_id: either from --plc flag or auto-detect the first enabled PLC.
    let plc_id: i64 = match plc {
        Some(name) => conn
            .query_row(
                "SELECT id FROM plc WHERE name = ?1",
                rusqlite::params![name],
                |row| row.get(0),
            )
            .map_err(|_| {
                anyhow::anyhow!("PLC '{name}' not found. Create it first with 's7trace plc-add'.")
            })?,
        None => conn
            .query_row(
                "SELECT id FROM plc WHERE enabled = 1 ORDER BY id LIMIT 1",
                [],
                |row| row.get(0),
            )
            .map_err(|_| {
                anyhow::anyhow!(
                    "no PLC found. Either create a PLC with 's7trace plc-add' or specify --plc."
                )
            })?,
    };

    // Resolve optional dataset_id
    let dataset_id: Option<i64> = match dataset {
        Some(name) => Some(
            conn.query_row(
                "SELECT id FROM dataset WHERE name = ?1 AND plc_id = ?2",
                rusqlite::params![name, plc_id],
                |row| row.get(0),
            )
            .map_err(|_| anyhow::anyhow!("Dataset '{name}' not found for PLC id={plc_id}."))?,
        ),
        None => None,
    };

    let now_us = crate::types::now_us();

    conn.execute(
        "INSERT INTO event_marker (dataset_id, plc_id, cycle_id, ts, kind, severity, details_json)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        rusqlite::params![dataset_id, plc_id, cycle_id, now_us, kind, severity, text,],
    )?;

    let id = conn.last_insert_rowid();
    println!("event id={id} plc_id={plc_id} kind={kind} ts={now_us}");
    Ok(ExitCode::Ok)
}
