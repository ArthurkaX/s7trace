//! `collection-start` / `collection-stop` command handlers.

use anyhow::Result;
use rusqlite::Connection;

use crate::cli;
use crate::cli::args::Cli;
use crate::server_base;
use crate::types::*;

pub(crate) fn handle_collection_start(
    _conn: &Connection,
    cli: &Cli,
    plc: &str,
    dataset: &str,
    sets: &[String],
) -> Result<ExitCode> {
    // Remote mode only
    let base = match server_base(cli) {
        Some(b) => b,
        None => {
            cli::format::print_json_error(
                "server_required",
                "collection-start requires a server. Set S7TRACE_SERVER or pass -r.",
                ExitCode::BadInput as i32,
            );
            return Ok(ExitCode::BadInput);
        }
    };
    let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);
    let req = serde_json::json!({
        "action": "collection_start",
        "plc": plc,
        "dataset": dataset,
        "sets": sets,
    });
    let id = cli::remote::enqueue_command(
        &base,
        "control",
        &req.to_string(),
        Some("cli"),
        None,
        timeout,
    )?;
    let (state, result, ok, err) =
        cli::remote::wait_command(&base, id, timeout, std::time::Duration::from_millis(200))?;
    if ok {
        let parsed: serde_json::Value =
            serde_json::from_str(&result).unwrap_or(serde_json::Value::String(result));
        cli::format::print_json_ok(&parsed);
        return Ok(ExitCode::Ok);
    }
    cli::format::print_json_error(
        &state,
        if err.is_empty() { &result } else { &err },
        ExitCode::PlcError as i32,
    );
    Ok(ExitCode::PlcError)
}

pub(crate) fn handle_collection_stop(_conn: &Connection, cli: &Cli, plc: &str) -> Result<ExitCode> {
    // Remote mode only
    let base = match server_base(cli) {
        Some(b) => b,
        None => {
            cli::format::print_json_error(
                "server_required",
                "collection-stop requires a server. Set S7TRACE_SERVER or pass -r.",
                ExitCode::BadInput as i32,
            );
            return Ok(ExitCode::BadInput);
        }
    };
    let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);
    let req = serde_json::json!({
        "action": "collection_stop",
        "plc": plc,
    });
    let id = cli::remote::enqueue_command(
        &base,
        "control",
        &req.to_string(),
        Some("cli"),
        None,
        timeout,
    )?;
    let (state, result, ok, err) =
        cli::remote::wait_command(&base, id, timeout, std::time::Duration::from_millis(200))?;
    if ok {
        let parsed: serde_json::Value =
            serde_json::from_str(&result).unwrap_or(serde_json::Value::String(result));
        cli::format::print_json_ok(&parsed);
        return Ok(ExitCode::Ok);
    }
    cli::format::print_json_error(
        &state,
        if err.is_empty() { &result } else { &err },
        ExitCode::PlcError as i32,
    );
    Ok(ExitCode::PlcError)
}
