//! `read` / `read-raw` command handlers.

use anyhow::Result;
use rusqlite::Connection;

use crate::cli;
use crate::cli::args::Cli;
use crate::server_base;
use crate::types::*;

pub(crate) fn handle_read(
    cli: &Cli,
    _conn: &Connection,
    plc: &str,
    address: &str,
    data_type: &str,
    fmt: &str,
) -> Result<ExitCode> {
    // Remote mode: send to server, wait, print result.
    if let Some(base) = server_base(cli) {
        let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);
        let req = serde_json::json!({
            "action": "read",
            "plc": plc,
            "address": address,
            "data_type": data_type,
            "format": fmt,
        });
        let id = cli::remote::enqueue_command(
            &base,
            "plc_read",
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
        } else {
            cli::format::print_json_error(
                &state,
                if err.is_empty() { &result } else { &err },
                ExitCode::PlcError as i32,
            );
            return Ok(ExitCode::PlcError);
        }
        return Ok(ExitCode::Ok);
    }

    // Local mode (server required)
    cli::format::print_json_error(
        "server_required",
        "read requires a server. Set S7TRACE_SERVER or pass -r.",
        ExitCode::BadInput as i32,
    );
    Ok(ExitCode::BadInput)
}

pub(crate) fn handle_read_raw(
    _conn: &Connection,
    cli: &Cli,
    plc: &str,
    db: u16,
    start: usize,
    len: usize,
    fmt: &str,
    save_probe: bool,
) -> Result<ExitCode> {
    // Remote mode: send to server, wait, print result.
    if let Some(base) = server_base(cli) {
        let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);
        let req = serde_json::json!({
            "action": "read_raw",
            "plc": plc,
            "db": db,
            "start": start,
            "len": len,
            "format": fmt,
            "save_probe": save_probe,
        });
        let id = cli::remote::enqueue_command(
            &base,
            "plc_read",
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
        } else {
            cli::format::print_json_error(
                &state,
                if err.is_empty() { &result } else { &err },
                ExitCode::PlcError as i32,
            );
            return Ok(ExitCode::PlcError);
        }
        return Ok(ExitCode::Ok);
    }

    // Local mode (server required)
    cli::format::print_json_error(
        "server_required",
        "read-raw requires a server. Set S7TRACE_SERVER or pass -r.",
        ExitCode::BadInput as i32,
    );
    Ok(ExitCode::BadInput)
}
