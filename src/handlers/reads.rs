//! `read` / `read-raw` command handlers.

use anyhow::Result;
use rusqlite::Connection;

use crate::cli::args::Cli;
use crate::cli::format::RawFormat;
use crate::cli::output::{self, fail};
use crate::cli::queue;
use crate::server_base;
use crate::types::*;

/// `read` and `read-raw` answer with the JSON envelope in every mode.
pub(crate) fn handle_read(
    cli: &Cli,
    _conn: &Connection,
    plc: &str,
    address: &str,
    data_type: &str,
) -> Result<ExitCode> {
    // Validate local args before any network work so a bad data type or
    // address isn't masked by a connection error.
    if let Err(e) = data_type.parse::<crate::types::DataType>() {
        return Err(fail(ErrorCode::InvalidArgument, e));
    }
    if let Err(e) = crate::types::S7Address::parse(address) {
        return Err(fail(ErrorCode::InvalidAddress, e.to_string()));
    }

    let base = server_base(cli).ok_or_else(|| {
        fail(
            ErrorCode::ServerRequired,
            "read requires a server. Set S7TRACE_SERVER or pass -r.",
        )
    })?;
    let req = serde_json::json!({
        "action": "read",
        "plc": plc,
        "address": address,
        "data_type": data_type,
    });
    let (id, result) = queue::submit_and_wait(cli, &base, "plc_read", &req, false)?;
    output::print_ok(&result, Some(id));
    Ok(ExitCode::Ok)
}

pub(crate) fn handle_read_raw(
    _conn: &Connection,
    cli: &Cli,
    plc: &str,
    db: u16,
    start: usize,
    len: usize,
    fmt: RawFormat,
    save_probe: bool,
) -> Result<ExitCode> {
    let base = server_base(cli).ok_or_else(|| {
        fail(
            ErrorCode::ServerRequired,
            "read-raw requires a server. Set S7TRACE_SERVER or pass -r.",
        )
    })?;
    let req = serde_json::json!({
        "action": "read_raw",
        "plc": plc,
        "db": db,
        "start": start,
        "len": len,
        "format": fmt.as_str(),
        "save_probe": save_probe,
    });
    let (id, result) = queue::submit_and_wait(cli, &base, "plc_read", &req, false)?;
    output::print_ok(&result, Some(id));
    Ok(ExitCode::Ok)
}
