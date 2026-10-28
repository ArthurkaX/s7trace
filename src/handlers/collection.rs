//! `collection-start` / `collection-stop` command handlers.
//!
//! Both answer with the JSON envelope in every mode.

use anyhow::Result;
use rusqlite::Connection;

use crate::cli::args::Cli;
use crate::cli::output::{self, fail};
use crate::cli::queue;
use crate::server_base;
use crate::types::*;

pub(crate) fn handle_collection_start(
    _conn: &Connection,
    cli: &Cli,
    plc: &str,
    dataset: &str,
    sets: &[String],
) -> Result<ExitCode> {
    let req = serde_json::json!({
        "action": "collection_start",
        "plc": plc,
        "dataset": dataset,
        "sets": sets,
    });
    run_control(cli, "collection-start", &req)
}

pub(crate) fn handle_collection_stop(_conn: &Connection, cli: &Cli, plc: &str) -> Result<ExitCode> {
    let req = serde_json::json!({
        "action": "collection_stop",
        "plc": plc,
    });
    run_control(cli, "collection-stop", &req)
}

fn run_control(cli: &Cli, name: &str, req: &serde_json::Value) -> Result<ExitCode> {
    let base = server_base(cli).ok_or_else(|| {
        fail(
            ErrorCode::ServerRequired,
            format!("{name} requires a server. Set S7TRACE_SERVER or pass -r."),
        )
    })?;
    let (id, result) = queue::submit_and_wait(cli, &base, "control", req, false)?;
    output::print_ok(&result, Some(id));
    Ok(ExitCode::Ok)
}
