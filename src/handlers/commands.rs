//! `command-list` / `-wait` / `-result` / `-cancel` handlers.
//!
//! Each works against the server with `-r` and against the local database
//! otherwise, so a command queued remotely can be followed up remotely.

use anyhow::Result;
use rusqlite::Connection;

use crate::cli::args::Cli;
use crate::cli::format::Format;
use crate::cli::output::{self, fail};
use crate::cli::queue;
use crate::cli::remote::{self, CommandStatus};
use crate::db;
use crate::server_base;
use crate::types::*;

pub(crate) fn handle_command_list(
    cli: &Cli,
    conn: &Connection,
    state: Option<&str>,
    fmt: Format,
) -> Result<ExitCode> {
    if let Some(st) = state {
        if serde_json::from_value::<CommandState>(st.into()).is_err() {
            return Err(fail(
                ErrorCode::InvalidArgument,
                format!("unknown state '{st}': expected pending, running, done, bad or cancelled"),
            ));
        }
    }
    let commands = match server_base(cli) {
        Some(base) => remote::list_commands(&base, state, queue::remote_timeout(cli))?,
        None => db::command_queue::list_pending(conn, state)?,
    };
    if cli.json {
        output::print_ok(&serde_json::to_value(&commands)?, None);
        return Ok(ExitCode::Ok);
    }
    match fmt {
        Format::Text => {
            println!(
                "{:<8} {:<12} {:<10} {:<20} {:<20} {:<30}",
                "ID", "State", "Kind", "Created", "Finished", "Error"
            );
            for cmd in &commands {
                let created = fmt_ts(cmd.created_ts);
                let finished = cmd
                    .finished_ts
                    .map(fmt_ts)
                    .unwrap_or_else(|| "-".to_string());
                let error = cmd.error.as_deref().unwrap_or("-");
                println!(
                    "{:<8} {:<12?} {:<10?} {:<20} {:<20} {:<30}",
                    cmd.id, cmd.state, cmd.kind, created, finished, error
                );
            }
        }
        Format::Json => {
            println!("{}", serde_json::to_string(&commands)?);
        }
        Format::Jsonl => {
            for cmd in &commands {
                println!("{}", serde_json::to_string(cmd)?);
            }
        }
    }
    Ok(ExitCode::Ok)
}

pub(crate) fn fmt_ts(us: i64) -> String {
    let secs = us / 1_000_000;
    let nsec = ((us % 1_000_000) * 1000) as u32;
    chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nsec)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| us.to_string())
}

/// `command-wait`: block until the command finishes, then print its result.
/// Past `timeout_ms` it fails with `timeout` and the command's state; the
/// command itself keeps running.
pub(crate) fn handle_command_wait(
    cli: &Cli,
    conn: &Connection,
    command_id: i64,
    poll_ms: u64,
    timeout_ms: u64,
) -> Result<ExitCode> {
    let st = queue::wait(
        command_id,
        std::time::Duration::from_millis(timeout_ms),
        std::time::Duration::from_millis(poll_ms),
        || queue::status(cli, conn, command_id),
    )
    .map_err(|e| output::with_command_id(e, command_id))?;
    print_settled(cli, &st, Format::Text)
}

/// `command-result`: print the result of a finished command, or fail with
/// `pending` while it is still queued or running.
pub(crate) fn handle_command_result(
    cli: &Cli,
    conn: &Connection,
    command_id: i64,
    fmt: Format,
) -> Result<ExitCode> {
    let st =
        queue::status(cli, conn, command_id).map_err(|e| output::with_command_id(e, command_id))?;
    print_settled(cli, &st, fmt)
}

fn print_settled(cli: &Cli, st: &CommandStatus, fmt: Format) -> Result<ExitCode> {
    let result = queue::settle(st)?;
    if cli.json || fmt != Format::Text {
        output::print_ok(&result, Some(st.id));
    } else {
        let shown = match &result {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        println!("success command_id={} result={shown}", st.id);
    }
    Ok(ExitCode::Ok)
}

pub(crate) fn handle_command_cancel(
    conn: &Connection,
    cli: &Cli,
    command_id: i64,
) -> Result<ExitCode> {
    if let Some(base) = server_base(cli) {
        remote::cancel_command(&base, command_id, queue::remote_timeout(cli))
            .map_err(|e| output::with_command_id(e, command_id))?;
    } else {
        // Check existence first: `cancel` alone cannot tell "unknown" from
        // "no longer pending".
        let st = queue::local_status(conn, command_id)?;
        if !db::command_queue::cancel(conn, command_id)? {
            return Err(output::CliError::new(
                ErrorCode::Conflict,
                format!(
                    "command {command_id} not cancellable (state {}, only pending commands can be cancelled)",
                    st.state_str()
                ),
            )
            .with_command(command_id, Some(st.state_str()))
            .into());
        }
    }
    output::emit(
        cli,
        serde_json::json!({ "cancelled": true }),
        Some(command_id),
        || println!("success command {command_id} cancelled"),
    );
    Ok(ExitCode::Ok)
}
