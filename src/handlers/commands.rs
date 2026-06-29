//! `command-list` / `-wait` / `-result` / `-cancel` handlers.

use anyhow::Result;
use rusqlite::Connection;

use crate::cli::args::Cli;
use crate::cli::format::Format;
use crate::server_base;
use crate::types::*;
use crate::{cli, db};

pub(crate) fn handle_command_list(
    conn: &Connection,
    state: Option<&str>,
    fmt: &str,
) -> Result<ExitCode> {
    let commands = db::command_queue::list_pending(conn, state)?;
    let out_fmt = Format::from_str(fmt);
    match out_fmt {
        Format::Table => {
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
        Format::CompactJson => {
            println!("{}", serde_json::to_string(&commands).unwrap_or_default());
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

pub(crate) fn wait_and_print(
    conn: &Connection,
    command_id: i64,
    poll_ms: u64,
    timeout_ms: u64,
) -> Result<ExitCode> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
    let poll_dur = std::time::Duration::from_millis(poll_ms);

    while std::time::Instant::now() < deadline {
        if let Some(result) = db::command_queue::get_result(conn, command_id)? {
            handle_command_result_inner(&result, &Format::CompactJson)?;
            return if result.ok {
                Ok(ExitCode::Ok)
            } else {
                Ok(ExitCode::PlcError)
            };
        }
        std::thread::sleep(poll_dur);
    }

    eprintln!("timeout waiting for command {command_id}");
    Ok(ExitCode::Overload)
}

pub(crate) fn handle_command_wait(
    conn: &Connection,
    command_id: i64,
    poll_ms: u64,
    timeout_ms: u64,
) -> Result<ExitCode> {
    wait_and_print(conn, command_id, poll_ms, timeout_ms)
}

pub(crate) fn handle_command_result(
    conn: &Connection,
    command_id: i64,
    fmt: &str,
) -> Result<ExitCode> {
    let out_fmt = Format::from_str(fmt);
    match db::command_queue::get_result(conn, command_id)? {
        Some(result) => handle_command_result_inner(&result, &out_fmt),
        None => {
            eprintln!("command {command_id}: no result yet (still pending or unknown)");
            Ok(ExitCode::Overload)
        }
    }
}

pub(crate) fn handle_command_cancel(
    conn: &Connection,
    cli: &Cli,
    command_id: i64,
) -> Result<ExitCode> {
    // Remote mode
    if let Some(base) = server_base(cli) {
        let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);
        cli::remote::cancel_command(&base, command_id, timeout)?;
        println!("success command {command_id} cancelled");
        return Ok(ExitCode::Ok);
    }

    match db::command_queue::cancel(conn, command_id)? {
        true => {
            println!("success command {command_id} cancelled");
            Ok(ExitCode::Ok)
        }
        false => {
            eprintln!("error: command {command_id} not cancellable (not in pending state)");
            Ok(ExitCode::BadInput)
        }
    }
}

pub(crate) fn handle_command_result_inner(
    result: &CommandResult,
    fmt: &Format,
) -> Result<ExitCode> {
    match fmt {
        Format::Table => {
            if result.ok {
                if let Some(ref json) = result.result_json {
                    println!("success command_id={} result={json}", result.command_id);
                } else {
                    println!("success command_id={}", result.command_id);
                }
            } else {
                let err = result.error.as_deref().unwrap_or("unknown error");
                println!("bad command_id={} detail=\"{err}\"", result.command_id);
            }
        }
        Format::CompactJson => {
            if result.ok {
                cli::format::print_json_ok(&serde_json::json!({
                    "command_id": result.command_id,
                    "result_json": result.result_json,
                }));
            } else {
                let err = result.error.as_deref().unwrap_or("unknown error");
                cli::format::print_json_error("command_failed", err, ExitCode::PlcError as i32);
            }
        }
    }
    Ok(if result.ok {
        ExitCode::Ok
    } else {
        ExitCode::PlcError
    })
}
