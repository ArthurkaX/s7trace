//! Following queued commands, the same way for a server (`-r`) and for a local
//! database: fetch a command's status, wait for it, and turn the finished
//! command into its result or a classified [`CliError`].

use std::time::{Duration, Instant};

use anyhow::Result;
use rusqlite::Connection;

use crate::cli::args::Cli;
use crate::cli::output::{self, CliError};
use crate::cli::remote::{self, CommandStatus};
use crate::db;
use crate::types::{CommandKind, CommandState, ErrorCode};

/// Poll interval while waiting on a remote command.
pub const POLL: Duration = Duration::from_millis(200);

/// Status of a command in the local database; `not_found` if it is unknown.
pub fn local_status(conn: &Connection, id: i64) -> Result<CommandStatus> {
    let req = db::command_queue::get_request(conn, id)?
        .ok_or_else(|| output::fail(ErrorCode::NotFound, format!("command {id} not found")))?;
    Ok(CommandStatus {
        id,
        kind: Some(req.kind),
        state: req.state,
        result: db::command_queue::get_result(conn, id)?,
    })
}

/// Status of a command, from the server when `-r` is set, else locally.
pub fn status(cli: &Cli, conn: &Connection, id: i64) -> Result<CommandStatus> {
    match crate::cli::remote_base(cli) {
        Some(base) => remote::get_command(&base, id, remote_timeout(cli)),
        None => local_status(conn, id),
    }
}

pub fn remote_timeout(cli: &Cli) -> Duration {
    Duration::from_millis(cli.remote_timeout_ms)
}

/// Poll `fetch` until the command finishes; past `timeout` fail with
/// `timeout`, carrying the command id and its last state.
pub fn wait(
    id: i64,
    timeout: Duration,
    poll: Duration,
    mut fetch: impl FnMut() -> Result<CommandStatus>,
) -> Result<CommandStatus> {
    let started = Instant::now();
    loop {
        let st = fetch()?;
        if st.finished() {
            return Ok(st);
        }
        if started.elapsed() >= timeout {
            return Err(CliError::new(
                ErrorCode::Timeout,
                format!(
                    "command {id} still {} after {} ms",
                    st.state_str(),
                    timeout.as_millis()
                ),
            )
            .with_command(id, Some(st.state_str()))
            .into());
        }
        std::thread::sleep(poll);
    }
}

/// The result of a finished command, or its failure as a [`CliError`].
///
/// A successful result is the command's `result_json` parsed as JSON (a plain
/// string if it is not JSON). A failure takes its code from the server's
/// `{"code": ...}` payload when present, else from the command kind.
pub fn settle(st: &CommandStatus) -> std::result::Result<serde_json::Value, CliError> {
    let state = Some(st.state_str());
    let fail =
        |code: ErrorCode, msg: String| Err(CliError::new(code, msg).with_command(st.id, state));
    match &st.result {
        Some(r) if r.ok => Ok(parse_payload(r.result_json.as_deref())),
        Some(r) => {
            let payload = parse_payload(r.result_json.as_deref());
            let code = payload["code"]
                .as_str()
                .and_then(|c| serde_json::from_value(c.into()).ok())
                .unwrap_or(match st.kind {
                    Some(CommandKind::PlcRead) => ErrorCode::PlcError,
                    _ => ErrorCode::CommandFailed,
                });
            let message = r
                .error
                .clone()
                .or_else(|| payload["message"].as_str().map(str::to_string))
                .unwrap_or_else(|| format!("command {} failed", st.id));
            Err(CliError::new(code, message)
                .with_command(st.id, state)
                .with_details(payload["data"].clone()))
        }
        None if st.state == CommandState::Cancelled => fail(
            ErrorCode::Cancelled,
            format!("command {} was cancelled", st.id),
        ),
        None if st.finished() => fail(
            ErrorCode::Internal,
            format!("command {} finished without a result", st.id),
        ),
        None => fail(
            ErrorCode::Pending,
            format!("command {} is still {}", st.id, st.state_str()),
        ),
    }
}

fn parse_payload(raw: Option<&str>) -> serde_json::Value {
    match raw {
        None => serde_json::Value::Null,
        Some(s) => serde_json::from_str(s).unwrap_or_else(|_| serde_json::Value::String(s.into())),
    }
}

/// Queue `request` on the server and wait for it, as every remote action does.
///
/// Returns the command id and its result. Any failure after the command was
/// queued — including losing the connection while waiting — carries the
/// command id, so the caller can follow it with `command-wait`.
/// `announce` prints `queued command_id=N` first in text mode.
pub fn submit_and_wait(
    cli: &Cli,
    base: &str,
    kind: &str,
    request: &serde_json::Value,
    announce: bool,
) -> Result<(i64, serde_json::Value)> {
    let timeout = remote_timeout(cli);
    let id = remote::enqueue_command(base, kind, &request.to_string(), Some("cli"), None, timeout)?;
    if announce && !cli.json {
        println!("queued command_id={id}");
    }
    let st = wait(id, timeout, POLL, || remote::get_command(base, id, timeout))
        .map_err(|e| output::with_command_id(e, id))?;
    let value = settle(&st)?;
    Ok((id, value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::CommandResult;

    fn status(state: CommandState, result: Option<CommandResult>) -> CommandStatus {
        CommandStatus {
            id: 5,
            kind: Some(CommandKind::Control),
            state,
            result,
        }
    }

    #[test]
    fn settle_parses_ok_payload() {
        let st = status(
            CommandState::Done,
            Some(CommandResult {
                command_id: 5,
                ok: true,
                result_json: Some(r#"{"message":"m","data":{"tag_id":3}}"#.into()),
                error: None,
            }),
        );
        assert_eq!(settle(&st).unwrap()["data"]["tag_id"], 3);
    }

    #[test]
    fn settle_takes_code_and_details_from_failure_payload() {
        let st = status(
            CommandState::Bad,
            Some(CommandResult {
                command_id: 5,
                ok: false,
                result_json: Some(r#"{"code":"not_found","message":"m","data":{"x":1}}"#.into()),
                error: Some("PLC 'a' not found".into()),
            }),
        );
        let e = settle(&st).unwrap_err();
        assert_eq!(e.code, ErrorCode::NotFound);
        assert_eq!(e.message, "PLC 'a' not found");
        assert_eq!(e.command_id, Some(5));
        assert_eq!(e.details.unwrap()["x"], 1);
    }

    #[test]
    fn settle_without_code_falls_back_by_kind() {
        let mut st = status(
            CommandState::Bad,
            Some(CommandResult {
                command_id: 5,
                ok: false,
                result_json: None,
                error: Some("connection refused".into()),
            }),
        );
        assert_eq!(settle(&st).unwrap_err().code, ErrorCode::CommandFailed);
        st.kind = Some(CommandKind::PlcRead);
        assert_eq!(settle(&st).unwrap_err().code, ErrorCode::PlcError);
    }

    #[test]
    fn settle_reports_cancelled_and_pending() {
        assert_eq!(
            settle(&status(CommandState::Cancelled, None))
                .unwrap_err()
                .code,
            ErrorCode::Cancelled
        );
        let e = settle(&status(CommandState::Running, None)).unwrap_err();
        assert_eq!(e.code, ErrorCode::Pending);
        assert_eq!(e.state.as_deref(), Some("running"));
    }

    #[test]
    fn wait_times_out_with_command_id_and_state() {
        let e = wait(9, Duration::ZERO, Duration::ZERO, || {
            Ok(CommandStatus {
                id: 9,
                kind: None,
                state: CommandState::Running,
                result: None,
            })
        })
        .unwrap_err();
        let c = output::classify(&e);
        assert_eq!(c.code, ErrorCode::Timeout);
        assert_eq!(c.command_id, Some(9));
        assert_eq!(c.state.as_deref(), Some("running"));
    }
}
