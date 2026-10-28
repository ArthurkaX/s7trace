//! The CLI's machine-readable contract.
//!
//! With `--json` (or `S7TRACE_JSON=1`) every client command prints exactly one
//! compact JSON line on stdout:
//!
//! ```text
//! {"ok":true,"result":<value>,"command_id":<id, queued commands only>}
//! {"ok":false,"error":{"code":"timeout","message":"...","retryable":true,
//!   "state":"running"},"command_id":42}
//! ```
//!
//! `error.code` is an [`ErrorCode`]; `retryable` says whether the same request
//! may succeed later unchanged (for `timeout`, by `command-wait <command_id>`,
//! not by resubmitting). Logs and notes stay on stderr.

use std::fmt;

use crate::cli::args::Cli;
use crate::cli::remote::RemoteError;
use crate::types::{CodedError, ErrorCode};

/// A classified CLI failure, as reported in the envelope's `error` object.
#[derive(Debug, Clone)]
pub struct CliError {
    pub code: ErrorCode,
    pub message: String,
    /// The queued command this failure belongs to, when there is one.
    pub command_id: Option<i64>,
    /// The command's last known state (`pending`, `running`, ...).
    pub state: Option<String>,
    /// Structured detail, e.g. the per-signal failures of `tag-import`.
    pub details: Option<serde_json::Value>,
}

impl CliError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            command_id: None,
            state: None,
            details: None,
        }
    }

    pub fn with_command(mut self, command_id: i64, state: Option<&str>) -> Self {
        self.command_id = Some(command_id);
        if let Some(st) = state {
            self.state = Some(st.to_string());
        }
        self
    }

    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        if !details.is_null() {
            self.details = Some(details);
        }
        self
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for CliError {}

/// Shorthand for returning a coded failure from a handler.
pub fn fail(code: ErrorCode, message: impl Into<String>) -> anyhow::Error {
    CliError::new(code, message).into()
}

/// Classify any error a handler returned. A [`CliError`], [`CodedError`] or
/// [`RemoteError`] anywhere in the chain decides the code; anything else is
/// `internal`.
pub fn classify(e: &anyhow::Error) -> CliError {
    for cause in e.chain() {
        if let Some(c) = cause.downcast_ref::<CliError>() {
            return c.clone();
        }
        if let Some(c) = cause.downcast_ref::<CodedError>() {
            return CliError::new(c.code, format!("{e:#}"));
        }
        if let Some(r) = cause.downcast_ref::<RemoteError>() {
            return CliError::new(r.code(), format!("{e:#}"));
        }
    }
    CliError::new(ErrorCode::Internal, format!("{e:#}"))
}

/// Attach `command_id` to a failure that happened while following a queued
/// command, so the caller can still look the command up afterwards.
pub fn with_command_id(e: anyhow::Error, command_id: i64) -> anyhow::Error {
    let mut err = classify(&e);
    if err.command_id.is_none() {
        err.command_id = Some(command_id);
    }
    err.into()
}

pub fn ok_envelope(result: &serde_json::Value, command_id: Option<i64>) -> serde_json::Value {
    let mut obj = serde_json::json!({ "ok": true, "result": result });
    if let Some(id) = command_id {
        obj["command_id"] = id.into();
    }
    obj
}

pub fn error_envelope(err: &CliError) -> serde_json::Value {
    let mut error = serde_json::json!({
        "code": err.code,
        "message": err.message,
        "retryable": err.code.retryable(),
    });
    if let Some(st) = &err.state {
        error["state"] = st.clone().into();
    }
    if let Some(d) = &err.details {
        error["details"] = d.clone();
    }
    let mut obj = serde_json::json!({ "ok": false, "error": error });
    if let Some(id) = err.command_id {
        obj["command_id"] = id.into();
    }
    obj
}

pub fn print_ok(result: &serde_json::Value, command_id: Option<i64>) {
    println!("{}", ok_envelope(result, command_id));
}

/// Print a success: the envelope in `--json` mode, otherwise the human text.
pub fn emit(cli: &Cli, result: serde_json::Value, command_id: Option<i64>, text: impl FnOnce()) {
    if cli.json {
        print_ok(&result, command_id);
    } else {
        text();
    }
}

/// Report a failure: the envelope on stdout for machine output, otherwise a
/// message on stderr (with how to follow a still-queued command).
pub fn report_error(err: &CliError, machine: bool) {
    if machine {
        println!("{}", error_envelope(err));
        return;
    }
    eprintln!("error: {}", err.message);
    if let Some(id) = err.command_id {
        let state = err
            .state
            .as_deref()
            .map(|s| format!(" (state {s})"))
            .unwrap_or_default();
        eprintln!("  command_id={id}{state}");
        if err.code == ErrorCode::Timeout {
            eprintln!("  it may still run: follow it with `s7trace -r <server> command-wait {id}`");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_envelope_carries_code_retryable_and_command_id() {
        let err = CliError::new(ErrorCode::Timeout, "command 7 still running")
            .with_command(7, Some("running"));
        let v = error_envelope(&err);
        assert_eq!(v["ok"], false);
        assert_eq!(v["error"]["code"], "timeout");
        assert_eq!(v["error"]["retryable"], true);
        assert_eq!(v["error"]["state"], "running");
        assert_eq!(v["command_id"], 7);
    }

    #[test]
    fn ok_envelope_omits_absent_command_id() {
        let v = ok_envelope(&serde_json::json!({"x": 1}), None);
        assert_eq!(v, serde_json::json!({"ok": true, "result": {"x": 1}}));
    }

    #[test]
    fn classify_finds_codes_through_context() {
        use anyhow::Context;
        let e: anyhow::Error = Err::<(), _>(crate::types::coded(ErrorCode::NotFound, "no PLC"))
            .context("resolving target")
            .unwrap_err();
        assert_eq!(classify(&e).code, ErrorCode::NotFound);

        let e: anyhow::Error = RemoteError::Unavailable("refused".into()).into();
        let c = classify(&e);
        assert_eq!(c.code, ErrorCode::ServerUnavailable);
        assert!(c.code.retryable());

        let e = anyhow::anyhow!("boom");
        assert_eq!(classify(&e).code, ErrorCode::Internal);
    }

    #[test]
    fn with_command_id_keeps_the_code() {
        let e: anyhow::Error = RemoteError::Unavailable("reset".into()).into();
        let c = classify(&with_command_id(e, 12));
        assert_eq!(c.code, ErrorCode::ServerUnavailable);
        assert_eq!(c.command_id, Some(12));
    }
}
