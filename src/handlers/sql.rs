//! `sql` / `admin-sql` command handlers.

use anyhow::Result;
use rusqlite::Connection;

use crate::cli;
use crate::cli::args::Cli;
use crate::cli::format::Format;
use crate::cli::output::fail;
use crate::cli::remote::{urlencode, RemoteError};
use crate::server_base;
use crate::types::*;

pub(crate) fn handle_sql(
    conn: &Connection,
    cli: &Cli,
    query: &str,
    fmt: Format,
    limit: u64,
    timeout_ms: u64,
) -> Result<ExitCode> {
    // Ensure read-only: reject anything that isn't a SELECT or WITH.
    // Remote mode uses the private HTTP transport, but the user interface
    // remains this CLI command.
    if let Some(base) = server_base(cli) {
        let encoded = urlencode(query);
        let url = format!("/v1/sql?query={encoded}&limit={limit}&timeout_ms={timeout_ms}");
        // Give the socket a little more than the query deadline, so a query the
        // server kills reports the server's own message rather than a read timeout.
        let timeout =
            std::time::Duration::from_millis(cli.remote_timeout_ms.max(timeout_ms + 2_000));
        // A rejected query is a 400, an interrupted one a 503; both carry the
        // server's message. Without this a /v1/sql that never came back looked
        // like "no rows" in the field.
        let resp = cli::remote::http_get(&base, &url, timeout).map_err(|e| {
            match e.downcast_ref::<RemoteError>() {
                Some(RemoteError::Http {
                    status: 400,
                    message,
                }) => fail(ErrorCode::SqlError, message.clone()),
                Some(RemoteError::Http {
                    status: 503,
                    message,
                }) => fail(ErrorCode::QueryTimeout, message.clone()),
                _ => e,
            }
        })?;
        let v: serde_json::Value = serde_json::from_str(&resp)
            .map_err(|e| RemoteError::Protocol(format!("invalid JSON from /v1/sql: {e}")))?;
        if let Some(msg) = v["error"].as_str() {
            return Err(fail(ErrorCode::SqlError, msg));
        }
        let columns: Vec<String> = v["columns"]
            .as_array()
            .map(|a| {
                a.iter()
                    .map(|x| x.as_str().unwrap_or("?").to_string())
                    .collect()
            })
            .unwrap_or_default();
        let rows = v["rows"].as_array().cloned().unwrap_or_default();
        // The server reports the row cap it actually applied (it may lower
        // ours); an older server does not, so fall back to the requested one.
        let applied_limit = v["limit"].as_u64().unwrap_or(limit);
        let truncated = v["truncated"].as_bool().unwrap_or(false);
        print_sql_result(cli, &columns, &rows, truncated, applied_limit, limit, fmt);
        return Ok(ExitCode::Ok);
    }

    let trimmed = query.trim().to_uppercase();
    if !trimmed.starts_with("SELECT")
        && !trimmed.starts_with("WITH")
        && !trimmed.starts_with("PRAGMA")
    {
        return Err(fail(
            ErrorCode::InvalidArgument,
            "only SELECT/WITH/PRAGMA allowed in sql mode",
        ));
    }

    let (cols, rows, truncated) = query_local(conn, query, limit)
        .map_err(|e| fail(ErrorCode::SqlError, format!("sql error: {e}")))?;
    print_sql_result(cli, &cols, &rows, truncated, limit, limit, fmt);
    Ok(ExitCode::Ok)
}

/// Run a read-only query against the local database, keeping at most `limit`
/// rows. Returns column names, rows (as JSON arrays) and whether the cap cut
/// the result short.
///
/// It stops at the cap instead of appending a LIMIT clause, as `/v1/sql` does:
/// the query text is left alone, its own LIMIT cannot widen the cap, and one
/// extra row tells whether there was more.
pub(crate) fn query_local(
    conn: &Connection,
    query: &str,
    limit: u64,
) -> rusqlite::Result<(Vec<String>, Vec<serde_json::Value>, bool)> {
    let mut stmt = conn.prepare(query)?;
    let col_count = stmt.column_count();
    let col_names: Vec<String> = (0..col_count)
        .map(|i| stmt.column_name(i).unwrap_or("?").to_string())
        .collect();

    let mut rows: Vec<serde_json::Value> = Vec::new();
    let mut truncated = false;
    let mut q = stmt.query([])?;
    while let Some(row) = q.next()? {
        if rows.len() as u64 >= limit {
            truncated = true;
            break;
        }
        let values: Vec<serde_json::Value> = (0..col_count)
            .map(|i| match row.get::<_, rusqlite::types::Value>(i) {
                Ok(v) => match v {
                    rusqlite::types::Value::Null => serde_json::Value::Null,
                    rusqlite::types::Value::Integer(n) => serde_json::Value::Number(n.into()),
                    rusqlite::types::Value::Real(f) => serde_json::Number::from_f64(f)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null),
                    rusqlite::types::Value::Text(s) => serde_json::Value::String(s),
                    rusqlite::types::Value::Blob(b) => {
                        serde_json::Value::String(format!("{b:02x?}"))
                    }
                },
                Err(_) => serde_json::Value::Null,
            })
            .collect();
        rows.push(serde_json::Value::Array(values));
    }
    Ok((col_names, rows, truncated))
}

/// Print a `sql` result in the requested format. JSON (and the `--json`
/// envelope) carries `truncated`, `limit` (the cap applied) and
/// `requested_limit` in the document itself; text and JSON Lines have no room
/// for them, so they get a note on stderr — otherwise a capped result reads as
/// complete.
fn print_sql_result(
    cli: &Cli,
    cols: &[String],
    rows: &[serde_json::Value],
    truncated: bool,
    limit: u64,
    requested_limit: u64,
    fmt: Format,
) {
    if cli.json {
        let result = serde_json::json!({
            "cols": cols,
            "rows": rows,
            "truncated": truncated,
            "limit": limit,
            "requested_limit": requested_limit,
        });
        cli::output::print_ok(&result, None);
        return;
    }
    match fmt {
        Format::Text => cli::format::print_table(cols, rows),
        Format::Json => cli::format::print_columnar_json(cols, rows, truncated, limit),
        Format::Jsonl => cli::format::print_jsonl_rows(cols, rows),
    }
    if truncated {
        eprintln!("{}", truncation_note(limit, requested_limit));
    }
}

/// What to tell the operator when the row cap cut a result short. When the
/// server lowered `--limit`, raising it again does nothing.
fn truncation_note(limit: u64, requested_limit: u64) -> String {
    if limit < requested_limit {
        format!(
            "note: result truncated at {limit} rows: the server caps sql at {limit} \
             (--limit {requested_limit} cannot raise it). Page by time or rowid, \
             or use `sample-export` for samples"
        )
    } else {
        format!("note: result truncated at {limit} rows; raise --limit for more")
    }
}

pub(crate) fn handle_admin_sql(cli: &Cli, conn: &Connection, query: &str) -> Result<ExitCode> {
    if cli.remote_server.is_some() {
        return Err(fail(
            ErrorCode::InvalidArgument,
            "admin-sql is local-only and was not run. Run it on the server host with the server --db path, or use a dedicated s7trace remote command. Do not use curl/direct HTTP SQL.",
        ));
    }

    conn.execute_batch(query)
        .map_err(|e| fail(ErrorCode::SqlError, format!("admin sql error: {e}")))?;
    let changes = conn.changes();
    cli::output::emit(
        cli,
        serde_json::json!({ "rows_affected": changes }),
        None,
        || println!("ok rows_affected={changes}"),
    );
    Ok(ExitCode::Ok)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_truncation_note_says_when_the_server_capped_the_limit() {
        assert!(truncation_note(200, 200).contains("raise --limit"));
        let capped = truncation_note(10_000, 100_000);
        assert!(capped.contains("cannot raise it"), "{capped}");
        assert!(capped.contains("sample-export"), "{capped}");
    }
}
