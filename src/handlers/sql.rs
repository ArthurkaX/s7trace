//! `sql` / `admin-sql` command handlers.

use anyhow::Result;
use rusqlite::Connection;

use crate::cli;
use crate::cli::args::Cli;
use crate::cli::format::Format;
use crate::cli::remote::urlencode;
use crate::server_base;
use crate::types::*;

pub(crate) fn handle_sql(
    conn: &Connection,
    cli: &Cli,
    query: &str,
    fmt: &str,
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
        let resp = match cli::remote::http_get(&base, &url, timeout) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("error: {e}");
                return Ok(ExitCode::BadInput);
            }
        };
        let v: serde_json::Value = match serde_json::from_str(&resp) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("error parsing server response: {e}");
                return Ok(ExitCode::BadInput);
            }
        };
        // A rejected or interrupted query answers with {"error": ...}. Without
        // this the CLI printed an empty table and exited 0 — which is how a
        // /v1/sql that never came back looked like "no rows" in the field.
        if let Some(msg) = v["error"].as_str() {
            eprintln!("error: {msg}");
            return Ok(ExitCode::BadInput);
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
        let out_fmt = Format::from_str(fmt);
        match out_fmt {
            Format::Table => {
                println!("{}", columns.join("\t"));
                for row in &rows {
                    let vals: Vec<String> = row
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|v| match v {
                            serde_json::Value::String(s) => s.clone(),
                            serde_json::Value::Null => "NULL".to_string(),
                            other => other.to_string(),
                        })
                        .collect();
                    println!("{}", vals.join("\t"));
                }
            }
            Format::CompactJson => {
                cli::format::print_columnar_json(&columns, &rows);
            }
        }
        // The server stops at the row cap rather than failing, so say so —
        // otherwise a capped result reads as the complete answer.
        if v["truncated"].as_bool().unwrap_or(false) {
            eprintln!("note: output truncated at {limit} rows; raise --limit for more");
        }
        return Ok(ExitCode::Ok);
    }

    let trimmed = query.trim().to_uppercase();
    if !trimmed.starts_with("SELECT")
        && !trimmed.starts_with("WITH")
        && !trimmed.starts_with("PRAGMA")
    {
        eprintln!("error: only SELECT/WITH/PRAGMA allowed in sql mode");
        return Ok(ExitCode::BadInput);
    }

    // Apply default limit if query doesn't have one
    let limited_query = if trimmed.contains(" LIMIT ") {
        query.to_string()
    } else {
        format!("{query} LIMIT {limit}")
    };

    let out_fmt = Format::from_str(fmt);
    let mut stmt = match conn.prepare(&limited_query) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("sql error: {e}");
            return Ok(ExitCode::BadInput);
        }
    };

    let col_count = stmt.column_count();
    let col_names: Vec<String> = (0..col_count)
        .map(|i| stmt.column_name(i).unwrap_or("?").to_string())
        .collect();

    let rows = match stmt.query_map([], |row| {
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
        Ok(values)
    }) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("sql error: {e}");
            return Ok(ExitCode::BadInput);
        }
    };

    let rows_as_values: Vec<serde_json::Value> = rows
        .filter_map(|r| r.ok())
        .map(serde_json::Value::Array)
        .collect();

    match out_fmt {
        Format::Table => {
            println!("{}", col_names.join("\t"));
            for row in &rows_as_values {
                if let serde_json::Value::Array(arr) = row {
                    let vals: Vec<String> = arr
                        .iter()
                        .map(|v| match v {
                            serde_json::Value::String(s) => s.clone(),
                            serde_json::Value::Null => "NULL".to_string(),
                            other => other.to_string(),
                        })
                        .collect();
                    println!("{}", vals.join("\t"));
                }
            }
        }
        Format::CompactJson => {
            cli::format::print_columnar_json(&col_names, &rows_as_values);
        }
    }

    Ok(ExitCode::Ok)
}

pub(crate) fn handle_admin_sql(cli: &Cli, conn: &Connection, query: &str) -> Result<ExitCode> {
    if cli.remote_server.is_some() {
        eprintln!(
            "error: admin-sql is local-only and was not run. Run it on the server host with the server --db path, or use a dedicated s7trace remote command. Do not use curl/direct HTTP SQL."
        );
        return Ok(ExitCode::BadInput);
    }

    match conn.execute_batch(query) {
        Ok(_) => {
            println!("ok rows_affected={}", conn.changes());
            Ok(ExitCode::Ok)
        }
        Err(e) => {
            eprintln!("admin sql error: {e}");
            Ok(ExitCode::BadInput)
        }
    }
}
