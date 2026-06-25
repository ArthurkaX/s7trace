use anyhow::{Context, Result};
use clap::{CommandFactory, Parser};
use rusqlite::Connection;

mod api;
mod cli;
mod db;
mod engine;
mod platform;
mod plc;
mod types;
mod version;

use cli::args::{Cli, Command, EventAction};
use cli::format::Format;
use cli::remote::urlencode;
use plc::create_plc_client;
use types::*;

fn main() -> Result<()> {
    std::thread::Builder::new()
        .name("s7trace-main".into())
        .stack_size(16 * 1024 * 1024)
        .spawn(run_main)
        .context("starting s7trace main thread")?
        .join()
        .map_err(|_| anyhow::anyhow!("s7trace main thread panicked"))?
}

fn run_main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_micros()
        .init();

    let cli = Cli::parse();

    match &cli.command {
        None => {
            eprintln!("error: no command specified. Use `s7trace server` explicitly, or choose a client target with `-r URL` or `--local-db`.");
            Cli::command().print_help()?;
            eprintln!();
            std::process::exit(ExitCode::BadInput as i32);
        }
        Some(Command::Server { dry_run: false, .. }) => {
            let (dry_run, http_bind, control_window, strict_overrun) = match &cli.command {
                Some(Command::Server {
                    dry_run,
                    http_bind,
                    control_window,
                    strict_overrun,
                }) => (
                    *dry_run,
                    http_bind.clone(),
                    *control_window,
                    *strict_overrun,
                ),
                _ => (false, None, false, false),
            };
            if dry_run {
                return dry_run_check(&cli.db);
            }
            dry_run_check(&cli.db)?;
            let overrun_mode = if strict_overrun {
                engine::poller::OverrunMode::Strict
            } else {
                engine::poller::OverrunMode::SkipMissedTicks
            };
            let config = engine::server::ServerConfig {
                db_path: cli.db.clone(),
                http_bind,
                control_window,
                overrun_mode,
            };
            engine::server::run_server_with_config(config)
        }
        Some(Command::Server { .. }) => dry_run_check(&cli.db),
        Some(_) => {
            validate_client_target(&cli)?;
            let conn = if cli.remote_server.is_some() {
                db::schema::open_memory()
                    .context("opening in-memory client placeholder database")?
            } else {
                db::schema::open(&cli.db)
                    .with_context(|| format!("opening database {} for s7trace", cli.db))?
            };
            let exit_code = dispatch(&cli, &conn)?;
            std::process::exit(exit_code as i32);
        }
    }
}

fn validate_client_target(cli: &Cli) -> Result<()> {
    let cmd = cli
        .command
        .as_ref()
        .expect("validate_client_target called without command");
    if cli.remote_server.is_none() && !cli.local_db {
        eprintln!(
            "error: client target not specified. Use `-r http://HOST:8080` for a server, or `--local-db` for direct local SQLite access."
        );
        std::process::exit(ExitCode::BadInput as i32);
    }
    if cli.remote_server.is_some() && !supports_remote(cmd) {
        eprintln!(
            "error: `{}` does not support remote mode. Use `--local-db` on the server host with the correct --db path, or use a dedicated supported remote command.",
            command_name(cmd)
        );
        std::process::exit(ExitCode::BadInput as i32);
    }
    Ok(())
}

fn supports_remote(cmd: &Command) -> bool {
    matches!(
        cmd,
        Command::Sql { .. }
            | Command::CommandCancel { .. }
            | Command::TagAdd { .. }
            | Command::TagUpdate { .. }
            | Command::TagDisable { .. }
            | Command::TagRevalidate { .. }
            | Command::Read { .. }
            | Command::ReadRaw { .. }
            | Command::Status { .. }
            | Command::SetCreate { .. }
            | Command::SetAddTag { .. }
            | Command::SetRemoveTag { .. }
            | Command::SetEnable { .. }
            | Command::SetDisable { .. }
            | Command::SetList { .. }
            | Command::SetShow { .. }
            | Command::CollectionStart { .. }
            | Command::CollectionStop { .. }
            | Command::PlcAdd { .. }
            | Command::PollGroupAdd { .. }
            | Command::TagImport { .. }
    )
}

fn command_name(cmd: &Command) -> &'static str {
    match cmd {
        Command::Sql { .. } => "sql",
        Command::AdminSql { .. } => "admin-sql",
        Command::CommandList { .. } => "command-list",
        Command::CommandWait { .. } => "command-wait",
        Command::CommandCancel { .. } => "command-cancel",
        Command::CommandResult { .. } => "command-result",
        Command::TagAdd { .. } => "tag-add",
        Command::TagUpdate { .. } => "tag-update",
        Command::TagDisable { .. } => "tag-disable",
        Command::TagRevalidate { .. } => "tag-revalidate",
        Command::Read { .. } => "read",
        Command::ReadRaw { .. } => "read-raw",
        Command::Status { .. } => "status",
        Command::SetCreate { .. } => "set-create",
        Command::SetAddTag { .. } => "set-add-tag",
        Command::SetRemoveTag { .. } => "set-remove-tag",
        Command::SetEnable { .. } => "set-enable",
        Command::SetDisable { .. } => "set-disable",
        Command::SetList { .. } => "set-list",
        Command::SetShow { .. } => "set-show",
        Command::CollectionStart { .. } => "collection-start",
        Command::CollectionStop { .. } => "collection-stop",
        Command::HistoryClean { .. } => "history-clean",
        Command::HistoryDeleteDataset { .. } => "history-delete-dataset",
        Command::HistoryVacuum { .. } => "history-vacuum",
        Command::PlcAdd { .. } => "plc-add",
        Command::PollGroupAdd { .. } => "poll-group-add",
        Command::TagImport { .. } => "tag-import",
        Command::Event { .. } => "event",
        Command::Note { .. } => "note",
        Command::Server { .. } => "server",
    }
}

/// Validate configuration and exit. Returns Ok(true) if check passed.
fn dry_run_check(db_path: &str) -> Result<()> {
    let conn = db::schema::open(db_path)?;
    let plc_count: i64 = conn.query_row("SELECT COUNT(*) FROM plc", [], |r| r.get(0))?;
    let pg_count: i64 = conn.query_row("SELECT COUNT(*) FROM poll_group", [], |r| r.get(0))?;
    let tag_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM tag WHERE enabled = 1 AND bad = 0",
        [],
        |r| r.get(0),
    )?;
    let set_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM metric_set WHERE enabled = 1",
        [],
        |r| r.get(0),
    )?;

    println!("Database: {db_path}");
    println!("  PLCs:         {plc_count}");
    println!("  Poll groups:  {pg_count}");
    println!("  Active tags:  {tag_count}");
    println!("  Enabled sets: {set_count}");

    if plc_count == 0 {
        eprintln!("warning: no PLCs configured. Add one with:");
        eprintln!("  s7trace plc-add --name my-plc --host 192.168.0.1:102 --rack 0 --slot 2");
    }
    if tag_count == 0 {
        eprintln!("warning: no active tags. The server will start no pollers.");
    }

    println!("Configuration looks valid.");
    Ok(())
}

fn dispatch(cli: &Cli, conn: &Connection) -> Result<ExitCode> {
    let cmd = cli
        .command
        .as_ref()
        .expect("dispatch called without command");
    match cmd {
        Command::Sql {
            query,
            format: fmt,
            limit,
            timeout_ms: _,
        } => handle_sql(conn, cli, query, fmt, *limit),
        Command::AdminSql { query } => handle_admin_sql(cli, conn, query),
        Command::CommandList { state, format: fmt } => {
            handle_command_list(conn, state.as_deref(), fmt)
        }
        Command::CommandWait {
            command_id,
            poll_ms,
            timeout_ms,
        } => handle_command_wait(conn, *command_id, *poll_ms, *timeout_ms),
        Command::CommandResult {
            command_id,
            format: fmt,
        } => handle_command_result(conn, *command_id, fmt),
        Command::CommandCancel { command_id } => handle_command_cancel(conn, cli, *command_id),
        Command::TagAdd {
            plc,
            name,
            address,
            data_type,
            poll_group,
            store_mode,
            storage_hook,
            value_role,
            metric_set,
            async_mode,
        } => handle_tag_add(
            cli,
            conn,
            plc,
            name,
            address,
            data_type,
            poll_group,
            store_mode,
            storage_hook,
            value_role,
            metric_set.as_deref(),
            *async_mode,
        ),
        Command::TagUpdate {
            tag_id,
            plc,
            name,
            address,
            data_type,
            poll_group,
            store_mode,
            storage_hook,
        } => handle_tag_update(
            conn,
            cli,
            *tag_id,
            plc.as_deref(),
            name.as_deref(),
            address.as_deref(),
            data_type.as_deref(),
            poll_group.as_deref(),
            store_mode.as_deref(),
            storage_hook.as_deref(),
        ),
        Command::TagDisable { tag_id, plc, name } => {
            handle_tag_disable(conn, cli, *tag_id, plc.as_deref(), name.as_deref())
        }
        Command::TagRevalidate { tag_id, plc, name } => {
            handle_tag_revalidate(conn, cli, *tag_id, plc.as_deref(), name.as_deref())
        }
        Command::Read {
            plc,
            address,
            data_type,
            format: fmt,
            no_wait,
        } => handle_read(cli, conn, plc, address, data_type, fmt, *no_wait),
        Command::ReadRaw {
            plc,
            db,
            start,
            len,
            format: fmt,
            save_probe,
            no_wait,
        } => handle_read_raw(
            conn,
            cli,
            plc,
            *db,
            *start,
            *len,
            fmt,
            *save_probe,
            *no_wait,
        ),
        Command::Status { format: fmt } => handle_status(cli, conn, fmt),
        Command::SetCreate {
            plc,
            name,
            description,
        } => handle_set_create(cli, conn, plc, name, description.as_deref()),
        Command::SetAddTag {
            set_name,
            tag,
            from_sql,
            from_file,
        } => handle_set_add_tag(
            conn,
            cli,
            set_name,
            tag.as_deref(),
            from_sql.as_deref(),
            from_file.as_deref(),
        ),
        Command::SetRemoveTag {
            set_name,
            tag,
            from_sql,
        } => handle_set_remove_tag(conn, cli, set_name, tag.as_deref(), from_sql.as_deref()),
        Command::SetEnable {
            set_name,
            async_mode,
        } => handle_set_enable(conn, cli, set_name, *async_mode),
        Command::SetDisable { set_name } => handle_set_disable(conn, cli, set_name),
        Command::SetList { plc } => handle_set_list(cli, conn, plc.as_deref()),
        Command::SetShow { set_name, set_flag } => {
            handle_set_show(conn, cli, set_name.as_deref(), set_flag.as_deref())
        }
        Command::CollectionStart {
            plc,
            dataset,
            sets,
            no_wait,
        } => handle_collection_start(conn, cli, plc, dataset, sets, *no_wait),
        Command::CollectionStop { plc, no_wait } => {
            handle_collection_stop(conn, cli, plc, *no_wait)
        }
        Command::HistoryClean {
            plc,
            before_ts,
            before,
            batch_size,
        } => handle_history_clean(conn, cli, plc, *before_ts, before.as_deref(), *batch_size),
        Command::HistoryDeleteDataset { dataset } => {
            handle_history_delete_dataset(conn, cli, dataset)
        }
        Command::HistoryVacuum { force } => handle_history_vacuum(conn, *force),
        Command::PlcAdd {
            name,
            host,
            rack,
            slot,
            disabled,
        } => handle_plc_add(cli, conn, name, host, *rack, *slot, *disabled),
        Command::PollGroupAdd { name, period_ms } => {
            handle_poll_group_add(cli, conn, name, *period_ms)
        }
        Command::Event { action } => handle_event_add(conn, cli, action),
        Command::Note { text, plc, dataset } => handle_event_add(
            conn,
            cli,
            &EventAction::Add {
                text: text.clone(),
                kind: "note".to_string(),
                severity: "info".to_string(),
                plc: plc.clone(),
                dataset: dataset.clone(),
                cycle_id: None,
            },
        ),
        Command::TagImport {
            plc,
            file,
            set,
            poll_group,
            store_mode,
            storage_hook,
            value_role,
            validate,
        } => handle_tag_import(
            cli,
            conn,
            plc,
            file,
            set.as_deref(),
            poll_group,
            store_mode,
            storage_hook,
            value_role,
            *validate,
        ),
        _ => unreachable!("dispatch called with Server or unknown command"),
    }
}

// ---------------------------------------------------------------------------
// SQL handlers
// ---------------------------------------------------------------------------

fn handle_sql(conn: &Connection, cli: &Cli, query: &str, fmt: &str, limit: u64) -> Result<ExitCode> {
    // Ensure read-only: reject anything that isn't a SELECT or WITH.
    // Remote mode uses the private HTTP transport, but the user interface
    // remains this CLI command.
    if let Some(base) = &cli.remote_server {
        let encoded = urlencode(query);
        let url = format!("/v1/sql?query={}&limit={}", encoded, limit);
        let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);
        let resp = match cli::remote::http_get(base, &url, timeout) {
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
        let columns: Vec<String> = v["columns"].as_array()
            .map(|a| a.iter().map(|x| x.as_str().unwrap_or("?").to_string()).collect())
            .unwrap_or_default();
        let rows = v["rows"].as_array().cloned().unwrap_or_default();
        let out_fmt = Format::from_str(fmt);
        match out_fmt {
            Format::Table => {
                println!("{}", columns.join("\t"));
                for row in &rows {
                    let vals: Vec<String> = row.as_array().unwrap().iter().map(|v| match v {
                        serde_json::Value::String(s) => s.clone(),
                        serde_json::Value::Null => "NULL".to_string(),
                        other => other.to_string(),
                    }).collect();
                    println!("{}", vals.join("\t"));
                }
            }
            Format::Json => {
                println!("{}", serde_json::to_string_pretty(&rows).unwrap_or_default());
            }
            _ => {
                for row in &rows {
                    let vals: Vec<String> = row.as_array().unwrap().iter().map(|v| match v {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    }).collect();
                    println!("{}", vals.join(" "));
                }
            }
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

    match out_fmt {
        Format::Table => {
            // Print header
            println!("{}", col_names.join("\t"));
            for row in rows {
                let row = row?;
                let vals: Vec<String> = row
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
        Format::Json => {
            let results: Vec<Vec<serde_json::Value>> = rows.filter_map(|r| r.ok()).collect();
            println!(
                "{}",
                serde_json::to_string_pretty(&results).unwrap_or_default()
            );
        }
        Format::Jsonl => {
            for row in rows {
                if let Ok(values) = row {
                    let obj: serde_json::Value = serde_json::json!({
                        "row": values,
                    });
                    println!("{}", serde_json::to_string(&obj).unwrap_or_default());
                }
            }
        }
        Format::Text => {
            for row in rows {
                let row = row?;
                let vals: Vec<String> = row
                    .iter()
                    .map(|v| match v {
                        serde_json::Value::String(s) => s.clone(),
                        _ => v.to_string(),
                    })
                    .collect();
                println!("{}", vals.join(" "));
            }
        }
    }

    Ok(ExitCode::Ok)
}

fn handle_admin_sql(cli: &Cli, conn: &Connection, query: &str) -> Result<ExitCode> {
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

// ---------------------------------------------------------------------------
// Command queue handlers
// ---------------------------------------------------------------------------

fn handle_command_list(conn: &Connection, state: Option<&str>, fmt: &str) -> Result<ExitCode> {
    let commands = db::command_queue::list_pending(conn, state)?;
    let out_fmt = Format::from_str(fmt);
    match out_fmt {
        Format::Text | Format::Table => {
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
        _ => {
            cli::format::print_result(out_fmt, "commands", &commands);
        }
    }
    Ok(ExitCode::Ok)
}

/// Format a microsecond timestamp as a human-readable UTC string.
fn fmt_ts(us: i64) -> String {
    let secs = us / 1_000_000;
    let nsec = ((us % 1_000_000) * 1000) as u32;
    chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nsec)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| us.to_string())
}

fn wait_and_print(
    conn: &Connection,
    command_id: i64,
    poll_ms: u64,
    timeout_ms: u64,
) -> Result<ExitCode> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
    let poll_dur = std::time::Duration::from_millis(poll_ms);

    while std::time::Instant::now() < deadline {
        if let Some(result) = db::command_queue::get_result(conn, command_id)? {
            handle_command_result_inner(&result, &Format::Text)?;
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

/// Forward a `control` command to a remote server, block until it finishes,
/// and print the result. Shared by the remote branches of CLI handlers.
fn forward_control(
    base: &str,
    req: &serde_json::Value,
    timeout: std::time::Duration,
) -> Result<ExitCode> {
    let id = cli::remote::enqueue_command(
        base,
        "control",
        &req.to_string(),
        Some("cli"),
        None,
        timeout,
    )?;
    println!("queued command_id={id}");
    let (state, result, ok, err) =
        cli::remote::wait_command(base, id, timeout, std::time::Duration::from_millis(200))?;
    if ok {
        println!("success {result}");
        Ok(ExitCode::Ok)
    } else {
        println!("{state}: {result} {err}");
        Ok(ExitCode::BadInput)
    }
}

fn handle_command_wait(
    conn: &Connection,
    command_id: i64,
    poll_ms: u64,
    timeout_ms: u64,
) -> Result<ExitCode> {
    wait_and_print(conn, command_id, poll_ms, timeout_ms)
}

fn handle_command_result(conn: &Connection, command_id: i64, fmt: &str) -> Result<ExitCode> {
    let out_fmt = Format::from_str(fmt);
    match db::command_queue::get_result(conn, command_id)? {
        Some(result) => handle_command_result_inner(&result, &out_fmt),
        None => {
            eprintln!("command {command_id}: no result yet (still pending or unknown)");
            Ok(ExitCode::Overload)
        }
    }
}

fn handle_command_cancel(conn: &Connection, cli: &Cli, command_id: i64) -> Result<ExitCode> {
    // Remote mode
    if let Some(base) = &cli.remote_server {
        let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);
        cli::remote::cancel_command(base, command_id, timeout)?;
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

fn handle_command_result_inner(result: &CommandResult, fmt: &Format) -> Result<ExitCode> {
    match fmt {
        Format::Text => {
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
        _ => {
            cli::format::print_result(*fmt, "result", result);
        }
    }
    Ok(if result.ok {
        ExitCode::Ok
    } else {
        ExitCode::PlcError
    })
}

// ---------------------------------------------------------------------------
// Tag handlers
// ---------------------------------------------------------------------------

fn handle_tag_add(
    cli: &Cli,
    conn: &Connection,
    plc: &str,
    name: &str,
    address: &str,
    data_type: &str,
    poll_group: &str,
    store_mode: &str,
    storage_hook: &str,
    value_role: &str,
    metric_set: Option<&str>,
    async_mode: bool,
) -> Result<ExitCode> {
    if name.trim().is_empty() {
        eprintln!("error: --name cannot be empty");
        return Ok(ExitCode::BadInput);
    }
    let dt: DataType = data_type
        .parse()
        .map_err(|e: String| anyhow::anyhow!("{e}"))?;
    let _sm: StoreMode = store_mode
        .parse()
        .map_err(|e: String| anyhow::anyhow!("{e}"))?;
    let _sh: StorageHook = storage_hook
        .parse()
        .map_err(|e: String| anyhow::anyhow!("{e}"))?;
    let addr = S7Address::parse(address).map_err(|e| anyhow::anyhow!("invalid address: {e}"))?;

    // Remote mode: send tag_add_full to the server, which will insert +
    // validate + schedule apply on its side. CLI doesn't need PLC access.
    if let Some(base) = &cli.remote_server {
        let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);
        let req = serde_json::json!({
            "action": "tag_add_full",
            "plc": plc,
            "name": name,
            "address": address,
            "data_type": data_type,
            "poll_group": poll_group,
            "store_mode": store_mode,
            "storage_hook": storage_hook,
            "value_role": value_role,
            "metric_set": metric_set,
        });
        let id = cli::remote::enqueue_command(
            base,
            "control",
            &req.to_string(),
            Some("cli"),
            None,
            timeout,
        )?;
        println!("queued command_id={id}");
        let (state, result, ok, err) =
            cli::remote::wait_command(base, id, timeout, std::time::Duration::from_millis(200))?;
        if ok {
            println!("success {result}");
        } else {
            println!("{state}: {result} {err}");
            return Ok(ExitCode::BadInput);
        }
        return Ok(ExitCode::Ok);
    }

    let inserted = db::validation::insert_tag(
        conn,
        plc,
        name,
        &addr,
        dt,
        poll_group,
        store_mode,
        storage_hook,
        value_role,
        metric_set,
    )?;
    let tag_id = inserted.tag_id;
    let set_name = metric_set.unwrap_or("default").to_string();

    // Schedule delayed config apply
    let _ = db::config_apply::schedule(
        conn,
        ApplyScope::Tag,
        Some(inserted.plc_id),
        None,
        None,
        Some(tag_id),
        &format!("tag_add id={tag_id}"),
        db::config_apply::PLAN_COOLDOWN_MS,
        None,
    );

    if !async_mode {
        // Synchronous validation: read one sample from the PLC now and
        // mark the tag as good or bad before reporting back.
        let plc_row: (String, String, i64, i64) = conn
            .query_row(
                "SELECT host, name, rack, slot FROM plc WHERE id = ?1",
                rusqlite::params![inserted.plc_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .map_err(|_| anyhow::anyhow!("PLC '{plc}' not found"))?;
        let (host, plc_name, rack, slot) = plc_row;

        let client = create_plc_client(&plc_name, &host, rack as u16, slot as u16);
        let result = db::validation::validate_tag(conn, &client, tag_id)?;
        if result.ok {
            let val_str = if let Some(v) = result.value_i {
                if let Some(f) = result.value_f {
                    format!("value={v} float={f}")
                } else {
                    format!("value={v}")
                }
            } else if let Some(f) = result.value_f {
                format!("value={f}")
            } else {
                "value=null".to_string()
            };
            println!("success {val_str} apply_state=pending tag_id={tag_id} set={set_name}");
        } else {
            let bc = result
                .bad_class
                .as_ref()
                .map(|b| b.to_string())
                .unwrap_or_default();
            let det = result.detail.as_deref().unwrap_or("");
            println!("bad bad_class={bc} detail=\"{det}\" tag_id={tag_id} set={set_name}");
        }
    } else {
        // Enqueue as command
        let req = serde_json::json!({
            "action": "tag_add",
            "plc_id": inserted.plc_id,
            "tag_id": tag_id,
        });
        let cmd_id = db::command_queue::enqueue(
            conn,
            CommandKind::Control,
            &req.to_string(),
            Some(inserted.plc_id),
            None,
            None,
        )?;
        println!("queued command_id={cmd_id}");
    }

    Ok(ExitCode::Ok)
}

fn handle_tag_update(
    conn: &Connection,
    cli: &Cli,
    tag_id_opt: Option<i64>,
    plc: Option<&str>,
    name: Option<&str>,
    address: Option<&str>,
    data_type: Option<&str>,
    poll_group: Option<&str>,
    store_mode: Option<&str>,
    storage_hook: Option<&str>,
) -> Result<ExitCode> {
    // Remote mode: forward the resolution criteria (id, or plc+name) and the
    // changed fields; the server resolves the tag and applies the update.
    if let Some(base) = &cli.remote_server {
        let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);
        let req = serde_json::json!({
            "action": "tag_update",
            "tag_id": tag_id_opt,
            "plc": plc,
            "name": name,
            "address": address,
            "data_type": data_type,
            "poll_group": poll_group,
            "store_mode": store_mode,
            "storage_hook": storage_hook,
        });
        return forward_control(base, &req, timeout);
    }
    let tag_id = match (tag_id_opt, plc, name) {
        (Some(id), _, _) => id,
        (None, Some(plc), Some(name)) => {
            match conn.query_row(
                "SELECT t.id FROM tag t JOIN plc p ON p.id = t.plc_id WHERE p.name = ?1 AND t.name = ?2",
                rusqlite::params![plc, name],
                |row| row.get::<_, i64>(0),
            ) {
                Ok(id) => id,
                Err(_) => {
                    eprintln!("error: tag '{name}' not found on PLC '{plc}'");
                    return Ok(ExitCode::BadInput);
                }
            }
        }
        _ => {
            eprintln!("error: provide --id or both --plc and --name");
            return Ok(ExitCode::BadInput);
        }
    };
    let now = now_us();

    if let Some(addr_str) = address {
        let addr =
            S7Address::parse(addr_str).map_err(|e| anyhow::anyhow!("invalid address: {e}"))?;
        conn.execute(
            "UPDATE tag SET address = ?1, db_num = ?2, byte_offset = ?3, updated_ts = ?4, desired_version = desired_version + 1 WHERE id = ?5",
            rusqlite::params![addr_str, addr.db_num, addr.byte_offset, now, tag_id],
        )?;
    }

    if let Some(dt) = data_type {
        let byte_size = dt
            .parse::<DataType>()
            .map_err(|e: String| anyhow::anyhow!("{e}"))?
            .byte_size() as i64;
        conn.execute(
            "UPDATE tag SET data_type = ?1, byte_size = ?2, updated_ts = ?3, desired_version = desired_version + 1 WHERE id = ?4",
            rusqlite::params![dt, byte_size, now, tag_id],
        )?;
    }

    if let Some(pg) = poll_group {
        let pg_id: i64 = conn
            .query_row(
                "SELECT id FROM poll_group WHERE name = ?1",
                rusqlite::params![pg],
                |row| row.get(0),
            )
            .map_err(|_| anyhow::anyhow!("poll group '{pg}' not found"))?;
        conn.execute(
            "UPDATE tag SET poll_group_id = ?1, updated_ts = ?2, desired_version = desired_version + 1 WHERE id = ?3",
            rusqlite::params![pg_id, now, tag_id],
        )?;
    }

    if let Some(sm) = store_mode {
        conn.execute(
            "UPDATE tag SET store_mode = ?1, updated_ts = ?2, desired_version = desired_version + 1 WHERE id = ?3",
            rusqlite::params![sm, now, tag_id],
        )?;
    }

    if let Some(sh) = storage_hook {
        conn.execute(
            "UPDATE tag SET storage_hook = ?1, updated_ts = ?2, desired_version = desired_version + 1 WHERE id = ?3",
            rusqlite::params![sh, now, tag_id],
        )?;
    }

    // Schedule config apply
    db::config_apply::schedule(
        conn,
        ApplyScope::Tag,
        None,
        None,
        None,
        Some(tag_id),
        "tag_updated",
        db::config_apply::PLAN_COOLDOWN_MS,
        None,
    )?;

    println!("success tag_id={tag_id} apply_state=pending");
    Ok(ExitCode::Ok)
}

fn handle_tag_disable(
    conn: &Connection,
    cli: &Cli,
    tag_id_opt: Option<i64>,
    plc: Option<&str>,
    name: Option<&str>,
) -> Result<ExitCode> {
    if let Some(base) = &cli.remote_server {
        let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);
        let req = serde_json::json!({
            "action": "tag_disable",
            "tag_id": tag_id_opt,
            "plc": plc,
            "name": name,
        });
        return forward_control(base, &req, timeout);
    }
    let tag_id = match (tag_id_opt, plc, name) {
        (Some(id), _, _) => id,
        (None, Some(plc), Some(name)) => {
            match conn.query_row(
                "SELECT t.id FROM tag t JOIN plc p ON p.id = t.plc_id WHERE p.name = ?1 AND t.name = ?2",
                rusqlite::params![plc, name],
                |row| row.get::<_, i64>(0),
            ) {
                Ok(id) => id,
                Err(_) => {
                    eprintln!("error: tag '{name}' not found on PLC '{plc}'");
                    return Ok(ExitCode::BadInput);
                }
            }
        }
        _ => {
            eprintln!("error: provide --id or both --plc and --name");
            return Ok(ExitCode::BadInput);
        }
    };
    conn.execute(
        "UPDATE tag SET enabled = 0, updated_ts = ?1 WHERE id = ?2",
        rusqlite::params![now_us(), tag_id],
    )?;

    if conn.changes() == 0 {
        eprintln!("tag {tag_id}: not found");
        return Ok(ExitCode::BadInput);
    }

    db::config_apply::schedule(
        conn,
        ApplyScope::Tag,
        None,
        None,
        None,
        Some(tag_id),
        "tag_disabled",
        db::config_apply::PLAN_COOLDOWN_MS,
        None,
    )?;

    println!("success tag_id={tag_id} enabled=0 apply_state=pending");
    Ok(ExitCode::Ok)
}

fn handle_tag_revalidate(
    conn: &Connection,
    cli: &Cli,
    tag_id_opt: Option<i64>,
    plc: Option<&str>,
    name: Option<&str>,
) -> Result<ExitCode> {
    if let Some(base) = &cli.remote_server {
        let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);
        let req = serde_json::json!({
            "action": "tag_revalidate",
            "tag_id": tag_id_opt,
            "plc": plc,
            "name": name,
        });
        return forward_control(base, &req, timeout);
    }
    let tag_id = match (tag_id_opt, plc, name) {
        (Some(id), _, _) => id,
        (None, Some(plc), Some(name)) => {
            match conn.query_row(
                "SELECT t.id FROM tag t JOIN plc p ON p.id = t.plc_id WHERE p.name = ?1 AND t.name = ?2",
                rusqlite::params![plc, name],
                |row| row.get::<_, i64>(0),
            ) {
                Ok(id) => id,
                Err(_) => {
                    eprintln!("error: tag '{name}' not found on PLC '{plc}'");
                    return Ok(ExitCode::BadInput);
                }
            }
        }
        _ => {
            eprintln!("error: provide --id or both --plc and --name");
            return Ok(ExitCode::BadInput);
        }
    };
    // Enqueue a revalidation command
    let req = serde_json::json!({
        "action": "tag_revalidate",
        "tag_id": tag_id,
    });
    let cmd_id = db::command_queue::enqueue(
        conn,
        CommandKind::Control,
        &req.to_string(),
        None,
        None,
        None,
    )?;
    println!("queued command_id={cmd_id}");
    Ok(ExitCode::Ok)
}

// ---------------------------------------------------------------------------
// Read handlers
// ---------------------------------------------------------------------------

fn handle_read(
    cli: &Cli,
    conn: &Connection,
    plc: &str,
    address: &str,
    data_type: &str,
    fmt: &str,
    no_wait: bool,
) -> Result<ExitCode> {
    // Remote mode: send to server, wait, print result.
    if let Some(base) = &cli.remote_server {
        let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);
        let req = serde_json::json!({
            "action": "read",
            "plc": plc,
            "address": address,
            "data_type": data_type,
            "format": fmt,
        });
        let id = cli::remote::enqueue_command(
            base,
            "plc_read",
            &req.to_string(),
            Some("cli"),
            None,
            timeout,
        )?;
        println!("queued command_id={id}");
        if !no_wait {
            let (state, result, ok, err) = cli::remote::wait_command(
                base,
                id,
                timeout,
                std::time::Duration::from_millis(200),
            )?;
            if ok {
                println!("{result}");
            } else {
                eprintln!("{state}: {result} {err}");
                return Ok(ExitCode::BadInput);
            }
        }
        return Ok(ExitCode::Ok);
    }

    // Local mode: enqueue a PLC read command (server must be running)
    let req = serde_json::json!({
        "action": "read",
        "plc": plc,
        "address": address,
        "data_type": data_type,
        "format": fmt,
    });
    let cmd_id = db::command_queue::enqueue(
        conn,
        CommandKind::PlcRead,
        &req.to_string(),
        None,
        None,
        None,
    )?;
    println!("queued command_id={cmd_id}");
    if !no_wait {
        return wait_and_print(conn, cmd_id, 200, 10_000);
    }
    println!("info: use 's7trace command wait {cmd_id}' or 's7trace command result {cmd_id}' to get the result");
    Ok(ExitCode::Ok)
}

fn handle_read_raw(
    conn: &Connection,
    _cli: &Cli,
    plc: &str,
    db: u16,
    start: usize,
    len: usize,
    fmt: &str,
    save_probe: bool,
    no_wait: bool,
) -> Result<ExitCode> {
    let req = serde_json::json!({
        "action": "read_raw",
        "plc": plc,
        "db": db,
        "start": start,
        "len": len,
        "format": fmt,
        "save_probe": save_probe,
    });
    let cmd_id = db::command_queue::enqueue(
        conn,
        CommandKind::PlcRead,
        &req.to_string(),
        None,
        None,
        None,
    )?;
    println!("queued command_id={cmd_id}");
    if save_probe {
        println!("info: raw probe will be saved to the database");
    }
    if !no_wait {
        return wait_and_print(conn, cmd_id, 200, 10_000);
    }
    println!("info: use 's7trace command wait {cmd_id}' to get the result");
    Ok(ExitCode::Ok)
}

// ---------------------------------------------------------------------------
// Status handler
// ---------------------------------------------------------------------------

fn handle_status(cli: &Cli, conn: &Connection, fmt: &str) -> Result<ExitCode> {
    // Remote mode: fetch JSON from server and pretty-print.
    if let Some(base) = &cli.remote_server {
        let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);
        let v = cli::remote::get_status(base, timeout)?;
        if fmt == "json" || fmt == "jsonl" {
            println!("{}", serde_json::to_string_pretty(&v)?);
        } else {
            // text mode: try to read the same text format we produce locally
            // by reading the local DB. If that's unavailable, fall back to JSON.
            println!("(remote server: {base})");
            println!("{}", serde_json::to_string_pretty(&v)?);
        }
        return Ok(ExitCode::Ok);
    }
    let out_fmt = Format::from_str(fmt);

    // Gather PLCs
    let mut stmt = conn
        .prepare("SELECT id, name, host, enabled, last_ok_ts, last_error, connectivity FROM plc")?;
    let plcs: Vec<serde_json::Value> = stmt
        .query_map([], |row| {
            let id: i64 = row.get(0)?;
            let name: String = row.get(1)?;
            let host: String = row.get(2)?;
            let enabled: bool = row.get::<_, i64>(3)? != 0;
            let last_ok_ts: Option<i64> = row.get(4)?;
            let last_error: Option<String> = row.get(5)?;
            let connectivity: String = row.get(6)?;

            // Active datasets for this PLC
            let mut ds_stmt = conn.prepare(
                "SELECT name, started_ts FROM dataset WHERE plc_id = ?1 AND state = 'active'",
            )?;
            let datasets: Vec<serde_json::Value> = ds_stmt
                .query_map(rusqlite::params![id], |r| {
                    let n: String = r.get(0)?;
                    let s: Option<i64> = r.get(1)?;
                    Ok(serde_json::json!({"name": n, "started_ts": s}))
                })?
                .filter_map(|x| x.ok())
                .collect();

            // Per-(plc, poll_group) most recent cycle metrics
            let mut pg_stmt = conn.prepare(
                "SELECT DISTINCT pg.id, pg.name, pg.period_ms,
                        (SELECT actual_ms FROM poll_cycle pc
                         WHERE pc.plc_id = ?1 AND pc.poll_group_id = pg.id
                         ORDER BY pc.id DESC LIMIT 1),
                        (SELECT COUNT(*) FROM poll_cycle pc
                         WHERE pc.plc_id = ?1 AND pc.poll_group_id = pg.id
                           AND pc.actual_ms > pg.period_ms)
                 FROM poll_group pg
                 JOIN tag t ON t.poll_group_id = pg.id
                 WHERE t.plc_id = ?1
                 ORDER BY pg.id",
            )?;
            let poll_groups: Vec<serde_json::Value> = pg_stmt
                .query_map(rusqlite::params![id], |r| {
                    let n: String = r.get(1)?;
                    let target: i64 = r.get(2)?;
                    let actual: Option<f64> = r.get(3)?;
                    let overruns: i64 = r.get(4)?;
                    Ok(serde_json::json!({
                        "name": n,
                        "target_ms": target,
                        "actual_ms": actual,
                        "overrun_count": overruns,
                    }))
                })?
                .filter_map(|x| x.ok())
                .collect();

            // Metric sets with member counts
            let mut ms_stmt = conn.prepare(
                "SELECT ms.name, ms.enabled,
                        (SELECT COUNT(*) FROM metric_set_tag mst WHERE mst.metric_set_id = ms.id),
                        (SELECT COUNT(*) FROM metric_set_tag mst
                         JOIN tag t ON t.id = mst.tag_id
                         WHERE mst.metric_set_id = ms.id AND t.bad = 0 AND t.enabled = 1)
                 FROM metric_set ms
                 WHERE ms.plc_id = ?1
                 ORDER BY ms.id",
            )?;
            let metric_sets: Vec<serde_json::Value> = ms_stmt
                .query_map(rusqlite::params![id], |r| {
                    let n: String = r.get(0)?;
                    let en: bool = r.get::<_, i64>(1)? != 0;
                    let total: i64 = r.get(2)?;
                    let healthy: i64 = r.get(3)?;
                    Ok(serde_json::json!({
                        "name": n,
                        "enabled": en,
                        "tags_total": total,
                        "tags_healthy": healthy,
                    }))
                })?
                .filter_map(|x| x.ok())
                .collect();

            Ok(serde_json::json!({
                "name": name,
                "host": host,
                "enabled": enabled,
                "last_ok_ts": last_ok_ts,
                "last_error": last_error,
                "connectivity": connectivity,
                "datasets": datasets,
                "poll_groups": poll_groups,
                "metric_sets": metric_sets,
            }))
        })?
        .filter_map(|r| r.ok())
        .collect();

    let bad_tags: i64 = conn
        .query_row("SELECT COUNT(*) FROM tag WHERE bad = 1", [], |row| {
            row.get(0)
        })
        .unwrap_or(0);

    let active_tags: i64 = conn
        .query_row("SELECT COUNT(*) FROM v_active_tag", [], |row| row.get(0))
        .unwrap_or(0);

    let pending_count = db::config_apply::pending_count(conn).unwrap_or(0);
    let command_pending: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM command_request WHERE state = 'pending'",
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);
    let command_running: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM command_request WHERE state = 'running'",
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);

    let status = serde_json::json!({
        "service": "s7trace",
        "version": version::status_json(conn),
        "plcs": plcs,
        "tags": {
            "active": active_tags,
            "bad": bad_tags,
        },
        "command_queue": {
            "pending": command_pending,
            "running": command_running,
        },
        "config_apply": {
            "pending": pending_count,
        },
    });

    match out_fmt {
        Format::Text | Format::Table => {
            let schema_version = version::schema_version(conn).unwrap_or_else(|| "unknown".into());
            println!(
                "s7trace version={} schema={} api={}",
                version::PACKAGE_VERSION,
                schema_version,
                version::API_VERSION
            );
            println!("PLCs:");
            for plc in &plcs {
                println!(
                    "  {} (host={}, enabled={}, conn={}) last_ok={:?}",
                    plc["name"],
                    plc["host"],
                    plc["enabled"],
                    plc["connectivity"],
                    plc["last_ok_ts"]
                );
                if let Some(err) = plc["last_error"].as_str() {
                    if !err.is_empty() {
                        println!("    last_error: {err}");
                    }
                }
                for pg in plc["poll_groups"].as_array().unwrap_or(&Vec::new()) {
                    let actual = pg["actual_ms"]
                        .as_f64()
                        .map(|f| format!("{f:.1}"))
                        .unwrap_or_else(|| "-".into());
                    println!(
                        "    poll_group {} target={}ms actual={}ms overruns={}",
                        pg["name"], pg["target_ms"], actual, pg["overrun_count"]
                    );
                }
                for ms in plc["metric_sets"].as_array().unwrap_or(&Vec::new()) {
                    println!(
                        "    metric_set {} enabled={} tags_total={} tags_healthy={}",
                        ms["name"], ms["enabled"], ms["tags_total"], ms["tags_healthy"]
                    );
                }
                for ds in plc["datasets"].as_array().unwrap_or(&Vec::new()) {
                    println!("    dataset {} started={:?}", ds["name"], ds["started_ts"]);
                }
            }
            println!("Tags: active={active_tags}, bad={bad_tags}");
            println!("Command queue pending={command_pending} running={command_running}");
            println!("Config apply pending: {pending_count}");
        }
        _ => {
            println!(
                "{}",
                serde_json::to_string_pretty(&status).unwrap_or_default()
            );
        }
    }

    // Show problems (bad tags)
    let mut pstmt = conn.prepare(
        "SELECT t.name, t.address, t.bad_class, t.bad_detail
         FROM tag t WHERE t.bad = 1 LIMIT 20",
    )?;
    let has_problems = pstmt.exists([])?;
    if has_problems {
        println!("\nProblems:");
        let problems = pstmt.query_map([], |row| {
            let name: String = row.get(0)?;
            let address: String = row.get(1)?;
            let bc: Option<String> = row.get(2)?;
            let bd: Option<String> = row.get(3)?;
            Ok((name, address, bc, bd))
        })?;
        for p in problems {
            if let Ok((name, addr, bc, bd)) = p {
                let detail = bd.unwrap_or_default();
                println!("  bad_tag: {name} ({addr}) class={bc:?} detail=\"{detail}\"");
            }
        }
    }

    Ok(ExitCode::Ok)
}

// ---------------------------------------------------------------------------
// Metric set handlers
// ---------------------------------------------------------------------------

fn handle_set_create(
    cli: &Cli,
    conn: &Connection,
    plc: &str,
    name: &str,
    description: Option<&str>,
) -> Result<ExitCode> {
    // Remote mode
    if let Some(base) = &cli.remote_server {
        let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);
        let req = serde_json::json!({
            "action": "set_create",
            "plc": plc,
            "name": name,
            "description": description,
        });
        let id = cli::remote::enqueue_command(
            base,
            "control",
            &req.to_string(),
            Some("cli"),
            None,
            timeout,
        )?;
        println!("queued command_id={id}");
        let (state, result, ok, err) =
            cli::remote::wait_command(base, id, timeout, std::time::Duration::from_millis(200))?;
        if ok {
            println!("success {result}");
        } else {
            println!("{state}: {result} {err}");
            return Ok(ExitCode::BadInput);
        }
        return Ok(ExitCode::Ok);
    }

    let plc_id: i64 = conn
        .query_row(
            "SELECT id FROM plc WHERE name = ?1",
            rusqlite::params![plc],
            |row| row.get(0),
        )
        .map_err(|_| anyhow::anyhow!("PLC '{plc}' not found"))?;

    let now = now_us();
    conn.execute(
        "INSERT INTO metric_set (plc_id, name, enabled, description, created_ts, updated_ts)
         VALUES (?1, ?2, 0, ?3, ?4, ?4)",
        rusqlite::params![plc_id, name, description, now],
    )?;

    let id = conn.last_insert_rowid();
    println!("success set_id={id} name={name} plc={plc}");
    Ok(ExitCode::Ok)
}

fn handle_set_add_tag(
    conn: &Connection,
    cli: &Cli,
    set_name: &str,
    tag: Option<&str>,
    from_sql: Option<&str>,
    from_file: Option<&str>,
) -> Result<ExitCode> {
    // Remote mode
    if let Some(base) = &cli.remote_server {
        let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);

        if from_sql.is_some() {
            return Err(anyhow::anyhow!("--from-sql not supported in remote mode"));
        }

        // Resolve from_file client-side into a comma list
        let tag_str = if let Some(path) = from_file {
            let content =
                std::fs::read_to_string(path).map_err(|e| anyhow::anyhow!("reading {path}: {e}"))?;
            let names: Vec<&str> = content
                .lines()
                .map(|l| l.trim())
                .filter(|l| !l.is_empty() && !l.starts_with('#'))
                .collect();
            if names.is_empty() {
                return Err(anyhow::anyhow!("no tag names found in {path}"));
            }
            Some(names.join(","))
        } else {
            tag.map(|s| s.to_string())
        };

        let tag_str =
            tag_str.ok_or_else(|| anyhow::anyhow!("provide --tag or --from-file"))?;

        let req = serde_json::json!({
            "action": "set_add_tag",
            "set_name": set_name,
            "tag": tag_str,
        });
        let id = cli::remote::enqueue_command(
            base,
            "control",
            &req.to_string(),
            Some("cli"),
            None,
            timeout,
        )?;
        println!("queued command_id={id}");
        let (state, result, ok, err) =
            cli::remote::wait_command(base, id, timeout, std::time::Duration::from_millis(200))?;
        if ok {
            println!("success {result}");
        } else {
            println!("{state}: {result} {err}");
            return Ok(ExitCode::BadInput);
        }
        return Ok(ExitCode::Ok);
    }

    // Resolve set
    let set: (i64, i64) = conn
        .query_row(
            "SELECT ms.id, ms.plc_id FROM metric_set ms WHERE ms.name = ?1",
            rusqlite::params![set_name],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|_| anyhow::anyhow!("metric set '{set_name}' not found"))?;
    let (set_id, plc_id) = set;

    let now = now_us();

    // Build the list of tag IDs to add, in priority order: file > sql > tag.
    let mut tag_ids: Vec<i64> = Vec::new();

    if let Some(path) = from_file {
        let content =
            std::fs::read_to_string(path).map_err(|e| anyhow::anyhow!("reading {path}: {e}"))?;
        for line in content.lines() {
            let name = line.trim();
            if name.is_empty() || name.starts_with('#') {
                continue;
            }
            let id: i64 = conn
                .query_row(
                    "SELECT id FROM tag WHERE plc_id = ?1 AND name = ?2",
                    rusqlite::params![plc_id, name],
                    |row| row.get(0),
                )
                .map_err(|_| anyhow::anyhow!("tag '{name}' not found"))?;
            tag_ids.push(id);
        }
    } else if let Some(sql) = from_sql {
        let sql_upper = sql.trim().to_uppercase();
        if !sql_upper.starts_with("SELECT") {
            return Err(anyhow::anyhow!("--from-sql must be a SELECT query"));
        }
        let mut stmt = conn.prepare(sql)?;
        let rows = stmt.query_map([], |row| {
            let tag_id: i64 = row.get(0)?;
            Ok(tag_id)
        })?;
        for row in rows {
            tag_ids.push(row?);
        }
    } else if let Some(tag_spec) = tag {
        // Single name/ID or comma-separated list
        for piece in tag_spec.split(',') {
            let piece = piece.trim();
            if piece.is_empty() {
                continue;
            }
            let id: i64 = if let Ok(id) = piece.parse::<i64>() {
                id
            } else {
                conn.query_row(
                    "SELECT id FROM tag WHERE plc_id = ?1 AND name = ?2",
                    rusqlite::params![plc_id, piece],
                    |row| row.get(0),
                )
                .map_err(|_| anyhow::anyhow!("tag '{piece}' not found"))?
            };
            tag_ids.push(id);
        }
    } else {
        return Err(anyhow::anyhow!("provide --tag, --from-sql, or --from-file"));
    }

    // Insert all, ignoring duplicates
    let mut added = 0usize;
    for tag_id in &tag_ids {
        let n = conn.execute(
            "INSERT OR IGNORE INTO metric_set_tag (metric_set_id, tag_id, plc_id, created_ts)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![set_id, tag_id, plc_id, now],
        )?;
        added += n;
    }
    println!(
        "success set='{set_name}' requested={} added={} already_member={}",
        tag_ids.len(),
        added,
        tag_ids.len() - added,
    );
    Ok(ExitCode::Ok)
}

fn handle_set_remove_tag(
    conn: &Connection,
    cli: &Cli,
    set_name: &str,
    tag: Option<&str>,
    from_sql: Option<&str>,
) -> Result<ExitCode> {
    // Remote mode
    if let Some(base) = &cli.remote_server {
        let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);

        if from_sql.is_some() {
            return Err(anyhow::anyhow!("--from-sql not supported in remote mode"));
        }

        let tag_str = tag.ok_or_else(|| anyhow::anyhow!("provide --tag"))?;

        let req = serde_json::json!({
            "action": "set_remove_tag",
            "set_name": set_name,
            "tag": tag_str,
        });
        let id = cli::remote::enqueue_command(
            base,
            "control",
            &req.to_string(),
            Some("cli"),
            None,
            timeout,
        )?;
        println!("queued command_id={id}");
        let (state, result, ok, err) =
            cli::remote::wait_command(base, id, timeout, std::time::Duration::from_millis(200))?;
        if ok {
            println!("success {result}");
        } else {
            println!("{state}: {result} {err}");
            return Ok(ExitCode::BadInput);
        }
        return Ok(ExitCode::Ok);
    }

    let set_id: i64 = conn
        .query_row(
            "SELECT id FROM metric_set WHERE name = ?1",
            rusqlite::params![set_name],
            |row| row.get(0),
        )
        .map_err(|_| anyhow::anyhow!("metric set '{set_name}' not found"))?;

    if let Some(sql) = from_sql {
        let mut stmt = conn.prepare(sql)?;
        let rows = stmt.query_map([], |row| {
            let tag_id: i64 = row.get(0)?;
            Ok(tag_id)
        })?;
        let mut count = 0;
        for row in rows {
            let tag_id = row?;
            conn.execute(
                "DELETE FROM metric_set_tag WHERE metric_set_id = ?1 AND tag_id = ?2",
                rusqlite::params![set_id, tag_id],
            )?;
            count += 1;
        }
        println!("success removed={count} tags from set '{set_name}'");
    } else if let Some(tag_str) = tag {
        if let Ok(tag_id) = tag_str.parse::<i64>() {
            conn.execute(
                "DELETE FROM metric_set_tag WHERE metric_set_id = ?1 AND tag_id = ?2",
                rusqlite::params![set_id, tag_id],
            )?;
        } else {
            // Try by name via subquery
            conn.execute(
                "DELETE FROM metric_set_tag WHERE metric_set_id = ?1 AND tag_id IN (SELECT id FROM tag WHERE name = ?2)",
                rusqlite::params![set_id, tag_str],
            )?;
        }
        println!("success tag removed from set '{set_name}'");
    }

    Ok(ExitCode::Ok)
}

fn handle_set_enable(
    conn: &Connection,
    _cli: &Cli,
    set_name: &str,
    async_mode: bool,
) -> Result<ExitCode> {
    // Remote mode
    if let Some(base) = &_cli.remote_server {
        let timeout = std::time::Duration::from_millis(_cli.remote_timeout_ms);
        let req = serde_json::json!({
            "action": "set_enable",
            "set_name": set_name,
        });
        let id = cli::remote::enqueue_command(
            base,
            "control",
            &req.to_string(),
            Some("cli"),
            None,
            timeout,
        )?;
        println!("queued command_id={id}");
        let (state, result, ok, err) =
            cli::remote::wait_command(base, id, timeout, std::time::Duration::from_millis(200))?;
        if ok {
            println!("success {result}");
        } else {
            println!("{state}: {result} {err}");
            return Ok(ExitCode::BadInput);
        }
        return Ok(ExitCode::Ok);
    }

    let now = now_us();
    conn.execute(
        "UPDATE metric_set SET enabled = 1, updated_ts = ?1 WHERE name = ?2",
        rusqlite::params![now, set_name],
    )?;

    if conn.changes() == 0 {
        eprintln!("metric set '{set_name}' not found");
        return Ok(ExitCode::BadInput);
    }

    // Schedule config apply
    let set_id: i64 = conn.query_row(
        "SELECT id FROM metric_set WHERE name = ?1",
        rusqlite::params![set_name],
        |row| row.get(0),
    )?;

    db::config_apply::schedule(
        conn,
        ApplyScope::MetricSet,
        None,
        None,
        Some(set_id),
        None,
        "metric_set_enabled",
        db::config_apply::PLAN_COOLDOWN_MS,
        None,
    )?;

    if async_mode {
        let req = serde_json::json!({
            "action": "set_enable",
            "set_name": set_name,
        });
        let cmd_id = db::command_queue::enqueue(
            conn,
            CommandKind::Control,
            &req.to_string(),
            None,
            None,
            None,
        )?;
        println!("queued command_id={cmd_id}");
    } else {
        println!("success set={set_name} enabled=1 apply_state=pending");
    }

    Ok(ExitCode::Ok)
}

fn handle_set_disable(conn: &Connection, _cli: &Cli, set_name: &str) -> Result<ExitCode> {
    // Remote mode
    if let Some(base) = &_cli.remote_server {
        let timeout = std::time::Duration::from_millis(_cli.remote_timeout_ms);
        let req = serde_json::json!({
            "action": "set_disable",
            "set_name": set_name,
        });
        let id = cli::remote::enqueue_command(
            base,
            "control",
            &req.to_string(),
            Some("cli"),
            None,
            timeout,
        )?;
        println!("queued command_id={id}");
        let (state, result, ok, err) =
            cli::remote::wait_command(base, id, timeout, std::time::Duration::from_millis(200))?;
        if ok {
            println!("success {result}");
        } else {
            println!("{state}: {result} {err}");
            return Ok(ExitCode::BadInput);
        }
        return Ok(ExitCode::Ok);
    }

    let now = now_us();
    conn.execute(
        "UPDATE metric_set SET enabled = 0, updated_ts = ?1 WHERE name = ?2",
        rusqlite::params![now, set_name],
    )?;

    if conn.changes() == 0 {
        eprintln!("metric set '{set_name}' not found");
        return Ok(ExitCode::BadInput);
    }

    let set_id: i64 = conn.query_row(
        "SELECT id FROM metric_set WHERE name = ?1",
        rusqlite::params![set_name],
        |row| row.get(0),
    )?;

    db::config_apply::schedule(
        conn,
        ApplyScope::MetricSet,
        None,
        None,
        Some(set_id),
        None,
        "metric_set_disabled",
        db::config_apply::PLAN_COOLDOWN_MS,
        None,
    )?;

    println!("success set={set_name} enabled=0 apply_state=pending");
    Ok(ExitCode::Ok)
}

fn handle_set_list(cli: &Cli, conn: &Connection, plc: Option<&str>) -> Result<ExitCode> {
    // Remote mode
    if let Some(base) = &cli.remote_server {
        let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);
        let data = cli::remote::get_set_list(base, plc, timeout)?;
        // Render same human output as local mode
        println!(
            "{:<20} {:<30} {:<8} {:<6} {:<8} {:<4}",
            "PLC", "Set", "Enabled", "Total", "Healthy", "Bad"
        );
        if let Some(arr) = data.as_array() {
            for item in arr {
                let plc_name = item["plc"].as_str().unwrap_or("?");
                let name = item["name"].as_str().unwrap_or("?");
                let enabled = item["enabled"].as_bool().unwrap_or(false);
                let total = item["tags_total"].as_i64().unwrap_or(0);
                let healthy = item["tags_healthy"].as_i64().unwrap_or(0);
                let bad = item["tags_bad"].as_i64().unwrap_or(0);
                println!("{plc_name:<20} {name:<30} {enabled:<8} {total:<6} {healthy:<8} {bad:<4}");
            }
        }
        return Ok(ExitCode::Ok);
    }

    let (query, params): (&str, Vec<Box<dyn rusqlite::ToSql>>) = if plc.is_some() {
        ("SELECT vms.plc, vms.metric_set, vms.enabled, vms.tags_total, vms.tags_healthy, vms.tags_bad
          FROM v_metric_set_status vms WHERE vms.plc = ?1 ORDER BY vms.plc, vms.metric_set",
         vec![Box::new(plc.unwrap().to_string())])
    } else {
        ("SELECT vms.plc, vms.metric_set, vms.enabled, vms.tags_total, vms.tags_healthy, vms.tags_bad
          FROM v_metric_set_status vms ORDER BY vms.plc, vms.metric_set",
         Vec::new())
    };

    let mut stmt = conn.prepare(query)?;
    let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|b| b.as_ref()).collect();
    let rows = stmt.query_map(
        rusqlite::params_from_iter(param_refs.iter().copied()),
        |row| {
            let plc: String = row.get(0)?;
            let name: String = row.get(1)?;
            let enabled: bool = row.get::<_, i64>(2)? != 0;
            let total: i64 = row.get(3)?;
            let healthy: i64 = row.get(4)?;
            let bad: i64 = row.get(5)?;
            Ok((plc, name, enabled, total, healthy, bad))
        },
    )?;

    println!(
        "{:<20} {:<30} {:<8} {:<6} {:<8} {:<4}",
        "PLC", "Set", "Enabled", "Total", "Healthy", "Bad"
    );
    for row in rows {
        let (plc, name, enabled, total, healthy, bad) = row?;
        println!("{plc:<20} {name:<30} {enabled:<8} {total:<6} {healthy:<8} {bad:<4}");
    }

    Ok(ExitCode::Ok)
}

fn handle_set_show(
    conn: &Connection,
    cli: &Cli,
    set_name_pos: Option<&str>,
    set_flag: Option<&str>,
) -> Result<ExitCode> {
    let set_name = match set_flag.or(set_name_pos) {
        Some(n) => n,
        None => {
            eprintln!("error: metric set name required (positional or --set)");
            return Ok(ExitCode::BadInput);
        }
    };
    // Remote mode
    if let Some(base) = &cli.remote_server {
        let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);
        let data = cli::remote::get_set_show(base, set_name, timeout)?;
        println!("Set: {}", data["name"].as_str().unwrap_or(set_name));
        println!("Enabled: {}", data["enabled"].as_bool().unwrap_or(false));
        if let Some(members) = data["members"].as_array() {
            println!("\nMembers:");
            for member in members {
                let id = member["id"].as_i64().unwrap_or(0);
                let name = member["name"].as_str().unwrap_or("?");
                let address = member["address"].as_str().unwrap_or("?");
                let dt = member["data_type"].as_str().unwrap_or("?");
                let bad = member["bad"].as_bool().unwrap_or(false);
                let bad_str = if bad {
                    format!(" BAD={}", member["bad_class"].as_str().unwrap_or("unknown"))
                } else {
                    String::new()
                };
                println!("  #{id} {name} ({address}, {dt}){bad_str}");
            }
        }
        return Ok(ExitCode::Ok);
    }

    let set_info = conn
        .query_row(
            "SELECT ms.id, ms.plc_id, ms.name, ms.enabled, ms.description
         FROM metric_set ms WHERE ms.name = ?1",
            rusqlite::params![set_name],
            |row| {
                let id: i64 = row.get(0)?;
                let plc_id: i64 = row.get(1)?;
                let name: String = row.get(2)?;
                let enabled: bool = row.get::<_, i64>(3)? != 0;
                let desc: Option<String> = row.get(4)?;
                Ok((id, plc_id, name, enabled, desc))
            },
        )
        .map_err(|_| anyhow::anyhow!("metric set '{set_name}' not found"))?;

    let (_id, _plc_id, _name, enabled, _desc) = &set_info;

    println!("Set: {set_name}");
    println!("Enabled: {enabled}");

    // List members
    let mut stmt = conn.prepare(
        "SELECT t.id, t.name, t.address, t.data_type, t.bad, t.bad_class
         FROM v_metric_set_member vmsm
         JOIN tag t ON t.id = vmsm.tag_id
         WHERE vmsm.metric_set = ?1",
    )?;
    let members = stmt.query_map(rusqlite::params![set_name], |row| {
        let id: i64 = row.get(0)?;
        let name: String = row.get(1)?;
        let address: String = row.get(2)?;
        let dt: String = row.get(3)?;
        let bad: bool = row.get::<_, i64>(4)? != 0;
        let bc: Option<String> = row.get(5)?;
        Ok((id, name, address, dt, bad, bc))
    })?;

    println!("\nMembers:");
    for member in members {
        let (id, name, address, dt, bad, bc) = member?;
        let bad_str = if bad {
            format!(" BAD={}", bc.as_deref().unwrap_or("unknown"))
        } else {
            String::new()
        };
        println!("  #{id} {name} ({address}, {dt}){bad_str}");
    }

    Ok(ExitCode::Ok)
}

// ---------------------------------------------------------------------------
// Collection handlers
// ---------------------------------------------------------------------------

fn handle_collection_start(
    conn: &Connection,
    cli: &Cli,
    plc: &str,
    dataset: &str,
    sets: &[String],
    no_wait: bool,
) -> Result<ExitCode> {
    // Remote mode: forward to the server, which resolves plc_id by name,
    // creates the dataset, and starts pollers on its side. The CLI's local
    // replica DB has no PLC/tags, so all resolution must happen server-side.
    if let Some(base) = &cli.remote_server {
        let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);
        let req = serde_json::json!({
            "action": "collection_start",
            "plc": plc,
            "dataset": dataset,
            "sets": sets,
        });
        let id = cli::remote::enqueue_command(
            base,
            "control",
            &req.to_string(),
            Some("cli"),
            None,
            timeout,
        )?;
        println!("queued command_id={id}");
        if no_wait {
            return Ok(ExitCode::Ok);
        }
        let (state, result, ok, err) =
            cli::remote::wait_command(base, id, timeout, std::time::Duration::from_millis(200))?;
        if ok {
            println!("success {result}");
            return Ok(ExitCode::Ok);
        }
        println!("{state}: {result} {err}");
        return Ok(ExitCode::BadInput);
    }

    let plc_id: i64 = conn
        .query_row(
            "SELECT id FROM plc WHERE name = ?1",
            rusqlite::params![plc],
            |row| row.get(0),
        )
        .map_err(|_| anyhow::anyhow!("PLC '{plc}' not found"))?;

    // Check no active dataset for this PLC
    let active: i64 = conn.query_row(
        "SELECT COUNT(*) FROM dataset WHERE plc_id = ?1 AND state = 'active'",
        rusqlite::params![plc_id],
        |row| row.get(0),
    )?;
    if active > 0 {
        eprintln!("error: PLC '{plc}' already has an active dataset. Stop it first.");
        return Ok(ExitCode::BadInput);
    }

    // Enqueue collection start
    let req = serde_json::json!({
        "action": "collection_start",
        "plc_id": plc_id,
        "plc": plc,
        "dataset": dataset,
        "sets": sets,
    });
    let cmd_id = db::command_queue::enqueue(
        conn,
        CommandKind::Control,
        &req.to_string(),
        Some(plc_id),
        None,
        None,
    )?;
    println!("queued command_id={cmd_id}");
    if !no_wait {
        return wait_and_print(conn, cmd_id, 200, 10_000);
    }
    Ok(ExitCode::Ok)
}

fn handle_collection_stop(
    conn: &Connection,
    cli: &Cli,
    plc: &str,
    no_wait: bool,
) -> Result<ExitCode> {
    // Remote mode: forward to the server (resolves plc_id by name).
    if let Some(base) = &cli.remote_server {
        let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);
        let req = serde_json::json!({
            "action": "collection_stop",
            "plc": plc,
        });
        let id = cli::remote::enqueue_command(
            base,
            "control",
            &req.to_string(),
            Some("cli"),
            None,
            timeout,
        )?;
        println!("queued command_id={id}");
        if no_wait {
            return Ok(ExitCode::Ok);
        }
        let (state, result, ok, err) =
            cli::remote::wait_command(base, id, timeout, std::time::Duration::from_millis(200))?;
        if ok {
            println!("success {result}");
            return Ok(ExitCode::Ok);
        }
        println!("{state}: {result} {err}");
        return Ok(ExitCode::BadInput);
    }

    let plc_id: i64 = conn
        .query_row(
            "SELECT id FROM plc WHERE name = ?1",
            rusqlite::params![plc],
            |row| row.get(0),
        )
        .map_err(|_| anyhow::anyhow!("PLC '{plc}' not found"))?;

    let req = serde_json::json!({
        "action": "collection_stop",
        "plc_id": plc_id,
        "plc": plc,
    });
    let cmd_id = db::command_queue::enqueue(
        conn,
        CommandKind::Control,
        &req.to_string(),
        Some(plc_id),
        None,
        None,
    )?;
    println!("queued command_id={cmd_id}");
    if !no_wait {
        return wait_and_print(conn, cmd_id, 200, 10_000);
    }
    Ok(ExitCode::Ok)
}

// ---------------------------------------------------------------------------
// History handlers
// ---------------------------------------------------------------------------

fn handle_history_clean(
    conn: &Connection,
    _cli: &Cli,
    plc: &str,
    before_ts: Option<i64>,
    before: Option<&str>,
    batch_size: u64,
) -> Result<ExitCode> {
    let plc_id: i64 = conn
        .query_row(
            "SELECT id FROM plc WHERE name = ?1",
            rusqlite::params![plc],
            |row| row.get(0),
        )
        .map_err(|_| anyhow::anyhow!("PLC '{plc}' not found"))?;

    // Check no active dataset
    let active: i64 = conn.query_row(
        "SELECT COUNT(*) FROM dataset WHERE plc_id = ?1 AND state = 'active'",
        rusqlite::params![plc_id],
        |row| row.get(0),
    )?;
    if active > 0 {
        eprintln!("error: PLC '{plc}' has active datasets. Stop collection first.");
        return Ok(ExitCode::BadInput);
    }

    let cutoff = match (before_ts, before) {
        (Some(ts), _) => ts,
        (_, Some(iso)) => {
            // Parse ISO 8601 to epoch microseconds
            let dt =
                chrono::NaiveDateTime::parse_from_str(&iso.replace('T', " "), "%Y-%m-%d %H:%M:%S")
                    .map_err(|e| anyhow::anyhow!("invalid datetime '{iso}': {e}"))?;
            dt.and_utc().timestamp_micros()
        }
        (None, None) => {
            eprintln!("error: specify --before-ts or --before");
            return Ok(ExitCode::BadInput);
        }
    };

    let req = serde_json::json!({
        "action": "history_clean",
        "plc_id": plc_id,
        "cutoff_ts": cutoff,
        "batch_size": batch_size,
    });
    let cmd_id = db::command_queue::enqueue(
        conn,
        CommandKind::Admin,
        &req.to_string(),
        Some(plc_id),
        None,
        None,
    )?;
    println!("queued command_id={cmd_id}");
    Ok(ExitCode::Ok)
}

fn handle_history_delete_dataset(conn: &Connection, _cli: &Cli, dataset: &str) -> Result<ExitCode> {
    // Check dataset exists and is stopped
    let state: String = conn
        .query_row(
            "SELECT state FROM dataset WHERE name = ?1",
            rusqlite::params![dataset],
            |row| row.get(0),
        )
        .map_err(|_| anyhow::anyhow!("dataset '{dataset}' not found"))?;

    if state == "active" {
        eprintln!("error: dataset '{dataset}' is active. Stop collection first.");
        return Ok(ExitCode::BadInput);
    }

    let req = serde_json::json!({
        "action": "delete_dataset",
        "dataset": dataset,
    });
    let cmd_id =
        db::command_queue::enqueue(conn, CommandKind::Admin, &req.to_string(), None, None, None)?;
    println!("queued command_id={cmd_id}");
    Ok(ExitCode::Ok)
}

fn handle_history_vacuum(conn: &Connection, force: bool) -> Result<ExitCode> {
    if !force {
        // Check for active datasets
        let active: i64 = conn.query_row(
            "SELECT COUNT(*) FROM dataset WHERE state = 'active'",
            [],
            |row| row.get(0),
        )?;
        if active > 0 {
            eprintln!("warning: {active} active dataset(s). VACUUM may block polling.");
            eprintln!("Use --force to proceed anyway.");
            return Ok(ExitCode::BadInput);
        }
    }

    conn.execute_batch("VACUUM")?;
    println!("success vacuum completed");
    Ok(ExitCode::Ok)
}

// ---------------------------------------------------------------------------
// PLC / poll group / bulk-import handlers
// ---------------------------------------------------------------------------

fn handle_plc_add(
    cli: &Cli,
    conn: &Connection,
    name: &str,
    host: &str,
    rack: i64,
    slot: i64,
    disabled: bool,
) -> Result<ExitCode> {
    // Remote mode
    if let Some(base) = &cli.remote_server {
        let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);
        let req = serde_json::json!({
            "action": "plc_add",
            "name": name,
            "host": host,
            "rack": rack,
            "slot": slot,
            "disabled": disabled,
        });
        let id = cli::remote::enqueue_command(
            base,
            "control",
            &req.to_string(),
            Some("cli"),
            None,
            timeout,
        )?;
        println!("queued command_id={id}");
        let (state, result, ok, err) =
            cli::remote::wait_command(base, id, timeout, std::time::Duration::from_millis(200))?;
        if ok {
            println!("success {result}");
        } else {
            println!("{state}: {result} {err}");
            return Ok(ExitCode::BadInput);
        }
        return Ok(ExitCode::Ok);
    }

    if name.trim().is_empty() {
        eprintln!("error: --name cannot be empty");
        return Ok(ExitCode::BadInput);
    }
    // Check if PLC already exists
    let exists: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM plc WHERE name = ?1",
            rusqlite::params![name],
            |row| row.get::<_, i64>(0),
        )
        .map(|c| c > 0)
        .unwrap_or(false);
    if exists {
        eprintln!("error: PLC '{name}' already exists");
        return Ok(ExitCode::BadInput);
    }

    let enabled = if disabled { 0 } else { 1 };
    conn.execute(
        "INSERT INTO plc (name, host, rack, slot, enabled)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![name, host, rack, slot, enabled],
    )?;

    let id = conn.last_insert_rowid();
    println!("success plc_id={id} name={name} host={host}");
    Ok(ExitCode::Ok)
}

fn handle_poll_group_add(
    cli: &Cli,
    conn: &Connection,
    name: &str,
    period_ms: i64,
) -> Result<ExitCode> {
    // Remote mode
    if let Some(base) = &cli.remote_server {
        let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);
        let req = serde_json::json!({
            "action": "poll_group_add",
            "name": name,
            "period_ms": period_ms,
        });
        let id = cli::remote::enqueue_command(
            base,
            "control",
            &req.to_string(),
            Some("cli"),
            None,
            timeout,
        )?;
        println!("queued command_id={id}");
        let (state, result, ok, err) =
            cli::remote::wait_command(base, id, timeout, std::time::Duration::from_millis(200))?;
        if ok {
            println!("success {result}");
        } else {
            println!("{state}: {result} {err}");
            return Ok(ExitCode::BadInput);
        }
        return Ok(ExitCode::Ok);
    }

    if name.trim().is_empty() {
        eprintln!("error: --name cannot be empty");
        return Ok(ExitCode::BadInput);
    }
    if period_ms < 1 {
        eprintln!("error: --period-ms must be at least 1");
        return Ok(ExitCode::BadInput);
    }
    // Check if poll group already exists
    let exists: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM poll_group WHERE name = ?1",
            rusqlite::params![name],
            |row| row.get::<_, i64>(0),
        )
        .map(|c| c > 0)
        .unwrap_or(false);
    if exists {
        eprintln!("error: poll group '{name}' already exists");
        return Ok(ExitCode::BadInput);
    }

    conn.execute(
        "INSERT INTO poll_group (name, period_ms) VALUES (?1, ?2)",
        rusqlite::params![name, period_ms],
    )?;

    println!("success poll_group={name} period_ms={period_ms}");
    Ok(ExitCode::Ok)
}

fn handle_event_add(conn: &Connection, cli: &Cli, action: &EventAction) -> Result<ExitCode> {
    let EventAction::Add {
        text,
        kind,
        severity,
        plc,
        dataset,
        cycle_id,
    } = action;

    // Remote mode not supported yet — event markers are local-only.
    if cli.remote_server.is_some() {
        eprintln!("error: event markers are not supported via remote server yet");
        return Ok(ExitCode::BadInput);
    }

    if text.trim().is_empty() {
        eprintln!("error: event text cannot be empty");
        return Ok(ExitCode::BadInput);
    }

    // Resolve plc_id: either from --plc flag or auto-detect the first enabled PLC.
    let plc_id: i64 = match plc {
        Some(name) => conn
            .query_row(
                "SELECT id FROM plc WHERE name = ?1",
                rusqlite::params![name],
                |row| row.get(0),
            )
            .map_err(|_| {
                anyhow::anyhow!("PLC '{name}' not found. Create it first with 's7trace plc-add'.")
            })?,
        None => conn
            .query_row(
                "SELECT id FROM plc WHERE enabled = 1 ORDER BY id LIMIT 1",
                [],
                |row| row.get(0),
            )
            .map_err(|_| {
                anyhow::anyhow!(
                    "no PLC found. Either create a PLC with 's7trace plc-add' or specify --plc."
                )
            })?,
    };

    // Resolve optional dataset_id
    let dataset_id: Option<i64> = match dataset {
        Some(name) => Some(
            conn.query_row(
                "SELECT id FROM dataset WHERE name = ?1 AND plc_id = ?2",
                rusqlite::params![name, plc_id],
                |row| row.get(0),
            )
            .map_err(|_| anyhow::anyhow!("Dataset '{name}' not found for PLC id={plc_id}."))?,
        ),
        None => None,
    };

    let now_us = types::now_us();

    conn.execute(
        "INSERT INTO event_marker (dataset_id, plc_id, cycle_id, ts, kind, severity, details_json)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        rusqlite::params![dataset_id, plc_id, cycle_id, now_us, kind, severity, text,],
    )?;

    let id = conn.last_insert_rowid();
    println!("event id={id} plc_id={plc_id} kind={kind} ts={now_us}");
    Ok(ExitCode::Ok)
}

fn handle_tag_import(
    cli: &Cli,
    conn: &Connection,
    plc: &str,
    file: &str,
    set: Option<&str>,
    poll_group: &str,
    store_mode: &str,
    storage_hook: &str,
    value_role: &str,
    do_validate: bool,
) -> Result<ExitCode> {
    // Remote mode: read file locally, send yaml content to server
    if let Some(base) = &cli.remote_server {
        let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);
        let content =
            std::fs::read_to_string(file).map_err(|e| anyhow::anyhow!("reading '{file}': {e}"))?;
        let req = serde_json::json!({
            "action": "tag_import",
            "plc": plc,
            "yaml": content,
            "set": set,
            "poll_group": poll_group,
            "store_mode": store_mode,
            "storage_hook": storage_hook,
            "value_role": value_role,
            "validate": do_validate,
        });
        let id = cli::remote::enqueue_command(
            base,
            "control",
            &req.to_string(),
            Some("cli"),
            None,
            timeout,
        )?;
        println!("queued command_id={id}");
        let (state, result, ok, err) =
            cli::remote::wait_command(base, id, timeout, std::time::Duration::from_millis(200))?;
        if ok {
            println!("success {result}");
        } else {
            println!("{state}: {result} {err}");
            return Ok(ExitCode::BadInput);
        }
        return Ok(ExitCode::Ok);
    }

    // Validate store_mode and storage_hook up front
    let _sm: StoreMode = store_mode
        .parse()
        .map_err(|e: String| anyhow::anyhow!("invalid store mode: {e}"))?;
    let _sh: StorageHook = storage_hook
        .parse()
        .map_err(|e: String| anyhow::anyhow!("invalid storage hook: {e}"))?;

    // Resolve PLC once up front
    let plc_id: i64 = conn
        .query_row(
            "SELECT id FROM plc WHERE name = ?1",
            rusqlite::params![plc],
            |row| row.get(0),
        )
        .map_err(|_| {
            anyhow::anyhow!("PLC '{plc}' not found. Create it first with 's7trace plc-add'.")
        })?;

    // Read file
    let content =
        std::fs::read_to_string(file).map_err(|e| anyhow::anyhow!("reading '{}': {}", file, e))?;

    // Parse signals
    let signals = cli::import::parse_signals(&content)?;

    // Import each signal, collecting errors
    let set_arg = set;
    let mut imported: u32 = 0;
    let mut imported_tags: Vec<(i64, String)> = Vec::new(); // (tag_id, name)
    let mut failures: Vec<(String, String)> = Vec::new(); // (name, reason)

    for sig in &signals {
        // Determine data type
        let dt = if let Some(ref type_str) = sig.data_type {
            match type_str.parse::<DataType>() {
                Ok(dt) => dt,
                Err(e) => {
                    failures.push((
                        sig.name.clone(),
                        format!("invalid type '{}': {}", type_str, e),
                    ));
                    continue;
                }
            }
        } else {
            match cli::import::infer_type(&sig.address) {
                Ok(dt) => dt,
                Err(e) => {
                    failures.push((sig.name.clone(), e.to_string()));
                    continue;
                }
            }
        };

        // Parse address
        let addr = match S7Address::parse(&sig.address) {
            Ok(a) => a,
            Err(e) => {
                failures.push((sig.name.clone(), format!("invalid address: {e}")));
                continue;
            }
        };

        // Insert tag (reuses auto-creation of poll_group and metric_set)
        match db::validation::insert_tag(
            conn,
            plc,
            &sig.name,
            &addr,
            dt,
            poll_group,
            store_mode,
            storage_hook,
            value_role,
            set_arg,
        ) {
            Ok(inserted) => {
                imported += 1;
                imported_tags.push((inserted.tag_id, sig.name.clone()));
            }
            Err(e) => {
                failures.push((sig.name.clone(), e.to_string()));
            }
        }
    }

    // Schedule one PLC-scoped config apply if anything was imported
    if imported > 0 {
        let _ = db::config_apply::schedule(
            conn,
            ApplyScope::Plc,
            Some(plc_id),
            None,
            None,
            None,
            "tag_import",
            db::config_apply::PLAN_COOLDOWN_MS,
            None,
        );
    }

    // Optionally validate each imported tag synchronously
    if do_validate && !imported_tags.is_empty() {
        let plc_row: (String, String, i64, i64) = conn
            .query_row(
                "SELECT host, name, rack, slot FROM plc WHERE id = ?1",
                rusqlite::params![plc_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .map_err(|_| anyhow::anyhow!("PLC '{plc}' not found"))?;
        let (host, plc_name, rack, slot) = plc_row;
        let client = create_plc_client(&plc_name, &host, rack as u16, slot as u16);

        for (tag_id, tag_name) in &imported_tags {
            let result = db::validation::validate_tag(conn, &client, *tag_id)?;
            if result.ok {
                let val_str = if let Some(v) = result.value_i {
                    if let Some(f) = result.value_f {
                        format!("value={v} float={f}")
                    } else {
                        format!("value={v}")
                    }
                } else if let Some(f) = result.value_f {
                    format!("value={f}")
                } else {
                    "value=null".to_string()
                };
                println!("  validate ok {tag_name} tag_id={tag_id} {val_str}");
            } else {
                let bc = result
                    .bad_class
                    .as_ref()
                    .map(|b| b.to_string())
                    .unwrap_or_default();
                let det = result.detail.as_deref().unwrap_or("");
                println!(
                    "  validate bad {tag_name} tag_id={tag_id} bad_class={bc} detail=\"{det}\""
                );
            }
        }
    }

    // Summary
    let set_name = set_arg.unwrap_or("default");
    println!(
        "imported={imported} failed={} set={set_name}",
        failures.len()
    );
    for (name, reason) in &failures {
        eprintln!("  failed: {name} ({reason})");
    }

    if imported > 0 {
        Ok(ExitCode::Ok)
    } else {
        Ok(ExitCode::BadInput)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn local_cli() -> Cli {
        Cli {
            db: ":memory:".into(),
            remote_server: None,
            local_db: true,
            remote_timeout_ms: 30_000,
            command: None,
        }
    }

    #[test]
    fn test_handle_sql_select() {
        let conn = db::schema::open_memory().unwrap();
        let cli = local_cli();
        let result = handle_sql(&conn, &cli, "SELECT 1 AS col", "json", 10);
        assert!(result.is_ok());
    }

    #[test]
    fn test_handle_sql_rejects_write() {
        let conn = db::schema::open_memory().unwrap();
        let cli = local_cli();
        let result = handle_sql(&conn, &cli, "DELETE FROM tag", "json", 10);
        assert!(result.is_ok());
        assert_eq!(result.unwrap() as i32, ExitCode::BadInput as i32);
    }

    #[test]
    fn test_admin_sql_rejects_remote_mode() {
        let conn = db::schema::open_memory().unwrap();
        let mut cli = local_cli();
        cli.remote_server = Some("http://127.0.0.1:8080".into());
        cli.local_db = false;
        let result = handle_admin_sql(&cli, &conn, "UPDATE plc SET enabled = 0");
        assert!(result.is_ok());
        assert_eq!(result.unwrap() as i32, ExitCode::BadInput as i32);
    }

    #[test]
    fn test_set_create_and_list() {
        let conn = db::schema::open_memory().unwrap();
        let cli = Cli {
            db: ":memory:".into(),
            remote_server: None,
            local_db: true,
            remote_timeout_ms: 30_000,
            command: None,
        };
        // Need a PLC first
        conn.execute(
            "INSERT INTO plc (name, host) VALUES ('test-plc', '127.0.0.1')",
            [],
        )
        .unwrap();

        handle_set_create(&cli, &conn, "test-plc", "my_set", None).unwrap();
        let id: i64 = conn
            .query_row(
                "SELECT id FROM metric_set WHERE name = 'my_set'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(id > 0);
    }

    #[test]
    fn test_tag_disable_unknown() {
        let conn = db::schema::open_memory().unwrap();
        let cli = Cli {
            db: ":memory:".into(),
            remote_server: None,
            local_db: true,
            remote_timeout_ms: 30_000,
            command: None,
        };
        let result = handle_tag_disable(&conn, &cli, Some(999), None, None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap() as i32, ExitCode::BadInput as i32);
    }

    #[test]
    fn test_set_add_tag_bulk_csv() {
        let conn = db::schema::open_memory().unwrap();
        let cli = Cli {
            db: ":memory:".into(),
            remote_server: None,
            local_db: true,
            remote_timeout_ms: 30_000,
            command: None,
        };
        conn.execute(
            "INSERT INTO plc (name, host) VALUES ('plc1', '127.0.0.1')",
            [],
        )
        .unwrap();
        // Need poll_group for the FK
        conn.execute(
            "INSERT INTO poll_group (name, period_ms) VALUES ('g1', 1000)",
            [],
        )
        .unwrap();
        for n in ["t1", "t2", "t3"] {
            conn.execute(
                "INSERT INTO tag (plc_id, name, address, data_type, poll_group_id,
                                  db_num, byte_offset, byte_size, created_ts, updated_ts)
                 VALUES (1, ?1, 'DB1.DBW0', 'i16', 1, 1, 0, 2, 1000000, 1000000)",
                rusqlite::params![n],
            )
            .unwrap();
        }
        handle_set_create(&cli, &conn, "plc1", "my_set", None).unwrap();
        handle_set_add_tag(&conn, &cli, "my_set", Some("t1,t2,t3"), None, None).unwrap();
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM metric_set_tag WHERE metric_set_id = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(n, 3);
    }

    #[test]
    fn test_set_add_tag_from_file() {
        let conn = db::schema::open_memory().unwrap();
        let cli = Cli {
            db: ":memory:".into(),
            remote_server: None,
            local_db: true,
            remote_timeout_ms: 30_000,
            command: None,
        };
        conn.execute(
            "INSERT INTO plc (name, host) VALUES ('plc1', '127.0.0.1')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO poll_group (name, period_ms) VALUES ('g1', 1000)",
            [],
        )
        .unwrap();
        for n in ["alpha", "beta"] {
            conn.execute(
                "INSERT INTO tag (plc_id, name, address, data_type, poll_group_id,
                                  db_num, byte_offset, byte_size, created_ts, updated_ts)
                 VALUES (1, ?1, 'DB1.DBW0', 'i16', 1, 1, 0, 2, 1000000, 1000000)",
                rusqlite::params![n],
            )
            .unwrap();
        }
        handle_set_create(&cli, &conn, "plc1", "s2", None).unwrap();

        let path =
            std::env::temp_dir().join(format!("s7trace_set_tags_{}.txt", std::process::id()));
        std::fs::write(&path, "# comment line\nalpha\n\nbeta\n").unwrap();
        handle_set_add_tag(&conn, &cli, "s2", None, None, Some(path.to_str().unwrap())).unwrap();
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM metric_set_tag WHERE metric_set_id = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(n, 2);
    }

    #[test]
    fn test_plc_connectivity_default_and_flip() {
        let conn = db::schema::open_memory().unwrap();
        conn.execute(
            "INSERT INTO plc (name, host) VALUES ('p1', '127.0.0.1')",
            [],
        )
        .unwrap();
        // Default is 'unknown'
        let s: String = conn
            .query_row(
                "SELECT connectivity FROM plc WHERE name = 'p1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(s, "unknown");
        // Flip to offline (simulates failed write_samples)
        conn.execute("UPDATE plc SET connectivity = 'offline' WHERE id = 1", [])
            .unwrap();
        let s: String = conn
            .query_row("SELECT connectivity FROM plc WHERE id = 1", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(s, "offline");
    }

    #[test]
    fn test_plc_add_and_duplicate() {
        let conn = db::schema::open_memory().unwrap();
        let cli = Cli {
            db: ":memory:".into(),
            remote_server: None,
            local_db: true,
            remote_timeout_ms: 30_000,
            command: None,
        };

        // First add should succeed
        let r = handle_plc_add(&cli, &conn, "myplc", "10.0.0.1", 0, 2, false).unwrap();
        assert_eq!(r as i32, ExitCode::Ok as i32);
        let id: i64 = conn
            .query_row("SELECT id FROM plc WHERE name = 'myplc'", [], |r| r.get(0))
            .unwrap();
        assert!(id > 0);
        let s: String = conn
            .query_row(
                "SELECT connectivity FROM plc WHERE id = ?1",
                rusqlite::params![id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(s, "unknown");

        // Duplicate name should fail
        let r = handle_plc_add(&cli, &conn, "myplc", "10.0.0.2", 0, 2, false).unwrap();
        assert_eq!(r as i32, ExitCode::BadInput as i32);
    }

    #[test]
    fn test_poll_group_add_and_duplicate() {
        let conn = db::schema::open_memory().unwrap();
        let cli = Cli {
            db: ":memory:".into(),
            remote_server: None,
            local_db: true,
            remote_timeout_ms: 30_000,
            command: None,
        };

        let r = handle_poll_group_add(&cli, &conn, "fast", 40).unwrap();
        assert_eq!(r as i32, ExitCode::Ok as i32);
        let id: i64 = conn
            .query_row("SELECT id FROM poll_group WHERE name = 'fast'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert!(id > 0);

        // Duplicate name should fail
        let r = handle_poll_group_add(&cli, &conn, "fast", 100).unwrap();
        assert_eq!(r as i32, ExitCode::BadInput as i32);
    }

    #[test]
    fn test_tag_import_end_to_end() {
        let conn = db::schema::open_memory().unwrap();
        // Seed a PLC (test fixture, not CLI)
        conn.execute(
            "INSERT INTO plc (name, host) VALUES ('test-plc', '127.0.0.1')",
            [],
        )
        .unwrap();

        // Write a small yaml-ish signal file
        let yaml = "\
signals:
  - name: SigA
    address: DB1.DBW0
    type: i16
  - name: SigB
    address: DB1.DBW2
  - name: SigC
    address: DB1.DBX10.0
";
        let path = std::env::temp_dir().join(format!(
            "s7trace_tag_import_test_{}.yaml",
            std::process::id()
        ));
        std::fs::write(&path, yaml).unwrap();

        let cli = Cli {
            db: ":memory:".into(),
            remote_server: None,
            local_db: true,
            remote_timeout_ms: 30_000,
            command: None,
        };

        // Use a known poll-group so it doesn't need auto-creation (the
        // poll_group FK is handled by insert_tag auto-create, so we're fine).
        let r = handle_tag_import(
            &cli,
            &conn,
            "test-plc",
            path.to_str().unwrap(),
            Some("testset"),
            "slow",
            "on_change",
            "on_change",
            "state",
            false,
        )
        .unwrap();
        assert_eq!(r as i32, ExitCode::Ok as i32);

        // Verify 3 tags were inserted
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM tag", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 3);

        // Verify metric set was created and membership exists
        let set_id: i64 = conn
            .query_row(
                "SELECT id FROM metric_set WHERE name = 'testset'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let m: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM metric_set_tag WHERE metric_set_id = ?1",
                rusqlite::params![set_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(m, 3);

        // Cleanup
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_tag_import_remote_file_not_found() {
        let conn = db::schema::open_memory().unwrap();
        let cli = Cli {
            db: ":memory:".into(),
            remote_server: Some("http://example.com".into()),
            local_db: false,
            remote_timeout_ms: 30_000,
            command: None,
        };
        // Remote path reads file first (before connecting to server); file-not-found
        // should propagate as an error the same way the local path does.
        let r = handle_tag_import(
            &cli,
            &conn,
            "x",
            "/nonexistent",
            None,
            "slow",
            "on_change",
            "on_change",
            "state",
            false,
        );
        assert!(r.is_err());
    }
}
