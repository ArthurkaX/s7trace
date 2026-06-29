#![allow(dead_code)]
// TODO(reboot): remove these once Block 1/2 replace the multi-arg handlers
// (typed `Op` structs) and simplify `CommandKind`.
#![allow(clippy::too_many_arguments)]
#![allow(clippy::enum_variant_names)]

use anyhow::{Context, Result};
use clap::{CommandFactory, Parser};
use rusqlite::Connection;

/// Resolve the server base URL from CLI args.
/// Returns None when no server address is configured.
fn server_base(cli: &Cli) -> Option<String> {
    cli.remote_server
        .as_ref()
        .map(|s| cli::remote::resolve_url(s, cli.server_port))
}

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
                    no_ui,
                    strict_overrun,
                }) => (
                    *dry_run,
                    http_bind.clone().or(Some("0.0.0.0:7707".to_string())),
                    !*no_ui,
                    *strict_overrun,
                ),
                _ => (false, Some("0.0.0.0:7707".to_string()), false, false),
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
            | Command::TagEnable { .. }
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
            | Command::SetDelete { .. }
            | Command::CollectionStart { .. }
            | Command::CollectionStop { .. }
            | Command::PlcAdd { .. }
            | Command::PollGroupAdd { .. }
            | Command::PollGroupSetPeriod { .. }
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
        Command::TagEnable { .. } => "tag-enable",
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
        Command::SetDelete { .. } => "set-delete",
        Command::CollectionStart { .. } => "collection-start",
        Command::CollectionStop { .. } => "collection-stop",
        Command::HistoryClean { .. } => "history-clean",
        Command::HistoryDeleteDataset { .. } => "history-delete-dataset",
        Command::HistoryVacuum { .. } => "history-vacuum",
        Command::PlcAdd { .. } => "plc-add",
        Command::PollGroupAdd { .. } => "poll-group-add",
        Command::PollGroupSetPeriod { .. } => "poll-group-set-period",
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
        } => run_op(
            cli,
            conn,
            engine::ops::Op::TagAddFull {
                plc: plc.clone(),
                name: name.clone(),
                address: address.clone(),
                data_type: data_type.clone(),
                poll_group: poll_group.clone(),
                store_mode: store_mode.clone(),
                storage_hook: storage_hook.clone(),
                value_role: value_role.clone(),
                metric_set: metric_set.clone(),
            },
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
        } => run_op(
            cli,
            conn,
            engine::ops::Op::TagUpdate {
                tag_id: *tag_id,
                plc: plc.clone(),
                name: name.clone(),
                address: address.clone(),
                data_type: data_type.clone(),
                poll_group: poll_group.clone(),
                store_mode: store_mode.clone(),
                storage_hook: storage_hook.clone(),
            },
        ),
        Command::TagDisable { tag_id, plc, name } => run_op(
            cli,
            conn,
            engine::ops::Op::TagDisable {
                tag_id: *tag_id,
                plc: plc.clone(),
                name: name.clone(),
            },
        ),
        Command::TagEnable { tag_id, plc, name } => run_op(
            cli,
            conn,
            engine::ops::Op::TagEnable {
                tag_id: *tag_id,
                plc: plc.clone(),
                name: name.clone(),
            },
        ),
        Command::TagRevalidate { tag_id, plc, name } => run_op(
            cli,
            conn,
            engine::ops::Op::TagRevalidate {
                tag_id: *tag_id,
                plc: plc.clone(),
                name: name.clone(),
            },
        ),
        Command::Read {
            plc,
            address,
            data_type,
            format: fmt,
        } => handle_read(cli, conn, plc, address, data_type, fmt),
        Command::ReadRaw {
            plc,
            db,
            start,
            len,
            format: fmt,
            save_probe,
        } => handle_read_raw(conn, cli, plc, *db, *start, *len, fmt, *save_probe),
        Command::Status { format: fmt } => handle_status(cli, conn, fmt),
        Command::SetCreate {
            plc,
            name,
            description,
        } => run_op(
            cli,
            conn,
            engine::ops::Op::SetCreate {
                plc: plc.clone(),
                name: name.clone(),
                description: description.clone(),
            },
        ),
        Command::SetAddTag {
            set_name,
            tag,
            from_sql,
            from_file,
        } => {
            // `--from-file` lives on the CLI host: resolve it to a tag-name list
            // here so the operation carries data the server can act on.
            let (tag_field, from_sql_field) = if let Some(path) = from_file {
                let content = std::fs::read_to_string(path)
                    .map_err(|e| anyhow::anyhow!("reading {path}: {e}"))?;
                let names: Vec<&str> = content
                    .lines()
                    .map(|l| l.trim())
                    .filter(|l| !l.is_empty() && !l.starts_with('#'))
                    .collect();
                if names.is_empty() {
                    return Err(anyhow::anyhow!("no tag names found in {path}"));
                }
                (Some(names.join(",")), None)
            } else {
                (tag.clone(), from_sql.clone())
            };
            if server_base(cli).is_some() && from_sql_field.is_some() {
                return Err(anyhow::anyhow!("--from-sql not supported in remote mode"));
            }
            run_op(
                cli,
                conn,
                engine::ops::Op::SetAddTag {
                    set_name: set_name.clone(),
                    tag: tag_field,
                    from_sql: from_sql_field,
                    from_file: None,
                },
            )
        }
        Command::SetRemoveTag {
            set_name,
            tag,
            from_sql,
        } => {
            if server_base(cli).is_some() && from_sql.is_some() {
                return Err(anyhow::anyhow!("--from-sql not supported in remote mode"));
            }
            run_op(
                cli,
                conn,
                engine::ops::Op::SetRemoveTag {
                    set_name: set_name.clone(),
                    tag: tag.clone(),
                    from_sql: from_sql.clone(),
                },
            )
        }
        Command::SetEnable { set_name } => run_op(
            cli,
            conn,
            engine::ops::Op::SetEnable {
                set_name: set_name.clone(),
            },
        ),
        Command::SetDisable { set_name } => run_op(
            cli,
            conn,
            engine::ops::Op::SetDisable {
                set_name: set_name.clone(),
            },
        ),
        Command::SetList { plc } => handle_set_list(cli, conn, plc.as_deref()),
        Command::SetShow { set_name, set_flag } => {
            handle_set_show(conn, cli, set_name.as_deref(), set_flag.as_deref())
        }
        Command::SetDelete { set_name } => run_op(
            cli,
            conn,
            engine::ops::Op::SetDelete {
                set_name: set_name.clone(),
            },
        ),
        Command::CollectionStart { plc, dataset, sets } => {
            handle_collection_start(conn, cli, plc, dataset, sets)
        }
        Command::CollectionStop { plc } => handle_collection_stop(conn, cli, plc),
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
        } => run_op(
            cli,
            conn,
            engine::ops::Op::PlcAdd {
                name: name.clone(),
                host: host.clone(),
                rack: *rack,
                slot: *slot,
                disabled: *disabled,
            },
        ),
        Command::PollGroupAdd { name, period_ms } => run_op(
            cli,
            conn,
            engine::ops::Op::PollGroupAdd {
                name: name.clone(),
                period_ms: *period_ms,
            },
        ),
        Command::PollGroupSetPeriod {
            poll_group_id,
            poll_group_name,
            period_ms,
        } => run_op(
            cli,
            conn,
            engine::ops::Op::PollGroupSetPeriod {
                poll_group_id: *poll_group_id,
                poll_group_name: poll_group_name.clone(),
                period_ms: *period_ms,
            },
        ),
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

fn handle_sql(
    conn: &Connection,
    cli: &Cli,
    query: &str,
    fmt: &str,
    limit: u64,
) -> Result<ExitCode> {
    // Ensure read-only: reject anything that isn't a SELECT or WITH.
    // Remote mode uses the private HTTP transport, but the user interface
    // remains this CLI command.
    if let Some(base) = server_base(cli) {
        let encoded = urlencode(query);
        let url = format!("/v1/sql?query={}&limit={}", encoded, limit);
        let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);
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

/// Run a mutating operation through the unified core ([`engine::ops::execute`]).
///
/// Remote mode serializes the `Op` and enqueues it on the server; local mode
/// executes it directly against the local database. This single helper
/// replaces the per-command "if remote { build JSON; enqueue; wait } else
/// { local work }" boilerplate that used to be copy-pasted into every handler.
fn run_op(cli: &Cli, conn: &Connection, op: engine::ops::Op) -> Result<ExitCode> {
    if let Some(base) = server_base(cli) {
        let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);
        let id = cli::remote::enqueue_command(
            &base,
            "control",
            &serde_json::to_string(&op)?,
            Some("cli"),
            None,
            timeout,
        )?;
        println!("queued command_id={id}");
        let (state, result, ok, err) =
            cli::remote::wait_command(&base, id, timeout, std::time::Duration::from_millis(200))?;
        if ok {
            println!("success {result}");
            Ok(ExitCode::Ok)
        } else {
            println!("{state}: {result} {err}");
            Ok(ExitCode::BadInput)
        }
    } else {
        let out = engine::ops::execute(&engine::ops::ExecCtx::local(conn), op)?;
        if out.ok {
            if out.data.is_null() {
                println!("success {}", out.message);
            } else {
                cli::format::print_json_ok(&out.data);
            }
            Ok(ExitCode::Ok)
        } else {
            println!("{}", out.message);
            Ok(ExitCode::BadInput)
        }
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

fn handle_command_result_inner(result: &CommandResult, fmt: &Format) -> Result<ExitCode> {
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

// ---------------------------------------------------------------------------
// Read handlers
// ---------------------------------------------------------------------------

fn handle_read(
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

fn handle_read_raw(
    _conn: &Connection,
    _cli: &Cli,
    _plc: &str,
    _db: u16,
    _start: usize,
    _len: usize,
    _fmt: &str,
    _save_probe: bool,
) -> Result<ExitCode> {
    // read-raw requires a server
    cli::format::print_json_error(
        "server_required",
        "read-raw requires a server. Set S7TRACE_SERVER or pass -r.",
        ExitCode::BadInput as i32,
    );
    Ok(ExitCode::BadInput)
}

// ---------------------------------------------------------------------------
// Status handler
// ---------------------------------------------------------------------------

fn handle_status(cli: &Cli, conn: &Connection, fmt: &str) -> Result<ExitCode> {
    // Remote mode: fetch JSON from server and pretty-print.
    if let Some(base) = server_base(cli) {
        let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);
        let v = cli::remote::get_status(&base, timeout)?;
        if fmt == "json" || fmt == "jsonl" {
            println!("{}", serde_json::to_string_pretty(&v)?);
        } else {
            // text mode: human-readable summary from the API JSON.
            println!("(remote server: {base})");
            println!(
                "s7trace version={} schema={} api={}",
                v["version"]["package"].as_str().unwrap_or("?"),
                v["version"]["schema"].as_str().unwrap_or("?"),
                v["version"]["api"].as_i64().unwrap_or(0)
            );
            println!(
                "tags: active={} bad={}",
                v["tags"]["active"], v["tags"]["bad"]
            );
            println!(
                "command_queue: pending={} running={}",
                v["command_queue"]["pending"], v["command_queue"]["running"]
            );
            println!("PLCs:");
            for plc in v["plcs"].as_array().unwrap_or(&Vec::new()) {
                let pg_array = plc["poll_groups"].as_array().cloned().unwrap_or_default();
                let pg_parts: Vec<String> = pg_array
                    .iter()
                    .map(|pg| {
                        let target = pg["target_ms"].as_i64().unwrap_or(0);
                        let act = pg["actual_ms"].as_f64().unwrap_or(-1.0);
                        let actual = if act >= 0.0 {
                            format!("{act:.2}")
                        } else {
                            "-".into()
                        };
                        let utilization = if act > 0.0 && target > 0 {
                            let pct = (act / target as f64) * 100.0;
                            if pct > 90.0 {
                                format!(" {pct:.0}% HIGH")
                            } else if pct > 50.0 {
                                format!(" {pct:.0}% mod")
                            } else {
                                format!(" {pct:.0}% low")
                            }
                        } else {
                            String::new()
                        };
                        format!(
                            "{} target={}ms actual={}ms{utilization} overruns={}",
                            pg["name"], target, actual, pg["overrun_count"]
                        )
                    })
                    .collect();
                println!(
                    "  {} (host={}, health={}, conn={}) last_ok_ts={:?}",
                    plc["name"],
                    plc["host"],
                    plc["health"].as_str().unwrap_or("?"),
                    plc["connectivity"],
                    plc["last_ok_ts"]
                );
                for pg_line in pg_parts {
                    println!("    poll_group: {pg_line}");
                }
                if let Some(err) = plc["last_error"].as_str() {
                    if !err.is_empty() {
                        println!("    last_error: {err}");
                    }
                }
                for ms in plc["metric_sets"].as_array().unwrap_or(&Vec::new()) {
                    let ep = &ms["effective_poll"];
                    let poll_desc = match ep["mode"].as_str() {
                        Some("single") => {
                            let pgn = ep["poll_group_name"].as_str().unwrap_or("?");
                            let per = ep["period_ms"].as_i64().unwrap_or(0);
                            let hz = ep["rate_hz"].as_f64().unwrap_or(0.0);
                            format!("{pgn} {per}ms / {hz:.1}Hz")
                        }
                        _ => "off".into(),
                    };
                    println!(
                        "    set {} enabled={} tags={}/{} poll={}",
                        ms["name"], ms["enabled"], ms["tags_healthy"], ms["tags_total"], poll_desc
                    );
                }
            }
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

            // Last poll cycle finished timestamp for this PLC
            let last_poll_cycle_finished_ts: Option<i64> = conn
                .query_row(
                    "SELECT MAX(finished_ts) FROM poll_cycle WHERE plc_id = ?1",
                    rusqlite::params![id],
                    |r| r.get(0),
                )
                .unwrap_or(None);

            // Metric sets with member counts
            let mut ms_stmt = conn.prepare(
                "SELECT ms.id, ms.name, ms.enabled,
                        (SELECT COUNT(*) FROM metric_set_tag mst WHERE mst.metric_set_id = ms.id),
                        (SELECT COUNT(*) FROM metric_set_tag mst
                         JOIN tag t ON t.id = mst.tag_id
                         WHERE mst.metric_set_id = ms.id AND t.bad = 0 AND t.enabled = 1)
                 FROM metric_set ms
                 WHERE ms.plc_id = ?1
                 ORDER BY ms.id",
            )?;
            let ms_rows: Vec<(i64, String, bool, i64, i64)> = ms_stmt
                .query_map(rusqlite::params![id], |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, i64>(2)? != 0,
                        r.get::<_, i64>(3)?,
                        r.get::<_, i64>(4)?,
                    ))
                })?
                .filter_map(|x| x.ok())
                .collect();

            let mut metric_sets: Vec<serde_json::Value> = Vec::new();
            for (ms_id, ms_name, ms_enabled, tags_total, tags_healthy) in ms_rows {
                // Determine effective_poll for this metric set
                let enabled_in_set: i64 = conn
                    .query_row(
                        "SELECT COUNT(*) FROM metric_set_tag mst
                     JOIN tag t ON t.id = mst.tag_id
                     WHERE mst.metric_set_id = ?1 AND t.enabled = 1",
                        rusqlite::params![ms_id],
                        |r| r.get(0),
                    )
                    .unwrap_or(0);

                let effective_poll = if enabled_in_set == 0 {
                    serde_json::json!({
                        "mode": "none",
                        "poll_group_id": null,
                        "poll_group_name": null,
                        "period_ms": null,
                        "rate_hz": null,
                    })
                } else {
                    let mut pg_stmt_eff = conn.prepare(
                        "SELECT DISTINCT t.poll_group_id, pg.name, pg.period_ms
                         FROM metric_set_tag mst
                         JOIN tag t ON t.id = mst.tag_id AND t.enabled = 1
                         JOIN poll_group pg ON pg.id = t.poll_group_id
                         WHERE mst.metric_set_id = ?1
                         ORDER BY t.poll_group_id",
                    )?;
                    let groups: Vec<(i64, String, i64)> = pg_stmt_eff
                        .query_map(rusqlite::params![ms_id], |row| {
                            Ok((
                                row.get::<_, i64>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, i64>(2)?,
                            ))
                        })?
                        .filter_map(|x| x.ok())
                        .collect();

                    if groups.is_empty() {
                        serde_json::json!({
                            "mode": "none",
                            "poll_group_id": null,
                            "poll_group_name": null,
                            "period_ms": null,
                            "rate_hz": null,
                        })
                    } else if groups.len() == 1 {
                        let (pg_id, ref pg_name, period_ms) = groups[0];
                        let rate_hz = (1000.0 / period_ms as f64 * 10000.0).round() / 10000.0;
                        serde_json::json!({
                            "mode": "single",
                            "poll_group_id": pg_id,
                            "poll_group_name": pg_name,
                            "period_ms": period_ms,
                            "rate_hz": rate_hz,
                        })
                    } else {
                        serde_json::json!({
                            "mode": "mixed",
                            "poll_group_id": null,
                            "poll_group_name": null,
                            "period_ms": null,
                            "rate_hz": null,
                        })
                    }
                };

                metric_sets.push(serde_json::json!({
                    "name": ms_name,
                    "enabled": ms_enabled,
                    "tags_total": tags_total,
                    "tags_healthy": tags_healthy,
                    "effective_poll": effective_poll,
                }));
            }

            Ok(serde_json::json!({
                "name": name,
                "host": host,
                "enabled": enabled,
                "last_ok_ts": last_ok_ts,
                "last_error": last_error,
                "connectivity": connectivity,
                "last_poll_cycle_finished_ts": last_poll_cycle_finished_ts,
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
        Format::Table => {
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
                    let act = pg["actual_ms"].as_f64().unwrap_or(-1.0);
                    let target = pg["target_ms"].as_i64().unwrap_or(0);
                    let actual = if act >= 0.0 {
                        format!("{act:.2}")
                    } else {
                        "-".into()
                    };
                    let utilization = if act > 0.0 && target > 0 {
                        let pct = (act / target as f64) * 100.0;
                        if pct > 90.0 {
                            format!(" {pct:.0}% HIGH")
                        } else if pct > 50.0 {
                            format!(" {pct:.0}% mod")
                        } else {
                            format!(" {pct:.0}% low")
                        }
                    } else {
                        String::new()
                    };
                    println!(
                        "    poll_group {}: target={}ms actual={}ms{utilization} overruns={}",
                        pg["name"], target, actual, pg["overrun_count"]
                    );
                }
                for ms in plc["metric_sets"].as_array().unwrap_or(&Vec::new()) {
                    let poll_str = match ms["effective_poll"]["mode"].as_str() {
                        Some("single") => {
                            let ms_pg_name = ms["effective_poll"]["poll_group_name"]
                                .as_str()
                                .unwrap_or("?");
                            let period = ms["effective_poll"]["period_ms"].as_i64().unwrap_or(0);
                            let hz = ms["effective_poll"]["rate_hz"].as_f64().unwrap_or(0.0);
                            format!("{ms_pg_name} {period}ms / {hz:.1}Hz")
                        }
                        Some("mixed") => "mixed".to_string(),
                        Some("none") => "none".to_string(),
                        _ => "?".to_string(),
                    };
                    println!(
                        "    metric_set {} enabled={} tags_total={} tags_healthy={} poll={}",
                        ms["name"], ms["enabled"], ms["tags_total"], ms["tags_healthy"], poll_str
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
        for (name, addr, bc, bd) in problems.flatten() {
            let detail = bd.unwrap_or_default();
            println!("  bad_tag: {name} ({addr}) class={bc:?} detail=\"{detail}\"");
        }
    }

    Ok(ExitCode::Ok)
}

// ---------------------------------------------------------------------------
// Metric set handlers
// ---------------------------------------------------------------------------

fn handle_set_list(cli: &Cli, conn: &Connection, plc: Option<&str>) -> Result<ExitCode> {
    // Remote mode
    if let Some(base) = server_base(cli) {
        let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);
        let data = cli::remote::get_set_list(&base, plc, timeout)?;
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

    let (query, params): (&str, Vec<Box<dyn rusqlite::ToSql>>) = if let Some(p) = plc {
        ("SELECT vms.plc, vms.metric_set, vms.enabled, vms.tags_total, vms.tags_healthy, vms.tags_bad
          FROM v_metric_set_status vms WHERE vms.plc = ?1 ORDER BY vms.plc, vms.metric_set",
         vec![Box::new(p.to_string())])
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
    if let Some(base) = server_base(cli) {
        let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);
        let data = cli::remote::get_set_show(&base, set_name, timeout)?;
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
    _conn: &Connection,
    cli: &Cli,
    plc: &str,
    dataset: &str,
    sets: &[String],
) -> Result<ExitCode> {
    // Remote mode only
    let base = match server_base(cli) {
        Some(b) => b,
        None => {
            cli::format::print_json_error(
                "server_required",
                "collection-start requires a server. Set S7TRACE_SERVER or pass -r.",
                ExitCode::BadInput as i32,
            );
            return Ok(ExitCode::BadInput);
        }
    };
    let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);
    let req = serde_json::json!({
        "action": "collection_start",
        "plc": plc,
        "dataset": dataset,
        "sets": sets,
    });
    let id = cli::remote::enqueue_command(
        &base,
        "control",
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
        return Ok(ExitCode::Ok);
    }
    cli::format::print_json_error(
        &state,
        if err.is_empty() { &result } else { &err },
        ExitCode::PlcError as i32,
    );
    Ok(ExitCode::PlcError)
}

fn handle_collection_stop(_conn: &Connection, cli: &Cli, plc: &str) -> Result<ExitCode> {
    // Remote mode only
    let base = match server_base(cli) {
        Some(b) => b,
        None => {
            cli::format::print_json_error(
                "server_required",
                "collection-stop requires a server. Set S7TRACE_SERVER or pass -r.",
                ExitCode::BadInput as i32,
            );
            return Ok(ExitCode::BadInput);
        }
    };
    let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);
    let req = serde_json::json!({
        "action": "collection_stop",
        "plc": plc,
    });
    let id = cli::remote::enqueue_command(
        &base,
        "control",
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
        return Ok(ExitCode::Ok);
    }
    cli::format::print_json_error(
        &state,
        if err.is_empty() { &result } else { &err },
        ExitCode::PlcError as i32,
    );
    Ok(ExitCode::PlcError)
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
    if let Some(base) = server_base(cli) {
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
            &base,
            "control",
            &req.to_string(),
            Some("cli"),
            None,
            timeout,
        )?;
        println!("queued command_id={id}");
        let (state, result, ok, err) =
            cli::remote::wait_command(&base, id, timeout, std::time::Duration::from_millis(200))?;
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
            server_port: 7707,
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
            server_port: 7707,
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
            server_port: 7707,
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
