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
pub(crate) fn server_base(cli: &Cli) -> Option<String> {
    cli.remote_server
        .as_ref()
        .map(|s| cli::remote::resolve_url(s, cli.server_port))
}

mod api;
mod cli;
mod codec;
mod db;
mod engine;
mod handlers;
mod platform;
mod plc;
mod types;
mod version;

use cli::args::{Cli, Command, EventAction};
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
    use tracing_subscriber::EnvFilter;
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
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
        } => crate::handlers::sql::handle_sql(conn, cli, query, fmt, *limit),
        Command::AdminSql { query } => crate::handlers::sql::handle_admin_sql(cli, conn, query),
        Command::CommandList { state, format: fmt } => {
            crate::handlers::commands::handle_command_list(conn, state.as_deref(), fmt)
        }
        Command::CommandWait {
            command_id,
            poll_ms,
            timeout_ms,
        } => {
            crate::handlers::commands::handle_command_wait(conn, *command_id, *poll_ms, *timeout_ms)
        }
        Command::CommandResult {
            command_id,
            format: fmt,
        } => crate::handlers::commands::handle_command_result(conn, *command_id, fmt),
        Command::CommandCancel { command_id } => {
            crate::handlers::commands::handle_command_cancel(conn, cli, *command_id)
        }
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
        } => crate::handlers::reads::handle_read(cli, conn, plc, address, data_type, fmt),
        Command::ReadRaw {
            plc,
            db,
            start,
            len,
            format: fmt,
            save_probe,
        } => crate::handlers::reads::handle_read_raw(
            conn,
            cli,
            plc,
            *db,
            *start,
            *len,
            fmt,
            *save_probe,
        ),
        Command::Status { format: fmt } => handlers::status::handle_status(cli, conn, fmt),
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
        Command::SetList { plc } => {
            crate::handlers::sets::handle_set_list(cli, conn, plc.as_deref())
        }
        Command::SetShow { set_name, set_flag } => crate::handlers::sets::handle_set_show(
            conn,
            cli,
            set_name.as_deref(),
            set_flag.as_deref(),
        ),
        Command::SetDelete { set_name } => run_op(
            cli,
            conn,
            engine::ops::Op::SetDelete {
                set_name: set_name.clone(),
            },
        ),
        Command::CollectionStart { plc, dataset, sets } => {
            crate::handlers::collection::handle_collection_start(conn, cli, plc, dataset, sets)
        }
        Command::CollectionStop { plc } => {
            crate::handlers::collection::handle_collection_stop(conn, cli, plc)
        }
        Command::HistoryClean {
            plc,
            before_ts,
            before,
            batch_size,
        } => crate::handlers::history::handle_history_clean(
            conn,
            cli,
            plc,
            *before_ts,
            before.as_deref(),
            *batch_size,
        ),
        Command::HistoryDeleteDataset { dataset } => {
            crate::handlers::history::handle_history_delete_dataset(conn, cli, dataset)
        }
        Command::HistoryVacuum { force } => {
            crate::handlers::history::handle_history_vacuum(conn, *force)
        }
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
        Command::Event { action } => crate::handlers::events::handle_event_add(conn, cli, action),
        Command::Note { text, plc, dataset } => crate::handlers::events::handle_event_add(
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
        } => handlers::import::handle_tag_import(
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

// ---------------------------------------------------------------------------
// Command queue handlers
// ---------------------------------------------------------------------------

/// Format a microsecond timestamp as a human-readable UTC string.
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

// ---------------------------------------------------------------------------
// Read handlers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Status handler
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Metric set handlers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Collection handlers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// History handlers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// PLC / poll group / bulk-import handlers
// ---------------------------------------------------------------------------

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
        let result = crate::handlers::sql::handle_sql(&conn, &cli, "SELECT 1 AS col", "json", 10);
        assert!(result.is_ok());
    }

    #[test]
    fn test_handle_sql_rejects_write() {
        let conn = db::schema::open_memory().unwrap();
        let cli = local_cli();
        let result = crate::handlers::sql::handle_sql(&conn, &cli, "DELETE FROM tag", "json", 10);
        assert!(result.is_ok());
        assert_eq!(result.unwrap() as i32, ExitCode::BadInput as i32);
    }

    #[test]
    fn test_admin_sql_rejects_remote_mode() {
        let conn = db::schema::open_memory().unwrap();
        let mut cli = local_cli();
        cli.remote_server = Some("http://127.0.0.1:8080".into());
        cli.local_db = false;
        let result =
            crate::handlers::sql::handle_admin_sql(&cli, &conn, "UPDATE plc SET enabled = 0");
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
        let r = handlers::import::handle_tag_import(
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
        let r = handlers::import::handle_tag_import(
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
