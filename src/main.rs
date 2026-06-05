use anyhow::{Context, Result};
use clap::Parser;
use rusqlite::Connection;

mod api;
mod cli;
mod db;
mod engine;
mod platform;
mod plc;
mod types;

use cli::args::{Cli, Command};
use cli::format::Format;
use plc::snap7::create_plc_client;
use types::*;

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_micros()
        .init();

    let cli = Cli::parse();

    match &cli.command {
        None | Some(Command::Server { dry_run: false, .. }) => {
            let (dry_run, http_bind, control_window) = match &cli.command {
                Some(Command::Server { dry_run, http_bind, control_window }) => {
                    (*dry_run, http_bind.clone(), *control_window)
                }
                _ => (false, None, false),
            };
            if dry_run {
                return dry_run_check(&cli.db);
            }
            dry_run_check(&cli.db)?;
            let config = engine::server::ServerConfig {
                db_path: cli.db.clone(),
                http_bind,
                control_window,
            };
            engine::server::run_server_with_config(config)
        }
        Some(Command::Server { .. }) => {
            dry_run_check(&cli.db)
        }
        Some(_) => {
            // CLI mode — open DB and dispatch command
            let conn = db::schema::open(&cli.db)
                .with_context(|| format!("opening database {} for s7trace", cli.db))?;
            let exit_code = dispatch(&cli, &conn)?;
            std::process::exit(exit_code as i32);
        }
    }
}

/// Validate configuration and exit. Returns Ok(true) if check passed.
fn dry_run_check(db_path: &str) -> Result<()> {
    let conn = db::schema::open(db_path)?;
    let plc_count: i64 = conn.query_row("SELECT COUNT(*) FROM plc", [], |r| r.get(0))?;
    let pg_count: i64 = conn.query_row("SELECT COUNT(*) FROM poll_group", [], |r| r.get(0))?;
    let tag_count: i64 = conn.query_row("SELECT COUNT(*) FROM tag WHERE enabled = 1 AND bad = 0", [], |r| r.get(0))?;
    let set_count: i64 = conn.query_row("SELECT COUNT(*) FROM metric_set WHERE enabled = 1", [], |r| r.get(0))?;

    println!("Database: {db_path}");
    println!("  PLCs:         {plc_count}");
    println!("  Poll groups:  {pg_count}");
    println!("  Active tags:  {tag_count}");
    println!("  Enabled sets: {set_count}");

    if plc_count == 0 {
        eprintln!("warning: no PLCs configured. Add one with:");
        eprintln!("  s7trace sql \"INSERT INTO plc (name, host) VALUES ('my-plc', '192.168.0.1')\" --admin");
    }
    if tag_count == 0 {
        eprintln!("warning: no active tags. The server will start no pollers.");
    }

    println!("Configuration looks valid.");
    Ok(())
}

fn dispatch(cli: &Cli, conn: &Connection) -> Result<ExitCode> {
    let cmd = cli.command.as_ref().expect("dispatch called without command");
    match cmd {
        Command::Sql { query, format: fmt, limit, timeout_ms: _ } => {
            handle_sql(conn, query, fmt, *limit)
        }
        Command::AdminSql { query } => handle_admin_sql(conn, query),
        Command::CommandList { state } => handle_command_list(conn, state.as_deref()),
        Command::CommandWait { command_id, poll_ms, timeout_ms } => {
            handle_command_wait(conn, *command_id, *poll_ms, *timeout_ms)
        }
        Command::CommandResult { command_id, format: fmt } => {
            handle_command_result(conn, *command_id, fmt)
        }
        Command::TagAdd { plc, name, address, data_type, poll_group, store_mode, storage_hook, value_role, metric_set, async_mode } => {
            handle_tag_add(cli, conn, plc, name, address, data_type, poll_group, store_mode, storage_hook, value_role, metric_set.as_deref(), *async_mode)
        }
        Command::TagUpdate { tag_id, address, data_type, poll_group, store_mode, storage_hook } => {
            handle_tag_update(conn, cli, *tag_id, address.as_deref(), data_type.as_deref(), poll_group.as_deref(), store_mode.as_deref(), storage_hook.as_deref())
        }
        Command::TagDisable { tag_id } => handle_tag_disable(conn, *tag_id),
        Command::TagRevalidate { tag_id } => handle_tag_revalidate(conn, cli, *tag_id),
        Command::Read { plc, address, data_type, format: fmt } => {
            handle_read(cli, conn, plc, address, data_type, fmt)
        }
        Command::ReadRaw { plc, db, start, len, format: fmt, save_probe } => {
            handle_read_raw(conn, cli, plc, *db, *start, *len, fmt, *save_probe)
        }
        Command::Status { format: fmt } => handle_status(cli, conn, fmt),
        Command::SetCreate { plc, name, description } => {
            handle_set_create(conn, plc, name, description.as_deref())
        }
        Command::SetAddTag { set_name, tag, from_sql, from_file } => {
            handle_set_add_tag(conn, set_name, tag.as_deref(), from_sql.as_deref(), from_file.as_deref())
        }
        Command::SetRemoveTag { set_name, tag, from_sql } => {
            handle_set_remove_tag(conn, set_name, tag.as_deref(), from_sql.as_deref())
        }
        Command::SetEnable { set_name, async_mode } => {
            handle_set_enable(conn, cli, set_name, *async_mode)
        }
        Command::SetDisable { set_name } => handle_set_disable(conn, cli, set_name),
        Command::SetList { plc } => handle_set_list(conn, plc.as_deref()),
        Command::SetShow { set_name } => handle_set_show(conn, set_name),
        Command::CollectionStart { plc, dataset, sets } => {
            handle_collection_start(conn, cli, plc, dataset, sets)
        }
        Command::CollectionStop { plc } => handle_collection_stop(conn, cli, plc),
        Command::HistoryClean { plc, before_ts, before, batch_size } => {
            handle_history_clean(conn, cli, plc, *before_ts, before.as_deref(), *batch_size)
        }
        Command::HistoryDeleteDataset { dataset } => {
            handle_history_delete_dataset(conn, cli, dataset)
        }
        Command::HistoryVacuum { force } => handle_history_vacuum(conn, *force),
        _ => unreachable!("dispatch called with Server or unknown command"),
    }
}

// ---------------------------------------------------------------------------
// SQL handlers
// ---------------------------------------------------------------------------

fn handle_sql(conn: &Connection, query: &str, fmt: &str, limit: u64) -> Result<ExitCode> {
    // Ensure read-only: reject anything that isn't a SELECT or WITH
    let trimmed = query.trim().to_uppercase();
    if !trimmed.starts_with("SELECT") && !trimmed.starts_with("WITH") && !trimmed.starts_with("PRAGMA") {
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
            .map(|i| {
                match row.get::<_, rusqlite::types::Value>(i) {
                    Ok(v) => match v {
                        rusqlite::types::Value::Null => serde_json::Value::Null,
                        rusqlite::types::Value::Integer(n) => serde_json::Value::Number(n.into()),
                        rusqlite::types::Value::Real(f) => {
                            serde_json::Number::from_f64(f)
                                .map(serde_json::Value::Number)
                                .unwrap_or(serde_json::Value::Null)
                        }
                        rusqlite::types::Value::Text(s) => serde_json::Value::String(s),
                        rusqlite::types::Value::Blob(b) => {
                            serde_json::Value::String(format!("{b:02x?}"))
                        }
                    },
                    Err(_) => serde_json::Value::Null,
                }
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
            let results: Vec<Vec<serde_json::Value>> = rows
                .filter_map(|r| r.ok())
                .collect();
            println!("{}", serde_json::to_string_pretty(&results).unwrap_or_default());
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

fn handle_admin_sql(conn: &Connection, query: &str) -> Result<ExitCode> {
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

fn handle_command_list(conn: &Connection, state: Option<&str>) -> Result<ExitCode> {
    let commands = db::command_queue::list_pending(conn, state)?;
    println!("{:<8} {:<12} {:<10} {:<20}", "ID", "State", "Kind", "Created");
    for cmd in &commands {
        println!(
            "{:<8} {:<12?} {:<10?} {:<20}",
            cmd.id, cmd.state, cmd.kind, cmd.created_ts
        );
    }
    Ok(ExitCode::Ok)
}

fn handle_command_wait(conn: &Connection, command_id: i64, poll_ms: u64, timeout_ms: u64) -> Result<ExitCode> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
    let poll_dur = std::time::Duration::from_millis(poll_ms);

    while std::time::Instant::now() < deadline {
        if let Some(result) = db::command_queue::get_result(conn, command_id)? {
            handle_command_result_inner(&result, &Format::Text)?;
            return if result.ok { Ok(ExitCode::Ok) } else { Ok(ExitCode::PlcError) };
        }
        std::thread::sleep(poll_dur);
    }

    eprintln!("timeout waiting for command {command_id}");
    Ok(ExitCode::Overload)
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
    Ok(if result.ok { ExitCode::Ok } else { ExitCode::PlcError })
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
    let dt: DataType = data_type.parse().map_err(|e: String| anyhow::anyhow!("{e}"))?;
    let _sm: StoreMode = store_mode.parse().map_err(|e: String| anyhow::anyhow!("{e}"))?;
    let _sh: StorageHook = storage_hook.parse().map_err(|e: String| anyhow::anyhow!("{e}"))?;
    let addr = S7Address::parse(address)
        .map_err(|e| anyhow::anyhow!("invalid address: {e}"))?;

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
            base, "control", &req.to_string(), Some("cli"), None, timeout,
        )?;
        println!("queued command_id={id}");
        let (state, result, ok, err) = cli::remote::wait_command(
            base, id, timeout, std::time::Duration::from_millis(200),
        )?;
        if ok {
            println!("success {result}");
        } else {
            println!("{state}: {result} {err}");
            return Ok(ExitCode::BadInput);
        }
        return Ok(ExitCode::Ok);
    }

    let inserted = db::validation::insert_tag(
        conn, plc, name, &addr, dt, poll_group, store_mode, storage_hook,
        value_role, metric_set,
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
        let plc_row: (String, String, i64, i64) = conn.query_row(
            "SELECT host, name, rack, slot FROM plc WHERE id = ?1",
            rusqlite::params![inserted.plc_id],
            |row| Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            )),
        ).map_err(|_| anyhow::anyhow!("PLC '{plc}' not found"))?;
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
            println!(
                "success {val_str} apply_state=pending tag_id={tag_id} set={set_name}"
            );
        } else {
            let bc = result.bad_class.as_ref().map(|b| b.to_string()).unwrap_or_default();
            let det = result.detail.as_deref().unwrap_or("");
            println!(
                "bad bad_class={bc} detail=\"{det}\" tag_id={tag_id} set={set_name}"
            );
        }
    } else {
        // Enqueue as command
        let req = serde_json::json!({
            "action": "tag_add",
            "plc_id": inserted.plc_id,
            "tag_id": tag_id,
        });
        let cmd_id = db::command_queue::enqueue(
            conn, CommandKind::Control, &req.to_string(), Some(inserted.plc_id), None, None,
        )?;
        println!("queued command_id={cmd_id}");
    }

    Ok(ExitCode::Ok)
}

fn handle_tag_update(
    conn: &Connection,
    _cli: &Cli,
    tag_id: i64,
    address: Option<&str>,
    data_type: Option<&str>,
    poll_group: Option<&str>,
    store_mode: Option<&str>,
    storage_hook: Option<&str>,
) -> Result<ExitCode> {
    let now = now_us();

    if let Some(addr_str) = address {
        let addr = S7Address::parse(addr_str)
            .map_err(|e| anyhow::anyhow!("invalid address: {e}"))?;
        conn.execute(
            "UPDATE tag SET address = ?1, db_num = ?2, byte_offset = ?3, updated_ts = ?4, desired_version = desired_version + 1 WHERE id = ?5",
            rusqlite::params![addr_str, addr.db_num, addr.byte_offset, now, tag_id],
        )?;
    }

    if let Some(dt) = data_type {
        let byte_size = dt.parse::<DataType>()
            .map_err(|e: String| anyhow::anyhow!("{e}"))?
            .byte_size() as i64;
        conn.execute(
            "UPDATE tag SET data_type = ?1, byte_size = ?2, updated_ts = ?3, desired_version = desired_version + 1 WHERE id = ?4",
            rusqlite::params![dt, byte_size, now, tag_id],
        )?;
    }

    if let Some(pg) = poll_group {
        let pg_id: i64 = conn.query_row(
            "SELECT id FROM poll_group WHERE name = ?1",
            rusqlite::params![pg],
            |row| row.get(0),
        ).map_err(|_| anyhow::anyhow!("poll group '{pg}' not found"))?;
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
        None, None, None, Some(tag_id),
        "tag_updated",
        db::config_apply::PLAN_COOLDOWN_MS,
        None,
    )?;

    println!("success tag_id={tag_id} apply_state=pending");
    Ok(ExitCode::Ok)
}

fn handle_tag_disable(conn: &Connection, tag_id: i64) -> Result<ExitCode> {
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
        None, None, None, Some(tag_id),
        "tag_disabled",
        db::config_apply::PLAN_COOLDOWN_MS,
        None,
    )?;

    println!("success tag_id={tag_id} enabled=0 apply_state=pending");
    Ok(ExitCode::Ok)
}

fn handle_tag_revalidate(
    conn: &Connection,
    _cli: &Cli,
    tag_id: i64,
) -> Result<ExitCode> {
    // Enqueue a revalidation command
    let req = serde_json::json!({
        "action": "tag_revalidate",
        "tag_id": tag_id,
    });
    let cmd_id = db::command_queue::enqueue(
        conn, CommandKind::Control, &req.to_string(), None, None, None,
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
            base, "plc_read", &req.to_string(), Some("cli"), None, timeout,
        )?;
        let (state, result, ok, err) = cli::remote::wait_command(
            base, id, timeout, std::time::Duration::from_millis(200),
        )?;
        if ok {
            println!("{result}");
        } else {
            eprintln!("{state}: {result} {err}");
            return Ok(ExitCode::BadInput);
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
        conn, CommandKind::PlcRead, &req.to_string(), None, None, None,
    )?;
    println!("queued command_id={cmd_id}");
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
        conn, CommandKind::PlcRead, &req.to_string(), None, None, None,
    )?;
    println!("queued command_id={cmd_id}");
    if save_probe {
        println!("info: raw probe will be saved to the database");
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
    let mut stmt = conn.prepare(
        "SELECT id, name, host, enabled, last_ok_ts, last_error, connectivity FROM plc"
    )?;
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
                "SELECT name, started_ts FROM dataset WHERE plc_id = ?1 AND state = 'active'"
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
                 ORDER BY pg.id"
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
                 ORDER BY ms.id"
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

    let bad_tags: i64 = conn.query_row(
        "SELECT COUNT(*) FROM tag WHERE bad = 1",
        [],
        |row| row.get(0),
    ).unwrap_or(0);

    let active_tags: i64 = conn.query_row(
        "SELECT COUNT(*) FROM v_active_tag",
        [],
        |row| row.get(0),
    ).unwrap_or(0);

    let pending_count = db::config_apply::pending_count(conn).unwrap_or(0);
    let command_pending: i64 = conn.query_row(
        "SELECT COUNT(*) FROM command_request WHERE state = 'pending'",
        [],
        |row| row.get(0),
    ).unwrap_or(0);
    let command_running: i64 = conn.query_row(
        "SELECT COUNT(*) FROM command_request WHERE state = 'running'",
        [],
        |row| row.get(0),
    ).unwrap_or(0);

    let status = serde_json::json!({
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
            println!("PLCs:");
            for plc in &plcs {
                println!(
                    "  {} (host={}, enabled={}, conn={}) last_ok={:?}",
                    plc["name"], plc["host"], plc["enabled"], plc["connectivity"], plc["last_ok_ts"]
                );
                if let Some(err) = plc["last_error"].as_str() {
                    if !err.is_empty() {
                        println!("    last_error: {err}");
                    }
                }
                for pg in plc["poll_groups"].as_array().unwrap_or(&Vec::new()) {
                    let actual = pg["actual_ms"].as_f64().map(|f| format!("{f:.1}")).unwrap_or_else(|| "-".into());
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
                    println!(
                        "    dataset {} started={:?}",
                        ds["name"], ds["started_ts"]
                    );
                }
            }
            println!("Tags: active={active_tags}, bad={bad_tags}");
            println!("Command queue pending={command_pending} running={command_running}");
            println!("Config apply pending: {pending_count}");
        }
        _ => {
            println!("{}", serde_json::to_string_pretty(&status).unwrap_or_default());
        }
    }

    // Show problems (bad tags)
    let mut pstmt = conn.prepare(
        "SELECT t.name, t.address, t.bad_class, t.bad_detail
         FROM tag t WHERE t.bad = 1 LIMIT 20"
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
    conn: &Connection,
    plc: &str,
    name: &str,
    description: Option<&str>,
) -> Result<ExitCode> {
    let plc_id: i64 = conn.query_row(
        "SELECT id FROM plc WHERE name = ?1",
        rusqlite::params![plc],
        |row| row.get(0),
    ).map_err(|_| anyhow::anyhow!("PLC '{plc}' not found"))?;

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
    set_name: &str,
    tag: Option<&str>,
    from_sql: Option<&str>,
    from_file: Option<&str>,
) -> Result<ExitCode> {
    // Resolve set
    let set: (i64, i64) = conn.query_row(
        "SELECT ms.id, ms.plc_id FROM metric_set ms WHERE ms.name = ?1",
        rusqlite::params![set_name],
        |row| Ok((row.get(0)?, row.get(1)?)),
    ).map_err(|_| anyhow::anyhow!("metric set '{set_name}' not found"))?;
    let (set_id, plc_id) = set;

    let now = now_us();

    // Build the list of tag IDs to add, in priority order: file > sql > tag.
    let mut tag_ids: Vec<i64> = Vec::new();

    if let Some(path) = from_file {
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("reading {path}: {e}"))?;
        for line in content.lines() {
            let name = line.trim();
            if name.is_empty() || name.starts_with('#') {
                continue;
            }
            let id: i64 = conn.query_row(
                "SELECT id FROM tag WHERE plc_id = ?1 AND name = ?2",
                rusqlite::params![plc_id, name],
                |row| row.get(0),
            ).map_err(|_| anyhow::anyhow!("tag '{name}' not found"))?;
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
                ).map_err(|_| anyhow::anyhow!("tag '{piece}' not found"))?
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
    set_name: &str,
    tag: Option<&str>,
    from_sql: Option<&str>,
) -> Result<ExitCode> {
    let set_id: i64 = conn.query_row(
        "SELECT id FROM metric_set WHERE name = ?1",
        rusqlite::params![set_name],
        |row| row.get(0),
    ).map_err(|_| anyhow::anyhow!("metric set '{set_name}' not found"))?;

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
        None, None, Some(set_id), None,
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
            conn, CommandKind::Control, &req.to_string(), None, None, None,
        )?;
        println!("queued command_id={cmd_id}");
    } else {
        println!("success set={set_name} enabled=1 apply_state=pending");
    }

    Ok(ExitCode::Ok)
}

fn handle_set_disable(
    conn: &Connection,
    _cli: &Cli,
    set_name: &str,
) -> Result<ExitCode> {
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
        None, None, Some(set_id), None,
        "metric_set_disabled",
        db::config_apply::PLAN_COOLDOWN_MS,
        None,
    )?;

    println!("success set={set_name} enabled=0 apply_state=pending");
    Ok(ExitCode::Ok)
}

fn handle_set_list(conn: &Connection, plc: Option<&str>) -> Result<ExitCode> {
    let query = if let Some(plc_name) = plc {
        format!(
            "SELECT vms.plc, vms.metric_set, vms.enabled, vms.tags_total, vms.tags_healthy, vms.tags_bad
             FROM v_metric_set_status vms WHERE vms.plc = '{}'",
            plc_name.replace('\'', "''")
        )
    } else {
        "SELECT vms.plc, vms.metric_set, vms.enabled, vms.tags_total, vms.tags_healthy, vms.tags_bad
         FROM v_metric_set_status vms ORDER BY vms.plc, vms.metric_set".to_string()
    };

    let mut stmt = conn.prepare(&query)?;
    let rows = stmt.query_map([], |row| {
        let plc: String = row.get(0)?;
        let name: String = row.get(1)?;
        let enabled: bool = row.get::<_, i64>(2)? != 0;
        let total: i64 = row.get(3)?;
        let healthy: i64 = row.get(4)?;
        let bad: i64 = row.get(5)?;
        Ok((plc, name, enabled, total, healthy, bad))
    })?;

    println!("{:<20} {:<30} {:<8} {:<6} {:<8} {:<4}", "PLC", "Set", "Enabled", "Total", "Healthy", "Bad");
    for row in rows {
        let (plc, name, enabled, total, healthy, bad) = row?;
        println!("{plc:<20} {name:<30} {enabled:<8} {total:<6} {healthy:<8} {bad:<4}");
    }

    Ok(ExitCode::Ok)
}

fn handle_set_show(conn: &Connection, set_name: &str) -> Result<ExitCode> {
    let set_info = conn.query_row(
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
    ).map_err(|_| anyhow::anyhow!("metric set '{set_name}' not found"))?;

    let (_id, _plc_id, _name, enabled, _desc) = &set_info;

    println!("Set: {set_name}");
    println!("Enabled: {enabled}");

    // List members
    let mut stmt = conn.prepare(
        "SELECT t.id, t.name, t.address, t.data_type, t.bad, t.bad_class
         FROM v_metric_set_member vmsm
         JOIN tag t ON t.id = vmsm.tag_id
         WHERE vmsm.metric_set = ?1"
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
    _cli: &Cli,
    plc: &str,
    dataset: &str,
    sets: &[String],
) -> Result<ExitCode> {
    let plc_id: i64 = conn.query_row(
        "SELECT id FROM plc WHERE name = ?1",
        rusqlite::params![plc],
        |row| row.get(0),
    ).map_err(|_| anyhow::anyhow!("PLC '{plc}' not found"))?;

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
        conn, CommandKind::Control, &req.to_string(), Some(plc_id), None, None,
    )?;
    println!("queued command_id={cmd_id}");
    Ok(ExitCode::Ok)
}

fn handle_collection_stop(
    conn: &Connection,
    _cli: &Cli,
    plc: &str,
) -> Result<ExitCode> {
    let plc_id: i64 = conn.query_row(
        "SELECT id FROM plc WHERE name = ?1",
        rusqlite::params![plc],
        |row| row.get(0),
    ).map_err(|_| anyhow::anyhow!("PLC '{plc}' not found"))?;

    let req = serde_json::json!({
        "action": "collection_stop",
        "plc_id": plc_id,
        "plc": plc,
    });
    let cmd_id = db::command_queue::enqueue(
        conn, CommandKind::Control, &req.to_string(), Some(plc_id), None, None,
    )?;
    println!("queued command_id={cmd_id}");
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
    let plc_id: i64 = conn.query_row(
        "SELECT id FROM plc WHERE name = ?1",
        rusqlite::params![plc],
        |row| row.get(0),
    ).map_err(|_| anyhow::anyhow!("PLC '{plc}' not found"))?;

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
            let dt = chrono::NaiveDateTime::parse_from_str(
                &iso.replace('T', " "),
                "%Y-%m-%d %H:%M:%S",
            ).map_err(|e| anyhow::anyhow!("invalid datetime '{iso}': {e}"))?;
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
        conn, CommandKind::Admin, &req.to_string(), Some(plc_id), None, None,
    )?;
    println!("queued command_id={cmd_id}");
    Ok(ExitCode::Ok)
}

fn handle_history_delete_dataset(
    conn: &Connection,
    _cli: &Cli,
    dataset: &str,
) -> Result<ExitCode> {
    // Check dataset exists and is stopped
    let state: String = conn.query_row(
        "SELECT state FROM dataset WHERE name = ?1",
        rusqlite::params![dataset],
        |row| row.get(0),
    ).map_err(|_| anyhow::anyhow!("dataset '{dataset}' not found"))?;

    if state == "active" {
        eprintln!("error: dataset '{dataset}' is active. Stop collection first.");
        return Ok(ExitCode::BadInput);
    }

    let req = serde_json::json!({
        "action": "delete_dataset",
        "dataset": dataset,
    });
    let cmd_id = db::command_queue::enqueue(
        conn, CommandKind::Admin, &req.to_string(), None, None, None,
    )?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handle_sql_select() {
        let conn = db::schema::open_memory().unwrap();
        let result = handle_sql(&conn, "SELECT 1 AS col", "json", 10);
        assert!(result.is_ok());
    }

    #[test]
    fn test_handle_sql_rejects_write() {
        let conn = db::schema::open_memory().unwrap();
        let result = handle_sql(&conn, "DELETE FROM tag", "json", 10);
        assert!(result.is_ok());
        assert_eq!(result.unwrap() as i32, ExitCode::BadInput as i32);
    }

    #[test]
    fn test_set_create_and_list() {
        let conn = db::schema::open_memory().unwrap();
        // Need a PLC first
        conn.execute(
            "INSERT INTO plc (name, host) VALUES ('test-plc', '127.0.0.1')",
            [],
        ).unwrap();

        handle_set_create(&conn, "test-plc", "my_set", None).unwrap();
        let id: i64 = conn.query_row(
            "SELECT id FROM metric_set WHERE name = 'my_set'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert!(id > 0);
    }

    #[test]
    fn test_tag_disable_unknown() {
        let conn = db::schema::open_memory().unwrap();
        let result = handle_tag_disable(&conn, 999);
        assert!(result.is_ok());
        assert_eq!(result.unwrap() as i32, ExitCode::BadInput as i32);
    }

    #[test]
    fn test_set_add_tag_bulk_csv() {
        let conn = db::schema::open_memory().unwrap();
        conn.execute(
            "INSERT INTO plc (name, host) VALUES ('plc1', '127.0.0.1')",
            [],
        ).unwrap();
        // Need poll_group for the FK
        conn.execute(
            "INSERT INTO poll_group (name, period_ms) VALUES ('g1', 1000)",
            [],
        ).unwrap();
        for n in ["t1", "t2", "t3"] {
            conn.execute(
                "INSERT INTO tag (plc_id, name, address, data_type, poll_group_id,
                                  db_num, byte_offset, byte_size, created_ts, updated_ts)
                 VALUES (1, ?1, 'DB1.DBW0', 'i16', 1, 1, 0, 2, 1000000, 1000000)",
                rusqlite::params![n],
            ).unwrap();
        }
        handle_set_create(&conn, "plc1", "my_set", None).unwrap();
        handle_set_add_tag(&conn, "my_set", Some("t1,t2,t3"), None, None).unwrap();
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM metric_set_tag WHERE metric_set_id = 1",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(n, 3);
    }

    #[test]
    fn test_set_add_tag_from_file() {
        let conn = db::schema::open_memory().unwrap();
        conn.execute(
            "INSERT INTO plc (name, host) VALUES ('plc1', '127.0.0.1')",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO poll_group (name, period_ms) VALUES ('g1', 1000)",
            [],
        ).unwrap();
        for n in ["alpha", "beta"] {
            conn.execute(
                "INSERT INTO tag (plc_id, name, address, data_type, poll_group_id,
                                  db_num, byte_offset, byte_size, created_ts, updated_ts)
                 VALUES (1, ?1, 'DB1.DBW0', 'i16', 1, 1, 0, 2, 1000000, 1000000)",
                rusqlite::params![n],
            ).unwrap();
        }
        handle_set_create(&conn, "plc1", "s2", None).unwrap();

        let path = std::env::temp_dir().join(format!("s7trace_set_tags_{}.txt", std::process::id()));
        std::fs::write(&path, "# comment line\nalpha\n\nbeta\n").unwrap();
        handle_set_add_tag(&conn, "s2", None, None, Some(path.to_str().unwrap())).unwrap();
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM metric_set_tag WHERE metric_set_id = 1",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(n, 2);
    }

    #[test]
    fn test_plc_connectivity_default_and_flip() {
        let conn = db::schema::open_memory().unwrap();
        conn.execute(
            "INSERT INTO plc (name, host) VALUES ('p1', '127.0.0.1')",
            [],
        ).unwrap();
        // Default is 'unknown'
        let s: String = conn.query_row(
            "SELECT connectivity FROM plc WHERE name = 'p1'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(s, "unknown");
        // Flip to offline (simulates failed write_samples)
        conn.execute(
            "UPDATE plc SET connectivity = 'offline' WHERE id = 1",
            [],
        ).unwrap();
        let s: String = conn.query_row(
            "SELECT connectivity FROM plc WHERE id = 1",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(s, "offline");
    }
}
