//! `tag-import` command handler (bulk signal import from YAML).

use anyhow::Result;
use rusqlite::Connection;

use crate::cli::args::Cli;
use crate::plc::create_plc_client;
use crate::server_base;
use crate::types::*;
use crate::{cli, db};

pub(crate) fn handle_tag_import(
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
