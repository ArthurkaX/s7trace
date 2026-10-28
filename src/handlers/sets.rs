//! `set-list` / `set-show` command handlers.
//!
//! Local and remote mode build the same JSON (the server's `/v1/sets` and
//! `/v1/set/{name}` shapes) and render it the same way.

use anyhow::Result;
use rusqlite::Connection;

use crate::cli;
use crate::cli::args::Cli;
use crate::cli::output::{self, fail};
use crate::server_base;
use crate::types::*;

pub(crate) fn handle_set_list(cli: &Cli, conn: &Connection, plc: Option<&str>) -> Result<ExitCode> {
    let items = match server_base(cli) {
        Some(base) => {
            let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);
            cli::remote::get_set_list(&base, plc, timeout)?
        }
        None => local_set_list(conn, plc)?,
    };
    output::emit(cli, items.clone(), None, || {
        println!(
            "{:<20} {:<30} {:<8} {:<6} {:<8} {:<4}",
            "PLC", "Set", "Enabled", "Total", "Healthy", "Bad"
        );
        for item in items.as_array().into_iter().flatten() {
            let plc_name = item["plc"].as_str().unwrap_or("?");
            let name = item["name"].as_str().unwrap_or("?");
            let enabled = item["enabled"].as_bool().unwrap_or(false);
            let total = item["tags_total"].as_i64().unwrap_or(0);
            let healthy = item["tags_healthy"].as_i64().unwrap_or(0);
            let bad = item["tags_bad"].as_i64().unwrap_or(0);
            println!("{plc_name:<20} {name:<30} {enabled:<8} {total:<6} {healthy:<8} {bad:<4}");
        }
    });
    Ok(ExitCode::Ok)
}

fn local_set_list(conn: &Connection, plc: Option<&str>) -> Result<serde_json::Value> {
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
            Ok(serde_json::json!({
                "plc": row.get::<_, String>(0)?,
                "name": row.get::<_, String>(1)?,
                "enabled": row.get::<_, i64>(2)? != 0,
                "tags_total": row.get::<_, i64>(3)?,
                "tags_healthy": row.get::<_, i64>(4)?,
                "tags_bad": row.get::<_, i64>(5)?,
            }))
        },
    )?;
    Ok(serde_json::Value::Array(
        rows.collect::<rusqlite::Result<Vec<_>>>()?,
    ))
}

pub(crate) fn handle_set_show(
    conn: &Connection,
    cli: &Cli,
    set_name_pos: Option<&str>,
    set_flag: Option<&str>,
    plc: Option<&str>,
) -> Result<ExitCode> {
    let set_name = set_flag.or(set_name_pos).ok_or_else(|| {
        fail(
            ErrorCode::InvalidArgument,
            "metric set name required (positional or --set)",
        )
    })?;
    let data = match server_base(cli) {
        Some(base) => {
            let timeout = std::time::Duration::from_millis(cli.remote_timeout_ms);
            cli::remote::get_set_show(&base, set_name, plc, timeout)?
        }
        None => local_set_show(conn, set_name, plc)?,
    };
    output::emit(cli, data.clone(), None, || {
        println!("Set: {}", data["name"].as_str().unwrap_or(set_name));
        if let Some(p) = data["plc"].as_str() {
            println!("Plc: {p}");
        }
        println!("Enabled: {}", data["enabled"].as_bool().unwrap_or(false));
        println!("\nMembers:");
        for member in data["members"].as_array().into_iter().flatten() {
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
    });
    Ok(ExitCode::Ok)
}

fn local_set_show(
    conn: &Connection,
    set_name: &str,
    plc: Option<&str>,
) -> Result<serde_json::Value> {
    // Metric set names are unique only per PLC. If a PLC is given, scope the
    // lookup to it; otherwise, fail clearly when the name is ambiguous.
    let (query, params): (&str, Vec<Box<dyn rusqlite::ToSql>>) = if let Some(p) = plc {
        (
            "SELECT ms.id, ms.plc_id, plc.name, ms.enabled, ms.description
          FROM metric_set ms
          JOIN plc ON plc.id = ms.plc_id
          WHERE ms.name = ?1 AND plc.name = ?2",
            vec![Box::new(set_name.to_string()), Box::new(p.to_string())],
        )
    } else {
        (
            "SELECT ms.id, ms.plc_id, plc.name, ms.enabled, ms.description
          FROM metric_set ms
          JOIN plc ON plc.id = ms.plc_id
          WHERE ms.name = ?1",
            vec![Box::new(set_name.to_string())],
        )
    };

    let mut stmt = conn.prepare(query)?;
    let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|b| b.as_ref()).collect();
    let rows = stmt.query_map(
        rusqlite::params_from_iter(param_refs.iter().copied()),
        |row| {
            let id: i64 = row.get(0)?;
            let plc_id: i64 = row.get(1)?;
            let plc_name: String = row.get(2)?;
            let enabled: bool = row.get::<_, i64>(3)? != 0;
            let desc: Option<String> = row.get(4)?;
            Ok((id, plc_id, plc_name, enabled, desc))
        },
    )?;
    let mut sets: Vec<(i64, i64, String, bool, Option<String>)> =
        rows.collect::<rusqlite::Result<_>>()?;

    if sets.is_empty() {
        return Err(fail(
            ErrorCode::NotFound,
            format!("metric set '{set_name}' not found"),
        ));
    }
    if sets.len() > 1 {
        // Ambiguous across PLCs: list the owners so the operator can disambiguate.
        let owners = sets
            .iter()
            .map(|(_, _, n, _, _)| n.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(fail(
            ErrorCode::InvalidArgument,
            format!(
                "metric set '{set_name}' exists on multiple PLCs ({owners}); pass --plc to disambiguate"
            ),
        ));
    }
    let (id, plc_id, plc_name, enabled, description) = sets.remove(0);

    // List members, scoped to the resolved PLC so names do not bleed across sets.
    let mut stmt = conn.prepare(
        "SELECT t.id, t.name, t.address, t.data_type, t.bad, t.bad_class
         FROM v_metric_set_member vmsm
         JOIN tag t ON t.id = vmsm.tag_id
         WHERE vmsm.metric_set = ?1 AND vmsm.plc = ?2",
    )?;
    let members = stmt
        .query_map(rusqlite::params![set_name, plc_name], |row| {
            Ok(serde_json::json!({
                "id": row.get::<_, i64>(0)?,
                "name": row.get::<_, String>(1)?,
                "address": row.get::<_, String>(2)?,
                "data_type": row.get::<_, String>(3)?,
                "bad": row.get::<_, i64>(4)? != 0,
                "bad_class": row.get::<_, Option<String>>(5)?,
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    Ok(serde_json::json!({
        "id": id,
        "plc_id": plc_id,
        "plc": plc_name,
        "name": set_name,
        "enabled": enabled,
        "description": description,
        "members": members,
    }))
}
