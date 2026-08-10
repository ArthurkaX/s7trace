//! `set-list` / `set-show` command handlers.

use anyhow::Result;
use rusqlite::Connection;

use crate::cli;
use crate::cli::args::Cli;
use crate::server_base;
use crate::types::*;

pub(crate) fn handle_set_list(cli: &Cli, conn: &Connection, plc: Option<&str>) -> Result<ExitCode> {
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

pub(crate) fn handle_set_show(
    conn: &Connection,
    cli: &Cli,
    set_name_pos: Option<&str>,
    set_flag: Option<&str>,
    plc: Option<&str>,
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
        let data = cli::remote::get_set_show(&base, set_name, plc, timeout)?;
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

    // Metric set names are unique only per PLC. If a PLC is given, scope the
    // lookup to it; otherwise, fail clearly when the name is ambiguous.
    let (query, params): (&str, Vec<Box<dyn rusqlite::ToSql>>) = if let Some(p) = plc {
        (
            "SELECT plc.name, ms.enabled, ms.description
          FROM metric_set ms
          JOIN plc ON plc.id = ms.plc_id
          WHERE ms.name = ?1 AND plc.name = ?2",
            vec![Box::new(set_name.to_string()), Box::new(p.to_string())],
        )
    } else {
        (
            "SELECT plc.name, ms.enabled, ms.description
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
            let plc_name: String = row.get(0)?;
            let enabled: bool = row.get::<_, i64>(1)? != 0;
            let desc: Option<String> = row.get(2)?;
            Ok((plc_name, enabled, desc))
        },
    )?;
    let sets: Vec<(String, bool, Option<String>)> = rows.filter_map(|r| r.ok()).collect();

    if sets.is_empty() {
        return Err(anyhow::anyhow!("metric set '{set_name}' not found"));
    }
    let (plc_name, enabled, _desc) = if sets.len() == 1 {
        sets.into_iter().next().unwrap()
    } else {
        // Ambiguous across PLCs: list the owners so the operator can disambiguate.
        let owners = sets
            .iter()
            .map(|(n, _, _)| n.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(anyhow::anyhow!(
            "metric set '{set_name}' exists on multiple PLCs ({owners}); pass --plc to disambiguate"
        ));
    };

    println!("Set: {set_name}");
    println!("Plc: {plc_name}");
    println!("Enabled: {enabled}");

    // List members, scoped to the resolved PLC so names do not bleed across sets.
    let mut stmt = conn.prepare(
        "SELECT t.id, t.name, t.address, t.data_type, t.bad, t.bad_class
         FROM v_metric_set_member vmsm
         JOIN tag t ON t.id = vmsm.tag_id
         WHERE vmsm.metric_set = ?1 AND vmsm.plc = ?2",
    )?;
    let members = stmt.query_map(rusqlite::params![set_name, plc_name], |row| {
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
