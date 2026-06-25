//! Minimal HTTP control API for s7trace.
//!
//! Endpoints (all under `/v1`):
//! - `GET  /status`             — full service status (JSON)
//! - `GET  /tags`               — list tags (`?plc=name`, `?enabled=1|0`)
//! - `GET  /sample`             — read samples (`?tag=name|id&since_ts=&until_ts=&limit=`)
//! - `GET  /rollup`             — read rollups (`?tag=name|id&since_ts=&until_ts=`)
//! - `GET  /event`              — read event markers (`?tag=&kind=&since_ts=&limit=`)
//! - `GET  /command`            — list recent commands (`?state=&limit=`)
//! - `GET  /command/{id}`       — single command + result
//! - `POST /command`            — submit a new command (JSON: `{kind, request, timeout_ms?}`)
//! - `POST /command/{id}/cancel` — cancel pending command
//!
//! The HTTP surface is an internal transport for the `s7trace` CLI. Operators
//! must use `s7trace` commands instead of calling these endpoints directly.
//! SQL access is especially restricted to `s7trace sql` / `s7trace admin-sql`;
//! direct `/sql` calls are a private CLI transport detail.
//!
//! Implementation is a tiny self-contained HTTP/1.1 server with a
//! thread-per-connection model. No external HTTP crate is required to
//! keep dependencies minimal and avoid pulling in TLS / async runtime.

use crate::db::command_queue::{self, CommandKind};
use crate::types::now_us;
use parking_lot::Mutex;
use rusqlite::Connection;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

/// Start the HTTP API on `bind_addr` (e.g. "127.0.0.1:8080").
/// Blocks the calling thread; spawn a thread to run in background.
pub fn serve(db: Arc<Mutex<Connection>>, bind_addr: String) -> std::io::Result<()> {
    let listener = TcpListener::bind(&bind_addr)?;
    log::info!("HTTP API listening on http://{bind_addr}");
    serve_with_listener(listener, db)
}

/// Like `serve`, but takes a pre-bound TcpListener (useful for tests
/// that want to discover the bound port without a race).
pub fn serve_with_listener(
    listener: TcpListener,
    db: Arc<Mutex<Connection>>,
) -> std::io::Result<()> {
    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let db = db.clone();
                thread::Builder::new()
                    .name("s7trace-http".into())
                    .spawn(move || {
                        if let Err(e) = handle_conn(stream, db) {
                            log::debug!("http conn closed: {e}");
                        }
                    })
                    .ok();
            }
            Err(e) => log::warn!("http accept error: {e}"),
        }
    }
    Ok(())
}

fn handle_conn(mut stream: TcpStream, db: Arc<Mutex<Connection>>) -> std::io::Result<()> {
    let peer = stream.peer_addr().ok();
    let mut reader = BufReader::new(stream.try_clone()?);

    // Parse request line
    let mut req_line = String::new();
    if reader.read_line(&mut req_line)? == 0 {
        return Ok(());
    }
    let mut parts = req_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("").to_string();
    let version = parts.next().unwrap_or("HTTP/1.1").to_string();

    // Headers
    let mut content_length: usize = 0;
    loop {
        let mut line = String::new();
        let n = reader.read_line(&mut line)?;
        if n == 0 {
            break;
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            break;
        }
        if let Some(rest) = trimmed.to_ascii_lowercase().strip_prefix("content-length:") {
            content_length = rest.trim().parse().unwrap_or(0);
        }
    }

    // Body
    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body)?;
    }

    // Route
    let (path, query) = split_query(&target);
    let resp = route(&db, &method, &path, &query, &body);

    let body_str = resp.body;
    let status_line = match resp.status {
        200 => format!("{version} 200 OK"),
        201 => format!("{version} 201 Created"),
        204 => format!("{version} 204 No Content"),
        400 => format!("{version} 400 Bad Request"),
        404 => format!("{version} 404 Not Found"),
        405 => format!("{version} 405 Method Not Allowed"),
        500 => format!("{version} 500 Internal Server Error"),
        _ => format!("{version} 200 OK"),
    };
    let payload = body_str.into_bytes();
    let response = format!(
        "{status_line}\r\nContent-Type: {}\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n",
        resp.content_type, payload.len(),
    );
    stream.write_all(response.as_bytes())?;
    stream.write_all(&payload)?;
    stream.flush()?;

    log::debug!(
        "http {} {} {} -> {} ({}b) peer={:?}",
        method,
        target,
        "",
        resp.status,
        payload.len(),
        peer
    );
    Ok(())
}

#[derive(Default)]
struct Response {
    status: u16,
    body: String,
    content_type: String,
}

impl Response {
    fn json(v: serde_json::Value) -> Self {
        Response {
            status: 200,
            body: serde_json::to_string_pretty(&v).unwrap_or_else(|_| "{}".into()),
            content_type: "application/json".into(),
        }
    }
    fn err(status: u16, msg: &str) -> Self {
        Response {
            status,
            body: serde_json::to_string(&serde_json::json!({"error": msg})).unwrap_or_default(),
            content_type: "application/json".into(),
        }
    }
    fn empty(status: u16) -> Self {
        Response {
            status,
            body: String::new(),
            content_type: "text/plain".into(),
        }
    }
}

fn split_query(target: &str) -> (String, Vec<(String, String)>) {
    let (path, query_str) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q),
        None => (target.to_string(), ""),
    };
    let mut params: Vec<(String, String)> = Vec::new();
    if !query_str.is_empty() {
        for pair in query_str.split('&') {
            if let Some((k, v)) = pair.split_once('=') {
                params.push((k.to_string(), v.to_string()));
            } else if !pair.is_empty() {
                params.push((pair.to_string(), String::new()));
            }
        }
    }
    (path, params)
}

fn route(
    db: &Arc<Mutex<Connection>>,
    method: &str,
    path: &str,
    query: &[(String, String)],
    body: &[u8],
) -> Response {
    // Strip /v1 prefix if present
    let p = path.strip_prefix("/v1").unwrap_or(path);

    match (method, p) {
        ("GET", "/status") => handle_status(db),
        ("GET", "/tags") => handle_tags(db, query),
        ("GET", "/sample") => handle_sample(db, query),
        ("GET", "/rollup") => handle_rollup(db, query),
        ("GET", "/event") => handle_event(db, query),
        ("GET", "/command") => handle_command_list(db, query),
        ("GET", "/sql") => handle_sql(db, query),
        ("GET", p) if p.starts_with("/command/") => {
            let rest = &p["/command/".len()..];
            if let Some(id_str) = rest.split('/').next() {
                if let Ok(id) = id_str.parse::<i64>() {
                    return handle_command_get(db, id);
                }
            }
            Response::err(400, "invalid command id")
        }
        ("POST", "/command") => handle_command_post(db, body),
        ("POST", p) if p.starts_with("/command/") && p.ends_with("/cancel") => {
            let rest = &p["/command/".len()..p.len() - "/cancel".len()];
            if let Ok(id) = rest.parse::<i64>() {
                return handle_command_cancel(db, id);
            }
            Response::err(400, "invalid command id")
        }
        ("GET", "/sets") => handle_set_list_api(db, query),
        ("GET", p) if p.starts_with("/set/") => {
            let name = &p["/set/".len()..];
            if name.is_empty() {
                return Response::err(400, "missing set name");
            }
            handle_set_show_api(db, name)
        }
        ("GET", "/") => Response::json(serde_json::json!({
            "service": "s7trace",
            "api_version": 1,
            "endpoints": [
                "GET /v1/status",
                "GET /v1/tags?plc=&enabled=",
                "GET /v1/sample?tag=&since_ts=&until_ts=&limit=",
                "GET /v1/rollup?tag=&since_ts=&until_ts=",
                "GET /v1/event?tag=&kind=&since_ts=&limit=",
                "GET /v1/command?state=&limit=",
                "GET /v1/command/{id}",
                "POST /v1/command",
                "POST /v1/command/{id}/cancel",
                "GET /v1/sets?plc=",
                "GET /v1/set/{name}",
            ],
            "operator_interface": "Use s7trace CLI commands only; HTTP is transport, not a public API.",
            "sql_policy": "Use s7trace sql for remote read-only SQL. admin-sql is local server-host maintenance only. Direct HTTP SQL/curl is forbidden.",
        })),
        ("GET", _) => Response::err(404, "not found"),
        ("POST", _) => Response::err(404, "not found"),
        _ => Response::err(405, "method not allowed"),
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

fn qparam<'a>(query: &'a [(String, String)], key: &str) -> Option<&'a str> {
    query
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(key))
        .map(|(_, v)| v.as_str())
}

fn param_owned<'a>(query: &'a [(String, String)], key: &str) -> Option<String> {
    qparam(query, key).map(|s| urldecode(s))
}

fn urldecode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'+' {
            out.push(' ');
        } else if b == b'%' && i + 2 < bytes.len() {
            if let Ok(c) = u8::from_str_radix(
                std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("00"),
                16,
            ) {
                out.push(c as char);
                i += 3;
                continue;
            } else {
                out.push(b as char);
            }
        } else {
            out.push(b as char);
        }
        i += 1;
    }
    out
}

fn handle_status(db: &Arc<Mutex<Connection>>) -> Response {
    let conn = db.lock();

    let plcs: Vec<serde_json::Value> = conn
        .prepare("SELECT id, name, host, enabled, last_ok_ts, last_error, connectivity FROM plc")
        .ok()
        .and_then(|mut s| {
            s.query_map([], |r| {
                let plc_id: i64 = r.get(0)?;
                let plc_name: String = r.get(1)?;

                // Per-PLC: poll groups with latest cycle stats
                let poll_groups: Vec<serde_json::Value> = conn
                    .prepare(
                        "SELECT pg.name, pg.period_ms,
                            COALESCE(latest.actual_ms, 0),
                            COALESCE(latest.overrun_count, 0)
                         FROM poll_group pg
                         JOIN tag t ON t.poll_group_id = pg.id
                         LEFT JOIN (
                            SELECT pc.poll_group_id,
                                AVG(pc.actual_ms) as actual_ms,
                                COUNT(CASE WHEN pc.actual_ms > pc.target_ms THEN 1 END) as overrun_count
                            FROM poll_cycle pc
                            WHERE pc.plc_id = ?1
                              AND pc.finished_ts > ?2
                            GROUP BY pc.poll_group_id
                         ) latest ON latest.poll_group_id = pg.id
                         WHERE t.plc_id = ?1 AND t.enabled = 1
                         GROUP BY pg.id
                         ORDER BY pg.id",
                    )
                    .ok()
                    .and_then(|mut stmt| {
                        let cutoff = now_us() - 3_600_000_000; // last hour
                        stmt.query_map(rusqlite::params![plc_id, cutoff], |row| {
                            Ok(serde_json::json!({
                                "name": row.get::<_, String>(0)?,
                                "target_ms": row.get::<_, i64>(1)?,
                                "actual_ms": row.get::<_, f64>(2)?,
                                "overrun_count": row.get::<_, i64>(3)?,
                            }))
                        })
                        .ok()
                        .map(|it| it.filter_map(|x| x.ok()).collect())
                    })
                    .unwrap_or_default();

                // Per-PLC: metric sets with tag counts
                let metric_sets: Vec<serde_json::Value> = conn
                    .prepare(
                        "SELECT ms.name, ms.enabled,
                            (SELECT COUNT(*) FROM metric_set_tag mst
                             JOIN tag t ON t.id = mst.tag_id
                             WHERE mst.metric_set_id = ms.id) as tags_total,
                            (SELECT COUNT(*) FROM metric_set_tag mst
                             JOIN tag t ON t.id = mst.tag_id
                             WHERE mst.metric_set_id = ms.id AND t.enabled = 1 AND t.bad = 0) as tags_active,
                            (SELECT COUNT(*) FROM metric_set_tag mst
                             JOIN tag t ON t.id = mst.tag_id
                             WHERE mst.metric_set_id = ms.id AND t.bad = 1) as tags_bad
                         FROM metric_set ms
                         WHERE ms.plc_id = ?1
                         ORDER BY ms.id",
                    )
                    .ok()
                    .and_then(|mut stmt| {
                        stmt.query_map(rusqlite::params![plc_id], |row| {
                            Ok(serde_json::json!({
                                "name": row.get::<_, String>(0)?,
                                "enabled": row.get::<_, i64>(1)? != 0,
                                "tags_total": row.get::<_, i64>(2)?,
                                "tags_active": row.get::<_, i64>(3)?,
                                "tags_bad": row.get::<_, i64>(4)?,
                            }))
                        })
                        .ok()
                        .map(|it| it.filter_map(|x| x.ok()).collect())
                    })
                    .unwrap_or_default();

                // Per-PLC: bad tags
                let bad_tags: Vec<serde_json::Value> = conn
                    .prepare(
                        "SELECT t.id, t.name, t.address, t.bad_class, t.bad_detail, t.last_error_ts
                         FROM tag t
                         WHERE t.plc_id = ?1 AND t.bad = 1
                         ORDER BY t.id",
                    )
                    .ok()
                    .and_then(|mut stmt| {
                        stmt.query_map(rusqlite::params![plc_id], |row| {
                            Ok(serde_json::json!({
                                "id": row.get::<_, i64>(0)?,
                                "name": row.get::<_, String>(1)?,
                                "address": row.get::<_, String>(2)?,
                                "bad_class": row.get::<_, Option<String>>(3)?,
                                "bad_detail": row.get::<_, Option<String>>(4)?,
                                "last_error_ts": row.get::<_, Option<i64>>(5)?,
                            }))
                        })
                        .ok()
                        .map(|it| it.filter_map(|x| x.ok()).collect())
                    })
                    .unwrap_or_default();

                Ok(serde_json::json!({
                    "name": plc_name,
                    "host": r.get::<_, String>(2)?,
                    "enabled": r.get::<_, i64>(3)? != 0,
                    "connected": r.get::<_, String>(6)? == "online",
                    "last_ok_ts": r.get::<_, Option<i64>>(4)?,
                    "last_error": r.get::<_, Option<String>>(5)?,
                    "connectivity": r.get::<_, String>(6)?,
                    "poll_groups": poll_groups,
                    "metric_sets": metric_sets,
                    "bad_tags": bad_tags,
                }))
            })
            .ok()
            .map(|it| it.filter_map(|x| x.ok()).collect())
        })
        .unwrap_or_default();

    let bad_tags_total: i64 = conn
        .query_row("SELECT COUNT(*) FROM tag WHERE bad = 1", [], |r| r.get(0))
        .unwrap_or(0);
    let active_tags: i64 = conn
        .query_row("SELECT COUNT(*) FROM v_active_tag", [], |r| r.get(0))
        .unwrap_or(0);
    let pending_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM command_request WHERE state = 'pending'",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);
    let running_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM command_request WHERE state = 'running'",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);
    let config_apply_pending: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM config_apply WHERE state IN ('pending','applying')",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);

    Response::json(serde_json::json!({
        "service": "s7trace",
        "plcs": plcs,
        "tags": { "active": active_tags, "bad": bad_tags_total },
        "command_queue": { "pending": pending_count, "running": running_count },
        "config_apply": { "pending": config_apply_pending },
    }))
}

fn handle_tags(db: &Arc<Mutex<Connection>>, query: &[(String, String)]) -> Response {
    let plc = param_owned(query, "plc");
    let enabled = param_owned(query, "enabled");
    let bad_only = matches!(param_owned(query, "bad"), Some(ref v) if v == "1" || v == "true");

    let conn = db.lock();
    let mut sql = String::from(
        "SELECT t.id, p.name, t.name, t.address, t.data_type,
                COALESCE(t.poll_group_id, 0), t.enabled, t.bad, t.bad_class, t.bad_detail,
                t.storage_hook, COALESCE(t.hook_params_json, ''),
                t.last_value_i, t.last_value_f
         FROM tag t JOIN plc p ON p.id = t.plc_id WHERE 1=1",
    );
    let mut binds: Vec<String> = Vec::new();
    if let Some(p) = &plc {
        sql.push_str(" AND p.name = ?");
        binds.push(p.clone());
    }
    if let Some(e) = &enabled {
        if e == "1" || e == "true" {
            sql.push_str(" AND t.enabled = 1");
        } else if e == "0" || e == "false" {
            sql.push_str(" AND t.enabled = 0");
        }
    }
    if bad_only {
        sql.push_str(" AND t.bad = 1");
    }
    sql.push_str(" ORDER BY p.id, t.id");

    let mut stmt = match conn.prepare(&sql) {
        Ok(s) => s,
        Err(e) => return Response::err(500, &format!("prepare: {e}")),
    };
    let mut q = stmt.query(rusqlite::params_from_iter(binds.iter()));
    let mut out: Vec<serde_json::Value> = Vec::new();
    if let Ok(rows) = q.as_mut() {
        while let Ok(Some(row)) = rows.next() {
            out.push(serde_json::json!({
                "id": row.get::<_, i64>(0).unwrap_or(0),
                "plc": row.get::<_, String>(1).unwrap_or_default(),
                "name": row.get::<_, String>(2).unwrap_or_default(),
                "address": row.get::<_, String>(3).unwrap_or_default(),
                "data_type": row.get::<_, String>(4).unwrap_or_default(),
                "poll_group_id": row.get::<_, i64>(5).unwrap_or(0),
                "enabled": row.get::<_, i64>(6).unwrap_or(0) != 0,
                "bad": row.get::<_, i64>(7).unwrap_or(0) != 0,
                "bad_class": row.get::<_, Option<String>>(8).unwrap_or(None),
                "bad_detail": row.get::<_, Option<String>>(9).unwrap_or(None),
                "storage_hook": row.get::<_, String>(10).unwrap_or_default(),
                "hook_params_json": row.get::<_, String>(11).unwrap_or_default(),
                "last_value_i": row.get::<_, Option<i64>>(12).unwrap_or(None),
                "last_value_f": row.get::<_, Option<f64>>(13).unwrap_or(None),
            }));
        }
    }
    Response::json(serde_json::json!({"tags": out, "count": out.len()}))
}

fn handle_sample(db: &Arc<Mutex<Connection>>, query: &[(String, String)]) -> Response {
    let tag = match param_owned(query, "tag") {
        Some(t) => t,
        None => return Response::err(400, "missing tag parameter"),
    };
    let limit: i64 = param_owned(query, "limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000)
        .min(100_000);
    let since_ts = param_owned(query, "since_ts").and_then(|s| s.parse::<i64>().ok());
    let until_ts = param_owned(query, "until_ts").and_then(|s| s.parse::<i64>().ok());

    let conn = db.lock();
    // tag can be id (i64) or name
    let tag_id: Option<i64> = if let Ok(id) = tag.parse::<i64>() {
        Some(id)
    } else {
        conn.query_row(
            "SELECT id FROM tag WHERE name = ?1",
            rusqlite::params![tag],
            |r| r.get(0),
        )
        .ok()
    };
    let tag_id = match tag_id {
        Some(i) => i,
        None => return Response::err(404, "tag not found"),
    };

    let mut sql =
        String::from("SELECT ts, vi, vf, span_id, '' AS reason FROM sample WHERE tag_id = ?1");
    if since_ts.is_some() {
        sql.push_str(" AND ts >= ?");
    }
    if until_ts.is_some() {
        sql.push_str(" AND ts <= ?");
    }
    sql.push_str(" ORDER BY ts DESC LIMIT ?");

    let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(tag_id)];
    if let Some(t) = since_ts {
        params.push(Box::new(t));
    }
    if let Some(t) = until_ts {
        params.push(Box::new(t));
    }
    params.push(Box::new(limit));

    let out = run_sample_query(&conn, &sql, &params);
    Response::json(serde_json::json!({"tag_id": tag_id, "samples": out, "count": out.len()}))
}

fn run_sample_query(
    conn: &Connection,
    sql: &str,
    params: &[Box<dyn rusqlite::ToSql>],
) -> Vec<serde_json::Value> {
    let mut stmt = match conn.prepare(sql) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|b| b.as_ref()).collect();
    let mut out = Vec::new();
    if let Ok(mut rows) = stmt.query(rusqlite::params_from_iter(param_refs.iter().copied())) {
        while let Some(row) = rows.next().ok().flatten() {
            out.push(serde_json::json!({
                "ts": row.get::<_, i64>(0).unwrap_or(0),
                "value_i": row.get::<_, Option<i64>>(1).unwrap_or(None),
                "value_f": row.get::<_, Option<f64>>(2).unwrap_or(None),
                "span_id": row.get::<_, Option<i64>>(3).unwrap_or(None),
                "reason": row.get::<_, String>(4).unwrap_or_default(),
            }));
        }
    }
    out
}

fn handle_rollup(db: &Arc<Mutex<Connection>>, query: &[(String, String)]) -> Response {
    let tag = match param_owned(query, "tag") {
        Some(t) => t,
        None => return Response::err(400, "missing tag parameter"),
    };
    let since_ts = param_owned(query, "since_ts").and_then(|s| s.parse::<i64>().ok());
    let until_ts = param_owned(query, "until_ts").and_then(|s| s.parse::<i64>().ok());
    let limit: i64 = param_owned(query, "limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000)
        .min(100_000);

    let conn = db.lock();
    let tag_id: Option<i64> = if let Ok(id) = tag.parse::<i64>() {
        Some(id)
    } else {
        conn.query_row(
            "SELECT id FROM tag WHERE name = ?1",
            rusqlite::params![tag],
            |r| r.get(0),
        )
        .ok()
    };
    let tag_id = match tag_id {
        Some(i) => i,
        None => return Response::err(404, "tag not found"),
    };

    let mut sql = String::from(
        "SELECT window_start_ts, window_end_ts, first_vi, first_vf,
                last_vi, last_vf, min_vi, min_vf,
                max_vi, max_vf, sample_count
         FROM sample_rollup WHERE tag_id = ?1",
    );
    if since_ts.is_some() {
        sql.push_str(" AND window_start_ts >= ?");
    }
    if until_ts.is_some() {
        sql.push_str(" AND window_end_ts <= ?");
    }
    sql.push_str(" ORDER BY window_start_ts DESC LIMIT ?");

    let mut stmt = match conn.prepare(&sql) {
        Ok(s) => s,
        Err(e) => return Response::err(500, &format!("prepare: {e}")),
    };
    let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(tag_id)];
    if let Some(t) = since_ts {
        params.push(Box::new(t));
    }
    if let Some(t) = until_ts {
        params.push(Box::new(t));
    }
    params.push(Box::new(limit));

    let mut out: Vec<serde_json::Value> = Vec::new();
    let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|b| b.as_ref()).collect();
    if let Ok(mut rows) = stmt.query(rusqlite::params_from_iter(param_refs.iter().copied())) {
        while let Some(row) = rows.next().ok().flatten() {
            out.push(serde_json::json!({
                "window_start_ts": row.get::<_, i64>(0).unwrap_or(0),
                "window_end_ts": row.get::<_, i64>(1).unwrap_or(0),
                "first_vi": row.get::<_, Option<i64>>(2).unwrap_or(None),
                "first_vf": row.get::<_, Option<f64>>(3).unwrap_or(None),
                "last_vi": row.get::<_, Option<i64>>(4).unwrap_or(None),
                "last_vf": row.get::<_, Option<f64>>(5).unwrap_or(None),
                "min_vi": row.get::<_, Option<i64>>(6).unwrap_or(None),
                "min_vf": row.get::<_, Option<f64>>(7).unwrap_or(None),
                "max_vi": row.get::<_, Option<i64>>(8).unwrap_or(None),
                "max_vf": row.get::<_, Option<f64>>(9).unwrap_or(None),
                "sample_count": row.get::<_, i64>(10).unwrap_or(0),
            }));
        }
    }
    Response::json(serde_json::json!({"tag_id": tag_id, "rollups": out, "count": out.len()}))
}

fn handle_event(db: &Arc<Mutex<Connection>>, query: &[(String, String)]) -> Response {
    let since_ts = param_owned(query, "since_ts").and_then(|s| s.parse::<i64>().ok());
    let kind = param_owned(query, "kind");
    let tag_filter = param_owned(query, "tag");
    let limit: i64 = param_owned(query, "limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000)
        .min(100_000);

    let conn = db.lock();
    let mut sql = String::from(
        "SELECT em.id, em.ts, em.kind, em.severity, em.tag_id,
                COALESCE(t.name, ''), em.value_i, COALESCE(em.details_json, '')
         FROM event_marker em
         LEFT JOIN tag t ON t.id = em.tag_id
         WHERE 1=1",
    );
    let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
    if let Some(t) = &tag_filter {
        sql.push_str(" AND (t.name = ? OR CAST(em.tag_id AS TEXT) = ?)");
        params.push(Box::new(t.clone()));
        params.push(Box::new(t.clone()));
    }
    if let Some(k) = &kind {
        sql.push_str(" AND em.kind = ?");
        params.push(Box::new(k.clone()));
    }
    if since_ts.is_some() {
        sql.push_str(" AND em.ts >= ?");
        params.push(Box::new(since_ts.unwrap()));
    }
    sql.push_str(" ORDER BY em.ts DESC LIMIT ?");
    params.push(Box::new(limit));

    let mut stmt = match conn.prepare(&sql) {
        Ok(s) => s,
        Err(e) => return Response::err(500, &format!("prepare: {e}")),
    };
    let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|b| b.as_ref()).collect();
    let mut out: Vec<serde_json::Value> = Vec::new();
    if let Ok(mut rows) = stmt.query(rusqlite::params_from_iter(param_refs.iter().copied())) {
        while let Some(row) = rows.next().ok().flatten() {
            out.push(serde_json::json!({
                "id": row.get::<_, i64>(0).unwrap_or(0),
                "ts": row.get::<_, i64>(1).unwrap_or(0),
                "kind": row.get::<_, String>(2).unwrap_or_default(),
                "severity": row.get::<_, String>(3).unwrap_or_default(),
                "tag_id": row.get::<_, Option<i64>>(4).unwrap_or(None),
                "tag": row.get::<_, String>(5).unwrap_or_default(),
                "value_i": row.get::<_, Option<i64>>(6).unwrap_or(None),
                "details_json": row.get::<_, String>(7).unwrap_or_default(),
            }));
        }
    }
    Response::json(serde_json::json!({"events": out, "count": out.len()}))
}

fn handle_command_list(db: &Arc<Mutex<Connection>>, query: &[(String, String)]) -> Response {
    let state = param_owned(query, "state");
    let limit: i64 = param_owned(query, "limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(50)
        .min(1000);

    let conn = db.lock();
    let mut sql = String::from(
        "SELECT id, kind, COALESCE(requested_by, ''), state, COALESCE(request_json, ''),
                created_ts
         FROM command_request WHERE 1=1",
    );
    let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
    if let Some(s) = &state {
        sql.push_str(" AND state = ?");
        params.push(Box::new(s.clone()));
    }
    sql.push_str(" ORDER BY id DESC LIMIT ?");
    params.push(Box::new(limit));

    let mut stmt = match conn.prepare(&sql) {
        Ok(s) => s,
        Err(e) => return Response::err(500, &format!("prepare: {e}")),
    };
    let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|b| b.as_ref()).collect();
    let mut out: Vec<serde_json::Value> = Vec::new();
    if let Ok(mut rows) = stmt.query(rusqlite::params_from_iter(param_refs.iter().copied())) {
        while let Some(row) = rows.next().ok().flatten() {
            out.push(serde_json::json!({
                "id": row.get::<_, i64>(0).unwrap_or(0),
                "kind": row.get::<_, String>(1).unwrap_or_default(),
                "actor": row.get::<_, String>(2).unwrap_or_default(),
                "state": row.get::<_, String>(3).unwrap_or_default(),
                "request_json": row.get::<_, String>(4).unwrap_or_default(),
                "created_ts": row.get::<_, i64>(5).unwrap_or(0),
            }));
        }
    }
    Response::json(serde_json::json!({"commands": out, "count": out.len()}))
}

fn handle_command_get(db: &Arc<Mutex<Connection>>, id: i64) -> Response {
    let conn = db.lock();
    let cmd = command_queue::get_request(&conn, id).ok().flatten();
    let res = command_queue::get_result(&conn, id).ok().flatten();
    match cmd {
        Some(c) => Response::json(serde_json::json!({
            "command": {
                "id": c.id,
                "kind": format!("{:?}", c.kind),
                "state": format!("{:?}", c.state),
                "plc_id": c.plc_id,
                "request_json": c.request_json,
                "created_ts": c.created_ts,
            },
            "result": res.map(|r| serde_json::json!({
                "ok": r.ok,
                "result_json": r.result_json,
                "error": r.error,
            })),
        })),
        None => Response::err(404, "command not found"),
    }
}

fn handle_command_post(db: &Arc<Mutex<Connection>>, body: &[u8]) -> Response {
    let v: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return Response::err(400, &format!("invalid JSON body: {e}")),
    };
    // Accept either an explicit `kind` (CommandKind) or an `action` shortcut.
    // The HTTP layer is intentionally thin: it forwards the JSON payload
    // verbatim to the queue. The actual dispatch happens in
    // `engine::server::execute_command`.
    let kind = match v.get("kind").and_then(|x| x.as_str()) {
        Some("query") | Some("Query") => CommandKind::Query,
        Some("control") | Some("Control") => CommandKind::Control,
        Some("plc_read") | Some("PlcRead") | Some("plc-read") => CommandKind::PlcRead,
        Some("admin") | Some("Admin") => CommandKind::Admin,
        Some(other) => return Response::err(400, &format!("unknown kind: {other}")),
        None => return Response::err(400, "missing kind"),
    };
    let request = v.get("request").cloned().unwrap_or(serde_json::json!({}));
    let request_json = request.to_string();
    let actor = v.get("actor").and_then(|x| x.as_str());
    let plc_id = request.get("plc_id").and_then(|x| x.as_i64());

    let conn = db.lock();
    match command_queue::enqueue(&conn, kind, &request_json, plc_id, actor, None) {
        Ok(id) => Response {
            status: 201,
            body: serde_json::to_string_pretty(&serde_json::json!({
                "id": id, "kind": format!("{:?}", kind), "state": "pending",
            }))
            .unwrap_or_default(),
            content_type: "application/json".into(),
        },
        Err(e) => Response::err(500, &format!("enqueue: {e}")),
    }
}

fn handle_command_cancel(db: &Arc<Mutex<Connection>>, id: i64) -> Response {
    let conn = db.lock();
    match command_queue::cancel(&conn, id) {
        Ok(true) => Response::empty(204),
        Ok(false) => Response::err(409, "command not cancellable (not pending)"),
        Err(e) => Response::err(500, &format!("cancel: {e}")),
    }
}

fn handle_set_list_api(db: &Arc<Mutex<Connection>>, query: &[(String, String)]) -> Response {
    let conn = db.lock();
    let plc = qparam(query, "plc");

    let (sql, plc_param): (&str, bool) = if plc.is_some() {
        ("SELECT vms.plc, vms.metric_set, vms.enabled, vms.tags_total, vms.tags_healthy, vms.tags_bad
          FROM v_metric_set_status vms WHERE vms.plc = ?1 ORDER BY vms.plc, vms.metric_set", true)
    } else {
        ("SELECT vms.plc, vms.metric_set, vms.enabled, vms.tags_total, vms.tags_healthy, vms.tags_bad
          FROM v_metric_set_status vms ORDER BY vms.plc, vms.metric_set", false)
    };

    let mut stmt = match conn.prepare(sql) {
        Ok(s) => s,
        Err(e) => return Response::err(500, &format!("query: {e}")),
    };

    let items: Vec<serde_json::Value> = if plc_param {
        let plc_name = plc.unwrap();
        match stmt.query_map(rusqlite::params![plc_name], |row| {
            Ok(serde_json::json!({
                "plc": row.get::<_, String>(0)?,
                "name": row.get::<_, String>(1)?,
                "enabled": row.get::<_, i64>(2)? != 0,
                "tags_total": row.get::<_, i64>(3)?,
                "tags_healthy": row.get::<_, i64>(4)?,
                "tags_bad": row.get::<_, i64>(5)?,
            }))
        }) {
            Ok(r) => r.filter_map(|x| x.ok()).collect(),
            Err(e) => return Response::err(500, &format!("query: {e}")),
        }
    } else {
        match stmt.query_map([], |row| {
            Ok(serde_json::json!({
                "plc": row.get::<_, String>(0)?,
                "name": row.get::<_, String>(1)?,
                "enabled": row.get::<_, i64>(2)? != 0,
                "tags_total": row.get::<_, i64>(3)?,
                "tags_healthy": row.get::<_, i64>(4)?,
                "tags_bad": row.get::<_, i64>(5)?,
            }))
        }) {
            Ok(r) => r.filter_map(|x| x.ok()).collect(),
            Err(e) => return Response::err(500, &format!("query: {e}")),
        }
    };
    Response::json(serde_json::json!(items))
}

fn handle_set_show_api(db: &Arc<Mutex<Connection>>, set_name: &str) -> Response {
    let conn = db.lock();

    let set_info = match conn.query_row(
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
    ) {
        Ok(info) => info,
        Err(_) => return Response::err(404, &format!("metric set '{set_name}' not found")),
    };

    let (id, plc_id, name, enabled, description) = set_info;

    let mut stmt = match conn.prepare(
        "SELECT t.id, t.name, t.address, t.data_type, t.bad, t.bad_class
         FROM v_metric_set_member vmsm
         JOIN tag t ON t.id = vmsm.tag_id
         WHERE vmsm.metric_set = ?1",
    ) {
        Ok(s) => s,
        Err(e) => return Response::err(500, &format!("query members: {e}")),
    };

    let members: Vec<serde_json::Value> = match stmt.query_map(rusqlite::params![set_name], |row| {
        Ok(serde_json::json!({
            "id": row.get::<_, i64>(0)?,
            "name": row.get::<_, String>(1)?,
            "address": row.get::<_, String>(2)?,
            "data_type": row.get::<_, String>(3)?,
            "bad": row.get::<_, i64>(4)? != 0,
            "bad_class": row.get::<_, Option<String>>(5)?,
        }))
    }) {
        Ok(rows) => rows.filter_map(|x| x.ok()).collect(),
        Err(e) => return Response::err(500, &format!("query members: {e}")),
    };

    Response::json(serde_json::json!({
        "id": id,
        "plc_id": plc_id,
        "name": name,
        "enabled": enabled,
        "description": description,
        "members": members,
    }))
}

// ---------------------------------------------------------------------------
// Tests

fn handle_sql(db: &Arc<Mutex<Connection>>, query: &[(String, String)]) -> Response {
    let raw = match param_owned(query, "query") {
        Some(q) => q,
        None => return Response::err(400, "missing query parameter"),
    };
    let trimmed = raw.trim().to_uppercase();
    if !trimmed.starts_with("SELECT")
        && !trimmed.starts_with("WITH")
        && !trimmed.starts_with("PRAGMA")
    {
        return Response::err(400, "only SELECT/WITH/PRAGMA allowed");
    }

    let limit: u64 = qparam(query, "limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(200);

    let limited_sql = if trimmed.contains(" LIMIT ") {
        raw.clone()
    } else {
        format!("{raw} LIMIT {limit}")
    };

    let conn = db.lock();
    let mut stmt = match conn.prepare(&limited_sql) {
        Ok(s) => s,
        Err(e) => return Response::err(400, &format!("sql error: {e}")),
    };

    let col_count = stmt.column_count();
    let col_names: Vec<String> = (0..col_count)
        .map(|i| stmt.column_name(i).unwrap_or("?").to_string())
        .collect();

    let rows: Vec<Vec<serde_json::Value>> = match stmt.query_map([], |row| {
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
        Ok(r) => r.filter_map(|x| x.ok()).collect(),
        Err(e) => return Response::err(400, &format!("sql error: {e}")),
    };

    Response::json(serde_json::json!({
        "columns": col_names,
        "rows": rows,
        "count": rows.len(),
    }))
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::schema;
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::time::Duration;

    fn temp_db() -> Arc<Mutex<Connection>> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let n = SEQ.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "s7trace_api_test_{}_{}_{}.db",
            std::process::id(),
            format!("{:?}", std::thread::current().id()).replace(':', "_"),
            n,
        ));
        let path_str = path.to_string_lossy().into_owned();
        let _ = std::fs::remove_file(&path);
        let conn = schema::open(&path_str).expect("open db");
        // Pre-seed one PLC + one tag so /v1/tags returns a row.
        conn.execute_batch(
            "INSERT INTO plc (name, host) VALUES ('plc1', '127.0.0.1:102');
             INSERT INTO poll_group (name, period_ms) VALUES ('g1', 1000);
             INSERT INTO tag (plc_id, name, address, data_type, poll_group_id,
                              db_num, byte_offset, byte_size, created_ts, updated_ts)
             VALUES (1, 'tagA', 'DB1.DBW0', 'i16', 1, 1, 0, 2, 1000000, 1000000);",
        )
        .expect("seed");
        Arc::new(Mutex::new(conn))
    }

    fn http_get(addr: &str, path: &str) -> String {
        let mut s = TcpStream::connect(addr).expect("connect");
        s.set_read_timeout(Some(Duration::from_secs(2))).ok();
        let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        s.write_all(req.as_bytes()).expect("write");
        let mut buf = String::new();
        s.read_to_string(&mut buf).expect("read");
        buf
    }

    fn pick_port() -> u16 {
        // Bind and drop quickly to pick a free port. Small race window
        // remains; in tests we use distinct seeds per call to reduce
        // collisions across parallel test threads.
        let l = TcpListener::bind("127.0.0.1:0").expect("bind");
        let p = l.local_addr().unwrap().port();
        drop(l);
        p
    }

    /// Spawn the HTTP server in a thread. Returns the port that the
    /// server actually bound to (after retries on EADDRINUSE).
    fn spawn_server(db: Arc<Mutex<Connection>>) -> u16 {
        use std::sync::mpsc::channel;
        let (tx, rx) = channel::<u16>();
        let _h = std::thread::spawn(move || {
            // Try a few ports; report the first one that bound successfully.
            for _ in 0..32 {
                let l = match TcpListener::bind("127.0.0.1:0") {
                    Ok(l) => l,
                    Err(_) => continue,
                };
                let port = l.local_addr().unwrap().port();
                let addr = format!("127.0.0.1:{port}");
                // Hand the listener to `serve` via the std TcpListener
                // we already have, by calling its lower-level accept loop.
                // Since `serve` takes a String, we re-bind through a custom
                // path: we own the listener, so we use it directly.
                let _ = tx.send(port);
                if serve_with_listener(l, db.clone()).is_err() {
                    // listener gone, exit
                }
                return;
            }
        });
        // Wait for the listener to come up.
        rx.recv_timeout(Duration::from_secs(2))
            .expect("server failed to start")
    }

    #[test]
    fn test_http_status() {
        let db = temp_db();
        let port = spawn_server(db);
        let resp = http_get(&format!("127.0.0.1:{port}"), "/v1/status");
        assert!(resp.contains("200 OK"), "status: {resp}");
        assert!(resp.contains("s7trace"), "body: {resp}");
        assert!(resp.contains("plcs"), "body: {resp}");
    }

    #[test]
    fn test_http_tags_list() {
        let db = temp_db();
        let port = spawn_server(db);
        let resp = http_get(&format!("127.0.0.1:{port}"), "/v1/tags");
        assert!(resp.contains("200 OK"), "tags: {resp}");
        assert!(resp.contains("tagA"), "tagA not in: {resp}");
    }

    #[test]
    fn test_http_command_post_and_get() {
        let db = temp_db();
        let port = spawn_server(db);
        let addr = format!("127.0.0.1:{port}");

        // POST /v1/command
        let body = r#"{"kind":"control","request":{"action":"tag_disable","plc_id":1,"tag_id":1}}"#;
        let mut s = TcpStream::connect(&addr).expect("connect");
        let req = format!(
            "POST /v1/command HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(), body,
        );
        s.write_all(req.as_bytes()).expect("write");
        let mut resp = String::new();
        s.read_to_string(&mut resp).expect("read");
        assert!(resp.contains("201 Created"), "post: {resp}");
        // Extract id
        let id_start = resp.find("\"id\":").expect("id field") + 5;
        let id_end = resp[id_start..]
            .find(',')
            .map(|i| id_start + i)
            .unwrap_or(resp.len());
        let id: i64 = resp[id_start..id_end].trim().parse().expect("parse id");

        // GET /v1/command/{id}
        let resp2 = http_get(&addr, &format!("/v1/command/{id}"));
        assert!(resp2.contains("200 OK"));
        assert!(resp2.contains(&format!("\"id\": {id}")));
    }
}
