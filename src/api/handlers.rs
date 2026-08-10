use crate::db::command_queue::{self, CommandKind};
use parking_lot::Mutex;
use rusqlite::Connection;
use std::sync::Arc;

use super::{param_owned, qparam, Response};

/// Max time a `/v1/sql` query may run before it is interrupted. Every request
/// funnels through a single shared connection, so one runaway scan over a
/// large table would otherwise hold the mutex — and with it every other
/// endpoint — for as long as it runs. A watchdog thread holds the connection's
/// interrupt handle and trips `SQLITE_INTERRUPT` once this deadline passes.
const SQL_QUERY_TIMEOUT_MS: u64 = 5000;

/// Ceiling for a caller-supplied `timeout_ms`. An operator running a genuinely
/// heavy report can ask for more than the default, but not for an unbounded
/// hold on the connection everything else shares.
const SQL_MAX_TIMEOUT_MS: u64 = 30_000;

/// Default row cap for `/v1/sql` when no `limit` parameter is given.
const SQL_DEFAULT_LIMIT: u64 = 200;
/// Absolute response row cap. The query parameter may lower this value, never
/// raise it; otherwise a remote client could turn the bounded endpoint into a
/// large response/memory allocation.
const SQL_MAX_ROWS: u64 = 10_000;

pub(crate) fn handle_tags(db: &Arc<Mutex<Connection>>, query: &[(String, String)]) -> Response {
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

pub(crate) fn handle_sample(db: &Arc<Mutex<Connection>>, query: &[(String, String)]) -> Response {
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

pub(crate) fn handle_rollup(db: &Arc<Mutex<Connection>>, query: &[(String, String)]) -> Response {
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

pub(crate) fn handle_event(db: &Arc<Mutex<Connection>>, query: &[(String, String)]) -> Response {
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
    if let Some(ts) = since_ts {
        sql.push_str(" AND em.ts >= ?");
        params.push(Box::new(ts));
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

pub(crate) fn handle_command_list(
    db: &Arc<Mutex<Connection>>,
    query: &[(String, String)],
) -> Response {
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

pub(crate) fn handle_command_get(db: &Arc<Mutex<Connection>>, id: i64) -> Response {
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

pub(crate) fn handle_command_post(db: &Arc<Mutex<Connection>>, body: &[u8]) -> Response {
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

pub(crate) fn handle_command_cancel(db: &Arc<Mutex<Connection>>, id: i64) -> Response {
    let conn = db.lock();
    match command_queue::cancel(&conn, id) {
        Ok(true) => Response::empty(204),
        Ok(false) => Response::err(409, "command not cancellable (not pending)"),
        Err(e) => Response::err(500, &format!("cancel: {e}")),
    }
}

pub(crate) fn handle_set_list_api(
    db: &Arc<Mutex<Connection>>,
    query: &[(String, String)],
) -> Response {
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

/// A metric set, optionally scoped to one PLC.
///
/// Set names are unique per PLC, not globally, so a bare name can match more
/// than one set. Without a `plc` the query stays as it was — first match wins —
/// but an ambiguous name is reported rather than silently resolved.
pub(crate) fn handle_set_show_api(
    db: &Arc<Mutex<Connection>>,
    set_name: &str,
    plc: Option<&str>,
) -> Response {
    let conn = db.lock();

    let (sql, params): (&str, Vec<&dyn rusqlite::ToSql>) = match &plc {
        Some(p) => (
            "SELECT ms.id, ms.plc_id, ms.name, ms.enabled, ms.description
             FROM metric_set ms JOIN plc ON plc.id = ms.plc_id
             WHERE ms.name = ?1 AND plc.name = ?2",
            vec![&set_name, p],
        ),
        None => (
            "SELECT ms.id, ms.plc_id, ms.name, ms.enabled, ms.description
             FROM metric_set ms WHERE ms.name = ?1",
            vec![&set_name],
        ),
    };

    let set_info = match conn.query_row(sql, params.as_slice(), |row| {
        let id: i64 = row.get(0)?;
        let plc_id: i64 = row.get(1)?;
        let name: String = row.get(2)?;
        let enabled: bool = row.get::<_, i64>(3)? != 0;
        let desc: Option<String> = row.get(4)?;
        Ok((id, plc_id, name, enabled, desc))
    }) {
        Ok(info) => info,
        Err(_) => {
            return match plc {
                Some(p) => Response::err(
                    404,
                    &format!("metric set '{set_name}' not found on PLC '{p}'"),
                ),
                None => Response::err(404, &format!("metric set '{set_name}' not found")),
            };
        }
    };

    let (id, plc_id, name, enabled, description) = set_info;

    // Members are keyed by set name in the view, so scope them by set id to
    // avoid pulling in a same-named set that belongs to another PLC.
    let mut stmt = match conn.prepare(
        "SELECT t.id, t.name, t.address, t.data_type, t.bad, t.bad_class
         FROM metric_set_tag mst
         JOIN tag t ON t.id = mst.tag_id
         WHERE mst.metric_set_id = ?1",
    ) {
        Ok(s) => s,
        Err(e) => return Response::err(500, &format!("query members: {e}")),
    };

    let members: Vec<serde_json::Value> = match stmt.query_map(rusqlite::params![id], |row| {
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

pub(crate) fn handle_sql(db: &Arc<Mutex<Connection>>, query: &[(String, String)]) -> Response {
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

    let requested_limit: u64 = qparam(query, "limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(SQL_DEFAULT_LIMIT);
    let limit = requested_limit.min(SQL_MAX_ROWS);

    // The caller may ask for a longer deadline than the default, up to the
    // server's ceiling. Anything unparseable falls back to the default rather
    // than erroring — a bad timeout should not lose the operator their query.
    let timeout_ms: u64 = qparam(query, "timeout_ms")
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|ms| *ms > 0)
        .unwrap_or(SQL_QUERY_TIMEOUT_MS)
        .min(SQL_MAX_TIMEOUT_MS);

    // If the caller already wrote a LIMIT, take the smaller of the two so the
    // row cap can never be widened by the query text.
    let limit = match raw
        .to_uppercase()
        .split(" LIMIT ")
        .nth(1)
        .and_then(|s| s.split_whitespace().next())
        .and_then(|s| s.parse::<u64>().ok())
    {
        Some(in_query) => limit.min(in_query),
        None => limit,
    };

    let conn = db.lock();
    // The statement borrows the connection, so it runs inside this block; it
    // (and the guard) drop before the response body is built below.
    let (col_names, result, truncated, rows) = {
        let mut stmt = match conn.prepare(&raw) {
            Ok(s) => s,
            Err(e) => return Response::err(400, &format!("sql error: {e}")),
        };

        let col_count = stmt.column_count();
        let col_names: Vec<String> = (0..col_count)
            .map(|i| stmt.column_name(i).unwrap_or("?").to_string())
            .collect();

        let mut rows: Vec<Vec<serde_json::Value>> = Vec::new();
        let mut truncated = false;
        let map_row = |row: &rusqlite::Row| -> rusqlite::Result<Vec<serde_json::Value>> {
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
        };

        // Arm a watchdog that interrupts the query if it runs past the
        // deadline. The handle works on the connection from another thread, so
        // a runaway scan cannot hold the shared mutex — and with it every other
        // endpoint — for longer than the limit. The query signals the channel
        // when it is done (or errored), so the watchdog exits immediately on
        // the normal path.
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
        let handle = conn.get_interrupt_handle();
        let watchdog = std::thread::spawn(move || {
            let timeout = std::time::Duration::from_millis(timeout_ms);
            if done_rx.recv_timeout(timeout).is_err() {
                // Deadline passed before the query signalled completion.
                handle.interrupt();
            }
        });

        let result = (|| -> rusqlite::Result<()> {
            let mut q = stmt.query([])?;
            while let Some(row) = q.next()? {
                // Stop at the cap instead of appending a LIMIT clause, so a
                // query that already wrote its own LIMIT cannot bypass it, and
                // so the caller learns from `truncated` that there was more.
                if rows.len() >= limit as usize {
                    truncated = true;
                    break;
                }
                rows.push(map_row(row)?);
            }
            Ok(())
        })();

        // Let the watchdog exit now that the query is finished.
        let _ = done_tx.send(());
        watchdog.join().ok();

        (col_names, result, truncated, rows)
    };
    // The guard is dropped here, before the (potentially large) JSON body is
    // serialised, so formatting a big result set never holds the connection.
    drop(conn);

    if let Err(e) = result {
        // `SQLITE_INTERRUPT` is what the watchdog raises when the deadline
        // trips; report it as the limit, not as a SQL error.
        let is_interrupt = matches!(
            e.sqlite_error_code(),
            Some(rusqlite::ErrorCode::OperationInterrupted)
        );
        if is_interrupt {
            return Response::err(503, &format!("query exceeded the {timeout_ms}ms limit"));
        }
        return Response::err(400, &format!("sql error: {e}"));
    }

    Response::json(serde_json::json!({
        "columns": col_names,
        "rows": rows,
        "count": rows.len(),
        "truncated": truncated,
    }))
}
