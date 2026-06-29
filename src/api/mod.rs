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
//! SQL access is especially restricted to `s7trace -r URL sql` for remote
//! reads or `s7trace --local-db sql/admin-sql` for local database work;
//! direct `/sql` calls are a private CLI transport detail.
//!
//! Implementation is a tiny self-contained HTTP/1.1 server with a
//! thread-per-connection model. No external HTTP crate is required to
//! keep dependencies minimal and avoid pulling in TLS / async runtime.

mod handlers;
mod status;

use crate::version;
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
    tracing::info!("HTTP API listening on http://{bind_addr}");
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
                            tracing::debug!("http conn closed: {e}");
                        }
                    })
                    .ok();
            }
            Err(e) => tracing::warn!("http accept error: {e}"),
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

    tracing::debug!(
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
pub(crate) struct Response {
    pub(crate) status: u16,
    pub(crate) body: String,
    pub(crate) content_type: String,
}

impl Response {
    pub(crate) fn json(v: serde_json::Value) -> Self {
        Response {
            status: 200,
            body: serde_json::to_string_pretty(&v).unwrap_or_else(|_| "{}".into()),
            content_type: "application/json".into(),
        }
    }
    pub(crate) fn err(status: u16, msg: &str) -> Self {
        Response {
            status,
            body: serde_json::to_string(&serde_json::json!({"error": msg})).unwrap_or_default(),
            content_type: "application/json".into(),
        }
    }
    pub(crate) fn empty(status: u16) -> Self {
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
        ("GET", "/status") => status::handle_status(db),
        ("GET", "/tags") => handlers::handle_tags(db, query),
        ("GET", "/sample") => handlers::handle_sample(db, query),
        ("GET", "/rollup") => handlers::handle_rollup(db, query),
        ("GET", "/event") => handlers::handle_event(db, query),
        ("GET", "/command") => handlers::handle_command_list(db, query),
        ("GET", "/sql") => handlers::handle_sql(db, query),
        ("GET", p) if p.starts_with("/command/") => {
            let rest = &p["/command/".len()..];
            if let Some(id_str) = rest.split('/').next() {
                if let Ok(id) = id_str.parse::<i64>() {
                    return handlers::handle_command_get(db, id);
                }
            }
            Response::err(400, "invalid command id")
        }
        ("POST", "/command") => handlers::handle_command_post(db, body),
        ("POST", p) if p.starts_with("/command/") && p.ends_with("/cancel") => {
            let rest = &p["/command/".len()..p.len() - "/cancel".len()];
            if let Ok(id) = rest.parse::<i64>() {
                return handlers::handle_command_cancel(db, id);
            }
            Response::err(400, "invalid command id")
        }
        ("GET", "/sets") => handlers::handle_set_list_api(db, query),
        ("GET", p) if p.starts_with("/set/") => {
            let name = &p["/set/".len()..];
            if name.is_empty() {
                return Response::err(400, "missing set name");
            }
            handlers::handle_set_show_api(db, name)
        }
        ("GET", "/") => Response::json(serde_json::json!({
            "service": "s7trace",
            "api_version": version::API_VERSION,
            "version": {
                "package": version::PACKAGE_VERSION,
                "api": version::API_VERSION,
            },
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
            "sql_policy": "Use s7trace -r URL sql for remote read-only SQL. Use --local-db for intentional local SQLite access. admin-sql is local server-host maintenance only. Direct HTTP SQL/curl is forbidden.",
        })),
        ("GET", _) => Response::err(404, "not found"),
        ("POST", _) => Response::err(404, "not found"),
        _ => Response::err(405, "method not allowed"),
    }
}

// ---------------------------------------------------------------------------
// Helpers used by route handlers in sibling modules
// ---------------------------------------------------------------------------

pub(crate) fn qparam<'a>(query: &'a [(String, String)], key: &str) -> Option<&'a str> {
    query
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(key))
        .map(|(_, v)| v.as_str())
}

pub(crate) fn param_owned(query: &[(String, String)], key: &str) -> Option<String> {
    qparam(query, key).map(urldecode)
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
                let _addr = format!("127.0.0.1:{port}");
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
        assert!(resp.contains("\"version\""), "body: {resp}");
        assert!(resp.contains("\"package\""), "body: {resp}");
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

    /// Seed a database with metric sets and poll cycle data for effective_poll tests.
    fn seed_effective_poll_db() -> Arc<Mutex<Connection>> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let n = SEQ.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "s7trace_api_eff_test_{}_{}_{}.db",
            std::process::id(),
            format!("{:?}", std::thread::current().id()).replace(':', "_"),
            n,
        ));
        let path_str = path.to_string_lossy().into_owned();
        let _ = std::fs::remove_file(&path);
        let conn = schema::open(&path_str).expect("open db");
        conn.execute_batch(
            "INSERT INTO plc (name, host) VALUES ('plc1', '127.0.0.1:102');
             INSERT INTO poll_group (name, period_ms) VALUES ('g1', 1000);
             INSERT INTO poll_group (name, period_ms) VALUES ('g2', 500);
             INSERT INTO tag (plc_id, name, address, data_type, poll_group_id,
                              db_num, byte_offset, byte_size, enabled, created_ts, updated_ts)
             VALUES (1, 'tagA', 'DB1.DBW0', 'i16', 1, 1, 0, 2, 1, 1000000, 1000000);
             INSERT INTO tag (plc_id, name, address, data_type, poll_group_id,
                              db_num, byte_offset, byte_size, enabled, created_ts, updated_ts)
             VALUES (1, 'tagB', 'DB1.DBW2', 'i16', 1, 1, 2, 2, 1, 1000000, 1000000);
             INSERT INTO tag (plc_id, name, address, data_type, poll_group_id,
                              db_num, byte_offset, byte_size, enabled, created_ts, updated_ts)
             VALUES (1, 'tagC', 'DB1.DBW4', 'i16', 2, 1, 4, 2, 1, 1000000, 1000000);
             INSERT INTO tag (plc_id, name, address, data_type, poll_group_id,
                              db_num, byte_offset, byte_size, enabled, created_ts, updated_ts)
             VALUES (1, 'tagD', 'DB1.DBW6', 'i16', 2, 1, 6, 2, 0, 1000000, 1000000);
             INSERT INTO metric_set (plc_id, name, enabled, created_ts, updated_ts) VALUES (1, 'set_single', 1, 1000000, 1000000);
             INSERT INTO metric_set (plc_id, name, enabled, created_ts, updated_ts) VALUES (1, 'set_mixed', 1, 1000000, 1000000);
             INSERT INTO metric_set (plc_id, name, enabled, created_ts, updated_ts) VALUES (1, 'set_none', 1, 1000000, 1000000);
             INSERT INTO metric_set_tag (metric_set_id, tag_id, plc_id, created_ts)
             VALUES (1, 1, 1, 1000000), (1, 2, 1, 1000000);
             INSERT INTO metric_set_tag (metric_set_id, tag_id, plc_id, created_ts)
             VALUES (2, 1, 1, 1000000), (2, 3, 1, 1000000);
             INSERT INTO dataset (plc_id, name, state, created_ts, updated_ts)
             VALUES (1, 'test_ds', 'active', 1000000, 1000000);
             INSERT INTO poll_cycle (plc_id, poll_group_id, dataset_id, seq, started_ts, finished_ts,
                                     actual_ms, target_ms, ok)
             VALUES (1, 1, 1, 1, 1000000, 2000000, 100.0, 1000, 1);",
        )
        .expect("seed effective_poll db");
        Arc::new(Mutex::new(conn))
    }

    #[test]
    fn test_status_effective_poll_single() {
        let db = seed_effective_poll_db();
        let port = spawn_server(db);
        let resp = http_get(&format!("127.0.0.1:{port}"), "/v1/status");
        assert!(resp.contains("200 OK"), "status: {resp}");
        // set_single has tagA and tagB, both in poll_group g1 (id=1)
        assert!(resp.contains("set_single"), "set_single not in: {resp}");
        assert!(
            resp.contains("\"mode\": \"single\""),
            "single mode not in: {resp}"
        );
        assert!(
            resp.contains("\"poll_group_name\": \"g1\""),
            "g1 not in: {resp}"
        );
        assert!(
            resp.contains("\"period_ms\": 1000"),
            "1000ms not in: {resp}"
        );
    }

    #[test]
    fn test_status_effective_poll_mixed() {
        let db = seed_effective_poll_db();
        let port = spawn_server(db);
        let resp = http_get(&format!("127.0.0.1:{port}"), "/v1/status");
        assert!(resp.contains("200 OK"), "status: {resp}");
        // set_mixed has tagA (poll_group g1) and tagC (poll_group g2)
        assert!(resp.contains("set_mixed"), "set_mixed not in: {resp}");
        assert!(
            resp.contains("\"mode\": \"mixed\""),
            "mixed mode not in: {resp}"
        );
    }

    #[test]
    fn test_status_effective_poll_none() {
        let db = seed_effective_poll_db();
        let port = spawn_server(db);
        let resp = http_get(&format!("127.0.0.1:{port}"), "/v1/status");
        assert!(resp.contains("200 OK"), "status: {resp}");
        // set_none has no metric_set_tag rows, so 0 enabled tags
        assert!(resp.contains("set_none"), "set_none not in: {resp}");
        assert!(
            resp.contains("\"mode\": \"none\""),
            "none mode not in: {resp}"
        );
    }

    #[test]
    fn test_status_last_poll_cycle_finished_ts() {
        let db = seed_effective_poll_db();
        let port = spawn_server(db);
        let resp = http_get(&format!("127.0.0.1:{port}"), "/v1/status");
        assert!(resp.contains("200 OK"), "status: {resp}");
        // PLC should have last_poll_cycle_finished_ts from the seeded poll_cycle
        assert!(
            resp.contains("last_poll_cycle_finished_ts"),
            "last_poll_cycle_finished_ts not in: {resp}"
        );
        assert!(resp.contains("2000000"), "2000000 timestamp not in: {resp}");
    }
}
