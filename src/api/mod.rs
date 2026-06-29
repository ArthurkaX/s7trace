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
//! Transport is `axum` (on the `tokio` runtime). The routing, request
//! handlers, and auth all live in this module and its siblings
//! (`handlers`, `status`); the axum layer is a thin adapter that captures the
//! method/path/query/body/`Authorization` header, enforces the optional bearer
//! token, and delegates to [`route`] on a blocking task (the DB layer is sync
//! `rusqlite`). It then maps the resulting [`Response`] back to an HTTP
//! response. Keeping one fallback handler means the route table is the single
//! `match` in [`route`], unchanged across the transport swap.

mod handlers;
mod status;

use crate::version;
use axum::body::{Body, Bytes};
use axum::extract::{Request, State};
use axum::http::{header, StatusCode};
use axum::response::Response as HttpResponse;
use axum::Router;
use parking_lot::Mutex;
use rusqlite::Connection;
use std::sync::Arc;

/// Max request body we will buffer (1 MiB). Command payloads are tiny JSON.
const MAX_BODY_BYTES: usize = 1024 * 1024;

/// Start the HTTP API on `bind_addr` (e.g. "127.0.0.1:8080").
/// Blocks the calling thread; spawn a thread to run in background.
pub fn serve(db: Arc<Mutex<Connection>>, bind_addr: String) -> std::io::Result<()> {
    run_on_runtime(async move {
        let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
        tracing::info!("HTTP API listening on http://{bind_addr}");
        axum::serve(listener, router(db)).await
    })
}

/// Like `serve`, but takes a pre-bound std `TcpListener` (useful for tests
/// that want to discover the bound port without a race).
pub fn serve_with_listener(
    listener: std::net::TcpListener,
    db: Arc<Mutex<Connection>>,
) -> std::io::Result<()> {
    run_on_runtime(async move {
        listener.set_nonblocking(true)?;
        let listener = tokio::net::TcpListener::from_std(listener)?;
        axum::serve(listener, router(db)).await
    })
}

/// Build a fresh single-purpose tokio runtime and block on `fut`. The HTTP API
/// runs on its own OS thread (spawned by the server, or by the test harness),
/// so owning a runtime here keeps it self-contained.
fn run_on_runtime<F>(fut: F) -> std::io::Result<()>
where
    F: std::future::Future<Output = std::io::Result<()>>,
{
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(fut)
}

/// The axum application: a single fallback handler over all routes, with the
/// shared DB connection as state.
fn router(db: Arc<Mutex<Connection>>) -> Router {
    Router::new().fallback(dispatch).with_state(db)
}

/// Adapter handler: capture the request, enforce auth, delegate to [`route`]
/// on a blocking task, and map the [`Response`] back to HTTP.
async fn dispatch(State(db): State<Arc<Mutex<Connection>>>, req: Request) -> HttpResponse {
    let method = req.method().as_str().to_string();
    let uri = req.uri().clone();
    // Reconstruct the raw request target so `split_query` parses it exactly as
    // the old transport did (values stay percent-encoded until `param_owned`).
    let target = match uri.query() {
        Some(q) => format!("{}?{}", uri.path(), q),
        None => uri.path().to_string(),
    };
    // Keep original case — the token is case-sensitive.
    let auth_value = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let body = match axum::body::to_bytes(req.into_body(), MAX_BODY_BYTES).await {
        Ok(b) => b,
        Err(_) => return to_http(Response::err(400, "request body too large or unreadable")),
    };

    // Optional bearer-token auth (S7TRACE_TOKEN). Disabled when the env var is
    // unset/empty — then the server is open, as before.
    if !is_authorized(required_token().as_deref(), auth_value.as_deref()) {
        return to_http(Response::err(401, "unauthorized: missing or invalid token"));
    }

    let (path, query) = split_query(&target);
    // `route` is synchronous and locks the rusqlite connection; run it off the
    // async worker so a slow query can't stall the runtime.
    let resp = tokio::task::spawn_blocking(move || route(&db, &method, &path, &query, &body))
        .await
        .unwrap_or_else(|e| Response::err(500, &format!("handler task failed: {e}")));

    to_http(resp)
}

/// Map our internal [`Response`] to an axum HTTP response, preserving the
/// permissive CORS header the old transport sent.
fn to_http(resp: Response) -> HttpResponse {
    let status = StatusCode::from_u16(resp.status).unwrap_or(StatusCode::OK);
    let body: Bytes = resp.body.into();
    HttpResponse::builder()
        .status(status)
        .header(header::CONTENT_TYPE, resp.content_type)
        .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
        .body(Body::from(body))
        .unwrap_or_else(|_| HttpResponse::new(Body::empty()))
}

/// The shared bearer token the server requires, from `S7TRACE_TOKEN`.
/// `None` (unset or empty) disables authentication — the server is open.
fn required_token() -> Option<String> {
    std::env::var("S7TRACE_TOKEN")
        .ok()
        .filter(|s| !s.is_empty())
}

/// Whether a request is authorized. `expected` is the configured token
/// (`None` => auth disabled => always authorized). `authorization` is the raw
/// `Authorization` header value, if any.
fn is_authorized(expected: Option<&str>, authorization: Option<&str>) -> bool {
    let Some(expected) = expected else {
        return true; // auth disabled
    };
    authorization
        .and_then(|v| {
            v.strip_prefix("Bearer ")
                .or_else(|| v.strip_prefix("bearer "))
        })
        .map(str::trim)
        == Some(expected)
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
    use std::net::{TcpListener, TcpStream};
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

    // ---------------------------------------------------------------------------
    // is_authorized — pure, no env vars, no server, no threads
    // ---------------------------------------------------------------------------

    #[test]
    fn is_authorized_no_token_always_true() {
        // auth disabled (expected=None) => always authorized
        assert!(is_authorized(None, None));
        assert!(is_authorized(None, Some("Bearer anything")));
    }

    #[test]
    fn is_authorized_missing_header() {
        assert!(!is_authorized(Some("s3cret"), None));
    }

    #[test]
    fn is_authorized_bearer_exact_match() {
        assert!(is_authorized(Some("s3cret"), Some("Bearer s3cret")));
    }

    #[test]
    fn is_authorized_bearer_lowercase_scheme() {
        assert!(is_authorized(Some("s3cret"), Some("bearer s3cret")));
    }

    #[test]
    fn is_authorized_wrong_token() {
        assert!(!is_authorized(Some("s3cret"), Some("Bearer wrong")));
    }

    #[test]
    fn is_authorized_no_scheme_rejected() {
        // Token value without "Bearer " prefix is not accepted
        assert!(!is_authorized(Some("s3cret"), Some("s3cret")));
    }
}
