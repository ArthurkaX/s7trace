/// Resolve a server address to a base URL.
///
/// Accepts:
/// - A bare IP/host (e.g. `192.168.0.50`) → `http://host:port`
/// - A full URL (e.g. `http://192.168.0.50:7707`) → used as-is
/// - A host with embedded port (e.g. `host:7707`) → `http://host:port`
///
/// The `default_port` is used when no port is specified (typically 7707).
pub fn resolve_url(host_or_url: &str, default_port: u16) -> String {
    let s = host_or_url.trim();
    // If it already has a scheme, use as-is (strip trailing slash)
    if s.contains("://") {
        return s.trim_end_matches('/').to_string();
    }
    // Bare host or host:port
    if let Some((host, port_str)) = s.split_once(':') {
        let port: u16 = port_str.parse().unwrap_or(default_port);
        return format!("http://{}:{}", host, port);
    }
    format!("http://{}:{}", s, default_port)
}

use anyhow::Result;
use std::fmt;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use crate::db::command_queue::CommandListRow;
use crate::types::{coded, CommandKind, CommandResult, CommandState, ErrorCode};

/// A failure talking to the server, kept typed so the CLI can tell an
/// unreachable server from one that answered with an error.
#[derive(Debug)]
pub enum RemoteError {
    /// No connection: refused, unresolvable host, reset or socket timeout.
    Unavailable(String),
    /// The server answered with a non-2xx status; `message` is its `error`.
    Http { status: u16, message: String },
    /// The response could not be read as HTTP/JSON.
    Protocol(String),
}

impl RemoteError {
    pub fn code(&self) -> ErrorCode {
        match self {
            RemoteError::Unavailable(_) => ErrorCode::ServerUnavailable,
            RemoteError::Http { status, .. } => match status {
                400 => ErrorCode::InvalidArgument,
                401 | 403 => ErrorCode::Unauthorized,
                404 => ErrorCode::NotFound,
                409 => ErrorCode::Conflict,
                503 => ErrorCode::ServerUnavailable,
                _ => ErrorCode::ServerError,
            },
            RemoteError::Protocol(_) => ErrorCode::ServerError,
        }
    }
}

impl fmt::Display for RemoteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RemoteError::Unavailable(m) => write!(f, "server unavailable: {m}"),
            // A 4xx is about the request: the server's own message says it
            // all, and `code` carries the status class.
            RemoteError::Http { status, message } if (400..500).contains(status) => {
                f.write_str(message)
            }
            RemoteError::Http { status, message } => write!(f, "server error {status}: {message}"),
            RemoteError::Protocol(m) => write!(f, "invalid server response: {m}"),
        }
    }
}

impl std::error::Error for RemoteError {}

/// Connect to a remote s7trace server and enqueue a command.
///
/// `kind` is the JSON string for `CommandKind` ("query" | "control" |
/// "plc_read" | "admin"). `request_json` is the body of the request
/// (e.g. `{"action": "tag_add_full", "plc": ..., "name": ...}`).
/// `plc_id` is optional.
pub fn enqueue_command(
    base_url: &str,
    kind: &str,
    request_json: &str,
    actor: Option<&str>,
    plc_id: Option<i64>,
    timeout: Duration,
) -> Result<i64> {
    let body = serde_json::json!({
        "kind": kind,
        "request": serde_json::from_str::<serde_json::Value>(request_json)
            .unwrap_or_else(|_| serde_json::Value::String(request_json.to_string())),
        "actor": actor,
        "plc_id": plc_id,
    });
    let body_str = body.to_string();
    let resp = http_post(base_url, "/v1/command", &body_str, timeout)?;
    let v = parse_json("/v1/command", &resp)?;
    v["id"].as_i64().ok_or_else(|| {
        RemoteError::Protocol(format!("/v1/command did not return id: {resp}")).into()
    })
}

/// Server-side view of one command: its state and, once finished, its result.
#[derive(Debug, Clone)]
pub struct CommandStatus {
    pub id: i64,
    pub kind: Option<CommandKind>,
    pub state: CommandState,
    pub result: Option<CommandResult>,
}

impl CommandStatus {
    /// Done, failed or cancelled: the state will not change any more.
    pub fn finished(&self) -> bool {
        self.result.is_some()
            || matches!(
                self.state,
                CommandState::Done | CommandState::Bad | CommandState::Cancelled
            )
    }

    pub fn state_str(&self) -> &'static str {
        match self.state {
            CommandState::Pending => "pending",
            CommandState::Running => "running",
            CommandState::Done => "done",
            CommandState::Bad => "bad",
            CommandState::Cancelled => "cancelled",
        }
    }
}

/// `GET /v1/command/{id}`.
pub fn get_command(base_url: &str, id: i64, timeout: Duration) -> Result<CommandStatus> {
    let url = format!("/v1/command/{id}");
    let resp = http_get(base_url, &url, timeout)?;
    let v = parse_json(&url, &resp)?;
    parse_command_status(id, &v)
}

/// Parse the `GET /v1/command/{id}` body. The server writes `state` and `kind`
/// in Rust Debug form (`"PlcRead"`), so both that and snake_case are accepted.
fn parse_command_status(id: i64, v: &serde_json::Value) -> Result<CommandStatus> {
    let state_raw = v["command"]["state"].as_str().unwrap_or("");
    let state: CommandState =
        serde_json::from_value(serde_json::Value::String(snake_case(state_raw))).map_err(|_| {
            RemoteError::Protocol(format!("command {id}: unknown state '{state_raw}'"))
        })?;
    let kind = v["command"]["kind"]
        .as_str()
        .and_then(|k| serde_json::from_value(serde_json::Value::String(snake_case(k))).ok());
    let result = match &v["result"] {
        serde_json::Value::Null => None,
        r => Some(CommandResult {
            command_id: id,
            ok: r["ok"].as_bool().unwrap_or(false),
            result_json: r["result_json"].as_str().map(str::to_string),
            error: r["error"].as_str().map(str::to_string),
        }),
    };
    Ok(CommandStatus {
        id,
        kind,
        state,
        result,
    })
}

fn snake_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    for (i, c) in s.chars().enumerate() {
        if c.is_ascii_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// `GET /v1/command?state=...`: the most recent commands, newest first.
pub fn list_commands(
    base_url: &str,
    state: Option<&str>,
    timeout: Duration,
) -> Result<Vec<CommandListRow>> {
    let url = match state {
        Some(st) => format!("/v1/command?limit=100&state={}", urlencode(st)),
        None => "/v1/command?limit=100".to_string(),
    };
    let resp = http_get(base_url, &url, timeout)?;
    let v = parse_json(&url, &resp)?;
    serde_json::from_value(v["commands"].clone())
        .map_err(|e| RemoteError::Protocol(format!("{url}: {e}")).into())
}

/// Fetch /v1/status and return the raw JSON string.
pub fn get_status(base_url: &str, timeout: Duration) -> Result<serde_json::Value> {
    let resp = http_get(base_url, "/v1/status", timeout)?;
    parse_json("/v1/status", &resp)
}

/// Fetch /v1/tags?... and return the JSON array.
pub fn get_tags(
    base_url: &str,
    plc: Option<&str>,
    enabled: Option<bool>,
    bad: Option<bool>,
    timeout: Duration,
) -> Result<serde_json::Value> {
    let mut q = Vec::new();
    if let Some(p) = plc {
        q.push(format!("plc={}", urlencode(p)));
    }
    if let Some(e) = enabled {
        q.push(format!("enabled={e}"));
    }
    if let Some(b) = bad {
        q.push(format!("bad={b}"));
    }
    let url = if q.is_empty() {
        "/v1/tags".to_string()
    } else {
        format!("/v1/tags?{}", q.join("&"))
    };
    let resp = http_get(base_url, &url, timeout)?;
    parse_json(&url, &resp)
}

/// Fetch /v1/sets?plc=... and return the JSON array.
pub fn get_set_list(
    base_url: &str,
    plc: Option<&str>,
    timeout: Duration,
) -> Result<serde_json::Value> {
    let url = if let Some(p) = plc {
        format!("/v1/sets?plc={}", urlencode(p))
    } else {
        "/v1/sets".to_string()
    };
    let resp = http_get(base_url, &url, timeout)?;
    parse_json(&url, &resp)
}

/// Fetch /v1/set/{name} and return the JSON object.
pub fn get_set_show(
    base_url: &str,
    set_name: &str,
    plc: Option<&str>,
    timeout: Duration,
) -> Result<serde_json::Value> {
    let mut url = format!("/v1/set/{}", urlencode(set_name));
    // Set names repeat across PLCs; pass the operator's selector on so the
    // server resolves the same set the local path would.
    if let Some(p) = plc {
        url.push_str(&format!("?plc={}", urlencode(p)));
    }
    let resp = http_get(base_url, &url, timeout)?;
    parse_json(&url, &resp)
}

/// POST /v1/command/{id}/cancel to cancel a pending command.
pub fn cancel_command(base_url: &str, command_id: i64, timeout: Duration) -> Result<()> {
    let path = format!("/v1/command/{command_id}/cancel");
    // 204 No Content on success; 409 when the command is no longer pending.
    http_post(base_url, &path, "", timeout)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Minimal std::net HTTP client (no external crate)
// ---------------------------------------------------------------------------

/// Optional `Authorization` header line for outgoing requests, from
/// `S7TRACE_TOKEN`. Empty when unset (auth disabled). Includes trailing CRLF.
fn auth_header() -> String {
    match std::env::var("S7TRACE_TOKEN") {
        Ok(t) if !t.is_empty() => format!("Authorization: Bearer {t}\r\n"),
        _ => String::new(),
    }
}

pub fn http_get(base_url: &str, path: &str, timeout: Duration) -> Result<String> {
    let auth = auth_header();
    let (host, port) = parse_base(base_url)?;
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\n{auth}Connection: close\r\nAccept: application/json\r\n\r\n"
    );
    exchange(&host, port, &req, timeout)
}

fn http_post(base_url: &str, path: &str, body: &str, timeout: Duration) -> Result<String> {
    let auth = auth_header();
    let (host, port) = parse_base(base_url)?;
    let req = format!(
        "POST {path} HTTP/1.1\r\n\
         Host: {host}\r\n\
         {auth}Connection: close\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {len}\r\n\
         Accept: application/json\r\n\
         \r\n{body}",
        len = body.len()
    );
    exchange(&host, port, &req, timeout)
}

fn parse_json(url: &str, resp: &str) -> Result<serde_json::Value> {
    serde_json::from_str(resp)
        .map_err(|e| RemoteError::Protocol(format!("invalid JSON from {url}: {e}: {resp}")).into())
}

/// Send one request and read the whole response. Any socket failure is
/// [`RemoteError::Unavailable`]; a host name is resolved, not just an IP.
fn exchange(host: &str, port: u16, req: &str, timeout: Duration) -> Result<String> {
    let addr = format!("{host}:{port}");
    let unavailable = |e: std::io::Error| RemoteError::Unavailable(format!("{addr}: {e}"));
    // Try every resolved address: `localhost` may resolve to `::1` first
    // while the server listens on 127.0.0.1 only.
    let mut last_err = None;
    let mut stream = None;
    for sock in (host, port).to_socket_addrs().map_err(unavailable)? {
        match TcpStream::connect_timeout(&sock, timeout) {
            Ok(s) => {
                stream = Some(s);
                break;
            }
            Err(e) => last_err = Some(e),
        }
    }
    let mut stream = match (stream, last_err) {
        (Some(s), _) => s,
        (None, Some(e)) => return Err(unavailable(e).into()),
        (None, None) => return Err(RemoteError::Unavailable(format!("{addr}: no address")).into()),
    };
    stream
        .set_read_timeout(Some(timeout))
        .map_err(unavailable)?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(unavailable)?;
    stream.write_all(req.as_bytes()).map_err(unavailable)?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).map_err(unavailable)?;
    Ok(parse_http_response(&buf)?)
}

fn parse_base(s: &str) -> Result<(String, u16)> {
    let s = s.trim();
    let s = s
        .strip_prefix("http://")
        .or_else(|| s.strip_prefix("https://"))
        .ok_or_else(|| {
            coded(
                ErrorCode::InvalidArgument,
                format!("URL must start with http:// or https://: {s}"),
            )
        })?;
    // No TLS support yet; warn if https
    if s.starts_with("https://") {
        return Err(coded(
            ErrorCode::InvalidArgument,
            format!("https is not supported (no TLS dependency): {s}"),
        ));
    }
    // strip trailing slash
    let s = s.trim_end_matches('/');
    if let Some((host_port, _path)) = s.split_once('/') {
        let (h, p) = host_port.split_once(':').ok_or_else(|| {
            coded(
                ErrorCode::InvalidArgument,
                format!("missing port in URL: {s}"),
            )
        })?;
        let port: u16 = p.parse().map_err(|_| {
            coded(
                ErrorCode::InvalidArgument,
                format!("invalid port in URL: {s}"),
            )
        })?;
        Ok((h.to_string(), port))
    } else {
        if let Some((h, p)) = s.split_once(':') {
            let port: u16 = p.parse().map_err(|_| {
                coded(
                    ErrorCode::InvalidArgument,
                    format!("invalid port in URL: {s}"),
                )
            })?;
            Ok((h.to_string(), port))
        } else {
            Ok((s.to_string(), 80))
        }
    }
}

fn parse_http_response(buf: &[u8]) -> std::result::Result<String, RemoteError> {
    let s = std::str::from_utf8(buf)
        .map_err(|_| RemoteError::Protocol("non-UTF8 HTTP response".into()))?;
    let (head, body) = s.split_once("\r\n\r\n").unwrap_or((s, ""));
    let status_line = head
        .lines()
        .next()
        .filter(|l| !l.is_empty())
        .ok_or_else(|| RemoteError::Protocol("empty HTTP response".into()))?;
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .ok_or_else(|| RemoteError::Protocol(format!("bad status line: {status_line}")))?;
    if !(200..300).contains(&status) {
        // The server's errors are {"error": "..."}; fall back to the raw body.
        let message = serde_json::from_str::<serde_json::Value>(body)
            .ok()
            .and_then(|v| v["error"].as_str().map(str::to_string))
            .unwrap_or_else(|| body.trim().to_string());
        return Err(RemoteError::Http { status, message });
    }
    Ok(body.to_string())
}

pub fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                out.push_str(&format!("%{b:02X}"));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_2xx_including_204_is_success() {
        assert_eq!(
            parse_http_response(b"HTTP/1.1 204 No Content\r\n\r\n").unwrap(),
            ""
        );
        // A body that itself contains a blank line is kept whole.
        assert_eq!(
            parse_http_response(b"HTTP/1.1 200 OK\r\n\r\na\r\n\r\nb").unwrap(),
            "a\r\n\r\nb"
        );
    }

    #[test]
    fn http_error_carries_status_and_server_message() {
        match parse_http_response(b"HTTP/1.1 409 Conflict\r\n\r\n{\"error\":\"not pending\"}") {
            Err(e @ RemoteError::Http { status: 409, .. }) => {
                assert_eq!(e.code(), ErrorCode::Conflict);
                assert!(e.to_string().contains("not pending"));
            }
            other => panic!("expected 409, got {other:?}"),
        }
        assert!(matches!(
            parse_http_response(b""),
            Err(RemoteError::Protocol(_))
        ));
    }

    #[test]
    fn unreachable_server_is_unavailable() {
        // Port 1 on localhost: nothing listens there.
        let e = http_get(
            "http://127.0.0.1:1",
            "/v1/status",
            Duration::from_millis(500),
        )
        .unwrap_err();
        let r = e.downcast_ref::<RemoteError>().expect("RemoteError");
        assert_eq!(r.code(), ErrorCode::ServerUnavailable);
    }

    #[test]
    fn command_status_accepts_debug_and_snake_case() {
        let v = serde_json::json!({
            "command": {"id": 3, "kind": "PlcRead", "state": "Running"},
            "result": null,
        });
        let st = parse_command_status(3, &v).unwrap();
        assert_eq!(st.kind, Some(CommandKind::PlcRead));
        assert_eq!(st.state, CommandState::Running);
        assert!(!st.finished());

        let v = serde_json::json!({
            "command": {"id": 3, "kind": "control", "state": "done"},
            "result": {"ok": true, "result_json": "{}", "error": null},
        });
        let st = parse_command_status(3, &v).unwrap();
        assert!(st.finished());
        assert!(st.result.unwrap().ok);
    }

    #[test]
    fn test_parse_base() {
        assert_eq!(
            parse_base("http://10.0.0.1:8080").unwrap(),
            ("10.0.0.1".to_string(), 8080)
        );
        assert_eq!(
            parse_base("http://10.0.0.1:8080/").unwrap(),
            ("10.0.0.1".to_string(), 8080)
        );
        assert_eq!(parse_base("http://host").unwrap(), ("host".to_string(), 80));
    }

    #[test]
    fn test_resolve_url_bare_ip_default_port() {
        assert_eq!(resolve_url("192.0.2.56", 7707), "http://192.0.2.56:7707");
    }

    #[test]
    fn test_resolve_url_bare_host_custom_port() {
        assert_eq!(resolve_url("plc-host", 7708), "http://plc-host:7708");
    }

    #[test]
    fn test_resolve_url_full_url_unchanged() {
        assert_eq!(
            resolve_url("http://plc-host:8080", 7707),
            "http://plc-host:8080"
        );
    }

    #[test]
    fn test_resolve_url_bare_host_default_port() {
        assert_eq!(resolve_url("my-plc", 7707), "http://my-plc:7707");
    }

    #[test]
    fn test_resolve_url_host_with_port() {
        assert_eq!(resolve_url("plc-host:9090", 7707), "http://plc-host:9090");
    }

    #[test]
    fn test_resolve_url_https_unchanged() {
        assert_eq!(
            resolve_url("https://secure-host:443", 7707),
            "https://secure-host:443"
        );
    }

    #[test]
    fn test_urlencode() {
        assert_eq!(urlencode("hello world"), "hello%20world");
        assert_eq!(urlencode("a-b_c.d~e"), "a-b_c.d~e");
    }

    #[test]
    fn test_parse_http_response_ok() {
        let resp = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"x\":1}";
        assert_eq!(parse_http_response(resp).unwrap(), "{\"x\":1}");
    }

    #[test]
    fn test_parse_http_response_500() {
        let resp = b"HTTP/1.1 500 Internal\r\n\r\nerror";
        assert!(parse_http_response(resp).is_err());
    }
}
