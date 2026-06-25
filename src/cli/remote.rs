//! Remote client: thin HTTP wrapper used by CLI commands when the
//! s7trace server is on a different host (no direct PLC access).
//!
//! Endpoints used:
//!   POST /v1/command        -> enqueue a command, returns {id}
//!   GET  /v1/command/{id}   -> poll a single command, returns id+state+result
//!   GET  /v1/status         -> status snapshot (used by `status` CLI)

use anyhow::{anyhow, Context, Result};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

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
    let v: serde_json::Value = serde_json::from_str(&resp)
        .with_context(|| format!("invalid JSON from /v1/command: {resp}"))?;
    v["id"].as_i64()
        .ok_or_else(|| anyhow!("/v1/command did not return id: {resp}"))
}

/// Poll a single command until it leaves the 'pending' or 'running' state.
/// Returns the final `state` ("ok", "failed", "cancelled", "rejected") and
/// the `result` string from the server.
pub fn wait_command(
    base_url: &str,
    id: i64,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<(String, String, bool, String)> {
    let started = Instant::now();
    let url = format!("/v1/command/{id}");
    loop {
        let resp = http_get(base_url, &url, timeout)?;
        let v: serde_json::Value = serde_json::from_str(&resp)
            .with_context(|| format!("invalid JSON from {url}: {resp}"))?;
        // Server returns state as Debug-formatted (e.g. "Pending", "Running",
        // "Ok", "Failed", "Cancelled", "Rejected"). Normalize to lowercase.
        let state_raw = v["command"]["state"].as_str().unwrap_or("unknown").to_string();
        let state = state_raw.to_lowercase();
        let result_json = v["result"]["result_json"].as_str().unwrap_or("");
        let ok = v["result"]["ok"].as_bool().unwrap_or(false);
        let error = v["result"]["error"].as_str().unwrap_or("").to_string();
        // The command is finished when there is a `result` row (which only
        // appears after the worker writes command_result).
        let finished = v.get("result").map(|r| !r.is_null()).unwrap_or(false);
        if finished {
            return Ok((state, result_json.to_string(), ok, error));
        }
        if state == "cancelled" || state == "rejected" {
            return Ok((state, result_json.to_string(), ok, error));
        }
        if started.elapsed() > timeout {
            return Err(anyhow!(
                "command {id} still in state '{state}' after {:?}",
                timeout
            ));
        }
        std::thread::sleep(poll_interval);
    }
}

/// Fetch /v1/status and return the raw JSON string.
pub fn get_status(base_url: &str, timeout: Duration) -> Result<serde_json::Value> {
    let resp = http_get(base_url, "/v1/status", timeout)?;
    serde_json::from_str(&resp)
        .with_context(|| format!("invalid JSON from /v1/status: {resp}"))
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
    if let Some(p) = plc { q.push(format!("plc={}", urlencode(p))); }
    if let Some(e) = enabled { q.push(format!("enabled={e}")); }
    if let Some(b) = bad { q.push(format!("bad={b}")); }
    let url = if q.is_empty() { "/v1/tags".to_string() } else { format!("/v1/tags?{}", q.join("&")) };
    let resp = http_get(base_url, &url, timeout)?;
    serde_json::from_str(&resp)
        .with_context(|| format!("invalid JSON from {url}: {resp}"))
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
    serde_json::from_str(&resp)
        .with_context(|| format!("invalid JSON from {url}: {resp}"))
}

/// Fetch /v1/set/{name} and return the JSON object.
pub fn get_set_show(
    base_url: &str,
    set_name: &str,
    timeout: Duration,
) -> Result<serde_json::Value> {
    let url = format!("/v1/set/{}", urlencode(set_name));
    let resp = http_get(base_url, &url, timeout)?;
    serde_json::from_str(&resp)
        .with_context(|| format!("invalid JSON from {url}: {resp}"))
}

/// POST /v1/command/{id}/cancel to cancel a pending command.
pub fn cancel_command(base_url: &str, command_id: i64, timeout: Duration) -> Result<()> {
    let path = format!("/v1/command/{command_id}/cancel");
    let resp = http_post(base_url, &path, "", timeout)?;
    // Server returns 204 No Content on success
    let _ = resp;
    Ok(())
}

// ---------------------------------------------------------------------------
// Minimal std::net HTTP client (no external crate)
// ---------------------------------------------------------------------------

pub fn http_get(base_url: &str, path: &str, timeout: Duration) -> Result<String> {
    let (host, port) = parse_base(base_url)?;
    let addr = format!("{host}:{port}");
    let mut stream = TcpStream::connect_timeout(&addr.parse()?, timeout)
        .with_context(|| format!("connect to {addr} failed"))?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;

    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\nAccept: application/json\r\n\r\n"
    );
    stream.write_all(req.as_bytes())?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf)?;
    parse_http_response(&buf)
}

fn http_post(base_url: &str, path: &str, body: &str, timeout: Duration) -> Result<String> {
    let (host, port) = parse_base(base_url)?;
    let addr = format!("{host}:{port}");
    let mut stream = TcpStream::connect_timeout(&addr.parse()?, timeout)
        .with_context(|| format!("connect to {addr} failed"))?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;

    let req = format!(
        "POST {path} HTTP/1.1\r\n\
         Host: {host}\r\n\
         Connection: close\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {len}\r\n\
         Accept: application/json\r\n\
         \r\n{body}",
        len = body.len()
    );
    stream.write_all(req.as_bytes())?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf)?;
    parse_http_response(&buf)
}

fn parse_base(s: &str) -> Result<(String, u16)> {
    let s = s.trim();
    let s = s.strip_prefix("http://")
        .or_else(|| s.strip_prefix("https://"))
        .ok_or_else(|| anyhow!("URL must start with http:// or https://: {s}"))?;
    // No TLS support yet; warn if https
    if s.starts_with("https://") {
        return Err(anyhow!("https is not supported (no TLS dependency): {s}"));
    }
    // strip trailing slash
    let s = s.trim_end_matches('/');
    if let Some((host_port, _path)) = s.split_once('/') {
        let (h, p) = host_port.split_once(':')
            .ok_or_else(|| anyhow!("missing port in URL: {s}"))?;
        let port: u16 = p.parse().context("invalid port")?;
        Ok((h.to_string(), port))
    } else {
        if let Some((h, p)) = s.split_once(':') {
            let port: u16 = p.parse().context("invalid port")?;
            Ok((h.to_string(), port))
        } else {
            Ok((s.to_string(), 80))
        }
    }
}

fn parse_http_response(buf: &[u8]) -> Result<String> {
    let s = std::str::from_utf8(buf).context("non-UTF8 HTTP response")?;
    let mut parts = s.split("\r\n\r\n");
    let head = parts.next().ok_or_else(|| anyhow!("empty HTTP response"))?;
    let body = parts.next().unwrap_or("").to_string();
    let status_line = head.lines().next().ok_or_else(|| anyhow!("missing status line"))?;
    if !status_line.contains(" 200 ") && !status_line.contains(" 201 ") {
        return Err(anyhow!("HTTP error: {status_line} body={body}"));
    }
    Ok(body)
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
    fn test_parse_base() {
        assert_eq!(parse_base("http://10.0.0.1:8080").unwrap(), ("10.0.0.1".to_string(), 8080));
        assert_eq!(parse_base("http://10.0.0.1:8080/").unwrap(), ("10.0.0.1".to_string(), 8080));
        assert_eq!(parse_base("http://host").unwrap(), ("host".to_string(), 80));
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
