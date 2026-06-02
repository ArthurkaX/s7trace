/// Output formatting helpers for CLI commands.

use serde::Serialize;
use serde_json::Value;

/// Supported output formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Text,
    Table,
    Json,
    Jsonl,
}

impl Format {
    pub fn from_str(s: &str) -> Format {
        match s {
            "json" => Format::Json,
            "jsonl" => Format::Jsonl,
            "table" => Format::Table,
            _ => Format::Text,
        }
    }
}

/// Print a result in the requested format.
 pub fn print_result<T: Serialize + std::fmt::Debug>(format: Format, label: &str, value: &T) {
    match format {
        Format::Text => println!("{}={}", label, serde_json::to_string(value).unwrap_or_default()),
        Format::Table => {
            // Simple key=value lines
            if let Ok(map) = serde_json::to_value(value) {
                if let Some(obj) = map.as_object() {
                    for (k, v) in obj {
                        print_value(k, v);
                    }
                } else {
                    println!("{label}: {value:?}");
                }
            }
        }
        Format::Json => {
            println!("{}", serde_json::to_string_pretty(value).unwrap_or_default());
        }
        Format::Jsonl => {
            println!("{}", serde_json::to_string(value).unwrap_or_default());
        }
    }
}

/// Print a single key=value pair with proper quoting.
fn print_value(key: &str, value: &Value) {
    match value {
        Value::String(s) => println!("{key}={s}"),
        Value::Number(n) => println!("{key}={n}"),
        Value::Bool(b) => println!("{key}={b}"),
        Value::Null => println!("{key}=null"),
        Value::Array(_) | Value::Object(_) => {
            println!("{key}={}", serde_json::to_string(value).unwrap_or_default());
        }
    }
}

/// Format a value based on data type.
pub fn format_value(vi: Option<i64>, vf: Option<f64>) -> String {
    match (vi, vf) {
        (Some(i), _) => i.to_string(),
        (_, Some(f)) => format!("{:.4}", f),
        (None, None) => "null".to_string(),
    }
}

/// Print command success output (D010-style).
pub fn print_success(key_values: Vec<(&str, &dyn std::fmt::Display)>) {
    let parts: Vec<String> = key_values
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect();
    println!("success {}", parts.join(" "));
}

/// Print command failure output (D010-style).
pub fn print_bad(bad_class: &str, detail: &str) {
    println!("bad bad_class={bad_class} detail=\"{detail}\"");
}

/// Print hex dump (read-raw output).
pub fn print_hex_dump(data: &[u8], start: usize) {
    for (chunk_idx, chunk) in data.chunks(16).enumerate() {
        let offset = start + chunk_idx * 16;
        let hex: String = chunk
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<Vec<_>>()
            .chunks(2)
            .map(|pair| pair.join(""))
            .collect::<Vec<_>>()
            .join(" ");
        let ascii: String = chunk
            .iter()
            .map(|b| if b.is_ascii_graphic() || *b == b' ' { *b as char } else { '.' })
            .collect();
        println!("{offset:08x}  {hex:<48}  {ascii}");
    }
}

/// Print machine-readable JSON for read-raw.
pub fn print_json_hex(plc: &str, db: u16, start: usize, len: usize, ts_us: i64, data: &[u8]) {
    let hex_str: String = data.iter().map(|b| format!("{b:02x}")).collect();
    let output = serde_json::json!({
        "plc": plc,
        "db": db,
        "start": start,
        "len": len,
        "ts_us": ts_us,
        "bytes_hex": hex_str,
    });
    println!("{output}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_value_i16() {
        assert_eq!(format_value(Some(42), None), "42");
    }

    #[test]
    fn test_format_value_f32() {
        let s = format_value(None, Some(3.1415));
        assert!(s.starts_with("3.14"));
    }

    #[test]
    fn test_format_value_null() {
        assert_eq!(format_value(None, None), "null");
    }
}
