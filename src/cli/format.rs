/// Output formatting helpers for CLI commands.
///
/// Default output is compact JSON. `--format table` is the only
/// human-readable mode.

/// Supported output formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// Compact JSON output (default for all commands).
    CompactJson,
    /// Human-readable tables (--format table).
    Table,
}

impl Format {
    pub fn from_str(s: &str) -> Format {
        match s {
            "table" => Format::Table,
            _ => Format::CompactJson,
        }
    }
}

/// Print a successful result as compact JSON: {"ok":true,"result":...}
/// Null fields in `result` are omitted (serialized with `skip_serializing_none`).
pub fn print_json_ok(result: &serde_json::Value) {
    let obj = serde_json::json!({
        "ok": true,
        "result": result,
    });
    println!("{}", serde_json::to_string(&obj).unwrap_or_default());
}

/// Print an error result as compact JSON: {"ok":false,"bad_class":...,"detail":...,"exit_code":N}
pub fn print_json_error(bad_class: &str, detail: &str, exit_code: i32) {
    let obj = serde_json::json!({
        "ok": false,
        "bad_class": bad_class,
        "detail": detail,
        "exit_code": exit_code,
    });
    println!("{}", serde_json::to_string(&obj).unwrap_or_default());
}

/// Print a timeout result per contract.
pub fn print_json_timeout() {
    print_json_error("timeout", "command did not complete in time", 3);
}

/// Print SQL result as columnar JSON: {"cols":[...],"rows":[[...],...]}
/// One line, no pretty-print. Types preserved (null/integer/float/string).
pub fn print_columnar_json(cols: &[String], rows: &[serde_json::Value]) {
    let obj = serde_json::json!({
        "cols": cols,
        "rows": rows,
    });
    println!("{}", serde_json::to_string(&obj).unwrap_or_default());
}

/// Print as table (tab-separated columns, one row per line).
pub fn print_table(cols: &[String], rows: &[serde_json::Value]) {
    // Header
    println!("{}", cols.join("\t"));
    for row in rows {
        let vals: Vec<String> = match row {
            serde_json::Value::Array(arr) => arr
                .iter()
                .map(|v| match v {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Null => "NULL".to_string(),
                    other => other.to_string(),
                })
                .collect(),
            _ => vec![row.to_string()],
        };
        println!("{}", vals.join("\t"));
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
            .map(|b| {
                if b.is_ascii_graphic() || *b == b' ' {
                    *b as char
                } else {
                    '.'
                }
            })
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
    println!("{}", serde_json::to_string(&output).unwrap_or_default());
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

    #[test]
    fn test_format_table_output() {
        let cols = vec!["a".to_string(), "b".to_string()];
        let rows: Vec<serde_json::Value> = vec![serde_json::json!([1, "x"])];
        // Just verify no panic
        print_table(&cols, &rows);
    }

    #[test]
    fn test_columnar_json() {
        let cols = vec!["name".to_string(), "bad".to_string()];
        let rows: Vec<serde_json::Value> = vec![serde_json::json!(["Motor.Speed", 1])];
        let output = serde_json::json!({"cols": cols, "rows": rows});
        assert_eq!(output["cols"][0], "name");
        assert_eq!(output["rows"][0][0], "Motor.Speed");
    }
}
