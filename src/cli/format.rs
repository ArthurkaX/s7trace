/// Output formatting helpers for CLI commands.
///
/// `--format` is parsed by clap into [`Format`], so a misspelt value is
/// rejected instead of silently falling back to some other output.
///
/// Output formats shared by the listing/report commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Format {
    /// Human-readable text or tab-separated table.
    #[value(alias = "table")]
    Text,
    /// One compact JSON document on a single line.
    Json,
    /// JSON Lines: one compact JSON object per record.
    Jsonl,
}

/// `sample-export` row encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum ExportFormat {
    /// One JSON object per sample.
    Jsonl,
    /// Comma-separated values with a header row.
    Csv,
}

/// `read` always answers with one JSON object. The flag stays so scripts that
/// already pass `--format json` keep working.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum ReadFormat {
    Json,
}

/// Byte encoding the server uses in a `read-raw` result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum RawFormat {
    /// `bytes_hex`: one hex string.
    #[value(alias = "raw")]
    Hex,
    /// `bytes_raw` plus `bytes_dec`: decimal byte values.
    Dec,
    /// `bytes_raw`: an array of byte values.
    Json,
}

impl RawFormat {
    /// The name the server's `read_raw` action expects.
    pub fn as_str(self) -> &'static str {
        match self {
            RawFormat::Hex => "hex",
            RawFormat::Dec => "dec",
            RawFormat::Json => "json",
        }
    }
}

/// Print SQL result as columnar JSON:
/// {"cols":[...],"rows":[[...],...],"truncated":bool,"limit":N}
/// One line, no pretty-print. Types preserved (null/integer/float/string).
/// `truncated` is true when the row cap `limit` cut the result short.
pub fn print_columnar_json(
    cols: &[String],
    rows: &[serde_json::Value],
    truncated: bool,
    limit: u64,
) {
    let obj = serde_json::json!({
        "cols": cols,
        "rows": rows,
        "truncated": truncated,
        "limit": limit,
    });
    println!("{}", serde_json::to_string(&obj).unwrap_or_default());
}

/// Print SQL result as JSON Lines: one object per row, keyed by column name.
/// A repeated column name (e.g. `SELECT a.id, b.id`) gets a `:N` suffix so no
/// value is silently overwritten.
pub fn print_jsonl_rows(cols: &[String], rows: &[serde_json::Value]) {
    let keys = unique_column_keys(cols);
    for row in rows {
        let obj: serde_json::Map<String, serde_json::Value> = match row {
            serde_json::Value::Array(arr) => {
                keys.iter().cloned().zip(arr.iter().cloned()).collect()
            }
            other => std::iter::once(("value".to_string(), other.clone())).collect(),
        };
        println!("{}", serde_json::to_string(&obj).unwrap_or_default());
    }
}

fn unique_column_keys(cols: &[String]) -> Vec<String> {
    let mut seen: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    cols.iter()
        .map(|c| {
            let n = seen.entry(c.as_str()).or_insert(0);
            *n += 1;
            if *n == 1 {
                c.clone()
            } else {
                format!("{c}:{n}")
            }
        })
        .collect()
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

/// Render a microsecond Unix timestamp from a JSON document for humans:
/// `2026-10-28 12:19:46 UTC`, or `-` when it is null or absent. Printing the
/// `serde_json::Value` with `{:?}` instead showed `Number(1793...)`.
pub fn fmt_ts_us(v: &serde_json::Value) -> String {
    match v.as_i64() {
        Some(us) => fmt_ts(us),
        None => "-".to_string(),
    }
}

/// [`fmt_ts_us`] for a timestamp already in hand.
pub fn fmt_ts(us: i64) -> String {
    match chrono::DateTime::from_timestamp_micros(us) {
        Some(t) => t.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        None => us.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_render_as_utc_time() {
        let v = serde_json::json!(1_793_190_000_123_456_i64);
        assert_eq!(fmt_ts_us(&v), "2026-10-28 12:20:00 UTC");
        assert_eq!(fmt_ts_us(&serde_json::Value::Null), "-");
        assert_eq!(fmt_ts_us(&serde_json::json!({})["missing"]), "-");
    }

    #[test]
    fn test_format_value_i16() {
        assert_eq!(format_value(Some(42), None), "42");
    }

    #[test]
    fn test_format_value_f32() {
        let s = format_value(None, Some(std::f64::consts::PI));
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
    fn test_format_rejects_unknown_value() {
        use clap::ValueEnum;
        assert_eq!(Format::from_str("table", false), Ok(Format::Text));
        assert_eq!(Format::from_str("jsonl", false), Ok(Format::Jsonl));
        assert!(Format::from_str("tabel", false).is_err());
    }

    #[test]
    fn test_unique_column_keys() {
        let cols = vec!["id".to_string(), "name".to_string(), "id".to_string()];
        assert_eq!(unique_column_keys(&cols), vec!["id", "name", "id:2"]);
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
