/// YAML-like signal file parser for `tag-import`.
///
/// Hand-rolled, line-oriented parser — no YAML crate dependency.
/// Expected format (the shape of temp/rtt_diagnostic.yaml):
///
/// ```yaml
/// plc:
///   server: "..."
///   rack: 0
///   slot: 2
///
/// signals:
///   # comment
///   - name: SignalName
///     address: DB5999.DBW8
///     type: i16        # optional
///   - name: AnotherSignal
///     address: DB5997.DBX10.1
/// ```
use crate::types::DataType;
use anyhow::Result;

/// A single signal parsed from the signals block.
#[derive(Debug, Clone)]
pub struct ImportSignal {
    pub name: String,
    pub address: String,
    pub data_type: Option<String>,
}

/// Parse the `signals:` block from a YAML-shaped file.
///
/// - Everything before the `signals:` line is ignored.
/// - Lines starting with `#` and blank lines are skipped.
/// - List items start with `- name:` followed by indented key:value lines.
pub fn parse_signals(content: &str) -> Result<Vec<ImportSignal>> {
    let mut signals: Vec<ImportSignal> = Vec::new();
    let mut in_signals = false;
    let mut current: Option<ImportSignal> = None;

    for line in content.lines() {
        let trimmed = line.trim();

        // Skip blank lines and comments
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        if !in_signals {
            // Wait for "signals:" at any indentation level
            if trimmed == "signals:" || trimmed.starts_with("signals:") {
                in_signals = true;
            }
            continue;
        }

        // Within signals block
        if let Some(rest) = trimmed.strip_prefix("- name:") {
            // Save previous signal
            if let Some(prev) = current.take() {
                signals.push(prev);
            }
            let name = rest.trim().to_string();
            current = Some(ImportSignal {
                name,
                address: String::new(),
                data_type: None,
            });
        } else if let Some(ref mut sig) = current {
            if let Some(addr) = trimmed.strip_prefix("address:") {
                sig.address = addr.trim().to_string();
            } else if let Some(dt) = trimmed.strip_prefix("type:") {
                sig.data_type = Some(dt.trim().to_string());
            }
            // Other keys inside a signal entry are ignored
        }
        // Lines outside a list item within signals (e.g., other top-level
        // keys at column 0) are silently ignored.
    }

    // Don't forget the last signal
    if let Some(sig) = current {
        signals.push(sig);
    }

    if signals.is_empty() {
        anyhow::bail!("no signals found in file (no 'signals:' block or empty)");
    }

    // Validate required fields
    for sig in &signals {
        if sig.name.is_empty() {
            anyhow::bail!("signal with empty name");
        }
        if sig.address.is_empty() {
            anyhow::bail!("signal '{}' has no address", sig.name);
        }
    }

    Ok(signals)
}

/// Infer the data type from an address string when no explicit type is given.
///
/// Rules:
/// - `.DBX` → `Bool`
/// - `.DBW` → `I16`
/// - `.DBD` → `U32`
/// - Anything else (e.g. `.DBB`) → error with a clear message.
pub fn infer_type(address: &str) -> Result<DataType> {
    if address.contains(".DBX") {
        Ok(DataType::Bool)
    } else if address.contains(".DBW") {
        Ok(DataType::I16)
    } else if address.contains(".DBD") {
        Ok(DataType::U32)
    } else if address.contains(".DBB") {
        anyhow::bail!(
            "cannot infer type for address '{}': DBB format requires explicit --type",
            address
        );
    } else {
        anyhow::bail!(
            "cannot infer type from address '{}': unrecognised address format",
            address
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_signals_basic() {
        let yaml = r#"
plc:
  server: "10.0.0.1:102"
  rack: 0
  slot: 2

signals:
  # Runtime settings
  - name: Signal1
    address: DB5999.DBW8
    type: i16
  - name: Signal2
    address: DB5997.DBX10.1
"#;
        let signals = parse_signals(yaml).unwrap();
        assert_eq!(signals.len(), 2);

        assert_eq!(signals[0].name, "Signal1");
        assert_eq!(signals[0].address, "DB5999.DBW8");
        assert_eq!(signals[0].data_type.as_deref(), Some("i16"));

        assert_eq!(signals[1].name, "Signal2");
        assert_eq!(signals[1].address, "DB5997.DBX10.1");
        assert_eq!(signals[1].data_type, None);
    }

    #[test]
    fn test_parse_signals_with_dbd_and_no_type() {
        let yaml = r#"
signals:
  - name: DWordSignal
    address: DB1.DBD240
"#;
        let signals = parse_signals(yaml).unwrap();
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].name, "DWordSignal");
        assert_eq!(signals[0].address, "DB1.DBD240");
        assert_eq!(signals[0].data_type, None);
    }

    #[test]
    fn test_parse_signals_empty() {
        let yaml = "some_key: value";
        let result = parse_signals(yaml);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no signals found"));
    }

    #[test]
    fn test_infer_type_dbx() {
        assert_eq!(infer_type("DB1.DBX10.1").unwrap(), DataType::Bool);
        assert_eq!(infer_type("DB5997.DBX10.0").unwrap(), DataType::Bool);
    }

    #[test]
    fn test_infer_type_dbw() {
        assert_eq!(infer_type("DB5999.DBW8").unwrap(), DataType::I16);
        assert_eq!(infer_type("DB1.DBW0").unwrap(), DataType::I16);
    }

    #[test]
    fn test_infer_type_dbd() {
        assert_eq!(infer_type("DB1.DBD240").unwrap(), DataType::U32);
    }

    #[test]
    fn test_infer_type_dbb_error() {
        let result = infer_type("DB1.DBB100");
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("DBB"));
        assert!(msg.contains("requires explicit"));
    }

    #[test]
    fn test_infer_type_unknown() {
        let result = infer_type("DB1.XXX100");
        assert!(result.is_err());
    }
}
