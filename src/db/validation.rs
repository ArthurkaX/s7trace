/// Tag validation: read one sample from the PLC and store the result.
///
/// Used both by the CLI (sync `tag add`) and the server command worker
/// (control action `tag_add`). The read attempt is mandatory so that bad
/// addresses are flagged immediately per `docs/architecture.md`.
///
/// Behaviour:
/// - On read OK: writes `last_value_i`, `last_value_f`, `last_ts`,
///   clears `bad=0`, `bad_class=NULL`, `bad_detail=NULL`, sets
///   `last_ok_ts` on the PLC row.
/// - On read error: writes `bad=1`, `bad_class`, `bad_detail`,
///   `last_error_ts`, sets `last_error` on the PLC row.
///
/// Returns `(ok, value_i, value_f)`.
use crate::plc::client::PlcClient;
use crate::types::*;
use anyhow::Result;
use parking_lot::Mutex;
use rusqlite::Connection;
use std::sync::Arc;

pub fn validate_tag(
    conn: &Connection,
    client_arc: &Arc<Mutex<dyn PlcClient + Send>>,
    tag_id: i64,
) -> Result<ValidationResult> {
    // Load tag — includes bit_offset for Bool tags
    let (plc_id, db_num, byte_offset, bit_offset, byte_size, data_type_str): (
        i64,
        i64,
        i64,
        Option<i64>,
        i64,
        String,
    ) = conn.query_row(
        "SELECT plc_id, db_num, byte_offset, bit_offset, byte_size, data_type
             FROM tag WHERE id = ?1",
        rusqlite::params![tag_id],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, String>(5)?,
            ))
        },
    )?;

    let dt: DataType = data_type_str
        .parse()
        .map_err(|e: String| anyhow::anyhow!("invalid data_type '{data_type_str}': {e}"))?;
    let bit_off = bit_offset.map(|b| b as u8);

    let now = now_us();
    let mut client = client_arc.lock();
    let read_result = client.read_db(db_num as u16, byte_offset as usize, byte_size as usize);

    match read_result {
        Ok(data) => {
            drop(client);
            let (vi, vf) = crate::codec::decode_value(&data, dt, bit_off);
            conn.execute(
                "UPDATE tag
                 SET bad = 0,
                     bad_class = NULL,
                     bad_detail = NULL,
                     last_value_i = ?1,
                     last_value_f = ?2,
                     last_ts = ?3
                 WHERE id = ?4",
                rusqlite::params![vi, vf, now, tag_id],
            )?;
            conn.execute(
                "UPDATE plc SET last_ok_ts = ?1, last_error = NULL WHERE id = ?2",
                rusqlite::params![now, plc_id],
            )?;
            Ok(ValidationResult {
                ok: true,
                bad_class: None,
                detail: None,
                value_i: vi,
                value_f: vf,
                ts_us: now,
            })
        }
        Err(e) => {
            drop(client);
            let bad_class = e.bad_class();
            let detail = e.detail().to_string();
            conn.execute(
                "UPDATE tag
                 SET bad = 1,
                     bad_class = ?1,
                     bad_detail = ?2,
                     last_error_ts = ?3
                 WHERE id = ?4",
                rusqlite::params![bad_class.to_string(), detail, now, tag_id],
            )?;
            conn.execute(
                "UPDATE plc SET last_error = ?1 WHERE id = ?2",
                rusqlite::params![detail, plc_id],
            )?;
            Ok(ValidationResult {
                ok: false,
                bad_class: Some(bad_class),
                detail: Some(detail),
                value_i: None,
                value_f: None,
                ts_us: now,
            })
        }
    }
}

/// Result of a tag validation attempt.
#[derive(Debug, Clone)]
pub struct ValidationResult {
    pub ok: bool,
    pub bad_class: Option<BadClass>,
    pub detail: Option<String>,
    pub value_i: Option<i64>,
    pub value_f: Option<f64>,
    pub ts_us: TimestampUs,
}

/// Insert a tag (and its metric_set membership) in the database,
/// then validate it via `client_arc`. Optionally schedule a config apply.
///
/// Used both by CLI sync mode (using a freshly created client) and by
/// the server command worker (using the in-process PLC registry).
pub struct InsertedTag {
    pub tag_id: i64,
    pub plc_id: i64,
    pub poll_group_id: i64,
    pub metric_set_id: i64,
}

pub fn insert_tag(
    conn: &Connection,
    plc_name: &str,
    name: &str,
    address: &S7Address,
    data_type: DataType,
    poll_group: &str,
    store_mode: &str,
    storage_hook: &str,
    value_role: &str,
    metric_set: Option<&str>,
) -> Result<InsertedTag> {
    let plc_id: i64 = conn
        .query_row(
            "SELECT id FROM plc WHERE name = ?1",
            rusqlite::params![plc_name],
            |row| row.get(0),
        )
        .map_err(|_| anyhow::anyhow!("PLC '{plc_name}' not found. Create it first."))?;

    let poll_group_id: i64 = match conn.query_row(
        "SELECT id FROM poll_group WHERE name = ?1",
        rusqlite::params![poll_group],
        |row| row.get(0),
    ) {
        Ok(id) => id,
        Err(_) => {
            let period = match poll_group {
                "fast" => 40,
                "medium" => 250,
                _ => 1000,
            };
            conn.execute(
                "INSERT INTO poll_group (name, period_ms) VALUES (?1, ?2)",
                rusqlite::params![poll_group, period],
            )?;
            conn.last_insert_rowid()
        }
    };

    let now = now_us();
    let byte_size = data_type.byte_size() as i64;
    let dt_str = data_type.to_string();
    let address_str = if let Some(bit) = address.bit_offset {
        format!("DB{}.DBX{}.{}", address.db_num, address.byte_offset, bit)
    } else {
        format!(
            "DB{}.DB{}{}",
            address.db_num,
            byte_size_label(data_type),
            address.byte_offset
        )
    };

    conn.execute(
        "INSERT INTO tag
            (plc_id, name, address, db_num, byte_offset, bit_offset, byte_size, data_type,
             value_role, poll_group_id, store_mode, storage_hook, enabled, created_ts, updated_ts)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, 1, ?13, ?13)",
        rusqlite::params![
            plc_id,
            name,
            address_str,
            address.db_num,
            address.byte_offset,
            address.bit_offset.map(|b| b as i64),
            byte_size,
            dt_str,
            value_role,
            poll_group_id,
            store_mode,
            storage_hook,
            now,
        ],
    )?;
    let tag_id = conn.last_insert_rowid();

    let set_name = metric_set.unwrap_or("default");
    let metric_set_id: i64 = match conn.query_row(
        "SELECT id FROM metric_set WHERE plc_id = ?1 AND name = ?2",
        rusqlite::params![plc_id, set_name],
        |row| row.get(0),
    ) {
        Ok(id) => id,
        Err(_) => {
            conn.execute(
                "INSERT INTO metric_set (plc_id, name, enabled, created_ts, updated_ts)
                 VALUES (?1, ?2, 1, ?3, ?3)",
                rusqlite::params![plc_id, set_name, now],
            )?;
            conn.last_insert_rowid()
        }
    };

    conn.execute(
        "INSERT OR IGNORE INTO metric_set_tag (metric_set_id, tag_id, plc_id, created_ts)
         VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![metric_set_id, tag_id, plc_id, now],
    )?;

    Ok(InsertedTag {
        tag_id,
        plc_id,
        poll_group_id,
        metric_set_id,
    })
}

fn byte_size_label(dt: DataType) -> &'static str {
    match dt {
        DataType::Bool => "X",
        DataType::I16 | DataType::U16 => "W",
        DataType::I32 | DataType::U32 | DataType::F32 => "D",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plc::snap7::MockPlcClient;

    fn insert_tag(conn: &Connection, name: &str, dt: &str, db: i64, off: i64) -> i64 {
        conn.execute(
            "INSERT INTO plc (id, name, host) VALUES (1, 'test-plc', '127.0.0.1')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO poll_group (id, name, period_ms) VALUES (1, 'fast', 40)",
            [],
        )
        .unwrap();
        let byte_size: i64 = match dt {
            "bool" => 1,
            "i16" | "u16" => 2,
            _ => 4,
        };
        conn.execute(
            "INSERT INTO tag
                (plc_id, name, address, db_num, byte_offset, byte_size, data_type,
                 poll_group_id, store_mode, storage_hook, enabled, created_ts, updated_ts)
             VALUES (1, ?1, 'DB1.DBW0', ?2, ?3, ?4, ?5, 1, 'on_change', 'on_change', 1, 0, 0)",
            rusqlite::params![name, db, off, byte_size, dt],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    #[test]
    fn test_validate_tag_ok() {
        let conn = crate::db::schema::open_memory().unwrap();
        let tag_id = insert_tag(&conn, "T1", "i16", 1, 0);

        let mut mock = MockPlcClient::new("test");
        mock.connect().unwrap();
        mock.seed(1, 0, vec![0x00, 0x2A]);
        let client: Arc<Mutex<dyn PlcClient + Send>> = Arc::new(Mutex::new(mock));

        let res = validate_tag(&conn, &client, tag_id).unwrap();
        assert!(res.ok);
        assert_eq!(res.value_i, Some(42));
        assert!(res.value_f.is_none());

        // Tag now marked good
        let bad: i64 = conn
            .query_row(
                "SELECT bad FROM tag WHERE id = ?1",
                rusqlite::params![tag_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(bad, 0);
        // PLC last_ok_ts updated
        let ts: i64 = conn
            .query_row("SELECT last_ok_ts FROM plc WHERE id = 1", [], |r| r.get(0))
            .unwrap();
        assert!(ts > 0);
    }

    #[test]
    fn test_validate_tag_bad() {
        let conn = crate::db::schema::open_memory().unwrap();
        let tag_id = insert_tag(&conn, "T2", "i16", 1, 0);

        let mut mock = MockPlcClient::new("test");
        mock.connect().unwrap();
        mock.fail_reads_with(PlcError::AddressOutOfRange {
            address: "DB1@0".into(),
            detail: "out of range".into(),
        });
        let client: Arc<Mutex<dyn PlcClient + Send>> = Arc::new(Mutex::new(mock));

        let res = validate_tag(&conn, &client, tag_id).unwrap();
        assert!(!res.ok);
        assert_eq!(res.bad_class, Some(BadClass::AddressOutOfRange));
        assert_eq!(res.detail.as_deref(), Some("out of range"));

        // Tag marked bad with class+detail
        let (bad, bc, bd): (i64, Option<String>, Option<String>) = conn
            .query_row(
                "SELECT bad, bad_class, bad_detail FROM tag WHERE id = ?1",
                rusqlite::params![tag_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(bad, 1);
        assert_eq!(bc.as_deref(), Some("address_out_of_range"));
        assert_eq!(bd.as_deref(), Some("out of range"));

        // PLC last_error populated
        let err: String = conn
            .query_row("SELECT last_error FROM plc WHERE id = 1", [], |r| r.get(0))
            .unwrap();
        assert!(!err.is_empty());
    }
}
