//! Paged sample reads for `sample-export`.
//!
//! `sql` caps every answer at 10000 rows, so a day of samples could not come
//! out in one query. An export reads one tag at a time
//! in pages keyed on `(ts, rowid)`: each page is a range on `ix_sample_tag_ts`,
//! whose entries end in the rowid, so no page sorts and none repeats or skips a
//! row, however many share a timestamp.

use rusqlite::{params, Connection, OptionalExtension};

/// Largest page a caller may ask for.
pub const MAX_PAGE_ROWS: i64 = 10_000;

/// One exported sample.
#[derive(Debug, Clone, PartialEq)]
pub struct ExportRow {
    pub rowid: i64,
    pub ts: i64,
    pub vi: Option<i64>,
    pub vf: Option<f64>,
    pub dataset_id: i64,
    pub cycle_id: Option<i64>,
    pub span_id: Option<i64>,
}

/// A tag as an export names it.
#[derive(Debug, Clone, PartialEq)]
pub struct ExportTag {
    pub id: i64,
    pub name: String,
    pub plc: String,
}

/// Why a tag could not be resolved.
#[derive(Debug, PartialEq)]
pub enum TagLookup {
    NotFound,
    /// The name exists on several PLCs; these are their names.
    Ambiguous(Vec<String>),
}

/// Resolve a tag given by id or name, optionally restricted to one PLC.
pub fn resolve_tag(
    conn: &Connection,
    tag: &str,
    plc: Option<&str>,
) -> rusqlite::Result<Result<ExportTag, TagLookup>> {
    let mut stmt = conn.prepare_cached(
        "SELECT t.id, t.name, p.name FROM tag t JOIN plc p ON p.id = t.plc_id
         WHERE (t.name = ?1 OR CAST(t.id AS TEXT) = ?1) AND (?2 IS NULL OR p.name = ?2)
         ORDER BY t.name = ?1 DESC, t.id",
    )?;
    let found: Vec<ExportTag> = stmt
        .query_map(params![tag, plc], |r| {
            Ok(ExportTag {
                id: r.get(0)?,
                name: r.get(1)?,
                plc: r.get(2)?,
            })
        })?
        .collect::<rusqlite::Result<_>>()?;
    // A name match wins over an id match: a tag literally named "12" is meant
    // by "12" before the tag whose id is 12.
    let by_name: Vec<&ExportTag> = found.iter().filter(|t| t.name == tag).collect();
    Ok(match (by_name.len(), found.first()) {
        (1, _) => Ok(by_name[0].clone()),
        (0, Some(t)) => Ok(t.clone()),
        (0, None) => Err(TagLookup::NotFound),
        _ => Err(TagLookup::Ambiguous(
            by_name.iter().map(|t| t.plc.clone()).collect(),
        )),
    })
}

/// Name and PLC of a tag id, for an export that was given ids.
pub fn tag_by_id(conn: &Connection, id: i64) -> rusqlite::Result<Option<ExportTag>> {
    conn.query_row(
        "SELECT t.id, t.name, p.name FROM tag t JOIN plc p ON p.id = t.plc_id WHERE t.id = ?1",
        [id],
        |r| {
            Ok(ExportTag {
                id: r.get(0)?,
                name: r.get(1)?,
                plc: r.get(2)?,
            })
        },
    )
    .optional()
}

/// Up to `limit` samples of `tag_id` with `since <= ts < until`, after the
/// cursor `(ts, rowid)` when one is given, in `(ts, rowid)` order. The second
/// value says whether more rows follow.
pub fn page(
    conn: &Connection,
    tag_id: i64,
    since: i64,
    until: i64,
    after: Option<(i64, i64)>,
    limit: i64,
) -> rusqlite::Result<(Vec<ExportRow>, bool)> {
    let limit = limit.clamp(1, MAX_PAGE_ROWS);
    let (from_ts, after_rowid) = match after {
        // Rows at the cursor's own timestamp are filtered by rowid; the index
        // range still starts at that timestamp.
        Some((ts, rowid)) => (ts.max(since), if ts >= since { rowid } else { i64::MIN }),
        None => (since, i64::MIN),
    };
    let mut stmt = conn.prepare_cached(
        "SELECT rowid, ts, vi, vf, dataset_id, cycle_id, span_id FROM sample
         WHERE tag_id = ?1 AND ts >= ?2 AND ts < ?3 AND (ts > ?2 OR rowid > ?4)
         ORDER BY ts, rowid LIMIT ?5",
    )?;
    let mut rows: Vec<ExportRow> = stmt
        .query_map(
            params![tag_id, from_ts, until, after_rowid, limit + 1],
            |r| {
                Ok(ExportRow {
                    rowid: r.get(0)?,
                    ts: r.get(1)?,
                    vi: r.get(2)?,
                    vf: r.get(3)?,
                    dataset_id: r.get(4)?,
                    cycle_id: r.get(5)?,
                    span_id: r.get(6)?,
                })
            },
        )?
        .collect::<rusqlite::Result<_>>()?;
    let more = rows.len() as i64 > limit;
    rows.truncate(limit as usize);
    Ok((rows, more))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> Connection {
        let conn = crate::db::schema::open_memory().unwrap();
        conn.execute_batch(
            "INSERT INTO plc (id, name, host) VALUES (1, 'a', 'h'), (2, 'b', 'h');
             INSERT INTO poll_group (id, name, period_ms) VALUES (1, 'g', 100);
             INSERT INTO dataset (id, plc_id, name, state, created_ts, updated_ts)
             VALUES (1, 1, 'd', 'active', 0, 0);
             INSERT INTO tag (id, plc_id, name, address, db_num, byte_offset, byte_size,
                              data_type, poll_group_id, created_ts, updated_ts)
             VALUES (1, 1, 'speed', 'DB1.DBW0', 1, 0, 2, 'i16', 1, 0, 0),
                    (2, 1, 'lpn', 'DB1.DBW2', 1, 2, 2, 'i16', 1, 0, 0),
                    (3, 2, 'lpn', 'DB1.DBW2', 1, 2, 2, 'i16', 1, 0, 0),
                    (4, 1, '1', 'DB1.DBW4', 1, 4, 2, 'i16', 1, 0, 0);",
        )
        .unwrap();
        conn
    }

    #[test]
    fn tags_resolve_by_name_id_and_plc() {
        let conn = fixture();
        let name = |t: &str, p: Option<&str>| resolve_tag(&conn, t, p).unwrap().map(|t| t.id);
        assert_eq!(name("speed", None), Ok(1));
        assert_eq!(name("2", None), Ok(2), "by id");
        assert_eq!(name("1", None), Ok(4), "a tag named '1' wins over id 1");
        assert_eq!(
            name("lpn", None),
            Err(TagLookup::Ambiguous(vec!["a".into(), "b".into()]))
        );
        assert_eq!(name("lpn", Some("b")), Ok(3));
        assert_eq!(name("nope", None), Err(TagLookup::NotFound));
        assert_eq!(name("speed", Some("b")), Err(TagLookup::NotFound));
    }

    #[test]
    fn pages_cover_the_range_exactly_once() {
        let conn = fixture();
        // 25 samples, several sharing a timestamp, plus one outside each end.
        for i in 0..25 {
            conn.execute(
                "INSERT INTO sample (dataset_id, tag_id, ts, vi) VALUES (1, 1, ?1, ?2)",
                params![100 + i / 3, i],
            )
            .unwrap();
        }
        conn.execute_batch(
            "INSERT INTO sample (dataset_id, tag_id, ts, vi) VALUES (1, 1, 99, -1), (1, 1, 200, -2);
             INSERT INTO sample (dataset_id, tag_id, ts, vi) VALUES (1, 2, 105, -3);",
        )
        .unwrap();

        let mut seen = Vec::new();
        let mut after = None;
        let mut pages = 0;
        loop {
            let (rows, more) = page(&conn, 1, 100, 200, after, 4).unwrap();
            pages += 1;
            assert!(rows.len() <= 4);
            seen.extend(rows.iter().map(|r| r.vi.unwrap()));
            after = rows.last().map(|r| (r.ts, r.rowid));
            if !more {
                break;
            }
        }
        assert_eq!(seen, (0..25).collect::<Vec<_>>());
        assert_eq!(pages, 7);
    }

    #[test]
    fn an_empty_range_is_one_empty_page() {
        let conn = fixture();
        assert_eq!(page(&conn, 1, 0, 10, None, 100).unwrap(), (vec![], false));
    }
}
