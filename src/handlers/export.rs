//! `sample-export` command handler: stream samples of one or more tags over a
//! time range, past the `sql` row cap.
//!
//! Each tag is read in pages (see [`crate::db::export`]) and the tags are
//! merged by timestamp, so memory stays at one page per tag however long the
//! range. Rows go to stdout or `--out` as they arrive.

use std::collections::VecDeque;
use std::io::Write;

use anyhow::{Context, Result};
use rusqlite::Connection;

use crate::cli;
use crate::cli::args::Cli;
use crate::cli::format::{fmt_ts, ExportFormat};
use crate::cli::output::{fail, CliError};
use crate::cli::remote::{urlencode, RemoteError};
use crate::db::export::{self, ExportRow, ExportTag, TagLookup};
use crate::server_base;
use crate::types::*;

/// What the merge reads pages from: the local database or a server.
trait PageSource {
    /// The first page names the tag (`tag`, `plc`); later ones pass its id.
    fn page(
        &mut self,
        tag: TagRef<'_>,
        since: i64,
        until: i64,
        after: Option<(i64, i64)>,
        limit: i64,
    ) -> Result<(ExportTag, Vec<ExportRow>, bool)>;
}

#[derive(Clone, Copy)]
enum TagRef<'a> {
    Named(&'a str, Option<&'a str>),
    Id(i64),
}

struct Local<'c>(&'c Connection);

impl PageSource for Local<'_> {
    fn page(
        &mut self,
        tag: TagRef<'_>,
        since: i64,
        until: i64,
        after: Option<(i64, i64)>,
        limit: i64,
    ) -> Result<(ExportTag, Vec<ExportRow>, bool)> {
        let tag = match tag {
            TagRef::Named(name, plc) => match export::resolve_tag(self.0, name, plc)? {
                Ok(t) => t,
                Err(TagLookup::NotFound) => {
                    return Err(fail(ErrorCode::NotFound, format!("tag '{name}' not found")))
                }
                Err(TagLookup::Ambiguous(plcs)) => {
                    return Err(fail(
                        ErrorCode::InvalidArgument,
                        format!(
                            "tag '{name}' exists on PLCs {}; pass --plc",
                            plcs.join(", ")
                        ),
                    ))
                }
            },
            TagRef::Id(id) => export::tag_by_id(self.0, id)?
                .ok_or_else(|| fail(ErrorCode::NotFound, format!("tag id {id} not found")))?,
        };
        let (rows, more) = export::page(self.0, tag.id, since, until, after, limit)?;
        Ok((tag, rows, more))
    }
}

struct Remote {
    base: String,
    timeout: std::time::Duration,
}

impl PageSource for Remote {
    fn page(
        &mut self,
        tag: TagRef<'_>,
        since: i64,
        until: i64,
        after: Option<(i64, i64)>,
        limit: i64,
    ) -> Result<(ExportTag, Vec<ExportRow>, bool)> {
        let mut url = format!("/v1/export?since_ts={since}&until_ts={until}&limit={limit}");
        match tag {
            TagRef::Named(name, plc) => {
                url.push_str(&format!("&tag={}", urlencode(name)));
                if let Some(p) = plc {
                    url.push_str(&format!("&plc={}", urlencode(p)));
                }
            }
            TagRef::Id(id) => url.push_str(&format!("&tag_id={id}")),
        }
        if let Some((ts, rowid)) = after {
            url.push_str(&format!("&after_ts={ts}&after_rowid={rowid}"));
        }
        let resp = cli::remote::http_get(&self.base, &url, self.timeout)?;
        let v: serde_json::Value = serde_json::from_str(&resp)
            .map_err(|e| RemoteError::Protocol(format!("invalid JSON from /v1/export: {e}")))?;
        let tag = ExportTag {
            id: v["tag_id"]
                .as_i64()
                .ok_or_else(|| RemoteError::Protocol("/v1/export: no tag_id".into()))?,
            name: v["tag"].as_str().unwrap_or("?").to_string(),
            plc: v["plc"].as_str().unwrap_or("?").to_string(),
        };
        let rows = v["rows"]
            .as_array()
            .map(|a| a.as_slice())
            .unwrap_or_default()
            .iter()
            .map(|r| {
                Some(ExportRow {
                    rowid: r[0].as_i64()?,
                    ts: r[1].as_i64()?,
                    vi: r[2].as_i64(),
                    vf: r[3].as_f64(),
                    dataset_id: r[4].as_i64()?,
                    cycle_id: r[5].as_i64(),
                    span_id: r[6].as_i64(),
                })
            })
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| RemoteError::Protocol("/v1/export: malformed row".into()))?;
        Ok((tag, rows, v["more"].as_bool().unwrap_or(false)))
    }
}

/// One tag's position in the merge.
struct Cursor<'a> {
    requested: &'a str,
    tag: Option<ExportTag>,
    buf: VecDeque<ExportRow>,
    after: Option<(i64, i64)>,
    more: bool,
    rows: u64,
}

impl<'a> Cursor<'a> {
    fn new(requested: &'a str) -> Self {
        Cursor {
            requested,
            tag: None,
            buf: VecDeque::new(),
            after: None,
            more: true,
            rows: 0,
        }
    }
}

/// Merge the tags' pages by timestamp, calling `emit` for each row in order;
/// ties go to the tag listed first. Each cursor is refilled only when drained,
/// so at most one page per tag is held.
fn merge(
    source: &mut dyn PageSource,
    cursors: &mut [Cursor],
    plc: Option<&str>,
    (since, until): (i64, i64),
    page_rows: i64,
    mut emit: impl FnMut(&ExportTag, &ExportRow) -> Result<()>,
) -> Result<()> {
    loop {
        for c in cursors.iter_mut() {
            if c.buf.is_empty() && c.more {
                let tag_ref = match &c.tag {
                    Some(t) => TagRef::Id(t.id),
                    None => TagRef::Named(c.requested, plc),
                };
                let (tag, rows, more) = source.page(tag_ref, since, until, c.after, page_rows)?;
                c.after = rows.last().map(|r| (r.ts, r.rowid)).or(c.after);
                c.more = more && !rows.is_empty();
                c.buf.extend(rows);
                c.tag = Some(tag);
            }
        }
        let next = cursors
            .iter()
            .enumerate()
            .filter_map(|(i, c)| c.buf.front().map(|r| (r.ts, i)))
            .min();
        let Some((_, i)) = next else {
            return Ok(());
        };
        let c = &mut cursors[i];
        let row = c.buf.pop_front().expect("the head was just seen");
        c.rows += 1;
        emit(c.tag.as_ref().expect("filled with the page"), &row)?;
    }
}

/// Parse a `--since`/`--until` value: Unix microseconds, RFC 3339, or
/// `YYYY-MM-DD[ HH:MM[:SS]]` (also with `T`) taken as UTC.
pub(crate) fn parse_time(s: &str) -> Option<i64> {
    let s = s.trim();
    if let Ok(us) = s.parse::<i64>() {
        return Some(us);
    }
    if let Ok(t) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(t.timestamp_micros());
    }
    let s = s.trim_end_matches('Z').replace('T', " ");
    for f in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d %H:%M"] {
        if let Ok(t) = chrono::NaiveDateTime::parse_from_str(&s, f) {
            return Some(t.and_utc().timestamp_micros());
        }
    }
    chrono::NaiveDate::parse_from_str(&s, "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|t| t.and_utc().timestamp_micros())
}

fn time_arg(flag: &str, s: &str) -> Result<i64> {
    parse_time(s).ok_or_else(|| {
        fail(
            ErrorCode::InvalidArgument,
            format!(
                "{flag} '{s}': expected RFC 3339, 'YYYY-MM-DD[ HH:MM[:SS]]' (UTC) or Unix microseconds"
            ),
        )
    })
}

fn csv_field(s: &str) -> String {
    if s.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

fn opt<T: ToString>(v: Option<T>) -> String {
    v.map(|x| x.to_string()).unwrap_or_default()
}

fn write_row(out: &mut dyn Write, fmt: ExportFormat, tag: &ExportTag, r: &ExportRow) -> Result<()> {
    let time = chrono::DateTime::from_timestamp_micros(r.ts)
        .map(|t| t.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string())
        .unwrap_or_default();
    match fmt {
        ExportFormat::Jsonl => {
            let v = serde_json::json!({
                "ts": r.ts,
                "time": time,
                "plc": tag.plc,
                "tag": tag.name,
                "tag_id": tag.id,
                "vi": r.vi,
                "vf": r.vf,
                "dataset_id": r.dataset_id,
                "cycle_id": r.cycle_id,
                "span_id": r.span_id,
            });
            writeln!(out, "{v}")?;
        }
        ExportFormat::Csv => writeln!(
            out,
            "{},{},{},{},{},{},{},{},{},{}",
            r.ts,
            time,
            csv_field(&tag.plc),
            csv_field(&tag.name),
            tag.id,
            opt(r.vi),
            opt(r.vf),
            r.dataset_id,
            opt(r.cycle_id),
            opt(r.span_id),
        )?,
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn handle_sample_export(
    cli: &Cli,
    conn: &Connection,
    tags: &[String],
    plc: Option<&str>,
    since: &str,
    until: &str,
    fmt: ExportFormat,
    out_path: Option<&std::path::Path>,
    page_rows: i64,
) -> Result<ExitCode> {
    let since = time_arg("--since", since)?;
    let until = time_arg("--until", until)?;
    if until <= since {
        return Err(fail(
            ErrorCode::InvalidArgument,
            "--until must be after --since",
        ));
    }
    if cli.json && out_path.is_none() {
        return Err(fail(
            ErrorCode::InvalidArgument,
            "sample-export --json needs --out FILE: stdout carries the one-line envelope, the rows go to the file",
        ));
    }
    let page_rows = page_rows.clamp(1, export::MAX_PAGE_ROWS);

    let mut source: Box<dyn PageSource> = match server_base(cli) {
        Some(base) => Box::new(Remote {
            base,
            timeout: std::time::Duration::from_millis(cli.remote_timeout_ms),
        }),
        None => Box::new(Local(conn)),
    };
    let mut out: Box<dyn Write> = match out_path {
        Some(p) => Box::new(std::io::BufWriter::new(
            std::fs::File::create(p).with_context(|| format!("creating {}", p.display()))?,
        )),
        None => Box::new(std::io::BufWriter::new(std::io::stdout().lock())),
    };
    if fmt == ExportFormat::Csv {
        writeln!(
            out,
            "ts,time,plc,tag,tag_id,vi,vf,dataset_id,cycle_id,span_id"
        )?;
    }

    let mut cursors: Vec<Cursor> = tags.iter().map(|t| Cursor::new(t)).collect();
    let started = std::time::Instant::now();
    let mut total: u64 = 0;
    let mut last_ts: Option<i64> = None;

    let result = merge(
        source.as_mut(),
        &mut cursors,
        plc,
        (since, until),
        page_rows,
        |tag, row| {
            write_row(&mut out, fmt, tag, row)?;
            total += 1;
            last_ts = Some(row.ts);
            Ok(())
        },
    );
    out.flush()?;
    drop(out);

    if let Err(e) = result {
        if total == 0 {
            return Err(e);
        }
        // Keep the classification of the underlying failure, but say how far
        // the export got: everything up to last_ts is in the output.
        let code = cli::output::classify(&e).code;
        let mut err = CliError::new(
            code,
            format!(
                "export interrupted after {total} row(s){}: {e}",
                last_ts
                    .map(|ts| format!(", last ts {ts} ({})", fmt_ts(ts)))
                    .unwrap_or_default()
            ),
        );
        err = err.with_details(serde_json::json!({ "rows": total, "last_ts": last_ts }));
        return Err(err.into());
    }

    let per_tag: Vec<serde_json::Value> = cursors
        .iter()
        .filter_map(|c| {
            c.tag.as_ref().map(|t| {
                serde_json::json!({ "plc": t.plc, "tag": t.name, "tag_id": t.id, "rows": c.rows })
            })
        })
        .collect();
    let elapsed = started.elapsed().as_secs_f64();
    if cli.json {
        cli::output::print_ok(
            &serde_json::json!({
                "rows": total,
                "tags": per_tag,
                "since_ts": since,
                "until_ts": until,
                "out": out_path.map(|p| p.display().to_string()),
            }),
            None,
        );
    } else {
        eprintln!(
            "exported {total} row(s) of {} tag(s), {} .. {}, in {elapsed:.1} s",
            per_tag.len(),
            fmt_ts(since),
            fmt_ts(until)
        );
    }
    Ok(ExitCode::Ok)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn times_parse_in_the_documented_forms() {
        let day = 1_792_972_800_000_000; // 2026-10-26 00:00:00 UTC
        assert_eq!(parse_time("2026-10-26"), Some(day));
        assert_eq!(parse_time("2026-10-26 01:00"), Some(day + 3_600_000_000));
        assert_eq!(
            parse_time("2026-10-26T01:00:00Z"),
            Some(day + 3_600_000_000)
        );
        assert_eq!(
            parse_time("2026-10-26T04:00:00+03:00"),
            Some(day + 3_600_000_000)
        );
        assert_eq!(parse_time("2026-10-26 00:00:00.5"), Some(day + 500_000));
        assert_eq!(parse_time("1792972800000000"), Some(day));
        assert_eq!(parse_time("yesterday"), None);
    }

    #[test]
    fn csv_quotes_only_when_needed() {
        assert_eq!(csv_field("a.b"), "a.b");
        assert_eq!(csv_field("a,b"), "\"a,b\"");
        assert_eq!(csv_field("say \"x\""), "\"say \"\"x\"\"\"");
    }

    #[test]
    fn tags_merge_by_time_across_pages() {
        let conn = crate::db::schema::open_memory().unwrap();
        conn.execute_batch(
            "INSERT INTO plc (id, name, host) VALUES (1, 'p', 'h');
             INSERT INTO poll_group (id, name, period_ms) VALUES (1, 'g', 100);
             INSERT INTO dataset (id, plc_id, name, state, created_ts, updated_ts)
             VALUES (1, 1, 'd', 'active', 0, 0);
             INSERT INTO tag (id, plc_id, name, address, db_num, byte_offset, byte_size,
                              data_type, poll_group_id, created_ts, updated_ts)
             VALUES (1, 1, 'a', 'DB1.DBW0', 1, 0, 2, 'i16', 1, 0, 0),
                    (2, 1, 'b', 'DB1.DBW2', 1, 2, 2, 'i16', 1, 0, 0);",
        )
        .unwrap();
        for ts in [10, 30, 50, 70] {
            conn.execute(
                "INSERT INTO sample (dataset_id, tag_id, ts, vi) VALUES (1, 1, ?1, ?1)",
                [ts],
            )
            .unwrap();
        }
        for ts in [20, 30, 40] {
            conn.execute(
                "INSERT INTO sample (dataset_id, tag_id, ts, vi) VALUES (1, 2, ?1, ?1)",
                [ts],
            )
            .unwrap();
        }
        let mut cursors: Vec<Cursor> = ["a", "b"].into_iter().map(Cursor::new).collect();
        let mut got = Vec::new();
        // One-row pages, so every row is a page boundary.
        merge(
            &mut Local(&conn),
            &mut cursors,
            None,
            (0, 60),
            1,
            |tag, row| {
                got.push((tag.name.clone(), row.ts));
                Ok(())
            },
        )
        .unwrap();
        let want: Vec<(String, i64)> = [
            ("a", 10),
            ("b", 20),
            ("a", 30),
            ("b", 30),
            ("b", 40),
            ("a", 50),
        ]
        .iter()
        .map(|(t, ts)| (t.to_string(), *ts))
        .collect();
        assert_eq!(got, want, "until is exclusive: 70 is out");
        assert_eq!((cursors[0].rows, cursors[1].rows), (3, 3));
    }
}
