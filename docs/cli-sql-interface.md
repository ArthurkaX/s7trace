# CLI and SQL Interface

## Goal

The CLI should be the primary low-friction interface between operators, LLM
tools, and the `s7trace` runtime.

The most useful default is to let the CLI execute bounded SQL queries against
SQLite. Time-series data can be large, and an LLM should not fetch it blindly.
SQL gives the caller filtering, aggregation, ordering, and limits close to the
data.

PLC reads and tag validation should stay as typed CLI commands. SQL is for
inspection and controlled database maintenance, not for direct PLC access.

## Interface Split

Recommended command families:

```text
s7trace -r URL sql       # remote read-only SQL through the CLI transport
s7trace --local-db sql   # local read-only SQL against --db
s7trace command-list     # inspect queued commands
s7trace command-wait     # wait for queued command completion
s7trace command-result   # print stored command result
s7trace set-create       # create a named activatable metric set
s7trace set-add-tag      # add a tag to a metric set
s7trace set-enable       # include a metric set in future poll plans
s7trace set-disable      # remove a metric set from future poll plans
s7trace collection-start # start writing into a named dataset
s7trace collection-stop  # stop polling before cleanup
s7trace tag-add          # validate one tag, then store success or bad state
s7trace tag-update       # revalidate changed tag
s7trace tag-disable      # remove from future plans without deleting history
s7trace read             # one-off typed PLC read, not added to polling
s7trace read-raw         # one-off raw DB byte read for offset diagnostics
s7trace status           # runtime health, bad tags, current plans
s7trace history-clean    # explicit cleanup by cutoff, requires stopped collection
s7trace history-delete-dataset # delete a stopped dataset
```

The service remains read-only with respect to PLC memory.

PLC-touching commands and runtime mutations should go through a database-backed
queue. The queue contract is documented in
[`docs/cli-command-queue.md`](cli-command-queue.md).

Read-only SQL can execute directly in the CLI.

## SQL Mode

The default SQL mode should be read-only:

```text
s7trace --local-db sql "select name, bad, bad_class, bad_detail from tag where bad = 1"
s7trace -r http://HOST:8080 sql "select name, bad, bad_class, bad_detail from tag where bad = 1"
```

Default behavior:

- Use the target selected by `-r URL` or `--local-db`. The CLI must not fall
  back to a local database implicitly.
- Open local SQLite with a read-only connection in `--local-db` mode.
- Allow only statements that produce rows.
- Apply a default row limit unless the query already has a tighter limit.
- Support `--format table`, `--format json`, and `--format jsonl`.
- Support `--timeout-ms`.
- Return non-zero exit code on SQL errors.

LLM-friendly default:

```text
s7trace --local-db sql --format jsonl --limit 200 "select * from v_tag_status"
s7trace -r http://HOST:8080 sql --format jsonl --limit 200 "select * from v_tag_status"
```

Write-capable SQL should not be the common path. If needed for maintenance, make
it explicit:

```text
s7trace --local-db admin-sql "update tag set enabled = 0 where bad = 1"
```

`admin-sql` is local-only maintenance. It is rejected with `-r`; remote writes
must use supported typed commands, or a new typed command must be added.

Normal tag changes should use typed commands instead of SQL updates because
typed commands can validate against the PLC and return a useful result.

Metric set changes should also use typed commands. A set enable/disable changes
the active polling plan and must go through the command queue plus delayed apply.

## Useful Views

Expose stable views so callers do not need to know every table detail.

```sql
CREATE VIEW v_tag_status AS
SELECT
    plc.name AS plc,
    tag.name AS tag,
    tag.address,
    tag.data_type,
    poll_group.name AS poll_group,
    tag.enabled,
    tag.bad,
    tag.bad_class,
    tag.bad_detail,
    tag.apply_state,
    tag.desired_version,
    tag.applied_version,
    tag.last_error_ts,
    tag.last_ts,
    tag.last_value_i,
    tag.last_value_f
FROM tag
JOIN plc ON plc.id = tag.plc_id
JOIN poll_group ON poll_group.id = tag.poll_group_id;
```

```sql
CREATE VIEW v_active_tag AS
SELECT DISTINCT
    tag.*
FROM tag
JOIN metric_set_tag ON metric_set_tag.tag_id = tag.id
JOIN metric_set ON metric_set.id = metric_set_tag.metric_set_id
WHERE tag.enabled = 1
  AND tag.bad = 0
  AND metric_set.enabled = 1;
```

```sql
CREATE VIEW v_latest_sample AS
SELECT
    plc.name AS plc,
    tag.name AS tag,
    tag.address,
    tag.last_ts AS ts,
    tag.last_value_i AS vi,
    tag.last_value_f AS vf
FROM tag
JOIN plc ON plc.id = tag.plc_id;
```

These views keep common LLM queries short:

```sql
select plc, tag, address, bad_class, bad_detail
from v_tag_status
where bad = 1
order by last_error_ts desc
limit 50;
```

```sql
select ts, vi, vf
from sample
where tag_id = :tag_id and ts between :from and :to
order by ts
limit 1000;
```

## Tag Assignment and Validation

Adding a tag must perform one real read before it becomes active:

```text
s7trace tag-add --plc line1 --name Motor.Speed --address DB10.DBD240 --type f32
success value=31.5 apply_state=pending
```

or:

```text
s7trace tag-add --plc line1 --name Motor.Speed --address DB10.DBD240 --type f32
bad bad_class=address_out_of_range detail="S7 read failed: address out of range"
```

Even bad tags should be stored. This preserves the operator's intent and makes
the problem visible in `status` and SQL views. Bad tags are excluded from polling
plans until they are changed, recreated, or retried on the next service start.

## Raw DB Read Mode

S7 DB layouts are not always obvious. During tag assignment it is useful to read
a raw byte window around a suspected address.

Prefer an explicit one-off command over storing raw DB snapshots by default:

```text
s7trace read-raw --plc line1 --db 5999 --start 112 --len 32 --format hex
```

Example output:

```text
plc=line1 db=5999 start=112 len=32 ts_us=1780390220031000
00000000  00 00 04 d2 00 00 00 00  42 48 00 00 00 01 00 00
00000010  00 00 00 00 00 00 00 00  00 00 13 88 00 00 00 00
```

For LLM tools, JSON is easier to consume:

```text
s7trace read-raw --plc line1 --db 5999 --start 112 --len 32 --format json
```

```json
{
  "plc": "line1",
  "db": 5999,
  "start": 112,
  "len": 32,
  "ts_us": 1780390220031000,
  "bytes_hex": "000004d200000000424800000001000000000000000000000000138800000000"
}
```

This is almost equivalent to a raw snapshot for offset diagnostics, but it does
not create large unmanaged blobs in SQLite.

If persistent diagnostic evidence is needed, add an explicit opt-in flag:

```text
s7trace read-raw --plc line1 --db 5999 --start 112 --len 32 --save-probe
```

Saved probes should be short-lived and covered by explicit history cleanup.

## Why Not Store Full Raw DB Snapshots By Default

Full raw snapshots are expensive and ambiguous:

- They duplicate data that is already represented as typed samples.
- They grow quickly if a DB is large or read often.
- They are hard to query semantically.
- History cleanup becomes more important and more expensive.

Use raw byte windows for commissioning and offset discovery. Store only typed
tags for regular history.

## Status Contract

`s7trace status` should include the next actionable problem, not only aggregate
counters.

Minimum fields:

```json
{
  "plcs": [
    {
      "name": "line1",
      "connected": true,
      "last_error": null,
      "poll_groups": [
        {
          "name": "fast",
          "target_ms": 40,
          "actual_ms": 37,
          "overrun_count": 0
        }
      ],
      "tags": {
        "active": 128,
        "bad": 3
      },
      "problems": [
        {
          "kind": "bad_tag",
          "tag": "Motor.Speed",
          "address": "DB10.DBD240",
          "bad_class": "address_out_of_range",
          "detail": "S7 read failed: address out of range"
        }
      ]
    }
  ]
}
```

The CLI can expose the same data as table or JSON:

```text
s7trace -r http://HOST:8080 status --format json
s7trace -r http://HOST:8080 sql "select * from v_tag_status where bad = 1"
```

## Practical Rule

Use SQL for asking questions about stored state.

Use typed commands for actions that validate, mutate tag configuration, touch
the PLC, rebuild plans, or affect history cleanup.

Use the command queue for typed commands while the service is running. The CLI
may wait synchronously by default, but the execution should still be serialized
through the service worker.
