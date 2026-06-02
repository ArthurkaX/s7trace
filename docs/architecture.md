# s7trace Architecture

## Purpose

`s7trace` is a read-only data collection service for Siemens S7 PLCs.

The service polls configured PLC tags, stores historical samples in SQLite, and
serves current values from an in-memory snapshot. It is designed for live
operation where tags may be added, changed, disabled, or diagnosed while the
service is running.

The service never writes to PLC memory.

## Core Decisions

- SQLite is the control plane and storage backend.
- Tags are not configured through TOML or static files.
- Each tag belongs to one PLC.
- Tags are organized into activatable metric sets. The tag catalog may be larger
  than the currently active polling plan.
- Multiple PLCs are supported through a PLC abstraction layer.
- Polling is read-only and uses grouped DB reads where possible.
- Bad tags are quarantined so the service does not repeatedly stress the PLC
  connection with invalid addresses.
- Current values are served from memory, not by querying SQLite.
- Historical samples are written in batches.
- History cleanup is an explicit maintenance action and is refused while the
  affected collection is active.

## Runtime Model

```text
CLI / HTTP / LLM tools
Windows control window / tray
        |
        v
SQLite control plane
        |
        v
Engine
 ├─ PLC registry
 │   ├─ PLC poller: line1
 │   │   ├─ Snap7 connection
 │   │   ├─ PDU-aware read plans
 │   │   └─ in-memory snapshot updates
 │   └─ PLC poller: line2
 │       ├─ Snap7 connection
 │       ├─ PDU-aware read plans
 │       └─ in-memory snapshot updates
 ├─ SQLite writer
 ├─ history cleanup worker (maintenance only)
 └─ HTTP API
```

Each PLC has its own connection, negotiated PDU length, read plan, retry state,
and status. Read plans are never shared across PLCs.

The optional Windows control window is only a local server control surface. It
can show status, start or stop collection, minimize to tray, and exit the
server. It is not an embedded CLI and should submit typed actions through the
same command queue as external CLI commands. Its behavior is documented in
[`docs/server-control-window.md`](server-control-window.md).

## PLC Abstraction

The polling engine should depend on a small PLC client interface rather than on
Snap7 directly:

```rust
trait PlcClient {
    fn connect(&mut self) -> Result<(), PlcError>;
    fn disconnect(&mut self);
    fn is_connected(&self) -> bool;
    fn pdu_len(&self) -> Result<usize, PlcError>;
    fn read_db(&mut self, db: u16, start: usize, len: usize) -> Result<Vec<u8>, PlcError>;
    fn read_multi(&mut self, items: &[ReadItem]) -> Result<Vec<ReadResult>, PlcError>;
}
```

`PlcError` must classify at least timeout, disconnect, address out of range,
decode/protocol failure, PLC unavailable, and internal errors. Bad-tag
quarantine and retry behavior should depend on this typed error class, not on
free-form text.

The first implementation should use Snap7 through Rust FFI bindings, because
that matches the behavior of the existing Python tooling and its libsnap7 stack.

This abstraction keeps planner, storage, and status logic testable without a
live PLC while still allowing the runtime to use real hardware immediately.

## Tag Lifecycle

Tags are stored in SQLite and are live runtime state.

Adding or updating a tag is not just a metadata operation. The service must
validate the tag immediately:

1. Parse the address.
2. Find the target PLC.
3. Perform a single read.
4. Decode the value.
5. Store the tag.
6. Return either `success, value=...` or `bad, why=...`.

If the read fails, the tag is still stored, but it is marked as bad. This makes
the problem visible through status APIs and avoids losing diagnostic context.

Example CLI behavior:

```text
s7trace tag add --plc line1 --name Cnt_Virtual_Position --address DB5999.DBW120 --type i16
success value=1234 apply_state=pending
```

or:

```text
s7trace tag add --plc line1 --name Motor.Speed --address DB10.DBD240 --type f32
bad bad_class=address_out_of_range detail="S7 read failed: address out of range"
```

## Metric Sets

Metric sets are named working contexts for a PLC. They let an operator keep a
large validated tag catalog while enabling only the area currently under
investigation.

Example:

```text
intermittent_fault
motor_health
commissioning_offsets
```

A metric set belongs to one PLC and contains tags from that PLC. Sets can be
enabled and disabled without deleting tags or history. Several sets may be
enabled at once.

A tag participates in polling only when:

```sql
tag.enabled = 1
AND tag.bad = 0
AND the tag belongs to at least one enabled metric set
```

This makes `tag.enabled` a hard local switch and `metric_set.enabled` an
operator workflow switch.

Metric sets are documented in detail in [`docs/metric-sets.md`](metric-sets.md).

## Collection Datasets

Metric sets describe what to collect. Datasets describe where collected history
is stored.

A dataset is a collection session for one PLC. It can be `active` or `stopped`.
The poller writes samples, cycles, rollups, events, PLC time samples, and raw
probes into the active dataset. Cleanup commands may delete old history by date
or delete a stopped dataset, but they must refuse to run while the affected
dataset or PLC poller is active.

Typical workflow:

```text
s7trace collection start --plc line1 --dataset intermittent_fault_2026_06_02 --set intermittent_fault
s7trace collection stop --plc line1
s7trace history delete-dataset --dataset intermittent_fault_2026_06_02
```

## Bad Tag Handling

Polling plans include only tags where:

```sql
tag.enabled = 1
AND tag.bad = 0
AND exists at least one enabled metric set containing this tag
```

If a planned read fails, the engine should localize the failure. A practical
first strategy is:

1. Mark the read span as suspect.
2. Retry smaller sub-spans or individual tag reads.
3. Identify the failing tag where possible.
4. Mark the tag as `bad = 1` with `bad_class`, `bad_detail`, and
   `last_error_ts`.
5. Rebuild the read plan without that tag.

The next status response must include the bad tag and the reason. The engine
should not keep polling known-bad tags.

On service startup, bad tags get one retry attempt. If the attempt fails, the tag
returns to quarantine. A tag is also revalidated when it is recreated, changed,
or explicitly retried with `s7trace tag revalidate`.

## One-Off Reads

The system must support one-off reads that do not add tags to the polling plan.

Example:

```text
s7trace read --plc line1 --address DB5999.DBW120 --type i16
value=1234 ts_us=1780390220031000
```

This is useful for diagnostics and for LLM-driven exploration before a tag is
made permanent.

## Read Plan

The read planner groups tags into physical S7 reads. For DB tags, the default
strategy is PDU-aware block reads.

After connecting to a PLC, the engine obtains the negotiated PDU length and
calculates the maximum useful payload size. It then takes tags from enabled
metric sets and groups them by PLC, poll group, and DB.

Within each DB:

```text
sort tags by byte offset
start a span [start, end]
for each tag:
  gap = tag.offset - span.end
  new_len = tag.end - span.start
  if gap <= max_gap_bytes and new_len <= pdu_payload:
    extend the span
  else:
    close the current span and start a new one
```

The planner has two important limits:

- `pdu_payload`: hard protocol limit for a single read response.
- `max_gap_bytes`: economic limit for reading unused bytes between useful tags.

Dense tags should become block reads. Sparse tags may later be optimized with
multi-variable reads, but the first implementation should start with block reads
and an explicit `max_gap_bytes` setting.

## Poll Groups

Poll groups define target periods, for example:

```text
fast = 40 ms
medium = 250 ms
slow = 1000 ms
```

Plans are built per `(plc, poll_group)`.

If tags with different periods are close enough to share a span, the faster plan
may read bytes that also contain slower tags. Slower tags should not necessarily
produce stored samples on every fast read; storage policy still decides whether a
sample is written.

In practice, hot and cold data should be separated at the PLC DB design level
where possible.

## Scheduling and Overrun Behavior

The poller attempts to keep the configured period. If a cycle exceeds its budget,
the engine should not accumulate an unbounded backlog of missed ticks.

Recommended default:

```text
overrun_mode = skip_missed_ticks
```

The status API should expose:

- target period
- actual cycle time
- overrun count
- last overrun duration
- degraded state

Strict mode can be added for commissioning and testing, where any missed period
is treated as a configuration or performance error.

## Storage Policies

Each tag has a storage policy:

- `on_change`: write only when the decoded value differs from the last value.
- `deadband`: write when the difference from the last stored value exceeds a
  configured deadband.
- `periodic`: write every poll.
- `min_max_rollup`: store window shape for high-churn values.
- `delta_rollup`: store first, last, and delta per window.
- `state_duration`: store state intervals instead of every state sample.
- `counter_delta`: store counter movement with wrap handling.

The in-memory snapshot is updated on every successful poll, even if no sample is
stored.

Important: `on_change` only detects changes visible at the polling cadence. A
short pulse between two reads cannot be recovered without PLC-side buffering.

## HTTP Status

The status endpoint should expose both logical and physical state:

```json
{
  "plcs": [
    {
      "name": "line1",
      "connected": true,
      "pdu_payload": 222,
      "poll_groups": [
        {
          "name": "fast",
          "target_ms": 40,
          "actual_ms": 37,
          "overrun_count": 0,
          "spans": [
            {
              "db": 5999,
              "start": 120,
              "len": 48,
              "tags": ["Cnt_Virtual_Position"]
            }
          ]
        }
      ],
      "metric_sets": [
        {
          "name": "intermittent_fault",
          "enabled": true,
          "tags_total": 42,
          "tags_active": 39,
          "tags_bad": 3
        }
      ],
      "tags": {
        "active": 128,
        "bad": 3
      },
      "bad_tags": [
        {
          "name": "Motor.Speed",
          "address": "DB10.DBD240",
          "bad_class": "address_out_of_range",
          "detail": "S7 read failed: address out of range",
          "last_error_ts": 1780390201220000
        }
      ]
    }
  ]
}
```

This makes the service inspectable: operators and LLM tools can see what is
configured, what is actually being read, and which addresses are unhealthy.

## CLI and Database Interface

The preferred CLI model is SQL-first for inspection and typed commands for
actions.

LLM tools should be able to run bounded read-only SQL queries against SQLite
instead of loading full time-series ranges into context. Tag changes, validation,
one-off PLC reads, raw byte reads, and history cleanup operations should remain typed
commands because they carry runtime side effects and must return precise
success or failure results.

The detailed contract is documented in
[`docs/cli-sql-interface.md`](cli-sql-interface.md).

Analysis hooks for cycle identity, transition views, semantic tag metadata, and
event markers are documented in [`docs/analysis-hooks.md`](analysis-hooks.md).
