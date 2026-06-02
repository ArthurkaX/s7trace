# Design Decisions

This document records decisions promoted from `docs/questions.md` into the
project contract. It is intentionally short and implementation-facing.

## D001: Timestamps Are Integer Microseconds

All persistent timestamps use `INTEGER` Unix epoch microseconds.

Rationale:

- avoids floating-point precision drift
- keeps ordering stable at 40 ms polling cadence
- leaves room for sub-millisecond diagnostic windows

Column names may keep the `_ts` suffix, but their unit is fixed by
`schema_meta.time_unit = unix_epoch_us`.

## D002: Latest Values Come From `tag.last_*`

`v_latest_sample` reads from `tag.last_ts`, `tag.last_value_i`, and
`tag.last_value_f`.

It must not use a correlated `max(sample.ts)` query. Current values are runtime
state and should be cheap to ask for.

Historical reconstruction still uses `sample`.

## D003: Queue Commands Use Leases

Queued commands have:

- `claimed_ts`
- `lease_expires_ts`
- `heartbeat_ts`
- `attempt_count`

A crashed worker cannot leave commands in `running` forever. Expired leases are
reclaimable, with a first implementation default of:

```text
lease_ms = 30000
heartbeat_ms = 5000
max_attempts = 3
```

## D004: Schema Version Is Stored In SQLite

The live database contains `schema_meta`.

Required keys:

```text
schema_version
time_unit
```

The initial schema is version `1`. Future changes should be additive migrations,
not manual edits against a production database.

## D005: PLC Errors Are Typed

The engine must classify PLC failures before storing them.

Bad tags store:

- `bad_class`
- `bad_detail`

`bad_class` is queryable and stable. `bad_detail` is explanatory text.

Recommended error classes:

```text
address_parse
address_out_of_range
decode_error
timeout
disconnected
protocol_error
plc_unavailable
internal
```

## D006: Data Type Encodes Width And Representation

`data_type` is the source of truth for PLC value width and representation:

```text
bool
i16
u16
i32
u32
f32
```

`value_role` describes how the value is used analytically:

```text
state
counter
position
speed
temperature
diagnostic
```

This prevents invalid combinations such as reading an `int` while interpreting
it as a `counter_u32`.

## D007: Storage Hooks V1 Is Small

The first supported storage hooks are:

```text
on_change
deadband
periodic
min_max_rollup
delta_rollup
state_duration
counter_delta
```

Other ideas such as triggered context capture, pulse counting, moving averages,
and top-k change analysis are useful but are not part of the v1 storage policy
enum.

## D008: Desired State And Applied State Are Visible

Live configuration changes are stored immediately but applied after a cooldown.
Tags therefore expose:

- `desired_version`
- `applied_version`
- `apply_state`

Status APIs should surface this per tag or per metric set. Operators should not
have to infer delayed-apply state from raw `config_apply` rows.

## D009: History Cleanup Requires Stopped Collection

History cleanup is not a background worker running beside polling. It is an
explicit maintenance action.

Rules:

- stop collection before cleanup
- refuse cleanup for active datasets or active PLC pollers
- clean by cutoff timestamp or delete a stopped dataset
- delete in bounded batches
- delete dependent rows before parent rows

Cleanup covers:

- `sample`
- `sample_rollup`
- `event_marker`
- `plc_time_sample`
- `raw_probe`
- `poll_cycle`
- inactive unreferenced `read_span`

## D010: CLI Exit Codes Are Stable

Typed CLI commands use:

```text
0 = ok
1 = bad input or validation failure
2 = PLC communication or PLC address error
3 = service overload or queue timeout
4 = internal error
```

Machine-readable output should still include `bad_class` and `detail`.
