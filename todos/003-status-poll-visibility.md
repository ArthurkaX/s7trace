# Task 003: expose poll freshness and effective set poll rate in `/v1/status`

## Goal

Make the status API provide enough data for an operator UI to show:

- whether polling is fresh or stalled
- the current effective poll period for each metric set
- whether a set has one poll rate or mixed poll rates

## Files

- `src/api/mod.rs`
- `src/main.rs`
- `src/types.rs`

## Required JSON additions

Extend `GET /v1/status`.

For each PLC object, include:

```json
"last_poll_cycle_finished_ts": 1710000000
```

Use the most recent `poll_cycle.finished_ts` for that PLC across all poll groups.

For each metric set object, include:

```json
"effective_poll": {
  "mode": "single",
  "poll_group_id": 3,
  "poll_group_name": "diag_split",
  "period_ms": 150,
  "rate_hz": 6.6667
}
```

If the set contains enabled tags from more than one poll group, return:

```json
"effective_poll": {
  "mode": "mixed",
  "poll_group_id": null,
  "poll_group_name": null,
  "period_ms": null,
  "rate_hz": null
}
```

If the set has no enabled tags, return:

```json
"effective_poll": {
  "mode": "none",
  "poll_group_id": null,
  "poll_group_name": null,
  "period_ms": null,
  "rate_hz": null
}
```

## Required behavior

1. Only enabled tags count when deciding `single`, `mixed`, or `none`.
2. `rate_hz` must be `1000.0 / period_ms` for `single`.
3. Do not remove or rename existing status fields.
4. Keep CLI `status` readable; if it prints set lines, include the effective period as `150ms / 6.7Hz`, or `mixed`, or `none`.

## Tests

Add tests for `/v1/status` JSON:

- one set with all enabled tags in one poll group returns `mode: single`
- one set with enabled tags in two poll groups returns `mode: mixed`
- one set with zero enabled tags returns `mode: none`
- PLC object includes `last_poll_cycle_finished_ts`

## Acceptance

- `cargo test` passes.
- Existing clients that read `/v1/status` continue to work.
- A UI can determine stale polling and per-set current poll rate without running SQL.
