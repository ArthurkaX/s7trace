# Task 004: add control action to update poll group period with 50 ms minimum

## Goal

Allow the operator UI to change a poll group's period through the same command queue path as set enable/disable.

Minimum allowed period is `50 ms`.

## Files

- `src/engine/server.rs`
- `src/main.rs`
- `src/cli/args.rs`

## Required control action

Add server-side control action:

```json
{
  "action": "poll_group_update_period",
  "poll_group_id": 3,
  "period_ms": 200
}
```

Also accept this selector:

```json
{
  "action": "poll_group_update_period",
  "poll_group": "diag_split",
  "period_ms": 200
}
```

## Required behavior

1. Reject `period_ms < 50`.
2. Reject missing or unknown poll group selector.
3. Update `poll_group.period_ms`.
4. Increment any existing version/change marker used to restart or refresh affected pollers. If no such marker exists, reuse the same restart mechanism currently triggered when tags change poll group.
5. Return a concise success string containing poll group name and new period.

## Required CLI

Add:

```powershell
s7trace poll-group-set-period --id 3 --period-ms 200
s7trace poll-group-set-period --name diag_split --period-ms 200
s7trace -r 127.0.0.1 poll-group-set-period --name diag_split --period-ms 200
```

## Tests

Add tests for:

- local period update succeeds for `50`
- local period update rejects `49`
- remote control payload uses `poll_group_update_period`
- unknown poll group returns an error

## Acceptance

- `cargo test` passes.
- Period changes are visible in `/v1/status`.
- Running pollers use the new period without requiring a manual server restart.
