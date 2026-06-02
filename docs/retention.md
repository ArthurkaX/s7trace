# History Cleanup

## Goal

History is useful for the current investigation, but it is not sacred archive
data. Cleanup should be explicit, simple, and safe for the poller.

The service must not delete history while collection is active for the affected
PLC or dataset.

Recommended operational rule:

```text
stop collection -> clean history -> start collection
```

## Dataset Model

Metric sets describe what should be collected. A dataset describes one stored
history run.

```text
metric_set = working context / tag selection
dataset    = stored history produced during a collection session
```

A dataset:

- belongs to one PLC
- has a stable name
- has `active` or `stopped` state
- records `started_ts` and optional `stopped_ts`
- can reference the metric sets that were active for the run
- owns the history rows produced during that run

This avoids using `metric_set` as a history boundary. A tag may belong to
several metric sets, but a sample belongs to exactly one dataset.

## Cleanup Modes

Two cleanup modes are supported.

Cut by date:

```text
s7trace history clean --plc line1 --before-ts 1780390221000000
```

Delete a stopped dataset:

```text
s7trace history delete-dataset --dataset intermittent_fault_2026_06_02
```

Both commands must refuse to run if the affected dataset is active.

## Stop Requirement

Cleanup is an explicit maintenance action, not a background worker running
beside fast polling.

Rules:

- no cleanup while any affected dataset has `state = 'active'`
- no cleanup while the affected PLC poller is running
- cleanup commands go through the command queue
- cleanup deletes in bounded batches
- after cleanup, the service may run `VACUUM` only as an explicit separate
  maintenance command

This prevents history deletes from contending with the 40 ms writer path.

## Delete Ordering

Foreign keys require dependent rows to be removed before parent rows.

Recommended order for a dataset delete:

1. `sample`
2. `sample_rollup`
3. `event_marker`
4. `plc_time_sample`
5. `raw_probe`
6. `poll_cycle`
7. inactive `read_span` rows that are no longer referenced
8. `dataset_metric_set`
9. `dataset`

For date-based cleanup, delete rows older than the cutoff first, then delete
orphaned `poll_cycle` rows:

```sql
delete from poll_cycle
where finished_ts < :cutoff_ts
  and not exists (
    select 1 from sample where sample.cycle_id = poll_cycle.id
  )
  and not exists (
    select 1 from event_marker where event_marker.cycle_id = poll_cycle.id
  )
  and not exists (
    select 1 from plc_time_sample where plc_time_sample.cycle_id = poll_cycle.id
  );
```

`read_span` is lower volume than `poll_cycle`, but it should still be cleaned
when it is inactive and no sample references it.

## Batch Rule

Every cleanup step must use bounded batches:

```sql
delete from sample
where rowid in (
  select rowid
  from sample
  where dataset_id = :dataset_id
  limit :delete_batch_rows
);
```

The default batch size is `10000` rows.

## Status

Status should expose whether cleanup is allowed:

```json
{
  "history": {
    "cleanup_allowed": false,
    "blocked_by": "collection_active",
    "active_datasets": [
      {
        "plc": "line1",
        "dataset": "intermittent_fault_2026_06_02",
        "started_ts": 1780390221000000
      }
    ],
    "last_cleanup": {
      "finished_ts": 1780391221000000,
      "deleted_rows": 812000,
      "error": null
    }
  }
}
```

## CLI Commands

```text
s7trace collection start --plc line1 --dataset intermittent_fault_2026_06_02 --set intermittent_fault
s7trace collection stop --plc line1
s7trace history clean --plc line1 --before-ts 1780390221000000
s7trace history delete-dataset --dataset intermittent_fault_2026_06_02
s7trace history vacuum
```

`history vacuum` should require explicit confirmation because it can hold large
SQLite locks.
