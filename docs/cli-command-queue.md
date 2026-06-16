# CLI Command Queue

## Purpose

The CLI should not call PLC connections directly when the service is running.
It should submit commands into a durable queue and read results back from the
same queue.

This gives the system one serialized place where PLC-touching actions are
executed. It also gives LLM tools a stable request/response trail in SQLite.

## Model

```text
CLI / LLM tool
    |
    | insert command_request
    v
SQLite command queue
    |
    | service worker claims next pending command
    v
s7trace service
    |
    | execute against snapshot, SQLite, or PLC client
    v
command_result
```

Commands are processed in FIFO order per target PLC. Commands that do not touch
a PLC may use a global queue or execute immediately, but the first implementation
should keep the model simple and process all queued commands serially.

## Command Classes

`query`

Read-only SQL. This can execute directly in the CLI because it does not touch
the PLC or mutate runtime state.

`control`

Configuration changes such as `tag add`, `tag update`, `tag disable`,
`set enable`, `set disable`, `collection start`, `collection stop`, history
cleanup requests, and plan rebuilds. These should go through the queue.

Configuration commands may complete before the runtime has applied the new
polling or storage plan. They should return the validation result and the
pending apply state.

`plc_read`

One-off typed reads and raw DB byte reads. These should go through the queue so
they do not race the poller or other diagnostic reads.

`admin`

Explicit maintenance commands. These should go through the queue unless they are
pure read-only inspection.

## CLI Behavior

Mutating commands are **queued and processed asynchronously** by the server's
cmd-worker. The CLI inserts a durable command row and returns immediately with
the assigned id:

```text
s7trace read --plc line1 --address DB5999.DBW120 --type i16
queued command_id=1842
info: use 's7trace command wait 1842' or 's7trace command result 1842' to get the result
```

To get synchronous semantics (block until the result is ready), chain
`command wait`:

```text
s7trace command wait 1842
```

`command wait` polls the queue until the command reaches a terminal state, then
prints the result and exits with success or failure. `command result` fetches
the result without blocking. `command list [--state <s>] [--format json]` shows
queued/recent commands with their created/finished timestamps and any error.

This keeps durable ordering and a traceable result while never blocking the CLI
on a slow or stuck PLC. (A future `--wait` flag could fold the wait into the
mutating command itself; today it is a separate `command wait` call.)

Inspecting a queued command:

```text
s7trace command wait 1842
s7trace command result 1842 --format json
s7trace command list --state pending
```

### Startup behavior

When the server starts, its cmd-worker immediately begins processing any
commands already sitting in the queue, in FIFO (creation) order.

This includes control commands like `collection-start` and `collection-stop`.

This is intentional -- the command queue is durable and replays on startup --
but it has a surprising consequence: a `collection-start` that was queued
before the server was running will execute the moment the server starts
(and if a `collection-stop` was queued right after it, the poller may start
and stop immediately).

If you need a control command applied only after the server is fully up,
queue it **after** starting the server.

## Output Contract

All typed commands should support `--format text`, `--format json`, and
`--format jsonl` where applicable.

Examples:

```text
success value=1234 ts_us=1780390220031000 command_id=1842
```

```text
bad bad_class=address_out_of_range detail="S7 read failed: address out of range" command_id=1843
```

JSON:

```json
{
  "command_id": 1842,
    "state": "done",
    "ok": true,
  "kind": "read",
  "plc": "line1",
  "result": {
    "value": 1234,
    "ts_us": 1780390220031000
  }
}
```

## Queue Tables

```sql
CREATE TABLE command_request (
    id INTEGER PRIMARY KEY,
    created_ts INTEGER NOT NULL,
    claimed_ts INTEGER,
    lease_expires_ts INTEGER,
    heartbeat_ts INTEGER,
    finished_ts INTEGER,
    state TEXT NOT NULL DEFAULT 'pending',
    priority INTEGER NOT NULL DEFAULT 100,
    plc_id INTEGER REFERENCES plc(id),
    kind TEXT NOT NULL,
    request_json TEXT NOT NULL,
    requested_by TEXT,
    idempotency_key TEXT,
    idempotency_expires_ts INTEGER,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    CHECK(state IN ('pending', 'running', 'done', 'bad', 'cancelled')),
    UNIQUE(idempotency_key)
);
```

```sql
CREATE TABLE command_result (
    command_id INTEGER PRIMARY KEY REFERENCES command_request(id),
    ok INTEGER NOT NULL,
    result_json TEXT,
    error TEXT
);
```

The service claims one pending command at a time with a short transaction and a
lease. If the worker crashes, another worker may reclaim `running` commands
whose `lease_expires_ts` is in the past. Each reclaim increments
`attempt_count` and updates the command result trail. For a multi-PLC
implementation, the worker may claim one command per PLC concurrently, but
commands for the same PLC must remain ordered.

Recommended first lease:

```text
lease_ms = 30000
heartbeat_ms = 5000
max_attempts = 3
```

## Why Queue PLC Reads

Snap7 connections are not a good shared concurrent resource. Even if the CLI
request is a single value read, it still competes with the polling loop.

Queueing gives:

- one owner of each PLC connection
- predictable ordering
- no accidental parallel reads during commissioning
- durable command history
- easy LLM inspection through SQL

## Interaction With The Poller

The first implementation should let queued PLC commands run between poll cycles.

If a fast poll group is active, the worker should enforce a small command budget
per cycle. If the queue grows, status should show it:

```json
{
  "command_queue": {
      "pending": 4,
      "running": 1,
      "oldest_pending_age_ms": 820,
      "oldest_running_lease_ms": 12000,
      "last_error": null
  }
}
```

For one-off raw reads that are large, the command should either wait for a safe
slot or return `bad` with a clear overload reason.

## Commands That Can Bypass The Queue

These can execute directly in the CLI:

- read-only SQL queries
- local formatting of previously stored command results
- help/version output
- status reads from HTTP or SQLite if they do not touch PLC connections

Everything that touches PLC state, validates a tag, rebuilds plans, or mutates
runtime configuration should go through the queue.

History cleanup also goes through the queue. Cleanup commands must first verify
that the affected PLC has no active dataset and that polling is stopped for that
scope.

## Exit Codes

Typed CLI commands should use stable exit codes:

```text
0 = ok
1 = bad input or validation failure
2 = PLC communication or PLC address error
3 = service overload or queue timeout
4 = internal error
```

The text and JSON output still carry the detailed `bad_class` and `detail`.

## Delayed Apply

Configuration commands should use the delayed apply model documented in
[`docs/config-apply-model.md`](config-apply-model.md).

The queue validates and stores desired state immediately, then schedules a
debounced runtime apply. This avoids repeated plan rebuilds when several similar
tags or storage hooks are edited in one CLI or LLM session.
