# Configuration Apply Model

## Purpose

Tag and logging configuration is live database state, but not every metadata
change should be applied to the running poller immediately.

LLM tools and CLI sessions may update several similar tags in a short burst:

- change `storage_hook` for a group of displacement tags
- adjust hook parameters such as `window_ms`
- move tags between poll groups
- enable or disable several metric sets
- recreate several bad tags while checking offsets

Applying each row immediately would rebuild plans repeatedly and may create
short unstable periods in the trace. The service should use a short cooldown
before applying related changes.

## Desired State vs Applied State

The database stores desired configuration immediately. The runtime applies a
stable snapshot after a debounce window.

```text
CLI / LLM update
    |
    v
desired config in SQLite
    |
    | config change event, cooldown starts
    v
pending apply window
    |
    | no related changes for cooldown_ms
    v
runtime plan rebuild + storage hook swap
```

This keeps writes simple and keeps the running collector stable.

## Apply Scope

Configuration changes should be grouped by apply scope:

```text
plc
poll_group
metric_set
tag
storage_hook
collection
history_cleanup
```

The most useful practical scopes are:

- `(plc_id)` for connection-level changes
- `(plc_id, poll_group_id)` for plan rebuilds
- `(plc_id, metric_set_id)` for activation/deactivation of a working set
- `(plc_id, storage_hook)` for logging-policy bursts
- `(tag_id)` for single-tag validation and retry

## Cooldown Defaults

Recommended defaults:

```text
tag add/update validation: immediate command result
poll plan rebuild: debounce 500-2000 ms
storage hook apply: debounce 1000-5000 ms
collection start/stop: immediate command result, plan apply may be debounced
history cleanup: explicit maintenance command, no debounce while active polling
```

Tag validation still happens immediately so the CLI can return:

```text
success value=1234 apply_state=pending
```

or:

```text
bad bad_class=address_out_of_range detail="address out of range"
```

The cooldown controls when the validated desired state becomes part of the
active polling and storage plan.

## Queue Interaction

Commands that mutate configuration should complete in two phases:

1. Validate and store desired state.
2. Schedule an apply request with a cooldown.

Example command result:

```json
{
  "ok": true,
  "state": "accepted",
  "value": 1234,
  "apply": {
    "state": "pending",
    "apply_after_ts": 1780390221000000,
    "scope": "plc_poll_group",
    "cooldown_ms": 1500
  }
}
```

If another similar change arrives before `apply_after_ts`, the pending apply is
pushed forward. This is debounce behavior, not a fixed delay per command.

## Tables

```sql
CREATE TABLE config_apply (
    id INTEGER PRIMARY KEY,
    created_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL,
    apply_after_ts INTEGER NOT NULL,
    applied_ts INTEGER,
    state TEXT NOT NULL DEFAULT 'pending',
    scope TEXT NOT NULL,
    plc_id INTEGER REFERENCES plc(id),
    poll_group_id INTEGER REFERENCES poll_group(id),
    metric_set_id INTEGER REFERENCES metric_set(id),
    tag_id INTEGER REFERENCES tag(id),
    reason TEXT,
    cooldown_ms INTEGER NOT NULL,
    details_json TEXT,
    CHECK(state IN ('pending', 'applying', 'done', 'bad', 'cancelled'))
);
```

The service periodically claims due rows:

```sql
select *
from config_apply
where state = 'pending' and apply_after_ts <= :now
order by apply_after_ts, id
limit 1;
```

## Status

Status should expose pending applies:

```json
{
  "config_apply": {
    "pending": 2,
    "next_apply_ts": 1780390221000000,
    "items": [
      {
        "scope": "plc_poll_group",
        "plc": "line1",
        "poll_group": "fast",
        "reason": "storage_hook_changed",
        "apply_after_ms": 820
      },
      {
        "scope": "metric_set",
        "plc": "line1",
        "metric_set": "intermittent_fault",
        "reason": "metric_set_enabled",
        "apply_after_ms": 1100
      }
    ]
  }
}
```

This makes delayed application visible and prevents confusion when SQL shows the
new desired value before the active plan has changed.

## Rule

Immediate validation, delayed apply.

The CLI should report both:

- whether the requested change is valid
- whether the running service has already applied it
