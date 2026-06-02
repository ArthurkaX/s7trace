# Metric Sets

## Purpose

A large PLC may describe a whole facility section. Operators often need to work
on one area, pause it, switch to another area, and later enable several areas at
the same time.

The tag catalog should therefore be larger than the active polling plan. A tag
can exist, keep its validation state, and retain history metadata without being
currently collected.

Metric sets provide this activation layer.

## Model

```text
PLC
 ├─ tag catalog
 │   ├─ known tag A
 │   ├─ known tag B
 │   └─ known tag C
 └─ metric sets
     ├─ intermittent_fault: A, B
     ├─ motor_health: B, C
     └─ commissioning_raw_offsets: A, C
```

A metric set:

- belongs to exactly one PLC
- has a stable name
- has an `enabled` flag
- contains zero or more tags from the same PLC
- can be enabled or disabled without deleting tags or history

A tag can belong to multiple sets. The tag is polled if:

```sql
tag.enabled = 1
AND tag.bad = 0
AND exists at least one enabled metric set containing this tag
```

This makes `tag.enabled` a hard per-tag switch and `metric_set.enabled` a
working-context switch.

## Why This Is Separate From Poll Groups

Poll groups answer "how often should this tag be read?"

Metric sets answer "is this working context currently active?"

Mixing those two concerns would make operator workflows awkward. For example,
an airport baggage section can contain fast counters, medium diagnostics, and
slow health values. They should be enabled together as one work set while still
using their own poll periods.

## Activation Semantics

Enabling or disabling a metric set is a configuration change. It should:

1. update desired state immediately
2. schedule a debounced config apply
3. rebuild affected read plans after the cooldown
4. expose pending/applied state in status

The command result should show both acceptance and apply state:

```json
{
  "ok": true,
  "state": "accepted",
  "metric_set": "intermittent_fault",
  "enabled": true,
  "apply": {
    "state": "pending",
    "scope": "metric_set",
    "cooldown_ms": 1500
  }
}
```

## CLI Commands

Recommended commands:

```text
s7trace set create --plc line1 --name intermittent_fault
s7trace set add-tag --set intermittent_fault --tag Cnt_Virtual_Position
s7trace set remove-tag --set intermittent_fault --tag Motor.Speed
s7trace set enable intermittent_fault
s7trace set disable intermittent_fault
s7trace set list --plc line1
s7trace set show intermittent_fault
```

Bulk membership should be supported because LLM tools will often build a set
from SQL results:

```text
s7trace set add-tags --set intermittent_fault --from-sql "
  select id from tag
  where plc_id = 1 and domain = 'rtt_exit' and bad = 0
"
```

`--from-sql` must only accept a bounded read-only query returning tag IDs.

## SQL Views

Useful views:

- `v_metric_set_status`: set name, PLC, enabled state, member counts, bad count
- `v_metric_set_member`: one row per set/tag membership
- `v_active_tag`: tags that currently participate in at least one enabled set

The planner should use `v_active_tag` or equivalent internal query instead of
reading directly from `tag`.

## Status

Status should expose active sets and pending set applies:

```json
{
  "plcs": [
    {
      "name": "line1",
      "metric_sets": [
        {
          "name": "intermittent_fault",
          "enabled": true,
          "apply_state": "applied",
          "tags_total": 42,
          "tags_active": 39,
          "tags_bad": 3
        },
        {
          "name": "motor_health",
          "enabled": false,
          "apply_state": "applied",
          "tags_total": 18,
          "tags_active": 0,
          "tags_bad": 1
        }
      ]
    }
  ]
}
```

## Default Set

For first deployment, create one default set per PLC:

```text
default
```

New tags can be added to this set unless the CLI specifies another set. This
keeps simple usage simple while allowing advanced workflows later.
