# Analysis Hooks

This document lists low-cost hooks that make the SQLite trace easier to analyze
without turning the database into an application-specific diagnostics engine.

## Highest Value Hook: Poll Cycle Identity

Every sample should carry the polling cycle that produced it.

Text logs make this hard: one PLC snapshot is printed as many log lines, so
events from the same read cycle get slightly different text timestamps. Analysis
then has to bucket by timestamp and guess which changes belong together.

Add a `poll_cycle` table and reference it from samples:

```sql
CREATE TABLE poll_cycle (
    id INTEGER PRIMARY KEY,
    plc_id INTEGER NOT NULL REFERENCES plc(id),
    poll_group_id INTEGER NOT NULL REFERENCES poll_group(id),
    seq INTEGER NOT NULL,
    started_ts INTEGER NOT NULL,
    finished_ts INTEGER NOT NULL,
    target_ms INTEGER NOT NULL,
    actual_ms REAL NOT NULL,
    ok INTEGER NOT NULL,
    error TEXT,
    UNIQUE(plc_id, poll_group_id, seq)
);
```

Then `sample.cycle_id` tells SQL exactly which changes were observed together.
This removes timestamp-bucket heuristics for cases like LNK swaps, burst
arrivals, missing increments, and short PEC events.

## Physical Read Span Identity

Samples should also know which physical S7 read produced them.

```sql
CREATE TABLE read_span (
    id INTEGER PRIMARY KEY,
    plc_id INTEGER NOT NULL REFERENCES plc(id),
    poll_group_id INTEGER NOT NULL REFERENCES poll_group(id),
    db_num INTEGER NOT NULL,
    byte_offset INTEGER NOT NULL,
    byte_len INTEGER NOT NULL,
    active INTEGER NOT NULL DEFAULT 1
);
```

Add `sample.span_id`. This makes it possible to separate PLC/process behavior
from collector behavior:

- Did a whole span fail?
- Are bad tags concentrated in one DB area?
- Did a planner change alter the observed timing?
- Which bytes should be captured for a raw diagnostic probe?

## Transition View

The raw `sample` table can store only the new value. SQL can reconstruct old
values with `lag()`.

```sql
CREATE VIEW v_transition AS
SELECT
    plc.name AS plc,
    tag.name AS tag,
    tag.address,
    sample.cycle_id,
    sample.ts,
    lag(sample.vi) OVER (PARTITION BY sample.tag_id ORDER BY sample.ts) AS old_vi,
    sample.vi AS new_vi,
    lag(sample.vf) OVER (PARTITION BY sample.tag_id ORDER BY sample.ts) AS old_vf,
    sample.vf AS new_vf
FROM sample
JOIN tag ON tag.id = sample.tag_id
JOIN plc ON plc.id = tag.plc_id;
```

This keeps storage normalized while giving analyzers the old/new shape that the
legacy log parser expects.

## Value Role

Raw PLC representation and analytical meaning are separate. The old analyzer
had to infer counters and state flags from tag names.

Store this in tag metadata:

```text
data_type = bool | i16 | u16 | i32 | u32 | f32
value_role = state | counter | position | speed | temperature | diagnostic
```

This allows views to expose correct deltas and wraparound behavior without
hard-coded tag names.

## Semantic Tags

Add optional metadata for grouping and filtering:

```text
role = counter | state | command_state | lpos | position | link | queue_depth
domain = rtt_exit | plt | pec | fho2plt | motor
```

SQL can solve most questions once tags have stable roles. Without roles, every
analysis script grows a list of name substrings such as `Missing`, `LPOS`,
`Entries_Used`, `Cnt_Delay_On`, and `Entry[`.

## Event Markers

Keep derived diagnostics separate from raw samples.

```sql
CREATE TABLE event_marker (
    id INTEGER PRIMARY KEY,
    plc_id INTEGER NOT NULL REFERENCES plc(id),
    cycle_id INTEGER REFERENCES poll_cycle(id),
    ts INTEGER NOT NULL,
    kind TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'info',
    tag_id INTEGER REFERENCES tag(id),
    value_i INTEGER,
    value_f REAL,
    details_json TEXT
);
```

Examples:

- `missing_increment`
- `performance_drop`
- `negative_lpos`
- `position_jump`
- `plt_burst_arrival`
- `link_swap`
- `short_pec_glitch`

These markers can be produced by a small rule engine or by offline SQL analysis.
They should not replace raw samples; they are an index into interesting moments.

## Triggered Context Capture

For rare faults, store context only around the trigger.

Example command:

```text
s7trace trace watch --tag s_TRS5_FB_TRK_Track_Samo.s_Statistical.Missing \
  --on increment --before-cycles 20 --after-cycles 40 --raw-spans related
```

When the trigger fires, the service saves:

- the triggering cycle
- selected tag values before and after the event
- optional raw byte windows for related spans
- event markers with a reason

This is more useful than storing full raw DB snapshots continuously.

## Recommended First Hooks

Implement these first:

1. `poll_cycle.id` and `sample.cycle_id`.
2. `read_span.id` and `sample.span_id`.
3. `v_transition`.
4. `tag.data_type`, `tag.value_role`, `tag.role`, and `tag.domain`.
5. `event_marker` for derived diagnostics.
6. Optional triggered `raw_probe` capture around event markers.

These hooks keep normal history compact, but make SQL analysis much closer to
the questions operators actually ask.

## High-Churn Value Aggregation

Some variables change often but do not carry useful value as individual
transitions. Examples are displacement, encoder-like position, continuously
moving virtual positions, and internal counters used only to show that movement
is alive.

For these tags, keep the latest value in the in-memory snapshot, but avoid
writing every change to `sample`.

Recommended storage hooks:

```text
storage_hook = on_change          # discrete state, counters, rare transitions
storage_hook = deadband           # analog values
storage_hook = periodic           # exact cadence required
storage_hook = min_max_rollup     # high-churn values summarized per window
storage_hook = delta_rollup       # encoder-like movement summary
storage_hook = state_duration     # state intervals
storage_hook = counter_delta      # counter increments with wraparound
```

The v1 storage enum is intentionally small. Ideas such as pulse counting,
moving averages, threshold crossing, top-k changes, and event-context capture
are useful, but they need their own schema and tests before they become storage
policies.

Hook parameters should be stored as JSON and validated by the engine before
apply:

```json
{
  "window_ms": 1000,
  "counter": "u16",
  "wrap": true
}
```

The old `store_mode` is still useful as a coarse category, but the hook decides
what is actually written.

## Storage Hook Catalog

`on_change`

Write when the decoded value differs from the last stored value. Best for
discrete states, alarms, fault counters, and rare transitions.

`deadband`

Write when `abs(new - last_stored) >= threshold`. Best for noisy analog values.

`periodic`

Write at a configured period regardless of intermediate changes. Best when a
fixed trend cadence matters more than exact transitions.

`min_max_rollup`

For each window, write `first`, `last`, `min`, `max`, and `sample_count`. Best
default for high-churn values where range and final value matter.

`delta_rollup`

For each window, write `first`, `last`, `delta`, and optionally `abs_delta`.
Best for displacement, encoder-like values, and virtual positions.

`rate_rollup`

Write movement rate as `delta / window`. Useful for speed proxies and conveyor
movement signals.

`moving_average`

Write the average over a window. Useful for analog smoothing. It is not the
preferred default for displacement because it can hide jumps.

`ema`

Write an exponential moving average. Cheaper than a full moving average and
useful for noisy analog values.

`edge_only`

For bool values, write only `rising`, `falling`, or `both` edges.

`pulse_count`

Count selected bool edges per window instead of storing every pulse.

`state_duration`

Store state intervals: `state`, `start_ts`, `end_ts`, and `duration_ms`. Useful
for `Running`, `Motor_On_Speed`, `Section_Run`, and similar state tags.

`counter_delta`

Store counter increments with configured wraparound semantics such as `u16` or
`u32`. Useful for Siemens counters read as signed words but interpreted as
unsigned counters.

`threshold_crossing`

Store only threshold crossings, for example a position moving below a configured
acceptance limit.

`window_entry_exit`

Store entry and exit events for a numeric range. Useful for position windows
such as `0..60`.

`top_k_changes`

Within a window, retain only the largest `k` changes. Useful for noisy values
where only jumps matter.

`event_context`

Normally suppress detailed writes, but store raw samples for selected tags
around an event marker. This is the preferred way to inspect displacement around
rare events such as `missing_increment`.

Rollup rows should preserve useful shape without storing every point:

```sql
CREATE TABLE sample_rollup (
    tag_id INTEGER NOT NULL REFERENCES tag(id),
    plc_id INTEGER NOT NULL REFERENCES plc(id),
    poll_group_id INTEGER NOT NULL REFERENCES poll_group(id),
    window_start_ts INTEGER NOT NULL,
    window_end_ts INTEGER NOT NULL,
    sample_count INTEGER NOT NULL,
    first_vi INTEGER,
    last_vi INTEGER,
    min_vi INTEGER,
    max_vi INTEGER,
    delta_vi INTEGER,
    first_vf REAL,
    last_vf REAL,
    min_vf REAL,
    max_vf REAL,
    delta_vf REAL
);
```

This is a better default for values like displacement: the database records
movement rate and range, while the snapshot still answers "what is it now?"

Triggered capture can still write raw samples for high-churn tags around an
event marker.

## PLC Time and Clock Hooks

Host timestamps are necessary, but they are not enough to understand timing
inside the PLC.

Add optional system tags for PLC time correlation:

- PLC date/time read through the client diagnostics API.
- A configured Siemens clock memory byte, if available.
- A user-program-copied clock byte or cycle counter inside a DB.
- OB1 previous cycle time copied by the PLC program into a DB, if the project
  exposes it.

Clock memory is especially useful as a low-cost phase signal. If the PLC program
copies a clock byte, OB1 cycle counter, or OB1 previous cycle time into a DB,
reading it in the same span as process data gives better correlation than an
out-of-band diagnostic request.

Recommended table:

```sql
CREATE TABLE plc_time_sample (
    id INTEGER PRIMARY KEY,
    plc_id INTEGER NOT NULL REFERENCES plc(id),
    cycle_id INTEGER REFERENCES poll_cycle(id),
    host_before_ts INTEGER NOT NULL,
    host_after_ts INTEGER NOT NULL,
    plc_datetime TEXT,
    clock_byte INTEGER,
    ob1_cycle_ms REAL,
    source TEXT NOT NULL
);
```

The pair `host_before_ts` / `host_after_ts` gives a host-side uncertainty window
for any PLC time read.

## Snap7 Diagnostics

Snap7 exposes generic S7 diagnostics primitives such as:

- PLC date/time reads.
- CPU state and identification.
- SZL / SSL reads.
- Available SZL list reads.
- Diagnostic buffer reads.

Cycle-time min/max/current is CPU-family and firmware dependent. Treat it as an
optional diagnostic probe, not as a guaranteed portable feature. The runtime
should discover supported SZLs per PLC, record what worked, and expose the
result through status.

For reliable per-cycle timing correlation, prefer PLC-side exported values in a
DB over out-of-band diagnostic requests.
