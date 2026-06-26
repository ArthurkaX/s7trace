# s7trace Concept

This document states the overall concept of the utility, viewed through two
priorities: **robustness** and **minimalism**. Where it diverges from other
docs, it is proposing what should be considered the load-bearing core and what
should be treated as a later layer.

## Problem

On site there are one or more Siemens S7 PLCs. We need to **observe** their
memory: see current values, accumulate a history of changes, and diagnose why a
given address fails to read. The working constraints are:

- The engineer works **live** — adding, disabling, and re-scoping tags without
  stopping the service.
- The PLC is production equipment. The utility **never writes to PLC memory** and
  must not stress the connection with invalid requests.
- The output must serve both humans (terminal tables) and LLM tooling (bounded
  SQL and typed JSON), without dumping large time-series into context.

In one line: **a read-only "scope + black box" for S7, driven through SQLite and
a thin CLI.**

## Core

The whole system reduces to **three components and one invariant**. Everything
else is a layer above this core.

**Three components:**

1. **Poller daemon** — the only process that talks to the PLC. It owns the
   connection, the read plan, and the in-memory snapshot.
2. **SQLite** — the single source of truth: configuration (tags, sets), history,
   command queue, and status all live here. Configuration is not in TOML or
   static files — only in the database.
3. **CLI** — a thin client. It decides nothing on its own: it either reads the
   database (SQL) or enqueues a typed command.

**One invariant:** at any moment exactly one process — the daemon — talks to the
PLC and writes to the database. The CLI does not duplicate it, does not open a
second PLC connection, and does not write to the live database directly (except
explicit local `admin-sql` while the service is stopped).

This is the minimalism: not "many components" but **one writer, one truth, one
interaction protocol**. It is also the robustness: there is no race for the PLC
connection or for the database.

## Interaction Model

There are two ways to address the system, strictly split by nature.

**Questions → SQL (directly from the CLI, read-only).**

```text
s7trace -r http://host:8080 sql --format jsonl "select * from v_tag_status where bad=1"
```

Reading state needs no daemon-side logic: open a read-only connection, apply a
limit, return rows. Cheap, safe, idempotent. This is the primary channel for both
humans and LLM tooling.

**Actions → typed commands (through the queue to the daemon).**

```text
s7trace tag-add --plc line1 --name Motor.Speed --address DB10.DBD240 --type f32
→ success value=31.5   |   bad bad_class=address_out_of_range
```

Anything that touches the PLC or changes the poll plan (`tag-add/update/disable`,
`read`, `read-raw`, `set-enable`, `collection-start/stop`, `history-clean`) is
serialized through the daemon. The CLI waits for the result synchronously by
default, but the daemon is what executes.

**Why this is robustness:** the split "question / action" is the split "safe to
repeat / has side effects." An LLM can hammer SQL freely but cannot accidentally
trigger production logic.

Key UX principle: **`status` shows the next problem, not counters.** "3 bad tags"
is useless; "`Motor.Speed` → address_out_of_range" is actionable. Machine-readable
output always carries `bad_class` plus a stable exit code.

## CLI ↔ Server Contract (LLM-first)

The CLI is consumed primarily by LLM tooling. A human only opens it occasionally
to poke around. The contract is therefore optimized for machine use and token
economy, with a human table mode as the exception, not the default.

### Server-only

The CLI does **nothing** on its own. Every action goes to a running server. The
only thing the CLI does without a server is **offline read-only SQL** via
`--local-db` (inspecting the database when the daemon is down). There is no
"local action" path — this removes the old per-command remote/local capability
split entirely.

### One address, from the environment

The server address is a **single IP held in an environment variable**:

```text
S7TRACE_SERVER=192.168.0.50
```

The port is **fixed in the binary** (default `7707`; PLC port 102 is taken by the
PLC itself), overridable with `S7TRACE_PORT`. The CLI builds `http://{ip}:{port}`
itself — the env holds only the IP. `-r URL` overrides the env when needed. (clap
is already built with the `env` feature.)

### Synchronous, the queue is invisible

One CLI call returns the final result. The caller never sees a `command_id` and
never polls. Example: `tag-add` returns the validated value or the bad reason
directly. The server still serializes commands internally through the queue (the
single-writer invariant holds), but it holds the response open until the command
finishes, bounded by `--timeout-ms`. On timeout the CLI returns
`{"ok":false,"exit_code":3,"detail":"timeout"}`; the command may still complete
server-side (leases cover that). The visible commands `command-list`,
`command-wait`, `command-result`, and the `--async`/`no-wait` flags drop out of
the contract.

### Uniform, compact output

- **Typed commands** return one compact JSON object, null fields omitted:
  - success: `{"ok":true,"result":{...}}`
  - failure: `{"ok":false,"bad_class":"...","detail":"...","exit_code":2}`
- **SQL reads** return **columnar JSON** — keys once, rows as arrays, one line,
  no indentation, types preserved:

  ```json
  {"cols":["name","bad","bad_class"],"rows":[["Motor.Speed",1,"address_out_of_range"]]}
  ```

  This is the default because it is structured *and* token-cheap: column names are
  not repeated per row.
- **Human mode** is `--format table` only. JSON is never pretty-printed by default
  (whitespace costs tokens).
- Stable exit codes (`docs/decisions.md` D010) are kept and mirrored in the
  envelope's `exit_code`.

Named subcommands are kept (typed args map cleanly to LLM tool schemas); only
their I/O is unified by the rules above.

## Robustness: Failure Modes and Their Containment

Each failure mode and its response:

| Failure | Containment |
|---|---|
| Bad tag address | **Bad-tag quarantine**: one real read on add; on failure the tag is stored as `bad`, excluded from the plan, and visible in `status`. One address cannot break the read plan. |
| Grouped-read failure | Localize: mark the span suspect → re-read sub-spans / individually → identify the culprit → set `bad=1` → rebuild the plan **without** it. |
| Daemon crash mid-command | **Queue leases** (`lease_expires_ts`, heartbeat, attempts). A command never sits in `running` forever — the lease expires and the command is reclaimed. |
| Poll period overrun | `overrun_mode = skip_missed_ticks` by default — no unbounded backlog. `status` exposes target/actual/overrun. |
| Degraded PLC connection | Typed `PlcError` (timeout/disconnected/protocol/...) — retry behavior depends on the error class, not on free-form text. |
| History cleanup | Explicit action only; **refused while the dataset/poller is active**; deletes in bounded batches; dependent rows before parent rows. No background deletion next to writes. |

These are already established in `docs/decisions.md`. This document's stance: this
is the load-bearing frame of robustness and must not be weakened.

## Minimalism: Core vs. Later Layer

This is where the concept takes a deliberate position. The current design is in
places richer than a robust MVP needs. Under the minimalism priority, the line is
drawn as follows.

**Core (v1 — without it the utility has no meaning):**

- Poller daemon + `PlcClient` (turbos7 backend) + in-memory snapshot.
- SQLite as control plane + history.
- CLI: `sql`, `tag-add/update/disable`, `read`, `read-raw`, `status`,
  `collection-start/stop`, `history-clean`.
- Bad-tag quarantine, typed errors, block reads with `max_gap_bytes`, command
  queue with leases.

**Defer / keep optional (useful but not load-bearing):**

- **Metric sets** — powerful, but a second grouping level on top of `enabled`.
  For one or two PLCs it is overhead; start with flat `tag.enabled` plus poll
  groups, and introduce sets when the catalog genuinely outgrows the area of
  interest. See [`docs/metric-sets.md`](metric-sets.md).
- **Poll groups fast/medium/slow** — needed eventually, but a single period per
  PLC is enough for v1; multiple periods add planner complexity.
- **Rollup / delta / state_duration storage policies** — v1 needs only
  `on_change` + `periodic` + `deadband`. The remaining storage hooks come as real
  high-frequency tags appear.
- **Windows control window / tray** — pure UX sugar over the same command queue.
  Not part of the load-bearing structure; it goes last. See
  [`docs/server-control-window.md`](server-control-window.md).
- **Analysis hooks (cycle identity, transition views, event markers)** — an
  analytical layer *above* collected data; it does not affect collection and can
  live as separate SQL views later. See
  [`docs/analysis-hooks.md`](analysis-hooks.md).
- **PLC SZL diagnostics (OB1 cycle time, connection-resource budget)** —
  explicitly **out of scope** for now. PDU size stays (it is free from session
  negotiation), but anything requiring an S7 SZL/Userdata read is not implemented:
  the turbos7 backend does only Read/Write Var, and these values vary by CPU
  family. May be revisited later as optional, best-effort, read-only capability
  methods that polling never depends on.

Cut-off principle: **the core keeps only what is required to safely collect data
from a production PLC.** Everything that improves analysis or ergonomics is a
layer above, added incrementally, and is never allowed to complicate the data
path PLC → snapshot → SQLite.

## Server Console Dashboard

When `s7trace server` runs in a terminal, its console is a **live, in-place
status table**, not a scrolling log. It shows PLC connectivity, metric sets with
their status, and a problems block when problems exist. The screen is redrawn in
place on a fixed interval; it never grows unbounded.

Layout (minimalist):

```text
s7trace  ●live  v0.1.0 schema=1      2026-06-25 17:42:10   up 3h12m   refresh 1s

PLC      conn    poll           tags         last_ok
line1    ✔ ok    fast 37/40ms   128a 3bad    2s ago
line2    ✖ down  —              64a  0bad     4m ago

METRIC SETS (line1)
set                    en  total  healthy  bad
intermittent_fault     ✔   42     39       3
motor_health           ✗   12     12       0

PROBLEMS (3)
! line1  Motor.Speed  DB10.DBD240   address_out_of_range  "read failed: out of range"
! line1  Conv.Temp    DB12.DBD8     timeout               "no response in 1000ms"
! line1  Enc.Pos      DB5999.DBW2   decode_error          "NaN for f32"

q quit · r refresh · p pause
```

When there are no problems the block collapses to one line: `PROBLEMS  none ✔`.

### Decisions

- **The dashboard is the server's own foreground console.** No separate process.
  Logs and an in-place table cannot share one terminal, so when the dashboard is
  active **logs are redirected to a file** (`--log-file`, with a default path).
  The terminal shows only the table; the log is never lost, just relocated.
- **Rendering uses raw ANSI**, no TUI framework. For a static table redrawn ~1×/s
  this is enough, and it keeps the dependency surface minimal. (`ratatui` would
  only be justified later for resize/scroll/tabs.)

### Robustness Requirements

These are mandatory, not nice-to-have — they are what keeps the dashboard from
breaking the terminal or the daemon:

- **TTY detection with graceful degradation.** If stdout is not a terminal
  (Windows service, redirection to a file), the daemon must **not** emit escape
  codes. No TTY → no dashboard; logging falls back to file/stderr as today. The
  background operation path is the Windows service or `--no-dashboard`.
- **Terminal restoration on every exit path.** Enter the alternate screen buffer
  on start; restore it on normal exit, on `Ctrl-C`, and on panic. A restore guard
  must run unconditionally, or the user is left with a broken terminal.
- **The dashboard is a read-only consumer, even in-process.** It uses its own
  read-only SQLite connection and bounded queries on a fixed tick. It must never
  contend with or block the writer/poller. It reuses the same status snapshot the
  `status` command already builds — one gather, two presentations.
- **Bounded output by construction.** The frame is built to the current terminal
  size; long detail strings are truncated to width, and the problems list is
  capped to what fits with a `… (+k more)` counter. This is the literal meaning of
  "does not run off into an infinite log."
- **Quitting the dashboard stops the daemon**, because the dashboard *is* the
  daemon's foreground. This must be explicit to the operator. To keep the daemon
  running without an attached console, use the service or `--no-dashboard`.

### Scope

v1 is render + `Ctrl-C` to quit. Interactive keys (`q`/`r`/`p`) require raw
keyboard input and are an optional refinement, not part of the minimal core.

## Relationship to Other Docs

- [`docs/architecture.md`](architecture.md) — runtime model and component
  detail.
- [`docs/decisions.md`](decisions.md) — locked contract (timestamps, typed
  errors, leases, exit codes). This concept treats those as the robustness frame.
- [`docs/cli-sql-interface.md`](cli-sql-interface.md) — the question/action split
  in concrete CLI and SQL terms.

This document is the "why and how much"; those documents are the "what exactly."
