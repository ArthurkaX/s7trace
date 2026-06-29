# s7trace architecture

Read-only S7 data-collection service. This document reflects the current
(post-reboot) structure.

## Data flow

```text
                         ┌──────────────── s7trace server ────────────────┐
CLI ──enqueue (HTTP)──▶  command_queue ──claim──▶ command_worker           │
                         (SQLite)                    │ execute(Op)         │
                                                     ▼                     │
                                          ops::execute / legacy arms       │
                                          (DB writes, poller control)      │
                                                                           │
   poll_group threads ──PollResult──▶ sample_writer ──▶ SQLite + Snapshot  │
                         (channel)                                         │
                         ◀── HTTP GET /status,/tags,... (dedicated read conn)
                         └─────────────────────────────────────────────────┘
```

## Two interaction modes

`s7trace` is one binary used two ways:

- **Server** (`s7trace server`): owns the PLC connections, the poller threads,
  the command queue worker, the config-apply worker, the sample-writer, and the
  HTTP API.
- **Client** (the other subcommands): mutating/action commands are routed to a
  running server with `-r HOST`. They are serialized and **enqueued** on the
  durable SQLite command queue; the server claims and executes them and writes
  a result the client waits for. Read-only `sql` / `admin-sql` may instead run
  directly against the database file with `--local-db`.

## Unified command core (`src/engine/ops.rs`)

Every mutating operation is described by one typed value, `Op`
(`#[serde(tag = "action")]`), and executed by exactly one function,
`execute(&ExecCtx, op) -> OpOutput`. Both entry points share it:

- the **server** command-worker deserializes a queued `request_json` into an
  `Op` and runs `execute(ExecCtx::server(..), op)` (with live registries);
- the **CLI** in local mode runs `execute(ExecCtx::local(conn), op)` directly.

`MIGRATED_ACTIONS` gates which actions go through the core; the server routes
those before the legacy `match` in `workers/command/`. The tags, metric-sets,
and PLC/poll-group operations are unified here (their old duplicate
local-vs-server implementations were removed). Some server-side maintenance
actions (history, collection, raw reads) remain on dedicated arms in
`workers/command/{admin,control,plc_read}.rs` by design — the CLI for those is a
thin validate-and-enqueue client.

## Server workers (`src/engine/workers/`)

- **poller** (`engine/poller.rs`): one thread per `(plc, poll_group)`. Builds a
  read plan, reads the PLC, and sends a `PollResult` batch down a channel.
  Reconnects use capped exponential backoff + jitter to avoid hammering an
  offline PLC.
- **command** (`workers/command/`): claims pending queue entries (lease-based),
  dispatches each by `CommandKind`, and writes the result.
- **apply** (`workers/apply.rs`): applies pending configuration changes
  (`db::config_apply`) and (re)spawns the affected poller threads, so config
  edits take effect without a restart.
- **sample_writer** (`workers/sample_writer.rs`): drains the poll-result
  channel, persists samples/cycles to SQLite, and updates the in-memory
  `Snapshot`.

## Storage

SQLite in WAL mode (`synchronous=NORMAL`, `busy_timeout=5000`,
`foreign_keys=ON`; see `db/schema.rs`). The schema lives in
`migrations/0001_initial.sql`, kept byte-identical to `docs/schema.sql`.

The server holds a single writer `Connection` behind a mutex, shared by the
workers (SQLite allows one writer). The HTTP API opens its **own** WAL
connection so read endpoints (`/status`, `/tags`, …) run concurrently with the
sample-writer instead of queueing behind it. Latest values for reads come from
the in-memory `Snapshot` (`engine/snapshot.rs`), not from SQLite.

The HTTP layer supports optional bearer-token authentication via the
`S7TRACE_TOKEN` environment variable; when unset or empty the server is open
(default). The CLI client sends the matching `Authorization: Bearer …` header
automatically when the variable is set.

## PLC backend (`src/plc/`)

`PlcClient` is a small trait; the default backend is `turbos7` (a vendored
pure-Rust S7 client — no native `snap7` needed). A mock backend powers tests.

Multi-reads are chunked to fit the negotiated PDU. Because some PLCs (notably
S7-300) truncate a multi-read response even when the request fits, each client
keeps an adaptive per-batch item cap that halves on truncation and converges to
a value the device honours.

## Module map

| Path | Responsibility |
|---|---|
| `src/main.rs` | Entry point, arg parsing, `dispatch` |
| `src/handlers/` | CLI command handlers, by domain |
| `src/engine/ops.rs` | Unified command core (`Op`, `ExecCtx`, `execute`) |
| `src/engine/server.rs` | `Server`, `ServerConfig`, `run_server_with_config` |
| `src/engine/workers/` | poller spawn, command, apply, sample_writer |
| `src/engine/poller.rs` | Polling loop + reconnect backoff |
| `src/engine/snapshot.rs` | In-memory latest-value cache |
| `src/api/` | HTTP transport (`mod.rs`) + handlers (`handlers.rs`, `status.rs`) |
| `src/db/` | schema/migrations, command_queue, validation, config_apply |
| `src/plc/` | `PlcClient` trait, `turbos7` backend, mock |
| `src/codec.rs` | Raw-bytes ↔ typed-value decoding |
| `src/types.rs` | Shared types (`Op` field shapes, `CommandKind`, `ExitCode`, …) |
