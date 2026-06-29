# s7trace

Read-only S7 data-collection service for Siemens PLCs.

`s7trace` runs as a long-lived service that polls one or more PLCs on
configurable schedules, stores value changes in SQLite, serves the latest
values from an in-memory snapshot, and keeps the tag list as live,
database-managed state. Configuration and control happen through a small CLI
that talks to the running service over an HTTP command queue.

> Status: pure-Rust S7 client (`turbos7`) by default; no native `snap7`
> dependency required. Read-only by design — `s7trace` never writes to a PLC.

## Build

```sh
cargo build --release
# produces target/release/s7trace (and a mock PLC server, s7mock, for tests)
```

Requires a stable Rust toolchain. CI (`.github/workflows/ci.yml`) runs
`cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and
`cargo test` on `windows-latest`.

## Run the service

```sh
# Start the server (HTTP API + control queue on 0.0.0.0:7707 by default)
s7trace --db s7trace.db server

# Headless (no Windows control window):
s7trace --db s7trace.db server --noui --http-bind 127.0.0.1:7707
```

## Use the CLI

Action commands are routed to a running server with `-r` (a bare host uses the
default port 7707, or pass `--server-port` / a full URL). They are enqueued on
the durable command queue, executed by the server, and the result is printed.

```sh
# Configure
s7trace -r 127.0.0.1 plc-add --name line1 --host 192.168.0.50:102 --rack 0 --slot 2
s7trace -r 127.0.0.1 poll-group-add --name fast --period-ms 100
s7trace -r 127.0.0.1 tag-add --plc line1 --name Motor.Speed --address DB1.DBW0 \
        --data-type i16 --poll-group fast
s7trace -r 127.0.0.1 set-create --plc line1 --name area_a
s7trace -r 127.0.0.1 set-add-tag --set area_a --tag Motor.Speed

# Collect & inspect
s7trace -r 127.0.0.1 collection-start --plc line1 --dataset run1 --sets area_a
s7trace -r 127.0.0.1 status
s7trace -r 127.0.0.1 read --plc line1 --address DB1.DBW0 --data-type i16
s7trace -r 127.0.0.1 collection-stop --plc line1
```

Read-only inspection of the SQLite file can also run directly, without a
server, using `--local-db` (valid only for `sql` and `admin-sql`):

```sh
s7trace --local-db --db s7trace.db sql "SELECT name, last_value_i FROM tag"
```

`s7trace --help` lists every command.

## Architecture

See [`docs/architecture.md`](docs/architecture.md) for the data flow, the
unified command core, the server worker threads, and the storage model.

Source layout:

| Path | Responsibility |
|---|---|
| `src/main.rs` | Thin entry point: arg parsing + `dispatch` |
| `src/handlers/` | CLI command handlers, by domain |
| `src/engine/ops.rs` | Unified command core (`Op` + `execute`) |
| `src/engine/workers/` | Server threads: poller spawn, command, apply, sample-writer |
| `src/engine/server.rs` | Server orchestration (`Server`, `run_server_with_config`) |
| `src/engine/poller.rs` | Per-poll-group polling loop |
| `src/api/` | HTTP transport + read endpoints |
| `src/db/` | Schema/migrations, command queue, validation, config-apply |
| `src/plc/` | PLC client trait + `turbos7` backend + mock |
| `src/codec.rs` | Raw-bytes ↔ typed-value decoding |

## Further reading

- [`docs/architecture.md`](docs/architecture.md) — system architecture (post-reboot).
- [`docs/concept.md`](docs/concept.md) — original project concept.
- [`docs/cli-ergonomics.md`](docs/cli-ergonomics.md) — CLI design notes.
- [`docs/server-ui.md`](docs/server-ui.md) — Windows control window.
- [`docs/spike-multiread-chunking.md`](docs/spike-multiread-chunking.md) — S7 multi-read PDU sizing.
- `docs/schema.sql` — canonical schema (kept in sync with `migrations/0001_initial.sql`).
