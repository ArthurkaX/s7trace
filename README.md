# s7trace

Read-only S7 data collection service for Siemens PLCs.

The project is intended to run as a Rust service that polls one or more PLCs,
stores value changes in SQLite, exposes current values from memory, and keeps
the tag list as live database-managed state.

Architecture notes are kept in [`docs/architecture.md`](docs/architecture.md).

The proposed CLI and SQL inspection contract is described in
[`docs/cli-sql-interface.md`](docs/cli-sql-interface.md).

The queued CLI command execution model is described in
[`docs/cli-command-queue.md`](docs/cli-command-queue.md).

The optional Windows server control window and tray behavior are described in
[`docs/server-control-window.md`](docs/server-control-window.md).

The delayed configuration apply model is described in
[`docs/config-apply-model.md`](docs/config-apply-model.md).

Activatable groups of tags for switching between working areas of a large PLC
are described in [`docs/metric-sets.md`](docs/metric-sets.md).

Accepted design decisions are tracked in
[`docs/decisions.md`](docs/decisions.md).

Schema migration rules are documented in
[`docs/migrations.md`](docs/migrations.md).

History cleanup behavior is documented in
[`docs/retention.md`](docs/retention.md).

Analysis-oriented database hooks are described in
[`docs/analysis-hooks.md`](docs/analysis-hooks.md).
