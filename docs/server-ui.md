# Native Windows control window

Status: **implemented** in `src/platform/windows.rs` and embedded in
`s7trace server` on Windows.

## Purpose

The control window provides a small local operator surface without duplicating
the CLI or adding a browser UI. It is intended for the industrial PC or service
laptop that runs the server.

It shows every enabled PLC and metric set with these columns:

| Column | Meaning |
|---|---|
| PLC | Configured PLC name |
| Set | Metric-set name |
| On | Whether the set is enabled |
| Tags | Healthy enabled tags / total members |
| Bad | Tags currently marked bad |
| Period | Effective period, `mixed`, or `none` |
| Rate | Effective polling rate for a single poll group |
| Age | Time since the latest relevant poll cycle |
| State | `RUN`, `off`, `MIXED`, `STALE`, or `no sets` |

An enabled row becomes stale when no relevant poll cycle has completed within
the greater of five polling periods or five seconds. This makes a stalled
poller visible even while the HTTP API and database remain responsive.

## Controls

- **Refresh** requests an immediate status refresh; the window also refreshes
  automatically once per second.
- **Enable** and **Disable** enqueue the same metric-set control actions used by
  the CLI.
- **−50 ms** and **+50 ms** change the selected set's poll-group period when all
  enabled member tags use one poll group. The minimum period is 50 ms.
- **Exit** enqueues `server_exit` and stops the server.

The title includes command-queue information, and the last queued command or
error is shown to the operator.

## Runtime model

The window runs in a thread inside the server process. It is not a separate
executable and does not call the local HTTP API. Status reads use the server's
shared SQLite connection, while control actions are inserted into the same
durable command queue used by remote CLI clients. Business logic remains in the
normal command worker.

The window is enabled by default on Windows. Use `server --noui` for headless
operation. Non-Windows builds do not start it.

## Deliberate limits

The window does not provide charts, tag-address editing, set creation, or
history analysis. Those remain CLI and SQL responsibilities. It is a local
operate/observe panel; remote engineering is handled by `s7trace -r HOST`.
