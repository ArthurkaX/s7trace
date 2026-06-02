# Server Control Window

The server may run as a Windows desktop process with a small control window and
a tray icon. This window is not the CLI. The CLI remains a separate console
program and has no interactive window.

The control window exists for local operator control:

- show runtime status
- start or stop collection
- minimize to tray
- exit the server

It should not expose deep database browsing, SQL editing, tag editing, or an
embedded CLI terminal. Settings and configuration changes are CLI and database
operations.

## Main Window

```text
+--------------------------------------------------------------+
| s7trace server                                      _  X      |
+--------------------------------------------------------------+
| Status: RUNNING        Uptime: 03:42:18        DB: WAL OK     |
| Active PLCs: 2         Active datasets: 1       Queue: 0      |
+--------------------------------------------------------------+
| PLC                 State      Poll       Dataset             |
| ------------------------------------------------------------ |
| example-plc       OK         40 ms      intermittent_fault  |
| test-plc-1200       STOPPED    -          -                   |
+--------------------------------------------------------------+
| Last events                                                   |
| 12:41:02  example-plc  cycle actual=37ms target=40ms        |
| 12:40:58  config         3 tag changes pending, apply in 8s   |
| 12:40:51  queue          command #184 read_raw completed      |
+--------------------------------------------------------------+
| [Start Collection] [Stop Collection] [Minimize to Tray]       |
|                                                [Exit Server]  |
+--------------------------------------------------------------+
```

The window close button should minimize to tray by default. Exiting the server
should require an explicit `Exit Server` action or tray-menu exit action.

## Tray Menu

```text
s7trace server
-------------------------------
Open window
Start collection
Stop collection
Exit server
```

The tray menu is intentionally only a process and collection control surface.
It should not expose settings, tag editing, SQL, history cleanup, diagnostics,
or maintenance commands.

## Command Semantics

The window should submit the same typed commands as the CLI through the command
queue. It must not call PLC clients directly.

This keeps ordering consistent:

```text
CLI command
control window action
LLM tool action
        |
        v
command_request queue
        |
        v
server-owned serial command worker
```

`Start Collection`, `Stop Collection`, and `Exit Server` should show the command
result after the queued operation completes. If a command fails, the window
should display the typed error class and short detail returned by the server.

## Safety Rules

- `Exit Server` should stop collection cleanly before process shutdown.
- `Stop Collection` is allowed while polling is active.
- `Start Collection` should fail if another active dataset already exists for
  the same PLC.
- History cleanup is intentionally not part of the minimal tray menu. Cleanup
  remains an explicit CLI or maintenance command and is refused while collection
  is active.
- Settings are intentionally not part of the window or tray menu. They are
  changed through CLI commands or direct database operations.
