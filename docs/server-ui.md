# Task: native Win32 control panel — see and toggle metric sets (on/off)

Status: **planned task** (not yet implemented). Written 2026-06-26. **No web UI** — a native Win32
GUI on Windows. Motivated by a live session: toggling metric sets on/off and seeing what is
actually collecting took many CLI round-trips, and a stalled poll engine was invisible (the server
answered queries and connectivity probes while **no poll cycle ran for ~8 h**).

## Goal

A small native Windows control panel that lets an operator, without the CLI:

1. See every PLC and its metric sets with clear collecting / not-collecting state.
2. Toggle each metric set **on/off** with a button.
3. Immediately notice when polling has stalled.

LAN/desk convenience tool. Native Win32, no browser, no web server dependency for the UI.

## Shape

- A separate Windows-only binary, e.g. `src/bin/s7panel.rs` (mirrors the existing `src/bin/s7mock.rs`
  pattern), gated `#![cfg(windows)]`. Keeps the GUI out of the core daemon and its dependency
  surface.
- Built on the `windows` crate (official Win32 bindings) — consistent with `src/platform/windows.rs`
  already using platform APIs. Plain Win32: a main window, a ListView (report mode) for sets, and
  command buttons. No heavy GUI framework.

## How it talks to the engine (two options)

The panel does **not** re-implement logic; it drives the same control path the CLI uses:

- **Primary: localhost HTTP.** Call the existing `/v1` API on `127.0.0.1:<port>`:
  - read `GET /v1/status` to render PLCs / sets / poll groups,
  - `POST /v1/command` `{kind:"control", request:{action:"set_enable"|"set_disable", ...}}` to
    toggle (actions already implemented in `engine/server.rs`: `set_enable` ~L1477, `set_disable`
    ~L1508; per-tag `tag_enable` ~L988 / `tag_disable` ~L958).
  This reuses `execute_command` dispatch and works whether the panel runs on the server host or not.
- **Alternative: direct command-queue enqueue.** If the panel always runs on the server host, it
  can `command_queue::enqueue(...)` against the local DB directly (no HTTP). Simpler deployment,
  but couples the panel to the DB/schema. Prefer the HTTP path unless there is a reason not to.

## Scope

In:

- Main window with a ListView: columns PLC, set, enabled, tags_active/total, bad, **last poll-cycle
  age**. Color/marker the row when collecting vs off, and when last-cycle age >> poll period (the
  invisible-stall signal).
- Per-row (or selection) **Enable / Disable** buttons that POST the control command, then refresh.
- A refresh timer (e.g. `SetTimer`, ~2 s) plus a manual Refresh button.
- Optional: a set-detail view listing member tags with per-tag enable/disable (covers the
  half-disabled-set issue from `fix.md`).

Out (non-goals for v1):

- Time-series / sample charts.
- Creating sets or editing tag addresses (stays in CLI / future `set-apply`).
- Auth, packaging/installer, cross-platform (Windows only by definition).

## Engine prerequisite (separate from the GUI)

The panel can only *show* a stall; it cannot fix the engine not resuming after restart. Track that
as its own server-side item. For the panel's freshness indicator, add the most-recent
`poll_cycle.finished_ts` per PLC to `GET /v1/status` (cheap `MAX(finished_ts)` subquery) so the GUI
gets last-cycle age in one call instead of issuing SQL.

## Acceptance

- `s7panel.exe` against a running server → window lists all PLCs and sets.
- Toggling a set flips its enabled state (confirmed via `set-show` / status).
- A stalled poll engine is visibly flagged (last-cycle age goes red) instead of looking healthy.

## Related

- `fix.md` — `tag-enable` CLI gap and `set-add-tag` not enabling members (the per-tag toggle here
  would also work around that until the CLI side is fixed).
- `cli-ergonomics.md` — config-as-code direction; this panel is the operate/observe counterpart.
