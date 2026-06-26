# Task 005: add native Win32 `s7panel` operator panel v1

## Goal

Create a Windows-only native GUI that lets an operator see metric sets, toggle them on/off, see poll freshness, and adjust poll period by 50 ms steps.

No web UI. No browser dependency.

## Files

- `src/bin/s7panel.rs`
- `Cargo.toml`

## Required binary

Add a new binary:

```powershell
cargo run --bin s7panel -- --server http://127.0.0.1:7707
```

The binary is Windows-only:

```rust
#![cfg(windows)]
```

Use the `windows` crate and plain Win32 controls.

## Required UI

Main window contains one ListView in report mode with these columns:

- PLC
- Set
- Enabled
- Tags active/total
- Bad
- Poll period
- Poll rate
- Last cycle age

Buttons:

- Refresh
- Enable
- Disable
- `-50 ms`
- `+50 ms`

## Required behavior

1. On startup, call `GET /v1/status`.
2. Refresh every 2 seconds with `SetTimer`.
3. Enable button posts a `set_enable` control command for the selected set.
4. Disable button posts a `set_disable` control command for the selected set.
5. `-50 ms` posts `poll_group_update_period` with `period_ms - 50`.
6. `+50 ms` posts `poll_group_update_period` with `period_ms + 50`.
7. Never post a period below `50 ms`.
8. If selected set has `effective_poll.mode != "single"`, disable `-50 ms` and `+50 ms`.
9. If last cycle age is much larger than the poll period, visually mark the row as stale.

## Stale rule

For v1, mark stale when:

```text
last_cycle_age_ms > max(period_ms * 5, 5000)
```

For `mixed` or `none`, use `5000 ms`.

## Required networking

Use the same HTTP endpoint shape as the CLI:

- `GET /v1/status`
- `POST /v1/command`

Do not read the SQLite database directly.

## Tests / verification

Manual verification is required:

1. Start a local `s7trace` server.
2. Run `cargo run --bin s7panel -- --server http://127.0.0.1:7707`.
3. Confirm the window lists PLCs and sets.
4. Select a set and click Disable; confirm status changes after refresh.
5. Click Enable; confirm status changes after refresh.
6. Click `+50 ms`; confirm period increases in the table.
7. Click `-50 ms`; confirm period decreases but never below `50 ms`.

## Acceptance

- `cargo check` passes on Windows.
- `cargo run --bin s7panel -- --server http://127.0.0.1:7707` opens a native window.
- The panel works through HTTP only.
- Period controls are disabled for `mixed` and `none` effective poll modes.
