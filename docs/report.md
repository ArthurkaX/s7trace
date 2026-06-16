# s7trace Test Report

**Date:** 2026-06-16 (rerun)
**Build:** `cargo build` (dev profile, 36 warnings)
**Target PLC:** `192.168.0.50:102` (Siemens S7, rack 0, slot 2, PDU=240)

---

## Remaining Issues

### B-7. Poller starvation under concurrent reads (reclassified)

The original report claimed the server crashes under concurrent poller + command load. **This hypothesis was disproven by investigation.**

Key findings:

- The **default** PLC backend is `turbos7` — a pure Rust, async implementation running on a global tokio runtime. The C snap7 library is only used when `S7TRACE_PLC_BACKEND=rust7` is set. A "silent crash" on a default run would be a Rust panic, not a C segfault.
- PLC access is **already serialized**: the poller and the cmd-worker share the same per-PLC `Arc<Mutex<dyn PlcClient>>`. The suggested fix of wrapping in a `Mutex` was already in place.
- 50 rapid `read --no-wait` calls against an active poller could not reproduce any crash — the server stayed alive, no panics occurred, and the queue drained steadily.
- The symptom "all reads stay pending" is actually **poller starvation**: the poller holds the client lock for the entire ~1.7s multi-read cycle, so CLI reads only make progress in the brief gap between cycles.

Defensive improvements applied:

- A global panic-logger and `catch_unwind` were added in the cmd-worker so any Rust panic is now logged and contained instead of silently killing the worker.

**Future work (not yet addressed):**

1. A per-cycle command budget so a fast poll group cannot starve CLI reads.
2. The global `current_thread` tokio runtime is shared across all PLC threads — this is fragile under multi-PLC concurrent `block_on`. A multi-thread or per-thread runtime should be considered.

---

### UX-4. `admin-sql` has no `--format` flag

Output is always `ok rows_affected=N`. Running a SELECT returns no data. This is by design — `admin-sql` is a maintenance-only tool — but it is occasionally inconvenient for diagnostics. **Won't fix** for now (low severity).

---

## What Works Well (all other items from the initial report are fixed)

- Input validation — empty names, zero periods, bad addresses, unknown types all rejected
- `plc-add`, `poll-group-add`, `tag-add` / `tag-import` — all correct
- Metric sets — `set-create`, `set-add-tag`, `set-remove-tag`, `set-enable`, `set-disable`, `set-list`, `set-show`
- Command queue — `command-list --format json`, `command-result`, `command-wait` all work, `Finished`/`Error` columns present
- `tag-update`, `tag-disable`, `tag-revalidate` — now accept `--plc --name` as alternative to `--id`
- `set-show` — accepts both positional `SET_NAME` and `--set` flag
- `server --dry-run` warning — now correctly suggests `plc-add`
- `read` — synchronous by default, `--no-wait` for async
- `tag-add` — synchronous by default, `--async` to skip waiting
- `collection-start` / `collection-stop` — synchronous by default, `--no-wait` for async (was async-only)
- Queue replay on startup — documented in `docs/cli-command-queue.md` (Startup behavior section)
- `status` — text and JSON formats comprehensive
- `sql` — read-only enforcement works
- Documentation — `cli-sql-interface.md`, `testing.md`, `test_pipeline.bat`, `cli-command-queue.md` all synced with CLI
