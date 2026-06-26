# Spike: Multi-read Chunking in turbos7 Backend

## What changed and where

**`src/plc/turbos7_backend.rs`** -- the `read_multi` method on `Turbo7Client` (the `PlcClient`
impl backed by the turbos7 crate) now splits the input `items` slice into sub-batches and calls
`client.read_multiple` once per batch, concatenating results **in order** so the returned
`Vec<ReadResult>` lines up 1:1 with the original input `items`.

A new pure function `plan_batches` was added:

```rust
pub(crate) fn plan_batches(
    items: &[ReadItem],
    pdu_bytes: usize,
    max_items: usize,
) -> Vec<Range<usize>>
```

`plan_batches` returns ranges into the original slice and is fully unit-testable without a
live PLC. Each emitted batch respects **both** the negotiated PDU byte budget on the request
and response sides (using the framing constants from `src/engine/plan.rs`) **and** the
per-batch item-count cap.

**`src/engine/plan.rs`** -- the S7 multi-read framing constants (`S7_MULTI_REQ_HEADER`,
`S7_MULTI_REQ_PER_ITEM`, `S7_MULTI_RESP_HEADER`, `S7_MULTI_RESP_PER_ITEM`,
`S7_PDU_SAFETY_MARGIN`) and the `pad_even` helper were promoted from `const`/private to
`pub(crate)` so the backend can reuse them without duplication.

**Files NOT touched:** `src/plc/client.rs` (trait unchanged), `src/plc/snap7.rs` (unchanged),
`src/engine/plan.rs` (only visibility changes), no write path code added anywhere.

## Knob for the experiment

Set `S7TRACE_MULTI_BATCH_MAX` (env var) before starting the daemon. It controls the
maximum number of items per multi-read batch. Default is `3`.

| Value | Meaning                                                                |
|-------|------------------------------------------------------------------------|
| 3     | Default -- up to 3 items per `read_multiple` call (may be PDU-split). |
| 2     | At most 2 items per call.                                              |
| 1     | Single reads -- effectively disables multi-read; one call per item.     |
| 0     | No item-count cap; rely on PDU byte budget alone for splitting.         |

Sweep 3 -> 2 -> 1 on the server. No recompile needed.

## Experiment protocol

Run this **on the server host** (the PLCs at 192.168.0.50 / 192.168.0.51:102 are only
reachable from the server).

1. Rebuild and deploy the updated binary.
2. Restart the daemon with the default batch size (3):
   ```
   set S7TRACE_MULTI_BATCH_MAX=3
   <start daemon>
   ```
3. Wait for a full poll cycle (~10 s) and check:
   ```
   s7trace status
   ```
   Note: are `rtt` and `plc2` online? What are their RTTs?
4. Stop the daemon, restart with batch size 2, repeat step 3.
5. Stop the daemon, restart with batch size 1, repeat step 3.
6. (Optional) Stop, restart with batch size 0 (PDU-only splitting), repeat step 3.

## Decision table

Interpret the results of the sweep:

| Recovery at batch=1 only            | turbos7's `read_multiple` has a parsing bug or the PLC has a hard CPU-item limit. Favours building our own S7 client.           |
|-------------------------------------|----------------------------------------------------------------------------------------------------------------------------------|
| Recovery at batch=2 or 3            | Backend chunking is the real fix. A full rewrite of the S7 stack is lower priority; tune `S7TRACE_MULTI_BATCH_MAX` to the right value. |
| Recovery even at batch=0 (no cap)   | It was purely a PDU-size miscalculation elsewhere. The planner's `MAX_MULTI_READ_ITEMS` or the backend's framing constants may be off, but chunking at default 3 fixes it. |
| No recovery at any batch size       | The root cause is not multi-read truncation. Investigate planner errors, connection issues, or CPU-side configuration.          |

## Separate open question: plc2 error_code=0x05 on DB1291@2022

The planner emits a multi-read item for DB1291 starting at byte 2022. When read as a
single item via `read_db` it succeeds, but inside a multi-read the PLC returns
`error_code=0x05` (address out of range). This may be a **planner start/len bug** (the
multi-read item may extend past the end of the DB) and is **not necessarily a protocol
issue**. Chunking alone may not fix it. If plc2 stays offline after the sweep, check the
DB1291 item geometry independently.

## Files changed

- `src/plc/turbos7_backend.rs` -- adds `plan_batches()`, `max_batch_items()`, batched `read_multi`
- `src/engine/plan.rs` -- made framing consts and `pad_even` `pub(crate)`
- `docs/spike-multiread-chunking.md` -- this file
