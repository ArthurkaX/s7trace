# Multi-read batching in the turbos7 backend

Status: **implemented**. This note records the reason for the batching and
adaptive-cap logic in `src/plc/turbos7_backend.rs`.

## Problem

An S7 multi-read request must fit both the request and response sides of the
negotiated PDU. Some S7-300 CPUs may also return fewer items than requested even
when the calculated byte budget fits. Treating that truncated response as a
complete batch misaligns results with requested addresses and can take an
otherwise valid polling group offline.

## Current implementation

`Turbo7Client::read_multi` splits input items into ordered sub-batches. The pure
`plan_batches` function returns ranges into the original item slice:

```rust
pub(crate) fn plan_batches(
    items: &[ReadItem],
    pdu_bytes: usize,
    max_items: usize,
) -> Vec<Range<usize>>
```

Every range respects:

- S7 request framing size;
- expected response framing and payload size;
- even-byte response padding;
- the negotiated PDU;
- an optional per-batch item-count cap.

Results from successful batches are concatenated in original request order so
the returned `Vec<ReadResult>` remains one-to-one with the input.

## Adaptive item cap

`S7TRACE_MULTI_BATCH_MAX` selects the initial item cap. The default is `3`:

| Value | Behaviour |
|---|---|
| `3` | Default: at most three items, still subject to PDU splitting |
| `2` | At most two items per multi-read |
| `1` | Effectively use one item per call |
| `0` | Disable the count cap and rely on the PDU budget |

If a response is truncated, that client halves its cap and retries from the
start. The reduced cap is retained for later calls. At the minimum cap, another
truncated response becomes an error instead of being accepted ambiguously.

This adaptation is per client connection; it does not rewrite global
configuration or affect another PLC.

## Diagnostic sweep

When investigating an unfamiliar CPU, restart the server with caps `3`, `2`,
and `1`, waiting for complete poll cycles between runs. Interpret the result as
follows:

| Observation | Likely conclusion |
|---|---|
| Recovery only at `1` | Device or library cannot reliably handle multi-item reads |
| Recovery at `2` or `3` | A conservative item cap is sufficient |
| Recovery at `0` | PDU byte planning alone is sufficient for that device |
| No recovery | Investigate address geometry, connection state, and CPU configuration |

An item-level S7 address error is different from response truncation. Verify the
planned DB number, start offset, and length independently; reducing the batch
size cannot repair an item that extends beyond the DB.

## Related code

- `src/plc/turbos7_backend.rs` — batching, adaptive cap, and tests.
- `src/engine/plan.rs` — shared S7 framing constants and padding helper.
- `src/plc/client.rs` — backend-neutral `PlcClient` contract.
