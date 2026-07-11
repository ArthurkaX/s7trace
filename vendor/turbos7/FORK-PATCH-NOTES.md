# turbos7 fork patch notes

This is a **local fork** of `turbos7` 0.2.1 (published on crates.io under
`MIT OR Apache-2.0`). The repository URL in the package metadata is unavailable,
so the reviewed source is maintained in-tree. Changes from the published crate:

---

## BUG: multi-variable read returns only the first item ("expected N, got 1")

**Affected:** `src/protocol/s7.rs`, `parse_read_multi_response` (S7 "Read Var",
function 0x04, multi-item response).

**Symptom:** `read_multiple` with N≥2 items returns exactly **1** `ReadResult`
(the first). Single-item `read` is unaffected. On real S7-300/400 CPUs the
caller sees "expected N results, got 1" on every poll.

**Repro:** request 2+ variables from one DB in a single `read_multiple` on an
S7-300/400; the response parses to 1 result regardless of how many items were
requested.

**Root cause:** the per-item length field in an S7 Read-Var **response** is
encoded in **bits** for transport sizes BIT/BYTE/INT, but the multi-var parser
consumed it as a **byte** count. A 2-byte item reports `length = 16` (bits), so
the cursor advanced 16 bytes instead of 2, overshooting every subsequent item;
the `pos < end_data` guard then ended the loop after item 1. The single-read
parser avoided this because it clamps by the byte-accurate S7-header
`data_length`; the multi-read loop had no equivalent clamp.

(Reference: snap7 applies exactly this bits->bytes rule — divide the item
length by 8 except for OCTET STRING / REAL / timer/counter transport sizes.)

**Fix applied in this fork:**
- Read the real `item_count` from the response parameter section and iterate
  exactly that many items.
- Convert the item length from bits to bytes (`raw_len / 8`), except for the
  octet/real transport sizes which are already byte counts.
- Advance the cursor over the **even-padded** data block (`byte_len + (byte_len & 1)`).
- Represent a per-item error (return code != 0xFF) as a failed `ReadResult`
  for that item (empty data, `error_code = Some(rc)`) instead of aborting the
  whole response — error items carry only the 4-byte header.
- Keep all slicing bounded by a clamped `end_data` (panic-safe).
- Added unit tests building synthetic multi-item responses (even payload, odd
  payload forcing pad, and a per-item error code) asserting N results parse
  correctly.

**Verified:** fix confirmed live on two S7-300/400 PLCs — both recovered,
polling live, zero bad tags.

---

## Other issues noticed in the multivar/write path (NOT fixed here)

Out of scope for this read-only consumer, but worth flagging for upstream:

1. **Write path** `parse_write_response`: checks only the global S7 error word,
   never the per-item write return codes, so a partial multi-write failure is
   reported as success.
2. **Request bit-address assumption**: the multi-var request multiplies the
   start address by 8 (bit addressing) only when `area == 0x84` (DB). For other
   byte areas (M/I/Q) the bit address would be wrong.
