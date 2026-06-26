/// PDU-aware read plan builder.
///
/// Groups tags into physical S7 reads (spans) respecting PDU payload limits
/// and a maximum gap between useful bytes. After the initial block-read
/// grouping, sparse tags in the same DB are merged into multi-variable read
/// spans to reduce round-trips.
use crate::types::*;

/// Default maximum gap between useful bytes before starting a new span.
pub const DEFAULT_MAX_GAP_BYTES: usize = 8;

/// Overhead per S7 read request/response in bytes.
/// Used to calculate effective payload size from PDU length.
const S7_READ_OVERHEAD: usize = 30; // approximate

/// Minimum number of tags in the same DB to consider multi-read grouping.
const MULTI_READ_MIN_TAGS: usize = 2;

// S7 multi-variable-read (Read Var, function 0x04) framing overhead, in bytes.
// turbos7's `read_multiple` issues exactly ONE request PDU and parses ONE
// response PDU — it never chunks — so a multi-read span must fit a single
// negotiated PDU on BOTH the request and response sides, or the PLC returns
// fewer item-results than requested (observed: "expected 15 results, got 5"
// on an S7-300 with pdu=240). Costs derived from turbos7 0.2.1 protocol/s7.rs:
//   request:  S7_MULTI_REQ_HEADER + S7_MULTI_REQ_PER_ITEM * N
//   response: S7_MULTI_RESP_HEADER + Σ (S7_MULTI_RESP_PER_ITEM + pad_even(len_i))
const S7_MULTI_REQ_HEADER: usize = 12; // S7 header (10) + read-param header (2)
const S7_MULTI_REQ_PER_ITEM: usize = 12; // 12-byte item specifier per request item
const S7_MULTI_RESP_HEADER: usize = 14; // S7 header (12) + param echo (2)
const S7_MULTI_RESP_PER_ITEM: usize = 4; // 4-byte data-item header per response item
/// Headroom kept below the negotiated PDU when sizing multi-read spans.
const S7_PDU_SAFETY_MARGIN: usize = 4;

/// Maximum items in a single multi-read request.
///
/// S7-300 / S7-400 CPUs often return fewer item-results than requested even
/// when the PDU budget looks sufficient. This cap forces the planner to split
/// large sparse-DB groups into several smaller multi-read requests, avoiding
/// the "expected N results, got M" failure. Value chosen conservatively based
/// on observed behaviour: DB1291 with 8 items on an S7-300 returned 5.
///
/// Set to 0 to disable the item-count cap (rely on PDU budget alone).
const MAX_MULTI_READ_ITEMS: usize = 3;

/// S7 pads odd-length item data up to an even boundary in the response PDU.
fn pad_even(x: usize) -> usize {
    x + (x & 1)
}

/// Build one `MultiRead` span from a set of same-DB block spans. Concatenates
/// their tags and turns each source span into one multi-read item; the
/// `byte_offset`/`byte_len` envelope spans min-start..max-end for diagnostics.
fn make_multi_span(spans: &[ReadSpan], poll_group_id: i64) -> ReadSpan {
    let mut multi = ReadSpan {
        db_num: spans[0].db_num,
        byte_offset: 0,
        byte_len: 0,
        tag_ids: Vec::new(),
        poll_group_id,
        kind: SpanKind::MultiRead,
        multi_items: Vec::new(),
    };
    let mut min_start = usize::MAX;
    let mut max_end = 0;
    for s in spans {
        multi.tag_ids.extend_from_slice(&s.tag_ids);
        multi.multi_items.push(ReadItem {
            db_num: s.db_num,
            start: s.byte_offset,
            len: s.byte_len,
        });
        min_start = min_start.min(s.byte_offset);
        max_end = max_end.max(s.byte_offset + s.byte_len);
    }
    multi.byte_offset = min_start;
    multi.byte_len = max_end - min_start;
    multi
}

/// Emit a packed bin: <2 items → keep the original block read(s) (a multi-read
/// of one is pointless), ≥2 items → one `MultiRead` span.
fn flush_bin(bin: &mut Vec<ReadSpan>, out: &mut Vec<ReadSpan>, poll_group_id: i64) {
    if bin.len() >= MULTI_READ_MIN_TAGS {
        out.push(make_multi_span(bin, poll_group_id));
        bin.clear();
    } else {
        out.append(bin);
    }
}

/// Build a read plan for a given PLC and poll group from the active tags.
///
/// Tags are grouped by DB number, sorted by offset, and merged into spans.
/// After the initial block-read pass, sparse same-DB spans are grouped into
/// multi-variable reads.
pub fn build_plan(
    plc_id: i64,
    poll_group_id: i64,
    tags: &[TagRow],
    pdu_payload: usize,
    max_gap_bytes: usize,
) -> ReadPlan {
    let effective_payload = pdu_payload.saturating_sub(S7_READ_OVERHEAD);

    // Filter to tags for this poll group, sort by db then offset
    let mut filtered: Vec<&TagRow> = tags
        .iter()
        .filter(|t| t.poll_group_id == poll_group_id)
        .collect();
    filtered.sort_by_key(|t| (t.db_num, t.byte_offset));

    // Phase 1: Build block-read spans by merging adjacent tags within max_gap.
    let mut spans: Vec<ReadSpan> = Vec::new();

    let mut iter = filtered.into_iter().peekable();
    while let Some(tag) = iter.next() {
        let mut span = ReadSpan {
            db_num: tag.db_num,
            byte_offset: tag.byte_offset,
            byte_len: tag.byte_size,
            tag_ids: vec![tag.id],
            poll_group_id,
            kind: SpanKind::BlockRead,
            multi_items: Vec::new(),
        };

        // Try to extend the span with subsequent tags
        while let Some(next) = iter.peek() {
            if next.db_num != span.db_num {
                break; // different DB, start a new span
            }

            let gap = next
                .byte_offset
                .saturating_sub(span.byte_offset + span.byte_len);
            let new_end = next.byte_offset + next.byte_size;
            let new_len = new_end - span.byte_offset;

            if gap <= max_gap_bytes && new_len <= effective_payload {
                // Extend the span
                span.byte_len = new_len;
                span.tag_ids.push(next.id);
                iter.next(); // consume
            } else {
                break; // gap too large or would exceed PDU, start new span
            }
        }

        spans.push(span);
    }

    // Phase 2: Group sparse same-DB spans into multi-read spans, packing them
    // into bins that each fit a single S7 multi-variable-read PDU (both the
    // request and the response must fit — see the S7_MULTI_* constants). An
    // oversized merge makes the PLC silently drop trailing items.
    let budget = pdu_payload.saturating_sub(S7_PDU_SAFETY_MARGIN);
    let mut optimized: Vec<ReadSpan> = Vec::new();
    let mut i = 0;
    while i < spans.len() {
        let db = spans[i].db_num;

        // Collect all contiguous spans for this DB.
        let mut db_spans: Vec<ReadSpan> = Vec::new();
        while i < spans.len() && spans[i].db_num == db {
            db_spans.push(spans[i].clone());
            i += 1;
        }

        // Greedily pack this DB's spans into multi-read bins. Start a new bin
        // whenever adding the next item would overflow either the request or
        // response PDU budget.
        let mut bin: Vec<ReadSpan> = Vec::new();
        let mut req_cost = S7_MULTI_REQ_HEADER;
        let mut resp_cost = S7_MULTI_RESP_HEADER;
        for s in db_spans {
            let item_req = S7_MULTI_REQ_PER_ITEM;
            let item_resp = S7_MULTI_RESP_PER_ITEM + pad_even(s.byte_len);
            if !bin.is_empty()
                && (bin.len() >= MAX_MULTI_READ_ITEMS
                    || req_cost + item_req > budget
                    || resp_cost + item_resp > budget)
            {
                flush_bin(&mut bin, &mut optimized, poll_group_id);
                req_cost = S7_MULTI_REQ_HEADER;
                resp_cost = S7_MULTI_RESP_HEADER;
            }
            req_cost += item_req;
            resp_cost += item_resp;
            bin.push(s);
        }
        flush_bin(&mut bin, &mut optimized, poll_group_id);
    }

    ReadPlan {
        plc_id,
        poll_group_id,
        spans: optimized,
    }
}

/// Build plans for all poll groups.
pub fn build_all_plans(
    plc_id: i64,
    tags: &[TagRow],
    pdu_payload: usize,
    poll_group_ids: &[i64],
    max_gap_bytes: usize,
) -> Vec<ReadPlan> {
    poll_group_ids
        .iter()
        .map(|&pg| build_plan(plc_id, pg, tags, pdu_payload, max_gap_bytes))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::*;

    fn make_tag(id: i64, db: u16, offset: usize, size: usize, pg: i64) -> TagRow {
        TagRow {
            id,
            plc_id: 1,
            name: format!("tag_{id}"),
            address: format!("DB{db}.DBB{offset}"),
            db_num: db,
            byte_offset: offset,
            bit_offset: None,
            byte_size: size,
            data_type: DataType::I16,
            poll_group_id: pg,
            store_mode: StoreMode::OnChange,
            storage_hook: StorageHook::OnChange,
            hook_params_json: None,
            enabled: true,
            bad: false,
            bad_class: None,
            bad_detail: None,
        }
    }

    #[test]
    fn test_single_tag_span() {
        let tags = vec![make_tag(1, 1, 0, 2, 1)];
        let plan = build_plan(1, 1, &tags, 480, 8);
        assert_eq!(plan.spans.len(), 1);
        assert_eq!(plan.spans[0].db_num, 1);
        assert_eq!(plan.spans[0].byte_offset, 0);
        assert_eq!(plan.spans[0].byte_len, 2);
        assert_eq!(plan.spans[0].kind, SpanKind::BlockRead);
    }

    #[test]
    fn test_merge_adjacent_tags() {
        let tags = vec![make_tag(1, 1, 0, 2, 1), make_tag(2, 1, 2, 4, 1)];
        let plan = build_plan(1, 1, &tags, 480, 8);
        assert_eq!(plan.spans.len(), 1);
        assert_eq!(plan.spans[0].byte_len, 6);
        assert_eq!(plan.spans[0].tag_ids.len(), 2);
        assert_eq!(plan.spans[0].kind, SpanKind::BlockRead);
    }

    #[test]
    fn test_gap_splits_span_into_multi() {
        // Two tags in the same DB but far apart. They should become one multi-read span.
        let tags = vec![
            make_tag(1, 1, 0, 2, 1),
            make_tag(2, 1, 20, 4, 1), // gap of 18 bytes > max_gap=8
        ];
        let plan = build_plan(1, 1, &tags, 480, 8);
        assert_eq!(plan.spans.len(), 1);
        assert_eq!(plan.spans[0].kind, SpanKind::MultiRead);
        assert_eq!(plan.spans[0].tag_ids.len(), 2);
        assert_eq!(plan.spans[0].multi_items.len(), 2);
    }

    #[test]
    fn test_different_db_stays_separate() {
        let tags = vec![make_tag(1, 1, 0, 2, 1), make_tag(2, 2, 0, 4, 1)];
        let plan = build_plan(1, 1, &tags, 480, 8);
        assert_eq!(plan.spans.len(), 2);
        assert_eq!(plan.spans[0].kind, SpanKind::BlockRead);
        assert_eq!(plan.spans[1].kind, SpanKind::BlockRead);
    }

    #[test]
    fn test_pdu_payload_limit() {
        let mut tags = Vec::new();
        for i in 0..10 {
            tags.push(make_tag(i as i64, 1, i * 4, 4, 1));
        }
        let plan = build_plan(1, 1, &tags, 50, 8);
        assert!(plan.spans.len() >= 2);
    }

    #[test]
    fn test_wrong_poll_group_excluded() {
        let tags = vec![
            make_tag(1, 1, 0, 2, 1),
            make_tag(2, 1, 2, 4, 2), // different poll group
        ];
        let plan = build_plan(1, 1, &tags, 480, 8);
        assert_eq!(plan.spans.len(), 1);
        assert_eq!(plan.spans[0].tag_ids, vec![1]);
        assert_eq!(plan.spans[0].kind, SpanKind::BlockRead);
    }

    #[test]
    fn test_three_sparse_in_same_db_become_multi() {
        let tags = vec![
            make_tag(1, 1, 0, 2, 1),
            make_tag(2, 1, 50, 2, 1),
            make_tag(3, 1, 100, 4, 1),
        ];
        let plan = build_plan(1, 1, &tags, 480, 8);
        assert_eq!(plan.spans.len(), 1);
        assert_eq!(plan.spans[0].kind, SpanKind::MultiRead);
        assert_eq!(plan.spans[0].multi_items.len(), 3);
    }

    // Recompute the actual S7 wire costs for a multi-read span, mirroring the
    // backend, to assert every emitted span fits one PDU.
    fn multi_req_cost(span: &ReadSpan) -> usize {
        S7_MULTI_REQ_HEADER + S7_MULTI_REQ_PER_ITEM * span.multi_items.len()
    }
    fn multi_resp_cost(span: &ReadSpan) -> usize {
        S7_MULTI_RESP_HEADER
            + span
                .multi_items
                .iter()
                .map(|it| S7_MULTI_RESP_PER_ITEM + pad_even(it.len))
                .sum::<usize>()
    }

    #[test]
    fn test_multi_read_split_to_fit_pdu() {
        // Many sparse same-DB tags that cannot all fit one 240-byte PDU.
        // They must split into several MultiRead spans, each within budget,
        // with no tag lost or duplicated. (Reproduces the S7-300 field case
        // that produced "expected N results, got M".)
        let pdu = 240;
        let n = 40;
        let mut tags = Vec::new();
        for i in 0..n {
            // 4-byte tags spaced 50 bytes apart → every tag is its own sparse span.
            tags.push(make_tag(i as i64, 1, (i as usize) * 50, 4, 1));
        }
        let plan = build_plan(1, 1, &tags, pdu, 8);

        // Must have split into more than one span.
        assert!(
            plan.spans.len() >= 2,
            "expected a split, got {} span(s)",
            plan.spans.len()
        );

        // Every MultiRead span must fit both PDU budgets; collect all tag ids.
        let budget = pdu - S7_PDU_SAFETY_MARGIN;
        let mut seen: Vec<i64> = Vec::new();
        for span in &plan.spans {
            if span.kind == SpanKind::MultiRead {
                assert!(span.multi_items.len() >= MULTI_READ_MIN_TAGS);
                assert!(
                    multi_req_cost(span) <= budget,
                    "req cost {} > budget {}",
                    multi_req_cost(span),
                    budget
                );
                assert!(
                    multi_resp_cost(span) <= budget,
                    "resp cost {} > budget {}",
                    multi_resp_cost(span),
                    budget
                );
            }
            seen.extend_from_slice(&span.tag_ids);
        }
        seen.sort_unstable();
        let expected: Vec<i64> = (0..n as i64).collect();
        assert_eq!(seen, expected, "every tag must appear exactly once");
    }

    #[test]
    fn test_single_sparse_span_stays_block_read() {
        // One lone sparse tag in a DB has nothing to pair with → BlockRead.
        let tags = vec![
            make_tag(1, 1, 0, 2, 1),
            make_tag(2, 2, 0, 2, 1), // different DB so DB1 has a single span
        ];
        let plan = build_plan(1, 1, &tags, 240, 8);
        let db1: Vec<&ReadSpan> = plan.spans.iter().filter(|s| s.db_num == 1).collect();
        assert_eq!(db1.len(), 1);
        assert_eq!(db1[0].kind, SpanKind::BlockRead);
    }
}
