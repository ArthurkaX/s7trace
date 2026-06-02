/// PDU-aware read plan builder.
///
/// Groups tags into physical S7 reads (spans) respecting PDU payload limits
/// and a maximum gap between useful bytes.

use crate::types::*;

/// Default maximum gap between useful bytes before starting a new span.
pub const DEFAULT_MAX_GAP_BYTES: usize = 8;

/// Overhead per S7 read request/response in bytes.
/// Used to calculate effective payload size from PDU length.
const S7_READ_OVERHEAD: usize = 30; // approximate

/// Build a read plan for a given PLC and poll group from the active tags.
///
/// Tags are grouped by DB number, sorted by offset, and merged into spans.
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

    let mut spans: Vec<ReadSpan> = Vec::new();

    // Group by DB number, then build spans within each group
    let mut iter = filtered.into_iter().peekable();
    while let Some(tag) = iter.next() {
        let mut span = ReadSpan {
            db_num: tag.db_num,
            byte_offset: tag.byte_offset,
            byte_len: tag.byte_size,
            tag_ids: vec![tag.id],
            poll_group_id,
        };

        // Try to extend the span with subsequent tags
        while let Some(next) = iter.peek() {
            if next.db_num != span.db_num {
                break; // different DB, start a new span
            }

            let gap = next.byte_offset.saturating_sub(span.byte_offset + span.byte_len);
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

    ReadPlan {
        plc_id,
        poll_group_id,
        spans,
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
    }

    #[test]
    fn test_merge_adjacent_tags() {
        let tags = vec![
            make_tag(1, 1, 0, 2, 1),
            make_tag(2, 1, 2, 4, 1),
        ];
        let plan = build_plan(1, 1, &tags, 480, 8);
        assert_eq!(plan.spans.len(), 1);
        assert_eq!(plan.spans[0].byte_len, 6);
        assert_eq!(plan.spans[0].tag_ids.len(), 2);
    }

    #[test]
    fn test_gap_splits_span() {
        let tags = vec![
            make_tag(1, 1, 0, 2, 1),
            make_tag(2, 1, 20, 4, 1), // gap of 18 bytes > max_gap=8
        ];
        let plan = build_plan(1, 1, &tags, 480, 8);
        assert_eq!(plan.spans.len(), 2);
    }

    #[test]
    fn test_different_db_splits() {
        let tags = vec![
            make_tag(1, 1, 0, 2, 1),
            make_tag(2, 2, 0, 4, 1),
        ];
        let plan = build_plan(1, 1, &tags, 480, 8);
        assert_eq!(plan.spans.len(), 2);
    }

    #[test]
    fn test_pdu_payload_limit() {
        // Create tags that would merge if not for PDU limit
        let mut tags = Vec::new();
        // Each tag takes 4 bytes, with 0 gap
        for i in 0..10 {
            tags.push(make_tag(i as i64, 1, i * 4, 4, 1));
        }
        // Use very small effective payload (only 20 bytes → at most 5 tags)
        let plan = build_plan(1, 1, &tags, 50, 8);
        // Each span can hold at most floor((50-30)/4) = 5 tags
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
    }
}
