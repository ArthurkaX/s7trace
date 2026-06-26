/// Poll cycle scheduler and executor.
///
/// Manages per-PLC polling with configurable periods and overrun behavior
/// (`skip_missed_ticks` by default per architecture doc).
use crate::engine::storage::{self, HookState, RollupPayload, WriteDecision};
use crate::plc::client::PlcClient;
use crate::types::*;
use log::{debug, error, info, warn};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Overrun behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverrunMode {
    /// Skip missed ticks to avoid backlog.
    SkipMissedTicks,
    /// Report overrun but keep polling on schedule.
    Strict,
}

/// Per-PLC poller state.
pub struct PlcPoller {
    pub plc_id: i64,
    pub plc_name: String,
    pub plan: ReadPlan,
    pub period_ms: u64,
    pub overrun_mode: OverrunMode,
    pub overrun_count: u64,
    pub last_actual_ms: f64,
    pub degraded: bool,
    // Internal
    seq: u64,
    tags: HashMap<i64, TagRow>,          // tag_id -> tag metadata
    tag_states: HashMap<i64, HookState>, // tag_id -> hook state
}

/// Result of a single poll cycle.
#[derive(Debug, Clone)]
pub struct PollResult {
    pub plc_id: i64,
    pub poll_group_id: i64,
    pub seq: u64,
    pub cycle_info: PollCycleInfo,
    pub samples: Vec<SampleRecord>,
    /// Physical read spans: one per S7 read, with the cycle-local span id
    /// used to populate `sample.span_id`.
    pub spans: Vec<SpanResult>,
    /// Rollup rows for tags whose storage hook decided to emit a window.
    pub rollups: Vec<RollupRecord>,
    /// State transitions accumulated during this cycle.
    pub state_intervals: Vec<StateIntervalRecord>,
    /// Tags that failed localized single reads after a span-level failure
    /// (per architecture.md: localize failures to individual tags).
    pub bad_tags: Vec<BadTagRecord>,
    /// Derived event markers (e.g. negative counter delta = wraparound
    /// that exceeded the configured `wrap_at`).
    pub event_markers: Vec<EventMarkerRecord>,
}

#[derive(Debug, Clone)]
pub struct EventMarkerRecord {
    pub tag_id: i64,
    pub ts: TimestampUs,
    pub kind: String,
    pub severity: String,
    pub value_i: Option<i64>,
    pub details_json: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RollupRecord {
    pub tag_id: i64,
    pub payload: RollupPayload,
}

#[derive(Debug, Clone)]
pub struct StateIntervalRecord {
    pub tag_id: i64,
    pub state: i64,
    pub start_ts: TimestampUs,
    pub end_ts: TimestampUs,
    pub duration_ms: i64,
}

#[derive(Debug, Clone)]
pub struct BadTagRecord {
    pub tag_id: i64,
    pub bad_class: BadClass,
    pub detail: String,
}

#[derive(Debug, Clone)]
pub struct SpanResult {
    /// Cycle-local 1-based span id (also written to `read_span.seq`).
    pub span_id: i64,
    pub db_num: u16,
    pub byte_offset: usize,
    pub byte_len: usize,
    pub ok: bool,
}

#[derive(Debug, Clone)]
pub struct SampleRecord {
    pub tag_id: i64,
    pub span_id: Option<i64>,
    pub vi: Option<i64>,
    pub vf: Option<f64>,
    pub ts: TimestampUs,
}

impl PlcPoller {
    pub fn new(
        plc_id: i64,
        plc_name: &str,
        plan: ReadPlan,
        period_ms: u64,
        overrun_mode: OverrunMode,
    ) -> Self {
        PlcPoller {
            plc_id,
            plc_name: plc_name.to_string(),
            plan,
            period_ms,
            overrun_mode,
            overrun_count: 0,
            last_actual_ms: 0.0,
            degraded: false,
            seq: 0,
            tags: HashMap::new(),
            tag_states: HashMap::new(),
        }
    }

    pub fn init_tags(&mut self, tags: &[TagRow]) {
        for tag in tags {
            let hook = tag.storage_hook;
            let params = tag.hook_params_json.as_deref();
            self.tag_states
                .entry(tag.id)
                .or_insert_with(|| storage::init_hook_state(hook, params));
            self.tags.entry(tag.id).or_insert_with(|| tag.clone());
        }
    }

    fn find_tag_in_plan(&self, tag_id: i64) -> Option<&TagRow> {
        self.tags.get(&tag_id)
    }

    /// Decode a single tag's value from raw bytes, evaluate storage hook,
    /// and update all output collections. Shared by block-read and multi-read paths.
    fn decode_and_store_tag(
        &mut self,
        tag: &TagRow,
        tag_data: &[u8],
        span_id: i64,
        samples: &mut Vec<SampleRecord>,
        rollups: &mut Vec<RollupRecord>,
        state_intervals: &mut Vec<StateIntervalRecord>,
        event_markers: &mut Vec<EventMarkerRecord>,
    ) {
        let (vi, vf) = storage::decode_value(tag_data, tag.data_type, tag.bit_offset);
        let ts = now_us();

        let prev_counter: Option<i64> =
            if let Some(HookState::CounterDelta(s)) = self.tag_states.get(&tag.id) {
                s.last_value
            } else {
                None
            };

        let decision = if let Some(state) = self.tag_states.get_mut(&tag.id) {
            storage::evaluate(state, vi, vf, ts)
        } else {
            WriteDecision {
                should_write: true,
                should_write_rollup: false,
                vi,
                vf,
                rollup: None,
            }
        };

        if decision.should_write {
            samples.push(SampleRecord {
                tag_id: tag.id,
                span_id: Some(span_id),
                vi: decision.vi,
                vf: decision.vf,
                ts,
            });
        }
        if decision.should_write_rollup {
            if let Some(payload) = decision.rollup {
                rollups.push(RollupRecord {
                    tag_id: tag.id,
                    payload,
                });
            }
        }

        if let (Some(curr), Some(prev)) = (vi, prev_counter) {
            if curr < prev {
                let details = serde_json::json!({
                    "prev": prev,
                    "curr": curr,
                    "delta": decision.vi,
                });
                event_markers.push(EventMarkerRecord {
                    tag_id: tag.id,
                    ts,
                    kind: "missing_increment".into(),
                    severity: "warn".into(),
                    value_i: decision.vi,
                    details_json: Some(details.to_string()),
                });
            }
        }

        if let Some(HookState::StateDuration(sd_state)) = self.tag_states.get_mut(&tag.id) {
            let drained = storage::drain_state_durations(sd_state);
            for iv in drained {
                state_intervals.push(StateIntervalRecord {
                    tag_id: tag.id,
                    state: iv.state,
                    start_ts: iv.start_ts,
                    end_ts: iv.end_ts,
                    duration_ms: iv.duration_ms,
                });
            }
        }
    }

    /// Execute one poll cycle against the given PLC client.
    pub fn poll_once(&mut self, client: &mut dyn PlcClient) -> Result<PollResult, PlcError> {
        let started_ts = now_us();
        let started = Instant::now();

        self.seq += 1;
        let seq = self.seq;

        let mut span_id_counter: i64 = 0;
        let mut samples = Vec::new();
        let mut spans: Vec<SpanResult> = Vec::with_capacity(self.plan.spans.len());
        let mut rollups: Vec<RollupRecord> = Vec::new();
        let mut state_intervals: Vec<StateIntervalRecord> = Vec::new();
        let mut event_markers: Vec<EventMarkerRecord> = Vec::new();
        let mut bad_tags: Vec<BadTagRecord> = Vec::new();

        // Clone spans to avoid borrow conflict with decode_and_store_tag
        let spans_plan = self.plan.spans.clone();

        for span in &spans_plan {
            span_id_counter += 1;
            let span_id = span_id_counter;

            match span.kind {
                SpanKind::BlockRead => {
                    match client.read_db(span.db_num, span.byte_offset, span.byte_len) {
                        Ok(data) => {
                            spans.push(SpanResult {
                                span_id,
                                db_num: span.db_num,
                                byte_offset: span.byte_offset,
                                byte_len: span.byte_len,
                                ok: true,
                            });
                            for &tag_id in &span.tag_ids {
                                if let Some(tag) = self.find_tag_in_plan(tag_id).cloned() {
                                    let offset_in_span = tag.byte_offset - span.byte_offset;
                                    let end = offset_in_span + tag.byte_size;
                                    if end > data.len() {
                                        warn!(
                                            "tag {} ({}): span data too short (need {}..{}, got {} bytes)",
                                            tag.name, tag.address, offset_in_span, end, data.len()
                                        );
                                        continue;
                                    }
                                    self.decode_and_store_tag(
                                        &tag,
                                        &data[offset_in_span..end],
                                        span_id,
                                        &mut samples,
                                        &mut rollups,
                                        &mut state_intervals,
                                        &mut event_markers,
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            error!(
                                "PLC {}: read span DB{} offset={} len={} failed: {}; localizing",
                                self.plc_name, span.db_num, span.byte_offset, span.byte_len, e
                            );
                            spans.push(SpanResult {
                                span_id,
                                db_num: span.db_num,
                                byte_offset: span.byte_offset,
                                byte_len: span.byte_len,
                                ok: false,
                            });
                            for &tag_id in &span.tag_ids {
                                if let Some(tag) = self.tags.get(&tag_id).cloned() {
                                    match client.read_db(
                                        tag.db_num,
                                        tag.byte_offset,
                                        tag.byte_size,
                                    ) {
                                        Ok(_) => {
                                            debug!(
                                                "PLC {}: tag {} ({}) reads OK on its own",
                                                self.plc_name, tag.name, tag.address
                                            );
                                        }
                                        Err(tag_err) => {
                                            let bc = tag_err.bad_class();
                                            let det = tag_err.detail().to_string();
                                            warn!(
                                                "PLC {}: tag {} ({}) localized bad: {}: {}",
                                                self.plc_name, tag.name, tag.address, bc, det
                                            );
                                            bad_tags.push(BadTagRecord {
                                                tag_id,
                                                bad_class: bc,
                                                detail: det,
                                            });
                                        }
                                    }
                                }
                            }
                            let elapsed = started.elapsed();
                            let actual_ms = elapsed.as_secs_f64() * 1000.0;
                            self.last_actual_ms = actual_ms;
                            return Ok(PollResult {
                                plc_id: self.plc_id,
                                poll_group_id: self.plan.poll_group_id,
                                seq,
                                cycle_info: PollCycleInfo {
                                    plc_id: self.plc_id,
                                    poll_group_id: self.plan.poll_group_id,
                                    seq,
                                    started_ts,
                                    finished_ts: now_us(),
                                    target_ms: self.period_ms,
                                    actual_ms,
                                    ok: false,
                                    error: Some(e.to_string()),
                                },
                                samples,
                                spans,
                                rollups,
                                state_intervals,
                                bad_tags,
                                event_markers,
                            });
                        }
                    }
                }
                SpanKind::MultiRead => match client.read_multi(&span.multi_items) {
                    Ok(results) => {
                        spans.push(SpanResult {
                            span_id,
                            db_num: span.db_num,
                            byte_offset: span.byte_offset,
                            byte_len: span.byte_len,
                            ok: true,
                        });
                        for (item_idx, result) in results.iter().enumerate() {
                            let item = &span.multi_items[item_idx];
                            for &tag_id in &span.tag_ids {
                                if let Some(tag) = self.find_tag_in_plan(tag_id).cloned() {
                                    let tag_end = tag.byte_offset + tag.byte_size;
                                    let item_end = item.start + item.len;
                                    if tag.byte_offset >= item.start && tag_end <= item_end {
                                        let offset_in_item = tag.byte_offset - item.start;
                                        let end = offset_in_item + tag.byte_size;
                                        if end > result.data.len() {
                                            warn!(
                                                "tag {} ({}): multi-read data too short",
                                                tag.name, tag.address
                                            );
                                            continue;
                                        }
                                        self.decode_and_store_tag(
                                            &tag,
                                            &result.data[offset_in_item..end],
                                            span_id,
                                            &mut samples,
                                            &mut rollups,
                                            &mut state_intervals,
                                            &mut event_markers,
                                        );
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            "PLC {}: multi-read span for DB{} failed: {}; localizing",
                            self.plc_name, span.db_num, e
                        );
                        spans.push(SpanResult {
                            span_id,
                            db_num: span.db_num,
                            byte_offset: span.byte_offset,
                            byte_len: span.byte_len,
                            ok: false,
                        });
                        for &tag_id in &span.tag_ids {
                            if let Some(tag) = self.tags.get(&tag_id).cloned() {
                                match client.read_db(
                                    tag.db_num,
                                    tag.byte_offset,
                                    tag.byte_size,
                                ) {
                                    Ok(_) => {
                                        debug!(
                                            "PLC {}: tag {} ({}) reads OK on its own",
                                            self.plc_name, tag.name, tag.address
                                        );
                                    }
                                    Err(tag_err) => {
                                        let bc = tag_err.bad_class();
                                        let det = tag_err.detail().to_string();
                                        warn!(
                                            "PLC {}: tag {} ({}) localized bad: {}: {}",
                                            self.plc_name, tag.name, tag.address, bc, det
                                        );
                                        bad_tags.push(BadTagRecord {
                                            tag_id,
                                            bad_class: bc,
                                            detail: det,
                                        });
                                    }
                                }
                            }
                        }
                        let elapsed = started.elapsed();
                        let actual_ms = elapsed.as_secs_f64() * 1000.0;
                        self.last_actual_ms = actual_ms;
                        return Ok(PollResult {
                            plc_id: self.plc_id,
                            poll_group_id: self.plan.poll_group_id,
                            seq,
                            cycle_info: PollCycleInfo {
                                plc_id: self.plc_id,
                                poll_group_id: self.plan.poll_group_id,
                                seq,
                                started_ts,
                                finished_ts: now_us(),
                                target_ms: self.period_ms,
                                actual_ms,
                                ok: false,
                                error: Some(e.to_string()),
                            },
                            samples,
                            spans,
                            rollups,
                            state_intervals,
                            bad_tags,
                            event_markers,
                        });
                    }
                },
            }
        }

        let elapsed = started.elapsed();
        let actual_ms = elapsed.as_secs_f64() * 1000.0;
        self.last_actual_ms = actual_ms;

        if actual_ms > self.period_ms as f64 {
            self.overrun_count += 1;
            self.degraded = true;
            warn!(
                "PLC {}: poll overrun (actual={:.1}ms, target={}ms, seq={})",
                self.plc_name, actual_ms, self.period_ms, seq,
            );
        } else {
            self.degraded = false;
        }

        let finished_ts = now_us();

        let cycle_info = PollCycleInfo {
            plc_id: self.plc_id,
            poll_group_id: self.plan.poll_group_id,
            seq,
            started_ts,
            finished_ts,
            target_ms: self.period_ms,
            actual_ms,
            ok: true,
            error: None,
        };

        Ok(PollResult {
            plc_id: self.plc_id,
            poll_group_id: self.plan.poll_group_id,
            seq,
            cycle_info,
            samples,
            spans,
            rollups,
            state_intervals,
            bad_tags,
            event_markers,
        })
    }
}

/// Poller handle that runs in its own thread.
pub struct PollerHandle {
    pub plc_id: i64,
    pub running: Arc<Mutex<bool>>,
}

impl PollerHandle {
    pub fn stop(&self) {
        let mut running = self.running.lock();
        *running = false;
    }
}

/// Spawn a polling thread for a PLC.
pub fn spawn_poller(
    plc_id: i64,
    plc_name: String,
    period_ms: u64,
    plan: ReadPlan,
    tags: Vec<TagRow>,
    client: Arc<Mutex<dyn PlcClient + Send>>,
    sample_tx: Sender<PollResult>,
    overrun_mode: OverrunMode,
) -> PollerHandle {
    let running = Arc::new(Mutex::new(true));
    let thread_running = running.clone();

    std::thread::Builder::new()
        .name(format!("poller-{}", plc_name))
        .spawn(move || {
            let plc_client = client;
            info!("poller started for {} (period={}ms)", plc_name, period_ms);

            let mut poller = PlcPoller::new(plc_id, &plc_name, plan, period_ms, overrun_mode);
            poller.init_tags(&tags);

            while *thread_running.lock() {
                let cycle_start = Instant::now();
                let mut inner = plc_client.lock();
                let result = poller.poll_once(&mut *inner);

                match result {
                    Ok(r) => {
                        if r.samples.is_empty() && r.spans.is_empty() {
                            warn!(
                                "poller-{}: cycle {} had empty samples+spans (decision path?)",
                                plc_name, r.seq
                            );
                        } else {
                            debug!(
                                "poller-{}: cycle {} in {:.1}ms ({} samples, {} spans)",
                                plc_name,
                                r.seq,
                                poller.last_actual_ms,
                                r.samples.len(),
                                r.spans.len()
                            );
                        }
                        if sample_tx.send(r).is_err() {
                            warn!("poller-{}: sample channel closed, stopping", plc_name);
                            break;
                        }
                    }
                    Err(e) => {
                        error!("PLC {}: poll cycle failed: {}", plc_name, e);
                        if matches!(&e, PlcError::Disconnected(_) | PlcError::Timeout(_)) {
                            warn!("poller-{}: attempting reconnect...", plc_name);
                            drop(inner);
                            let mut guard = plc_client.lock();
                            match guard.connect() {
                                Ok(()) => info!("poller-{}: reconnected OK", plc_name),
                                Err(e2) => error!("poller-{}: reconnect failed: {}", plc_name, e2),
                            }
                            drop(guard);
                            continue;
                        }
                    }
                }

                drop(inner);

                let elapsed = cycle_start.elapsed();
                let target = Duration::from_millis(period_ms);
                if elapsed < target {
                    std::thread::sleep(target - elapsed);
                } else if poller.overrun_mode == OverrunMode::SkipMissedTicks {
                    continue;
                } else {
                    std::thread::sleep(target);
                }
            }

            info!("poller stopped for {}", plc_name);
        })
        .expect("spawn poller thread");

    PollerHandle { plc_id, running }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::plan::build_plan;
    use crate::plc::snap7::MockPlcClient;

    #[test]
    fn test_poller_basic_cycle() {
        let mut plc = MockPlcClient::new("test");
        plc.connect().unwrap();

        let tag = TagRow {
            id: 1,
            plc_id: 1,
            name: "test".into(),
            address: "DB1.DBW0".into(),
            db_num: 1,
            byte_offset: 0,
            bit_offset: None,
            byte_size: 2,
            data_type: DataType::I16,
            poll_group_id: 1,
            store_mode: StoreMode::OnChange,
            storage_hook: StorageHook::OnChange,
            hook_params_json: None,
            enabled: true,
            bad: false,
            bad_class: None,
            bad_detail: None,
        };
        let tags = vec![tag];
        let plan = build_plan(1, 1, &tags, 480, 8);
        let mut poller = PlcPoller::new(1, "test", plan, 100, OverrunMode::SkipMissedTicks);
        poller.init_tags(&tags);

        plc.seed(1, 0, vec![0x00, 0x2A]);

        let result = poller.poll_once(&mut plc).unwrap();
        assert_eq!(result.seq, 1);
        assert!(result.cycle_info.ok);
        assert_eq!(result.samples.len(), 1);
    }

    #[test]
    fn test_localize_span_failure_to_bad_tag() {
        let mut plc = MockPlcClient::new("test");
        plc.connect().unwrap();
        plc.seed(1, 0, vec![0x00, 0x01]);

        let tag_a = TagRow {
            id: 10,
            plc_id: 1,
            name: "a".into(),
            address: "DB1.DBW0".into(),
            db_num: 1,
            byte_offset: 0,
            bit_offset: None,
            byte_size: 2,
            data_type: DataType::I16,
            poll_group_id: 1,
            store_mode: StoreMode::OnChange,
            storage_hook: StorageHook::OnChange,
            hook_params_json: None,
            enabled: true,
            bad: false,
            bad_class: None,
            bad_detail: None,
        };
        let tag_b = TagRow {
            id: 11,
            plc_id: 1,
            name: "b".into(),
            address: "DB1.DBW4".into(),
            db_num: 1,
            byte_offset: 4,
            bit_offset: None,
            byte_size: 2,
            data_type: DataType::I16,
            poll_group_id: 1,
            store_mode: StoreMode::OnChange,
            storage_hook: StorageHook::OnChange,
            hook_params_json: None,
            enabled: true,
            bad: false,
            bad_class: None,
            bad_detail: None,
        };
        let tags = vec![tag_a, tag_b];
        let plan = build_plan(1, 1, &tags, 480, 8);
        let mut poller = PlcPoller::new(1, "test", plan, 100, OverrunMode::SkipMissedTicks);
        poller.init_tags(&tags);

        let result = poller.poll_once(&mut plc).unwrap();
        assert!(result.cycle_info.ok);
        assert!(result.bad_tags.is_empty());
    }

    #[test]
    fn test_multi_read_poller_cycle() {
        let mut plc = MockPlcClient::new("test");
        plc.connect().unwrap();

        let tags = vec![
            TagRow {
                id: 1,
                plc_id: 1,
                name: "t1".into(),
                address: "DB1.DBW0".into(),
                db_num: 1,
                byte_offset: 0,
                bit_offset: None,
                byte_size: 2,
                data_type: DataType::I16,
                poll_group_id: 1,
                store_mode: StoreMode::OnChange,
                storage_hook: StorageHook::OnChange,
                hook_params_json: None,
                enabled: true,
                bad: false,
                bad_class: None,
                bad_detail: None,
            },
            TagRow {
                id: 2,
                plc_id: 1,
                name: "t2".into(),
                address: "DB1.DBW50".into(),
                db_num: 1,
                byte_offset: 50,
                bit_offset: None,
                byte_size: 2,
                data_type: DataType::I16,
                poll_group_id: 1,
                store_mode: StoreMode::OnChange,
                storage_hook: StorageHook::OnChange,
                hook_params_json: None,
                enabled: true,
                bad: false,
                bad_class: None,
                bad_detail: None,
            },
            TagRow {
                id: 3,
                plc_id: 1,
                name: "t3".into(),
                address: "DB1.DBW100".into(),
                db_num: 1,
                byte_offset: 100,
                bit_offset: None,
                byte_size: 2,
                data_type: DataType::I16,
                poll_group_id: 1,
                store_mode: StoreMode::OnChange,
                storage_hook: StorageHook::OnChange,
                hook_params_json: None,
                enabled: true,
                bad: false,
                bad_class: None,
                bad_detail: None,
            },
        ];

        plc.seed(1, 0, vec![0x00, 0x0A]);
        plc.seed(1, 50, vec![0x00, 0x14]);
        plc.seed(1, 100, vec![0x00, 0x1E]);

        let plan = build_plan(1, 1, &tags, 480, 8);
        assert_eq!(plan.spans.len(), 1);
        assert_eq!(plan.spans[0].kind, SpanKind::MultiRead);

        let mut poller = PlcPoller::new(1, "test", plan, 100, OverrunMode::SkipMissedTicks);
        poller.init_tags(&tags);

        let result = poller.poll_once(&mut plc).unwrap();
        assert!(result.cycle_info.ok);
        assert_eq!(result.samples.len(), 3);
        assert!(result.bad_tags.is_empty());
    }
}
