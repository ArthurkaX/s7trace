/// Poll cycle scheduler and executor.
///
/// Manages per-PLC polling with configurable periods and overrun behavior
/// (`skip_missed_ticks` by default per architecture doc).
use crate::engine::health::GroupHealth;
use crate::engine::storage::{self, HookState, RollupPayload, WriteDecision};
use crate::plc::client::PlcClient;
use crate::plc::PlcHandle;
use crate::types::*;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

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
    /// Set when the cycle aborted because the S7 link is down.
    ///
    /// A span-level read failure aborts the cycle but still yields a
    /// `PollResult` (so the failed cycle is recorded), which means the poll
    /// loop cannot tell "link is dead" from "cycle completed" by looking at
    /// the `Ok`/`Err` discriminant alone. This field carries that signal
    /// through so the loop can enter the reconnect path.
    pub link_failure: Option<PlcError>,
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
        let (vi, vf) = crate::codec::decode_value(tag_data, tag.data_type, tag.bit_offset);
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
        let _span = tracing::info_span!(
            "poll_cycle",
            plc = %self.plc_name,
            group = self.period_ms,
            seq
        )
        .entered();

        let mut samples = Vec::new();
        let mut spans: Vec<SpanResult> = Vec::with_capacity(self.plan.spans.len());
        let mut rollups: Vec<RollupRecord> = Vec::new();
        let mut state_intervals: Vec<StateIntervalRecord> = Vec::new();
        let mut event_markers: Vec<EventMarkerRecord> = Vec::new();
        let mut bad_tags: Vec<BadTagRecord> = Vec::new();

        // Clone spans to avoid borrow conflict with decode_and_store_tag
        let spans_plan = self.plan.spans.clone();

        for (idx, span) in spans_plan.iter().enumerate() {
            let span_id = idx as i64 + 1;

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
                                "PLC {}: read span DB{} offset={} len={} failed: {}",
                                self.plc_name, span.db_num, span.byte_offset, span.byte_len, e
                            );
                            spans.push(SpanResult {
                                span_id,
                                db_num: span.db_num,
                                byte_offset: span.byte_offset,
                                byte_len: span.byte_len,
                                ok: false,
                            });
                            if !e.is_link_failure() {
                                self.localize_span_failure(client, span, &mut bad_tags);
                            }
                            return Ok(self.aborted_cycle(
                                seq,
                                started,
                                started_ts,
                                e,
                                samples,
                                spans,
                                rollups,
                                state_intervals,
                                bad_tags,
                                event_markers,
                            ));
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
                            "PLC {}: multi-read span for DB{} failed: {}",
                            self.plc_name, span.db_num, e
                        );
                        spans.push(SpanResult {
                            span_id,
                            db_num: span.db_num,
                            byte_offset: span.byte_offset,
                            byte_len: span.byte_len,
                            ok: false,
                        });
                        if !e.is_link_failure() {
                            self.localize_span_failure(client, span, &mut bad_tags);
                        }
                        return Ok(self.aborted_cycle(
                            seq,
                            started,
                            started_ts,
                            e,
                            samples,
                            spans,
                            rollups,
                            state_intervals,
                            bad_tags,
                            event_markers,
                        ));
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
            // A marginal overrun (a few ms over) on a sub-second period would
            // WARN every single cycle and drown the log. Warn on the first one,
            // then only periodically (~every 60th cycle), or immediately when a
            // cycle runs long enough (>2x target) to actually skip a tick.
            // Everything in between is DEBUG. The running total is in /status.
            let severe = actual_ms > self.period_ms as f64 * 2.0;
            if severe || self.overrun_count == 1 || self.overrun_count.is_multiple_of(60) {
                warn!(
                    "PLC {}: poll overrun (actual={:.1}ms, target={}ms, seq={}, total_overruns={})",
                    self.plc_name, actual_ms, self.period_ms, seq, self.overrun_count,
                );
            } else {
                debug!(
                    "PLC {}: poll overrun (actual={:.1}ms, target={}ms, seq={})",
                    self.plc_name, actual_ms, self.period_ms, seq,
                );
            }
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
            link_failure: None,
        })
    }

    /// Probe each tag of a failed span with an individual read, so a span
    /// failure caused by one bad address is attributed to that address rather
    /// than to the whole span.
    ///
    /// Only meaningful for address- or content-level failures. Callers must
    /// skip this when the link itself is down (see `PlcError::is_link_failure`).
    fn localize_span_failure(
        &self,
        client: &mut dyn PlcClient,
        span: &ReadSpan,
        bad_tags: &mut Vec<BadTagRecord>,
    ) {
        for &tag_id in &span.tag_ids {
            let Some(tag) = self.tags.get(&tag_id).cloned() else {
                continue;
            };
            match client.read_db(tag.db_num, tag.byte_offset, tag.byte_size) {
                Ok(_) => debug!(
                    "PLC {}: tag {} ({}) reads OK on its own",
                    self.plc_name, tag.name, tag.address
                ),
                Err(tag_err) => {
                    let bad_class = tag_err.bad_class();
                    let detail = tag_err.detail().to_string();
                    warn!(
                        "PLC {}: tag {} ({}) localized bad: {}: {}",
                        self.plc_name, tag.name, tag.address, bad_class, detail
                    );
                    bad_tags.push(BadTagRecord {
                        tag_id,
                        bad_class,
                        detail,
                    });
                }
            }
        }
    }

    /// Build the `PollResult` for a cycle aborted by a span read failure.
    ///
    /// The cycle is still reported (so the failure lands in `poll_cycle`),
    /// but `link_failure` is set for link-level errors so the poll loop
    /// reconnects instead of spinning through cycles that cannot succeed.
    #[allow(clippy::too_many_arguments)]
    fn aborted_cycle(
        &mut self,
        seq: u64,
        started: Instant,
        started_ts: TimestampUs,
        err: PlcError,
        samples: Vec<SampleRecord>,
        spans: Vec<SpanResult>,
        rollups: Vec<RollupRecord>,
        state_intervals: Vec<StateIntervalRecord>,
        bad_tags: Vec<BadTagRecord>,
        event_markers: Vec<EventMarkerRecord>,
    ) -> PollResult {
        let actual_ms = started.elapsed().as_secs_f64() * 1000.0;
        self.last_actual_ms = actual_ms;
        let link_failure = err.is_link_failure().then(|| err.clone());
        PollResult {
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
                error: Some(err.to_string()),
            },
            samples,
            spans,
            rollups,
            state_intervals,
            bad_tags,
            event_markers,
            link_failure,
        }
    }
}

/// Poller handle that runs in its own thread.
pub struct PollerHandle {
    pub plc_id: i64,
    pub running: Arc<Mutex<bool>>,
    /// Liveness of the thread this handle spawned.
    ///
    /// `running` is intent — what the operator asked for. `health.is_alive()`
    /// is fact — whether a thread is still there to honor it. The 22-day
    /// outage lived exactly in the gap between the two: every poller had
    /// exited, and nothing compared the two flags to notice.
    pub health: Arc<GroupHealth>,
}

impl PollerHandle {
    pub fn stop(&self) {
        let mut running = self.running.lock();
        *running = false;
    }

    /// True when this poller is wanted but its thread is gone — the condition
    /// the supervisor respawns on.
    pub fn is_dead(&self) -> bool {
        *self.running.lock() && !self.health.is_alive()
    }
}

/// Marks a poll group's health record dead when the poller thread leaves its
/// loop. A drop guard rather than a call at the end of the thread body, so a
/// panic unwinding out of the loop is reported the same as a clean exit —
/// a silently panicking poller is the failure mode with no other symptom.
///
/// Carries the generation it was started under so an outgoing poller, which
/// may not notice `stop()` until its next tick, cannot mark the replacement
/// that has already taken its place as dead.
struct AliveGuard(Arc<GroupHealth>, u64);

impl Drop for AliveGuard {
    fn drop(&mut self) {
        self.0.mark_stopped(self.1);
    }
}

/// Capped exponential backoff (in milliseconds) for the Nth consecutive
/// reconnect attempt (1-based). Without this, a poller whose PLC has dropped
/// re-attempts `connect()` every cycle with no delay, hammering the device and
/// starving other work. Doubles per attempt up to a 10s ceiling.
fn reconnect_backoff_ms(attempt: u32) -> u64 {
    const BASE_MS: u64 = 500;
    const CAP_MS: u64 = 10_000;
    let exp = attempt.saturating_sub(1).min(5); // 2^5 = 32x
    BASE_MS.saturating_mul(1u64 << exp).min(CAP_MS)
}

/// Small non-negative jitter (0..=base/4 ms) derived from the wall clock, to
/// avoid synchronized reconnect storms across pollers without pulling in a RNG.
fn reconnect_jitter_ms(base_ms: u64) -> u64 {
    let span = base_ms / 4 + 1;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    nanos % span
}

/// Spawn a polling thread for a PLC.
pub fn spawn_poller(
    plc_id: i64,
    plc_name: String,
    period_ms: u64,
    plan: ReadPlan,
    tags: Vec<TagRow>,
    client: PlcHandle,
    sample_tx: Sender<PollResult>,
    overrun_mode: OverrunMode,
    health: Arc<GroupHealth>,
) -> PollerHandle {
    let running = Arc::new(Mutex::new(true));
    let thread_running = running.clone();
    let thread_health = health.clone();
    // Claimed here, not inside the thread: the handle returned below is
    // published to the registry immediately, and a supervisor tick landing
    // before the thread got scheduled would otherwise read it as dead.
    let generation = health.mark_started();

    std::thread::Builder::new()
        .name(format!("poller-{}", plc_name))
        .spawn(move || {
            // Marks the group dead on any exit from this thread, including a
            // panic. Must outlive the loop below.
            let _alive = AliveGuard(thread_health, generation);

            // Each poller drives its own handle to the per-PLC actor at a
            // priority equal to its poll period (lower period = more urgent).
            // The actor serializes the actual reads but interleaves them at
            // single-read granularity, so a slow group can no longer block a
            // fast one for a whole cycle (see `plc::actor`).
            let mut plc_client = client.with_priority(period_ms.max(1) as u32);
            info!("poller started for {} (period={}ms)", plc_name, period_ms);

            let mut poller = PlcPoller::new(plc_id, &plc_name, plan, period_ms, overrun_mode);
            poller.init_tags(&tags);

            // Consecutive reconnect attempts, for capped exponential backoff.
            let mut reconnect_attempts: u32 = 0;

            while *thread_running.lock() {
                let cycle_start = Instant::now();
                let result = poller.poll_once(&mut plc_client);

                // A dead S7 link surfaces two different ways: as `Err` when the
                // cycle could not start, and as `Ok` with `link_failure` set
                // when a span read aborted it. Both must reach the reconnect
                // path below — treating only `Err` as a link failure is what
                // let a disconnected PLC poll forever without reconnecting.
                let link_error: Option<PlcError> = match result {
                    Ok(r) => {
                        let link_failure = r.link_failure.clone();
                        if link_failure.is_none() {
                            // Only a cycle that actually reached the PLC clears
                            // the backoff. Clearing it on any `Ok` would also
                            // clear it for cycles that failed on the first span.
                            reconnect_attempts = 0;
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
                        }
                        if sample_tx.send(r).is_err() {
                            // The receiving end is the sample-writer. If it is
                            // gone, this poller cannot store anything and no
                            // amount of retrying will help — exiting is correct,
                            // but it means collection has stopped, so it is an
                            // error and the supervisor must hear about it.
                            error!(
                                "poller-{}: sample channel closed (sample-writer gone) — \
                                 collection stopped for this group",
                                plc_name
                            );
                            break;
                        }
                        link_failure
                    }
                    Err(e) => {
                        error!("PLC {}: poll cycle failed: {}", plc_name, e);
                        e.is_link_failure().then_some(e)
                    }
                };

                if let Some(e) = link_error {
                    reconnect_attempts = reconnect_attempts.saturating_add(1);
                    // Back off before retrying so an unreachable PLC is
                    // not hammered with a connect() every cycle.
                    let base = reconnect_backoff_ms(reconnect_attempts);
                    let delay = Duration::from_millis(base + reconnect_jitter_ms(base));
                    warn!(
                        "poller-{}: link down ({}), reconnect attempt {} in {:?}",
                        plc_name, e, reconnect_attempts, delay
                    );
                    std::thread::sleep(delay);
                    if !*thread_running.lock() {
                        break;
                    }
                    match plc_client.connect() {
                        Ok(()) => {
                            info!("poller-{}: reconnected OK", plc_name);
                            reconnect_attempts = 0;
                        }
                        Err(e2) => error!(
                            "poller-{}: reconnect failed (attempt {}): {}",
                            plc_name, reconnect_attempts, e2
                        ),
                    }
                    continue;
                }

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

    PollerHandle {
        plc_id,
        running,
        health,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::plan::build_plan;
    use crate::plc::snap7::MockPlcClient;

    #[test]
    fn test_reconnect_backoff_is_capped_and_monotonic() {
        assert_eq!(reconnect_backoff_ms(1), 500);
        assert_eq!(reconnect_backoff_ms(2), 1000);
        assert_eq!(reconnect_backoff_ms(3), 2000);
        assert_eq!(reconnect_backoff_ms(6), 10_000); // capped
        assert_eq!(reconnect_backoff_ms(100), 10_000);
        // non-decreasing
        let mut prev = 0;
        for a in 1..20 {
            let cur = reconnect_backoff_ms(a);
            assert!(cur >= prev, "backoff decreased at {a}");
            prev = cur;
        }
        // jitter stays within base/4
        let base = 1000;
        assert!(reconnect_jitter_ms(base) <= base / 4);
    }

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

    /// One tag, so the plan is a single BlockRead span.
    fn single_tag() -> Vec<TagRow> {
        vec![TagRow {
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
        }]
    }

    fn poller_for(tags: &[TagRow]) -> PlcPoller {
        let plan = build_plan(1, 1, tags, 480, 8);
        let mut poller = PlcPoller::new(1, "test", plan, 100, OverrunMode::SkipMissedTicks);
        poller.init_tags(tags);
        poller
    }

    /// A span read failing with `Disconnected` must be reported as a link
    /// failure. Without this the poll loop sees a plain `Ok`, never enters
    /// backoff/reconnect, and polls a dead PLC indefinitely.
    #[test]
    fn test_disconnected_span_marks_link_failure() {
        let mut plc = MockPlcClient::new("test");
        plc.connect().unwrap();
        plc.fail_reads_with(PlcError::Disconnected("read_db: not connected".into()));

        let tags = single_tag();
        let result = poller_for(&tags).poll_once(&mut plc).unwrap();

        assert!(!result.cycle_info.ok);
        assert!(
            matches!(result.link_failure, Some(PlcError::Disconnected(_))),
            "expected link_failure to be set, got {:?}",
            result.link_failure
        );
        // Probing tags one by one over a dead link tells us nothing, so the
        // cycle must not mark them bad.
        assert!(result.bad_tags.is_empty());
    }

    /// An address-level failure is not a link failure: the cycle localizes it
    /// to the offending tag and the poll loop keeps its schedule.
    #[test]
    fn test_address_failure_localizes_without_link_failure() {
        let mut plc = MockPlcClient::new("test");
        plc.connect().unwrap();
        plc.fail_reads_with(PlcError::AddressOutOfRange {
            address: "DB1.DBW0".into(),
            detail: "beyond DB length".into(),
        });

        let tags = single_tag();
        let result = poller_for(&tags).poll_once(&mut plc).unwrap();

        assert!(!result.cycle_info.ok);
        assert!(result.link_failure.is_none());
        assert_eq!(result.bad_tags.len(), 1);
        assert_eq!(result.bad_tags[0].bad_class, BadClass::AddressOutOfRange);
    }

    #[test]
    fn test_timeout_and_unavailable_count_as_link_failures() {
        for err in [
            PlcError::Timeout("read timed out".into()),
            PlcError::PlcUnavailable("no route to host".into()),
        ] {
            let mut plc = MockPlcClient::new("test");
            plc.connect().unwrap();
            plc.fail_reads_with(err.clone());

            let tags = single_tag();
            let result = poller_for(&tags).poll_once(&mut plc).unwrap();
            assert!(
                result.link_failure.is_some(),
                "{err} should be a link failure"
            );
            assert!(result.bad_tags.is_empty());
        }
    }

    /// The exact shape of the 22-day outage: the sample-writer disappears, the
    /// poller exits because it has nowhere to send, and nobody asked it to
    /// stop. The handle must report that gap so the supervisor can act on it.
    #[test]
    fn a_poller_that_loses_its_writer_is_reported_dead() {
        use crate::engine::health::CollectorHealth;
        use crate::plc::spawn_actor;

        let mut mock = MockPlcClient::new("test");
        mock.connect().unwrap();
        mock.seed(1, 0, vec![0x00, 0x2A]);
        let client = spawn_actor("test".into(), Arc::new(Mutex::new(mock)));

        let tags = single_tag();
        let plan = build_plan(1, 1, &tags, 480, 8);
        let health = Arc::new(CollectorHealth::new());
        let group = health.group(1, 1, "test", 20);

        let (tx, rx) = std::sync::mpsc::channel();
        let poller = spawn_poller(
            1,
            "test".into(),
            20,
            plan,
            tags,
            client.clone(),
            tx,
            OverrunMode::SkipMissedTicks,
            group,
        );

        // A running poller is alive and not in need of supervision.
        assert!(
            rx.recv_timeout(Duration::from_secs(5)).is_ok(),
            "poller produced no cycle"
        );
        assert!(poller.health.is_alive());
        assert!(!poller.is_dead());

        // The writer goes away.
        drop(rx);

        let deadline = Instant::now() + Duration::from_secs(5);
        while poller.health.is_alive() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(
            !poller.health.is_alive(),
            "poller thread exited but is still reported alive"
        );
        // Still wanted, no longer running: this is what the supervisor
        // restarts. Before it existed, this state was invisible.
        assert!(poller.is_dead());

        client.stop();
    }

    /// A poller that was asked to stop is not a fault, and must not be
    /// restarted by the supervisor.
    #[test]
    fn a_stopped_poller_is_not_reported_dead() {
        use crate::engine::health::CollectorHealth;
        use crate::plc::spawn_actor;

        let mut mock = MockPlcClient::new("test");
        mock.connect().unwrap();
        mock.seed(1, 0, vec![0x00, 0x2A]);
        let client = spawn_actor("test".into(), Arc::new(Mutex::new(mock)));

        let tags = single_tag();
        let plan = build_plan(1, 1, &tags, 480, 8);
        let health = Arc::new(CollectorHealth::new());
        let group = health.group(1, 1, "test", 20);

        let (tx, rx) = std::sync::mpsc::channel();
        let poller = spawn_poller(
            1,
            "test".into(),
            20,
            plan,
            tags,
            client.clone(),
            tx,
            OverrunMode::SkipMissedTicks,
            group,
        );
        assert!(rx.recv_timeout(Duration::from_secs(5)).is_ok());

        poller.stop();
        let deadline = Instant::now() + Duration::from_secs(5);
        while poller.health.is_alive() && Instant::now() < deadline {
            let _ = rx.try_recv();
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(!poller.health.is_alive());
        assert!(!poller.is_dead(), "a stopped poller must not be respawned");

        client.stop();
    }
}
