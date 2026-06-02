/// Poll cycle scheduler and executor.
///
/// Manages per-PLC polling with configurable periods and overrun behavior
/// (`skip_missed_ticks` by default per architecture doc).

 use crate::types::ReadPlan;
use crate::engine::snapshot::Snapshot;
use crate::engine::storage::{self, HookState, WriteDecision};
use crate::plc::client::PlcClient;
use crate::types::*;
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::Mutex;

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
    tags: HashMap<i64, TagRow>,        // tag_id -> tag metadata
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

    /// Ensure hook state exists for all tags in the plan, and store tag metadata.
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

    /// Execute one poll cycle against the given PLC client.
    ///
    /// Returns `None` if the cycle should be skipped due to overrun.
    pub fn poll_once(
        &mut self,
        client: &mut dyn PlcClient,
        snapshot: &Snapshot,
    ) -> Result<PollResult, PlcError> {
        let started_ts = now_us();
        let started = Instant::now();

        self.seq += 1;
        let seq = self.seq;

        // Track which tags are in each span
        let mut span_id_counter: i64 = 0;
        let mut samples = Vec::new();

        for span in &self.plan.spans {
            span_id_counter += 1;
            let span_id = span_id_counter;

            match client.read_db(span.db_num, span.byte_offset, span.byte_len) {
                Ok(data) => {
                    // Decode tags from this span
                    for &tag_id in &span.tag_ids {
                        if let Some(tag) = self.find_tag_in_plan(tag_id) {
                            let offset_in_span = tag.byte_offset - span.byte_offset;
                            let end = offset_in_span + tag.byte_size;
                            if end > data.len() {
                                warn!(
                                    "tag {} ({}): span data too short (need {}..{}, got {} bytes)",
                                    tag.name, tag.address, offset_in_span, end, data.len()
                                );
                                continue;
                            }
                            let tag_data = &data[offset_in_span..end];

                            let (vi, vf) = storage::decode_value(tag_data, tag.data_type);
                            let ts = now_us();

                            // Update snapshot
                            snapshot.update(
                                tag_id, self.plc_id, &tag.name, &tag.address,
                                tag.data_type, vi, vf, ts,
                            );

                            // Evaluate storage hook
                            let decision = if let Some(state) = self.tag_states.get_mut(&tag_id) {
                                storage::evaluate(state, vi, vf, ts)
                            } else {
                                WriteDecision { should_write: true, vi, vf }
                            };

                            if decision.should_write {
                                samples.push(SampleRecord {
                                    tag_id,
                                    span_id: Some(span_id),
                                    vi: decision.vi,
                                    vf: decision.vf,
                                    ts,
                                });
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(
                        "PLC {}: read span DB{} offset={} len={} failed: {}",
                        self.plc_name, span.db_num, span.byte_offset, span.byte_len, e
                    );
                    // Mark all tags in this span as suspect
                    return Err(e);
                }
            }
        }

        let elapsed = started.elapsed();
        let actual_ms = elapsed.as_secs_f64() * 1000.0;
        self.last_actual_ms = actual_ms;

        // Check overrun
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
    client: Arc<Mutex<dyn PlcClient + Send>>,
    snapshot: Arc<Snapshot>,
) -> PollerHandle {
    let running = Arc::new(Mutex::new(true));
    let thread_running = running.clone();

    std::thread::Builder::new()
        .name(format!("poller-{}", plc_name))
        .spawn(move || {
            info!("poller started for {} (period={}ms)", plc_name, period_ms);

            let mut poller = PlcPoller::new(plc_id, &plc_name, plan, period_ms, OverrunMode::SkipMissedTicks);

            while *thread_running.lock() {
                let cycle_start = Instant::now();
                let mut client = client.lock();
                let result = poller.poll_once(&mut *client, &snapshot);

                match result {
                    Ok(r) => {
                        debug!(
                            "PLC {}: cycle {} completed in {:.1}ms ({} samples)",
                            plc_name, r.seq, poller.last_actual_ms, r.samples.len()
                        );
                    }
                    Err(e) => {
                        error!("PLC {}: poll cycle failed: {}", plc_name, e);
                    }
                }

                drop(client);

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
    }
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
        let snapshot = Snapshot::new();

        plc.seed(1, 0, vec![0x00, 0x2A]);

        let result = poller.poll_once(&mut plc, &snapshot).unwrap();
        assert_eq!(result.seq, 1);
        assert!(result.cycle_info.ok);

        // Should have produced a sample from decoding
        let tag_snap = snapshot.get_by_id(1).unwrap();
        assert_eq!(tag_snap.last_value_i, Some(42));
    }
}
