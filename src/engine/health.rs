//! Collector liveness and runtime metrics, held in memory.
//!
//! Two field failures motivate this module.
//!
//! The first: on 2026-07-19 the sample-writer stopped and every poller exited
//! behind it, yet `/v1/status` kept reporting both PLCs as `online` for 22
//! days. Everything `/status` knew came from the `plc` table, which is only
//! ever written *by* the collector — so a dead collector froze the status
//! rather than changing it. Liveness has to be tracked by something that stops
//! being updated when collection stops, and read from memory rather than from
//! the last row the collector managed to write.
//!
//! The second: `/v1/status` took 12 s because it derived its runtime numbers
//! from `poll_cycle` — six unindexed scans of a 12.7M-row table per request.
//! Those numbers are produced by this process; counting them in memory as they
//! happen is both cheaper and more current than re-deriving them from storage.
//!
//! Everything here is written by the sample-writer and the poller threads, and
//! read by the HTTP API, so all state is atomics or short-lived locks.

use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

use crate::types::now_us;

/// Ordering used throughout: these counters are independent of each other and
/// nothing branches on their relative order, so `Relaxed` is sufficient.
const ORD: Ordering = Ordering::Relaxed;

/// Minimum staleness threshold, whatever the poll period.
///
/// A 50 ms group would otherwise be called stale 150 ms after a cycle, which
/// no status query could ever win against.
const MIN_STALE_AFTER_MS: u64 = 5_000;

/// How many poll periods may pass before a group counts as stale.
const STALE_PERIODS: u64 = 3;

// ---------------------------------------------------------------------------
// Per-poll-group health
// ---------------------------------------------------------------------------

/// Liveness and recent behavior of one (PLC, poll group) collector.
///
/// Created when a poller is spawned and kept in the registry after it exits —
/// a group that vanished from the map would be indistinguishable from one that
/// was never configured, and "the poller is gone" is precisely the state that
/// went unnoticed in the field.
pub struct GroupHealth {
    pub plc_id: i64,
    pub poll_group_id: i64,
    pub plc_name: String,
    /// Target period, used to scale the staleness threshold.
    pub period_ms: u64,

    /// False once the poller thread has left its loop, for any reason
    /// including a panic. Set by a drop guard inside the thread.
    alive: AtomicBool,
    /// Incremented every time a thread claims this record. A poller replaced
    /// by a config apply keeps running until its next tick, so its exit can
    /// land *after* its replacement has started; the generation lets the drop
    /// guard tell "I am the current poller, mark the group dead" from "I have
    /// already been superseded, leave it alone".
    generation: AtomicU64,
    /// Wall-clock time the poller thread last started running.
    started_ts: AtomicI64,
    /// How many times the supervisor has respawned this group.
    restarts: AtomicU64,

    /// Last cycle observed by the writer, successful or not.
    last_cycle_ts: AtomicI64,
    /// Last cycle that reached the PLC and returned data.
    last_ok_ts: AtomicI64,
    cycles_ok: AtomicU64,
    cycles_failed: AtomicU64,
    last_error: Mutex<Option<String>>,
    /// Rolling hour of this group's cycle timings. Held per group rather than
    /// once per process so `/status` can report each poll group's own latency
    /// and overrun count — the numbers it used to get from a `GROUP BY` over
    /// the whole `poll_cycle` table.
    latency: Mutex<LatencyWindow>,
}

impl GroupHealth {
    fn new(plc_id: i64, poll_group_id: i64, plc_name: &str, period_ms: u64) -> Self {
        GroupHealth {
            plc_id,
            poll_group_id,
            plc_name: plc_name.to_string(),
            period_ms,
            alive: AtomicBool::new(false),
            generation: AtomicU64::new(0),
            started_ts: AtomicI64::new(0),
            restarts: AtomicU64::new(0),
            last_cycle_ts: AtomicI64::new(0),
            last_ok_ts: AtomicI64::new(0),
            cycles_ok: AtomicU64::new(0),
            cycles_failed: AtomicU64::new(0),
            last_error: Mutex::new(None),
            latency: Mutex::new(LatencyWindow::new()),
        }
    }

    /// Mark the poller thread as running, and claim this record for it.
    ///
    /// Returns the generation the caller must pass to [`mark_stopped`]. Called
    /// on the spawning thread, before the handle is published, so a supervisor
    /// tick cannot catch the group as "wanted but not yet alive".
    pub fn mark_started(&self) -> u64 {
        let generation = self.generation.fetch_add(1, ORD) + 1;
        self.alive.store(true, ORD);
        self.started_ts.store(now_us(), ORD);
        generation
    }

    /// Mark the poller thread as gone. Called from a drop guard so a panic
    /// unwinding out of the poll loop reports the same as a clean exit.
    ///
    /// Ignored if another thread has since claimed the record: an outgoing
    /// poller must not report the incoming one as dead.
    pub fn mark_stopped(&self, generation: u64) {
        if self.generation.load(ORD) == generation {
            self.alive.store(false, ORD);
        }
    }

    pub fn is_alive(&self) -> bool {
        self.alive.load(ORD)
    }

    pub fn restarts(&self) -> u64 {
        self.restarts.load(ORD)
    }

    pub fn note_restart(&self) {
        self.restarts.fetch_add(1, ORD);
    }

    pub fn started_ts(&self) -> Option<i64> {
        opt_ts(self.started_ts.load(ORD))
    }

    pub fn last_cycle_ts(&self) -> Option<i64> {
        opt_ts(self.last_cycle_ts.load(ORD))
    }

    pub fn last_ok_ts(&self) -> Option<i64> {
        opt_ts(self.last_ok_ts.load(ORD))
    }

    pub fn cycles_ok(&self) -> u64 {
        self.cycles_ok.load(ORD)
    }

    pub fn cycles_failed(&self) -> u64 {
        self.cycles_failed.load(ORD)
    }

    pub fn last_error(&self) -> Option<String> {
        self.last_error.lock().clone()
    }

    /// Record one completed poll cycle.
    fn record_cycle(&self, finished_ts: i64, ok: bool, actual_ms: f64, error: Option<&str>) {
        self.last_cycle_ts.store(finished_ts, ORD);
        if ok {
            self.last_ok_ts.store(finished_ts, ORD);
            self.cycles_ok.fetch_add(1, ORD);
            // Keep the last error visible only while it is the current state;
            // a successful cycle means whatever it was has been resolved.
            *self.last_error.lock() = None;
        } else {
            self.cycles_failed.fetch_add(1, ORD);
            if let Some(e) = error {
                *self.last_error.lock() = Some(e.to_string());
            }
        }
        self.latency.lock().record(
            finished_ts,
            ok,
            actual_ms,
            actual_ms > self.period_ms as f64,
        );
    }

    /// This group's cycle statistics over the last hour.
    pub fn last_hour(&self) -> LatencySummary {
        self.latency.lock().summary(now_us())
    }

    /// How long a group may go without a successful cycle before it is stale.
    ///
    /// Scaled to the group's own period rather than a fixed 60 s: a 50 ms group
    /// that has produced nothing for 10 s is plainly broken, while an 850 ms
    /// group is not, and one threshold cannot be right for both. The field
    /// report saw both PLCs reported `stale` while cycles were arriving
    /// normally, precisely because the fixed threshold was compared against a
    /// timestamp the collector had stopped updating.
    pub fn stale_after_us(&self) -> i64 {
        let ms = self
            .period_ms
            .saturating_mul(STALE_PERIODS)
            .max(MIN_STALE_AFTER_MS);
        (ms as i64).saturating_mul(1_000)
    }

    /// True when no successful cycle has landed within the staleness window.
    pub fn is_stale(&self, now: i64) -> bool {
        match self.last_ok_ts() {
            Some(ts) => now.saturating_sub(ts) > self.stale_after_us(),
            // Never produced a cycle. Stale only once the poller has had time
            // to produce one, so a freshly spawned group is not immediately
            // reported as broken.
            None => match self.started_ts() {
                Some(ts) => now.saturating_sub(ts) > self.stale_after_us(),
                None => true,
            },
        }
    }
}

fn opt_ts(v: i64) -> Option<i64> {
    (v != 0).then_some(v)
}

// ---------------------------------------------------------------------------
// Rolling latency window
// ---------------------------------------------------------------------------

const WINDOW_MINUTES: usize = 60;
const US_PER_MINUTE: i64 = 60_000_000;

#[derive(Clone, Copy, Default)]
struct Bucket {
    /// Epoch minute this bucket holds, so a bucket left untouched for over an
    /// hour is recognized as expired instead of read as current data.
    minute: i64,
    cycles_ok: u64,
    cycles_failed: u64,
    overruns: u64,
    latency_sum_ms: f64,
    latency_max_ms: f64,
}

/// Summary of the last hour of poll cycles.
#[derive(Clone, Copy, Default, Debug, PartialEq)]
pub struct LatencySummary {
    pub cycles_ok: u64,
    pub cycles_failed: u64,
    /// Cycles that took longer than their poll group's target period.
    pub overruns: u64,
    pub latency_avg_ms: f64,
    pub latency_max_ms: f64,
}

impl LatencySummary {
    /// Total cycles behind this summary — the weight its average carries.
    pub fn cycles(&self) -> u64 {
        self.cycles_ok + self.cycles_failed
    }

    /// Combine two summaries, weighting the averages by cycle count so that
    /// merging per-group windows yields the same number a single shared window
    /// would have produced.
    pub fn merge(self, other: LatencySummary) -> LatencySummary {
        let sum = self.latency_avg_ms * self.cycles() as f64
            + other.latency_avg_ms * other.cycles() as f64;
        let cycles = self.cycles() + other.cycles();
        LatencySummary {
            cycles_ok: self.cycles_ok + other.cycles_ok,
            cycles_failed: self.cycles_failed + other.cycles_failed,
            overruns: self.overruns + other.overruns,
            latency_avg_ms: if cycles > 0 { sum / cycles as f64 } else { 0.0 },
            latency_max_ms: self.latency_max_ms.max(other.latency_max_ms),
        }
    }
}

/// One hour of poll-cycle statistics in 60 one-minute buckets.
///
/// Replaces `SELECT ... FROM poll_cycle WHERE finished_ts > ?` — the same
/// question, answered in constant time and constant space instead of by
/// scanning every cycle ever recorded.
struct LatencyWindow {
    buckets: [Bucket; WINDOW_MINUTES],
}

impl LatencyWindow {
    fn new() -> Self {
        LatencyWindow {
            buckets: [Bucket::default(); WINDOW_MINUTES],
        }
    }

    fn record(&mut self, finished_ts: i64, ok: bool, actual_ms: f64, overrun: bool) {
        let minute = finished_ts.div_euclid(US_PER_MINUTE);
        let idx = minute.rem_euclid(WINDOW_MINUTES as i64) as usize;
        let b = &mut self.buckets[idx];
        if b.minute != minute {
            *b = Bucket {
                minute,
                ..Bucket::default()
            };
        }
        if ok {
            b.cycles_ok += 1;
        } else {
            b.cycles_failed += 1;
        }
        if overrun {
            b.overruns += 1;
        }
        b.latency_sum_ms += actual_ms;
        if actual_ms > b.latency_max_ms {
            b.latency_max_ms = actual_ms;
        }
    }

    fn summary(&self, now: i64) -> LatencySummary {
        let current = now.div_euclid(US_PER_MINUTE);
        let oldest = current - (WINDOW_MINUTES as i64 - 1);
        let mut out = LatencySummary::default();
        let mut sum = 0.0f64;
        for b in &self.buckets {
            if b.minute < oldest || b.minute > current {
                continue;
            }
            out.cycles_ok += b.cycles_ok;
            out.cycles_failed += b.cycles_failed;
            out.overruns += b.overruns;
            sum += b.latency_sum_ms;
            if b.latency_max_ms > out.latency_max_ms {
                out.latency_max_ms = b.latency_max_ms;
            }
        }
        let n = out.cycles();
        out.latency_avg_ms = if n > 0 { sum / n as f64 } else { 0.0 };
        out
    }
}

// ---------------------------------------------------------------------------
// Collector health
// ---------------------------------------------------------------------------

/// Cached row counts, refreshed on a timer by the monitor loop.
///
/// `COUNT(*)` on `sample` and `poll_cycle` is a full scan in SQLite; at 12.7M
/// rows that is most of the cost of a status request, for a number that no
/// operator needs to the row. Refreshing it once a minute off the request path
/// keeps `/status` constant-time and is honest about its age via `as_of_ts`.
#[derive(Clone, Copy, Default, Debug)]
pub struct DbTotals {
    pub samples: i64,
    pub poll_cycles: i64,
    /// When these were measured; `None` if never.
    pub as_of_ts: Option<i64>,
}

/// Process-wide collector liveness, shared by the workers and the HTTP API.
pub struct CollectorHealth {
    writer_alive: AtomicBool,
    writer_restarts: AtomicU64,
    /// Last time the writer completed a batch — the heartbeat that was missing
    /// when the writer died silently.
    writer_last_write_ts: AtomicI64,
    samples_written: AtomicU64,

    groups: RwLock<HashMap<(i64, i64), Arc<GroupHealth>>>,

    db_samples: AtomicI64,
    db_poll_cycles: AtomicI64,
    db_totals_ts: AtomicI64,
}

impl Default for CollectorHealth {
    fn default() -> Self {
        Self::new()
    }
}

impl CollectorHealth {
    pub fn new() -> Self {
        CollectorHealth {
            writer_alive: AtomicBool::new(false),
            writer_restarts: AtomicU64::new(0),
            writer_last_write_ts: AtomicI64::new(0),
            samples_written: AtomicU64::new(0),
            groups: RwLock::new(HashMap::new()),
            db_samples: AtomicI64::new(0),
            db_poll_cycles: AtomicI64::new(0),
            db_totals_ts: AtomicI64::new(0),
        }
    }

    // -- writer ------------------------------------------------------------

    pub fn writer_started(&self) {
        self.writer_alive.store(true, ORD);
    }

    pub fn writer_stopped(&self) {
        self.writer_alive.store(false, ORD);
    }

    pub fn writer_restarted(&self) {
        self.writer_restarts.fetch_add(1, ORD);
    }

    pub fn writer_alive(&self) -> bool {
        self.writer_alive.load(ORD)
    }

    pub fn writer_restarts(&self) -> u64 {
        self.writer_restarts.load(ORD)
    }

    pub fn writer_last_write_ts(&self) -> Option<i64> {
        opt_ts(self.writer_last_write_ts.load(ORD))
    }

    pub fn samples_written(&self) -> u64 {
        self.samples_written.load(ORD)
    }

    // -- groups ------------------------------------------------------------

    /// Get or create the health record for one (PLC, poll group).
    ///
    /// Reuses the existing record on respawn so restart counts and history
    /// survive a poller being rebuilt after a config apply.
    pub fn group(
        &self,
        plc_id: i64,
        poll_group_id: i64,
        plc_name: &str,
        period_ms: u64,
    ) -> Arc<GroupHealth> {
        let key = (plc_id, poll_group_id);
        if let Some(g) = self.groups.read().get(&key) {
            // The period can change under a config apply; the rest is stable.
            if g.period_ms == period_ms {
                return g.clone();
            }
        }
        let mut w = self.groups.write();
        let existing = w.get(&key).cloned();
        let fresh = Arc::new(GroupHealth::new(plc_id, poll_group_id, plc_name, period_ms));
        if let Some(old) = existing {
            fresh.restarts.store(old.restarts(), ORD);
            fresh.cycles_ok.store(old.cycles_ok.load(ORD), ORD);
            fresh.cycles_failed.store(old.cycles_failed.load(ORD), ORD);
            fresh.last_cycle_ts.store(old.last_cycle_ts.load(ORD), ORD);
            fresh.last_ok_ts.store(old.last_ok_ts.load(ORD), ORD);
        }
        w.insert(key, fresh.clone());
        fresh
    }

    /// Look up an existing group without creating one.
    pub fn get_group(&self, plc_id: i64, poll_group_id: i64) -> Option<Arc<GroupHealth>> {
        self.groups.read().get(&(plc_id, poll_group_id)).cloned()
    }

    /// Every known group, including those whose poller has exited.
    pub fn groups(&self) -> Vec<Arc<GroupHealth>> {
        self.groups.read().values().cloned().collect()
    }

    /// Groups belonging to one PLC.
    pub fn groups_for_plc(&self, plc_id: i64) -> Vec<Arc<GroupHealth>> {
        let mut v: Vec<Arc<GroupHealth>> = self
            .groups
            .read()
            .values()
            .filter(|g| g.plc_id == plc_id)
            .cloned()
            .collect();
        v.sort_by_key(|g| g.poll_group_id);
        v
    }

    /// Groups whose poller thread has died while still being wanted. These are
    /// what the supervisor respawns.
    pub fn dead_groups(&self) -> Vec<Arc<GroupHealth>> {
        self.groups
            .read()
            .values()
            .filter(|g| !g.is_alive())
            .cloned()
            .collect()
    }

    /// Latest successful cycle across all of a PLC's groups.
    pub fn plc_last_ok_ts(&self, plc_id: i64) -> Option<i64> {
        self.groups
            .read()
            .values()
            .filter(|g| g.plc_id == plc_id)
            .filter_map(|g| g.last_ok_ts())
            .max()
    }

    /// Latest cycle of any kind across all of a PLC's groups.
    pub fn plc_last_cycle_ts(&self, plc_id: i64) -> Option<i64> {
        self.groups
            .read()
            .values()
            .filter(|g| g.plc_id == plc_id)
            .filter_map(|g| g.last_cycle_ts())
            .max()
    }

    // -- cycles ------------------------------------------------------------

    /// Record one completed poll cycle: updates the owning group, the rolling
    /// latency window, and the writer heartbeat. Called by the sample-writer
    /// for every batch it processes, whether or not the write succeeded — the
    /// point is to prove the pipeline is moving.
    pub fn record_cycle(
        &self,
        plc_id: i64,
        poll_group_id: i64,
        finished_ts: i64,
        ok: bool,
        actual_ms: f64,
        error: Option<&str>,
        sample_count: usize,
    ) {
        if let Some(g) = self.get_group(plc_id, poll_group_id) {
            g.record_cycle(finished_ts, ok, actual_ms, error);
        }
        self.writer_last_write_ts.store(now_us(), ORD);
        self.samples_written.fetch_add(sample_count as u64, ORD);
    }

    /// Poll-cycle statistics over the last hour, across every group.
    pub fn last_hour(&self) -> LatencySummary {
        self.groups
            .read()
            .values()
            .map(|g| g.last_hour())
            .fold(LatencySummary::default(), LatencySummary::merge)
    }

    // -- cached DB totals --------------------------------------------------

    pub fn set_db_totals(&self, samples: i64, poll_cycles: i64) {
        self.db_samples.store(samples, ORD);
        self.db_poll_cycles.store(poll_cycles, ORD);
        self.db_totals_ts.store(now_us(), ORD);
    }

    pub fn db_totals(&self) -> DbTotals {
        DbTotals {
            samples: self.db_samples.load(ORD),
            poll_cycles: self.db_poll_cycles.load(ORD),
            as_of_ts: opt_ts(self.db_totals_ts.load(ORD)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_threshold_scales_with_period_but_has_a_floor() {
        let fast = GroupHealth::new(1, 1, "plc1", 50);
        let slow = GroupHealth::new(1, 2, "plc1", 850);
        // 3 * 50ms = 150ms, below the floor, so the floor applies.
        assert_eq!(fast.stale_after_us(), 5_000_000);
        // 3 * 850ms = 2550ms, still below the 5s floor.
        assert_eq!(slow.stale_after_us(), 5_000_000);
        let slower = GroupHealth::new(1, 3, "plc1", 10_000);
        assert_eq!(slower.stale_after_us(), 30_000_000);
    }

    #[test]
    fn a_fresh_group_is_not_stale_before_it_can_produce_a_cycle() {
        let g = GroupHealth::new(1, 1, "plc1", 100);
        let now = now_us();
        // Never started and never polled: nothing proves it is working.
        assert!(g.is_stale(now));

        g.mark_started();
        assert!(!g.is_stale(now_us()));
        // Started but silent well past the window.
        assert!(g.is_stale(now_us() + 6_000_000));
    }

    #[test]
    fn a_successful_cycle_clears_staleness_and_the_last_error() {
        let g = GroupHealth::new(1, 1, "plc1", 100);
        g.mark_started();
        g.record_cycle(now_us(), false, 40.0, Some("disconnected: not connected"));
        assert_eq!(
            g.last_error().as_deref(),
            Some("disconnected: not connected")
        );
        assert_eq!(g.cycles_failed(), 1);
        // A failed cycle does not count as proof the link works.
        assert!(g.is_stale(now_us() + 6_000_000));

        g.record_cycle(now_us(), true, 12.0, None);
        assert_eq!(g.last_error(), None);
        assert_eq!(g.cycles_ok(), 1);
        assert!(!g.is_stale(now_us()));
    }

    #[test]
    fn liveness_survives_the_thread_that_reports_it() {
        let h = CollectorHealth::new();
        let g = h.group(1, 1, "plc1", 100);
        let generation = g.mark_started();
        assert!(h.dead_groups().is_empty());

        // A poller that exits — cleanly or by panic — leaves its record
        // behind marked dead, which is what the supervisor looks for.
        g.mark_stopped(generation);
        let dead = h.dead_groups();
        assert_eq!(dead.len(), 1);
        assert_eq!(dead[0].poll_group_id, 1);
        // And the group is still listed for status purposes.
        assert_eq!(h.groups_for_plc(1).len(), 1);
    }

    #[test]
    fn an_outgoing_poller_cannot_mark_its_replacement_dead() {
        // A config apply stops a poller and starts a new one for the same
        // group. The old thread only notices `stop()` on its next tick, so its
        // drop guard runs after the replacement is already polling.
        let h = CollectorHealth::new();
        let g = h.group(1, 1, "plc1", 100);
        let old_generation = g.mark_started();
        let new_generation = g.mark_started();
        assert_ne!(old_generation, new_generation);

        g.mark_stopped(old_generation);
        assert!(g.is_alive(), "outgoing poller marked the live one dead");
        assert!(h.dead_groups().is_empty());

        g.mark_stopped(new_generation);
        assert!(!g.is_alive());
    }

    #[test]
    fn respawning_a_group_keeps_its_counters() {
        let h = CollectorHealth::new();
        let g = h.group(1, 1, "plc1", 100);
        g.mark_started();
        h.record_cycle(1, 1, now_us(), true, 12.0, None, 5);
        g.note_restart();
        g.mark_stopped(1);

        // Same period: same record.
        let again = h.group(1, 1, "plc1", 100);
        assert_eq!(again.cycles_ok(), 1);
        assert_eq!(again.restarts(), 1);

        // Changed period (config apply): fresh record, history carried over.
        let repaired = h.group(1, 1, "plc1", 200);
        assert_eq!(repaired.period_ms, 200);
        assert_eq!(repaired.cycles_ok(), 1);
        assert_eq!(repaired.restarts(), 1);
        assert_eq!(h.groups().len(), 1);
    }

    #[test]
    fn latency_window_covers_an_hour_and_drops_what_falls_out() {
        let mut w = LatencyWindow::new();
        let now = 1_000 * US_PER_MINUTE;
        // One cycle per minute for two hours, ending at `now`.
        for i in 0..120i64 {
            let ts = now - (119 - i) * US_PER_MINUTE;
            w.record(ts, i % 10 != 0, (i % 50) as f64, i % 5 == 0);
        }
        let s = w.summary(now);
        // Only the last 60 minutes are counted, not all 120.
        assert_eq!(s.cycles_ok + s.cycles_failed, 60);
        assert!(s.latency_max_ms <= 49.0);

        // A window nobody has written to for hours reports nothing rather
        // than replaying stale buckets.
        let later = w.summary(now + 120 * US_PER_MINUTE);
        assert_eq!(later, LatencySummary::default());
    }

    #[test]
    fn db_totals_are_absent_until_measured() {
        let h = CollectorHealth::new();
        assert_eq!(h.db_totals().as_of_ts, None);
        h.set_db_totals(1_431_943, 12_744_003);
        let t = h.db_totals();
        assert_eq!(t.samples, 1_431_943);
        assert_eq!(t.poll_cycles, 12_744_003);
        assert!(t.as_of_ts.is_some());
    }
}
