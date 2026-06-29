/// Storage hooks that decide when and how samples are written.
///
/// Each hook implements the `StorageHook` trait. The engine calls
/// `should_write` after each successful poll to decide whether to
/// insert a sample row.
use crate::types::*;
use serde::{Deserialize, Serialize};

/// Result of evaluating a storage hook.
#[derive(Debug, Clone)]
pub struct WriteDecision {
    /// Whether a sample should be written into the `sample` table.
    pub should_write: bool,
    /// Whether a rollup row should be written into the `sample_rollup` table.
    pub should_write_rollup: bool,
    /// The integer value to store (if applicable).
    pub vi: Option<i64>,
    /// The float value to store (if applicable).
    pub vf: Option<f64>,
    /// Rollup payload (only when `should_write_rollup` is true).
    pub rollup: Option<RollupPayload>,
}

/// Per-window rollup row produced by `min_max_rollup` / `delta_rollup`.
#[derive(Debug, Clone)]
pub struct RollupPayload {
    pub window_start_ts: TimestampUs,
    pub window_end_ts: TimestampUs,
    pub first_vi: Option<i64>,
    pub last_vi: Option<i64>,
    pub min_vi: Option<i64>,
    pub max_vi: Option<i64>,
    pub delta_vi: Option<i64>,
    pub first_vf: Option<f64>,
    pub last_vf: Option<f64>,
    pub min_vf: Option<f64>,
    pub max_vf: Option<f64>,
    pub delta_vf: Option<f64>,
    pub sample_count: i64,
}

/// Per-tag storage hook state kept in memory across poll cycles.
#[derive(Debug, Clone)]
pub enum HookState {
    OnChange(OnChangeState),
    Deadband(DeadbandState),
    Periodic,
    MinMaxRollup(MinMaxRollupState),
    DeltaRollup(DeltaRollupState),
    StateDuration(StateDurationState),
    CounterDelta(CounterDeltaState),
}

#[derive(Debug, Clone)]
pub struct OnChangeState {
    pub last_stored_vi: Option<i64>,
    pub last_stored_vf: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct DeadbandState {
    pub last_stored_vi: Option<i64>,
    pub last_stored_vf: Option<f64>,
    pub threshold: f64,
}

#[derive(Debug, Clone)]
pub struct MinMaxRollupState {
    pub window_start_ts: TimestampUs,
    pub window_ms: i64,
    pub first_vi: Option<i64>,
    pub last_vi: Option<i64>,
    pub min_vi: Option<i64>,
    pub max_vi: Option<i64>,
    pub first_vf: Option<f64>,
    pub last_vf: Option<f64>,
    pub min_vf: Option<f64>,
    pub max_vf: Option<f64>,
    pub sample_count: u64,
}

#[derive(Debug, Clone)]
pub struct DeltaRollupState {
    pub window_start_ts: TimestampUs,
    pub window_ms: i64,
    pub first_vi: Option<i64>,
    pub last_vi: Option<i64>,
    pub first_vf: Option<f64>,
    pub last_vf: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct StateDurationState {
    pub current_state: Option<i64>,
    pub state_start_ts: TimestampUs,
    pub states: Vec<StateInterval>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateInterval {
    pub state: i64,
    pub start_ts: TimestampUs,
    pub end_ts: TimestampUs,
    pub duration_ms: i64,
}

#[derive(Debug, Clone)]
pub struct CounterDeltaState {
    pub last_value: Option<i64>,
    pub wrap_at: i64,
}

// ---------------------------------------------------------------------------
// Storage hook parameters parsed from JSON
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadbandParams {
    pub threshold: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowParams {
    pub window_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CounterParams {
    pub wrap_at: i64,
}

// ---------------------------------------------------------------------------
// Hook evaluation functions
// ---------------------------------------------------------------------------

/// Initialize hook state from tag metadata. Parses `hook_params_json` to
/// customize hook parameters (e.g. counter wrap, deadband threshold, window).
pub fn init_hook_state(hook: StorageHook, params: Option<&str>) -> HookState {
    let parsed: Option<serde_json::Value> = params.and_then(|p| serde_json::from_str(p).ok());

    match hook {
        StorageHook::OnChange => HookState::OnChange(OnChangeState {
            last_stored_vi: None,
            last_stored_vf: None,
        }),
        StorageHook::Deadband => {
            let threshold = parsed
                .as_ref()
                .and_then(|v| v.get("threshold"))
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            HookState::Deadband(DeadbandState {
                last_stored_vi: None,
                last_stored_vf: None,
                threshold,
            })
        }
        StorageHook::Periodic => HookState::Periodic,
        StorageHook::MinMaxRollup => {
            let window_ms = parsed
                .as_ref()
                .and_then(|v| v.get("window_ms"))
                .and_then(|v| v.as_i64())
                .unwrap_or(1000);
            HookState::MinMaxRollup(MinMaxRollupState {
                window_start_ts: now_us(),
                window_ms,
                first_vi: None,
                last_vi: None,
                min_vi: None,
                max_vi: None,
                first_vf: None,
                last_vf: None,
                min_vf: None,
                max_vf: None,
                sample_count: 0,
            })
        }
        StorageHook::DeltaRollup => {
            let window_ms = parsed
                .as_ref()
                .and_then(|v| v.get("window_ms"))
                .and_then(|v| v.as_i64())
                .unwrap_or(1000);
            HookState::DeltaRollup(DeltaRollupState {
                window_start_ts: now_us(),
                window_ms,
                first_vi: None,
                last_vi: None,
                first_vf: None,
                last_vf: None,
            })
        }
        StorageHook::StateDuration => HookState::StateDuration(StateDurationState {
            current_state: None,
            state_start_ts: now_us(),
            states: Vec::new(),
        }),
        StorageHook::CounterDelta => {
            let wrap_at = parsed
                .as_ref()
                .and_then(|v| v.get("wrap_at"))
                .and_then(|v| v.as_i64())
                .unwrap_or(65536); // default u16 wrap
            HookState::CounterDelta(CounterDeltaState {
                last_value: None,
                wrap_at,
            })
        }
    }
}

/// Evaluate whether to write a sample, and update hook state.
pub fn evaluate(
    hook: &mut HookState,
    vi: Option<i64>,
    vf: Option<f64>,
    ts: TimestampUs,
) -> WriteDecision {
    match hook {
        HookState::OnChange(state) => eval_on_change(state, vi, vf),
        HookState::Deadband(state) => eval_deadband(state, vi, vf),
        HookState::Periodic => WriteDecision {
            should_write: true,
            should_write_rollup: false,
            vi,
            vf,
            rollup: None,
        },
        HookState::MinMaxRollup(state) => eval_min_max_rollup(state, vi, vf, ts),
        HookState::DeltaRollup(state) => eval_delta_rollup(state, vi, vf, ts),
        HookState::StateDuration(state) => eval_state_duration(state, vi, vf, ts),
        HookState::CounterDelta(state) => eval_counter_delta(state, vi),
    }
}

fn eval_on_change(state: &mut OnChangeState, vi: Option<i64>, vf: Option<f64>) -> WriteDecision {
    let changed = match (vi, state.last_stored_vi) {
        (Some(a), Some(b)) => a != b,
        (None, None) => match (vf, state.last_stored_vf) {
            (Some(a), Some(b)) => (a - b).abs() > f64::EPSILON,
            _ => true,
        },
        _ => true,
    };

    if changed {
        state.last_stored_vi = vi;
        state.last_stored_vf = vf;
        WriteDecision {
            should_write: true,
            should_write_rollup: false,
            vi,
            vf,
            rollup: None,
        }
    } else {
        WriteDecision {
            should_write: false,
            should_write_rollup: false,
            vi,
            vf,
            rollup: None,
        }
    }
}

fn eval_deadband(state: &mut DeadbandState, vi: Option<i64>, vf: Option<f64>) -> WriteDecision {
    let threshold = state.threshold;
    let changed = match (vf, state.last_stored_vf) {
        (Some(a), Some(b)) => (a - b).abs() >= threshold,
        _ => match (vi, state.last_stored_vi) {
            (Some(a), Some(b)) => (a - b).unsigned_abs() >= threshold as u64,
            _ => true,
        },
    };

    if changed {
        state.last_stored_vi = vi;
        state.last_stored_vf = vf;
        WriteDecision {
            should_write: true,
            should_write_rollup: false,
            vi,
            vf,
            rollup: None,
        }
    } else {
        WriteDecision {
            should_write: false,
            should_write_rollup: false,
            vi,
            vf,
            rollup: None,
        }
    }
}

fn eval_min_max_rollup(
    state: &mut MinMaxRollupState,
    vi: Option<i64>,
    vf: Option<f64>,
    ts: TimestampUs,
) -> WriteDecision {
    let window_end = state.window_start_ts + state.window_ms * 1000;

    // First sample in window
    if state.sample_count == 0 {
        state.first_vi = vi;
        state.last_vi = vi;
        state.min_vi = vi;
        state.max_vi = vi;
        state.first_vf = vf;
        state.last_vf = vf;
        state.min_vf = vf;
        state.max_vf = vf;
    } else {
        state.last_vi = vi;
        state.last_vf = vf;
        if let Some(v) = vi {
            state.min_vi = Some(state.min_vi.unwrap_or(i64::MAX).min(v));
            state.max_vi = Some(state.max_vi.unwrap_or(i64::MIN).max(v));
        }
        if let Some(v) = vf {
            state.min_vf = Some(state.min_vf.unwrap_or(f64::MAX).min(v));
            state.max_vf = Some(state.max_vf.unwrap_or(f64::MIN).max(v));
        }
    }
    state.sample_count += 1;

    if ts >= window_end {
        // Window complete — write rollup row and reset
        let delta_vi = match (state.first_vi, state.last_vi) {
            (Some(a), Some(b)) => Some(b - a),
            _ => None,
        };
        let delta_vf = match (state.first_vf, state.last_vf) {
            (Some(a), Some(b)) => Some(b - a),
            _ => None,
        };
        let payload = RollupPayload {
            window_start_ts: state.window_start_ts,
            window_end_ts: ts,
            first_vi: state.first_vi,
            last_vi: state.last_vi,
            min_vi: state.min_vi,
            max_vi: state.max_vi,
            delta_vi,
            first_vf: state.first_vf,
            last_vf: state.last_vf,
            min_vf: state.min_vf,
            max_vf: state.max_vf,
            delta_vf,
            sample_count: state.sample_count as i64,
        };
        let next_start = ts;
        state.window_start_ts = next_start;
        state.sample_count = 0;
        state.first_vi = None;
        state.last_vi = None;
        state.min_vi = None;
        state.max_vi = None;
        state.first_vf = None;
        state.last_vf = None;
        state.min_vf = None;
        state.max_vf = None;
        WriteDecision {
            should_write: false,
            should_write_rollup: true,
            vi,
            vf,
            rollup: Some(payload),
        }
    } else {
        WriteDecision {
            should_write: false,
            should_write_rollup: false,
            vi,
            vf,
            rollup: None,
        }
    }
}

fn eval_delta_rollup(
    state: &mut DeltaRollupState,
    vi: Option<i64>,
    vf: Option<f64>,
    ts: TimestampUs,
) -> WriteDecision {
    let window_end = state.window_start_ts + state.window_ms * 1000;

    if state.first_vi.is_none() {
        state.first_vi = vi;
        state.first_vf = vf;
    }
    state.last_vi = vi;
    state.last_vf = vf;

    if ts >= window_end {
        let delta_vi = match (state.first_vi, state.last_vi) {
            (Some(a), Some(b)) => Some(b - a),
            _ => None,
        };
        let delta_vf = match (state.first_vf, state.last_vf) {
            (Some(a), Some(b)) => Some(b - a),
            _ => None,
        };
        let payload = RollupPayload {
            window_start_ts: state.window_start_ts,
            window_end_ts: ts,
            first_vi: state.first_vi,
            last_vi: state.last_vi,
            min_vi: None,
            max_vi: None,
            delta_vi,
            first_vf: state.first_vf,
            last_vf: state.last_vf,
            min_vf: None,
            max_vf: None,
            delta_vf,
            sample_count: 1,
        };
        let next_start = ts;
        state.window_start_ts = next_start;
        state.first_vi = None;
        state.last_vi = None;
        state.first_vf = None;
        state.last_vf = None;
        WriteDecision {
            should_write: false,
            should_write_rollup: true,
            vi,
            vf,
            rollup: Some(payload),
        }
    } else {
        WriteDecision {
            should_write: false,
            should_write_rollup: false,
            vi,
            vf,
            rollup: None,
        }
    }
}

fn eval_state_duration(
    state: &mut StateDurationState,
    vi: Option<i64>,
    vf: Option<f64>,
    ts: TimestampUs,
) -> WriteDecision {
    let new_state = vi;

    if new_state != state.current_state {
        // State changed
        if let Some(old_state_val) = state.current_state {
            let duration_us = ts - state.state_start_ts;
            state.states.push(StateInterval {
                state: old_state_val,
                start_ts: state.state_start_ts,
                end_ts: ts,
                duration_ms: duration_us / 1000,
            });
        }
        state.current_state = new_state;
        state.state_start_ts = ts;
    }

    // State transitions are flushed separately (see `drain_state_durations`).
    WriteDecision {
        should_write: false,
        should_write_rollup: false,
        vi,
        vf,
        rollup: None,
    }
}

/// Drain accumulated state durations. Returns the intervals and resets state.
pub fn drain_state_durations(state: &mut StateDurationState) -> Vec<StateInterval> {
    let mut out = std::mem::take(&mut state.states);
    // Close current open state if any
    if let Some(current) = state.current_state {
        let now = now_us();
        out.push(StateInterval {
            state: current,
            start_ts: state.state_start_ts,
            end_ts: now,
            duration_ms: (now - state.state_start_ts) / 1000,
        });
    }
    out
}

fn eval_counter_delta(state: &mut CounterDeltaState, vi: Option<i64>) -> WriteDecision {
    let delta = match (vi, state.last_value) {
        (Some(curr), Some(prev)) => {
            if curr < prev {
                // Possible wraparound
                curr + state.wrap_at - prev
            } else {
                curr - prev
            }
        }
        _ => 0,
    };

    state.last_value = vi;

    if delta != 0 {
        WriteDecision {
            should_write: true,
            should_write_rollup: false,
            vi: Some(delta),
            vf: None,
            rollup: None,
        }
    } else {
        WriteDecision {
            should_write: false,
            should_write_rollup: false,
            vi,
            vf: None,
            rollup: None,
        }
    }
}

/// Decode raw bytes into a typed value.
///
/// `bit_offset` is used for `DataType::Bool` to extract a single bit.
/// When `None`, any non‑zero byte value is treated as `true`.
pub fn decode_value(
    data: &[u8],
    data_type: DataType,
    bit_offset: Option<u8>,
) -> (Option<i64>, Option<f64>) {
    match data_type {
        DataType::Bool => {
            if data.is_empty() {
                (None, None)
            } else {
                let val = match bit_offset {
                    Some(bit) if bit < 8 => ((data[0] >> bit) & 1) as i64,
                    _ => (data[0] != 0) as i64,
                };
                (Some(val), None)
            }
        }
        DataType::I16 => {
            if data.len() < 2 {
                (None, None)
            } else {
                let val = i16::from_be_bytes([data[0], data[1]]);
                (Some(val as i64), None)
            }
        }
        DataType::U16 => {
            if data.len() < 2 {
                (None, None)
            } else {
                let val = u16::from_be_bytes([data[0], data[1]]);
                (Some(val as i64), None)
            }
        }
        DataType::I32 => {
            if data.len() < 4 {
                (None, None)
            } else {
                let val = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                (Some(val as i64), None)
            }
        }
        DataType::U32 => {
            if data.len() < 4 {
                (None, None)
            } else {
                let val = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                (Some(val as i64), None)
            }
        }
        DataType::F32 => {
            if data.len() < 4 {
                (None, None)
            } else {
                let val = f32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                (None, Some(val as f64))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_on_change_same_value() {
        let mut state = init_hook_state(StorageHook::OnChange, None);
        let d1 = evaluate(&mut state, Some(42), None, 1000);
        assert!(d1.should_write);
        let d2 = evaluate(&mut state, Some(42), None, 2000);
        assert!(!d2.should_write);
        let d3 = evaluate(&mut state, Some(43), None, 3000);
        assert!(d3.should_write);
    }

    #[test]
    fn test_periodic_always_writes() {
        let mut state = init_hook_state(StorageHook::Periodic, None);
        let d1 = evaluate(&mut state, Some(1), None, 1000);
        assert!(d1.should_write);
        let d2 = evaluate(&mut state, Some(1), None, 2000);
        assert!(d2.should_write);
    }

    #[test]
    fn test_decode_i16() {
        let (vi, vf) = decode_value(&[0x00, 0x2A], DataType::I16, None);
        assert_eq!(vi, Some(42));
        assert!(vf.is_none());
    }

    #[test]
    fn test_decode_negative_i16() {
        let (vi, vf) = decode_value(&[0xFF, 0xCE], DataType::I16, None);
        assert_eq!(vi, Some(-50));
        assert!(vf.is_none());
    }

    #[test]
    fn test_decode_f32() {
        let (vi, vf) = decode_value(&[0x42, 0x48, 0x00, 0x00], DataType::F32, None);
        assert!(vi.is_none());
        assert!(vf.is_some());
        let v = vf.unwrap();
        assert!((v - 50.0).abs() < 0.01);
    }

    #[test]
    fn test_decode_u16() {
        let (vi, vf) = decode_value(&[0xEA, 0x60], DataType::U16, None);
        assert_eq!(vi, Some(60000));
        assert!(vf.is_none());
    }

    #[test]
    fn test_decode_bool_true() {
        let (vi, _vf) = decode_value(&[0x01], DataType::Bool, None);
        assert_eq!(vi, Some(1));
    }

    #[test]
    fn test_decode_bool_false() {
        let (vi, _vf) = decode_value(&[0x00], DataType::Bool, None);
        assert_eq!(vi, Some(0));
    }

    #[test]
    fn test_decode_bool_bit() {
        // Byte 0b01000000, bit 6 -> true, bit 5 -> false
        let (vi, _vf) = decode_value(&[0b01000000], DataType::Bool, Some(6));
        assert_eq!(vi, Some(1));
        let (vi, _vf) = decode_value(&[0b01000000], DataType::Bool, Some(5));
        assert_eq!(vi, Some(0));
    }

    #[test]
    fn test_counter_delta() {
        let mut state = init_hook_state(StorageHook::CounterDelta, None);
        let d1 = evaluate(&mut state, Some(100), None, 1000);
        assert!(!d1.should_write); // first read, no delta
        let d2 = evaluate(&mut state, Some(105), None, 2000);
        assert!(d2.should_write);
        assert_eq!(d2.vi, Some(5));
    }

    #[test]
    fn test_counter_delta_wrap_at_from_params() {
        // u32 counter, wraps at 2^32
        let mut state =
            init_hook_state(StorageHook::CounterDelta, Some(r#"{"wrap_at":4294967296}"#));
        let d1 = evaluate(&mut state, Some(4_000_000_000_u32 as i64), None, 1000);
        assert!(!d1.should_write);
        let d2 = evaluate(&mut state, Some(10), None, 2000);
        // Wrap: 10 - 4e9 + 2^32 = 294967306
        assert!(d2.should_write);
        assert_eq!(d2.vi, Some(294_967_306));
    }

    #[test]
    fn test_deadband_threshold_from_params() {
        let mut state = init_hook_state(StorageHook::Deadband, Some(r#"{"threshold":5.0}"#));
        let d1 = evaluate(&mut state, Some(10), None, 1000);
        assert!(d1.should_write);
        let d2 = evaluate(&mut state, Some(12), None, 2000); // diff 2 < 5
        assert!(!d2.should_write);
        let d3 = evaluate(&mut state, Some(20), None, 3000); // diff 10 >= 5
        assert!(d3.should_write);
    }

    #[test]
    fn test_min_max_rollup_window_completion() {
        let mut state = init_hook_state(StorageHook::MinMaxRollup, None);
        if let HookState::MinMaxRollup(ref mut s) = state {
            s.window_start_ts = 0;
            s.window_ms = 1; // 1ms window: window_end = 0 + 1000us
        }
        // First sample inside the window (ts=500 < 1000us)
        let d1 = evaluate(&mut state, Some(10), None, 500);
        assert!(!d1.should_write);
        assert!(!d1.should_write_rollup);

        // Second sample past the window (ts=5000 >= 1000us)
        let d2 = evaluate(&mut state, Some(50), None, 5_000);
        assert!(d2.should_write_rollup);
        let p = d2.rollup.unwrap();
        assert_eq!(p.first_vi, Some(10));
        assert_eq!(p.last_vi, Some(50));
        assert_eq!(p.max_vi, Some(50));
        assert_eq!(p.min_vi, Some(10));
        assert_eq!(p.delta_vi, Some(40));
        assert_eq!(p.sample_count, 2);
    }

    #[test]
    fn test_delta_rollup_window_completion() {
        let mut state = init_hook_state(StorageHook::DeltaRollup, None);
        if let HookState::DeltaRollup(ref mut s) = state {
            s.window_start_ts = 0;
            s.window_ms = 1;
        }
        let d1 = evaluate(&mut state, Some(100), None, 500);
        assert!(!d1.should_write_rollup);
        let d2 = evaluate(&mut state, Some(150), None, 5_000);
        assert!(d2.should_write_rollup);
        let p = d2.rollup.unwrap();
        assert_eq!(p.first_vi, Some(100));
        assert_eq!(p.last_vi, Some(150));
        assert_eq!(p.delta_vi, Some(50));
    }
}
