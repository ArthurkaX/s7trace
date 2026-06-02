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
    /// Whether a sample should be written.
    pub should_write: bool,
    /// The integer value to store (if applicable).
    pub vi: Option<i64>,
    /// The float value to store (if applicable).
    pub vf: Option<f64>,
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

/// Initialize hook state from tag metadata.
pub fn init_hook_state(hook: StorageHook, _params: Option<&str>) -> HookState {
    match hook {
        StorageHook::OnChange => HookState::OnChange(OnChangeState {
            last_stored_vi: None,
            last_stored_vf: None,
        }),
        StorageHook::Deadband => HookState::Deadband(DeadbandState {
            last_stored_vi: None,
            last_stored_vf: None,
            threshold: 0.0,
        }),
        StorageHook::Periodic => HookState::Periodic,
        StorageHook::MinMaxRollup => HookState::MinMaxRollup(MinMaxRollupState {
            window_start_ts: now_us(),
            window_ms: 1000,
            first_vi: None,
            last_vi: None,
            min_vi: None,
            max_vi: None,
            first_vf: None,
            last_vf: None,
            min_vf: None,
            max_vf: None,
            sample_count: 0,
        }),
        StorageHook::DeltaRollup => HookState::DeltaRollup(DeltaRollupState {
            window_start_ts: now_us(),
            window_ms: 1000,
            first_vi: None,
            last_vi: None,
            first_vf: None,
            last_vf: None,
        }),
        StorageHook::StateDuration => HookState::StateDuration(StateDurationState {
            current_state: None,
            state_start_ts: now_us(),
            states: Vec::new(),
        }),
        StorageHook::CounterDelta => HookState::CounterDelta(CounterDeltaState {
            last_value: None,
            wrap_at: 65536, // default u16 wrap
        }),
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
            vi,
            vf,
        },
        HookState::MinMaxRollup(state) => eval_min_max_rollup(state, vi, vf, ts),
        HookState::DeltaRollup(state) => eval_delta_rollup(state, vi, vf, ts),
        HookState::StateDuration(state) => eval_state_duration(state, vi, vf, ts),
        HookState::CounterDelta(state) => eval_counter_delta(state, vi),
    }
}

fn eval_on_change(
    state: &mut OnChangeState,
    vi: Option<i64>,
    vf: Option<f64>,
) -> WriteDecision {
    let changed = match (vi, state.last_stored_vi) {
        (Some(a), Some(b)) => a != b,
        (None, None) => {
            match (vf, state.last_stored_vf) {
                (Some(a), Some(b)) => (a - b).abs() > f64::EPSILON,
                _ => true,
            }
        }
        _ => true,
    };

    if changed {
        state.last_stored_vi = vi;
        state.last_stored_vf = vf;
        WriteDecision { should_write: true, vi, vf }
    } else {
        WriteDecision { should_write: false, vi, vf }
    }
}

fn eval_deadband(
    state: &mut DeadbandState,
    vi: Option<i64>,
    vf: Option<f64>,
) -> WriteDecision {
    let threshold = state.threshold;
    let changed = match (vf, state.last_stored_vf) {
        (Some(a), Some(b)) => (a - b).abs() >= threshold,
        _ => {
            match (vi, state.last_stored_vi) {
                (Some(a), Some(b)) => (a - b).unsigned_abs() >= threshold as u64,
                _ => true,
            }
        }
    };

    if changed {
        state.last_stored_vi = vi;
        state.last_stored_vf = vf;
        WriteDecision { should_write: true, vi, vf }
    } else {
        WriteDecision { should_write: false, vi, vf }
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
        let decision = WriteDecision {
            should_write: true,
            vi: state.last_vi,
            vf: state.last_vf,
        };
        let next_start = now_us();
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
        decision
    } else {
        WriteDecision { should_write: false, vi, vf }
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
        let decision = WriteDecision {
            should_write: true,
            vi: state.last_vi,
            vf: state.last_vf,
        };
        state.window_start_ts = now_us();
        state.first_vi = None;
        state.last_vi = None;
        state.first_vf = None;
        state.last_vf = None;
        decision
    } else {
        WriteDecision { should_write: false, vi, vf }
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

    // Write state transitions (not every sample)
    WriteDecision { should_write: false, vi, vf }
}

fn eval_counter_delta(
    state: &mut CounterDeltaState,
    vi: Option<i64>,
) -> WriteDecision {
    let delta = match (vi, state.last_value) {
        (Some(curr), Some(prev)) => {
            if curr < prev {
                // Possible wraparound
                (curr + state.wrap_at - prev) as i64
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
            vi: Some(delta),
            vf: None,
        }
    } else {
        WriteDecision { should_write: false, vi, vf: None }
    }
}

/// Decode raw bytes into a typed value.
pub fn decode_value(data: &[u8], data_type: DataType) -> (Option<i64>, Option<f64>) {
    match data_type {
        DataType::Bool => {
            if data.is_empty() {
                (None, None)
            } else {
                (Some((data[0] != 0) as i64), None)
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
        let (vi, vf) = decode_value(&[0x00, 0x2A], DataType::I16);
        assert_eq!(vi, Some(42));
        assert!(vf.is_none());
    }

    #[test]
    fn test_decode_negative_i16() {
        let (vi, vf) = decode_value(&[0xFF, 0xCE], DataType::I16);
        assert_eq!(vi, Some(-50));
        assert!(vf.is_none());
    }

    #[test]
    fn test_decode_f32() {
        let (vi, vf) = decode_value(&[0x42, 0x48, 0x00, 0x00], DataType::F32);
        assert!(vi.is_none());
        assert!(vf.is_some());
        let v = vf.unwrap();
        assert!((v - 50.0).abs() < 0.01);
    }

    #[test]
    fn test_decode_u16() {
        let (vi, vf) = decode_value(&[0xEA, 0x60], DataType::U16);
        assert_eq!(vi, Some(60000));
        assert!(vf.is_none());
    }

    #[test]
    fn test_decode_bool_true() {
        let (vi, _vf) = decode_value(&[0x01], DataType::Bool);
        assert_eq!(vi, Some(1));
    }


    #[test]
    fn test_decode_bool_false() {
        let (vi, _vf) = decode_value(&[0x00], DataType::Bool);
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
}
