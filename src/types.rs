/// Core types shared across s7trace modules.
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

// ---------------------------------------------------------------------------
// Timestamps
// ---------------------------------------------------------------------------

/// Unix epoch microseconds (per D001).
pub type TimestampUs = i64;

/// Returns the current time as UNIX epoch microseconds.
pub fn now_us() -> TimestampUs {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}

// ---------------------------------------------------------------------------
// Data types (D006)
// ---------------------------------------------------------------------------

/// PLC data type encoding width and representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataType {
    Bool,
    I16,
    U16,
    I32,
    U32,
    F32,
}

impl DataType {
    pub fn byte_size(&self) -> usize {
        match self {
            DataType::Bool => 1,
            DataType::I16 => 2,
            DataType::U16 => 2,
            DataType::I32 => 4,
            DataType::U32 => 4,
            DataType::F32 => 4,
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Bool => write!(f, "bool"),
            DataType::I16 => write!(f, "i16"),
            DataType::U16 => write!(f, "u16"),
            DataType::I32 => write!(f, "i32"),
            DataType::U32 => write!(f, "u32"),
            DataType::F32 => write!(f, "f32"),
        }
    }
}

impl FromStr for DataType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "bool" => Ok(DataType::Bool),
            "i16" => Ok(DataType::I16),
            "u16" => Ok(DataType::U16),
            "i32" => Ok(DataType::I32),
            "u32" => Ok(DataType::U32),
            "f32" => Ok(DataType::F32),
            _ => Err(format!("unknown data type: {s}")),
        }
    }
}

// ---------------------------------------------------------------------------
// Value role (D006)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueRole {
    State,
    Counter,
    Position,
    Speed,
    Temperature,
    Diagnostic,
}

impl fmt::Display for ValueRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValueRole::State => write!(f, "state"),
            ValueRole::Counter => write!(f, "counter"),
            ValueRole::Position => write!(f, "position"),
            ValueRole::Speed => write!(f, "speed"),
            ValueRole::Temperature => write!(f, "temperature"),
            ValueRole::Diagnostic => write!(f, "diagnostic"),
        }
    }
}

impl FromStr for ValueRole {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "state" => Ok(ValueRole::State),
            "counter" => Ok(ValueRole::Counter),
            "position" => Ok(ValueRole::Position),
            "speed" => Ok(ValueRole::Speed),
            "temperature" => Ok(ValueRole::Temperature),
            "diagnostic" => Ok(ValueRole::Diagnostic),
            _ => Err(format!("unknown value role: {s}")),
        }
    }
}

// ---------------------------------------------------------------------------
// Store mode / storage hook (D007)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoreMode {
    OnChange,
    Deadband,
    Periodic,
    Rollup,
}

impl fmt::Display for StoreMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreMode::OnChange => write!(f, "on_change"),
            StoreMode::Deadband => write!(f, "deadband"),
            StoreMode::Periodic => write!(f, "periodic"),
            StoreMode::Rollup => write!(f, "rollup"),
        }
    }
}

impl FromStr for StoreMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "on_change" => Ok(StoreMode::OnChange),
            "deadband" => Ok(StoreMode::Deadband),
            "periodic" => Ok(StoreMode::Periodic),
            "rollup" => Ok(StoreMode::Rollup),
            _ => Err(format!("unknown store mode: {s}")),
        }
    }
}

/// Storage hook — determines when and how a sample is written (D007).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageHook {
    OnChange,
    Deadband,
    Periodic,
    MinMaxRollup,
    DeltaRollup,
    StateDuration,
    CounterDelta,
}

impl fmt::Display for StorageHook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageHook::OnChange => write!(f, "on_change"),
            StorageHook::Deadband => write!(f, "deadband"),
            StorageHook::Periodic => write!(f, "periodic"),
            StorageHook::MinMaxRollup => write!(f, "min_max_rollup"),
            StorageHook::DeltaRollup => write!(f, "delta_rollup"),
            StorageHook::StateDuration => write!(f, "state_duration"),
            StorageHook::CounterDelta => write!(f, "counter_delta"),
        }
    }
}

impl FromStr for StorageHook {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "on_change" => Ok(StorageHook::OnChange),
            "deadband" => Ok(StorageHook::Deadband),
            "periodic" => Ok(StorageHook::Periodic),
            "min_max_rollup" => Ok(StorageHook::MinMaxRollup),
            "delta_rollup" => Ok(StorageHook::DeltaRollup),
            "state_duration" => Ok(StorageHook::StateDuration),
            "counter_delta" => Ok(StorageHook::CounterDelta),
            _ => Err(format!("unknown storage hook: {s}")),
        }
    }
}

// ---------------------------------------------------------------------------
// Bad tag classification (D005)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BadClass {
    AddressParse,
    AddressOutOfRange,
    DecodeError,
    Timeout,
    Disconnected,
    ProtocolError,
    PlcUnavailable,
    Internal,
}

impl fmt::Display for BadClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BadClass::AddressParse => write!(f, "address_parse"),
            BadClass::AddressOutOfRange => write!(f, "address_out_of_range"),
            BadClass::DecodeError => write!(f, "decode_error"),
            BadClass::Timeout => write!(f, "timeout"),
            BadClass::Disconnected => write!(f, "disconnected"),
            BadClass::ProtocolError => write!(f, "protocol_error"),
            BadClass::PlcUnavailable => write!(f, "plc_unavailable"),
            BadClass::Internal => write!(f, "internal"),
        }
    }
}

impl FromStr for BadClass {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "address_parse" => Ok(BadClass::AddressParse),
            "address_out_of_range" => Ok(BadClass::AddressOutOfRange),
            "decode_error" => Ok(BadClass::DecodeError),
            "timeout" => Ok(BadClass::Timeout),
            "disconnected" => Ok(BadClass::Disconnected),
            "protocol_error" => Ok(BadClass::ProtocolError),
            "plc_unavailable" => Ok(BadClass::PlcUnavailable),
            "internal" => Ok(BadClass::Internal),
            _ => Err(format!("unknown bad class: {s}")),
        }
    }
}

// ---------------------------------------------------------------------------
// Apply state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplyState {
    Pending,
    Applied,
    Bad,
}

impl fmt::Display for ApplyState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApplyState::Pending => write!(f, "pending"),
            ApplyState::Applied => write!(f, "applied"),
            ApplyState::Bad => write!(f, "bad"),
        }
    }
}

impl FromStr for ApplyState {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(ApplyState::Pending),
            "applied" => Ok(ApplyState::Applied),
            "bad" => Ok(ApplyState::Bad),
            _ => Err(format!("unknown apply state: {s}")),
        }
    }
}

// ---------------------------------------------------------------------------
// Command queue types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandKind {
    Query,
    Control,
    PlcRead,
    Admin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandState {
    Pending,
    Running,
    Done,
    Bad,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandRequest {
    pub id: i64,
    pub created_ts: TimestampUs,
    pub state: CommandState,
    pub kind: CommandKind,
    pub request_json: String,
    pub plc_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandResult {
    pub command_id: i64,
    pub ok: bool,
    pub result_json: Option<String>,
    pub error: Option<String>,
}

// ---------------------------------------------------------------------------
// Config apply
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigApplyState {
    Pending,
    Applying,
    Done,
    Bad,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplyScope {
    Plc,
    PollGroup,
    MetricSet,
    Tag,
    StorageHook,
    Collection,
    HistoryCleanup,
}

// ---------------------------------------------------------------------------
// S7 address parsing
// ---------------------------------------------------------------------------

/// Parsed S7 address, e.g. `DB5999.DBW120` or `DB5999.DBD240`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S7Address {
    pub db_num: u16,
    pub byte_offset: usize,
    pub bit_offset: Option<u8>,
}

impl S7Address {
    /// Parse an address string like `DB5999.DBW120` or `DB5997.DBX10.1`.
    ///
    /// Format:
    /// - `DB<n>.<DBW|DBD|DBB><byte_offset>` (byte/word/dword)
    /// - `DB<n>.<DBX><byte>.<bit>` (bit at byte.bit; S7-300 standard)
    /// - Legacy: `DB<n>.<DBX><linear_bit>` (linear bit offset, must be < 8
    ///   for non-byte-aligned form; just use DBX<byte>.<bit> in new code)
    ///
    /// Examples:
    ///   DB5999.DBW8      — word at byte 8 in DB5999
    ///   DB5997.DBX10.1   — bit 1 of byte 10 in DB5997 (Siemens notation)
    ///   DB1.DBD240       — dword at byte 240
    pub fn parse(s: &str) -> Result<S7Address, String> {
        // Try bit-with-dot form first: DB<n>.DBX<num>.<bit>
        let re_bit_dot =
            regex::Regex::new(r"^DB(\d+)\.DBX(\d+)\.(\d+)$").expect("valid S7 address regex");
        if let Some(caps) = re_bit_dot.captures(s) {
            let db_num: u16 = caps[1]
                .parse()
                .map_err(|e| format!("invalid DB number: {e}"))?;
            let byte: usize = caps[2]
                .parse()
                .map_err(|e| format!("invalid byte offset: {e}"))?;
            let bit: u8 = caps[3]
                .parse()
                .map_err(|e| format!("invalid bit offset: {e}"))?;
            if bit > 7 {
                return Err(format!("bit index must be 0..7, got {bit}"));
            }
            return Ok(S7Address {
                db_num,
                byte_offset: byte,
                bit_offset: Some(bit),
            });
        }
        // Then try the simple form: DB<n>.<DB{W|D|B|X})<num>
        let re_simple =
            regex::Regex::new(r"^DB(\d+)\.(DB[WDBX])(\d+)$").expect("valid S7 address regex");
        let caps = re_simple.captures(s).ok_or_else(|| {
            format!("invalid address format: {s} (expected DB<n>.DBW<n>, DB<n>.DBX<n>.<bit>, etc.)")
        })?;

        let db_num: u16 = caps[1]
            .parse()
            .map_err(|e| format!("invalid DB number: {e}"))?;
        let offset: usize = caps[3]
            .parse()
            .map_err(|e| format!("invalid offset: {e}"))?;
        let area = &caps[2];

        match area {
            "DBW" => Ok(S7Address {
                db_num,
                byte_offset: offset,
                bit_offset: None,
            }),
            "DBD" => Ok(S7Address {
                db_num,
                byte_offset: offset,
                bit_offset: None,
            }),
            "DBB" => Ok(S7Address {
                db_num,
                byte_offset: offset,
                bit_offset: None,
            }),
            "DBX" => Ok(S7Address {
                db_num,
                byte_offset: offset / 8,
                bit_offset: Some((offset % 8) as u8),
            }),
            _ => Err(format!("unknown area type: {area}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_s7_word() {
        let a = S7Address::parse("DB5999.DBW22").unwrap();
        assert_eq!(a.db_num, 5999);
        assert_eq!(a.byte_offset, 22);
        assert_eq!(a.bit_offset, None);
    }

    #[test]
    fn test_parse_s7_dword() {
        let a = S7Address::parse("DB10.DBD240").unwrap();
        assert_eq!(a.db_num, 10);
        assert_eq!(a.byte_offset, 240);
        assert_eq!(a.bit_offset, None);
    }

    #[test]
    fn test_parse_s7_bit_with_dot() {
        // Real S7-300 style: DB<n>.DBX<byte>.<bit>
        let a = S7Address::parse("DB5997.DBX10.1").unwrap();
        assert_eq!(a.db_num, 5997);
        assert_eq!(a.byte_offset, 10);
        assert_eq!(a.bit_offset, Some(1));
    }

    #[test]
    fn test_parse_s7_bit_byte7() {
        let a = S7Address::parse("DB1.DBX7.0").unwrap();
        assert_eq!(a.db_num, 1);
        assert_eq!(a.byte_offset, 7);
        assert_eq!(a.bit_offset, Some(0));
    }

    #[test]
    fn test_parse_s7_legacy_bit() {
        // Linear bit: 10 = byte 1, bit 2
        let a = S7Address::parse("DB5.DBX10").unwrap();
        assert_eq!(a.db_num, 5);
        assert_eq!(a.byte_offset, 1);
        assert_eq!(a.bit_offset, Some(2));
    }

    #[test]
    fn test_parse_s7_bit_out_of_range() {
        assert!(S7Address::parse("DB1.DBX10.8").is_err());
    }

    #[test]
    fn test_parse_s7_invalid() {
        assert!(S7Address::parse("DB10.DBW").is_err());
        assert!(S7Address::parse("invalid").is_err());
        assert!(S7Address::parse("DB10.DBW22.1").is_err());
    }
}

// ---------------------------------------------------------------------------
// PLC error types (D005)
// ---------------------------------------------------------------------------

/// Classification of PLC communication failures.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlcError {
    Timeout(String),
    Disconnected(String),
    AddressOutOfRange { address: String, detail: String },
    DecodeError { detail: String },
    ProtocolError(String),
    PlcUnavailable(String),
    Internal(String),
}

impl PlcError {
    pub fn bad_class(&self) -> BadClass {
        match self {
            PlcError::Timeout(_) => BadClass::Timeout,
            PlcError::Disconnected(_) => BadClass::Disconnected,
            PlcError::AddressOutOfRange { .. } => BadClass::AddressOutOfRange,
            PlcError::DecodeError { .. } => BadClass::DecodeError,
            PlcError::ProtocolError(_) => BadClass::ProtocolError,
            PlcError::PlcUnavailable(_) => BadClass::PlcUnavailable,
            PlcError::Internal(_) => BadClass::Internal,
        }
    }

    pub fn detail(&self) -> &str {
        match self {
            PlcError::Timeout(d) => d,
            PlcError::Disconnected(d) => d,
            PlcError::AddressOutOfRange { detail, .. } => detail,
            PlcError::DecodeError { detail } => detail,
            PlcError::ProtocolError(d) => d,
            PlcError::PlcUnavailable(d) => d,
            PlcError::Internal(d) => d,
        }
    }
}

impl fmt::Display for PlcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlcError::Timeout(d) => write!(f, "timeout: {d}"),
            PlcError::Disconnected(d) => write!(f, "disconnected: {d}"),
            PlcError::AddressOutOfRange { address, detail } => {
                write!(f, "address {address} out of range: {detail}")
            }
            PlcError::DecodeError { detail } => write!(f, "decode error: {detail}"),
            PlcError::ProtocolError(d) => write!(f, "protocol error: {d}"),
            PlcError::PlcUnavailable(d) => write!(f, "PLC unavailable: {d}"),
            PlcError::Internal(d) => write!(f, "internal: {d}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Read items and results
// ---------------------------------------------------------------------------

impl std::error::Error for PlcError {}
#[derive(Debug, Clone)]
pub struct ReadItem {
    pub db_num: u16,
    pub start: usize,
    pub len: usize,
}

#[derive(Debug, Clone)]
pub struct ReadResult {
    pub item: ReadItem,
    pub data: Vec<u8>,
}

// ---------------------------------------------------------------------------
// Tag snapshot (in-memory)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagSnapshot {
    pub tag_id: i64,
    pub plc_id: i64,
    pub name: String,
    pub address: String,
    pub data_type: DataType,
    pub last_value_i: Option<i64>,
    pub last_value_f: Option<f64>,
    pub last_ts: Option<TimestampUs>,
}

// ---------------------------------------------------------------------------
// Poll cycle info
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct PollCycleInfo {
    pub plc_id: i64,
    pub poll_group_id: i64,
    pub seq: u64,
    pub started_ts: TimestampUs,
    pub finished_ts: TimestampUs,
    pub target_ms: u64,
    pub actual_ms: f64,
    pub ok: bool,
    pub error: Option<String>,
}

// ---------------------------------------------------------------------------
// Read plan
// ---------------------------------------------------------------------------

/// Whether a span is a contiguous block read or a multi-variable read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanKind {
    /// Single contiguous block read via `read_db(db, start, len)`.
    BlockRead,
    /// Multi-variable read via `read_multi(items)`.
    /// Each item is decoded independently from its own data buffer.
    MultiRead,
}

#[derive(Debug, Clone)]
pub struct ReadSpan {
    pub db_num: u16,
    pub byte_offset: usize,
    pub byte_len: usize,
    pub tag_ids: Vec<i64>,
    pub poll_group_id: i64,
    /// What kind of read this span requires.
    pub kind: SpanKind,
    /// Per-tag offsets for MultiRead spans (offset into the multi-read item).
    /// For BlockRead spans, each tag's offset is `tag.byte_offset - span.byte_offset`.
    pub multi_items: Vec<ReadItem>,
}

#[derive(Debug, Clone)]
pub struct ReadPlan {
    pub plc_id: i64,
    pub poll_group_id: i64,
    pub spans: Vec<ReadSpan>,
}

// ---------------------------------------------------------------------------
// Tag row from DB
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct TagRow {
    pub id: i64,
    pub plc_id: i64,
    pub name: String,
    pub address: String,
    pub db_num: u16,
    pub byte_offset: usize,
    pub bit_offset: Option<u8>,
    pub byte_size: usize,
    pub data_type: DataType,
    pub poll_group_id: i64,
    pub store_mode: StoreMode,
    pub storage_hook: StorageHook,
    pub hook_params_json: Option<String>,
    pub enabled: bool,
    pub bad: bool,
    pub bad_class: Option<BadClass>,
    pub bad_detail: Option<String>,
}

// ---------------------------------------------------------------------------
// Status types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlcStatus {
    pub name: String,
    pub connected: bool,
    pub last_error: Option<String>,
    pub poll_groups: Vec<PollGroupStatus>,
    pub tags: TagCounts,
    pub problems: Vec<Problem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PollGroupStatus {
    pub name: String,
    pub target_ms: u64,
    pub actual_ms: f64,
    pub overrun_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagCounts {
    pub active: u64,
    pub bad: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Problem {
    pub kind: String,
    pub tag: String,
    pub address: String,
    pub bad_class: String,
    pub detail: String,
}

// ---------------------------------------------------------------------------
// Exit codes (D010)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitCode {
    Ok = 0,
    BadInput = 1,
    PlcError = 2,
    Overload = 3,
    Internal = 4,
}
