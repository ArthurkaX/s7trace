/// PLC client abstraction — trait and core types.
use crate::types::*;

/// Trait for PLC communication.
///
/// The engine depends on this trait rather than on Snap7 directly,
/// making planner, storage, and status logic testable without a live PLC.
pub trait PlcClient: Send {
    /// Open the connection. May retry internally; returns error if unreachable.
    fn connect(&mut self) -> Result<(), PlcError>;

    /// Close the connection gracefully.
    fn disconnect(&mut self);

    /// Returns `true` if the connection is currently established.
    fn is_connected(&self) -> bool;

    /// Returns the negotiated PDU length in bytes.
    fn pdu_len(&mut self) -> Result<usize, PlcError>;

    /// Read a contiguous block from a data block.
    ///
    /// `db` — DB number.
    /// `start` — byte offset (0-indexed).
    /// `len` — number of bytes to read.
    fn read_db(&mut self, db: u16, start: usize, len: usize) -> Result<Vec<u8>, PlcError>;

    /// Read multiple items in one S7 multi-variable request.
    fn read_multi(&mut self, items: &[ReadItem]) -> Result<Vec<ReadResult>, PlcError>;

    /// Return a human-readable label for the target (e.g. host:port).
    fn label(&self) -> String;
}
