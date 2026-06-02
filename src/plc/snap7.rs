/// Snap7 FFI bindings scaffold.
///
/// This module provides a `Snap7Client` that implements `PlcClient`
/// via `libsnap7` FFI. The library is loaded at runtime (or linked)
/// and calls are marshalled through `libc` FFI.
///
/// # Safety
///
/// Raw FFI calls are `unsafe`. Each public function documents its
/// safety preconditions.

use crate::plc::client::PlcClient;
use crate::types::*;
use libc::c_uint;
use log::{debug, error, info};
use std::ffi::CString;

// ---------------------------------------------------------------------------
// Snap7 FFI constants
// ---------------------------------------------------------------------------

const S7_CLIENT_DEFAULT: i32 = 0;
const S7_ISO_ON_TCP: i32 = 1;
const S7_OVER_PNI: i32 = 2;
const S7_OVER_INDUSTRIAL_ETHERNET: i32 = 3;

const S7AREA_DB: i32 = 0x84;

const S7WL_BYTE: i32 = 0x02;
const S7WL_WORD: i32 = 0x04;
const S7WL_DWORD: i32 = 0x06;

// ---------------------------------------------------------------------------
// FFI declarations (external from libsnap7)
// ---------------------------------------------------------------------------

mod ffi {
    #![allow(non_camel_case_types, dead_code)]
    use libc::{c_char, c_int, c_uint, c_void};

    pub type s7_object = c_void;

    extern "C" {
        pub fn Cli_Create() -> *mut s7_object;
        pub fn Cli_Destroy(client: *mut s7_object);
        pub fn Cli_ConnectTo(
            client: *mut s7_object,
            address: *const c_char,
            rack: c_int,
            slot: c_int,
        ) -> c_int;
        pub fn Cli_Disconnect(client: *mut s7_object);
        pub fn Cli_Connected(client: *const s7_object) -> c_int;
        pub fn Cli_GetParam(
            client: *const s7_object,
            param_number: c_int,
            value: *mut c_void,
        ) -> c_int;
        pub fn Cli_SetParam(
            client: *mut s7_object,
            param_number: c_int,
            value: *mut c_void,
        ) -> c_int;
        pub fn Cli_ReadArea(
            client: *mut s7_object,
            area: c_int,
            db_number: c_uint,
            start: c_uint,
            amount: c_uint,
            word_len: c_int,
            data: *mut c_void,
        ) -> c_int;
        pub fn Cli_WriteArea(
            client: *mut s7_object,
            area: c_int,
            db_number: c_uint,
            start: c_uint,
            amount: c_uint,
            word_len: c_int,
            data: *const c_void,
        ) -> c_int;
        pub fn Cli_LastError(client: *const s7_object) -> c_int;
        pub fn Cli_ErrorText(
            err_no: c_int,
            buffer: *mut c_char,
            size: c_int,
        );
    }
}

// ---------------------------------------------------------------------------
// Snap7 error mapping
// ---------------------------------------------------------------------------

fn map_snap7_error(err_code: i32, context: &str) -> PlcError {
    match err_code {
        0 => PlcError::Internal(format!("{context}: unexpected zero error")),
        // evrTimeOut
        0x00010100..=0x000101FF => PlcError::Timeout(format!("{context}: timeout (code 0x{err_code:x})")),
        // evrNegotiationPDU, evrCliNegotiatingPDU
        0x00100000..=0x0010FFFF => PlcError::ProtocolError(format!("{context}: PDU negotiation error (0x{err_code:x})")),
        // ISO connection errors
        0x00020000..=0x0002FFFF => PlcError::Disconnected(format!("{context}: connection error (0x{err_code:x})")),
        // Address errors
        0x00A00000..=0x00A0FFFF => PlcError::AddressOutOfRange {
            address: context.to_string(),
            detail: format!("Snap7 error 0x{err_code:x}"),
        },
        _ => {
            // Try to get error text
            let mut buf = [0i8; 256];
            unsafe {
                ffi::Cli_ErrorText(err_code, buf.as_mut_ptr(), buf.len() as i32);
            }
            let c_str = unsafe { std::ffi::CStr::from_ptr(buf.as_ptr()) };
            let text = c_str.to_string_lossy();
            PlcError::ProtocolError(format!("{context}: {text} (0x{err_code:x})"))
        }
    }
}

// ---------------------------------------------------------------------------
// Snap7Client
// ---------------------------------------------------------------------------

/// A PLC client backed by a live Snap7 connection.
pub struct Snap7Client {
    handle: *mut ffi::s7_object,
    host: String,
    rack: i32,
    slot: i32,
    connected: bool,
}

unsafe impl Send for Snap7Client {}

impl Snap7Client {
    /// Create a new Snap7 client targeting the given host/rack/slot.
    ///
    /// Does not connect until `connect()` is called.
    pub fn new(host: &str, rack: i32, slot: i32) -> Self {
        let handle = unsafe { ffi::Cli_Create() };
        if handle.is_null() {
            panic!("Snap7 Cli_Create returned null pointer");
        }

        // Set connection type to ISO over TCP
        let conn_type = S7_ISO_ON_TCP;
        let pconn = &conn_type as *const i32 as *mut libc::c_void;
        unsafe {
            ffi::Cli_SetParam(handle, 1, pconn); // p_u16_ConnType = 1
        }

        Snap7Client {
            handle,
            host: host.to_string(),
            rack,
            slot,
            connected: false,
        }
    }

    /// Convert Snap7 error codes to our type.
    fn last_error(&self, context: &str) -> PlcError {
        let code = unsafe { ffi::Cli_LastError(self.handle) };
        map_snap7_error(code, context)
    }
}

impl Drop for Snap7Client {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            if self.connected {
                unsafe { ffi::Cli_Disconnect(self.handle) };
            }
            unsafe { ffi::Cli_Destroy(self.handle) };
        }
    }
}

impl PlcClient for Snap7Client {
    fn connect(&mut self) -> Result<(), PlcError> {
        let addr = CString::new(self.host.clone())
            .map_err(|e| PlcError::Internal(format!("invalid hostname: {e}")))?;

        let err = unsafe { ffi::Cli_ConnectTo(self.handle, addr.as_ptr(), self.rack, self.slot) };

        if err == 0 {
            self.connected = true;
            info!("connected to {} (rack={}, slot={})", self.host, self.rack, self.slot);
            Ok(())
        } else {
            let plc_err = map_snap7_error(err, &format!("connect to {}", self.host));
            error!("{}", plc_err);
            Err(plc_err)
        }
    }

    fn disconnect(&mut self) {
        if self.connected {
            unsafe { ffi::Cli_Disconnect(self.handle) };
            self.connected = false;
            info!("disconnected from {}", self.host);
        }
    }

    fn is_connected(&self) -> bool {
        if self.handle.is_null() {
            return false;
        }
        let rc = unsafe { ffi::Cli_Connected(self.handle) };
        rc != 0
    }

    fn pdu_len(&mut self) -> Result<usize, PlcError> {
        let mut pdu: i32 = 0;
        let err = unsafe {
            ffi::Cli_GetParam(
                self.handle,
                10, // p_i32_PDULength
                (&mut pdu as *mut i32) as *mut libc::c_void,
            )
        };
        if err != 0 {
            return Err(self.last_error("get PDU length"));
        }
        if pdu <= 0 {
            return Err(PlcError::Internal(format!("invalid PDU length: {pdu}")));
        }
        Ok(pdu as usize)
    }

    fn read_db(&mut self, db: u16, start: usize, len: usize) -> Result<Vec<u8>, PlcError> {
        // Cap reads to prevent buffer overruns
        if len > 65535 {
            return Err(PlcError::AddressOutOfRange {
                address: format!("DB{db}.DBB{start}"),
                detail: format!("read length {len} exceeds Snap7 maximum"),
            });
        }

        let mut buffer = vec![0u8; len];

        let err = unsafe {
            ffi::Cli_ReadArea(
                self.handle,
                S7AREA_DB,
                db as c_uint,
                start as c_uint,
                len as c_uint,
                S7WL_BYTE,
                buffer.as_mut_ptr() as *mut libc::c_void,
            )
        };

        if err != 0 {
            return Err(map_snap7_error(
                err,
                &format!("read DB{db} start={start} len={len}"),
            ));
        }

        debug!("read DB{db} start={start} len={len}: {} bytes", buffer.len());
        Ok(buffer)
    }

    fn read_multi(&mut self, items: &[ReadItem]) -> Result<Vec<ReadResult>, PlcError> {
        // For now, implement read_multi as sequential single reads.
        // A future optimization can use multiple read jobs in one PDU.
        let mut results = Vec::with_capacity(items.len());
        for item in items {
            let data = self.read_db(item.db_num, item.start, item.len)?;
            results.push(ReadResult {
                item: item.clone(),
                data,
            });
        }
        Ok(results)
    }

    fn label(&self) -> String {
        format!("{}:{}:{}", self.host, self.rack, self.slot)
    }
}

// ---------------------------------------------------------------------------
// Mock client for testing
// ---------------------------------------------------------------------------

/// A mock PLC client for testing without a live PLC.
pub struct MockPlcClient {
    connected: bool,
    label: String,
    pdu: usize,
    data: std::collections::HashMap<(u16, usize), Vec<u8>>,
}

impl MockPlcClient {
    pub fn new(label: &str) -> Self {
        MockPlcClient {
            connected: false,
            label: label.to_string(),
            pdu: 480,
            data: std::collections::HashMap::new(),
        }
    }

    /// Seed a DB block with known data.
    pub fn seed(&mut self, db: u16, start: usize, data: Vec<u8>) {
        self.data.insert((db, start), data);
    }
}

impl PlcClient for MockPlcClient {
    fn connect(&mut self) -> Result<(), PlcError> {
        self.connected = true;
        Ok(())
    }

    fn disconnect(&mut self) {
        self.connected = false;
    }

    fn is_connected(&self) -> bool {
        self.connected
    }

    fn pdu_len(&mut self) -> Result<usize, PlcError> {
        Ok(self.pdu)
    }

    fn read_db(&mut self, db: u16, start: usize, len: usize) -> Result<Vec<u8>, PlcError> {
        // Try exact match
        if let Some(data) = self.data.get(&(db, start)) {
            if data.len() >= len {
                return Ok(data[..len].to_vec());
            }
        }

        // Try prefix match
        for ((d, s), data) in &self.data {
            if *d == db && *s <= start && *s + data.len() >= start + len {
                let offset = start - s;
                return Ok(data[offset..offset + len].to_vec());
            }
        }

        Err(PlcError::AddressOutOfRange {
            address: format!("DB{db}.DBB{start}"),
            detail: "no mock data seeded for this range".to_string(),
        })
    }

    fn read_multi(&mut self, items: &[ReadItem]) -> Result<Vec<ReadResult>, PlcError> {
        let mut results = Vec::with_capacity(items.len());
        for item in items {
            let data = self.read_db(item.db_num, item.start, item.len)?;
            results.push(ReadResult {
                item: item.clone(),
                data,
            });
        }
        Ok(results)
    }

    fn label(&self) -> String {
        self.label.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_client_connect() {
        let mut client = MockPlcClient::new("test");
        assert!(!client.is_connected());
        client.connect().unwrap();
        assert!(client.is_connected());
        assert_eq!(client.pdu_len().unwrap(), 480);
    }

    #[test]
    fn test_mock_read_db() {
        let mut client = MockPlcClient::new("test");
        client.connect().unwrap();

        // Seed a 16-byte block at DB1 start=0
        let block: Vec<u8> = (0..16u8).collect();
        client.seed(1, 0, block);

        let data = client.read_db(1, 4, 4).unwrap();
        assert_eq!(data, vec![4, 5, 6, 7]);
    }

    #[test]
    fn test_mock_read_missing() {
        let mut client = MockPlcClient::new("test");
        client.connect().unwrap();

        let result = client.read_db(99, 0, 4);
        assert!(result.is_err());
    }
}
