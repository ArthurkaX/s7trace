/// Snap7 FFI bindings — runtime DLL loading via `libloading`.
///
/// `snap7.dll` is loaded at runtime (Windows). If the DLL is not available,
/// `Snap7Client::try_new()` returns `None`, and the server falls back to
/// `MockPlcClient`.
///
/// # Safety
///
/// Raw FFI calls are `unsafe`. Each public function documents its preconditions.

use crate::plc::client::PlcClient;
use crate::types::*;
use libc::{c_char, c_int, c_void};
use libloading::Library;
use log::{debug, error, info, warn};
use parking_lot::Mutex;
use std::ffi::CString;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Snap7 constants
// ---------------------------------------------------------------------------

const S7_CLIENT_DEFAULT: i32 = 0;
const S7_ISO_ON_TCP: i32 = 1;

const S7AREA_DB: i32 = 0x84;

const S7WL_BYTE: i32 = 0x02;

// Param IDs for Cli_GetParam / Cli_SetParam
const PDU_LENGTH_PARAM: i32 = 10; // p_i32_PDULength

// ---------------------------------------------------------------------------
// Dynamically loaded Snap7 library
// ---------------------------------------------------------------------------

/// Runtime-loaded `snap7.dll`. Holds all resolved function pointers.
pub struct Snap7Library {
    _lib: Library,

    // Lifecycle
    cli_create: unsafe extern "C" fn() -> *mut c_void,
    cli_destroy: unsafe extern "C" fn(*mut c_void),

    // Connection
    cli_connect_to:
        unsafe extern "C" fn(*mut c_void, *const c_char, c_int, c_int) -> c_int,
    cli_disconnect: unsafe extern "C" fn(*mut c_void),
    cli_connected: unsafe extern "C" fn(*const c_void) -> c_int,

    // Parameters
    cli_get_param:
        unsafe extern "C" fn(*const c_void, c_int, *mut c_void) -> c_int,
    cli_set_param: unsafe extern "C" fn(*mut c_void, c_int, *mut c_void) -> c_int,

    // I/O
    cli_read_area:
        unsafe extern "C" fn(*mut c_void, c_int, c_int, c_int, c_int, c_int, *mut c_void)
            -> c_int,

    // Error
    cli_last_error: unsafe extern "C" fn(*const c_void) -> c_int,
    cli_error_text: unsafe extern "C" fn(c_int, *mut c_char, c_int),
}

impl Snap7Library {
    /// Try to load `snap7.dll` and resolve all function symbols.
    pub fn try_load() -> Result<Self, String> {
        unsafe {
            let lib = Library::new("snap7.dll").map_err(|e| {
                format!("cannot load snap7.dll: {e}")
            })?;


            Ok(Snap7Library {
                cli_create: *lib.get(b"Cli_Create\0")
                    .map_err(|e| format!("Cli_Create: {e}"))?,
                cli_destroy: *lib.get(b"Cli_Destroy\0")
                    .map_err(|e| format!("Cli_Destroy: {e}"))?,
                cli_connect_to: *lib.get(b"Cli_ConnectTo\0")
                    .map_err(|e| format!("Cli_ConnectTo: {e}"))?,
                cli_disconnect: *lib.get(b"Cli_Disconnect\0")
                    .map_err(|e| format!("Cli_Disconnect: {e}"))?,
                cli_connected: *lib.get(b"Cli_Connected\0")
                    .map_err(|e| format!("Cli_Connected: {e}"))?,
                cli_get_param: *lib.get(b"Cli_GetParam\0")
                    .map_err(|e| format!("Cli_GetParam: {e}"))?,
                cli_set_param: *lib.get(b"Cli_SetParam\0")
                    .map_err(|e| format!("Cli_SetParam: {e}"))?,
                cli_read_area: *lib.get(b"Cli_ReadArea\0")
                    .map_err(|e| format!("Cli_ReadArea: {e}"))?,
                cli_last_error: *lib.get(b"Cli_LastError\0")
                    .map_err(|e| format!("Cli_LastError: {e}"))?,
                cli_error_text: *lib.get(b"Cli_ErrorText\0")
                    .map_err(|e| format!("Cli_ErrorText: {e}"))?,
                _lib: lib,
            })
        }
    }

    /// Create a new Snap7 client handle.
    pub fn create(&self) -> *mut c_void {
        unsafe { (self.cli_create)() }
    }

    /// Destroy a Snap7 client handle.
    pub fn destroy(&self, handle: *mut c_void) {
        unsafe { (self.cli_destroy)(handle) }
    }

    /// Connect to a PLC.
    pub fn connect_to(
        &self,
        handle: *mut c_void,
        address: *const c_char,
        rack: c_int,
        slot: c_int,
    ) -> c_int {
        unsafe { (self.cli_connect_to)(handle, address, rack, slot) }
    }

    /// Disconnect from a PLC.
    pub fn disconnect(&self, handle: *mut c_void) {
        unsafe { (self.cli_disconnect)(handle) }
    }

    /// Check if connected.
    pub fn is_connected(&self, handle: *const c_void) -> bool {
        unsafe { (self.cli_connected)(handle) != 0 }
    }

    /// Get a parameter value.
    pub fn get_param(&self, handle: *const c_void, param: c_int, value: *mut c_void) -> c_int {
        unsafe { (self.cli_get_param)(handle, param, value) }
    }

    /// Set a parameter value.
    pub fn set_param(&self, handle: *mut c_void, param: c_int, value: *mut c_void) -> c_int {
        unsafe { (self.cli_set_param)(handle, param, value) }
    }

    /// Read an area of PLC memory.
    #[allow(clippy::too_many_arguments)]
    pub fn read_area(
        &self,
        handle: *mut c_void,
        area: c_int,
        db_number: c_int,
        start: c_int,
        amount: c_int,
        word_len: c_int,
        data: *mut c_void,
    ) -> c_int {
        unsafe { (self.cli_read_area)(handle, area, db_number, start, amount, word_len, data) }
    }

    /// Get the last error code.
    pub fn last_error(&self, handle: *const c_void) -> c_int {
        unsafe { (self.cli_last_error)(handle) }
    }

    /// Convert an error code to human-readable text.
    pub fn error_text(&self, err_no: c_int, buffer: *mut c_char, size: c_int) {
        unsafe { (self.cli_error_text)(err_no, buffer, size) }
    }
}

// ---------------------------------------------------------------------------
// Snap7 error mapping
// ---------------------------------------------------------------------------

/// Map Snap7 error codes to `PlcError`.
fn map_snap7_error(err_code: i32, context: &str) -> PlcError {
    use PlcError::*;

    match err_code {
        0 => Internal(format!("{}: no error but called with error", context)),

        // libnoresults (0x00010000)
        err if err & 0x00010000 != 0 => {
            if err & 0x00000010 != 0 {
                AddressOutOfRange {
                    address: context.to_string(),
                    detail: "address out of range".into(),
                }
            } else if err & 0x00000040 != 0 {
                Timeout(context.to_string())
            } else if err & 0x00000001 != 0 {
                PlcUnavailable(context.to_string())
            } else if err & 0x00000100 != 0 {
                Disconnected(context.to_string())
            } else {
                ProtocolError(format!("{} (snap7 error 0x{err_code:08x})", context))
            }
        }
        // litexceptions (0x00020000) — protocol level
        err if err & 0x00020000 != 0 => ProtocolError(format!(
            "{} (snap7 error 0x{err_code:08x})",
            context
        )),

        // Parameter / function ref errors (0x00040000)
        err if err & 0x00040000 != 0 => Internal(format!("{} (snap7 0x{err_code:08x})", context)),

        _ => DecodeError { detail: format!("{} (snap7 error 0x{err_code:08x})", context) },
    }
}

/// Get the error text from snap7 for logging.
fn snap7_error_text(lib: &Snap7Library, err_code: i32) -> String {
    let mut buf = [0i8; 256];
    lib.error_text(err_code, buf.as_mut_ptr(), buf.len() as i32);
    let cstr = unsafe { std::ffi::CStr::from_ptr(buf.as_ptr()) };
    cstr.to_string_lossy().into_owned()
}

// ---------------------------------------------------------------------------
// Snap7Client
// ---------------------------------------------------------------------------

/// A PLC client backed by a live Snap7 connection via dynamically loaded DLL.
pub struct Snap7Client {
    lib: Arc<Snap7Library>,
    handle: *mut c_void,
    host: String,
    rack: i32,
    slot: i32,
    connected: bool,
}

unsafe impl Send for Snap7Client {}

impl Snap7Client {
    /// Try to create a new Snap7 client.
    ///
    /// Returns `None` if `snap7.dll` cannot be loaded (e.g. not installed).
    pub fn try_new(host: &str, rack: i32, slot: i32) -> Option<Self> {
        let lib = match Snap7Library::try_load() {
            Ok(lib) => Arc::new(lib),
            Err(e) => {
                warn!("Snap7 not available: {e}");
                return None;
            }
        };

        let handle = lib.create();
        if handle.is_null() {
            error!("Snap7 Cli_Create returned null pointer");
            return None;
        }

        // Set connection type to ISO over TCP
        let conn_type = S7_ISO_ON_TCP;
        let pconn = &conn_type as *const i32 as *mut c_void;
        lib.set_param(handle, 1, pconn); // p_u16_ConnType = 1

        Some(Snap7Client {
            lib,
            handle,
            host: host.to_string(),
            rack,
            slot,
            connected: false,
        })
    }

    /// Convert snap7 error code with human-readable text.
    fn last_error(&self, context: &str) -> PlcError {
        let code = self.lib.last_error(self.handle);
        let text = snap7_error_text(&self.lib, code);
        map_snap7_error(code, &format!("{}: {}", context, text))
    }
}

impl Drop for Snap7Client {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            if self.connected {
                self.lib.disconnect(self.handle);
            }
            self.lib.destroy(self.handle);
        }
    }
}

impl PlcClient for Snap7Client {
    fn connect(&mut self) -> Result<(), PlcError> {
        let addr = CString::new(self.host.clone())
            .map_err(|e| PlcError::Internal(format!("invalid hostname: {e}")))?;

        let err = self.lib.connect_to(self.handle, addr.as_ptr(), self.rack, self.slot);

        if err == 0 {
            self.connected = true;
            info!("connected to {} (rack={}, slot={})", self.host, self.rack, self.slot);
            Ok(())
        } else {
            let plc_err = self.last_error(&format!("connect to {}", self.host));
            error!("{}", plc_err);
            Err(plc_err)
        }
    }

    fn disconnect(&mut self) {
        if self.connected {
            self.lib.disconnect(self.handle);
            self.connected = false;
            info!("disconnected from {}", self.host);
        }
    }

    fn is_connected(&self) -> bool {
        if self.handle.is_null() {
            return false;
        }
        self.lib.is_connected(self.handle)
    }

    fn pdu_len(&mut self) -> Result<usize, PlcError> {
        let mut pdu: i32 = 0;
        let err = self.lib.get_param(
            self.handle,
            PDU_LENGTH_PARAM,
            (&mut pdu as *mut i32) as *mut c_void,
        );
        if err != 0 {
            return Err(self.last_error("get PDU length"));
        }
        if pdu <= 0 {
            return Err(PlcError::Internal(format!("invalid PDU length: {pdu}")));
        }
        Ok(pdu as usize)
    }

    fn read_db(&mut self, db: u16, start: usize, len: usize) -> Result<Vec<u8>, PlcError> {
        if len > 65535 {
            return Err(PlcError::AddressOutOfRange {
                address: format!("DB{db}.DBB{start}"),
                detail: format!("read length {len} exceeds Snap7 maximum"),
            });
        }

        let mut buffer = vec![0u8; len];

        let err = self.lib.read_area(
            self.handle,
            S7AREA_DB,
            db as c_int,
            start as c_int,
            len as c_int,
            S7WL_BYTE,
            buffer.as_mut_ptr() as *mut c_void,
        );

        if err != 0 {
            return Err(map_snap7_error(
                err,
                &snap7_error_text(&self.lib, err),
            ));
        }

        debug!("read DB{db} start={start} len={len}: {} bytes", buffer.len());
        Ok(buffer)
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
        format!("{}:{}:{}", self.host, self.rack, self.slot)
    }
}

// ---------------------------------------------------------------------------
// Mock client for testing
// ---------------------------------------------------------------------------

/// A mock PLC client for testing without a live PLC.
///
/// Supports `seed(db, start, data)` to pre-populate read responses.
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

    /// Seed read data for a specific DB/offset range.
    pub fn seed(&mut self, db: u16, start: usize, data: Vec<u8>) {
        self.data.insert((db, start), data);
    }
}

impl PlcClient for MockPlcClient {
    fn connect(&mut self) -> Result<(), PlcError> {
        self.connected = true;
        debug!("MockPlcClient({}) connected", self.label);
        Ok(())
    }

    fn disconnect(&mut self) {
        self.connected = false;
        debug!("MockPlcClient({}) disconnected", self.label);
    }

    fn is_connected(&self) -> bool {
        self.connected
    }

    fn pdu_len(&mut self) -> Result<usize, PlcError> {
        Ok(self.pdu)
    }

    fn read_db(&mut self, db: u16, start: usize, len: usize) -> Result<Vec<u8>, PlcError> {
        if let Some(data) = self.data.get(&(db, start)) {
            if data.len() >= len {
                return Ok(data[..len].to_vec());
            }
            let mut padded = data.clone();
            padded.resize(len, 0);
            return Ok(padded);
        }
        // Return zeros for unseeded reads (like a real PLC with zeroed memory)
        Ok(vec![0u8; len])
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

// ---------------------------------------------------------------------------
// Try to create a Snap7 client or fall back to mock
// ---------------------------------------------------------------------------

/// Create a `PlcClient` for the given PLC. Tries Snap7 first; falls back to Mock.
pub fn create_plc_client(plc_name: &str, host: &str) -> Arc<Mutex<dyn PlcClient + Send>> {
    // Try real Snap7 connection
    if let Some(client) = Snap7Client::try_new(host, 0, 0) {
        info!("PLC '{}': using Snap7 client for {}", plc_name, host);
        return Arc::new(Mutex::new(client));
    }

    // Fall back to mock
    warn!(
        "PLC '{}': snap7.dll not available, using MockPlcClient for {}",
        plc_name, host
    );
    Arc::new(Mutex::new(MockPlcClient::new(plc_name)))
}

// ---------------------------------------------------------------------------
// Re-export for convenience
// ---------------------------------------------------------------------------

/// Type alias for a shareable PLC client.
pub type SharedPlcClient = Arc<Mutex<dyn PlcClient + Send>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_basic_read() {
        let mut client = MockPlcClient::new("test");
        client.connect().unwrap();
        client.seed(1, 0, vec![0x00, 0x2A]);
        let data = client.read_db(1, 0, 2).unwrap();
        assert_eq!(data, vec![0x00, 0x2A]);
        client.disconnect();
        assert!(!client.is_connected());
    }

    #[test]
    fn test_mock_empty_read() {
        let mut client = MockPlcClient::new("test");
        client.connect().unwrap();
        // Unseeded read returns zeros
        let data = client.read_db(42, 100, 4).unwrap();
        assert_eq!(data, vec![0x00; 4]);
    }

    #[test]
    fn test_mock_pdu_len() {
        let mut client = MockPlcClient::new("test");
        assert_eq!(client.pdu_len().unwrap(), 480);
    }

    #[test]
    fn test_snap7_try_new_dll_not_found() {
        // Should gracefully return None when snap7.dll is absent
        let client = Snap7Client::try_new("127.0.0.1", 0, 0);
        assert!(client.is_none());
    }
}
