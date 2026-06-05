/// S7 PLC client — pure Rust via the `rust7` crate.
///
/// Replaces the earlier Snap7 FFI + `libloading` approach. No DLL needed,
/// no `unsafe` code, works out of the box on any platform.
///
/// On connect failure (PLC unreachable) the server logs an error and
/// the poller will retry on the next cycle.

use crate::plc::client::PlcClient;
use crate::types::*;
use log::{debug, error, info};
use rust7::client::{S7Client as Rust7Inner, S7Error};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Snap7Client — wraps rust7::client::S7Client
// ---------------------------------------------------------------------------

/// A PLC client backed by a native Rust S7 connection (no external DLL).
pub struct Snap7Client {
    inner: Rust7Inner,
    host: String,
    port: u16,
    rack: u16,
    slot: u16,
    connected: bool,
}

impl Snap7Client {
    /// Create a new client. Does not connect.
    ///
    /// `host` may include a port suffix (`host:102`). If absent, port 102 is used
    /// (standard ISO-on-TCP for Siemens S7).
    pub fn new(host: &str, rack: u16, slot: u16) -> Self {
        let (host_clean, port) = Self::parse_host(host);
        let mut inner = Rust7Inner::new();
        let _ = inner.set_connection_port(port);
        inner.set_timeout(3000, 3000, 3000).ok();

        Snap7Client {
            inner,
            host: host_clean,
            port,
            rack,
            slot,
            connected: false,
        }
    }

    /// Parse `host:port` → (host, port). Defaults to 102.
    fn parse_host(addr: &str) -> (String, u16) {
        if let Some(idx) = addr.rfind(':') {
            let host = &addr[..idx];
            if let Ok(port) = addr[idx + 1..].parse::<u16>() {
                return (host.to_string(), port);
            }
        }
        (addr.to_string(), 102)
    }

    /// Map rust7 errors to our PlcError.
    fn map_error(err: &S7Error, context: &str) -> PlcError {
        use PlcError::*;
        use S7Error::*;
        match err {
            TcpConnectionFailed => {
                if context.contains("connect") {
                    PlcUnavailable(format!("{}: {err}", context))
                } else {
                    Disconnected(format!("{}: {err}", context))
                }
            }
            ConnectionClosed => Disconnected(format!("{}: {err}", context)),
            IsoConnectionFailed => ProtocolError(format!("{}: {err}", context)),
            IsoFragmentedPacket | IsoInvalidHeader | IsoInvalidTelegram => {
                ProtocolError(format!("{}: {err}", context))
            }
            PduNegotiationFailed => ProtocolError(format!("{}: {err}", context)),
            S7NotFound => AddressOutOfRange {
                address: context.to_string(),
                detail: err.to_string(),
            },
            S7InvalidAddress => AddressOutOfRange {
                address: context.to_string(),
                detail: err.to_string(),
            },
            S7Unspecified => ProtocolError(format!("{}: {err}", context)),
            NotConnected => {
                if context.contains("connect") {
                    PlcUnavailable(format!("{}: {err}", context))
                } else {
                    Disconnected(format!("{}: {err}", context))
                }
            }
            _ => Internal(format!("{}: {err}", context)),
        }
    }
}

impl PlcClient for Snap7Client {
    fn connect(&mut self) -> Result<(), PlcError> {
        let ip = if self.host.starts_with("//") {
            &self.host[2..]
        } else {
            &self.host
        };

        // Strip trailing port if present
        let ip = ip.split(':').next().unwrap_or(ip);

        let result = self.inner.connect_rack_slot(ip, self.rack, self.slot);
        match result {
            Ok(()) => {
                self.connected = true;
                info!(
                    "connected to {} (rack={}, slot={}, port={})",
                    ip, self.rack, self.slot, self.port
                );
                // Try PDU negotiation
                let mut buf = vec![0u8; 4];
                match self.inner.read_db(1, 0, &mut buf) {
                    Ok(_) => debug!("PDU test read succeeded"),
                    Err(e) => debug!("PDU test read (expected if DB1 empty): {e}"),
                }
                Ok(())
            }
            Err(e) => {
                let plc_err = Self::map_error(&e, &format!("connect to {}", ip));
                error!("{}", plc_err);
                Err(plc_err)
            }
        }
    }

    fn disconnect(&mut self) {
        if self.connected {
            self.inner.disconnect();
            self.connected = false;
            info!("disconnected from {}", self.host);
        }
    }

    fn is_connected(&self) -> bool {
        self.connected
    }

    fn pdu_len(&mut self) -> Result<usize, PlcError> {
        // rust7 doesn't expose PDU length after negotiation.
        // Reasonable default for S7-1200/1500: 240 bytes.
        Ok(240)
    }

    fn read_db(&mut self, db: u16, start: usize, len: usize) -> Result<Vec<u8>, PlcError> {
        let mut buffer = vec![0u8; len];
        let result = self.inner.read_db(db, start as u16, &mut buffer);
        match result {
            Ok(()) => {
                debug!("read DB{db} start={start} len={len}: OK");
                Ok(buffer)
            }
            Err(e) => {
                let context = format!("read DB{db} start={start} len={len}");
                let plc_err = Self::map_error(&e, &context);

                // Mark disconnected on transport errors so poller reconnects
                if matches!(&e, S7Error::ConnectionClosed | S7Error::TcpConnectionFailed | S7Error::NotConnected) {
                    self.connected = false;
                }
                Err(plc_err)
            }
        }
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
pub struct MockPlcClient {
    connected: bool,
    label: String,
    pdu: usize,
    data: HashMap<(u16, usize), Vec<u8>>,
    /// If set, every read returns this error.
    pub fail_read_with: Option<PlcError>,
}

impl MockPlcClient {
    pub fn new(label: &str) -> Self {
        MockPlcClient {
            connected: false,
            label: label.to_string(),
            pdu: 480,
            data: HashMap::new(),
            fail_read_with: None,
        }
    }

    /// Seed read data for a specific DB/offset range.
    pub fn seed(&mut self, db: u16, start: usize, data: Vec<u8>) {
        self.data.insert((db, start), data);
    }

    /// Make subsequent reads fail with the given error.
    pub fn fail_reads_with(&mut self, err: PlcError) {
        self.fail_read_with = Some(err);
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
        if let Some(ref err) = self.fail_read_with {
            return Err(match err {
                PlcError::AddressOutOfRange { .. } => PlcError::AddressOutOfRange {
                    address: format!("DB{db}@{start}"),
                    detail: err.detail().to_string(),
                },
                other => other.clone(),
            });
        }
        if let Some(data) = self.data.get(&(db, start)) {
            if data.len() >= len {
                return Ok(data[..len].to_vec());
            }
            let mut padded = data.clone();
            padded.resize(len, 0);
            return Ok(padded);
        }
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
// Factory removed: use `turbos7_backend::create_plc_client` instead.
// The unified factory routes to rust7/turbos7/mock based on
// S7TRACE_PLC_BACKEND. Keep MockPlcClient here for tests.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_host_plain() {
        let (h, p) = Snap7Client::parse_host("192.168.0.1");
        assert_eq!(h, "192.168.0.1");
        assert_eq!(p, 102);
    }

    #[test]
    fn test_parse_host_with_port() {
        let (h, p) = Snap7Client::parse_host("192.168.0.1:2000");
        assert_eq!(h, "192.168.0.1");
        assert_eq!(p, 2000);
    }

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
        let data = client.read_db(42, 100, 4).unwrap();
        assert_eq!(data, vec![0x00; 4]);
    }

    #[test]
    fn test_mock_pdu_len() {
        let mut client = MockPlcClient::new("test");
        assert_eq!(client.pdu_len().unwrap(), 480);
    }
}
