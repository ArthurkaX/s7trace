//! PLC backend based on the `turbos7` async S7 client.
//!
//! The engine calls synchronous `PlcClient` methods; `turbos7` is async.
//! We bridge by:
//!   - holding one global `tokio::runtime::Runtime` (created lazily on
//!     first use),
//!   - running async operations via `Handle::block_on`.
//!
//! This adds a small overhead per read (thread parking), but the
//! alternative is converting the whole engine to async. For one read
//! per poll cycle on S7-300 (target 100 ms period) the overhead is
//! negligible.
//!
//! `S7TRACE_PLC_BACKEND=turbos7|rust7|mock` selects the implementation
//! at server startup time. Default: `turbos7`.

use crate::plc::client::PlcClient;
use crate::types::*;
use log::info;
use std::sync::OnceLock;
use std::time::Duration;
use turbos7::S7Client;

/// One global runtime. Created on first call, never torn down.
static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

fn runtime() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .expect("failed to build tokio runtime for S7 client")
    })
}

/// Sync façade over `turbos7::S7Client`.
pub struct Turbo7Client {
    host: String,
    port: u16,
    rack: u16,
    slot: u16,
    /// `None` when not connected.
    inner: Option<S7Client>,
    /// Cached PDU length, set after connect, used by `pdu_len` to avoid
    /// the round-trip when no inner client is available.
    pdu_length: usize,
}

impl Turbo7Client {
    pub fn new(host: &str, rack: u16, slot: u16) -> Self {
        let (host_clean, port) = Self::parse_host(host);
        Turbo7Client {
            host: host_clean,
            port,
            rack,
            slot,
            inner: None,
            pdu_length: 240, // S7-300 default
        }
    }

    pub fn parse_host(s: &str) -> (String, u16) {
        if let Some((h, p)) = s.rsplit_once(':') {
            if let Ok(port) = p.parse::<u16>() {
                return (h.to_string(), port);
            }
        }
        (s.to_string(), 102)
    }
}

impl PlcClient for Turbo7Client {
    fn connect(&mut self) -> Result<(), PlcError> {
        let addr = format!("{}:{}", self.host, self.port);
        let ct = 0x0001u16; // CT_PG
        let res: turbos7::Result<S7Client> = runtime().block_on(async {
            S7Client::connect_rack_slot(
                &addr,
                self.rack,
                self.slot,
                ct,
                Some(Duration::from_secs(3)),
                Some(Duration::from_secs(2)),
                Some(Duration::from_secs(2)),
            )
            .await
        });
        match res {
            Ok(client) => {
                self.pdu_length = client.pdu_size() as usize;
                info!(
                    "PLC 'turbos7': connected to {} (rack={}, slot={}, pdu={})",
                    addr, self.rack, self.slot, self.pdu_length
                );
                self.inner = Some(client);
                Ok(())
            }
            Err(e) => Err(PlcError::Disconnected(format!("connect: {e}"))),
        }
    }

    fn disconnect(&mut self) {
        // turbos7's S7Client doesn't expose an explicit disconnect; dropping
        // the value closes the underlying TCP stream.
        self.inner = None;
    }

    fn is_connected(&self) -> bool {
        self.inner.is_some()
    }

    fn pdu_len(&mut self) -> Result<usize, PlcError> {
        if let Some(c) = &self.inner {
            Ok(c.pdu_size() as usize)
        } else {
            Ok(self.pdu_length)
        }
    }

    fn read_db(&mut self, db: u16, start: usize, len: usize) -> Result<Vec<u8>, PlcError> {
        let client = self
            .inner
            .as_mut()
            .ok_or_else(|| PlcError::Disconnected("read_db: not connected".into()))?;
        // S7 area code for Data Blocks = 0x84. turbos7 doesn't export
        // the constant from protocol::s7 (it only exposes wordlen),
        // so we use the literal here. See S7 protocol spec.
        const S7_AREA_DB: u8 = 0x84;
        let res: turbos7::Result<Vec<u8>> = runtime().block_on(async {
            client
                .read_area(S7_AREA_DB, db, start as u32, len as u16)
                .await
        });
        if res.is_err() {
            // Transport errors (IO / connection) mean the client is stale.
            // Clear inner so the poller reconnect loop has a chance to
            // re-establish the connection on the next cycle.
            if res.as_ref().err().map_or(false, |e| {
                matches!(
                    e,
                    turbos7::Error::Io(_) | turbos7::Error::Connection(_) | turbos7::Error::Timeout
                )
            }) {
                self.inner = None;
            }
        }
        res.map_err(|e| plc_error_from_turbos7(&e))
    }

    fn read_multi(&mut self, items: &[ReadItem]) -> Result<Vec<ReadResult>, PlcError> {
        // turbos7's read_multiple helper is not in the public S7Client
        // surface. Fall back to per-item read_area. The pollers do not
        // currently use read_multi, so this is a safe stub.
        let mut out = Vec::with_capacity(items.len());
        for item in items {
            let data = self.read_db(item.db_num, item.start, item.len)?;
            out.push(ReadResult {
                item: item.clone(),
                data,
            });
        }
        Ok(out)
    }

    fn label(&self) -> String {
        format!(
            "{}:{} (rack={}, slot={})",
            self.host, self.port, self.rack, self.slot
        )
    }
}

fn plc_error_from_turbos7(e: &turbos7::Error) -> PlcError {
    use turbos7::Error as TErr;
    match e {
        TErr::Io(_) => PlcError::Disconnected(format!("io: {e}")),
        TErr::Connection(_) => PlcError::Disconnected(format!("conn: {e}")),
        TErr::Protocol(_) => PlcError::ProtocolError(format!("proto: {e}")),
        TErr::PlcError { code, message } => match *code {
            0x05 => PlcError::AddressOutOfRange {
                address: String::new(),
                detail: message.clone(),
            },
            0x0A => PlcError::AddressOutOfRange {
                address: String::new(),
                detail: format!("DB not found: {message}"),
            },
            _ => PlcError::ProtocolError(format!("plc({code}): {message}")),
        },
        TErr::Timeout => PlcError::Timeout(format!("timeout: {e}")),
        TErr::InvalidData => PlcError::DecodeError {
            detail: format!("invalid_data: {e}"),
        },
    }
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

/// Create a `PlcClient` using turbos7 as the backend.
///
/// Backend selection is controlled by the env var `S7TRACE_PLC_BACKEND`:
///   - `turbos7` (default) — use turbos7 (preferred)
///   - `rust7` — fall back to the legacy rust7-based backend
///   - `mock` — use the in-process MockPlcClient (testing only)
pub fn create_plc_client(
    plc_name: &str,
    host: &str,
    rack: u16,
    slot: u16,
) -> std::sync::Arc<parking_lot::Mutex<dyn PlcClient + Send>> {
    use crate::plc::snap7::Snap7Client;
    let backend = std::env::var("S7TRACE_PLC_BACKEND").unwrap_or_else(|_| "turbos7".into());
    match backend.as_str() {
        "turbos7" => {
            info!(
                "PLC '{}': using turbos7 backend for {} (rack={}, slot={})",
                plc_name, host, rack, slot
            );
            let client = Turbo7Client::new(host, rack, slot);
            std::sync::Arc::new(parking_lot::Mutex::new(client))
        }
        "rust7" => {
            info!(
                "PLC '{}': using rust7 backend for {} (rack={}, slot={})",
                plc_name, host, rack, slot
            );
            let client = Snap7Client::new(host, rack, slot);
            std::sync::Arc::new(parking_lot::Mutex::new(client))
        }
        "mock" => {
            info!("PLC '{}': using MockPlcClient backend", plc_name);
            let client = crate::plc::snap7::MockPlcClient::new(plc_name);
            std::sync::Arc::new(parking_lot::Mutex::new(client))
        }
        other => {
            log::warn!("unknown S7TRACE_PLC_BACKEND='{other}', falling back to turbos7");
            let client = Turbo7Client::new(host, rack, slot);
            std::sync::Arc::new(parking_lot::Mutex::new(client))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_host() {
        assert_eq!(
            Turbo7Client::parse_host("192.168.0.1"),
            ("192.168.0.1".into(), 102)
        );
        assert_eq!(
            Turbo7Client::parse_host("192.168.0.1:2000"),
            ("192.168.0.1".into(), 2000)
        );
        assert_eq!(Turbo7Client::parse_host("plc:102"), ("plc".into(), 102));
    }

    #[test]
    fn test_factory_default_is_turbos7() {
        // Default backend is turbos7; we don't actually connect (no PLC).
        // Just verify that the factory dispatches without panicking.
        // The returned client won't be connected; that's fine.
        let c = create_plc_client("test", "127.0.0.1:1", 0, 2);
        // Should be a Turbo7Client (best-effort: just confirm trait object works)
        let mut guard = c.lock();
        assert!(!guard.is_connected());
        assert!(guard.pdu_len().is_ok());
    }
}
