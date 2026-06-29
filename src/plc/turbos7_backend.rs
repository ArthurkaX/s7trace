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

use crate::engine::plan::{
    pad_even, S7_MULTI_REQ_HEADER, S7_MULTI_REQ_PER_ITEM, S7_MULTI_RESP_HEADER,
    S7_MULTI_RESP_PER_ITEM, S7_PDU_SAFETY_MARGIN,
};
use crate::plc::client::PlcClient;
use crate::types::*;
use std::ops::Range;
use std::sync::OnceLock;
use std::time::Duration;
use tracing::{info, warn};
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
    /// Adaptive per-batch item cap for multi-reads. Starts at the configured
    /// value and is lowered automatically when the PLC truncates a response
    /// (S7-300s answer fewer items than requested); sticky-down, so it
    /// converges to a cap the device actually honours. See `read_multi`.
    multi_cap: usize,
}

impl Turbo7Client {
    pub fn new(host: &str, rack: u16, slot: u16) -> Self {
        let (host_clean, port) = Self::parse_host(host);
        let multi_cap = match Self::max_batch_items() {
            0 => 3, // 0 (legacy "no cap") -> start at the default and adapt
            n => n,
        };
        Turbo7Client {
            host: host_clean,
            port,
            rack,
            slot,
            inner: None,
            pdu_length: 240, // S7-300 default
            multi_cap,
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

    /// Initial maximum items per multi-read batch, controlled by the
    /// `S7TRACE_MULTI_BATCH_MAX` env var (default 3).
    ///
    /// This is only the *starting* cap: `read_multi` lowers a client's cap
    /// automatically when the PLC truncates a response, so a too-optimistic
    /// value self-corrects. A value of 0 (legacy "no cap") starts at the
    /// default of 3.
    pub fn max_batch_items() -> usize {
        match std::env::var("S7TRACE_MULTI_BATCH_MAX") {
            Ok(v) => v.parse().unwrap_or_else(|_| {
                tracing::warn!(
                    "S7TRACE_MULTI_BATCH_MAX='{v}' is not a valid usize; using default 3"
                );
                3
            }),
            Err(_) => 3,
        }
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
            if res.as_ref().err().is_some_and(|e| {
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
        const S7_AREA_DB: u8 = 0x84;
        let pdu = self.pdu_length;

        // Adaptive multi-read: some PLCs (notably S7-300) answer a multi-read
        // with fewer items than requested even when the request fits the PDU.
        // Rather than fail the whole cycle, halve this client's per-batch cap
        // and retry the entire read; the cap is sticky-down so it converges to
        // a value the device honours. The loop terminates because the cap
        // strictly decreases and bottoms out at 1.
        loop {
            let cap = self.multi_cap;
            let batches = plan_batches(items, pdu, cap);
            let mut all_results: Vec<ReadResult> = Vec::with_capacity(items.len());
            let mut retry = false;

            for (batch_idx, range) in batches.iter().enumerate() {
                let batch = &items[range.start..range.end];
                let t_items: Vec<turbos7::ReadItem> = batch
                    .iter()
                    .map(|it| turbos7::ReadItem {
                        area: S7_AREA_DB,
                        db_number: it.db_num,
                        start: it.start as u32,
                        size: it.len as u16,
                    })
                    .collect();

                // Isolate the client borrow so NLL releases it before
                // we potentially mutate self.inner on transport error.
                let res = {
                    let client = self.inner.as_mut().ok_or_else(|| {
                        PlcError::Disconnected("read_multi: not connected".into())
                    })?;
                    runtime().block_on(async { client.read_multiple(&t_items).await })
                };

                // Transport errors mean the TCP connection is dead — clear inner
                // so the poller reconnect loop re-establishes it next cycle.
                if let Err(e) = &res {
                    if matches!(
                        e,
                        turbos7::Error::Io(_)
                            | turbos7::Error::Connection(_)
                            | turbos7::Error::Timeout
                    ) {
                        self.inner = None;
                    }
                }
                let results = res.map_err(|e| plc_error_from_turbos7(&e))?;

                // Truncation: fewer items came back than we asked for. Lower the
                // cap and retry the whole read (unless we are already at 1).
                if results.len() < batch.len() {
                    if cap > 1 {
                        let new_cap = (cap / 2).max(1);
                        warn!(
                            "read_multi: response truncated ({} of {} items); \
                             lowering multi-read cap {} -> {}",
                            results.len(),
                            batch.len(),
                            cap,
                            new_cap
                        );
                        self.multi_cap = new_cap;
                        retry = true;
                        break;
                    }
                    return Err(PlcError::ProtocolError(format!(
                        "read_multi batch {}: response truncated ({} of {}) at minimum cap",
                        batch_idx,
                        results.len(),
                        batch.len()
                    )));
                }
                if results.len() != batch.len() {
                    return Err(PlcError::ProtocolError(format!(
                        "read_multi batch {}: expected {} results, got {}",
                        batch_idx,
                        batch.len(),
                        results.len()
                    )));
                }
                for (item, r) in batch.iter().zip(results.iter()) {
                    if !r.success {
                        return Err(PlcError::ProtocolError(format!(
                            "read_multi batch {}: item DB{} @{} failed (error_code={:?})",
                            batch_idx, item.db_num, item.start, r.error_code
                        )));
                    }
                }
                all_results.extend(batch.iter().zip(results).map(|(item, r)| ReadResult {
                    item: item.clone(),
                    data: r.data,
                }));
            }

            if retry {
                continue;
            }
            return Ok(all_results);
        }
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

/// Split a list of multi-read items into batches, each fitting within the
/// negotiated PDU byte budget on both the request and response sides, and
/// respecting an optional per-batch item-count cap.
///
/// Returns a vector of `Range<usize>` into the original `items` slice so the
/// caller can slice batches and concatenate results in the original order.
///
/// `max_items` — 0 means "no item-count cap" (rely on PDU budget alone).
pub(crate) fn plan_batches(
    items: &[ReadItem],
    pdu_bytes: usize,
    max_items: usize,
) -> Vec<Range<usize>> {
    let budget = pdu_bytes.saturating_sub(S7_PDU_SAFETY_MARGIN);
    let cap = if max_items == 0 {
        usize::MAX
    } else {
        max_items
    };

    let mut batches: Vec<Range<usize>> = Vec::new();
    let mut i = 0;
    while i < items.len() {
        let mut req_cost = S7_MULTI_REQ_HEADER;
        let mut resp_cost = S7_MULTI_RESP_HEADER;
        let mut j = i;
        while j < items.len() && (j - i) < cap {
            let item_req = S7_MULTI_REQ_PER_ITEM;
            let item_resp = S7_MULTI_RESP_PER_ITEM + pad_even(items[j].len);
            if req_cost + item_req > budget || resp_cost + item_resp > budget {
                break;
            }
            req_cost += item_req;
            resp_cost += item_resp;
            j += 1;
        }
        batches.push(i..j);
        i = j;
    }
    batches
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
            tracing::warn!("unknown S7TRACE_PLC_BACKEND='{other}', falling back to turbos7");
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
        let c = create_plc_client("test", "127.0.0.1:1", 0, 2);
        let mut guard = c.lock();
        assert!(!guard.is_connected());
        assert!(guard.pdu_len().is_ok());
    }

    // -----------------------------------------------------------------------
    // plan_batches tests
    // -----------------------------------------------------------------------

    fn item(len: usize) -> ReadItem {
        ReadItem {
            db_num: 1,
            start: 0,
            len,
        }
    }

    #[test]
    fn plan_batches_single_item_always_one_batch() {
        let items = vec![item(4)];
        let batches = plan_batches(&items, 240, 3);
        assert_eq!(batches, vec![0..1]);
    }

    #[test]
    fn plan_batches_within_max_items_is_one_batch() {
        let items = vec![item(4), item(4), item(4)];
        let batches = plan_batches(&items, 240, 3);
        assert_eq!(batches, vec![0..3]);
    }

    #[test]
    fn plan_batches_exceeding_max_items_splits() {
        // max_items=2 with 5 items => 3 batches (2+2+1)
        let items = vec![item(4); 5];
        let batches = plan_batches(&items, 240, 2);
        assert_eq!(batches, vec![0..2, 2..4, 4..5]);
    }

    #[test]
    fn plan_batches_zero_max_items_disables_count_cap() {
        // 10 items with max=0 should all fit in one batch (PDU is large enough)
        let items = vec![item(4); 10];
        let batches = plan_batches(&items, 240, 0);
        assert_eq!(batches, vec![0..10]);
    }

    #[test]
    fn plan_batches_empty_items_yields_empty_batches() {
        let items: Vec<ReadItem> = vec![];
        let batches = plan_batches(&items, 240, 3);
        assert!(batches.is_empty());
    }

    #[test]
    fn plan_batches_order_and_count_preserved() {
        // 7 items, max=3 => batches [0..3, 3..6, 6..7]
        // total items across ranges should match original
        let items = vec![item(4); 7];
        let batches = plan_batches(&items, 240, 3);
        let total: usize = batches.iter().map(|r| r.len()).sum();
        assert_eq!(total, 7);
        // Verify ranges are contiguous and non-overlapping
        for w in batches.windows(2) {
            assert_eq!(w[0].end, w[1].start);
        }
        assert_eq!(batches[0].start, 0);
        assert_eq!(batches.last().unwrap().end, 7);
    }

    #[test]
    fn plan_batches_pdu_budget_splits_when_under_item_cap() {
        // Each item has a payload that takes 200 bytes in the response.
        // resp_cost per item = 4 + pad_even(200) = 204
        // resp header = 14
        // With pdu=240, budget = 236
        // Batch 1: 1 item => resp_cost = 14 + 204 = 218 <= 236
        // Adding a 2nd item: resp_cost = 218 + 204 = 422 > 236 => split
        // So we get 200-byte items split one per batch even with max_items=3
        let items = vec![item(200), item(200), item(200)];
        let batches = plan_batches(&items, 240, 3);
        assert_eq!(batches, vec![0..1, 1..2, 2..3]);
    }

    #[test]
    fn plan_batches_pdu_budget_mixed_item_sizes() {
        // small items bulk together, then a large item forces a split.
        // small: len=4 => resp_cost = 4+4=8 per item
        // large: len=100 => resp_cost = 4+100=104 per item (100 is even)
        // pdu=200 => budget=196, resp header=14
        // With max_items=5:
        //   Small items: each adds 12+8=20 cost. 14+20=34, 14+40=54, ...
        //   After 9 small items: resp_cost = 14 + 9*8 = 86, req_cost = 12 + 9*12 = 120
        //   Total after 9: max(120, 86) = 120 <= 196
        //   10th small: 120+12=132 req, 86+8=94 resp => 132 <= 196, fits
        //   Actually let me just test with specific numbers.
        let mut items = Vec::new();
        // 3 small items (len=4 each), then 1 large (len=100)
        for _ in 0..3 {
            items.push(item(4));
        }
        items.push(item(100));
        // pdu=140 => budget=136
        // req: 3 small = 12 + 3*12 = 48, + large = 48+12 = 60 <= 136
        // resp: 3 small = 14 + 3*(4+4) = 38, + large = 38 + (4+100) = 142 > 136!
        // So large should be its own batch.
        let batches = plan_batches(&items, 140, 5);
        assert_eq!(batches, vec![0..3, 3..4]);
    }

    #[test]
    fn plan_batches_pdu_budget_small_pdu() {
        // Very small PDU: each 4-byte item needs 24 req_bytes + 8 resp_bytes,
        // minus headers: req=12, resp=14.
        // req per item = 12, resp per item = 8.
        // budget = 50 - 4 = 46.
        // Batch: 12 + k*12 <= 46 => k*12 <= 34 => k <= 2 (24+12=36 <=46, 36+12=48 >46)
        // resp: 14 + k*8 <= 46 => k*8 <= 32 => k <= 4
        // So req side dominates: max 2 items per batch with pdu=50.
        let items = vec![item(4); 6];
        let batches = plan_batches(&items, 50, 10);
        assert_eq!(batches, vec![0..2, 2..4, 4..6]);
    }

    #[test]
    fn plan_batches_pdu_budget_resp_side_dominated() {
        // Items with large response payloads but small request footprint.
        // Each item: len=200 (even, resp=4+200=204), req=12.
        // PDU=420 => budget=416.
        //   req: 12 + 12 = 24 for 1 item, +12=36 for 2, etc.
        //   resp: 14 + 204 = 218 for 1 item, 14 + 408 = 422 > 416 for 2!
        // So only 1 item per batch, resp-side bound.
        let items = vec![item(200); 4];
        let batches = plan_batches(&items, 420, 10);
        assert_eq!(batches, vec![0..1, 1..2, 2..3, 3..4]);
    }

    #[test]
    fn plan_batches_pdu_budget_odd_item_padding() {
        // Odd-length payloads get padded in the response. len=5 => pad_even=6.
        // resp per item = 4 + 6 = 10.
        // PDU=70 => budget=66.
        //   req: 12 + k*12 <= 66 => k*12 <= 54 => k <= 4 (60 <= 66, 72 > 66)
        //   resp: 14 + k*10 <= 66 => k*10 <= 52 => k <= 5
        // So req side limits to 4 per batch.
        // But if items have varying lengths (some odd, some even), the padding
        // affects the response budget differently.
        let items = vec![item(5); 10]; // each costs 10 resp_bytes after padding
        let batches = plan_batches(&items, 70, 10);
        assert_eq!(batches, vec![0..4, 4..8, 8..10]);
    }
}
