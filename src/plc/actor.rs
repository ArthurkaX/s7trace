//! Per-PLC connection actor.
//!
//! A single S7 connection is a single TCP socket — every read is serialized on
//! the wire no matter what. The problem this module solves is *fairness*: with
//! the old design each poll-group thread locked a shared `Mutex<dyn PlcClient>`
//! for the duration of its **whole** poll cycle, so a slow group (many tags, a
//! long multi-read sweep) would block a fast group sharing the same PLC for the
//! entire slow cycle. Under enough contention that manifested as the fast group
//! never getting a turn ("crash under concurrent reads" — really starvation).
//!
//! The actor owns the connection on its own thread and serves read requests
//! from a **priority mailbox**. Each consumer (poll group, interactive read)
//! holds a cheap, cloneable [`PlcHandle`] carrying its own priority; a request
//! is queued and the owning thread picks the highest-priority one next. Lower
//! priority value = served first; ties break FIFO by sequence. Pollers use
//! their poll period as the priority (a 100 ms group preempts a 5 s group), and
//! interactive reads use [`PRIORITY_INTERACTIVE`] so an ad-hoc `read` never
//! waits behind a polling sweep. The lock is now held only for one read at a
//! time, so a fast group waits at most one in-flight read, not a whole cycle.

use crate::plc::client::PlcClient;
use crate::types::{PlcError, ReadItem, ReadResult};
use parking_lot::{Condvar, Mutex};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::mpsc::{channel, Sender};
use std::sync::Arc;

/// Priority for interactive (one-off) reads — preempts all pollers.
pub const PRIORITY_INTERACTIVE: u32 = 0;

/// Operations the actor can perform on the owned client.
enum ActorOp {
    Connect,
    Disconnect,
    IsConnected,
    PduLen,
    ReadDb { db: u16, start: usize, len: usize },
    ReadMulti(Vec<ReadItem>),
    Label,
}

/// Responses returned over the per-request channel.
enum ActorResp {
    Connect(Result<(), PlcError>),
    Unit,
    Bool(bool),
    PduLen(Result<usize, PlcError>),
    ReadDb(Result<Vec<u8>, PlcError>),
    ReadMulti(Result<Vec<ReadResult>, PlcError>),
    Label(String),
}

/// One queued request, ordered by `(priority, seq)`.
struct Request {
    priority: u32,
    seq: u64,
    op: ActorOp,
    resp: Sender<ActorResp>,
}

impl PartialEq for Request {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.seq == other.seq
    }
}
impl Eq for Request {}
impl Ord for Request {
    fn cmp(&self, other: &Self) -> Ordering {
        // `BinaryHeap` is a max-heap, but we want the *lowest* priority value
        // (and, within a tie, the *lowest* seq = oldest) served first, so the
        // ordering is reversed: a smaller priority compares as "greater".
        other
            .priority
            .cmp(&self.priority)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}
impl PartialOrd for Request {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Shared mailbox between handles and the actor thread.
struct Mailbox {
    heap: Mutex<BinaryHeap<Request>>,
    cv: Condvar,
    shutdown: AtomicBool,
    seq: AtomicU64,
}

/// A cheap, cloneable handle to a PLC connection actor.
///
/// Implements [`PlcClient`] by forwarding each call to the owning thread, so it
/// is a drop-in replacement for the old `Arc<Mutex<dyn PlcClient>>` — but two
/// handles to the same PLC no longer block each other on a shared lock.
#[derive(Clone)]
pub struct PlcHandle {
    mb: Arc<Mailbox>,
    priority: u32,
    label: String,
}

impl PlcHandle {
    /// Derive a handle to the same actor with a different priority.
    pub fn with_priority(&self, priority: u32) -> PlcHandle {
        PlcHandle {
            mb: self.mb.clone(),
            priority,
            label: self.label.clone(),
        }
    }

    /// Signal the actor thread to stop. Pending requests still in flight get a
    /// disconnected error once the thread exits.
    pub fn stop(&self) {
        self.mb.shutdown.store(true, AtomicOrdering::Relaxed);
        // Take the mailbox lock around `notify_all`. The actor checks `shutdown`
        // only while holding this lock (then atomically releases it in
        // `cv.wait`), so acquiring it here guarantees we don't notify in the
        // window between its check and its park — i.e. no lost wakeup.
        let _g = self.mb.heap.lock();
        self.mb.cv.notify_all();
    }

    fn call(&self, op: ActorOp) -> ActorResp {
        let (tx, rx) = channel();
        let seq = self.mb.seq.fetch_add(1, AtomicOrdering::Relaxed);
        {
            let mut heap = self.mb.heap.lock();
            // Check `shutdown` *while holding the lock*, before enqueuing. The
            // actor drains and drops the heap on exit (also under this lock), so
            // serializing here guarantees we never leave a request — and its
            // response `Sender` — stranded in a heap no thread will service,
            // which would block `rx.recv()` below forever.
            if self.mb.shutdown.load(AtomicOrdering::Relaxed) {
                return ActorResp::Connect(Err(PlcError::Internal("plc actor stopped".into())));
            }
            heap.push(Request {
                priority: self.priority,
                seq,
                op,
                resp: tx,
            });
        }
        self.mb.cv.notify_one();
        rx.recv().unwrap_or_else(|_| {
            ActorResp::Connect(Err(PlcError::Internal("plc actor stopped".into())))
        })
    }

    fn err(&self) -> PlcError {
        PlcError::Internal(format!("plc actor '{}' stopped", self.label))
    }
}

impl PlcClient for PlcHandle {
    fn connect(&mut self) -> Result<(), PlcError> {
        match self.call(ActorOp::Connect) {
            ActorResp::Connect(r) => r,
            _ => Err(self.err()),
        }
    }

    fn disconnect(&mut self) {
        let _ = self.call(ActorOp::Disconnect);
    }

    fn is_connected(&self) -> bool {
        matches!(self.call(ActorOp::IsConnected), ActorResp::Bool(true))
    }

    fn pdu_len(&mut self) -> Result<usize, PlcError> {
        match self.call(ActorOp::PduLen) {
            ActorResp::PduLen(r) => r,
            _ => Err(self.err()),
        }
    }

    fn read_db(&mut self, db: u16, start: usize, len: usize) -> Result<Vec<u8>, PlcError> {
        match self.call(ActorOp::ReadDb { db, start, len }) {
            ActorResp::ReadDb(r) => r,
            _ => Err(self.err()),
        }
    }

    fn read_multi(&mut self, items: &[ReadItem]) -> Result<Vec<ReadResult>, PlcError> {
        match self.call(ActorOp::ReadMulti(items.to_vec())) {
            ActorResp::ReadMulti(r) => r,
            _ => Err(self.err()),
        }
    }

    fn label(&self) -> String {
        match self.call(ActorOp::Label) {
            ActorResp::Label(s) => s,
            _ => self.label.clone(),
        }
    }
}

/// Spawn a connection actor that owns `client` and serve it from a priority
/// mailbox. The returned [`PlcHandle`] is the default (lowest-urgency) handle;
/// derive per-consumer handles with [`PlcHandle::with_priority`].
///
/// `label` is used only for the thread name and as a fallback label.
pub fn spawn_actor(label: String, client: Arc<Mutex<dyn PlcClient + Send>>) -> PlcHandle {
    let mb = Arc::new(Mailbox {
        heap: Mutex::new(BinaryHeap::new()),
        cv: Condvar::new(),
        shutdown: AtomicBool::new(false),
        seq: AtomicU64::new(0),
    });
    let mb_thread = mb.clone();
    let thread_label = label.clone();
    std::thread::Builder::new()
        .name(format!("plc-actor-{label}"))
        .spawn(move || {
            // The actor is the sole owner of this Arc clone, so the lock is
            // always uncontended — it exists only to satisfy the `dyn` type.
            actor_loop(&mb_thread, &client, &thread_label);
        })
        .expect("spawn plc actor thread");

    PlcHandle {
        mb,
        priority: u32::MAX, // default handle is lowest urgency
        label,
    }
}

fn actor_loop(mb: &Arc<Mailbox>, client: &Arc<Mutex<dyn PlcClient + Send>>, label: &str) {
    loop {
        // Wait for the next request (highest priority first).
        let req = {
            let mut heap = mb.heap.lock();
            loop {
                if mb.shutdown.load(AtomicOrdering::Relaxed) {
                    tracing::debug!("plc-actor {label}: shutting down");
                    // Drop every pending request so its response `Sender` is
                    // dropped and the waiting caller's `recv()` returns an
                    // error instead of hanging.
                    heap.clear();
                    return;
                }
                if let Some(r) = heap.pop() {
                    break r;
                }
                mb.cv.wait(&mut heap);
            }
        };

        let mut c = client.lock();
        let resp = match req.op {
            ActorOp::Connect => ActorResp::Connect(c.connect()),
            ActorOp::Disconnect => {
                c.disconnect();
                ActorResp::Unit
            }
            ActorOp::IsConnected => ActorResp::Bool(c.is_connected()),
            ActorOp::PduLen => ActorResp::PduLen(c.pdu_len()),
            ActorOp::ReadDb { db, start, len } => ActorResp::ReadDb(c.read_db(db, start, len)),
            ActorOp::ReadMulti(items) => ActorResp::ReadMulti(c.read_multi(&items)),
            ActorOp::Label => ActorResp::Label(c.label()),
        };
        drop(c);
        // Requester may have given up (timed out); dropping the response is fine.
        let _ = req.resp.send(resp);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plc::snap7::MockPlcClient;

    fn mock_handle(name: &str) -> PlcHandle {
        let client: Arc<Mutex<dyn PlcClient + Send>> =
            Arc::new(Mutex::new(MockPlcClient::new(name)));
        spawn_actor(name.to_string(), client)
    }

    #[test]
    fn forwards_basic_ops() {
        let mut h = mock_handle("t");
        assert!(h.connect().is_ok());
        assert!(h.is_connected());
        // MockPlcClient returns zeroed data for unseeded reads.
        let _ = h.read_db(1, 0, 2);
        h.disconnect();
        h.stop();
    }

    #[test]
    fn seeded_read_roundtrips_through_actor() {
        let mut mock = MockPlcClient::new("t");
        mock.connect().unwrap();
        mock.seed(1, 0, vec![0xDE, 0xAD]);
        let client: Arc<Mutex<dyn PlcClient + Send>> = Arc::new(Mutex::new(mock));
        let mut h = spawn_actor("t".into(), client);
        h.connect().unwrap();
        let data = h.read_db(1, 0, 2).unwrap();
        assert_eq!(data, vec![0xDE, 0xAD]);
        h.stop();
    }

    #[test]
    fn priority_orders_pending_requests() {
        // Ordering check at the queue level: lower priority value first, then
        // FIFO. We compare `Request` ordering directly via a heap.
        let mut heap: BinaryHeap<Request> = BinaryHeap::new();
        let (tx, _rx) = channel();
        let mk = |priority: u32, seq: u64| Request {
            priority,
            seq,
            op: ActorOp::Label,
            resp: tx.clone(),
        };
        heap.push(mk(5000, 0));
        heap.push(mk(100, 1));
        heap.push(mk(100, 2));
        heap.push(mk(PRIORITY_INTERACTIVE, 3));
        // Interactive (0) first, then the two 100s in FIFO order, then 5000.
        let order: Vec<(u32, u64)> =
            std::iter::from_fn(|| heap.pop().map(|r| (r.priority, r.seq))).collect();
        assert_eq!(order, vec![(0, 3), (100, 1), (100, 2), (5000, 0)]);
    }

    #[test]
    fn handle_after_stop_returns_error() {
        let mut h = mock_handle("t");
        h.stop();
        // Give the thread a moment to observe shutdown.
        std::thread::sleep(std::time::Duration::from_millis(20));
        assert!(h.read_db(1, 0, 2).is_err());
    }
}
