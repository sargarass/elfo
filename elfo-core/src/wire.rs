use std::{
    cell::Cell,
    future::poll_fn,
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicIsize, AtomicU64, Ordering},
    },
    task::Poll,
};

use cordyceps::MpscQueue;
use diatomic_waker::DiatomicWaker;
use metrics::{self, Key};
use parking_lot::Mutex;
use tracing::trace;

use elfo_utils::{CachePadded, unlikely};

use crate::{
    addr::Addr,
    dumping::{Direction, Dump, Dumper, INTERNAL_CLASS},
    envelope::{Envelope, EnvelopeHeader, MessageKind},
    errors::{SendError, TrySendError},
    mailbox::RecvResult,
    message::Message,
    scope,
    tracing::TraceId,
};

/// CLOSED bit packed into [`Wire::version`]; low 63 bits are a reconfig counter.
const VERSION_CLOSED: u64 = 1 << 63;

/// Cap on declared capacity. Fits `u32` and stays well below `isize::MAX / 4`
/// so `state` arithmetic doesn't overflow.
const MAX_PERMITS: usize = u32::MAX as usize;

/// Sentinel written into `state` on close.
const CLOSED_SENTINEL: isize = isize::MIN;

/// `state ≤ CLOSED_THRESHOLD` ⇒ closed. Gap to normal lows is wide enough
/// that no single RMW can cross it.
const CLOSED_THRESHOLD: isize = isize::MIN / 2;

#[inline]
fn add_permits(state: &AtomicIsize, tx_waker: &DiatomicWaker, n: usize) {
    if n == 0 {
        return;
    }
    let n = n as isize;
    let prev = state.fetch_add(n, Ordering::Release);
    // Wake only on the `≤0 → >0` crossing; coalesces per-message notifies
    // into one per refill cycle. `acquire_permit` re-loads `state` Acquire
    // after register, so a concurrently-parking producer can't miss it.
    if unlikely(prev <= 0 && prev + n > 0) {
        tx_waker.notify();
    }
}

#[inline]
fn forget_permits(state: &AtomicIsize, n: usize) {
    if n == 0 {
        return;
    }
    state.fetch_sub(n as isize, Ordering::AcqRel);
}

#[inline]
fn is_closed(state: &AtomicIsize) -> bool {
    state.load(Ordering::Acquire) <= CLOSED_THRESHOLD
}

/// Sets CLOSED on `version`, writes the sentinel into `state`, wakes both ends.
/// Returns `true` on the first close.
#[cold]
fn mark_closed(
    state: &AtomicIsize,
    version: &AtomicU64,
    tx_waker: &DiatomicWaker,
    rx_waker: &DiatomicWaker,
) -> bool {
    let prev = version.fetch_or(VERSION_CLOSED, Ordering::AcqRel);
    let newly = prev & VERSION_CLOSED == 0;
    if newly {
        state.store(CLOSED_SENTINEL, Ordering::Release);
        tx_waker.notify();
        rx_waker.notify();
    }
    newly
}

// === Wire ===

struct Wire {
    /// Intrusive linked list of envelopes.
    // TODO: replace with a bespoke SPSC queue to drop the `head.swap` RMW.
    queue: MpscQueue<EnvelopeHeader>,

    /// Signed permit balance: `>0` available, `0` empty, `<0` shrink debt,
    /// `≤ CLOSED_THRESHOLD` closed.
    state: CachePadded<AtomicIsize>,

    /// Parks the sole producer when no permits are available.
    tx_waker: CachePadded<DiatomicWaker>,

    /// Reconfig counter with CLOSED in the top bit; producers re-sync on mismatch.
    version: CachePadded<AtomicU64>,

    /// Wakes the receiver on enqueue or close.
    // TODO: coalesce notifies via empty→non-empty transition.
    rx_waker: CachePadded<DiatomicWaker>,

    /// Serialises close / `set_capacity`.
    control: Mutex<Control>,
}

struct Control {
    closed_trace_id: Option<TraceId>,
    capacity: usize,
}

pub fn wire(capacity: usize) -> (WireSender, WireReceiver) {
    let capacity = clamp_capacity(capacity);

    let shared = Arc::new(Wire {
        queue: MpscQueue::new_with_stub(Envelope::stub()),
        state: CachePadded::new(AtomicIsize::new(capacity as isize)),
        tx_waker: CachePadded::new(DiatomicWaker::new()),
        version: CachePadded::new(AtomicU64::new(0)),
        rx_waker: CachePadded::new(DiatomicWaker::new()),
        control: Mutex::new(Control {
            closed_trace_id: None,
            capacity,
        }),
    });
    let sender = WireSender {
        shared: shared.clone(),
        dumper: Dumper::new(INTERNAL_CLASS),
        local_permits: 0,
        cached_version: 0,
        _not_sync: PhantomData,
    };
    let receiver = WireReceiver { shared };
    (sender, receiver)
}

// === WireSender ===

/// The sole producer half of a [`Wire`]. `!Sync` + `!Clone` + `&mut self`
/// on every send path, so `local_permits` and `tx_waker` need no extra sync.
pub struct WireSender {
    shared: Arc<Wire>,
    dumper: Dumper,

    /// Credits checked out from `state`; refilled in one CAS when depleted.
    local_permits: u32,

    /// Last seen counter bits of `version`; mismatch ⇒ resync, CLOSED ⇒ end.
    cached_version: u64,

    _not_sync: PhantomData<Cell<()>>,
}

impl Drop for WireSender {
    fn drop(&mut self) {
        // Return unused local credit so capacity accounting doesn't leak.
        if self.local_permits > 0 {
            self.shared
                .state
                .fetch_add(self.local_permits as isize, Ordering::Release);
            self.local_permits = 0;
        }
        self.close();
    }
}

impl WireSender {
    pub async fn send<M: Message>(&mut self, message: M) -> Result<(), SendError<M>> {
        let kind = MessageKind::regular(current_actor());
        self.emit_out_telemetry(&message, &kind); // TODO: only if successful?

        if self.acquire_permit().await.is_err() {
            return Err(SendError(message));
        }

        self.shared.queue.enqueue(Envelope::new(message, kind));
        self.shared.rx_waker.notify();
        Ok(())
    }

    pub fn try_send<M: Message>(&mut self, message: M) -> Result<(), TrySendError<M>> {
        let kind = MessageKind::regular(current_actor());
        self.emit_out_telemetry(&message, &kind); // TODO: only if successful?

        if self.reconcile_version().is_err() {
            return Err(TrySendError::Closed(message));
        }

        if self.local_permits == 0 {
            match self.refill() {
                Ok(true) => {}
                Ok(false) => return Err(TrySendError::Full(message)),
                Err(()) => return Err(TrySendError::Closed(message)),
            }
        }

        self.local_permits -= 1;
        self.shared.queue.enqueue(Envelope::new(message, kind));
        self.shared.rx_waker.notify();
        Ok(())
    }

    #[cold]
    pub fn close(&self) -> bool {
        // Lock taken before close so `recv` can't observe closed before
        // `closed_trace_id` is set.
        let mut control = self.shared.control.lock();

        if is_closed(&self.shared.state) {
            return false;
        }

        control.closed_trace_id = Some(scope::trace_id());

        mark_closed(
            &self.shared.state,
            &self.shared.version,
            &self.shared.tx_waker,
            &self.shared.rx_waker,
        )
    }

    /// On version mismatch returns all `local_permits` to the pool. `Err(())` if closed.
    #[inline]
    fn reconcile_version(&mut self) -> Result<(), ()> {
        let v = self.shared.version.load(Ordering::Acquire);
        if v == self.cached_version {
            return Ok(());
        }
        self.resync_to_version(v)
    }

    #[cold]
    fn resync_to_version(&mut self, v: u64) -> Result<(), ()> {
        if v & VERSION_CLOSED != 0 {
            return Err(());
        }
        if self.local_permits > 0 {
            // Signed `fetch_add` absorbs shrink debt transparently.
            self.shared
                .state
                .fetch_add(self.local_permits as isize, Ordering::Release);
            self.local_permits = 0;
        }
        self.cached_version = v;
        Ok(())
    }

    /// `Ok(true)` — pulled all available permits, `state` reset to 0.
    /// `Ok(false)` — empty or paying down shrink debt.
    /// `Err(())` — closed.
    fn refill(&mut self) -> Result<bool, ()> {
        debug_assert_eq!(self.local_permits, 0, "refill called with nonzero local");
        let mut s = self.shared.state.load(Ordering::Relaxed);
        loop {
            if s <= CLOSED_THRESHOLD {
                return Err(());
            }
            if s <= 0 {
                return Ok(false);
            }
            match self.shared.state.compare_exchange_weak(
                s,
                0,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.local_permits = s as u32;
                    return Ok(true);
                }
                Err(actual) => s = actual,
            }
        }
    }

    async fn acquire_permit(&mut self) -> Result<(), ()> {
        loop {
            self.reconcile_version()?;

            if self.local_permits > 0 {
                self.local_permits -= 1;
                return Ok(());
            }

            if self.refill()? {
                debug_assert!(self.local_permits > 0);
                self.local_permits -= 1;
                return Ok(());
            }

            poll_fn(|cx| {
                // SAFETY: `WireSender` is `!Sync` and owns the sole `tx_waker`
                // sink; `&mut self` rules out concurrent `register`s.
                unsafe { self.shared.tx_waker.register(cx.waker()) };

                // Re-check: any progress-enabling change happens-before its notify.
                let s = self.shared.state.load(Ordering::Acquire);
                let v = self.shared.version.load(Ordering::Acquire);
                let wakeable = (v & VERSION_CLOSED != 0) || s > 0 || v != self.cached_version;
                if wakeable {
                    // SAFETY: see `register` above.
                    unsafe { self.shared.tx_waker.unregister() };
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
        }
    }

    #[inline]
    fn emit_out_telemetry<M: Message>(&self, message: &M, kind: &MessageKind) {
        if let Some(recorder) = metrics::try_recorder() {
            let key = Key::from_static_parts("elfo_sent_messages_total", message.labels());
            recorder.increment_counter(&key, 1);
        }

        trace!("> {:?}", message);

        if let Some(permit) = self.dumper.acquire_m(message) {
            permit.record(Dump::message(message, kind, Direction::Out));
        }
    }
}

// === WireReceiver ===

pub struct WireReceiver {
    shared: Arc<Wire>,
}

impl WireReceiver {
    pub fn set_capacity(&self, capacity: usize) {
        let mut control = self.shared.control.lock();

        if capacity == control.capacity {
            return;
        }

        let capacity = clamp_capacity(capacity);
        if capacity < control.capacity {
            forget_permits(&self.shared.state, control.capacity - capacity);
        } else {
            add_permits(
                &self.shared.state,
                &self.shared.tx_waker,
                capacity - control.capacity,
            );
        }
        control.capacity = capacity;

        // Bump version after `state` so a producer seeing the new version
        // is guaranteed to see the updated `state`.
        self.shared.version.fetch_add(1, Ordering::Release);
        self.shared.tx_waker.notify();
    }

    pub (crate) async fn recv(&mut self) -> Option<Envelope> {
        let envelope = poll_fn(|cx| {
            if let Some(res) = self.try_recv() {
                return match res {
                    RecvResult::Data(envelope) => Poll::Ready(Some(envelope)),
                    RecvResult::Closed(_) => Poll::Ready(None),
                };
            }

            // SAFETY: `&mut self` + `!Clone` ⇒ sole sink.
            unsafe { self.shared.rx_waker.register(cx.waker()) };

            // Re-check after register: any enqueue happens-before its notify.
            match self.try_recv() {
                Some(RecvResult::Data(envelope)) => {
                    // SAFETY: see `register` above.
                    unsafe { self.shared.rx_waker.unregister() };
                    Poll::Ready(Some(envelope))
                }
                Some(RecvResult::Closed(_)) => {
                    // SAFETY: see `register` above.
                    unsafe { self.shared.rx_waker.unregister() };
                    Poll::Ready(None)
                }
                None => Poll::Pending,
            }
        })
        .await?;
        Some(envelope)
    }

    pub(crate) fn try_recv(&mut self) -> Option<RecvResult> {
        // SAFETY: `&mut self` + `!Clone` ⇒ sole consumer.
        match unsafe { self.shared.queue.dequeue_unchecked() } {
            Some(envelope) => {
                add_permits(&self.shared.state, &self.shared.tx_waker, 1);
                Some(RecvResult::Data(envelope))
            }
            None if is_closed(&self.shared.state) => Some(self.on_close()),
            None => None,
        }
    }

    #[cold]
    fn on_close(&mut self) -> RecvResult {
        // Some messages may remain in the queue after close.
        // SAFETY: see `try_recv_raw`.
        match unsafe { self.shared.queue.dequeue_unchecked() } {
            Some(envelope) => RecvResult::Data(envelope),
            None => {
                let control = self.shared.control.lock();
                let trace_id = control.closed_trace_id.expect("called before close()");
                RecvResult::Closed(trace_id)
            }
        }
    }
}

impl Drop for WireReceiver {
    #[cold]
    fn drop(&mut self) {
        mark_closed(
            &self.shared.state,
            &self.shared.version,
            &self.shared.tx_waker,
            &self.shared.rx_waker,
        );
        // Drain so resources held by pending envelopes (e.g. `RequestTable`
        // slots) release now, not when the last `Arc<Wire>` drops.
        // SAFETY: `Drop` ⇒ sole owner ⇒ sole consumer.
        while unsafe { self.shared.queue.dequeue_unchecked() }.is_some() {}
    }
}

#[inline]
fn current_actor() -> Addr {
    scope::with(|s| s.actor())
}

fn clamp_capacity(capacity: usize) -> usize {
    capacity.min(MAX_PERMITS)
}

