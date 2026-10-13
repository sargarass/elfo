//! Provides two buffers for one owner thread and one collector, without locks.
//!
//! The owner writes to the buffer selected by `epoch`. The collector advances
//! `epoch` and waits for the owner to leave the previous buffer before collection.
//! Later writes use the new epoch, so the collector waits for at most one write.
//!
//! In [`Mode::Membarrier`], the owner uses only a compiler barrier.
//! The collector calls `membarrier(MEMBARRIER_CMD_PRIVATE_EXPEDITED)`
//! for barriers on CPUs that execute threads of this process.
//! In [`Mode::Fence`], both sides use `fence(SeqCst)`.

#[cfg(target_os = "linux")]
use std::sync::LazyLock;

use self::sync::{AtomicU64, Ordering, UnsafeCell, compiler_fence, fence};

#[cfg(target_os = "linux")]
mod membarrier;
mod sync;

// === DoubleBuffer ===

/// Marks an idle owner in `writing`. Never used as an epoch.
const IDLE: u64 = u64::MAX;

pub(crate) struct DoubleBuffer<T> {
    epochs: Epochs,
    buffers: [UnsafeCell<T>; 2],
}

// Keep both fields in one cache line: both threads access them on each step.
#[repr(align(16))]
struct Epochs {
    /// The epoch claimed by the owner, or [`IDLE`]. Only the owner stores here.
    writing: AtomicU64,
    /// Selects `buffers[epoch & 1]` for the owner. Only the collector stores here.
    epoch: AtomicU64,
}

// SAFETY: the owner claims an epoch and reads it again before buffer access.
// The collector advances the epoch, calls the barrier, then waits until
// `writing` differs from the previous epoch. The barriers prevent both threads
// from missing the other store, so only one thread can access each buffer.
// The collector keeps the epoch unchanged until collection is complete.
unsafe impl<T: Send> Sync for DoubleBuffer<T> {}

impl<T: Default> Default for DoubleBuffer<T> {
    fn default() -> Self {
        Self {
            epochs: Epochs {
                writing: AtomicU64::new(IDLE),
                epoch: AtomicU64::new(0),
            },
            buffers: [UnsafeCell::new(T::default()), UnsafeCell::new(T::default())],
        }
    }
}

impl<T> DoubleBuffer<T> {
    /// Calls `f` with the current buffer.
    ///
    /// # Safety
    ///
    /// Only the owner thread can call this method.
    /// `f` must not call `write` on the same instance.
    #[inline]
    pub(crate) unsafe fn write<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        debug_assert_eq!(
            self.epochs.writing.load(Ordering::Relaxed),
            IDLE,
            "nested write"
        );

        let _guard = IdleGuard(&self.epochs.writing);
        // Read the mode before the claim: on arm64, an acquire load directly
        // after the claim's `stlr` waits for that store.
        let mode = mode();
        let mut epoch = self.epochs.epoch.load(Ordering::Relaxed);
        loop {
            // `Release` makes the previous write visible to the collector,
            // even if it missed `IDLE` between writes.
            self.epochs.writing.store(epoch, Ordering::Release);
            match mode {
                Mode::Membarrier => compiler_fence(Ordering::SeqCst),
                Mode::Fence => fence(Ordering::SeqCst),
            }

            // `Acquire` pairs with `advance` to make collector changes visible.
            let confirmed = self.epochs.epoch.load(Ordering::Acquire);
            if confirmed == epoch {
                break;
            }
            // The collector advanced `epoch` after the first load.
            epoch = confirmed;
        }

        // SAFETY: the epoch still matches the claim. See the `Sync` impl.
        unsafe { self.buffers[(epoch & 1) as usize].with_mut(|value| f(&mut *value)) }
    }

    /// Switches the owner to the other buffer.
    ///
    /// Call [`barrier`] after all buffers advance.
    /// Do not call `advance` again until collection is complete.
    pub(crate) fn advance(&self) {
        let epoch = self.epochs.epoch.load(Ordering::Relaxed);
        self.epochs.epoch.store(epoch + 1, Ordering::Release);
    }

    /// Returns `true` when the owner no longer uses the previous buffer.
    /// Call this method after [`barrier`].
    pub(crate) fn can_collect(&self) -> bool {
        let previous = self.epochs.epoch.load(Ordering::Relaxed).wrapping_sub(1);
        self.epochs.writing.load(Ordering::Acquire) != previous
    }

    /// Calls `f` with the previous buffer.
    ///
    /// # Safety
    ///
    /// The caller must be the only collector.
    /// Call [`advance`](Self::advance), then [`barrier`], then
    /// [`can_collect`](Self::can_collect). Both checks must return `true`.
    /// Do not call `advance` again until `f` returns.
    pub(crate) unsafe fn collect<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        let epoch = self.epochs.epoch.load(Ordering::Relaxed);
        let previous = (epoch.wrapping_sub(1) & 1) as usize;
        // SAFETY: the caller satisfies the collection protocol. See the `Sync` impl.
        unsafe { self.buffers[previous].with_mut(|value| f(&mut *value)) }
    }
}

// === Mode ===

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    /// The owner uses a compiler barrier. The collector calls `membarrier()`.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    Membarrier,
    /// Both sides use `fence(SeqCst)`.
    Fence,
}

/// Uses [`Mode::Membarrier`] if registration succeeds, otherwise [`Mode::Fence`].
/// Loom and `ELFO_TELEMETRY_BARRIER=fence` always select [`Mode::Fence`].
#[cfg(target_os = "linux")]
static MODE: LazyLock<Mode> = LazyLock::new(|| {
    if cfg!(telemeter_loom) {
        return Mode::Fence;
    }
    if std::env::var("ELFO_TELEMETRY_BARRIER").is_ok_and(|v| v == "fence") {
        return Mode::Fence;
    }
    if membarrier::register() {
        Mode::Membarrier
    } else {
        Mode::Fence
    }
});

#[cfg(target_os = "linux")]
pub(crate) fn mode() -> Mode {
    *MODE
}

/// Returns the only available mode, without a runtime check.
#[cfg(not(target_os = "linux"))]
pub(crate) const fn mode() -> Mode {
    Mode::Fence
}

/// Applies the collector barrier between the `epoch` store and the `writing` load.
///
/// If the barrier fails, returns `false`. The collector must skip collection
/// until the next attempt because the owner can still write to either buffer.
pub(crate) fn barrier() -> bool {
    match mode() {
        Mode::Membarrier => membarrier::barrier(),
        Mode::Fence => {
            fence(Ordering::SeqCst);
            true
        }
    }
}

struct IdleGuard<'a>(&'a AtomicU64);

impl Drop for IdleGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        self.0.store(IDLE, Ordering::Release);
    }
}

#[cfg(not(target_os = "linux"))]
mod membarrier {
    pub(super) fn barrier() -> bool {
        unreachable!("membarrier mode is never selected on this platform")
    }
}

#[cfg(all(test, telemeter_loom))]
mod tests_loom {
    use loom::{sync::Arc, thread};

    use super::*;

    // The owner adds one three times. Collection overlaps writes, then drains
    // both buffers after the owner stops. Loom detects data races, and the
    // total detects lost or duplicate updates.
    #[test]
    fn owner_and_collector() {
        loom::model(|| {
            let value = Arc::new(DoubleBuffer::<u32>::default());

            let owner = {
                let value = value.clone();
                thread::spawn(move || {
                    for _ in 0..3 {
                        // SAFETY: this thread is the only owner of `value`.
                        unsafe { value.write(|v| *v += 1) };
                    }
                })
            };

            let take_all = |value: &DoubleBuffer<u32>| -> u32 {
                value.advance();
                assert!(barrier());
                while !value.can_collect() {
                    thread::yield_now();
                }
                // SAFETY: the only collector advanced the epoch, and both
                // `barrier` and `can_collect` returned true.
                unsafe { value.collect(std::mem::take) }
            };

            let during = take_all(&value) + take_all(&value);
            owner.join().unwrap();
            let after = take_all(&value) + take_all(&value);
            assert_eq!(during + after, 3);
        });
    }
}
