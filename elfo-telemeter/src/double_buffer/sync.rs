#[cfg(not(telemeter_loom))]
use std::cell::UnsafeCell as InnerUnsafeCell;
#[cfg(not(telemeter_loom))]
pub(super) use std::sync::atomic::{AtomicU64, Ordering, compiler_fence, fence};

#[cfg(telemeter_loom)]
use loom::cell::UnsafeCell as InnerUnsafeCell;
#[cfg(telemeter_loom)]
pub(super) use loom::sync::atomic::{AtomicU64, Ordering, fence};

pub(super) struct UnsafeCell<T>(InnerUnsafeCell<T>);

impl<T> UnsafeCell<T> {
    pub(super) fn new(value: T) -> Self {
        Self(InnerUnsafeCell::new(value))
    }

    #[inline]
    pub(super) unsafe fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
        #[cfg(not(telemeter_loom))]
        {
            f(self.0.get())
        }
        #[cfg(telemeter_loom)]
        {
            self.0.with_mut(f)
        }
    }
}

#[cfg(telemeter_loom)]
// Loom has no compiler fences. The model uses `Mode::Fence` instead.
pub(super) fn compiler_fence(_: Ordering) {}
