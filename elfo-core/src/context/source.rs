use std::task::{self, Poll};

use derive_more::Constructor;

use crate::envelope::Envelope;

// TODO: seal it.
/// Note that implementations must be fused.
#[allow(unreachable_pub)]
pub trait Source {
    fn poll_recv(&self, cx: &mut task::Context<'_>) -> Poll<Option<Envelope>>;

    // TODO: try_recv.
}

impl<S: Source> Source for &S {
    #[inline]
    fn poll_recv(&self, cx: &mut task::Context<'_>) -> Poll<Option<Envelope>> {
        (**self).poll_recv(cx)
    }
}

impl Source for () {
    #[inline]
    fn poll_recv(&self, _cx: &mut task::Context<'_>) -> Poll<Option<Envelope>> {
        Poll::Ready(None)
    }
}

#[derive(Constructor)]
pub struct Combined<L, R> {
    left: L,
    right: R,
}

impl<L, R> Source for Combined<L, R>
where
    L: Source,
    R: Source,
{
    #[inline]
    fn poll_recv(&self, cx: &mut task::Context<'_>) -> Poll<Option<Envelope>> {
        match self.left.poll_recv(cx) {
            v @ Poll::Ready(Some(_)) => v,
            Poll::Ready(None) => self.right.poll_recv(cx),
            Poll::Pending => match self.right.poll_recv(cx) {
                v @ Poll::Ready(Some(_)) => v,
                _ => Poll::Pending,
            },
        }
    }
}
