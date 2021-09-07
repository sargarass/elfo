use std::{cell::Cell, future::Future, sync::Arc};

use crate::{addr::Addr, object::ObjectMeta, trace_id::TraceId};

tokio::task_local! {
    static META: Arc<ObjectMeta>;
    static TRACE_ID: Cell<TraceId>;
}

#[deprecated(note = "use `elfo::scope::trace_id()` instead")]
pub fn trace_id() -> TraceId {
    crate::scope::trace_id()
}

#[deprecated(note = "use `elfo::scope::try_trace_id()` instead")]
pub fn try_trace_id() -> Option<TraceId> {
    crate::scope::try_trace_id()
}

#[deprecated(note = "use `elfo::scope::set_trace_id()` instead")]
pub fn set_trace_id(trace_id: TraceId) {
    crate::scope::set_trace_id(trace_id);
}

#[deprecated(note = "use `elfo::scope::meta()` instead")]
pub fn meta() -> Arc<ObjectMeta> {
    crate::scope::meta()
}

#[deprecated(note = "use `elfo::scope::try_meta()` instead")]
pub fn try_meta() -> Option<Arc<ObjectMeta>> {
    crate::scope::try_meta()
}

#[deprecated(note = "use `elfo::scope` instead")]
pub async fn scope<F: Future>(meta: Arc<ObjectMeta>, trace_id: TraceId, f: F) -> F::Output {
    let scope = crate::scope::Scope::new(Addr::NULL, meta);
    scope.set_trace_id(trace_id);
    scope.within(f).await
}

#[deprecated(note = "use `elfo::scope` instead")]
pub fn sync_scope<R>(meta: Arc<ObjectMeta>, trace_id: TraceId, f: impl FnOnce() -> R) -> R {
    let scope = crate::scope::Scope::new(Addr::NULL, meta);
    scope.set_trace_id(trace_id);
    scope.sync_within(f)
}
