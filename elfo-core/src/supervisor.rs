use std::{future::Future, mem, ops::Deref, sync::Arc, time::Duration};

use dashmap::{
    DashMap,
    Entry::{Occupied, Vacant},
};
use futures::future::BoxFuture;
use fxhash::FxBuildHasher;
use metrics::{decrement_gauge, increment_gauge};
use parking_lot::{Condvar, Mutex, RwLock};
use tracing::{Instrument, Span, debug, error, error_span, info, warn};

use elfo_utils::CachePadded;

use self::{error_chain::ErrorChain, measure_poll::MeasurePoll};
use crate::{
    ResponseToken,
    actor::{Actor, ActorMeta, ActorStartInfo},
    actor_status::ActorStatus,
    addr::{Addr, GroupNo, NodeLaunchId, NodeNo},
    config::{AnyConfig, Config, SystemConfig},
    context::Context,
    envelope::Envelope,
    exec::{Exec, ExecResult},
    group::TerminationPolicy,
    message::Request,
    messages, msg,
    object::{GroupVisitor, Object, OwnedObject},
    panic,
    restarting::{RestartBackoff, RestartPolicy},
    routers::{Outcome, Router},
    runtime::RuntimeManager,
    scope::{self, Scope, ScopeGroupShared},
    subscription::SubscriptionManager,
    tracing::TraceId,
};

mod error_chain;
mod measure_poll;

pub(crate) struct Supervisor<R: Router<C>, C, X> {
    meta: Arc<ActorMeta>,
    restart_policy: RestartPolicy,
    termination_policy: TerminationPolicy,
    span: Span,
    context: Context,
    objects: DashMap<R::Key, OwnedObject, FxBuildHasher>,
    pending_remote_spawns: DashMap<R::Key, Arc<PendingRemoteSpawn>>,
    router: R,
    exec: X,
    control: CachePadded<RwLock<Control<C>>>,
    scope_shared: Arc<ScopeGroupShared>,
    status_subscription: Arc<SubscriptionManager>,
    rt_manager: RuntimeManager,
}

struct Control<C> {
    system_config: Arc<SystemConfig>,
    user_config: Option<Arc<C>>,
    is_started: bool,
    stop_spawning: bool,
}

enum SpawnResult {
    Spawned(OwnedObject),
    SpawnStopped,
    WaitRemoteSpawn(Arc<PendingRemoteSpawn>),
}

/// Returns `None` if cannot be spawned.
macro_rules! get_or_spawn {
    ($this:ident, $key:expr, $start_info:expr) => {{
        let key = $key;
        match $this.objects.get(&key) {
            Some(object) => Some(object),
            None => match $this.objects.entry(key.clone()) {
                // FIXME(loyd): take an exclusive lock here.
                Occupied(entry) => Some(entry.into_ref().downgrade()),
                Vacant(entry) => match $this.spawn(key.clone(), $start_info, Default::default()) {
                    // FIXME(loyd): take an exclusive lock here.
                    SpawnResult::Spawned(object) => Some(entry.insert(object).downgrade()),
                    SpawnResult::SpawnStopped => None,
                    SpawnResult::WaitRemoteSpawn(pending) => {
                        drop(entry);
                        pending.wait_for_slot();
                        $this.objects.get(&key)
                    }
                },
            },
        }
    }};
}

impl<R, C, X> Supervisor<R, C, X>
where
    R: Router<C>,
    X: Exec<Context<C, R::Key>>,
    <X::Output as Future>::Output: ExecResult,
    C: Config,
{
    #[allow(clippy::too_many_arguments)] // I know :(
    pub(crate) fn new(
        ctx: Context,
        node_no: NodeNo,
        node_launch_id: NodeLaunchId,
        group: String,
        exec: X,
        router: R,
        restart_policy: RestartPolicy,
        termination_policy: TerminationPolicy,
        rt_manager: RuntimeManager,
    ) -> Self {
        let control = Control {
            system_config: Default::default(),
            user_config: None,
            is_started: false,
            stop_spawning: false,
        };

        let status_subscription = SubscriptionManager::new(ctx.clone());

        Self {
            span: error_span!(parent: Span::none(), "", actor_group = group.as_str()),
            meta: Arc::new(ActorMeta {
                group,
                key: String::new(),
            }),
            restart_policy,
            termination_policy,
            objects: DashMap::default(),
            pending_remote_spawns: Default::default(),
            router,
            exec,
            control: CachePadded::new(RwLock::new(control)),
            scope_shared: Arc::new(ScopeGroupShared::new(node_no, node_launch_id, ctx.group())),
            status_subscription: Arc::new(status_subscription),
            context: ctx,
            rt_manager,
        }
    }

    // This method shouldn't be called often.
    fn in_scope(&self, f: impl FnOnce()) {
        Scope::new(
            scope::trace_id(),
            Addr::NULL,
            self.meta.clone(),
            // TODO: do not limit logging and dumping in supervisor.
            self.scope_shared.clone(),
        )
        .sync_within(|| self.span.in_scope(f));
    }

    pub(crate) fn handle(self: &Arc<Self>, mut envelope: Envelope, visitor: &mut dyn GroupVisitor) {
        let outcome = msg!(match &envelope {
            messages::ValidateConfig { config } => match config.decode::<C>() {
                Ok(config) => {
                    // Make all updates under lock, including telemetry/dumper ones.
                    let mut control = self.control.write();

                    if control.user_config.is_none() {
                        // We should update configs before spawning any actors
                        // to avoid a race condition at startup.
                        // So, we update the config on `ValidateConfig` at the first time.
                        self.update_config(&mut control, &config);
                        let token = extract_response_token::<messages::ValidateConfig>(envelope);
                        self.context.respond(token, Ok(()));
                        return visitor.done();
                    } else {
                        drop(control);
                        envelope.set_message(messages::ValidateConfig { config });
                        self.router.route(&envelope).or(Outcome::Discard)
                    }
                }
                Err(reason) => {
                    let reject = messages::ConfigRejected { reason };
                    let token = extract_response_token::<messages::ValidateConfig>(envelope);
                    self.context.respond(token, Err(reject));
                    return visitor.done();
                }
            },
            messages::UpdateConfig { config } => match config.decode::<C>() {
                Ok(config) => {
                    // Make all updates under lock, including telemetry/dumper ones.
                    let mut control = self.control.write();

                    let only_spawn = !control.is_started;
                    if !only_spawn || control.user_config.is_none() {
                        // At the first time the config is updated on `ValidateConfig`.
                        self.update_config(&mut control, &config);
                    }

                    control.is_started = true;
                    drop(control);

                    let outcome = self.router.route(&envelope);

                    if only_spawn {
                        self.spawn_on_group_mounted(outcome);
                        let token = extract_response_token::<messages::UpdateConfig>(envelope);
                        self.context.respond(token, Ok(()));
                        return visitor.done();
                    } else {
                        // Send `UpdateConfig` across actors.
                        envelope.set_message(messages::UpdateConfig { config });
                        outcome.or(Outcome::Broadcast)
                    }
                }
                Err(reason) => {
                    self.in_scope(
                        || error!(group = %self.meta.group, %reason, "invalid config is ignored"),
                    );
                    let reject = messages::ConfigRejected { reason };
                    let token = extract_response_token::<messages::UpdateConfig>(envelope);
                    self.context.respond(token, Err(reject));
                    return visitor.done();
                }
            },
            messages::SubscribeToActorStatuses { forcing } => {
                let sender = envelope.sender();
                self.in_scope(|| self.subscribe_to_statuses(sender, *forcing));
                return visitor.done();
            }
            messages::Terminate => {
                if self.termination_policy.stop_spawning {
                    let is_newly = !mem::replace(&mut self.control.write().stop_spawning, true);
                    if is_newly {
                        self.in_scope(|| info!("stopped spawning new actors"));
                    }
                }

                self.router.route(&envelope).or(Outcome::Broadcast)
            }
            messages::Ping => {
                self.router.route(&envelope).or(Outcome::Broadcast)
            }
            _ => {
                self.router.route(&envelope).or(Outcome::Discard)
            }
        });

        let start_info = ActorStartInfo::on_message();
        match outcome {
            Outcome::Unicast(key) => match get_or_spawn!(self, key, start_info) {
                Some(object) => visitor.visit_last(&object, envelope),
                None => visitor.empty(envelope),
            },
            Outcome::GentleUnicast(key) => match self.objects.get(&key) {
                Some(object) => visitor.visit_last(&object, envelope),
                None => visitor.empty(envelope),
            },
            Outcome::Multicast(list) => {
                for key in list.iter() {
                    if !self.objects.contains_key(key) {
                        get_or_spawn!(self, key.clone(), start_info.clone());
                    }
                }
                let iter = list.into_iter().filter_map(|key| self.objects.get(&key));
                self.visit_multiple(envelope, visitor, iter);
            }
            Outcome::GentleMulticast(list) => {
                let iter = list.into_iter().filter_map(|key| self.objects.get(&key));
                self.visit_multiple(envelope, visitor, iter);
            }
            Outcome::Broadcast => self.visit_multiple(envelope, visitor, self.objects.iter()),
            Outcome::Discard => visitor.empty(envelope),
            Outcome::Default => unreachable!("must be altered earlier"),
        }
    }

    fn visit_multiple(
        &self,
        envelope: Envelope,
        visitor: &mut dyn GroupVisitor,
        iter: impl Iterator<Item = impl Deref<Target = OwnedObject>>,
    ) {
        let mut iter = iter.peekable();

        if iter.peek().is_none() {
            return visitor.empty(envelope);
        }

        loop {
            let object = iter.next().unwrap();
            if iter.peek().is_none() {
                return visitor.visit_last(&object, envelope);
            } else {
                visitor.visit(&object, &envelope);
            }
        }
    }

    fn spawn(
        self: &Arc<Self>,
        key: R::Key,
        start_info: ActorStartInfo,
        backoff: RestartBackoff,
    ) -> SpawnResult {
        let key_str = key.to_string();
        let group_name = self.meta.group.clone();
        let rt_selection_meta = ActorMeta {
            group: group_name.clone(),
            key: key_str.clone(),
        };
        let rt = self.rt_manager.get(&rt_selection_meta);
        drop(rt_selection_meta);

        let notify_waiters_by_key = |key| {
            if let Some((_, pending)) = self.pending_remote_spawns.remove(key) {
                let mut in_flight = pending.in_flight.lock();
                *in_flight = false;
                pending.cv.notify_all();
            }
        };

        let on_target = tokio::runtime::Handle::try_current()
            .ok()
            .map(|cur| cur.id() == rt.id())
            .unwrap_or(false);

        if on_target {
            let control = self.control.read();
            if control.stop_spawning {
                notify_waiters_by_key(&key);
                return SpawnResult::SpawnStopped;
            }

            let group_no = self.context.group().group_no().expect("invalid group addr");
            let system_config = control.system_config.clone();

            let user_config = control
                .user_config
                .as_ref()
                .cloned()
                .expect("config is unset");

            drop(control);

            let sv = self.clone();
            let termination_policy = self.termination_policy.clone();
            let status_sub = self.status_subscription.clone();
            let ctx = self
                .context
                .clone()
                .with_key(key.clone())
                .with_config(user_config);

            let actor = run_spawn(
                sv,
                ctx,
                key.clone(),
                start_info,
                backoff,
                group_no,
                group_name,
                key_str,
                system_config,
                termination_policy,
                status_sub,
            );
            let res = match actor {
                Some(actor) => SpawnResult::Spawned(actor),
                None => SpawnResult::SpawnStopped,
            };
            notify_waiters_by_key(&key);
            res
        } else {
            let pending = self
                .pending_remote_spawns
                .entry(key.clone())
                .or_insert_with(|| Arc::new(PendingRemoteSpawn::default()))
                .clone();

            if *pending.in_flight.lock() {
                return SpawnResult::WaitRemoteSpawn(pending);
            }

            let guard = InFlightGuard::arm(pending.clone());

            // We want this code to run on the tokio's worker thread to get all allocations
            // locally
            rt.spawn({
                let sv = self.clone();
                let scope = scope::expose();
                let f = move || {
                    let _guard = guard;
                    let Vacant(entry) = sv.objects.entry(key.clone()) else {
                        // Another on-target caller already spawned this actor
                        return;
                    };

                    let object = match sv.spawn(key.clone(), start_info, backoff) {
                        SpawnResult::Spawned(object) => object,
                        SpawnResult::SpawnStopped => return,
                        SpawnResult::WaitRemoteSpawn(_) => {
                            unreachable!("worker is already on remote rt!")
                        }
                    };
                    entry.insert(object);
                };

                async move { scope.sync_within(f) }
            });

            SpawnResult::WaitRemoteSpawn(pending)
        }
    }

    fn spawn_on_group_mounted(self: &Arc<Self>, outcome: Outcome<R::Key>) {
        let start_info = ActorStartInfo::on_group_mounted();
        match outcome {
            Outcome::Unicast(key) => {
                get_or_spawn!(self, key, start_info);
            }
            Outcome::Multicast(keys) => {
                for key in keys {
                    get_or_spawn!(self, key, start_info.clone());
                }
            }
            Outcome::GentleUnicast(_)
            | Outcome::GentleMulticast(_)
            | Outcome::Broadcast
            | Outcome::Discard
            | Outcome::Default => {}
        }
    }

    fn update_config(&self, control: &mut Control<C>, config: &AnyConfig) {
        let system = config.get_system();
        self.scope_shared.configure(system);

        let need_to_update_actors = control.system_config.mailbox != system.mailbox;

        // Update user's config.
        control.system_config = system.clone();
        control.user_config = Some(config.get_user::<C>().clone());

        self.router
            .update(control.user_config.as_ref().expect("just saved"));

        if need_to_update_actors {
            for object in self.objects.iter() {
                let actor = object
                    .value()
                    .as_actor()
                    .expect("a supervisor stores only actors");

                actor.set_mailbox_capacity_config(system.mailbox.capacity);
            }
        }

        self.in_scope(|| {
            debug!(
                message = "config updated",
                system = ?control.system_config,
                custom = ?control.user_config.as_ref().unwrap(),
            )
        });
    }

    fn subscribe_to_statuses(&self, addr: Addr, forcing: bool) {
        // Firstly, add the subscriber to handle new objects right way.
        if !self.status_subscription.add(addr) && !forcing {
            // Already subscribed.
            return;
        }

        // Send active statuses to the subscriber.
        for item in self.objects.iter() {
            let actor = item
                .value()
                .as_actor()
                .expect("a supervisor stores only actors");

            let result = actor.with_status(|report| self.context.try_send_to(addr, report));

            if result.is_err() {
                // TODO: `unbounded_send(Unsubscribed)`
                warn!(%addr, "status cannot be sent, unsubscribing");
                self.status_subscription.remove(addr);
                break;
            }
        }
    }

    pub(crate) fn finished(self: &Arc<Self>) -> BoxFuture<'static, ()> {
        let sv = self.clone();
        let addrs = self
            .objects
            .iter()
            .map(|r| r.value().addr())
            .collect::<Vec<_>>();

        let fut = async move {
            for addr in addrs {
                let object = ward!(sv.context.book().get_owned(addr), continue);
                let actor = object.as_actor().expect("a supervisor stores only actors");
                actor.finished().await;
            }
        };

        Box::pin(fut)
    }
}

#[allow(clippy::too_many_arguments)]
fn run_spawn<R, C, X>(
    sv: Arc<Supervisor<R, C, X>>,
    ctx: Context<C, R::Key>,
    key: R::Key,
    start_info: ActorStartInfo,
    backoff: RestartBackoff,
    group_no: GroupNo,
    group_name: String,
    key_str: String,
    system_config: Arc<SystemConfig>,
    termination_policy: TerminationPolicy,
    status_sub: Arc<SubscriptionManager>,
) -> Option<OwnedObject>
where
    R: Router<C>,
    X: Exec<Context<C, R::Key>>,
    <X::Output as Future>::Output: ExecResult,
    C: Config,
{
    let control = sv.control.read();
    if control.stop_spawning {
        return None;
    }
    let entry = sv.context.book().vacant_entry(group_no);
    let addr = entry.addr();
    drop(control);

    let span = error_span!(
        parent: Span::none(),
        "",
        actor_group = group_name.as_str(),
        actor_key = key_str.as_str()
    );
    let meta = Arc::new(ActorMeta {
        group: group_name,
        key: key_str,
    });
    let boxed = Box::new(Actor::new(
        meta.clone(),
        addr,
        &system_config.mailbox,
        termination_policy,
        status_sub,
    ));

    let scope = Scope::new(
        scope::trace_id(),
        addr,
        meta.clone(),
        sv.scope_shared.clone(),
    )
    .with_telemetry(&system_config.telemetry);
    drop(system_config);

    let body = build_actor_body(sv.clone(), ctx, key, start_info, span, addr, backoff);

    entry.insert(Object::new(addr, boxed));

    crate::task::Builder::new(scope)
        .spawn(body)
        .expect("spawn an actor's task");

    // Register the actor in the book to make it reachable.
    let object = sv.context.book().get_owned(addr).expect("just created");
    Some(object)
}

#[allow(clippy::too_many_arguments)]
fn build_actor_body<R, C, X>(
    sv: Arc<Supervisor<R, C, X>>,
    ctx: Context<C, R::Key>,
    key: R::Key,
    start_info: ActorStartInfo,
    span: Span,
    addr: Addr,
    mut backoff: RestartBackoff,
) -> impl Future<Output = ()> + Send + 'static
where
    R: Router<C>,
    X: Exec<Context<C, R::Key>>,
    <X::Output as Future>::Output: ExecResult,
    C: Config,
{
    #[cfg(feature = "unstable-stuck-detection")]
    let stuck_detector = sv.rt_manager.stuck_detector();

    let fut = async move {
        let thread = std::thread::current();

        info!(%addr, thread = %thread.name().unwrap_or("?"), "started");

        sv.objects
            .get(&key)
            .expect("where is the current actor?")
            .as_actor()
            .expect("a supervisor stores only actors")
            .on_start();

        // It must be called after `entry.insert()`.
        let ctx = ctx.with_addr(addr).with_start_info(start_info);
        let fut = async { sv.exec.exec(ctx).await.unify() };
        let new_status = match panic::catch(fut).await {
            Ok(Ok(())) => ActorStatus::TERMINATED,
            Ok(Err(err)) => ActorStatus::FAILED.with_details(ErrorChain(&*err)),
            Err(panic) => ActorStatus::FAILED.with_details(panic),
        };

        let restart_after = {
            let object = sv.objects.get(&key).expect("where is the current actor?");

            let actor = object.as_actor().expect("a supervisor stores only actors");

            // Select the restart policy with the following priority: actor override >
            // config override > blueprint restart policy..
            let default_restart_policy = sv
                .control
                .read()
                .system_config
                .restart_policy
                .make_policy()
                .unwrap_or(sv.restart_policy.clone());
            let restart_policy = actor.restart_policy().unwrap_or(default_restart_policy);

            let restarting_allowed =
                restart_policy.restarting_allowed(&new_status) && !sv.control.read().stop_spawning;

            actor.set_status(new_status);

            restarting_allowed
                .then(|| {
                    restart_policy
                        .restart_params()
                        .and_then(|p| backoff.next(&p))
                })
                .flatten()
        };

        let _ = if let Some(after) = restart_after {
            if after == Duration::ZERO {
                debug!("actor will be restarted immediately");
            } else {
                debug!(?after, "actor will be restarted");

                increment_gauge!("elfo_restarting_actors", 1.);
                tokio::time::sleep(after).await;
                decrement_gauge!("elfo_restarting_actors", 1.);
            }

            // Restarted actors should have a new trace id.
            scope::set_trace_id(TraceId::generate());

            backoff.start();

            match sv.spawn(key.clone(), ActorStartInfo::on_restart(), backoff) {
                SpawnResult::Spawned(object) => sv.objects.insert(key.clone(), object),
                SpawnResult::SpawnStopped => sv.objects.remove(&key).map(|(_, v)| v),
                // it is already on target rt
                SpawnResult::WaitRemoteSpawn(_) => unreachable!(),
            }
        } else {
            debug!("actor won't be restarted");
            sv.objects.remove(&key).map(|(_, v)| v)
        }
        .expect("where is the current actor?");

        // TODO: should we unregister the address right after failure?
        sv.context.book().remove(addr);
    };

    // tokio-console complains about futures larger than 1KiB even if tokio boxes
    // such futures by itself, so we reduce noise in the console this way =(
    #[cfg(all(tokio_unstable, feature = "tokio-tracing"))]
    let fut = Box::pin(fut);

    #[cfg(feature = "unstable-stuck-detection")]
    let wrapped = MeasurePoll::new(fut.instrument(span), stuck_detector);
    #[cfg(not(feature = "unstable-stuck-detection"))]
    let wrapped = MeasurePoll::new(fut.instrument(span));

    wrapped
}

#[derive(Default)]
struct PendingRemoteSpawn {
    in_flight: Mutex<bool>,
    cv: Condvar,
}

impl PendingRemoteSpawn {
    fn wait_for_slot(&self) {
        let mut in_flight = self.in_flight.lock();
        while *in_flight {
            self.cv.wait(&mut in_flight);
        }
    }
}

struct InFlightGuard(Arc<PendingRemoteSpawn>);

impl InFlightGuard {
    fn arm(pending: Arc<PendingRemoteSpawn>) -> Self {
        *pending.in_flight.lock() = true;
        Self(pending)
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        let mut in_flight = self.0.in_flight.lock();
        *in_flight = false;
        self.0.cv.notify_all();
    }
}

fn extract_response_token<R: Request>(envelope: Envelope) -> ResponseToken<R> {
    msg!(match envelope {
        (R, token) => token,
        _ => unreachable!(),
    })
}
