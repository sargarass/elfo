#![allow(private_interfaces)]

use std::{
    hash::Hash,
    mem,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use fxhash::FxHashMap;
use metrics::{Key, Unit};
use parking_lot::{Mutex, MutexGuard};
use thread_local::ThreadLocal;

use elfo_core::{ActorMeta, Addr, coop, scope::Scope};
use elfo_utils::CachePadded;

use crate::{
    double_buffer::{self, DoubleBuffer},
    metrics::{Counter, Gauge, GaugeOrigin, Histogram, MetricKind},
    protocol::{Description, Metrics, Snapshot},
    stats::{ShardStats, StorageStats},
};

// === Scopes ===

// TODO: use real `Key` (with pointer comparison).
type KeyHash = u64;

pub(crate) trait ScopeKind: Sized {
    type Scope;
    type Key: Copy + Hash + Eq;
    type Meta: Clone;

    fn get_meta(scope: &Self::Scope) -> &Self::Meta;
    fn make_key(scope: &Self::Scope, key: &Key) -> Self::Key;
    fn registries(shard: &mut ShardData) -> &mut Registries<Self>;
    fn gauge_shared(storage: &Storage) -> &Mutex<GaugeOrigins<Self>>;
    fn snapshot<'s>(snapshot: &'s mut Snapshot, meta: &Self::Meta) -> &'s mut Metrics;
}

pub(crate) struct GlobalScope;

impl ScopeKind for GlobalScope {
    type Key = KeyHash;
    type Meta = ();
    type Scope = ();

    fn get_meta(_scope: &Self::Scope) -> &Self::Meta {
        &()
    }

    fn make_key(_scope: &Self::Scope, key: &Key) -> Self::Key {
        key.get_hash()
    }

    fn registries(shard: &mut ShardData) -> &mut Registries<Self> {
        &mut shard.global
    }

    fn gauge_shared(storage: &Storage) -> &Mutex<GaugeOrigins<Self>> {
        &storage.gauge_shared.global
    }

    fn snapshot<'s>(snapshot: &'s mut Snapshot, _meta: &Self::Meta) -> &'s mut Metrics {
        &mut snapshot.global
    }
}

pub(crate) struct GroupScope;

impl ScopeKind for GroupScope {
    type Key = (Addr, KeyHash);
    // TODO: replace with EBR?
    type Meta = Arc<ActorMeta>;
    type Scope = Scope;

    fn get_meta(scope: &Self::Scope) -> &Self::Meta {
        scope.telemetry_meta()
    }

    fn make_key(scope: &Self::Scope, key: &Key) -> Self::Key {
        debug_assert_ne!(scope.group(), Addr::NULL);
        (scope.group(), key.get_hash())
    }

    fn registries(shard: &mut ShardData) -> &mut Registries<Self> {
        &mut shard.groupwise
    }

    fn gauge_shared(storage: &Storage) -> &Mutex<GaugeOrigins<Self>> {
        &storage.gauge_shared.groupwise
    }

    fn snapshot<'s>(snapshot: &'s mut Snapshot, meta: &Self::Meta) -> &'s mut Metrics {
        snapshot.groupwise.entry(meta.group.clone()).or_default()
    }
}

pub(crate) struct ActorScope;

impl ScopeKind for ActorScope {
    type Key = (/* group */ Addr, KeyHash);
    // TODO: replace with EBR?
    type Meta = Arc<ActorMeta>;
    type Scope = Scope;

    fn get_meta(scope: &Self::Scope) -> &Self::Meta {
        scope.telemetry_meta()
    }

    fn make_key(scope: &Self::Scope, key: &Key) -> Self::Key {
        debug_assert_ne!(scope.group(), Addr::NULL);

        let telemetry_key = &scope.telemetry_meta().key;
        debug_assert!(!telemetry_key.is_empty());

        // TODO: cache a hash of the telemetry key.
        let key_hash = fxhash::hash64(&(telemetry_key, key.get_hash()));
        (scope.group(), key_hash)
    }

    fn registries(shard: &mut ShardData) -> &mut Registries<Self> {
        &mut shard.actorwise
    }

    fn gauge_shared(storage: &Storage) -> &Mutex<GaugeOrigins<Self>> {
        &storage.gauge_shared.actorwise
    }

    fn snapshot<'s>(snapshot: &'s mut Snapshot, meta: &Self::Meta) -> &'s mut Metrics {
        snapshot.actorwise.entry(meta.clone()).or_default()
    }
}

// === Storage ===

/// The storage for metrics.
///
/// The main idea here is to have a separate shard for each thread and merge
/// them periodically by the telemeter actor. It *dramatically* reduces
/// contention, especially if multiple actors use the same telemetry key,
/// what's common for per-group telemetry or per-actor grouped telemetry.
pub(crate) struct Storage {
    shards: ThreadLocal<Shard>,
    // Shared gauge origins between shards. See `Gauge` for more details.
    gauge_shared: GaugeShared,
    descriptions: Mutex<FxHashMap<String, Description>>,
    // Allows only one collector at a time.
    merging: AtomicBool,
}

// Wait for busy shards: spin first, then yield, then sleep between passes.
const SPIN_PASSES: u32 = 256;
const YIELD_PASSES: u32 = 16;
const SLEEP_PASS: Duration = Duration::from_millis(1);

// The owner writes to one buffer while the telemeter collects the other.
// See `double_buffer` for the synchronization protocol.
#[derive(Default)]
struct Shard {
    data: DoubleBuffer<ShardData>,
    // `ThreadLocal` stores shards next to each other. Padding prevents false
    // sharing between the fields that neighboring owners access on each write.
    _align: CachePadded<()>,
}

#[derive(Default)]
struct ShardData {
    global: Registries<GlobalScope>,
    groupwise: Registries<GroupScope>,
    actorwise: Registries<ActorScope>,
}

struct Registries<S: ScopeKind> {
    counters: Registry<S, Counter>,
    gauges: Registry<S, Gauge>,
    histograms: Registry<S, Histogram>,
}

impl ShardData {
    /// Creates empty registries with capacity for the current keys.
    /// This reduces tail latency by avoiding allocation on the write path.
    fn empty_like(&self) -> Self {
        Self {
            global: self.global.empty_like(),
            groupwise: self.groupwise.empty_like(),
            actorwise: self.actorwise.empty_like(),
        }
    }
}

impl<S: ScopeKind> Registries<S> {
    fn empty_like(&self) -> Self {
        fn empty<K, V>(registry: &FxHashMap<K, V>) -> FxHashMap<K, V> {
            FxHashMap::with_capacity_and_hasher(registry.len(), <_>::default())
        }

        Self {
            counters: empty(&self.counters),
            gauges: empty(&self.gauges),
            histograms: empty(&self.histograms),
        }
    }
}

impl<S: ScopeKind> Default for Registries<S> {
    fn default() -> Self {
        Self {
            counters: Default::default(),
            gauges: Default::default(),
            histograms: Default::default(),
        }
    }
}

type Registry<S, M> = FxHashMap<<S as ScopeKind>::Key, RegEntry<S, M>>;

struct RegEntry<S: ScopeKind, M> {
    key: Key,
    data: M,
    meta: S::Meta,
}

impl<S: ScopeKind, M: MetricKind> RegEntry<S, M> {
    #[cold]
    fn new(scope: &S::Scope, key: &Key, shared: M::Shared) -> Self {
        Self {
            key: key.clone(),
            data: M::new(shared),
            meta: S::get_meta(scope).clone(),
        }
    }
}

#[derive(Default)]
struct GaugeShared {
    global: Mutex<GaugeOrigins<GlobalScope>>,
    groupwise: Mutex<GaugeOrigins<GroupScope>>,
    actorwise: Mutex<GaugeOrigins<ActorScope>>,
}

type GaugeOrigins<S> = FxHashMap<<S as ScopeKind>::Key, Arc<GaugeOrigin>>;

impl Default for Storage {
    fn default() -> Self {
        // Select the barrier mode before the first write.
        let mode = double_buffer::mode();
        tracing::info!(?mode, "telemetry storage barrier");

        Self {
            shards: ThreadLocal::new(),
            gauge_shared: Default::default(),
            descriptions: Default::default(),
            merging: AtomicBool::new(false),
        }
    }
}

impl Storage {
    pub(crate) fn descriptions(&self) -> MutexGuard<'_, FxHashMap<String, Description>> {
        self.descriptions.lock()
    }

    pub(crate) fn describe(&self, key: &Key, unit: Option<Unit>, details: Option<&'static str>) {
        if unit.is_none() && details.is_none() {
            return;
        }

        let mut descriptions = self.descriptions.lock();
        descriptions.insert(key.name().to_string(), Description { details, unit });
    }

    pub(crate) fn upsert<S, M>(&self, scope: &S::Scope, key: &Key, value: M::Value)
    where
        S: ScopeKind,
        M: Storable,
    {
        let shard = self.shard();
        let reg_key = S::make_key(scope, key);

        let write = |data: &mut ShardData| {
            let registry = M::registry(S::registries(data));
            let entry = registry.entry(reg_key).or_insert_with(|| {
                let shared = M::shared::<S>(self, reg_key);
                RegEntry::<S, M>::new(scope, key, shared)
            });
            entry.data.update(value);
        };

        // SAFETY: this thread owns the shard, and `write` does not call `upsert`.
        unsafe { shard.data.write(write) };
    }

    #[inline]
    fn shard(&self) -> &Shard {
        match self.shards.get() {
            Some(shard) => shard,
            None => self.shard_slow(),
        }
    }

    // Keep shard allocation out of line to avoid stack realignment in callers.
    #[cold]
    #[inline(never)]
    fn shard_slow(&self) -> &Shard {
        self.shards.get_or_default()
    }

    /// Switches all shards to their other buffers and merges the previous buffers.
    ///
    /// Each call merges all metric kinds into `snapshot`, including counters and
    /// gauges. Otherwise, the next scrape misses values in the previous buffers.
    /// `only_compact` skips only storage statistics.
    pub(crate) async fn merge(&self, snapshot: &mut Snapshot, only_compact: bool) {
        // Reject concurrent collection.
        if self.merging.swap(true, Ordering::Acquire) {
            tracing::error!("concurrent merge of the telemetry storage is skipped");
            return;
        }
        let _merging = MergingGuard(&self.merging);

        let mut storage_stats = StorageStats::new::<Self>();

        if !only_compact {
            storage_stats.add_descriptions(&*self.descriptions.lock());
        }

        let shards: Vec<&Shard> = self.shards.iter().collect();
        shards.iter().for_each(|shard| shard.data.advance());
        if !double_buffer::barrier() {
            // Owners can still write to either buffer. Leave the data for later merges.
            tracing::error!("membarrier failed, metrics are left for the next cycle");
            return;
        }

        // Defer busy shards so a preempted owner does not delay other shards.
        // Each shard waits for at most one write to its previous buffer.
        let mut pending = shards;
        let mut idle_passes = 0u32;
        loop {
            let mut i = 0;
            while i < pending.len() {
                if pending[i].data.can_collect() {
                    let shard = pending.swap_remove(i);
                    self.merge_shard(shard, snapshot, &mut storage_stats).await;
                } else {
                    i += 1;
                }
            }

            if pending.is_empty() {
                break;
            }

            // Limit the spin count because an owner can be preempted.
            idle_passes += 1;
            if idle_passes <= SPIN_PASSES {
                std::hint::spin_loop();
            } else if idle_passes <= SPIN_PASSES + YIELD_PASSES {
                tokio::task::yield_now().await;
            } else {
                tokio::time::sleep(SLEEP_PASS).await;
            }
        }

        if !only_compact {
            storage_stats.emit();
        }
    }

    async fn merge_shard(&self, shard: &Shard, snapshot: &mut Snapshot, stats: &mut StorageStats) {
        // Replace the previous buffer with empty registries before the async merge.
        // SAFETY: `merge` advanced the epoch, and both `barrier` and `can_collect`
        // returned true. `merging` excludes other collectors until the merge ends.
        let mut data = unsafe {
            shard.data.collect(|previous| {
                let empty = previous.empty_like();
                mem::replace(previous, empty)
            })
        };

        let mut shard_stats = ShardStats::new::<Shard>();
        self.merge_registries::<GlobalScope>(&mut data, snapshot, &mut shard_stats)
            .await;
        self.merge_registries::<GroupScope>(&mut data, snapshot, &mut shard_stats)
            .await;
        self.merge_registries::<ActorScope>(&mut data, snapshot, &mut shard_stats)
            .await;
        stats.add_shard(&shard_stats);
    }

    async fn merge_registries<S: ScopeKind>(
        &self,
        shard: &mut ShardData,
        snapshot: &mut Snapshot,
        stats: &mut ShardStats,
    ) {
        let registries = S::registries(shard);

        self.merge_registry::<S, Counter>(registries, snapshot, stats)
            .await;
        self.merge_registry::<S, Gauge>(registries, snapshot, stats)
            .await;
        self.merge_registry::<S, Histogram>(registries, snapshot, stats)
            .await;
    }

    async fn merge_registry<S: ScopeKind, M: Storable>(
        &self,
        registries: &mut Registries<S>,
        snapshot: &mut Snapshot,
        stats: &mut ShardStats,
    ) {
        let registry = mem::take(M::registry(registries));

        stats.add_registry(&registry);

        for (_, entry) in registry {
            let metrics = S::snapshot(snapshot, &entry.meta);
            let out = M::snapshot(metrics, &entry.key);
            let additional_size = entry.data.merge(out);
            stats.add_additional_size(additional_size);
        }

        // The merge process can be quite long, so we should be preemptive.
        coop::consume_budget().await;
    }
}

struct MergingGuard<'a>(&'a AtomicBool);

impl Drop for MergingGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

// === Storable ===

pub(crate) trait Storable: MetricKind {
    fn registry<S: ScopeKind>(registries: &mut Registries<S>) -> &mut Registry<S, Self>;
    fn shared<S: ScopeKind>(storage: &Storage, key: S::Key) -> Self::Shared;
    fn snapshot<'s>(metrics: &'s mut Metrics, key: &Key) -> &'s mut Self::Output;
}

impl Storable for Counter {
    fn registry<S: ScopeKind>(registries: &mut Registries<S>) -> &mut Registry<S, Self> {
        &mut registries.counters
    }

    fn shared<S: ScopeKind>(_: &Storage, _: S::Key) -> Self::Shared {}

    fn snapshot<'s>(metrics: &'s mut Metrics, key: &Key) -> &'s mut Self::Output {
        // TODO: hashbrown `entry_ref` (extra crate) or `contains_key` (double lookup).
        metrics.counters.entry(key.clone()).or_default()
    }
}

impl Storable for Gauge {
    fn registry<S: ScopeKind>(registries: &mut Registries<S>) -> &mut Registry<S, Self> {
        &mut registries.gauges
    }

    fn shared<S: ScopeKind>(storage: &Storage, key: S::Key) -> Self::Shared {
        let mut shared = S::gauge_shared(storage).lock();
        shared.entry(key).or_default().clone()
    }

    fn snapshot<'s>(metrics: &'s mut Metrics, key: &Key) -> &'s mut Self::Output {
        // TODO: hashbrown `entry_ref` (extra crate) or `contains_key` (double lookup).
        metrics.gauges.entry(key.clone()).or_default()
    }
}

impl Storable for Histogram {
    fn registry<S: ScopeKind>(registries: &mut Registries<S>) -> &mut Registry<S, Self> {
        &mut registries.histograms
    }

    fn shared<S: ScopeKind>(_: &Storage, _: S::Key) -> Self::Shared {}

    fn snapshot<'s>(metrics: &'s mut Metrics, key: &Key) -> &'s mut Self::Output {
        // TODO: hashbrown `entry_ref` (extra crate) or `contains_key` (double lookup).
        metrics.histograms.entry(key.clone()).or_default()
    }
}

#[cfg(all(test, not(telemeter_loom)))]
mod tests {
    use std::{
        sync::{
            Barrier,
            atomic::{AtomicBool, Ordering},
        },
        thread,
    };

    use metrics::GaugeValue;

    use super::*;

    // Concurrent writes and merges must preserve every update exactly once.
    // Use release mode on Linux for `membarrier`, or set
    // `ELFO_TELEMETRY_BARRIER=fence` for the fence fallback.
    #[test]
    fn concurrent_writes_and_merges_lose_nothing() {
        let threads: usize =
            std::env::var("TEST_THREADS_WRITERS").map_or(4, |v| v.parse().unwrap());
        let merges: u64 = if cfg!(miri) { 3 } else { 2_000 };
        // Limit raw histogram samples: on a shared core, writes can outpace merges.
        let hist_every: u64 = std::env::var("TEST_HIST_EVERY").map_or(64, |v| v.parse().unwrap());

        let storage = Arc::new(Storage::default());
        let counter = Key::from_name("test_counter");
        let histogram = Key::from_name("test_histogram");
        let gauge = Key::from_name("test_gauge");
        let start = Arc::new(Barrier::new(threads + 1));
        let stop = Arc::new(AtomicBool::new(false));

        let owners: Vec<_> = (0..threads)
            .map(|_| {
                let (storage, start, stop) = (storage.clone(), start.clone(), stop.clone());
                let (counter, histogram, gauge) =
                    (counter.clone(), histogram.clone(), gauge.clone());
                thread::spawn(move || {
                    start.wait();
                    let (mut counted, mut sampled) = (0u64, 0usize);
                    while !stop.load(Ordering::Relaxed) {
                        storage.upsert::<GlobalScope, Counter>(&(), &counter, 1);
                        storage.upsert::<GlobalScope, Gauge>(
                            &(),
                            &gauge,
                            GaugeValue::Increment(1.0),
                        );
                        counted += 1;
                        if counted % hist_every == 0 {
                            storage.upsert::<GlobalScope, Histogram>(&(), &histogram, 1.0);
                            sampled += 1;
                        }
                    }
                    (counted, sampled)
                })
            })
            .collect();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let mut snapshot = Snapshot::default();
        start.wait();
        let began = std::time::Instant::now();
        for i in 0..merges {
            rt.block_on(storage.merge(&mut snapshot, i % 2 == 1));
            if std::env::var_os("TEST_PROGRESS").is_some() && i % 100 == 0 {
                eprintln!("merge {i} at {:?}", began.elapsed());
            }
        }
        stop.store(true, Ordering::Relaxed);
        let (counted, sampled) = owners
            .into_iter()
            .map(|t| t.join().unwrap())
            .fold((0, 0), |(c, s), (c1, s1)| (c + c1, s + s1));
        rt.block_on(storage.merge(&mut snapshot, false));

        assert_eq!(snapshot.global.counters[&counter], counted);
        assert_eq!(snapshot.global.gauges[&gauge].0, counted as f64);
        assert_eq!(
            snapshot
                .global
                .histograms
                .get(&histogram)
                .map_or(0, |h| h.cumulative_count()),
            sampled
        );
        eprintln!(
            "mode {:?}: {merges} merges, {counted} counts",
            double_buffer::mode()
        );
    }
}
