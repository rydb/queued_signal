//! Bevy query mirroring via QueuedSignals.
//!
//! Provides [`use_bevy_query`] and [`use_bevy_single`] to create dioxus-side
//! signal mirrors of bevy queries, with automatic bidirectional synchronization.

use std::{
    any::TypeId,
    collections::{HashMap, HashSet},
    fmt::Debug,
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

mod macros;
pub mod single;

use crate::macros::*;

use crate::schedules::{DioxusSyncLast, DioxusSyncPostUpdate};
use bevy_ecs::{
    component::Mutable,
    prelude::*,
    query::{IterQueryData, QueryData, QueryFilter},
    world::CommandQueue,
};
use dioxus_core::use_drop;
use dioxus_hooks::{use_context, use_effect, use_future, use_signal};
use dioxus_signals::{ReadableExt, Signal, WritableExt};
use parking_lot::Mutex;
use queued_signal::state::{HealthStatus, QueuedSignal, SignalReadGuard, WriterDriver};
use tokio::sync::oneshot;
use trait_set::trait_set;

use crate::{CommandQueueSender, add_systems_through_world};

trait_set! {
    /// Component that can be synced with dioxus.
    pub trait DioxusComponentSync = Component<Mutability = Mutable> + Clone + 'static;
    /// Mirror query data that is Send + Sync.
    pub trait DioxusQuerySync = MirrorQueryData + Send + Sync;
}

/// Error state for an uninitialized query signal.
#[derive(Clone, Debug, PartialEq)]
pub enum QueryNoneState {
    /// Query not yet initialized down stream
    NotInitialized,
}

/// Tick the writer driver for each component mirror.
fn drive_component_signals<T: DioxusComponentSync>(mut components: Query<&mut DioxusMirror<T>>) {
    for mut component in &mut components {
        let old_version = component.version.load(std::sync::atomic::Ordering::Relaxed);
        // Drop guard before set_changed to avoid borrow conflict
        {
            let mut guard = component.driver.lock();
            guard.tick(Duration::ZERO);
        }
        let new_version = component.version.load(std::sync::atomic::Ordering::Relaxed);
        // Only mark as changed if the signal published,
        // avoiding unnecessary change detection on idle ticks.
        if new_version != old_version {
            component.set_changed();
        }
    }
}

fn drive_query_signal<Q: DioxusQuerySync + 'static, F: QueryFilter + 'static>(
    driver: ResMut<MirrorQueryWriteDriver<Q, F>>,
) {
    let mut guard = driver.0.lock();
    guard.tick(Duration::ZERO);
}

/// Mirrored version of a bevy component with query infrastructure.
#[derive(Component)]
pub struct DioxusMirror<T: DioxusComponentSync> {
    /// The queued signal holding the component's value.
    pub value: QueuedSignal<T>,

    /// Driver for ticking the signal associated with this component.
    pub(crate) driver: Arc<Mutex<WriterDriver<T>>>,

    /// Atomic version counter for change detection.
    pub version: Arc<AtomicU64>,
}

/// Tracking component counting active query references.
#[derive(Component)]
pub struct DioxusTrackingQueries<T: DioxusComponentSync> {
    /// Per-query-type tracking reference counts.
    pub tracking_counts: HashMap<(TypeId, TypeId), i32>,
    _component: PhantomData<T>,
}

impl<T: DioxusComponentSync + Debug> Debug for DioxusMirror<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DioxusMirror")
            .field("value", &self.value)
            .field("driver", &self.driver)
            .finish()
    }
}

impl<T: DioxusComponentSync> DioxusMirror<T> {
    /// Obtain a lightweight handle to this component mirror.
    pub fn handle(&self) -> DioxusMirrorHandle<T> {
        DioxusMirrorHandle {
            value: self.value.clone(),
        }
    }
}

/// Dioxus handle for a bevy-side mirrored component.
#[derive(Clone)]
pub struct DioxusMirrorHandle<T: DioxusComponentSync> {
    /// The signal handle for the mirrored value.
    pub value: QueuedSignal<T>,
}

/// Total number of active dioxus components using this query.
#[derive(Resource)]
pub struct MirrorQueryHandleCount<Q: MirrorQueryData, F: QueryFilter> {
    /// Current count of active handles.
    pub count: i32,
    _phantom: fn() -> PhantomData<(Q, F)>,
}

impl<Q: MirrorQueryData, F: QueryFilter> Default for MirrorQueryHandleCount<Q, F> {
    fn default() -> Self {
        Self {
            count: Default::default(),
            _phantom: || Default::default(),
        }
    }
}

/// Whether any dioxus component currently needs this query.
#[derive(Resource)]
pub struct MirrorQueryActive<Q: MirrorQueryData, F: QueryFilter> {
    /// Whether any dioxus component currently needs this query.
    pub active: bool,
    _phantom: fn() -> PhantomData<(Q, F)>,
}

impl<Q: MirrorQueryData, F: QueryFilter> PartialEq for MirrorQueryActive<Q, F> {
    fn eq(&self, other: &Self) -> bool {
        self.active == other.active
    }
}

impl<Q: MirrorQueryData, F: QueryFilter> Default for MirrorQueryActive<Q, F> {
    fn default() -> Self {
        Self {
            active: Default::default(),
            _phantom: || Default::default(),
        }
    }
}

impl<T: DioxusComponentSync> std::ops::Deref for DioxusMirrorHandle<T> {
    type Target = QueuedSignal<T>;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

fn query_to_tracking_id<Q: DioxusQuerySync, F: QueryFilter + 'static>() -> (TypeId, TypeId) {
    (TypeId::of::<Q::MirrorItem>(), TypeId::of::<F>())
}

impl<T: DioxusComponentSync> DioxusMirror<T> {
    /// initialize DioxusMirror and decompose it into its other dependent components (for impl Bundle)
    pub fn init_and_decompose<Q: DioxusQuerySync, F: QueryFilter + 'static>(
        value: T,
    ) -> (Self, DioxusTrackingQueries<T>) {
        let mut driver = WriterDriver::new(value.clone());

        let set_value_tx = driver.set_value_tx.clone();
        let set_tx = driver.set_tx.clone();
        let add_tx = driver.add_tx.clone();
        let queued_state = driver.queued_state.clone();

        let version = Arc::new(AtomicU64::new(0));
        driver.set_publish_counter(version.clone());

        let driver_arc = Arc::new(Mutex::new(driver));

        let mut map = HashMap::new();
        map.insert(query_to_tracking_id::<Q, F>(), 0);
        (
            Self {
                value: QueuedSignal::from_parts(
                    queued_state,
                    Some(driver_arc.clone()),
                    add_tx,
                    set_tx,
                    set_value_tx,
                ),
                driver: driver_arc,
                version,
            },
            DioxusTrackingQueries {
                tracking_counts: map,
                _component: PhantomData,
            },
        )
    }
}

/// Copies a dioxus-side signal value back into the bevy component.
#[allow(clippy::type_complexity)]
fn sync_component_to_mirror<T: DioxusComponentSync>(
    components: Query<(Entity, &mut DioxusMirror<T>, &mut T), Changed<DioxusMirror<T>>>,
    mut last_versions: Local<HashMap<Entity, u64>>,
) {
    for (entity, mut mirror, mut value) in components {
        let current_version = mirror.version.load(Ordering::Acquire);
        // First time seeing this entity: always sync so that no
        // dioxus-side mutation is silently dropped.
        let is_changed = match last_versions.get(&entity) {
            Some(last) => &current_version != last,
            None => true,
        };
        if is_changed {
            *value.bypass_change_detection() = mirror.value.read().as_ref().clone();
            mirror.set_changed();
            last_versions.insert(entity, current_version);
        }
    }
}

fn delete_unused_mirrors<T: DioxusComponentSync>(
    mut commands: Commands,
    trackers: Query<(Entity, &DioxusTrackingQueries<T>), Changed<DioxusTrackingQueries<T>>>,
) {
    for (entity, tracker) in &trackers {
        if tracker.tracking_counts.is_empty() {
            commands
                .entity(entity)
                .remove::<DioxusMirror<T>>()
                .remove::<DioxusTrackingQueries<T>>();
        }
    }
}

/// Sync bevy components to their changed mirrors.
fn sync_mirror_to_component<T: DioxusComponentSync>(
    mut components: Query<(&T, &DioxusMirror<T>), Changed<T>>,
) {
    for (value, mirror) in &mut components {
        mirror.value.set_value(value.clone());
    }
}

/// Accumulates tracking delta requests for batch processing.
#[derive(Resource)]
struct PendingQueryTrackingDeltas<Q: MirrorQueryData, F: QueryFilter> {
    /// Cumulative tracking delta.
    pending: i32,
    _querydata: fn() -> PhantomData<Q>,
    _filter: fn() -> PhantomData<F>,
}

impl<Q: MirrorQueryData, F: QueryFilter> Default for PendingQueryTrackingDeltas<Q, F> {
    fn default() -> Self {
        Self {
            pending: Default::default(),
            _querydata: || Default::default(),
            _filter: || Default::default(),
        }
    }
}

/// Command updating tracking reference counts for a query.
pub struct UpdateTrackingQueries<Q: DioxusQuerySync, F: QueryFilter> {
    /// The tracking delta (+1 for mount, -1 for unmount).
    pub delta: i32,
    _phantom: fn() -> PhantomData<(Q, F)>,
}

impl<Q: DioxusQuerySync + 'static, F: QueryFilter + 'static> Command
    for UpdateTrackingQueries<Q, F>
{
    type Out = ();

    fn apply(self, world: &mut World) {
        let mut pending_delta = world.get_resource_or_init::<PendingQueryTrackingDeltas<Q, F>>();
        pending_delta.pending += self.delta;
    }
}

/// Apply tracking query deltas to relevant mirrored components.
fn apply_tracking_queries_delta<Q: DioxusQuerySync + 'static, F: QueryFilter + 'static>(
    mirror_components: Query<Q::TrackingQueriesQuerydataMut, F>,
    mut pending_tracking_delta: ResMut<PendingQueryTrackingDeltas<Q, F>>,
    mut handle_count: ResMut<MirrorQueryHandleCount<Q, F>>,
    mut active: ResMut<MirrorQueryActive<Q, F>>,
) {
    let delta = pending_tracking_delta.bypass_change_detection().pending;
    if delta != 0 {
        debug!("applying delta {}", delta);
        for item in mirror_components {
            Q::apply_tracking_delta::<F>(item, delta);
        }
        handle_count.bypass_change_detection().count += delta;
        active.bypass_change_detection().active = handle_count.bypass_change_detection().count > 0;
        pending_tracking_delta.bypass_change_detection().pending = 0;
    }
}

#[derive(Resource)]
/// Marker indicating a query's mirror systems have been registered and initialized.
pub struct QueryMirrorInitialized<Q: QueryData, F: QueryFilter> {
    initialized: bool,
    _querydata: fn() -> PhantomData<Q>,
    _filter: fn() -> PhantomData<F>,
}

/// Synchronize a bevy query's results into the dioxus signal mirror.
pub fn sync_query_mirror_to_signal<T: DioxusQuerySync + 'static, F: QueryFilter + 'static>(
    components_without_signals: Query<T, (F, T::MirrorSignalsWithoutFilter)>,
    mirror_components: Query<T::MirrorSignalsQueryDataImMut, F>,
    mirror_signal: ResMut<MirrorQuerySignal<T, F>>,
    mut commands: Commands,
    mut init_status: ResMut<QueryMirrorInitialized<T, F>>,
    // Track last-seen entity count to skip removal detection when stable
    mut last_entity_count: Local<usize>,
    // Track per-entity combined version for cheap change detection (single u64, no alloc)
    mut last_versions: Local<HashMap<Entity, u64>>,
) {
    // Pre-init: spawn mirrors and wait for all entities to be ready.
    if !init_status.initialized {
        // Expensive Without+Or filter is only paid on pre-init ticks.
        // Once init completes, this block is skipped.
        if components_without_signals.count() == 0 {
            let current_map = mirror_components
                .iter()
                .map(|item| (T::get_mirror_entity(&item), T::clone_dioxus_signals(&item)))
                .collect::<HashMap<_, _>>();
            mirror_signal.0.set_value(MirrorQuery {
                value: current_map,
                _marker: PhantomData,
            });
            init_status.initialized = true;

            // Seed version tracker with initial values
            *last_entity_count = mirror_components.iter().count();
            for item in mirror_components.iter() {
                let entity = T::get_mirror_entity(&item);
                last_versions.insert(entity, T::extract_version(&item));
            }
            trace!(
                "finished initializing component mirror to: {}",
                mirror_components.iter().count()
            );
        } else {
            // Pre-init: spawn mirrors for entities that need them
            for item in components_without_signals {
                trace!("component without signal found");
                let entity = T::get_query_entity(&item);
                let bundle = T::get_mirror_bundle::<F>(item);
                commands.entity(entity).insert_if_new(bundle);
            }
        }
        return;
    }

    // Post-init: detect new entities needing mirrors.
    for item in components_without_signals {
        trace!("component without signal found");
        let entity = T::get_query_entity(&item);
        let bundle = T::get_mirror_bundle::<F>(item);
        commands.entity(entity).insert_if_new(bundle);
    }

    // Single pass over mirror components to count entities
    // while detecting changes via atomic version loads.
    let mut current_count: usize = 0;
    let mut has_changes = false;
    let mut changed_entities: Vec<(Entity, T::MirrorItemHandles)> = Vec::new();

    for item in mirror_components.iter() {
        current_count += 1;
        let entity = T::get_mirror_entity(&item);
        let current_ver = T::extract_version(&item);

        let changed = match last_versions.get(&entity) {
            Some(last) => current_ver != *last,
            None => true, // new entity, always needs update
        };

        if changed {
            has_changes = true;
            let handles = T::clone_dioxus_signals(&item);
            changed_entities.push((entity, handles));
            last_versions.insert(entity, current_ver);
        }
    }

    let count_changed = current_count != *last_entity_count;
    *last_entity_count = current_count;

    // Detect removals and seed new entity versions on count change.
    if count_changed {
        // Seed versions for any new entities that weren't in the scan above
        for item in mirror_components.iter() {
            let entity = T::get_mirror_entity(&item);
            last_versions
                .entry(entity)
                .or_insert_with(|| T::extract_version(&item));
        }
    }

    // Entity removal detection when count changed.
    let mut removed_entities: Vec<Entity> = Vec::new();
    if count_changed {
        for (&entity, _) in last_versions.iter() {
            if !mirror_components.contains(entity) {
                removed_entities.push(entity);
            }
        }
        for entity in &removed_entities {
            last_versions.remove(entity);
        }
    }

    // Apply insertions and removals directly into the signal map.
    if has_changes || !removed_entities.is_empty() {
        mirror_signal.0.mutate_set(move |n| {
            let map = &mut n.value;

            for entity in &removed_entities {
                map.remove(entity);
            }
            for (entity, handles) in &changed_entities {
                map.insert(*entity, handles.clone());
            }
        });
    }
}

#[derive(Resource, Default)]
/// Set of component types that have registered mirror sync systems.
pub struct MirroredComponents(HashSet<TypeId>);

/// Command requesting mirror sync systems for a component type.
pub struct RequestComponentsMirror<T: DioxusComponentSync> {
    _marker: PhantomData<T>,
}

impl<T: DioxusComponentSync> Default for RequestComponentsMirror<T> {
    fn default() -> Self {
        Self {
            _marker: Default::default(),
        }
    }
}

impl<T: DioxusComponentSync> Command for RequestComponentsMirror<T> {
    type Out = ();

    fn apply(self, world: &mut World) {
        let mut mirrored_components =
            world.get_resource_or_insert_with(MirroredComponents::default);
        if !mirrored_components.0.contains(&TypeId::of::<T>()) {
            // Mark T as mirrored before adding systems so subsequent
            // requests for the same component type are no-ops.
            mirrored_components.0.insert(TypeId::of::<T>());

            add_systems_through_world(
                world,
                DioxusSyncPostUpdate,
                (
                    sync_mirror_to_component::<T>,
                    drive_component_signals::<T>,
                    sync_component_to_mirror::<T>,
                    delete_unused_mirrors::<T>,
                )
                    .chain(),
            );
        }
    }
}
/// Command requesting a mirror for a specific query type.
pub struct RequestQueryMirror<T: DioxusQuerySync, F: QueryFilter + 'static> {
    response_tx: oneshot::Sender<QueuedSignal<MirrorQuery<T, F>>>,
}

impl<T: DioxusQuerySync + 'static, F: QueryFilter> Command for RequestQueryMirror<T, F> {
    type Out = ();

    fn apply(self, world: &mut World) {
        let signal_to_send: QueuedSignal<MirrorQuery<T, F>> =
            match world.get_resource::<MirrorQuerySignal<T, F>>() {
                Some(signal) => signal.0.clone(),
                None => {
                    T::register_mirror_sync_systems::<F>(world);

                    let query_driver = WriterDriver::new(MirrorQuery::default());
                    // sync must run before drive: sync sends mutations to the
                    // flume, then drive ticks the writer and publishes them.
                    // .chain() guarantees this order.
                    add_systems_through_world(
                        world,
                        DioxusSyncLast,
                        (
                            sync_query_mirror_to_signal::<T, F>.run_if(resource_equals(
                                MirrorQueryActive::<T, F> {
                                    active: true,
                                    _phantom: || PhantomData,
                                },
                            )),
                            drive_query_signal::<T, F>,
                        )
                            .chain(),
                    );
                    add_systems_through_world(
                        world,
                        DioxusSyncPostUpdate,
                        apply_tracking_queries_delta::<T, F>
                            .run_if(resource_changed::<PendingQueryTrackingDeltas<T, F>>),
                    );

                    let set_value_tx = query_driver.set_value_tx.clone();
                    let set_tx = query_driver.set_tx.clone();
                    let add_tx = query_driver.add_tx.clone();
                    let queued_state = query_driver.queued_state.clone();

                    let driver_arc = Arc::new(Mutex::new(query_driver));

                    let signal = QueuedSignal::from_parts(
                        queued_state,
                        Some(driver_arc.clone()),
                        add_tx,
                        set_tx,
                        set_value_tx,
                    );

                    world.insert_resource(MirrorQueryWriteDriver(driver_arc));
                    world.insert_resource(MirrorQuerySignal(signal.clone()));
                    world.insert_resource(QueryMirrorInitialized::<T, F> {
                        initialized: false,
                        _querydata: || PhantomData,
                        _filter: || PhantomData,
                    });
                    world.insert_resource(MirrorQueryActive::<T, F> {
                        active: false,
                        _phantom: || PhantomData,
                    });
                    world.insert_resource(MirrorQueryHandleCount::<T, F> {
                        count: 0,
                        _phantom: || PhantomData,
                    });
                    world.init_resource::<PendingQueryTrackingDeltas<T, F>>();
                    signal
                }
            };

        let _ = self.response_tx.send(signal_to_send);
    }
}

/// Query data trait for mirroring bevy queries to dioxus signals.
/// query that corresponds to the mirorred signals that correspond to the underlying bevy value,
/// ```text
/// let signal = MirrorQuery<(Entity, &mut A, &mut B)> -> Query<(Entity, &mut DioxusMirror<A>, &mut DioxusMirror<B>)> -> HashMap<Entity, (Entity, DioxusMirror<A>, DioxusMirror<B>)> -> QueuedQuerySignal
/// ```
pub trait MirrorQueryData: QueryData + IterQueryData {
    /// Mirrored query item type.
    ///
    /// E.G;
    type MirrorItem: Send + Sync + 'static;

    /// Handle type for the mirrored query item results.
    ///
    /// E.G; if `MirrorItem` is
    /// ```text
    /// (Entity, DioxusMirror<A>, DioxusMirror<A>)
    /// ```
    /// this would be
    /// ```text
    /// (Entity, DioxusMirrorHandle<A>, DioxusMirrorHandle<B>)
    /// ```
    type MirrorItemHandles: Clone + Send + Sync + 'static;

    /// Query data type for reading the mirrored signals.
    ///
    /// E.G; if MirrorQueryData is
    ///
    /// ```text
    /// Query<(Entity, &mut A, &mut B)>
    /// ```
    ///
    /// This would be:
    ///
    /// ```text
    /// Query<(Entity, &mut DioxusMirror<A>, &mut DioxusMirror<B>)>
    /// ```
    type MirrorSignalsQueryDataImMut: QueryData + IterQueryData;

    /// Filter for components that are missing their mirrors.
    type MirrorSignalsWithoutFilter: QueryFilter;

    /// Filter for entities with changed mirrored components.
    type MirrorSignalsChangedFilter: QueryFilter;

    /// Query type for tracking reference counts per component.
    ///
    /// E.G; if the query is:
    ///
    /// ```text
    /// Query<(Entity, &mut A, &mut B)>
    /// ```
    ///
    /// this will be
    /// ```text
    /// Query<(Entity, &mut DioxusTrackingQueries<A>, &mut DioxusTrackingQueries<B>)>
    /// ```
    type TrackingQueriesQuerydataMut: QueryData + IterQueryData;

    /// Register the sync systems for this mirror query.
    fn register_mirror_sync_systems<F: QueryFilter>(world: &mut World);

    /// Get the entity from a mirrored query item.
    ///
    /// E.G, for:
    ///
    /// Query<(Entity, &mut DioxusMirror<A>, &mut DioxusMirror<B>)>
    fn get_mirror_entity<'w, 's>(
        item: &<<Self::MirrorSignalsQueryDataImMut as QueryData>::ReadOnly as QueryData>::Item<
            'w,
            's,
        >,
    ) -> Entity;
    /// Get the entity from the original bevy query item.
    ///
    /// E.G, for:
    /// QueryMirror<(Entity, &mut A, &mut B)>
    fn get_query_entity<'w, 's>(item: &Self::Item<'w, 's>) -> Entity;

    /// Create an insertable bundle of mirror components.
    fn get_mirror_bundle<'w, 's, F: QueryFilter + 'static>(item: Self::Item<'w, 's>)
    -> impl Bundle;

    /// Adjust the tracking reference count per mirror item.
    fn apply_tracking_delta<'w, 's, F: QueryFilter + 'static>(
        item: <Self::TrackingQueriesQuerydataMut as QueryData>::Item<'w, 's>,
        delta: i32,
    );

    /// Clone a borrowed mirror query item into owned handles.
    fn clone_dioxus_signals<'w, 's>(
        item: &<<Self::MirrorSignalsQueryDataImMut as QueryData>::ReadOnly as QueryData>::Item<
            'w,
            's,
        >,
    ) -> Self::MirrorItemHandles;

    /// Extract a combined version identifier from mirrored components.
    fn extract_version<'w, 's>(
        item: &<<Self::MirrorSignalsQueryDataImMut as QueryData>::ReadOnly as QueryData>::Item<
            'w,
            's,
        >,
    ) -> u64;
}

impl<A: DioxusComponentSync, B: DioxusComponentSync> MirrorQueryData for (Entity, &mut A, &mut B) {
    type MirrorItem = (Entity, DioxusMirror<A>, DioxusMirror<B>);

    type MirrorSignalsQueryDataImMut = (Entity, &'static DioxusMirror<A>, &'static DioxusMirror<B>);

    type MirrorSignalsWithoutFilter = Or<(Without<DioxusMirror<A>>, Without<DioxusMirror<B>>)>;

    type MirrorItemHandles = (Entity, DioxusMirrorHandle<A>, DioxusMirrorHandle<B>);

    type MirrorSignalsChangedFilter = Or<(Changed<DioxusMirror<A>>, Changed<DioxusMirror<B>>)>;

    type TrackingQueriesQuerydataMut = (
        Entity,
        &'static mut DioxusTrackingQueries<A>,
        &'static mut DioxusTrackingQueries<B>,
    );

    fn register_mirror_sync_systems<F: QueryFilter>(world: &mut World) {
        world
            .commands()
            .queue(RequestComponentsMirror::<A>::default());
        world
            .commands()
            .queue(RequestComponentsMirror::<B>::default());
    }

    fn get_mirror_entity<'w, 's>(
        item: &<<Self::MirrorSignalsQueryDataImMut as QueryData>::ReadOnly as QueryData>::Item<
            'w,
            's,
        >,
    ) -> Entity {
        item.0
    }

    fn get_mirror_bundle<'w, 's, F: QueryFilter + 'static>(
        item: Self::Item<'w, 's>,
    ) -> impl Bundle {
        (
            DioxusMirror::init_and_decompose::<Self, F>(item.1.clone()),
            DioxusMirror::init_and_decompose::<Self, F>(item.2.clone()),
        )
    }

    fn clone_dioxus_signals<'w, 's>(
        item: &<<Self::MirrorSignalsQueryDataImMut as QueryData>::ReadOnly as QueryData>::Item<
            'w,
            's,
        >,
    ) -> Self::MirrorItemHandles {
        (item.0, item.1.handle(), item.2.handle())
    }

    fn get_query_entity<'w, 's>(item: &Self::Item<'w, 's>) -> Entity {
        item.0
    }

    fn apply_tracking_delta<'w, 's, F: QueryFilter + 'static>(
        mut item: <Self::TrackingQueriesQuerydataMut as QueryData>::Item<'w, 's>,
        delta: i32,
    ) {
        let id = query_to_tracking_id::<Self, F>();

        let current_delta = item.1.tracking_counts.entry(id).or_insert(0);
        *current_delta += delta;
        if *current_delta <= 0 {
            item.1.tracking_counts.remove(&id);
        }

        let current_delta = item.2.tracking_counts.entry(id).or_insert(0);
        *current_delta += delta;
        if *current_delta <= 0 {
            item.2.tracking_counts.remove(&id);
        }
    }

    fn extract_version<'w, 's>(
        item: &<<Self::MirrorSignalsQueryDataImMut as QueryData>::ReadOnly as QueryData>::Item<
            'w,
            's,
        >,
    ) -> u64 {
        let (_, a, b) = item;
        let va = a.version.load(std::sync::atomic::Ordering::Relaxed);
        let vb = b.version.load(std::sync::atomic::Ordering::Relaxed);
        // XOR + rotate to combine two u64 values into one unique identifier
        va ^ vb.rotate_left(32)
    }
}

/// A mirrored bevy query result: maps entities to their dioxus signal handles.
pub struct MirrorQuery<Q: MirrorQueryData, F: QueryFilter> {
    value: HashMap<Entity, Q::MirrorItemHandles>,
    _marker: PhantomData<fn() -> F>,
}

impl<Q: MirrorQueryData, F: QueryFilter> Clone for MirrorQuery<Q, F> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            _marker: self._marker,
        }
    }
}

impl<'a, Q, F> IntoIterator for &'a MirrorQuery<Q, F>
where
    Q: MirrorQueryData,
    F: QueryFilter,
{
    type Item = &'a Q::MirrorItemHandles;
    type IntoIter = std::collections::hash_map::Values<'a, Entity, Q::MirrorItemHandles>;

    /// Yields shared references to each mirror item.
    fn into_iter(self) -> Self::IntoIter {
        self.value.values()
    }
}

impl<'a, Q, F> IntoIterator for &'a mut MirrorQuery<Q, F>
where
    Q: MirrorQueryData,
    F: QueryFilter,
{
    type Item = &'a mut Q::MirrorItemHandles;
    type IntoIter = std::collections::hash_map::ValuesMut<'a, Entity, Q::MirrorItemHandles>;

    fn into_iter(self) -> Self::IntoIter {
        self.value.values_mut()
    }
}

impl<T: MirrorQueryData, F: QueryFilter> Default for MirrorQuery<T, F> {
    fn default() -> Self {
        Self {
            value: Default::default(),
            _marker: Default::default(),
        }
    }
}
/// A mirrored bevy query holding signals for matching components.
#[derive(Resource)]
pub struct MirrorQuerySignal<Q: MirrorQueryData, F: QueryFilter>(QueuedSignal<MirrorQuery<Q, F>>);

/// Write driver for ticking query signal updates.
#[derive(Resource)]
pub struct MirrorQueryWriteDriver<Q: DioxusQuerySync, F: QueryFilter>(
    Arc<Mutex<WriterDriver<MirrorQuery<Q, F>>>>,
);

/// Handle to a mirrored bevy query, usable from dioxus.
pub struct MirrorQuerySignalHandle<Q: MirrorQueryData, F: QueryFilter> {
    pub(crate) signal: Signal<Result<Arc<MirrorQuery<Q, F>>, QueryNoneState>>,
    /// Health status of the underlying signal.
    pub health: Signal<HealthStatus>,
    /// None until the bevy round-trip completes.
    pub writer: Signal<Option<QueuedSignal<MirrorQuery<Q, F>>>>,
    _filter: PhantomData<F>,
}

impl<Q: MirrorQueryData, F: QueryFilter> Clone for MirrorQuerySignalHandle<Q, F> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Q: MirrorQueryData, F: QueryFilter> Copy for MirrorQuerySignalHandle<Q, F> {}

/// Iterator over mirrored query results.
pub struct MirrorQueryIter<Q: MirrorQueryData, F: QueryFilter> {
    items: std::vec::IntoIter<Q::MirrorItemHandles>,
    _filter: PhantomData<F>,
}

impl<Q: MirrorQueryData, F: QueryFilter> Iterator for MirrorQueryIter<Q, F> {
    type Item = Q::MirrorItemHandles;

    fn next(&mut self) -> Option<Self::Item> {
        self.items.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.items.size_hint()
    }
}

impl<Q: MirrorQueryData + 'static, F: QueryFilter + 'static> MirrorQuerySignalHandle<Q, F> {
    /// Read into a guard of the query. Use `&*self.read()` to obtain
    /// a reference to the inner result.
    pub fn read(&self) -> SignalReadGuard<'_, Result<Arc<MirrorQuery<Q, F>>, QueryNoneState>> {
        SignalReadGuard::new(self.signal.read())
    }

    /// Iterate over all mirrored query items.
    pub fn iter(&self) -> MirrorQueryIter<Q, F> {
        let guard = self.read();
        let items = match &*guard {
            Ok(mq) => mq.value.values().cloned().collect::<Vec<_>>(),
            Err(_) => Vec::new(),
        };

        MirrorQueryIter {
            items: items.into_iter(),
            _filter: PhantomData,
        }
    }
}

/// Create or fetch a mirrored query signal.
pub fn use_bevy_query<Q: DioxusQuerySync + 'static, F: QueryFilter + 'static>()
-> MirrorQuerySignalHandle<Q, F> {
    let ctx = use_context::<CommandQueueSender>();

    // Increment tracking when the component mounts
    let r = ctx.clone();
    use_effect(move || {
        trace!("query signal mounted, sending increment command");
        let mut queue = CommandQueue::default();
        queue.push(UpdateTrackingQueries::<Q, F> {
            delta: 1,
            _phantom: || PhantomData,
        });
        let _ = r.tx.send(queue);
    });

    // Cleanup: decrement when component unmounts.
    let r = ctx.clone();
    use_drop(move || {
        trace!("query signal dropped, sending decrement command");
        let mut queue = CommandQueue::default();
        queue.push(UpdateTrackingQueries::<Q, F> {
            delta: -1,
            _phantom: || PhantomData,
        });
        let _ = r.tx.send(queue);
    });

    let value_signal = use_signal(|| Err(QueryNoneState::NotInitialized));
    let health_signal = use_signal(|| HealthStatus::Healthy);
    let mut writer: Signal<Option<QueuedSignal<MirrorQuery<Q, F>>>> = use_signal(|| None);

    let ctx2 = ctx.clone();
    use_future(move || {
        let ctx = ctx2.clone();
        async move {
            match ctx
                .send_command_async(|tx| {
                    let mut q = CommandQueue::default();
                    q.push(RequestQueryMirror::<Q, F> { response_tx: tx });
                    q
                })
                .await
            {
                Ok(signal) => {
                    signal.state.forward_to(value_signal, health_signal, Ok);
                    writer.set(Some(signal));
                }
                Err(err) => warn!("use_bevy_query: {}", err),
            }
        }
    });

    MirrorQuerySignalHandle {
        signal: value_signal,
        health: health_signal,
        writer,
        _filter: PhantomData,
    }
}
