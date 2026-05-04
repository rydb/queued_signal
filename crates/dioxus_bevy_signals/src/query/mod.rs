use std::{any::{TypeId, type_name, type_name_of_val}, collections::{HashMap, HashSet}, fmt::Debug, marker::PhantomData, sync::{Arc, Mutex, atomic::{AtomicU64, Ordering}}, time::Duration};

use bevy_app::{Last, PostUpdate, Update};
use bevy_ecs::{component::Mutable, prelude::*, query::{QueryData, QueryFilter}, world::CommandQueue};
use bevy_log::warn;
use dioxus_core::use_hook;
use dioxus_hooks::use_context;
use dioxus_signals::{ReadableExt, Signal};
use flume::Sender;
use queued_signal::signal::{HealthStatus, QueuedSignal, WriterDriver};
use trait_set::trait_set;

use crate::{CommandQueueSender, add_systems_through_world};


trait_set! {
    /// Component that is syncable with dioxus
    /// 
    /// TODO: support immutable components as well
    pub trait DioxusComponentSync = Component<Mutability = Mutable> + Clone + 'static;
    pub trait DioxusQuerySync = MirrorQueryData + Send + Sync;
}

// #[derive(Resource)]
// pub struct ComponentWriteDrivers<T: Clone + Send + Sync + 'static>(pub Mutex<WriterDriver<T>>);

/// Minimum time to pass til queued mutations from QueuedSignal are published. The time to publish may be longer then this duration, but no shorter then this duration.
#[derive(Resource)]
pub struct ComponentSyncTickRate(Duration);

fn drive_component_signals<T: DioxusComponentSync>(
    components: Query<&DioxusMirror<T>>,
    tick_rate: Res<ComponentSyncTickRate>,
) {
    for component in components {
        if let Ok(mut guard) = component.driver.lock().inspect_err(|err| warn!("UNABLE TO AQUIRE LOCK FOR {}, : {}", type_name::<T>(), err)) {
            guard.tick(tick_rate.0);
        }
    }
}

#[derive(Resource)]
pub struct QuerySyncTickRate(Duration);

fn drive_query_signal<Q: DioxusQuerySync + 'static, F: QueryFilter + 'static>(
    driver: ResMut<MirrorQueryWriteDriver<Q, F>>,
    tick_rate: Res<QuerySyncTickRate>,
) {
    if let Ok(mut guard) = driver.0.lock().inspect_err(|err| warn!("UNABLE TO AQUIRE LOCK FOR {}, : {}", type_name::<Q>().to_owned() + type_name::<F>(), err)) {
        guard.tick(tick_rate.0);
    }
}

/// mirrored version of bevy component + infastructure for queries
#[derive(Component)]
pub struct DioxusMirror<T: DioxusComponentSync> {
    pub value: QueuedSignal<T>,

    /// driver for this component to make signal associated with it tick
    pub(crate) driver: Mutex<WriterDriver<T>>,

    pub version: Arc<AtomicU64>,
}

#[derive(Component)]
pub struct DioxusTrackingQueries<T: DioxusComponentSync> {
    pub tracking_queries: HashSet<TypeId>,
    _component: PhantomData<T>,
}

impl<T: DioxusComponentSync + Debug> Debug for DioxusMirror<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DioxusMirror").field("value", &self.value).field("driver", &self.driver).finish()
    }
}

impl<T: DioxusComponentSync> DioxusMirror<T>{
    pub fn handle(&self) -> DioxusMirrorHandle<T> {
        DioxusMirrorHandle { value: self.value.clone()}
    }
}

/// dioxus handle to bevy side DioxusMirror
#[derive(Clone)]
pub struct DioxusMirrorHandle<T: DioxusComponentSync> {
    pub value: QueuedSignal<T>,
}


pub const DEFAULT_COMPONENT_SYNC_INTERVAL: Duration = Duration::from_millis(100);

fn query_to_tracking_id<Q: DioxusQuerySync>() -> TypeId {
    TypeId::of::<Q::MirrorItem>()
}

impl<T: DioxusComponentSync> DioxusMirror<T> {
    /// initialize DioxusMirror and decompose it into its other dependent components (for impl Bundle)
    pub fn init_and_decompose<Q: DioxusQuerySync>(value: T) -> (Self, DioxusTrackingQueries<T>) {
        
        let mut driver = WriterDriver::new(value.clone());
        let version = Arc::new(AtomicU64::new(0));
        driver.set_publish_counter(version.clone());
        
        let mut map = HashSet::new();
        map.insert(query_to_tracking_id::<Q>());
        (
            Self {
            value: QueuedSignal::from_parts(driver.queued_state.clone(), driver.add_tx.clone(), driver.set_tx.clone(), driver.set_value_tx.clone()),
            driver: Mutex::new(driver),
            version
        },
        DioxusTrackingQueries {
            tracking_queries: map,
            _component: PhantomData
        }
        )
    }
}




// /// sync mirrors to their changed components
// pub fn sync_component_to_mirror<T: DioxusComponentSync>(
// 	components: Query<(&mut T, &DioxusMirror<T>), Changed<DioxusMirror<T>>>
// ) {

//     for (mut value, mirror) in components {
//         println!("change recieved for mirror, updating T");
//         let mirror = mirror.value.read();
//         *value.bypass_change_detection() = mirror.as_ref().clone()
//     }
// }

/// Copies the current value of a Dioxus‑side signal back into the Bevy component,
/// but only when the signal has been updated since the last sync for that entity.
/// Uses an internal version counter to avoid work every frame.
pub fn sync_component_to_mirror<T: DioxusComponentSync>(
    components: Query<(Entity, &mut DioxusMirror<T>, &mut T)>,
    mut last_versions: Local<HashMap<Entity, u64>>,
) {
    for (entity, mut mirror, mut value) in components {
        let current_version = mirror.version.load(Ordering::Acquire);
        let last = last_versions.get(&entity).copied().unwrap_or(0);
        if current_version != last {
            // The signal has been updated since we last checked.
            *value.bypass_change_detection() = mirror.value.read().as_ref().clone();

            //setting mirror value changed here as well since current verzion != last means mirror needs to be marked as changed as well
            mirror.set_changed();
            last_versions.insert(entity, current_version);
        }
    }
}

pub fn delete_unused_mirrors<T: DioxusComponentSync>(
    components: Query<(Entity, &DioxusTrackingQueries<T>), Changed<DioxusTrackingQueries<T>>>,
    mut commands: Commands,
) {
    for (e, changed) in components.iter() {
        if changed.tracking_queries.is_empty() {
            println!("emptying tracking queries...");
            commands.entity(e).remove::<DioxusMirror<T>>();
        }
    }
}

/// sync components to their changed mirrors
pub fn sync_mirror_to_component<T: DioxusComponentSync>(
	mut components: Query<(&T, &DioxusMirror<T>), Changed<T>>

) {
     for (value,  mirror) in &mut components {
        let _ = mirror.value.set_value(value.clone().into());

    }
}
///increment any new tracking queries on relevant DioxusMirrors
pub fn increment_tracking_queries<T: DioxusQuerySync + 'static, F: QueryFilter + 'static>(
    mut mirror_components: Query<T::TrackingQueriesQuerydataMut, F>,
) {
    for item in &mut mirror_components {
        // println!("incrementing tracking queries..");
        T::increment_tracking_queries(item);
    }
}

#[derive(Resource)]
pub struct QueryMirrorInitailized<Q: QueryData, F: QueryFilter> {
    initialized: bool,
    _querydata: fn() -> PhantomData<Q>,
    _filter: fn() -> PhantomData<F>,
}

pub fn sync_query_mirror_to_signal<T: DioxusQuerySync + 'static, F: QueryFilter + 'static>(
    components_without_signals: Query<T, (F, T::MirrorSignalsWithoutFilter)>,
    mirror_components: Query<T::MirrorSignalsQueryDataImMut, F>,
    changed_mirror_components: Query<T::MirrorSignalsQueryDataImMut, T::MirrorSignalsChangedFilter>,
    mirror_signal: ResMut<MirrorQuerySignal<T, F>>,
    mut commands: Commands,
    mut init_status: ResMut<QueryMirrorInitailized<T, F>>,
) {


    if components_without_signals.count() <= 0 {
        // if the map has not been initailized yet, force a full sync in order to get it up to data
        if init_status.initialized == false {
            let current_map = mirror_components.iter()
            .map(|item| {
                (T::get_mirror_entity(&item), T::clone_dioxus_signals(&item))
            }).collect::<HashMap<_, _>>();
            mirror_signal.0.set_value(
                Arc::new(
                MirrorQuery {
                    value: current_map,
                    _marker: PhantomData::default()
            }));
            init_status.initialized = true;
            println!("finished initializing component mirror to: {}", mirror_components.iter().count());
            return;
        }
    }

    for item in components_without_signals {
        println!("componenet without signal found");
        let entity = T::get_query_entity(&item);
        let bundle = T::get_mirror_bundle(item);
        commands.entity(entity).insert_if_new(bundle);
    }

    let mut add_map = HashMap::new();
    let mut remove_list = Vec::new();


    let last_map = &mirror_signal.0.read().value;

    for (key, _value) in last_map.iter() {
        if mirror_components.contains(*key) == false {
            remove_list.push(key)
        }
    }

    // TODO: so apparently size_hint().1 returns the upper bound of the query ignoring non-archetypal query filters e.g; Changed<T>.
    // So in the meantime, .count() must be used instead in order to not count unchanged components as changed...
    if changed_mirror_components.iter().count() >= 1 {
        for value in &changed_mirror_components {
            let entity = T::get_mirror_entity(&value);

            add_map.insert(entity, value);
        }
    }
    
    if remove_list.len() > 0 || add_map.len() > 0 {
        let remove_list = remove_list.iter().map(|n| **n).collect::<Vec<_>>();

        let add_map = add_map.iter()
        .map(|item| {
            (item.0.clone(), T::clone_dioxus_signals(item.1))
        }).collect::<HashMap<_, _>>();

        mirror_signal.0.mutate_set(move |n| {
            let map = &mut n.value;

            for item in remove_list.clone() {
                map.remove(&item);
            }
            map.extend(add_map.clone());
        });
    }
}


#[derive(Resource, Default)]
pub struct MirroredComponents(HashSet<TypeId>);

pub struct RequestComponentsMirror<T: DioxusComponentSync> {
    _marker: PhantomData<T>
}

impl<T: DioxusComponentSync> Default for RequestComponentsMirror<T> {
    fn default() -> Self {
        Self { _marker: Default::default() }
    }
}

impl<T: DioxusComponentSync> Command for RequestComponentsMirror<T> {
    fn apply(self, world: &mut World) -> () {
        let mirrored_components = world.get_resource_or_insert_with(|| {
            let mut new_map = MirroredComponents::default();
            new_map.0.insert(TypeId::of::<T>());
            new_map
        });
        if !mirrored_components.0.contains(&TypeId::of::<T>()) {      
            add_systems_through_world(world, Update, drive_component_signals::<T>);
            add_systems_through_world(world, PostUpdate, sync_component_to_mirror::<T>);
            add_systems_through_world(world, PostUpdate, sync_mirror_to_component::<T>);
            add_systems_through_world(world, PostUpdate, delete_unused_mirrors::<T>);

            world.insert_resource(ComponentSyncTickRate(Duration::from_millis(16)));
        }
    }
}
pub struct RequestQueryMirror<T: DioxusQuerySync, F: QueryFilter + 'static> {
    response_tx: Sender<QueuedSignal<MirrorQuery<T, F>>>,
}

impl<T: DioxusQuerySync + 'static, F: QueryFilter> Command for RequestQueryMirror<T, F> {
    fn apply(self, world: &mut World) -> () {
        let signal_to_send: QueuedSignal<MirrorQuery<T, F>> = match world.get_resource::<MirrorQuerySignal<T, F>>() {
            Some(signal) => signal.0.clone(),
            None => {
                T::register_mirror_sync_systems::<F>(world);

                let query_driver = WriterDriver::new(MirrorQuery::default());
                add_systems_through_world(world, Update, drive_query_signal::<T, F>);
                add_systems_through_world(world, PostUpdate, increment_tracking_queries::<T, F>);
                add_systems_through_world(world, Last, sync_query_mirror_to_signal::<T, F>);
                let signal = QueuedSignal::from_parts(query_driver.queued_state.clone(), query_driver.add_tx.clone(), query_driver.set_tx.clone(), query_driver.set_value_tx.clone());
                
                world.insert_resource(MirrorQueryWriteDriver(Mutex::new(query_driver)));
                world.insert_resource(QuerySyncTickRate(Duration::ZERO));
                world.insert_resource(MirrorQuerySignal(signal.clone()));
                world.insert_resource(QueryMirrorInitailized::<T, F> {
                    initialized: false,
                    _querydata: || PhantomData::default(),
                    _filter: || PhantomData::default(),
                });
                signal


            },
        };

        let _ = self.response_tx.send(signal_to_send);
    }
}

/// query that corresponds to the mirorred signals that correspond to the underlying bevy value,
/// ```
/// let signal = MirrorQuery<(Entity, &mut A, &mut B)> -> Query<(Entity, &mut DioxusMirror<A>, &mut DioxusMirror<B>)> -> HashMap<Entity, (Entity, DioxusMirror<A>, DioxusMirror<B>)> -> QueuedQuerySignal
/// ```
pub trait MirrorQueryData: QueryData {
    
    /// query item results for mirror items of query;
    /// 
    /// E.G;
    type MirrorItem: Send + Sync + 'static;

    /// query item handles to the results for mirror items of query,
    /// 
    /// E.G; if `MirrorItem` is 
    /// ```rust
    /// (Entity, DioxusMirror<A>, DioxusMirror<A>)
    /// ```
    /// this would be
    /// ```rust
    /// (Entity, DioxusMirrorHandle<A>, DioxusMirrorHandle<B>)
    /// ```
    type MirrorItemHandles: Clone + Send + Sync + 'static;

    /// query as its DioxusMirror<T> encapsulated variant.
    /// 
    /// E.G; if MirrorQueryData is 
    /// 
    /// ```rust
    /// Query<(Entity, &mut A, &mut B)>
    /// ```
    /// 
    /// This would be:
    /// 
    /// ```rust
    /// Query<(Entity, &mut DioxusMirror<A>, &mut DioxusMirror<B>)>
    /// ```
    /// 
    // type MirrorSignalsQueryDataMut: QueryData;

    /// immutable version of [`MirrorSignalsQueryDataMut`]
    type MirrorSignalsQueryDataImMut: QueryData;

    /// Query filter version of query data signals to check for components which DON'T, have DioxusMirrors attached which should.
    type MirrorSignalsWithoutFilter: QueryFilter;

    /// the max number of entries this query is allowed to track. 
    /// 
    /// if there exists more matches then this number, then query is cleared and no longer tracked.
    /// 
    /// This exists in order to prevent un-optimized queries from marking every single world component to sync.
    /// 
    /// E.G: 
    /// ```rust
    /// QueryMirror<Entity, &mut A> // -> 2 million entities with A -> 2 million A(s) marked for sync on update.
    /// ```
    /// 
    /// 
    /// vs
    /// 
    /// ```rust
    /// QueryMirror<Entity, &mut A, With<Marker>> //-> 20 entries with A + marker -> 20 A(s) marked for sync on update.
    /// ```
    /// 
    /// entities in a query iter are un-ordered, so its not possible to make a "shallow iterator" that could go from x..y range of entries
    /// 
    /// What can be garunteed though, is that there are x or less number of entries. So this exists as a stop-gap for optimization.
    /// 
    /// TODO: Implement a better solution
    const MAX_TRACKED_COUNT: usize = 2000;

    /// filter for entities with changed MirrorComponents.
    /// 
    /// used for query sync to only sync when work has been done
    type MirrorSignalsChangedFilter: QueryFilter;

    /// queries that are tracking the components in [`MirrorItem`], e.g; 
    /// 
    /// if query is:
    /// 
    /// ```rust
    /// Query<(Entity, &mut A, &mut B)>
    /// ```
    ///
    /// 
    /// this will be
    /// ```rust
    /// Query<(Entity, &mut DioxusTrackingQueries<A>, &mut DioxusTrackingQueries<B>)>
    /// ```
    type TrackingQueriesQuerydataMut: QueryData;

    /// queues commands for dioxus mirror <-> bevy value sync setup 
    fn register_mirror_sync_systems<F: QueryFilter>(world: &mut World);

    /// get the entity of the mirror version of the bevy query
    /// 
    /// E.G, for:
    /// 
    /// Query<(Entity, &mut DioxusMirror<A>, &mut DioxusMirror<B>)>
    fn get_mirror_entity<'w, 's>(
        // item: &<<Self as MirrorQueryData>::MirrorSignalsQueryDataImMut as QueryData>::Item<'w, 's>,
        item: &<<Self::MirrorSignalsQueryDataImMut as QueryData>::ReadOnly as QueryData>::Item<'w, 's>
    ) -> Entity;
    /// get the entity of the original bevy query:
    /// 
    /// E.G, for:
    /// QueryMirror<(Entity, &mut A, &mut B)>
    fn get_query_entity<'w, 's>(
        item: &Self::Item<'w, 's>
    ) -> Entity;

    /// get the mirrored components as an insertable bundle
    fn get_mirror_bundle<'w, 's>(item: Self::Item<'w, 's>) -> impl Bundle;

    // /// increment the number of tracking queries per mirror item
    // fn increment_tracking_queries<'w, 's>(item: <<Self as MirrorQueryData>::MirrorSignalsQueryDataMut as QueryData>::Item<'w, 's>);

    /// increment the number of tracking queries per mirror item
    fn increment_tracking_queries<'w, 's>(item: <Self::TrackingQueriesQuerydataMut as QueryData>::Item<'w, 's>);

    /// clone `Query<(&mut DioxusMirror<A>, ...)>::Item<'w, 's>`(borrowed `MirrorItem`) to owned `MirrorItem`
    fn clone_dioxus_signals<'w, 's>(
        item: &<<Self::MirrorSignalsQueryDataImMut as QueryData>::ReadOnly as QueryData>::Item<'w, 's>,
    ) -> Self::MirrorItemHandles;
}

impl<A: DioxusComponentSync, B: DioxusComponentSync> MirrorQueryData for (Entity, &mut A, &mut B) {
    type MirrorItem = (Entity, DioxusMirror<A>, DioxusMirror<B>);

    // type MirrorSignalsQueryDataMut = (Entity, &'static mut DioxusMirror<A>, &'static mut DioxusMirror<B>);

    type MirrorSignalsQueryDataImMut = (Entity, &'static DioxusMirror<A>, &'static DioxusMirror<B>);

    type MirrorSignalsWithoutFilter = (Without<DioxusMirror<A>>, Without<DioxusMirror<B>>);

    type MirrorItemHandles = (Entity, DioxusMirrorHandle<A>, DioxusMirrorHandle<B>);

    type MirrorSignalsChangedFilter = Or<(Changed<DioxusMirror<A>>, Changed<DioxusMirror<B>>)>;

    type TrackingQueriesQuerydataMut = (Entity, &'static mut DioxusTrackingQueries<A>, &'static mut DioxusTrackingQueries<B>) ;

    fn register_mirror_sync_systems<F: QueryFilter>(world: &mut World) {
        world.commands().queue(RequestComponentsMirror::<A>::default());
        world.commands().queue(RequestComponentsMirror::<B>::default());

    }

    fn get_mirror_entity<'w, 's>(
        item: &<<Self::MirrorSignalsQueryDataImMut as QueryData>::ReadOnly as QueryData>::Item<'w, 's>,
    ) -> Entity {
        item.0
    }

    fn get_mirror_bundle<'w, 's>(item: Self::Item<'w, 's>) -> impl Bundle {
        (DioxusMirror::init_and_decompose::<Self>(item.1.clone()), DioxusMirror::init_and_decompose::<Self>(item.2.clone()))
    }

    fn clone_dioxus_signals<'w, 's>(
        item: &<<Self::MirrorSignalsQueryDataImMut as QueryData>::ReadOnly as QueryData>::Item<'w, 's>,
    ) -> Self::MirrorItemHandles {
        (item.0, item.1.handle(), item.2.handle())
    }
    
    fn get_query_entity<'w, 's>(
        item: &Self::Item<'w, 's>
    ) -> Entity {
        item.0
    }
    

    fn increment_tracking_queries<'w, 's>(mut item: <Self::TrackingQueriesQuerydataMut as QueryData>::Item<'w, 's>) {
        item.1.bypass_change_detection().tracking_queries.insert(query_to_tracking_id::<Self>());
        item.2.bypass_change_detection().tracking_queries.insert(query_to_tracking_id::<Self>());
    }
    
}
// // registry of the latest dioxus clones of the matching query
// #[derive(Resource)]
pub struct MirrorQuery<Q: MirrorQueryData, F: QueryFilter> {
    value: HashMap<Entity, Q::MirrorItemHandles>,
    _marker: PhantomData<fn() ->F>
}

impl<Q: MirrorQueryData, F: QueryFilter> Clone for MirrorQuery<Q, F> {
    fn clone(&self) -> Self {
        Self { value: self.value.clone(), _marker: self._marker.clone() }
    }
}

impl<'a, Q, F> IntoIterator for &'a MirrorQuery<Q, F>
where
    Q: MirrorQueryData,
    F: QueryFilter,
{
    type Item = &'a Q::MirrorItemHandles;
    type IntoIter = std::collections::hash_map::Values<'a, Entity, Q::MirrorItemHandles>;

    /// Yields shared references to each `MirrorItem` tuple without cloning.
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
        Self { value: Default::default(), _marker: Default::default() }
    }
}
/// A mirror version of bevy query, that holds `QueuedSignal<T>` of matching components
#[derive(Resource)]
pub struct MirrorQuerySignal<Q: MirrorQueryData, F: QueryFilter>(QueuedSignal<MirrorQuery<Q, F>>);

/// write driver to make query updates tick for QuerySignal
#[derive(Resource)]
pub struct MirrorQueryWriteDriver<Q: DioxusQuerySync, F: QueryFilter>(Mutex<WriterDriver<MirrorQuery<Q, F>>>);

/// Handle to mirror version fo bevy query, that can be used from dioxus.
pub struct MirrorQuerySignalHandle<Q: MirrorQueryData, F: QueryFilter> {
    signal: Signal<Option<Arc<MirrorQuery<Q, F>>>>,
    pub health: Signal<HealthStatus>,
    pub writer: QueuedSignal<MirrorQuery<Q, F>>,
    _filter: PhantomData<F>,
}

impl<Q: MirrorQueryData, F: QueryFilter> Clone for MirrorQuerySignalHandle<Q, F> {
    fn clone(&self) -> Self {
        Self { signal: self.signal.clone(), health: self.health.clone(), writer: self.writer.clone(), _filter: self._filter.clone() }
    }
}

pub struct MirrorQueryIter<Q: MirrorQueryData, F: QueryFilter> {
    items: std::vec::IntoIter<Q::MirrorItemHandles>,
    _filter: PhantomData<F>
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
    pub fn iter(&self) -> MirrorQueryIter<Q, F> {
        let items = self.signal
            .read()
            .as_ref()
            .map(|arc| arc.value.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default();       // empty Vec if not initialized

        MirrorQueryIter {
            items: items.into_iter(),
            _filter: PhantomData,
        }
    }
}

/// Create or fetch a [`MirrorQuery`] signal of mirrored components that can be edited to reflect changes to and read from the bevy world.
pub fn use_bevy_query<Q: DioxusQuerySync + 'static, F: QueryFilter + 'static>() -> MirrorQuerySignalHandle<Q, F> {
    let ctx = use_context::<CommandQueueSender>();
    
    let signal = use_hook(|| {
        println!("sending signal");
        ctx.send_command(|tx| {
            let mut command_queue = CommandQueue::default();
            let command = RequestQueryMirror::<Q, F> { response_tx: tx };
            command_queue.push(command);
            command_queue
        }).inspect_err(|err| warn!("{}", err))
    }).unwrap();

    let (value_signal, health) = signal.use_hook();

    MirrorQuerySignalHandle { 
        signal: value_signal, 
        health: health, 
        writer: signal, 
        _filter: PhantomData::default() 
    }
}