use std::{any::{TypeId, type_name}, collections::{HashMap, HashSet}, marker::PhantomData, sync::{Arc, Mutex}, time::Duration};

use bevy_app::{PostUpdate, Update};
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
    components: Query<&mut DioxusMirror<T>>,
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

// /// holder for internal structures that need to exist in clonable structs but which should not be cloned
// pub enum NonClone<T> {
//     /// Original structure if no clone is attempted.
//     Original(T),
//     /// This structure was attempted to be cloned, but was skipped.
//     NotCloned
// }

// impl<T> Clone for NonClone<T> {
//     fn clone(&self) -> Self {
//         match self {
//             Self::Original(_arg0) => Self::NotCloned,
//             Self::NotCloned => Self::NotCloned,
//         }
//     }
// }


/// mirrored version of bevy component + infastructure for queries
#[derive(Component)]
pub struct DioxusMirror<T: DioxusComponentSync> {
    pub value: QueuedSignal<T>,

    /// driver for this component to make signal associated with it tick
    pub(crate) driver: Mutex<WriterDriver<T>>,
    /// queries referencing this component. Once this is empty, this component is deleted.
    pub tracking_queries: HashSet<TypeId>,
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

// impl<T: DioxusSync> DioxusMirrorHandle<T> {
//     pub fn new(signal: QueuedSignal<T>) -> Self {
//         Self {
//             signal
//         }
//     }
// }

// impl<T: DioxusSync> From<DioxusMirror<T>> for DioxusMirrorHandle<T> {
//     fn from(value: DioxusMirror<T>) -> Self {
//         DioxusMirrorHandle { value: value.value }
//     }
// }



pub const DEFAULT_COMPONENT_SYNC_INTERVAL: Duration = Duration::from_millis(100);

fn query_to_tracking_id<Q: DioxusQuerySync>() -> TypeId {
    TypeId::of::<Q::MirrorItem>()
}

impl<T: DioxusComponentSync> DioxusMirror<T> {
    pub fn new<Q: DioxusQuerySync>(value: T, ) -> Self {
        
        let driver = WriterDriver::new(value.clone());
        
        let mut map = HashSet::new();
        map.insert(query_to_tracking_id::<Q>());
        Self {
            value: QueuedSignal::from_parts(driver.queued_state.clone(), driver.add_tx.clone(), driver.set_tx.clone(), driver.set_value_tx.clone()),
            tracking_queries: map,
            driver: Mutex::new(driver),
        }
    }
}




/// sync mirrors to their changed components
pub fn sync_component_to_mirror<T: DioxusComponentSync>(
	components: Query<(&mut T, &DioxusMirror<T>), Changed<DioxusMirror<T>>>
) {
    for (mut value, mirror) in components {

        // let x = mirror.value.read();
        // let Ok(mirror) = mirror.value.read().inspect_err(|err| warn!("{err}")) else {
        //     continue
        // };

        let mirror = mirror.value.read();
        *value.bypass_change_detection() = mirror.as_ref().clone()
    }
}


/// cleanup unused dioxus mirrors
pub fn delete_unused_mirrors<T: DioxusComponentSync>(
    components: Query<(Entity, &DioxusMirror<T>), Changed<DioxusMirror<T>>>,
    mut commands: Commands,
) {
    for (e, changed) in components.iter() {
        if changed.tracking_queries.is_empty() {
            commands.entity(e).remove::<DioxusMirror<T>>();
        }
    }
}

/// sync components to their changed mirrors
pub fn sync_mirror_to_component<T: DioxusComponentSync>(
	components: Query<(&T, &mut DioxusMirror<T>), Changed<T>>

) {
     for (value, mut mirror) in components {
        
        // let _ = mirror.bypass_change_detection().value.set_value(value.clone()).inspect_err(|err| warn!("{err}"));
        let _ = mirror.bypass_change_detection().value.set_value(value.clone().into());
    }
}

pub fn sync_query_mirror_to_signal<T: DioxusQuerySync + 'static, F: QueryFilter + 'static>(
    components_without_signals: Query<T, (F, T::MirrorSignalsWithoutFilter)>,
    mut mirror_components: Query<T::MirrorSignalsQueryData, F>,
    mirror_signal: ResMut<MirrorQuerySignal<T, F>>,
    mut commands: Commands,
) {

    for item in components_without_signals {
        let entity = T::get_query_entity(&item);
        let mirrors = T::wrap_as_dioxus_signals(item);
        let bundle = T::get_mirror_bundle(mirrors);
        commands.entity(entity).insert_if_new(bundle);
    }

    for item in &mut mirror_components {
        T::increment_tracking_queries(item);
    }

    let new_values: HashMap<Entity, T::MirrorItemHandles> = mirror_components
        .iter_mut()
        .map(|item| {
            let entity = T::get_mirror_entity(&item);
            (entity, T::clone_dioxus_signals(item))
        })
        .collect();

    mirror_signal.0.set_value(Arc::new(MirrorQuery {
        value: new_values,
        _marker: PhantomData,
    }));

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
                add_systems_through_world(world, PostUpdate, sync_query_mirror_to_signal::<T, F>);
                let signal = QueuedSignal::from_parts(query_driver.queued_state.clone(), query_driver.add_tx.clone(), query_driver.set_tx.clone(), query_driver.set_value_tx.clone());
                
                world.insert_resource(MirrorQueryWriteDriver(Mutex::new(query_driver)));
                world.insert_resource(QuerySyncTickRate(Duration::from_millis(16)));
                world.insert_resource(MirrorQuerySignal(signal.clone()));
                signal


            },
        };

        let _ = self.response_tx.send(signal_to_send);

        // if !world.contains_resource::<MirrorQuerySignal::<T, F>>() {
        //     T::register_mirror_sync_systems::<F>(world)
        // }


        // let mirrored_query = world.get_resource_or_insert_with(|| {
        //     MirrorQuerySignal::<T, F>(QueuedSignal::new(MirrorQuery::default(), 1000))
        // });

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
    type MirrorSignalsQueryData: QueryData;

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

    /// queues commands for dioxus mirror <-> bevy value sync setup 
    fn register_mirror_sync_systems<F: QueryFilter>(world: &mut World);

    /// clones a bevy component into a pointer that a dioxus signal can read from.
    /// create new `Mirroritem` from borrowed `Query<(&mut A, ...)>::Item<'w, 's>`
    fn wrap_as_dioxus_signals<'w, 's>(item: Self::Item<'w, 's>) -> Self::MirrorItem;

    // /// clones dioxus mirror signals into dioxus mirror handles for dioxus to read without unnecessary internals.
    // fn wrap_as_dioxus_mirror_handles<'w, 's>(item: Self::MirrorItem) -> Self::MirrorItemHandles;

    /// get the entity of the mirror version of the bevy query
    /// 
    /// E.G, for:
    /// 
    /// Query<(Entity, &mut DioxusMirror<A>, &mut DioxusMirror<B>)>
    fn get_mirror_entity<'w, 's>(
        item: &<<Self as MirrorQueryData>::MirrorSignalsQueryData as QueryData>::Item<'w, 's>,
    ) -> Entity;
    /// get the entity of the original bevy query:
    /// 
    /// E.G, for:
    /// QueryMirror<(Entity, &mut A, &mut B)>
    fn get_query_entity<'w, 's>(
        item: &Self::Item<'w, 's>
    ) -> Entity;

    /// get the mirrored components as an insertable bundle
    fn get_mirror_bundle<'w, 's>(item: Self::MirrorItem) -> impl Bundle;

    /// increment the number of tracking queries per mirror item
    fn increment_tracking_queries<'w, 's>(item: <<Self as MirrorQueryData>::MirrorSignalsQueryData as QueryData>::Item<'w, 's>);

    /// clone `Query<(&mut DioxusMirror<A>, ...)>::Item<'w, 's>`(borrowed `MirrorItem`) to owned `MirrorItem`
    fn clone_dioxus_signals<'w, 's>(
        item: <<Self as MirrorQueryData>::MirrorSignalsQueryData as QueryData>::Item<'w, 's>,
    ) -> Self::MirrorItemHandles;
}

impl<A: DioxusComponentSync, B: DioxusComponentSync> MirrorQueryData for (Entity, &mut A, &mut B) {
    type MirrorItem = (Entity, DioxusMirror<A>, DioxusMirror<B>);

    type MirrorSignalsQueryData = (Entity, &'static mut DioxusMirror<A>, &'static mut DioxusMirror<B>);

    type MirrorSignalsWithoutFilter = (Without<DioxusMirror<A>>, Without<DioxusMirror<B>>);

    type MirrorItemHandles = (Entity, DioxusMirrorHandle<A>, DioxusMirrorHandle<B>);


    fn register_mirror_sync_systems<F: QueryFilter>(world: &mut World) {
        world.commands().queue(RequestComponentsMirror::<A>::default());
        world.commands().queue(RequestComponentsMirror::<B>::default());

    }

    fn wrap_as_dioxus_signals<'w, 's>(item: Self::Item<'w, 's>) -> Self::MirrorItem {
        // let item1 = item.1.clone();
        (item.0, DioxusMirror::new::<Self>(item.1.clone()), DioxusMirror::new::<Self>(item.2.clone()))
        // (item.0, DioxusMirror::new::<Self>(item.1.clone()), DioxusMirror::new::<Self>(item.2.clone()))
    }

    fn get_mirror_entity<'w, 's>(
        item: &<<Self as MirrorQueryData>::MirrorSignalsQueryData as QueryData>::Item<'w, 's>,
    ) -> Entity {
        item.0
    }

    fn get_mirror_bundle<'w, 's>(item: Self::MirrorItem) -> impl Bundle {
        (item.1, item.2)
    }

    fn clone_dioxus_signals<'w, 's>(
        item: <<Self as MirrorQueryData>::MirrorSignalsQueryData as QueryData>::Item<'w, 's>,
    ) -> Self::MirrorItemHandles {
        (item.0, item.1.handle(), item.2.handle())
    }
    
    fn get_query_entity<'w, 's>(
        item: &Self::Item<'w, 's>
    ) -> Entity {
        item.0
    }
    
    fn increment_tracking_queries<'w, 's>(mut item: <<Self as MirrorQueryData>::MirrorSignalsQueryData as QueryData>::Item<'w, 's>) {
        item.1.tracking_queries.insert(query_to_tracking_id::<Self>());
        item.2.tracking_queries.insert(query_to_tracking_id::<Self>());
    }
    
    
    // fn wrap_as_dioxus_mirror_handles<'w, 's>(item: Self::MirrorItem) -> Self::MirrorItemHandles {
    //     todo!()
    // }
    
    
    
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