use std::{any::{TypeId, type_name}, collections::{HashMap, HashSet}, marker::PhantomData, sync::Arc, time::Duration};

use bevy_app::Update;
use bevy_ecs::{component::Mutable, prelude::*, query::{QueryData, QueryFilter, QueryItem}};
use bevy_log::warn;
use queued_signal::signal::QueuedSignal;
use trait_set::trait_set;

use crate::add_systems_through_world;
pub mod query;


trait_set! {
    /// Component that is syncable with dioxus
    /// 
    /// TODO: support immutable components as well
    pub trait DioxusSync = Component<Mutability = Mutable> + Clone
}

#[derive(Component, Clone)]
pub struct DioxusMirror<T: DioxusSync> {
    pub value: QueuedSignal<T>,

    /// queries referencing this component. Once this is empty, this component is deleted.
    pub tracking_queries: HashSet<TypeId>,
}

pub const DEFAULT_COMPONENT_SYNC_INTERVAL: Duration = Duration::from_millis(100);

impl<T: DioxusSync> DioxusMirror<T> {
    pub fn new<Q: MirrorQueryData>(value: T) -> Self {
        let mut map = HashSet::new();
        map.insert(TypeId::of::<Q::MirrorItem>());
        Self {
            value: QueuedSignal::new(value, DEFAULT_COMPONENT_SYNC_INTERVAL),
            tracking_queries: map
        }
    }
}


/// sync mirrors to their changed components
pub fn sync_component_to_mirror<T: DioxusSync>(
	components: Query<(&mut T, &DioxusMirror<T>), Changed<DioxusMirror<T>>>
) {
    for (mut value, mirror) in components {
        let Ok(mirror) = mirror.value.read().inspect_err(|err| warn!("{err}")) else {
            continue
        };
        *value.bypass_change_detection() = mirror.as_ref().clone()
    }
}


/// cleanup unused dioxus mirrors
pub fn delete_unused_mirrors<T: DioxusSync>(
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
pub fn sync_mirror_to_component<T: DioxusSync>(
	components: Query<(&T, &mut DioxusMirror<T>), Changed<T>>

) {
     for (value, mut mirror) in components {
        let _ = mirror.bypass_change_detection().value.set_value(value.clone()).inspect_err(|err| warn!("{err}"));
    }
}

pub fn sync_query_mirror_to_signal<T: MirrorQueryData + 'static, F: QueryFilter + 'static>(
    components_without_signals: Query<T, (F, T::MirrorSignalsWithoutFilter)>,
    mut mirror_components: Query<T::MirrorSignalsQueryData, F>,
    mirror_signal: ResMut<MirrorQuerySignal<T, F>>,
    mut commands: Commands,
) {
    let results = mirror_components.iter_mut();

    if let Some(upper_bound) = results.size_hint().1 && upper_bound > T::MAX_TRACKED_COUNT {
        warn!("There are more entries in {} then allowed by MAX_TRACKED_COUNT, {}. Killing query for performance.", type_name::<T>(), T::MAX_TRACKED_COUNT);
        todo!()
    }

    // Insert DioxusMirror clones on synced bevy components which don't have them already
    for component in components_without_signals {
        let entity = T::get_query_entity(&component);
        let mirrors = T::wrap_as_dioxus_signals(component);
        let bundle = T::get_mirror_bundle(mirrors);
        commands.entity(entity).insert(bundle);
    }


    let current_values = results.map(|n| (T::get_mirror_entity(&n), T::clone_dioxus_signals(n))).collect::<HashMap<_, _>>();

    //TODO: instead of clearing entire hashmap, only merge new items/delete non-matching entities
    mirror_signal.into_inner().value = current_values;
}


#[derive(Resource, Default)]
pub struct MirroredComponents(HashSet<TypeId>);

pub struct RequestComponentsMirror<T: DioxusSync> {
    _marker: PhantomData<T>
}

impl<T: DioxusSync> Default for RequestComponentsMirror<T> {
    fn default() -> Self {
        Self { _marker: Default::default() }
    }
}

impl<T: DioxusSync> Command for RequestComponentsMirror<T> {
    fn apply(self, world: &mut World) -> () {
        let mirrored_components = world.get_resource_or_insert_with(|| {
            let mut new_map = MirroredComponents::default();
            new_map.0.insert(TypeId::of::<T>());
            new_map
        });
        if !mirrored_components.0.contains(&TypeId::of::<T>()) {
            add_systems_through_world(world, Update, sync_component_to_mirror::<T>);
            add_systems_through_world(world, Update, sync_mirror_to_component::<T>);
            add_systems_through_world(world, Update, delete_unused_mirrors::<T>);
        }
    }
}
pub struct RequestQueryMirror<T: MirrorQueryData + 'static, F: QueryFilter + 'static> {
    _marker_a: PhantomData<T>,
    _marker_b: PhantomData<F>
}

impl<T: MirrorQueryData + Send + Sync + 'static, F: QueryFilter + Send + Sync + 'static> Command for RequestQueryMirror<T, F> {
    fn apply(self, world: &mut World) -> () {
        if !world.contains_resource::<MirrorQuerySignal::<T, F>>() {
            T::register_mirror_sync_systems::<F>(world)
        }
        let mirrored_query = world.get_resource_or_insert_with(|| {
            MirrorQuerySignal::<T, F>::default()
        });

    }
}

/// query that corresponds to the mirorred signals that correspond to the underlying bevy value,
/// ```
/// let signal = MirrorQuery<(Entity, &mut A, &mut B)> -> Query<(Entity, &mut DioxusMirror<A>, &mut DioxusMirror<B>)> -> HashMap<Entity, (Entity, DioxusMirror<A>, DioxusMirror<B>)> -> QueuedQuerySignal
/// ```
pub trait MirrorQueryData: QueryData {
    type MirrorItem: Clone + Send + Sync + 'static;

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

    /// get the entity of the mirror version of the bevy query
    /// 
    /// E.G, for:
    /// 
    /// Query<(&mut DioxusMirror<A>, &mut DioxusMirror<B>)>
    fn get_mirror_entity<'w, 's>(
        item: &<<Self as MirrorQueryData>::MirrorSignalsQueryData as QueryData>::Item<'w, 's>,
    ) -> Entity;
    /// get the entity of the original bevy query:
    /// 
    /// E.G, for:
    /// QueryMirror<(&mut A, &mut B)>
    fn get_query_entity<'w, 's>(
        item: &Self::Item<'w, 's>
    ) -> Entity;

    /// get the mirrored components as an insertable bundle
    fn get_mirror_bundle<'w, 's>(item: Self::MirrorItem) -> impl Bundle;

    /// clone `Query<(&mut DioxusMirror<A>, ...)>::Item<'w, 's>`(borrowed `MirrorItem`) to owned `MirrorItem`
    fn clone_dioxus_signals<'w, 's>(
        item: <<Self as MirrorQueryData>::MirrorSignalsQueryData as QueryData>::Item<'w, 's>,
    ) -> Self::MirrorItem;
}

impl<A: DioxusSync, B: DioxusSync> MirrorQueryData for (Entity, &mut A, &mut B) {
    type MirrorItem = (Entity, DioxusMirror<A>, DioxusMirror<B>);

    type MirrorSignalsQueryData = (Entity, &'static mut DioxusMirror<A>, &'static mut DioxusMirror<B>);

    type MirrorSignalsWithoutFilter = (Without<DioxusMirror<A>>, Without<DioxusMirror<B>>);

    fn register_mirror_sync_systems<F: QueryFilter>(world: &mut World) {
        world.commands().queue(RequestComponentsMirror::<A>::default());
        world.commands().queue(RequestComponentsMirror::<B>::default());

    }

    fn wrap_as_dioxus_signals<'w, 's>(item: Self::Item<'w, 's>) -> Self::MirrorItem {
        (item.0, DioxusMirror::new::<Self>(item.1.clone()), DioxusMirror::new::<Self>(item.2.clone()))
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
    ) -> Self::MirrorItem {
        (item.0, item.1.clone(), item.2.clone())
    }
    
    fn get_query_entity<'w, 's>(
        item: &Self::Item<'w, 's>
    ) -> Entity {
        item.0
    }
    
    
}

// pub enum MirrorQueryState {
//     Uninitialized
//     Capped
// }

#[derive(Resource)]
pub struct MirrorQuerySignal<T: MirrorQueryData, F: QueryFilter> {
    value: HashMap<Entity, T::MirrorItem>,
    _marker: PhantomData<fn() ->F>
}

impl<T: MirrorQueryData, F: QueryFilter> Default for MirrorQuerySignal<T, F> {
    fn default() -> Self {
        Self { value: Default::default(), _marker: Default::default() }
    }
}