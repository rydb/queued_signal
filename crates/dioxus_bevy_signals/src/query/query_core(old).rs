//! Lock‑free, ergonomic Bevy query mirrors for Dioxus — core types.

use std::any::TypeId;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use arc_swap::ArcSwap;
use bevy_app::{App, Plugin, Update};
use bevy_ecs::resource::Resource;
use bevy_ecs::{
    change_detection::{Mut, Ref},
    component::Component,
    entity::Entity,
    query::{QueryData, QueryFilter, QueryState},
    system::{Commands, Res, ResMut},
    world::World,
};
use dioxus::prelude::*;
use flume::{Receiver, Sender};

// -----------------------------------------------------------------------------
// 1. Command Infrastructure
// -----------------------------------------------------------------------------

pub enum QueryCommand {
    MutateComponent {
        entity: Entity,
        type_id: TypeId,
        component: Box<dyn std::any::Any + Send>,
    },
}

#[derive(Resource, Clone)]
pub struct QueryCommandSender {
    pub tx: Sender<QueryCommand>,
}

impl QueryCommandSender {
    fn send(&self, cmd: QueryCommand) {
        let _ = self.tx.send(cmd);
    }
}

#[derive(Resource)]
pub struct QueryCommandReceiver {
    pub rx: Receiver<QueryCommand>,
}

// -----------------------------------------------------------------------------
// 2. BevyCommand Trait (for Dioxus -> Bevy requests)
// -----------------------------------------------------------------------------

pub trait BevyCommand: Send + 'static {
    fn apply(self: Box<Self>, world: &mut World);
}

#[derive(Resource)]
pub struct QueryRequestReceiver {
    pub rx: Receiver<Box<dyn BevyCommand>>,
}

// -----------------------------------------------------------------------------
// 3. Component Mutation Registry
// -----------------------------------------------------------------------------

type ComponentInserter = Box<dyn Fn(&mut Commands, Entity, Box<dyn std::any::Any + Send>) + Send + Sync>;

#[derive(Resource, Default)]
pub struct ComponentMutationRegistry {
    inserters: HashMap<TypeId, ComponentInserter>,
}

impl ComponentMutationRegistry {
    fn register<T: Component + Clone + Send + Sync + 'static>(&mut self) {
        let type_id = TypeId::of::<T>();
        self.inserters.entry(type_id).or_insert_with(|| {
            Box::new(|commands: &mut Commands, entity: Entity, component: Box<dyn std::any::Any + Send>| {
                if let Ok(value) = component.downcast::<T>() {
                    commands.entity(entity).insert(*value);
                }
            })
        });
    }

    fn apply(&self, commands: &mut Commands, entity: Entity, type_id: TypeId, component: Box<dyn std::any::Any + Send>) {
        if let Some(inserter) = self.inserters.get(&type_id) {
            inserter(commands, entity, component);
        } else {
            bevy_log::warn!("Unregistered component type in mutation: {:?}", type_id);
        }
    }
}

// -----------------------------------------------------------------------------
// 4. Process Commands (Mutations and Requests)
// -----------------------------------------------------------------------------

fn process_query_commands(
    mut commands: Commands,
    receiver: Res<QueryCommandReceiver>,
    registry: Res<ComponentMutationRegistry>,
) {
    while let Ok(cmd) = receiver.rx.try_recv() {
        match cmd {
            QueryCommand::MutateComponent { entity, type_id, component } => {
                registry.apply(&mut commands, entity, type_id, component);
            }
        }
    }
}

fn process_query_requests(world: &mut World) {
    let rx = world.resource::<QueryRequestReceiver>().rx.clone();
    while let Ok(cmd) = rx.try_recv() {
        cmd.apply(world);
    }
}

// -----------------------------------------------------------------------------
// 5. RefWrapper<T> – Immutable wrapper (holds Arc)
// -----------------------------------------------------------------------------

pub struct RefWrapper<T> {
    arc: Arc<T>,
}

impl<T> Clone for RefWrapper<T> {
    fn clone(&self) -> Self {
        Self { arc: self.arc.clone() }
    }
}

impl<T> Deref for RefWrapper<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.arc
    }
}

// -----------------------------------------------------------------------------
// 6. MutWrapper<T> – Mutable wrapper (requires PartialEq, owns data, 'static)
// -----------------------------------------------------------------------------

pub struct MutWrapper<T: Component + Clone + PartialEq> {
    inner: T,
    original: T,
    entity: Entity,
    sender: QueryCommandSender,
}

impl<T: Component + Clone + PartialEq> Deref for MutWrapper<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<T: Component + Clone + PartialEq> DerefMut for MutWrapper<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T: Component + Clone + PartialEq> Drop for MutWrapper<T> {
    fn drop(&mut self) {
        if self.inner != self.original {
            self.sender.send(QueryCommand::MutateComponent {
                entity: self.entity,
                type_id: TypeId::of::<T>(),
                component: Box::new(self.inner.clone()),
            });
        }
    }
}

// -----------------------------------------------------------------------------
// 7. MirrorComponent – converts a query item into owned cache data
// -----------------------------------------------------------------------------

pub trait MirrorComponent {
    type Cache: Clone + Send + Sync + 'static;
    type Iter: 'static;

    fn into_cache(self) -> Self::Cache;
    fn iter_from_cache(cache: &Self::Cache, entity: Entity, sender: QueryCommandSender) -> Self::Iter;
    fn register_component(registry: &mut ComponentMutationRegistry);
}

// Immutable: for Ref<'_, T>
impl<'w, T> MirrorComponent for Ref<'w, T>
where
    T: Component + Clone + Send + Sync + 'static,
{
    type Cache = Arc<T>;
    type Iter = RefWrapper<T>;

    fn into_cache(self) -> Self::Cache {
        Arc::new((*self).clone())
    }

    fn iter_from_cache(cache: &Self::Cache, _entity: Entity, _sender: QueryCommandSender) -> Self::Iter {
        RefWrapper { arc: cache.clone() }
    }

    fn register_component(registry: &mut ComponentMutationRegistry) {
        registry.register::<T>();
    }
}

// Mutable: for Mut<'_, T>
impl<'w, T> MirrorComponent for Mut<'w, T>
where
    T: Component + Clone + PartialEq + Send + Sync + 'static,
{
    type Cache = Arc<T>;
    type Iter = MutWrapper<T>;

    fn into_cache(self) -> Self::Cache {
        Arc::new((*self).clone())
    }

    fn iter_from_cache(cache: &Self::Cache, entity: Entity, sender: QueryCommandSender) -> Self::Iter {
        let inner = (**cache).clone();
        let original = (**cache).clone();
        MutWrapper {
            inner,
            original,
            entity,
            sender,
        }
    }

    fn register_component(registry: &mut ComponentMutationRegistry) {
        registry.register::<T>();
    }
}

// -----------------------------------------------------------------------------
// 8. IntoStaticItem – converts a borrowed query item into a 'static owned version
// -----------------------------------------------------------------------------

pub trait IntoStaticItem {
    type Static: Clone + Send + Sync + 'static;
    fn into_static(self) -> Self::Static;
    fn register_components(registry: &mut ComponentMutationRegistry);
}

// Blanket impl for tuples of MirrorComponent is generated in query_macros.rs

// -----------------------------------------------------------------------------
// 9. QuerySnapshot – the cloneable handle passed to Dioxus
// -----------------------------------------------------------------------------

pub struct QuerySnapshot<Q>
where
    Q: QueryData + 'static,
    for<'w, 's> Q::Item<'w, 's>: IntoStaticItem,
{
    cache: Arc<ArcSwap<Vec<<Q::Item<'static, 'static> as IntoStaticItem>::Static>>>,
    command_sender: Option<QueryCommandSender>,
}

impl<Q> Clone for QuerySnapshot<Q>
where
    Q: QueryData + 'static,
    for<'w, 's> Q::Item<'w, 's>: IntoStaticItem,
{
    fn clone(&self) -> Self {
        Self {
            cache: Arc::clone(&self.cache),
            command_sender: self.command_sender.clone(),
        }
    }
}

impl<Q> QuerySnapshot<Q>
where
    Q: QueryData + 'static,
    for<'w, 's> Q::Item<'w, 's>: IntoStaticItem,
    <Q::Item<'static, 'static> as IntoStaticItem>::Static: QuerySnapshotItem,
{
    fn with_sender(mut self, sender: QueryCommandSender) -> Self {
        self.command_sender = Some(sender);
        self
    }

    pub fn iter(&self) -> QuerySnapshotIter<'_, Q> {
        let data = self.cache.load();
        let items = data.as_ref().clone();
        let sender = self.command_sender.clone().expect("QuerySnapshot missing command sender");
        QuerySnapshotIter {
            items,
            sender,
            index: 0,
            _marker: PhantomData,
        }
    }

    pub fn len(&self) -> usize {
        self.cache.load().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub trait QuerySnapshotItem {
    type IterItem: 'static;
    fn into_iter_item(self, entity: Entity, sender: QueryCommandSender) -> Self::IterItem;
}

pub struct QuerySnapshotIter<'a, Q>
where
    Q: QueryData + 'static,
    for<'w, 's> Q::Item<'w, 's>: IntoStaticItem,
{
    items: Vec<<Q::Item<'static, 'static> as IntoStaticItem>::Static>,
    sender: QueryCommandSender,
    index: usize,
    _marker: PhantomData<&'a ()>,
}

impl<'a, Q> Iterator for QuerySnapshotIter<'a, Q>
where
    Q: QueryData + 'static,
    for<'w, 's> Q::Item<'w, 's>: IntoStaticItem,
    <Q::Item<'static, 'static> as IntoStaticItem>::Static: QuerySnapshotItem,
{
    type Item = <<Q::Item<'static, 'static> as IntoStaticItem>::Static as QuerySnapshotItem>::IterItem;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.items.len() {
            return None;
        }
        let item = self.items[self.index].clone();
        self.index += 1;
        Some(item.into_iter_item(Entity::PLACEHOLDER, self.sender.clone()))
    }
}

// -----------------------------------------------------------------------------
// 10. Query Cache (Type‑Erased)
// -----------------------------------------------------------------------------

trait QueryCacheTrait: Send + Sync + 'static {
    fn update(&self, world: &mut World);
    fn as_snapshot_any(&self) -> Box<dyn std::any::Any + Send>;
}

struct QueryCache<Q, F>
where
    Q: QueryData + 'static,
    F: QueryFilter + Send + Sync + 'static,
    for<'w, 's> Q::Item<'w, 's>: IntoStaticItem,
{
    data: ArcSwap<Vec<<Q::Item<'static, 'static> as IntoStaticItem>::Static>>,
    _marker: PhantomData<F>,
}

impl<Q, F> QueryCache<Q, F>
where
    Q: QueryData + 'static,
    F: QueryFilter + Send + Sync + 'static,
    for<'w, 's> Q::Item<'w, 's>: IntoStaticItem,
{
    fn new(world: &mut World) -> Self {
        let data = ArcSwap::new(Arc::new(Self::build_initial_data(world)));
        Self {
            data,
            _marker: PhantomData,
        }
    }

    fn build_initial_data(world: &mut World) -> Vec<<Q::Item<'static, 'static> as IntoStaticItem>::Static> {
        let mut query_state: QueryState<Q, F> = world.query_filtered();
        let mut data = Vec::new();
        for item in query_state.iter_mut(world) {
            data.push(item.into_static());
        }
        data
    }

    fn update_incremental(&self, world: &mut World) {
        let mut query_state: QueryState<Q, F> = world.query_filtered();
        let mut new_data = Vec::new();
        for item in query_state.iter_mut(world) {
            new_data.push(item.into_static());
        }
        self.data.store(Arc::new(new_data));
    }
}

impl<Q, F> QueryCacheTrait for QueryCache<Q, F>
where
    Q: QueryData + 'static,
    F: QueryFilter + Send + Sync + 'static,
    for<'w, 's> Q::Item<'w, 's>: IntoStaticItem,
{
    fn update(&self, world: &mut World) {
        self.update_incremental(world);
    }

    fn as_snapshot_any(&self) -> Box<dyn std::any::Any + Send> {
        Box::new(QuerySnapshot::<Q> {
            cache: Arc::new(self.data.clone()),
            command_sender: None,
        })
    }
}

// -----------------------------------------------------------------------------
// 11. Registry and Sync System
// -----------------------------------------------------------------------------

#[derive(Resource, Default)]
struct QueryCacheRegistry {
    caches: HashMap<(TypeId, TypeId), Box<dyn QueryCacheTrait>>,
}

fn update_all_query_caches(world: &mut World) {
    let registry = world.resource::<QueryCacheRegistry>();
    let caches: Vec<*const dyn QueryCacheTrait> = registry.caches.values().map(|c| c.as_ref() as *const _).collect();
    for cache_ptr in caches {
        let cache = unsafe { &*cache_ptr };
        cache.update(world);
    }
}

// -----------------------------------------------------------------------------
// 12. Command to Request a Query Snapshot
// -----------------------------------------------------------------------------

pub struct RequestBevyQuery<Q, F>
where
    Q: QueryData + 'static,
    F: QueryFilter + Send + Sync + 'static,
    for<'w, 's> Q::Item<'w, 's>: IntoStaticItem,
{
    response_tx: Sender<QuerySnapshot<Q>>,
    _marker: PhantomData<F>,
}

impl<Q, F> BevyCommand for RequestBevyQuery<Q, F>
where
    Q: QueryData + 'static,
    F: QueryFilter + Send + Sync + 'static,
    for<'w, 's> Q::Item<'w, 's>: IntoStaticItem,
{
    fn apply(self: Box<Self>, world: &mut World) {
        // Register components
        <Q::Item<'static, 'static> as IntoStaticItem>::register_components(
            &mut world.resource_mut::<ComponentMutationRegistry>(),
        );

        let key = (TypeId::of::<Q>(), TypeId::of::<F>());
        let mut registry = world.resource_mut::<QueryCacheRegistry>();
        if !registry.caches.contains_key(&key) {
            let cache = Box::new(QueryCache::<Q, F>::new(world));
            registry.caches.insert(key, cache);
        }

        let snapshot_any = registry.caches.get(&key).unwrap().as_snapshot_any();
        let snapshot_box = snapshot_any.downcast::<QuerySnapshot<Q>>()
            .expect("Failed to downcast to QuerySnapshot");
        let snapshot = (*snapshot_box).clone();

        let command_sender = world.resource::<QueryCommandSender>().clone();
        let snapshot_with_sender = snapshot.with_sender(command_sender);
        let _ = self.response_tx.send(snapshot_with_sender);
    }
}

// -----------------------------------------------------------------------------
// 13. Bevy Plugin
// -----------------------------------------------------------------------------

pub struct BevyQuerySyncPlugin {
    command_rx: Receiver<QueryCommand>,
    request_rx: Receiver<Box<dyn BevyCommand>>,
}

impl BevyQuerySyncPlugin {
    pub fn new(command_rx: Receiver<QueryCommand>, request_rx: Receiver<Box<dyn BevyCommand>>) -> Self {
        Self { command_rx, request_rx }
    }
}

impl Plugin for BevyQuerySyncPlugin {
    fn build(&self, app: &mut App) {
        app.init_resource::<ComponentMutationRegistry>()
            .init_resource::<QueryCacheRegistry>()
            .insert_resource(QueryCommandReceiver { rx: self.command_rx.clone() })
            .insert_resource(QueryRequestReceiver { rx: self.request_rx.clone() })
            .add_systems(Update, (
                process_query_commands,
                process_query_requests,
                update_all_query_caches,
            ));
    }
}

// -----------------------------------------------------------------------------
// 14. Dioxus Hook and Context
// -----------------------------------------------------------------------------

#[derive(Clone)]
pub struct QueryRequestContext {
    pub tx: Sender<Box<dyn BevyCommand>>,
}

impl QueryRequestContext {
    pub fn request_query<Q, F>(&self) -> Option<QuerySnapshot<Q>>
    where
        Q: QueryData + 'static,
        F: QueryFilter + Send + Sync + 'static,
        for<'w, 's> Q::Item<'w, 's>: IntoStaticItem,
    {
        let (tx, rx) = flume::bounded(1);
        let cmd = Box::new(RequestBevyQuery::<Q, F> { response_tx: tx, _marker: PhantomData });
        self.tx.send(cmd).ok()?;
        rx.recv_timeout(std::time::Duration::from_millis(100)).ok()
    }
}

pub fn use_bevy_query<Q, F>() -> QuerySnapshot<Q>
where
    Q: QueryData + 'static,
    F: QueryFilter + Send + Sync + 'static,
    for<'w, 's> Q::Item<'w, 's>: IntoStaticItem,
{
    let ctx = use_context::<QueryRequestContext>();
    use_hook(|| ctx.request_query::<Q, F>().expect("Failed to request query snapshot"))
}