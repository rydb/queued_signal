//! Reflect-driven component and query mirroring.

use std::{
    any::{Any, TypeId},
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use bevy_ecs::archetype::{ArchetypeGeneration, ArchetypeId};
use bevy_ecs::change_detection::Tick;
use bevy_ecs::component::ComponentId;
use bevy_ecs::entity::Entity;
use bevy_ecs::prelude::*;
use bevy_ecs::query::{FilteredAccess, FilteredAccessSet, QueryFilter};
use bevy_ecs::reflect::AppTypeRegistry;
use bevy_ecs::system::{
    ParamBuilder, SystemMeta, SystemParam, SystemParamBuilder, SystemParamValidationError,
};
use bevy_ecs::world::CommandQueue;
use bevy_ecs::world::unsafe_world_cell::UnsafeWorldCell;
use bevy_ptr::{Ptr, PtrMut};
use bevy_reflect::{Reflect, ReflectFromPtr};
use dioxus_core::{Task, use_drop};
use dioxus_hooks::{use_context, use_future, use_signal};
use dioxus_signals::{ReadableExt, Signal, WritableExt};
use imbl::HashMap as ImHashMap;
use kovan::Atom;
use parking_lot::Mutex;
use queued_signal::atom_coerce::AtomCoerceHandle;
use queued_signal::atom_coerce_dyn::{AtomCoerceDyn, AtomCoerceDynHandle, Erase};
use queued_signal::state::{
    HealthStatus, QueuedSignal, QueuedStateDyn, TrackedReadGuardDyn, WriterDriver,
};
use queued_signal_tracing::{error, warn};
use tokio::sync::{oneshot, watch};

use crate::query::{
    MirrorQuery, MirrorQueryData, MirrorQuerySignal, RequestQueryMirror, UpdateTrackingQueries,
};
use crate::reflect::NameResolutionError;
use crate::schedules::{DioxusSyncPostUpdate, DioxusSyncUpdate};
use crate::{CommandQueueSender, add_systems_through_world};

use super::{ErasedMutation, clone_into_arc, enumerate_reflect_types, resolve_name};

/// Error state for a reflect query signal that has not initialized yet.
#[derive(Clone, Debug, PartialEq)]
pub enum ReflectQueryNoneState {
    /// The mirror request has not resolved yet.
    NotInitialized,
    /// One or more names could not be resolved.
    NameError(String),
}

/// Type-erased handle for mutating one component of one query item.
#[derive(Clone)]
pub struct ReflectComponentHandle {
    /// Enqueue a relative reflect mutation.
    mutate: Arc<dyn Fn(ErasedMutation) + Send + Sync>,
    /// Replace the component value.
    set_value: Arc<dyn Fn(Arc<dyn Reflect>) + Send + Sync>,
}

impl ReflectComponentHandle {
    /// Enqueue a relative reflect mutation into the active component signal.
    pub fn mutate(&self, f: ErasedMutation) {
        (self.mutate)(f);
    }

    /// Replace the active component value.
    pub fn set_value(&self, value: Arc<dyn Reflect>) {
        (self.set_value)(value);
    }
}

/// Type-erased routing for per-component query mutations.
trait QueryWriteRouter: Send + Sync {
    /// Apply a relative reflect mutation to one component.
    fn mutate(&self, entity: Entity, idx: usize, f: ErasedMutation);

    /// Replace one component value.
    fn set_value(&self, entity: Entity, idx: usize, value: Arc<dyn Reflect>);
}

/// Routes writes into the owned reflect snapshot.
struct SnapshotQueryWriteRouter {
    signal: QueuedSignal<OwnedQuerySnapshot>,
}

impl QueryWriteRouter for SnapshotQueryWriteRouter {
    fn mutate(&self, entity: Entity, idx: usize, f: ErasedMutation) {
        self.signal.mutate(move |snapshot: &mut OwnedQuerySnapshot| {
            let Some(values) = snapshot.map.get(&entity) else {
                return;
            };
            let mut new_values = values.clone();
            let Some(slot) = new_values.get_mut(idx) else {
                return;
            };
            let Some(inner) = slot else {
                return;
            };
            apply_cow(inner, f.clone());
            snapshot.map.insert(entity, new_values);
        });
    }

    fn set_value(&self, entity: Entity, idx: usize, value: Arc<dyn Reflect>) {
        self.signal
            .mutate_set(move |snapshot: &mut OwnedQuerySnapshot| {
                let Some(values) = snapshot.map.get(&entity) else {
                    return;
                };
                let mut new_values = values.clone();
                if let Some(slot) = new_values.get_mut(idx) {
                    *slot = Some(value.clone());
                }
                snapshot.map.insert(entity, new_values);
            });
    }
}

/// Routes writes into the typed per-component signals.
struct TypedQueryWriteRouter<Q: MirrorQueryData + Send + Sync + 'static, F: QueryFilter + 'static> {
    typed: QueuedSignal<MirrorQuery<Q, F>>,
}

impl<Q: MirrorQueryData + Send + Sync + 'static, F: QueryFilter + 'static> QueryWriteRouter
    for TypedQueryWriteRouter<Q, F>
{
    fn mutate(&self, entity: Entity, idx: usize, f: ErasedMutation) {
        let guard = self.typed.read();
        let reflect = &guard.as_ref().reflect;
        for handles in guard.as_ref() {
            if Q::handles_entity(handles) == entity {
                Q::mutate_component(reflect, handles, idx, f.clone());
                return;
            }
        }
    }

    fn set_value(&self, entity: Entity, idx: usize, value: Arc<dyn Reflect>) {
        let guard = self.typed.read();
        let reflect = &guard.as_ref().reflect;
        for handles in guard.as_ref() {
            if Q::handles_entity(handles) == entity {
                Q::set_component(reflect, handles, idx, value.clone());
                return;
            }
        }
    }
}

/// Zero-copy reflect read that keeps the backing allocation alive.
pub struct ReflectReadGuard {
    _keep_alive: Box<dyn Any>,
    ptr: *const dyn Reflect,
}

impl ReflectReadGuard {
    /// Wraps an owned reflected arc.
    fn from_arc(arc: Arc<dyn Reflect>) -> Self {
        let ptr: *const dyn Reflect = arc.as_ref();
        ReflectReadGuard {
            _keep_alive: Box::new(arc),
            ptr,
        }
    }

    /// Wraps a raw pointer whose backing owner is kept alive.
    fn from_raw(owner: Box<dyn Any>, ptr: *const dyn Reflect) -> Self {
        ReflectReadGuard {
            _keep_alive: owner,
            ptr,
        }
    }

    /// The reflected value this guard reads.
    pub fn as_reflect(&self) -> &dyn Reflect {
        // SAFETY: ptr points into the allocation owned by _keep_alive.
        unsafe { &*self.ptr }
    }
}

impl std::ops::Deref for ReflectReadGuard {
    type Target = dyn Reflect;

    fn deref(&self) -> &dyn Reflect {
        self.as_reflect()
    }
}

/// Type-erased view over a query's entities and component reads.
pub trait QueryView: Any + Send + Sync {
    /// Entity ids in this query.
    fn entities(&self) -> Vec<Entity>;

    /// Number of components per item.
    fn component_count(&self) -> usize;

    /// Read one component as a zero-copy reflect guard.
    fn read_component(
        &self,
        entity: Entity,
        idx: usize,
    ) -> Option<Result<ReflectReadGuard, ComponentReflectError>>;
}

/// Owned reflect snapshot backing an unelevated query mirror.
#[derive(Clone)]
pub struct OwnedQuerySnapshot {
    /// Per-entity component values with structural sharing.
    pub map: ImHashMap<Entity, Vec<Option<Arc<dyn Reflect>>>>,
    /// Number of components per item.
    pub component_count: usize,
}

impl Erase<dyn QueryView> for OwnedQuerySnapshot {
    fn erase(ptr: *const ()) -> *const dyn QueryView {
        let this: *const OwnedQuerySnapshot = ptr.cast();
        this
    }
}

impl<Q: MirrorQueryData + Send + Sync + 'static, F: QueryFilter + 'static> Erase<dyn QueryView>
    for MirrorQuery<Q, F>
{
    fn erase(ptr: *const ()) -> *const dyn QueryView {
        let this: *const MirrorQuery<Q, F> = ptr.cast();
        this
    }
}

impl QueryView for OwnedQuerySnapshot {
    fn entities(&self) -> Vec<Entity> {
        let mut out: Vec<Entity> = self.map.keys().copied().collect();
        out.sort_unstable();
        out
    }

    fn component_count(&self) -> usize {
        self.component_count
    }

    fn read_component(
        &self,
        entity: Entity,
        idx: usize,
    ) -> Option<Result<ReflectReadGuard, ComponentReflectError>> {
        let values = self.map.get(&entity)?;
        match values.get(idx)? {
            Some(arc) => Some(Ok(ReflectReadGuard::from_arc(arc.clone()))),
            None => Some(Err(ComponentReflectError::NoReflectData)),
        }
    }
}

impl<Q: MirrorQueryData + Send + Sync + 'static, F: QueryFilter + 'static> QueryView
    for MirrorQuery<Q, F>
{
    fn entities(&self) -> Vec<Entity> {
        let mut out: Vec<Entity> = self
            .into_iter()
            .map(|handles| Q::handles_entity(handles))
            .collect();
        out.sort_unstable();
        out
    }

    fn component_count(&self) -> usize {
        Q::component_type_ids().len()
    }

    fn read_component(
        &self,
        entity: Entity,
        idx: usize,
    ) -> Option<Result<ReflectReadGuard, ComponentReflectError>> {
        let handles = self
            .into_iter()
            .find(|handles| Q::handles_entity(handles) == entity)?;
        Q::read_component(&self.reflect, handles, idx)
    }
}

/// Type-erased handle to the active query view and mutation routing.
#[derive(Clone)]
pub struct QuerySignalHandle {
    /// Rebindable erased read view into the current query snapshot.
    view: QueuedStateDyn<dyn QueryView>,
    /// Number of components per query item.
    component_count: Arc<dyn Fn() -> usize + Send + Sync>,
    /// Build a per-component mutation handle for an entity and component index.
    component_handle: Arc<dyn Fn(Entity, usize) -> ReflectComponentHandle + Send + Sync>,
    /// Component names in query order.
    component_names: Vec<String>,
    /// Forward version changes into dioxus signals, returning the forward task.
    forward_to: Arc<dyn Fn(Signal<u64>, Signal<HealthStatus>) -> Task + Send + Sync>,
}

impl QuerySignalHandle {
    /// Number of components per query item.
    pub fn component_count(&self) -> usize {
        (self.component_count)()
    }

    /// Build a per-component mutation handle for an entity and component index.
    pub fn component_handle(&self, entity: Entity, idx: usize) -> ReflectComponentHandle {
        (self.component_handle)(entity, idx)
    }

    /// Component names in query order.
    pub fn component_names(&self) -> &[String] {
        &self.component_names
    }

    /// A zero-copy read view over the current query snapshot.
    pub fn read_view(&self) -> TrackedReadGuardDyn<dyn QueryView> {
        self.view.read()
    }

    /// Forward version and health into dioxus signals, returning the forward task.
    pub fn forward_to(&self, version: Signal<u64>, health: Signal<HealthStatus>) -> Task {
        (self.forward_to)(version, health)
    }
}

/// Runtime reflect operations for mirror queries with per-component reflect data.
pub trait ReflectRuntimeOps: MirrorQueryData {
    /// Number of components in this query item, excluding the entity.
    fn component_count() -> usize {
        Self::component_type_ids().len()
    }

    /// Read the idx-th component as a zero-copy reflect guard.
    fn read_component(
        reflect: &[Option<Arc<dyn Any + Send + Sync>>],
        handles: &Self::MirrorItemHandles,
        idx: usize,
    ) -> Option<Result<ReflectReadGuard, ComponentReflectError>>;

    /// Enqueue a relative reflect mutation into the idx-th component.
    fn mutate_component(
        reflect: &[Option<Arc<dyn Any + Send + Sync>>],
        handles: &Self::MirrorItemHandles,
        idx: usize,
        f: ErasedMutation,
    );

    /// Replace the idx-th component value.
    fn set_component(
        reflect: &[Option<Arc<dyn Any + Send + Sync>>],
        handles: &Self::MirrorItemHandles,
        idx: usize,
        value: Arc<dyn Reflect>,
    );
}

impl<Q: MirrorQueryData> ReflectRuntimeOps for Q {
    fn read_component(
        reflect: &[Option<Arc<dyn Any + Send + Sync>>],
        handles: &Self::MirrorItemHandles,
        idx: usize,
    ) -> Option<Result<ReflectReadGuard, ComponentReflectError>> {
        let slot = reflect.get(idx)?;
        let Some(rfp) = slot
            .as_ref()
            .and_then(|any| any.clone().downcast::<ReflectFromPtr>().ok())
        else {
            return Some(Err(ComponentReflectError::NoReflectData));
        };
        let Some(guard) = Q::read_component_erased(handles, idx) else {
            return None;
        };
        // SAFETY: guard.ptr points at the live component mirrored by rfp.
        let value = unsafe { rfp.as_reflect(Ptr::new(guard.ptr)) };
        Some(Ok(ReflectReadGuard::from_raw(guard.owner, value)))
    }

    fn mutate_component(
        reflect: &[Option<Arc<dyn Any + Send + Sync>>],
        handles: &Self::MirrorItemHandles,
        idx: usize,
        f: ErasedMutation,
    ) {
        let Some(rfp) = reflect
            .get(idx)
            .and_then(|slot| slot.as_ref())
            .and_then(|any| any.clone().downcast::<ReflectFromPtr>().ok())
        else {
            return;
        };
        Q::with_component_mut(handles, idx, Box::new(move |ptr| {
            // SAFETY: ptr points at the live component mirrored by rfp.
            let value = unsafe { rfp.as_reflect_mut(PtrMut::new(ptr)) };
            f(value);
        }));
    }

    fn set_component(
        reflect: &[Option<Arc<dyn Any + Send + Sync>>],
        handles: &Self::MirrorItemHandles,
        idx: usize,
        value: Arc<dyn Reflect>,
    ) {
        let Some(rfp) = reflect
            .get(idx)
            .and_then(|slot| slot.as_ref())
            .and_then(|any| any.clone().downcast::<ReflectFromPtr>().ok())
        else {
            return;
        };
        Q::with_component_mut(handles, idx, Box::new(move |ptr| {
            // SAFETY: ptr points at the live component mirrored by rfp.
            let target = unsafe { rfp.as_reflect_mut(PtrMut::new(ptr)) };
            if let Err(err) = target.try_apply(value.as_ref()) {
                error!("reflect apply failed: {}", err);
            }
        }));
    }
}

/// Erased per-component mirror keyed by TypeId.
pub struct ReflectComponentMirror {
    /// ComponentId of the component.
    pub component_id: ComponentId,
    /// Type data for pointer conversion.
    pub reflect_from_ptr: ReflectFromPtr,
    /// The erased signal mirror mapping entities to values.
    pub signal: QueuedSignal<HashMap<Entity, Arc<dyn Reflect>>>,
    /// Active selection count.
    pub active_count: i32,
    /// Whether a typed mirror has taken over.
    pub elevated: bool,
}

/// Registry of per-component reflect mirrors.
#[derive(Resource, Default)]
pub struct ReflectComponentRegistry {
    /// Mirrors keyed by TypeId.
    pub map: HashMap<TypeId, ReflectComponentMirror>,
}

/// Erased multi-component query mirror.
pub struct ReflectQueryMirror {
    /// ComponentIds of the query.
    pub component_ids: Vec<ComponentId>,
    /// Type data for pointer conversion per component.
    pub reflect_from_ptrs: Vec<Option<ReflectFromPtr>>,
    /// Queued signal holding the owned snapshot with ordered mutations.
    pub signal: Option<QueuedSignal<OwnedQuerySnapshot>>,
    /// Driver that publishes queued mutations into the signal read buffer.
    pub driver: Option<Arc<Mutex<WriterDriver<OwnedQuerySnapshot>>>>,
    /// Erased read view bound to the signal cell or the typed query cell.
    pub view: AtomCoerceDyn<dyn QueryView>,
    /// Active selection count.
    pub active_count: i32,
    /// Whether a typed query has taken over.
    pub elevated: bool,
    /// Last version written back to bevy.
    pub last_written_version: u64,
    /// Last world change tick observed by the read system.
    pub last_change_tick: Tick,
    /// Entities written back to bevy by this mirror's write system.
    pub recently_written: HashSet<Entity>,
    /// Rebindable router for per-component writes.
    write: Arc<Atom<Box<dyn QueryWriteRouter>>>,
    /// Handle to the active query view and mutation routing.
    pub handle: QuerySignalHandle,
    /// Sender for replacing the active handle when elevation happens.
    pub handle_tx: watch::Sender<QuerySignalHandle>,
}

/// Registry of reflect query mirrors.
#[derive(Resource, Default)]
pub struct ReflectQueryRegistry {
    /// Mirrors keyed by the sorted TypeId list of the query.
    pub map: HashMap<Vec<TypeId>, ReflectQueryMirror>,
}

/// Sorted component TypeId keys with an active typed query mirror.
#[derive(Resource, Default)]
pub struct ReflectActiveTypedQueries {
    /// Active typed query keys.
    pub keys: HashSet<Vec<TypeId>>,
}

/// Marks a typed query's component set as active and elevates any matching
/// reflect mirror so its sync systems stop writing.
pub fn register_typed_query_active(world: &mut World, mut ids: Vec<TypeId>) {
    ids.sort_unstable();
    world
        .get_resource_or_init::<ReflectActiveTypedQueries>()
        .keys
        .insert(ids.clone());
    if let Some(mirror) = world
        .resource_mut::<ReflectQueryRegistry>()
        .map
        .get_mut(&ids)
    {
        mirror.elevated = true;
        mirror.active_count = 0;
    }
}

/// Future returned by a typed query spawner.
pub type TypedSpawnFuture = Pin<Box<dyn Future<Output = Result<(), String>>>>;

/// Type-erased handle for spinning up a typed query and sharing its count.
#[derive(Clone)]
pub struct TypedQuerySpawner {
    /// Spins up the typed query, adopts the reflect mirror, and increments the count.
    pub spawn: Arc<dyn Fn(CommandQueueSender) -> TypedSpawnFuture + Send + Sync>,
    /// Decrements the shared count when the untyped query unmounts.
    pub despawn: Arc<dyn Fn(&CommandQueueSender) + Send + Sync>,
}

impl TypedQuerySpawner {
    /// Spins up the typed query and shares its count.
    pub fn spawn(&self, ctx: CommandQueueSender) -> TypedSpawnFuture {
        (self.spawn)(ctx)
    }

    /// Decrements the shared count.
    pub fn despawn(&self, ctx: &CommandQueueSender) {
        (self.despawn)(ctx)
    }
}

/// Spawners keyed by sorted component TypeIds.
#[derive(Resource, Default)]
pub struct TypedQuerySpawnerRegistry {
    /// Registered typed query spawners.
    pub spawners: HashMap<Vec<TypeId>, TypedQuerySpawner>,
}

/// Command fetching a spawner for a component TypeId key.
pub struct GetTypedQuerySpawner {
    /// Sorted component TypeIds to look up.
    pub key: Vec<TypeId>,
    /// Response channel.
    pub response_tx: oneshot::Sender<Option<TypedQuerySpawner>>,
}

impl Command for GetTypedQuerySpawner {
    type Out = ();

    fn apply(self, world: &mut World) {
        let spawner = world
            .resource::<TypedQuerySpawnerRegistry>()
            .spawners
            .get(&self.key)
            .cloned();
        let _ = self.response_tx.send(spawner);
    }
}

/// Registers a spawner for a typed query when at least one component reflects.
pub fn register_typed_query_spawner_runtime<Q, F>(world: &mut World)
where
    Q: MirrorQueryData + Send + Sync + 'static,
    F: QueryFilter + 'static,
{
    let mut key = Q::component_type_ids();
    key.sort_unstable();

    let any_reflectable = {
        let Some(registry) = world.get_resource::<AppTypeRegistry>() else {
            return;
        };
        let registry = registry.read();
        key.iter().any(|type_id| {
            registry
                .get(*type_id)
                .is_some_and(|registration| registration.data::<ReflectFromPtr>().is_some())
        })
    };
    if !any_reflectable {
        return;
    }

    let spawner = TypedQuerySpawner {
        spawn: Arc::new(|ctx: CommandQueueSender| -> TypedSpawnFuture {
            Box::pin(spawn_typed_query::<Q, F>(ctx))
        }),
        despawn: Arc::new(|ctx: &CommandQueueSender| {
            let mut q = CommandQueue::default();
            q.push(UpdateTrackingQueries::<Q, F> {
                delta: -1,
                _phantom: || std::marker::PhantomData,
            });
            let _ = ctx.tx.send(q);
        }),
    };

    world
        .get_resource_or_init::<TypedQuerySpawnerRegistry>()
        .spawners
        .insert(key, spawner);
}

async fn spawn_typed_query<Q, F>(ctx: CommandQueueSender) -> Result<(), String>
where
    Q: MirrorQueryData + Send + Sync + 'static,
    F: QueryFilter + 'static,
{
    let _: QueuedSignal<MirrorQuery<Q, F>> = ctx
        .send_command_async(|tx| {
            let mut q = CommandQueue::default();
            q.push(RequestQueryMirror::<Q, F> { response_tx: tx });
            q
        })
        .await?;

    let mut q = CommandQueue::default();
    q.push(AdoptTypedQuery::<Q, F> {
        _marker: std::marker::PhantomData,
    });
    let _ = ctx.tx.send(q);

    let mut q = CommandQueue::default();
    q.push(UpdateTrackingQueries::<Q, F> {
        delta: 1,
        _phantom: || std::marker::PhantomData,
    });
    let _ = ctx.tx.send(q);

    Ok(())
}

/// Ticks every reflect query driver so queued mutations publish.
pub fn drive_reflect_query_signals(mut registry: ResMut<ReflectQueryRegistry>) {
    for mirror in registry.map.values_mut() {
        if mirror.elevated {
            continue;
        }
        if let Some(driver) = &mirror.driver {
            let mut guard = driver.lock();
            guard.tick(Duration::ZERO);
        }
    }
}

/// State for the [`ReflectedComponents`] parameter.
pub struct ReflectedComponentsState {
    /// ComponentIds this parameter may access.
    pub component_ids: Vec<ComponentId>,
    /// Whether access is read or write.
    pub write: bool,
    /// Cached archetypes containing all requested components.
    pub matched_archetypes: Vec<ArchetypeId>,
    /// Archetype generation the cache is valid for.
    pub archetype_generation: ArchetypeGeneration,
}

impl Default for ReflectedComponentsState {
    fn default() -> Self {
        Self {
            component_ids: Vec::new(),
            write: false,
            matched_archetypes: Vec::new(),
            archetype_generation: ArchetypeGeneration::initial(),
        }
    }
}

impl ReflectedComponentsState {
    fn refresh(&mut self, world: UnsafeWorldCell) {
        self.matched_archetypes.clear();
        for archetype in world.archetypes().iter() {
            let matches = self
                .component_ids
                .iter()
                .all(|cid| archetype.components().contains(cid));
            if matches {
                self.matched_archetypes.push(archetype.id());
            }
        }
        self.archetype_generation = world.archetypes().generation();
    }
}

/// System parameter granting data-driven access to a set of components.
///
/// Access is declared in `init_access` from the parameter state, so this
/// parameter participates in normal scheduler conflict detection and never
/// requires `&World` or `&mut World` in the system body.
pub struct ReflectedComponents<'w, 's> {
    world: UnsafeWorldCell<'w>,
    component_ids: &'s [ComponentId],
    matched_archetypes: &'s [ArchetypeId],
}

impl ReflectedComponents<'_, '_> {
    /// The world cell backing this parameter.
    pub fn world(&self) -> UnsafeWorldCell<'_> {
        self.world
    }

    /// The component ids this parameter may access.
    pub fn component_ids(&self) -> &[ComponentId] {
        self.component_ids
    }

    /// The cached archetypes containing all requested components.
    pub fn matched_archetypes(&self) -> &[ArchetypeId] {
        self.matched_archetypes
    }
}

// SAFETY: init_access declares exactly the access in state, and get_param only
// reads those declared components through UnsafeWorldCell.
unsafe impl SystemParam for ReflectedComponents<'_, '_> {
    type State = ReflectedComponentsState;
    type Item<'w, 's> = ReflectedComponents<'w, 's>;

    fn init_state(_world: &mut World) -> Self::State {
        ReflectedComponentsState::default()
    }

    fn init_access(
        state: &Self::State,
        _system_meta: &mut SystemMeta,
        component_access_set: &mut FilteredAccessSet,
        _world: &mut World,
    ) {
        let mut filtered = FilteredAccess::default();
        for cid in &state.component_ids {
            if state.write {
                filtered.add_write(*cid);
            } else {
                filtered.add_read(*cid);
            }
        }
        component_access_set.add(filtered);
    }

    unsafe fn get_param<'w, 's>(
        state: &'s mut Self::State,
        _system_meta: &SystemMeta,
        world: UnsafeWorldCell<'w>,
        _change_tick: Tick,
    ) -> Result<Self::Item<'w, 's>, SystemParamValidationError> {
        let generation = world.archetypes().generation();
        if generation != state.archetype_generation {
            state.refresh(world);
        }
        Ok(ReflectedComponents {
            world,
            component_ids: &state.component_ids,
            matched_archetypes: &state.matched_archetypes,
        })
    }
}

/// Builder producing [`ReflectedComponentsState`] for a concrete component set.
pub struct ReflectedComponentsBuilder {
    /// ComponentIds to grant access to.
    pub component_ids: Vec<ComponentId>,
    /// Whether access is write.
    pub write: bool,
}

// SAFETY: build produces a state that matches the access declared in init_access.
unsafe impl<'w, 's> SystemParamBuilder<ReflectedComponents<'w, 's>> for ReflectedComponentsBuilder {
    fn build(self, world: &mut World) -> ReflectedComponentsState {
        let mut state = ReflectedComponentsState {
            component_ids: self.component_ids,
            write: self.write,
            matched_archetypes: Vec::new(),
            archetype_generation: world.archetypes().generation(),
        };
        state.refresh(world.as_unsafe_world_cell());
        state
    }
}

/// Command marking a reflect query mirror as elevated to a typed query.
pub struct ElevateReflectQuery {
    /// Type ids of the query components to elevate.
    pub type_ids: Vec<TypeId>,
}

impl Command for ElevateReflectQuery {
    type Out = ();

    fn apply(self, world: &mut World) {
        let mut type_ids = self.type_ids;
        type_ids.sort_unstable();
        if let Some(mirror) = world
            .resource_mut::<ReflectQueryRegistry>()
            .map
            .get_mut(&type_ids)
        {
            mirror.elevated = true;
            mirror.active_count = 0;
        }
    }
}

/// Elevate the reflect query mirror matching the given component type ids,
/// disabling its reflect sync so the typed query is authoritative.
pub fn elevate_query(ctx: &CommandQueueSender, type_ids: impl IntoIterator<Item = TypeId>) {
    let mut queue = CommandQueue::default();
    queue.push(ElevateReflectQuery {
        type_ids: type_ids.into_iter().collect(),
    });
    let _ = ctx.tx.send(queue);
}

/// Handles for a reflect query mirror returned to dioxus.
#[derive(Clone)]
pub struct ReflectQueryHandles {
    /// Handle to the active query view and mutation routing.
    pub handle: QuerySignalHandle,
    /// Receiver observing replacements of the active handle.
    pub handle_rx: watch::Receiver<QuerySignalHandle>,
    /// Sorted component TypeIds for this query.
    pub type_ids: Vec<TypeId>,
}

/// Command requesting a reflect mirror for a query by component names.
pub struct RequestBevyQueryDyn {
    response_tx: oneshot::Sender<Result<ReflectQueryHandles, Vec<ComponentReflectError>>>,
    names: Vec<String>,
}

impl Command for RequestBevyQueryDyn {
    type Out = ();

    fn apply(self, world: &mut World) {
        let result = register_or_get_query_dyn(world, &self.names);
        let _ = self.response_tx.send(result);
    }
}

/// Apply a mutation to a snapshot slot with copy-on-write semantics.
fn apply_cow(slot: &mut Arc<dyn Reflect>, f: ErasedMutation) {
    if let Some(inner) = Arc::get_mut(slot) {
        f(inner);
    } else if let Ok(mut cloned) = slot.reflect_clone() {
        f(&mut *cloned);
        *slot = Arc::from(cloned);
    }
}

/// Builds a component handle that dispatches through the current router.
fn component_handle_from_slot(
    write: Arc<Atom<Box<dyn QueryWriteRouter>>>,
) -> Arc<dyn Fn(Entity, usize) -> ReflectComponentHandle + Send + Sync> {
    Arc::new(move |entity: Entity, idx: usize| {
        let mutate_write = write.clone();
        let mutate = Arc::new(move |f: ErasedMutation| {
            let router = mutate_write.load();
            router.mutate(entity, idx, f);
        });

        let set_write = write.clone();
        let set_value = Arc::new(move |value: Arc<dyn Reflect>| {
            let router = set_write.load();
            router.set_value(entity, idx, value);
        });

        ReflectComponentHandle { mutate, set_value }
    })
}

/// Builds the query handle routing to the owned reflect snapshot.
fn reflect_query_handle(
    signal: QueuedSignal<OwnedQuerySnapshot>,
    view: AtomCoerceDynHandle<dyn QueryView>,
    write: Arc<Atom<Box<dyn QueryWriteRouter>>>,
    component_count: usize,
    names: Vec<String>,
) -> QuerySignalHandle {
    let count = Arc::new(move || component_count);
    let component_handle = component_handle_from_slot(write);

    let view = QueuedStateDyn {
        view,
        notify_rx: signal.state.notify_rx(),
        health_rx: signal.state.health_rx.clone(),
        registry: signal.state.registry.clone(),
    };

    let state = signal.state.clone();
    let forward_to = Arc::new(
        move |version_signal: Signal<u64>, health_signal: Signal<HealthStatus>| {
            state.forward_to(version_signal, health_signal)
        },
    );

    QuerySignalHandle {
        view,
        component_count: count,
        component_handle,
        component_names: names,
        forward_to,
    }
}

/// Builds the query handle routing to the typed per-component signals.
fn typed_query_handle<Q: MirrorQueryData + Send + Sync + 'static, F: QueryFilter + 'static>(
    view: AtomCoerceDynHandle<dyn QueryView>,
    write: Arc<Atom<Box<dyn QueryWriteRouter>>>,
    typed: QueuedSignal<MirrorQuery<Q, F>>,
    names: Vec<String>,
) -> QuerySignalHandle {
    let count = Arc::new(|| Q::component_type_ids().len());
    let component_handle = component_handle_from_slot(write);

    let view = QueuedStateDyn {
        view,
        notify_rx: typed.state.notify_rx(),
        health_rx: typed.state.health_rx.clone(),
        registry: typed.state.registry.clone(),
    };

    let state = typed.state.clone();
    let forward_to = Arc::new(
        move |version_signal: Signal<u64>, health_signal: Signal<HealthStatus>| {
            state.forward_to(version_signal, health_signal)
        },
    );

    QuerySignalHandle {
        view,
        component_count: count,
        component_handle,
        component_names: names,
        forward_to,
    }
}

/// The sorted type id list for a typed query.
fn query_type_ids<Q: MirrorQueryData>() -> Vec<TypeId> {
    let mut ids = Q::component_type_ids();
    ids.sort_unstable();
    ids
}

/// Short type path for a registered reflect type, falling back to the type id.
fn type_short_path(world: &World, type_id: TypeId) -> String {
    let registry = world.resource::<AppTypeRegistry>();
    let registry = registry.read();
    registry
        .get(type_id)
        .map(|registration| {
            registration
                .type_info()
                .type_path_table()
                .short_path()
                .to_owned()
        })
        .unwrap_or_else(|| format!("{type_id:?}"))
}

/// Error returned when a query component cannot be reflected or resolved.
#[derive(Clone, Debug, PartialEq)]
pub enum ComponentReflectError {
    /// Component id is valid but the component does not reflect.
    ValidIDNoReflect(String),
    /// Component has no reflect data at runtime.
    NoReflectData,
    /// Error resolving component name.
    NameError(NameResolutionError),
}

impl std::fmt::Display for ComponentReflectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComponentReflectError::ValidIDNoReflect(name) => write!(
                f,
                "{name} exists, but does not reflect. Did you add #[reflect(Component)] to it?"
            ),
            ComponentReflectError::NoReflectData => write!(f, "component has no reflect data"),
            ComponentReflectError::NameError(e) => write!(f, "{e:?}"),
        }
    }
}

/// Registers or returns the reflect mirror for a query by names.
pub fn register_or_get_query_dyn(
    world: &mut World,
    names: &[String],
) -> Result<ReflectQueryHandles, Vec<ComponentReflectError>> {
    let type_registry = world.resource::<AppTypeRegistry>();
    let infos = enumerate_reflect_types(&type_registry);

    let registry = type_registry.read();
    let mut type_ids = Vec::with_capacity(names.len());
    let mut component_ids = Vec::with_capacity(names.len());
    let mut reflect_from_ptrs: Vec<Option<ReflectFromPtr>> = Vec::with_capacity(names.len());
    let mut errors = vec![];

    for name in names {
        if let Ok(type_id) = resolve_name(&infos, name) {
            let Some(component_id) = world.components().get_id(type_id) else {
                errors.push(ComponentReflectError::NameError(
                    NameResolutionError::NotFound(name.to_owned()),
                ));
                continue;
            };
            let reflect_from_ptr = registry
                .get(type_id)
                .and_then(|registration| registration.data::<ReflectFromPtr>())
                .cloned();
            type_ids.push(type_id);
            component_ids.push(component_id);
            reflect_from_ptrs.push(reflect_from_ptr);
            continue;
        }

        // A component registered with the world that lacks reflection data
        // becomes a non-reflectable slot instead of a hard failure.
        let found = world.components().iter_registered().find(|info| {
            let debug_name = info.name();
            let full_str: &str = &debug_name;
            full_str == name.as_str() || debug_name.shortname().to_string() == *name
        });
        match found {
            Some(info) => {
                let Some(type_id) = info.type_id() else {
                    errors.push(ComponentReflectError::NameError(
                        NameResolutionError::NotFound(name.to_owned()),
                    ));
                    continue;
                };
                type_ids.push(type_id);
                component_ids.push(info.id());
                reflect_from_ptrs.push(None);
            }
            None => {
                errors.push(ComponentReflectError::NameError(
                    NameResolutionError::NotFound(name.to_owned()),
                ));
            }
        }
    }
    drop(registry);

    if !errors.is_empty() {
        return Err(errors);
    }
    // Canonicalize the registry key by sorting while keeping the query order
    // for the component vectors below.
    let mut key_type_ids = type_ids.clone();
    key_type_ids.sort_unstable();

    let query_registry = world.resource::<ReflectQueryRegistry>();
    if let Some(mirror) = query_registry.map.get(&key_type_ids) {
        return Ok(ReflectQueryHandles {
            handle: mirror.handle.clone(),
            handle_rx: mirror.handle_tx.subscribe(),
            type_ids: key_type_ids.clone(),
        });
    }

    let component_count = component_ids.len();
    let initial = OwnedQuerySnapshot {
        map: ImHashMap::new(),
        component_count,
    };

    let driver = WriterDriver::new(initial);
    let set_value_tx = driver.set_value_tx.clone();
    let set_tx = driver.set_tx.clone();
    let add_tx = driver.add_tx.clone();
    let queued_state = driver.queued_state.clone();
    let driver_arc = Arc::new(Mutex::new(driver));

    let signal = QueuedSignal::from_parts(
        queued_state,
        Some(driver_arc.clone()),
        add_tx,
        set_tx,
        set_value_tx,
    );

    // The read view shares the signal cell allocation.
    let view = AtomCoerceDyn::<dyn QueryView>::new(OwnedQuerySnapshot {
        map: ImHashMap::new(),
        component_count,
    });
    view.bind_handle(signal.state.cell.coerce::<dyn QueryView>());

    let write: Arc<Atom<Box<dyn QueryWriteRouter>>> =
        Arc::new(Atom::new(Box::new(SnapshotQueryWriteRouter {
            signal: signal.clone(),
        })));

    let handle = reflect_query_handle(
        signal.clone(),
        view.handle_dyn(),
        write.clone(),
        component_count,
        names.to_vec(),
    );
    let (handle_tx, handle_rx) = watch::channel(handle.clone());

    let auto_elevated = world
        .get_resource::<ReflectActiveTypedQueries>()
        .map(|active| active.keys.contains(&key_type_ids))
        .unwrap_or(false);

    let mirror = ReflectQueryMirror {
        component_ids: component_ids.clone(),
        reflect_from_ptrs: reflect_from_ptrs.clone(),
        signal: Some(signal.clone()),
        driver: Some(driver_arc.clone()),
        view,
        active_count: if auto_elevated { 0 } else { 1 },
        elevated: auto_elevated,
        last_written_version: 0,
        last_change_tick: Tick::new(0),
        recently_written: HashSet::new(),
        write,
        handle: handle.clone(),
        handle_tx,
    };

    let read_key = key_type_ids.clone();
    let write_key = key_type_ids.clone();

    let read_system = (
        ReflectedComponentsBuilder {
            component_ids: component_ids.clone(),
            write: false,
        },
        ParamBuilder::resource_mut::<ReflectQueryRegistry>(),
    )
        .build_state(world)
        .build_system(
            move |components: ReflectedComponents, mut registry: ResMut<ReflectQueryRegistry>| {
                let Some(mirror) = registry.map.get_mut(&read_key) else {
                    return;
                };
                if mirror.elevated || mirror.active_count <= 0 {
                    return;
                }
                let Some(signal) = mirror.signal.clone() else {
                    return;
                };
                // Skip reading while a signal edit has not yet reached bevy.
                let version = signal.state.peek_version();
                if version != mirror.last_written_version {
                    return;
                }
                let this_run = components.world().change_tick();
                let last_run = mirror.last_change_tick;
                mirror.last_change_tick = this_run;

                let guard = signal.read();
                let current: &OwnedQuerySnapshot = guard.as_ref();

                let mut changes: Vec<(Entity, Vec<Option<Arc<dyn Reflect>>>)> = Vec::new();
                let mut removals: Vec<Entity> = Vec::new();
                let mut matched: HashSet<Entity> = HashSet::new();
                let mut any_changed = false;

                for &archetype_id in components.matched_archetypes() {
                    let archetype = &components.world().archetypes()[archetype_id];
                    for archetype_entity in archetype.entities() {
                        let entity = archetype_entity.id();
                        matched.insert(entity);
                        let self_written = mirror.recently_written.remove(&entity);
                        let Ok(entity_cell) = components.world().get_entity(entity) else {
                            continue;
                        };
                        let mut values: Vec<Option<Arc<dyn Reflect>>> =
                            Vec::with_capacity(mirror.component_ids.len());
                        let mut complete = true;
                        let mut changed = false;
                        for (cid, reflect_from_ptr) in mirror
                            .component_ids
                            .iter()
                            .zip(mirror.reflect_from_ptrs.iter())
                        {
                            let Some(reflect_from_ptr) = reflect_from_ptr else {
                                values.push(None);
                                continue;
                            };
                            // SAFETY: cid was declared in init_access.
                            let Some(ptr) = (unsafe { entity_cell.get_by_id(*cid) }) else {
                                complete = false;
                                break;
                            };
                            // Only external changes count, so our own writes do
                            // not trigger a republish that would clobber them.
                            if !self_written
                                && (unsafe { entity_cell.get_change_ticks_by_id(*cid) })
                                    .is_some_and(|ticks| ticks.is_changed(last_run, this_run))
                            {
                                changed = true;
                            }
                            // SAFETY: ptr holds the type mirrored by reflect_from_ptr.
                            let value = unsafe { reflect_from_ptr.as_reflect(ptr) };
                            match clone_into_arc(value) {
                                Ok(arc) => values.push(Some(arc)),
                                Err(err) => {
                                    error!("reflect clone failed: {}", err);
                                    complete = false;
                                    break;
                                }
                            }
                        }
                        if complete && (changed || current.map.get(&entity).is_none()) {
                            any_changed = true;
                            changes.push((entity, values));
                        }
                    }
                }

                // Drop entities that left the query.
                for &entity in current.map.keys() {
                    if !matched.contains(&entity) {
                        removals.push(entity);
                        any_changed = true;
                    }
                }

                if any_changed {
                    signal.mutate_set(move |snapshot: &mut OwnedQuerySnapshot| {
                        for entity in &removals {
                            snapshot.map.remove(entity);
                        }
                        for (entity, values) in &changes {
                            snapshot.map.insert(*entity, values.clone());
                        }
                    });
                }
            },
        );

    let write_system = (
        ReflectedComponentsBuilder {
            component_ids: component_ids.clone(),
            write: true,
        },
        ParamBuilder::resource_mut::<ReflectQueryRegistry>(),
    )
        .build_state(world)
        .build_system(
            move |components: ReflectedComponents, mut registry: ResMut<ReflectQueryRegistry>| {
                let Some(mirror) = registry.map.get_mut(&write_key) else {
                    return;
                };
                if mirror.elevated || mirror.active_count <= 0 {
                    return;
                }
                let Some(signal) = mirror.signal.clone() else {
                    return;
                };
                let version = signal.state.peek_version();
                if version == mirror.last_written_version {
                    return;
                }
                let mut written = Vec::new();
                let mut gone_entities = Vec::new();
                let mut all_written = true;
                {
                    let guard = signal.read();
                    let snapshot: &OwnedQuerySnapshot = guard.as_ref();
                    for (&entity, values) in &snapshot.map {
                        let Ok(entity_cell) = components.world().get_entity(entity) else {
                            gone_entities.push(entity);
                            all_written = false;
                            continue;
                        };
                        let mut entity_written = true;
                        for ((cid, reflect_from_ptr), value) in mirror
                            .component_ids
                            .iter()
                            .zip(mirror.reflect_from_ptrs.iter())
                            .zip(values)
                        {
                            let (Some(reflect_from_ptr), Some(value)) = (reflect_from_ptr, value)
                            else {
                                continue;
                            };
                            // SAFETY: cid was declared in init_access.
                            let Ok(untyped) = (unsafe { entity_cell.get_mut_by_id(*cid) }) else {
                                gone_entities.push(entity);
                                entity_written = false;
                                break;
                            };
                            // SAFETY: untyped holds the type mirrored by reflect_from_ptr.
                            let mut reflect = untyped.map_unchanged(|ptr| unsafe {
                                reflect_from_ptr.as_reflect_mut(ptr)
                            });
                            if let Err(err) = reflect.try_apply(value.as_ref()) {
                                error!("reflect apply failed: {}", err);
                                entity_written = false;
                                break;
                            }
                        }
                        if entity_written {
                            written.push(entity);
                        } else {
                            all_written = false;
                        }
                    }
                }

                // Despawned or no-longer-matching entities cannot be written
                // back. Remove them so a later flush can complete.
                if !gone_entities.is_empty() {
                    signal.mutate_set(move |snapshot: &mut OwnedQuerySnapshot| {
                        for entity in &gone_entities {
                            snapshot.map.remove(entity);
                        }
                    });
                }

                mirror.recently_written.extend(written);
                if all_written {
                    mirror.last_written_version = version;
                }
            },
        );

    add_systems_through_world(
        world,
        DioxusSyncUpdate,
        read_system.after(drive_reflect_query_signals),
    );
    add_systems_through_world(world, DioxusSyncPostUpdate, write_system);

    let handle_type_ids = key_type_ids.clone();
    world
        .resource_mut::<ReflectQueryRegistry>()
        .map
        .insert(key_type_ids, mirror);

    Ok(ReflectQueryHandles {
        handle,
        handle_rx,
        type_ids: handle_type_ids,
    })
}

/// Hook called from the typed query request after the typed mirror exists.
/// Binds the erased mirror view to the typed query cell and notifies dioxus.
pub fn notify_typed_query_mirror<
    Q: MirrorQueryData + Send + Sync + 'static,
    F: QueryFilter + 'static,
>(
    world: &mut World,
) {
    register_typed_query_spawner_runtime::<Q, F>(world);

    let type_ids = query_type_ids::<Q>();

    let Some(typed) = world.get_resource::<MirrorQuerySignal<Q, F>>() else {
        return;
    };
    let typed_signal = typed.signal_cloned();
    let names = Q::component_type_ids()
        .into_iter()
        .map(|type_id| type_short_path(world, type_id))
        .collect();

    {
        let registry = world.resource::<ReflectQueryRegistry>();
        let Some(mirror) = registry.map.get(&type_ids) else {
            return;
        };
        let typed_cell = &typed_signal.state.cell;
        // Rebind the erased view so every existing read handle reveals the
        // typed query cell instead of the owned snapshot.
        let view: &AtomCoerceHandle<MirrorQuery<Q, F>, dyn QueryView> = typed_cell.coerce();
        mirror.view.bind_handle(view);
    }

    let (view, write) = {
        let registry = world.resource::<ReflectQueryRegistry>();
        let Some(mirror) = registry.map.get(&type_ids) else {
            return;
        };
        (mirror.view.handle_dyn(), mirror.write.clone())
    };

    // Route component writes through the typed signal so handles created
    // before elevation continue to work.
    write.store(Box::new(TypedQueryWriteRouter::<Q, F> {
        typed: typed_signal.clone(),
    }));

    let handle = typed_query_handle::<Q, F>(view, write, typed_signal.clone(), names);

    let mut registry = world.resource_mut::<ReflectQueryRegistry>();
    let Some(mirror) = registry.map.get_mut(&type_ids) else {
        return;
    };
    mirror.elevated = true;
    mirror.active_count = 0;
    mirror.signal.take();
    mirror.driver.take();
    mirror.handle = handle.clone();
    let _ = mirror.handle_tx.send_replace(handle);
}

/// Command adopting a reflect query mirror into a concrete typed query.
pub struct AdoptTypedQuery<
    Q: MirrorQueryData + Send + Sync + 'static,
    F: QueryFilter + 'static,
> {
    /// Marker for the typed query data and filter.
    pub _marker: std::marker::PhantomData<fn() -> (Q, F)>,
}

impl<Q: MirrorQueryData + Send + Sync + 'static, F: QueryFilter + 'static> Command
    for AdoptTypedQuery<Q, F>
{
    type Out = ();

    fn apply(self, world: &mut World) {
        notify_typed_query_mirror::<Q, F>(world);
    }
}

/// Adopt a reflect query mirror into a concrete typed query.
pub fn adopt_typed_query<
    Q: MirrorQueryData + Send + Sync + 'static,
    F: QueryFilter + 'static,
>(
    ctx: &CommandQueueSender,
) {
    let mut queue = CommandQueue::default();
    queue.push(AdoptTypedQuery::<Q, F> {
        _marker: std::marker::PhantomData,
    });
    let _ = ctx.tx.send(queue);
}

/// Zero-copy read view over a reflect query snapshot.
pub struct ReflectQueryViewGuard {
    view: TrackedReadGuardDyn<dyn QueryView>,
}

impl ReflectQueryViewGuard {
    /// Number of components per query item.
    pub fn component_count(&self) -> usize {
        self.view.as_ref().component_count()
    }

    /// Entity ids in this query.
    pub fn entities(&self) -> Vec<Entity> {
        self.view.as_ref().entities()
    }

    /// Read one component as a zero-copy reflect guard.
    pub fn read_component(
        &self,
        entity: Entity,
        idx: usize,
    ) -> Option<Result<ReflectReadGuard, ComponentReflectError>> {
        self.view.as_ref().read_component(entity, idx)
    }
}

/// Dioxus handle for a reflect query mirror.
#[derive(Clone, Copy)]
pub struct ReflectQuerySignal {
    version: Signal<u64>,
    health: Signal<HealthStatus>,
    handle: Signal<Option<QuerySignalHandle>>,
    error: Signal<Option<ReflectQueryNoneState>>,
}

impl ReflectQuerySignal {
    /// Read the current query snapshot as a zero-copy view.
    pub fn read(&self) -> Result<ReflectQueryViewGuard, ReflectQueryNoneState> {
        let _ = self.version.read();
        if let Some(err) = self.error.read().clone() {
            return Err(err);
        }
        let handle = self.handle.read();
        match handle.as_ref() {
            Some(h) => Ok(ReflectQueryViewGuard {
                view: h.read_view(),
            }),
            None => Err(ReflectQueryNoneState::NotInitialized),
        }
    }

    /// Current health status of the underlying signal.
    pub fn health(&self) -> HealthStatus {
        *self.health.read()
    }

    /// Number of components per query item.
    pub fn component_count(&self) -> usize {
        self.handle
            .read()
            .as_ref()
            .map(|h| h.component_count())
            .unwrap_or(0)
    }

    /// Iterate over entities with named per-component mutation handles.
    pub fn iter(&self) -> Vec<(Entity, Vec<(String, ReflectComponentHandle)>)> {
        let _ = self.version.read();
        let handle = match self.handle.read().as_ref() {
            Some(h) => h.clone(),
            None => return Vec::new(),
        };
        let count = handle.component_count();
        let names = handle.component_names().to_vec();
        let view = handle.read_view();
        let mut entities = view.as_ref().entities();
        entities.sort_unstable();
        entities
            .into_iter()
            .map(|entity| {
                let component_handles = (0..count)
                    .map(|idx| {
                        let name = names
                            .get(idx)
                            .cloned()
                            .unwrap_or_else(|| format!("component {idx}"));
                        (name, handle.component_handle(entity, idx))
                    })
                    .collect();
                (entity, component_handles)
            })
            .collect()
    }
}

/// Create or fetch a reflect mirror for a query by component names.
pub fn use_bevy_query_dyn<const N: usize>(names: [&str; N]) -> ReflectQuerySignal {
    let names: Vec<String> = names.iter().map(|s| s.to_string()).collect();
    let ctx = use_context::<CommandQueueSender>();

    let version_signal = use_signal(|| 0u64);
    let health_signal = use_signal(|| HealthStatus::Healthy);
    let mut handle_signal: Signal<Option<QuerySignalHandle>> = use_signal(|| None);
    let mut error_signal: Signal<Option<ReflectQueryNoneState>> = use_signal(|| None);
    let mut spawner_signal: Signal<Option<TypedQuerySpawner>> = use_signal(|| None);

    let ctx_clone = ctx.clone();
    use_future(move || {
        let ctx = ctx_clone.clone();
        let names = names.clone();
        async move {
            let handles = match ctx
                .send_command_async(|tx| {
                    let mut q = CommandQueue::default();
                    q.push(RequestBevyQueryDyn {
                        response_tx: tx,
                        names: names.clone(),
                    });
                    q
                })
                .await
            {
                Ok(Ok(handles)) => handles,
                Ok(Err(errors)) => {
                    let message = errors
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join("; ");
                    error_signal.set(Some(ReflectQueryNoneState::NameError(message)));
                    return;
                }
                Err(e) => {
                    error_signal.set(Some(ReflectQueryNoneState::NameError(e)));
                    return;
                }
            };

            // Spin up and share the typed query when a spawner is registered.
            let spawner = ctx
                .send_command_async(|tx| {
                    let mut q = CommandQueue::default();
                    q.push(GetTypedQuerySpawner {
                        key: handles.type_ids.clone(),
                        response_tx: tx,
                    });
                    q
                })
                .await
                .ok()
                .flatten();
            if let Some(spawner) = spawner {
                if let Err(err) = spawner.spawn(ctx.clone()).await {
                    warn!("typed query spawn failed: {}", err);
                } else {
                    spawner_signal.set(Some(spawner.clone()));
                }
            }

            // Bind the initial handle and start forwarding version updates.
            handle_signal.set(Some(handles.handle.clone()));
            let mut task = handles.handle.forward_to(version_signal, health_signal);

            // Re-bind mutation routing whenever the handle is replaced.
            let mut rx = handles.handle_rx;
            while rx.changed().await.is_ok() {
                let handle = rx.borrow().clone();
                task.cancel();
                handle_signal.set(Some(handle.clone()));
                task = handle.forward_to(version_signal, health_signal);
            }
        }
    });

    let ctx_drop = ctx.clone();
    use_drop(move || {
        if let Some(spawner) = spawner_signal.read().as_ref() {
            spawner.despawn(&ctx_drop);
        }
    });

    ReflectQuerySignal {
        version: version_signal,
        health: health_signal,
        handle: handle_signal,
        error: error_signal,
    }
}
