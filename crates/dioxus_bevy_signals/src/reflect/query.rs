//! Reflect-driven component and query mirroring.

use std::{
    any::TypeId,
    collections::{HashMap, HashSet},
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
use bevy_reflect::{Reflect, ReflectFromPtr};
use dioxus_hooks::{use_context, use_future, use_signal};
use dioxus_signals::{ReadableExt, Signal, WritableExt};
use parking_lot::Mutex;
use queued_signal::state::{HealthStatus, QueuedSignal, SignalReadGuard, WriterDriver};
use queued_signal_tracing::error;
use tokio::sync::{oneshot, watch};

use crate::query::{
    DioxusComponentSync, MirrorQuery, MirrorQueryData, MirrorQuerySignal,
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

/// Type-erased handle to the active mutation routing for a query mirror.
#[derive(Clone)]
pub struct QueryMutationHandle {
    /// Number of components per query item.
    component_count: Arc<dyn Fn() -> usize + Send + Sync>,
    /// Build a per-component mutation handle for an entity and component index.
    component_handle: Arc<dyn Fn(Entity, usize) -> ReflectComponentHandle + Send + Sync>,
    /// Component names in query order.
    component_names: Vec<String>,
}

impl QueryMutationHandle {
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
}

/// Type-erased dispatch over a typed query's per-component handles.
pub trait ReflectMirrorQueryData: MirrorQueryData {
    /// Type ids of the queried components, in tuple order.
    fn type_ids() -> Vec<TypeId>;

    /// Number of components in this query item, excluding the entity.
    fn component_count() -> usize;

    /// The entity of a mirror item handle tuple.
    fn handles_entity(handles: &Self::MirrorItemHandles) -> Entity;

    /// Read the idx-th component value as a reflected Arc.
    fn read_component(handles: &Self::MirrorItemHandles, idx: usize) -> Option<Arc<dyn Reflect>>;

    /// Enqueue a relative reflect mutation into the idx-th component.
    fn mutate_component(handles: &Self::MirrorItemHandles, idx: usize, f: ErasedMutation);

    /// Replace the idx-th component value.
    fn set_component(handles: &Self::MirrorItemHandles, idx: usize, value: Arc<dyn Reflect>);
}

impl<A: DioxusComponentSync + Reflect> ReflectMirrorQueryData for (Entity, &mut A) {
    fn type_ids() -> Vec<TypeId> {
        vec![TypeId::of::<A>()]
    }

    fn component_count() -> usize {
        1
    }

    fn handles_entity(handles: &Self::MirrorItemHandles) -> Entity {
        handles.0
    }

    fn read_component(handles: &Self::MirrorItemHandles, idx: usize) -> Option<Arc<dyn Reflect>> {
        if idx == 0 {
            let guard = handles.1.read();
            clone_into_arc(guard.as_ref()).ok()
        } else {
            None
        }
    }

    fn mutate_component(handles: &Self::MirrorItemHandles, idx: usize, f: ErasedMutation) {
        if idx == 0 {
            handles
                .1
                .mutate(move |value: &mut A| f(value.as_reflect_mut()));
        }
    }

    fn set_component(handles: &Self::MirrorItemHandles, idx: usize, value: Arc<dyn Reflect>) {
        if idx == 0 {
            handles.1.mutate_set(move |v: &mut A| {
                if let Err(err) = v.as_reflect_mut().try_apply(value.as_ref()) {
                    error!("reflect apply failed: {}", err);
                }
            });
        }
    }
}

impl<A: DioxusComponentSync + Reflect, B: DioxusComponentSync + Reflect> ReflectMirrorQueryData
    for (Entity, &mut A, &mut B)
{
    fn type_ids() -> Vec<TypeId> {
        vec![TypeId::of::<A>(), TypeId::of::<B>()]
    }

    fn component_count() -> usize {
        2
    }

    fn handles_entity(handles: &Self::MirrorItemHandles) -> Entity {
        handles.0
    }

    fn read_component(handles: &Self::MirrorItemHandles, idx: usize) -> Option<Arc<dyn Reflect>> {
        match idx {
            0 => {
                let guard = handles.1.read();
                clone_into_arc(guard.as_ref()).ok()
            }
            1 => {
                let guard = handles.2.read();
                clone_into_arc(guard.as_ref()).ok()
            }
            _ => None,
        }
    }

    fn mutate_component(handles: &Self::MirrorItemHandles, idx: usize, f: ErasedMutation) {
        match idx {
            0 => handles
                .1
                .mutate(move |value: &mut A| f(value.as_reflect_mut())),
            1 => handles
                .2
                .mutate(move |value: &mut B| f(value.as_reflect_mut())),
            _ => {}
        }
    }

    fn set_component(handles: &Self::MirrorItemHandles, idx: usize, value: Arc<dyn Reflect>) {
        match idx {
            0 => handles.1.mutate_set(move |v: &mut A| {
                if let Err(err) = v.as_reflect_mut().try_apply(value.as_ref()) {
                    error!("reflect apply failed: {}", err);
                }
            }),
            1 => handles.2.mutate_set(move |v: &mut B| {
                if let Err(err) = v.as_reflect_mut().try_apply(value.as_ref()) {
                    error!("reflect apply failed: {}", err);
                }
            }),
            _ => {}
        }
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
    pub reflect_from_ptrs: Vec<ReflectFromPtr>,
    /// The erased signal mirror mapping entities to per-component values.
    pub signal: QueuedSignal<HashMap<Entity, Vec<Arc<dyn Reflect>>>>,
    /// Active selection count.
    pub active_count: i32,
    /// Whether a typed query has taken over.
    pub elevated: bool,
    /// Last signal version written back to bevy.
    pub last_written_version: u64,
    /// Last world change tick observed by the read system.
    pub last_change_tick: Tick,
    /// Entities written back to bevy by this mirror's write system.
    pub recently_written: HashSet<Entity>,
    /// Driver that publishes queued mutations into the signal read buffer.
    pub driver: Arc<Mutex<WriterDriver<HashMap<Entity, Vec<Arc<dyn Reflect>>>>>>,
    /// Handle to the active mutation routing.
    pub handle: QueryMutationHandle,
    /// Sender for replacing the active handle when elevation happens.
    pub handle_tx: watch::Sender<QueryMutationHandle>,
}

/// Registry of reflect query mirrors.
#[derive(Resource, Default)]
pub struct ReflectQueryRegistry {
    /// Mirrors keyed by the sorted TypeId list of the query.
    pub map: HashMap<Vec<TypeId>, ReflectQueryMirror>,
}

/// Ticks every reflect query driver so queued mutations publish.
pub fn drive_reflect_query_signals(mut registry: ResMut<ReflectQueryRegistry>) {
    for mirror in registry.map.values_mut() {
        let mut guard = mirror.driver.lock();
        guard.tick(Duration::ZERO);
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
    /// The snapshot signal, forwarded into the dioxus value signal.
    pub signal: QueuedSignal<HashMap<Entity, Vec<Arc<dyn Reflect>>>>,
    /// Handle to the active mutation routing.
    pub handle: QueryMutationHandle,
    /// Receiver observing replacements of the active handle.
    pub handle_rx: watch::Receiver<QueryMutationHandle>,
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

/// Builds the mutation handle routing to the reflect snapshot.
fn reflect_query_mutation_handle(
    signal: QueuedSignal<HashMap<Entity, Vec<Arc<dyn Reflect>>>>,
    component_count: usize,
    names: Vec<String>,
) -> QueryMutationHandle {
    let count = Arc::new(move || component_count);

    let s = signal.clone();
    let component_handle = Arc::new(move |entity: Entity, idx: usize| {
        let s_mutate = s.clone();
        let mutate = Arc::new(move |f: ErasedMutation| {
            s_mutate.mutate(move |map: &mut HashMap<Entity, Vec<Arc<dyn Reflect>>>| {
                let Some(values) = map.get_mut(&entity) else {
                    return;
                };
                let Some(slot) = values.get_mut(idx) else {
                    return;
                };
                apply_cow(slot, f.clone());
            });
        });

        let s_set = s.clone();
        let set_value = Arc::new(move |value: Arc<dyn Reflect>| {
            s_set.mutate_set(move |map: &mut HashMap<Entity, Vec<Arc<dyn Reflect>>>| {
                let Some(values) = map.get_mut(&entity) else {
                    return;
                };
                if let Some(slot) = values.get_mut(idx) {
                    *slot = value.clone();
                }
            });
        });

        ReflectComponentHandle { mutate, set_value }
    });

    QueryMutationHandle {
        component_count: count,
        component_handle,
        component_names: names,
    }
}

/// Builds the mutation handle routing to the typed per-component signals.
fn typed_query_mutation_handle<Q: ReflectMirrorQueryData + 'static, F: QueryFilter + 'static>(
    typed: QueuedSignal<MirrorQuery<Q, F>>,
    names: Vec<String>,
) -> QueryMutationHandle {
    let count = Arc::new(|| Q::component_count());

    let t = typed.clone();
    let component_handle = Arc::new(move |entity: Entity, idx: usize| {
        let t_mutate = t.clone();
        let mutate = Arc::new(move |f: ErasedMutation| {
            let guard = t_mutate.read();
            for handles in guard.as_ref() {
                if Q::handles_entity(handles) == entity {
                    Q::mutate_component(handles, idx, f.clone());
                    return;
                }
            }
        });

        let t_set = t.clone();
        let set_value = Arc::new(move |value: Arc<dyn Reflect>| {
            let guard = t_set.read();
            for handles in guard.as_ref() {
                if Q::handles_entity(handles) == entity {
                    Q::set_component(handles, idx, value.clone());
                    return;
                }
            }
        });

        ReflectComponentHandle { mutate, set_value }
    });

    QueryMutationHandle {
        component_count: count,
        component_handle,
        component_names: names,
    }
}

/// The sorted type id list for a typed query.
fn query_type_ids<Q: ReflectMirrorQueryData>() -> Vec<TypeId> {
    let mut ids = Q::type_ids();
    ids.sort_unstable();
    ids
}

/// Short type path for a registered reflect type, falling back to the type id.
fn type_short_path(world: &World, type_id: TypeId) -> String {
    let registry = world.resource::<AppTypeRegistry>();
    let registry = registry.read();
    registry
        .get(type_id)
        .map(|registration| registration.type_info().type_path_table().short_path().to_owned())
        .unwrap_or_else(|| format!("{type_id:?}"))
}

/// Error returned when a query component cannot be reflected or resolved.
#[derive(Clone, Debug, PartialEq)]
pub enum ComponentReflectError {
    /// Component id is valid but the component does not reflect.
    ValidIDNoReflect(String),
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

    let mut type_ids = Vec::with_capacity(names.len());
    let mut errors = vec![];
    for name in names {
        let type_id = match resolve_name(&infos, name) {
            Ok(type_id) => type_id,
            Err(e) => {
                // A component registered with the world that lacks reflection
                // data should produce a targeted hint instead of a bare name error.
                let exists = world.components().iter_registered().any(|info| {
                    let debug_name = info.name();
                    let short = debug_name.shortname();
                    let short_str: &str = &short.0;
                    let full_str: &str = &debug_name;
                    full_str == name.as_str() || short_str == name.as_str()
                });
                if exists {
                    errors.push(ComponentReflectError::ValidIDNoReflect(name.to_owned()));
                    continue;
                } else {
                    errors.push(ComponentReflectError::NameError(e));
                    continue;
                }
            }
        };
        type_ids.push(type_id);
    }
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
            signal: mirror.signal.clone(),
            handle: mirror.handle.clone(),
            handle_rx: mirror.handle_tx.subscribe(),
        });
    }

    let registry = type_registry.read();
    let mut component_ids = Vec::with_capacity(type_ids.len());
    let mut reflect_from_ptrs = Vec::with_capacity(type_ids.len());
    for type_id in &type_ids {
        let registration = registry.get(*type_id).unwrap();
        let reflect_from_ptr = registration
            .data::<ReflectFromPtr>().unwrap().clone();
        let component_id = world
            .components()
            .get_id(*type_id)
            .unwrap();
        component_ids.push(component_id);
        reflect_from_ptrs.push(reflect_from_ptr);
    }
    drop(registry);

    let component_count = component_ids.len();
    let initial: HashMap<Entity, Vec<Arc<dyn Reflect>>> = HashMap::new();

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

    let handle = reflect_query_mutation_handle(signal.clone(), component_count, names.to_vec());
    let (handle_tx, handle_rx) = watch::channel(handle.clone());

    let mirror = ReflectQueryMirror {
        component_ids: component_ids.clone(),
        reflect_from_ptrs: reflect_from_ptrs.clone(),
        signal: signal.clone(),
        active_count: 1,
        elevated: false,
        last_written_version: 0,
        last_change_tick: Tick::new(0),
        recently_written: HashSet::new(),
        driver: driver_arc.clone(),
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
                // Skip reading while a signal edit has not yet reached bevy.
                let version = mirror.signal.state.peek_version();
                if version != mirror.last_written_version {
                    return;
                }
                let this_run = components.world().change_tick();
                let last_run = mirror.last_change_tick;
                mirror.last_change_tick = this_run;
                let mut any_changed = false;
                let mut out: HashMap<Entity, Vec<Arc<dyn Reflect>>> = HashMap::new();
                for &archetype_id in components.matched_archetypes() {
                    let archetype = &components.world().archetypes()[archetype_id];
                    for archetype_entity in archetype.entities() {
                        let entity = archetype_entity.id();
                        let self_written = mirror.recently_written.remove(&entity);
                        let Ok(entity_cell) = components.world().get_entity(entity) else {
                            continue;
                        };
                        let mut values = Vec::with_capacity(mirror.component_ids.len());
                        let mut complete = true;
                        for (cid, reflect_from_ptr) in mirror
                            .component_ids
                            .iter()
                            .zip(mirror.reflect_from_ptrs.iter())
                        {
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
                                any_changed = true;
                            }
                            // SAFETY: ptr holds the type mirrored by reflect_from_ptr.
                            let value = unsafe { reflect_from_ptr.as_reflect(ptr) };
                            match clone_into_arc(value) {
                                Ok(arc) => values.push(arc),
                                Err(err) => {
                                    error!("reflect clone failed: {}", err);
                                    complete = false;
                                    break;
                                }
                            }
                        }
                        if complete {
                            out.insert(entity, values);
                        }
                    }
                }
                if any_changed {
                    mirror.signal.set_value(out);
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
                let version = mirror.signal.state.peek_version();
                if version == mirror.last_written_version {
                    return;
                }
                let mut written = Vec::new();
                {
                    let guard = mirror.signal.read();
                    let map: &HashMap<Entity, Vec<Arc<dyn Reflect>>> = guard.as_ref();
                    for (&entity, values) in map {
                        let Ok(entity_cell) = components.world().get_entity(entity) else {
                            continue;
                        };
                        for ((cid, reflect_from_ptr), value) in mirror
                            .component_ids
                            .iter()
                            .zip(mirror.reflect_from_ptrs.iter())
                            .zip(values)
                        {
                            // SAFETY: cid was declared in init_access.
                            let Ok(untyped) = (unsafe { entity_cell.get_mut_by_id(*cid) }) else {
                                continue;
                            };
                            // SAFETY: untyped holds the type mirrored by reflect_from_ptr.
                            let mut reflect = untyped
                                .map_unchanged(|ptr| unsafe { reflect_from_ptr.as_reflect_mut(ptr) });
                            // Marking the component changed lets the typed query
                            // observe this write. The read system skips these
                            // entities to avoid echoing the write back.
                            reflect.apply(value.as_ref());
                        }
                        written.push(entity);
                    }
                }
                mirror.recently_written.extend(written);
                mirror.last_written_version = version;
            },
        );

    add_systems_through_world(
        world,
        DioxusSyncUpdate,
        read_system.after(drive_reflect_query_signals),
    );
    add_systems_through_world(world, DioxusSyncPostUpdate, write_system);

    world
        .resource_mut::<ReflectQueryRegistry>()
        .map
        .insert(key_type_ids, mirror);

    Ok(ReflectQueryHandles {
        signal,
        handle,
        handle_rx,
    })
}

/// Composes the typed query snapshot into the reflect snapshot when elevated.
pub fn bridge_typed_query<Q: ReflectMirrorQueryData + 'static, F: QueryFilter + 'static>(
    typed: Res<MirrorQuerySignal<Q, F>>,
    mut erased: ResMut<ReflectQueryRegistry>,
) {
    let Some(mirror) = erased.map.get_mut(&query_type_ids::<Q>()) else {
        return;
    };
    if !mirror.elevated {
        return;
    }
    let guard = typed.signal().read();
    let mut out: HashMap<Entity, Vec<Arc<dyn Reflect>>> = HashMap::new();
    for handles in guard.as_ref() {
        let mut values = Vec::with_capacity(Q::component_count());
        for idx in 0..Q::component_count() {
            if let Some(value) = Q::read_component(handles, idx) {
                values.push(value);
            }
        }
        out.insert(Q::handles_entity(handles), values);
    }
    mirror.signal.set_value(out);
}

/// Hook called from the typed query request after the typed mirror exists.
/// Replaces the erased mirror handle with the typed handle and notifies dioxus.
pub fn notify_typed_query_mirror<Q: ReflectMirrorQueryData + 'static, F: QueryFilter + 'static>(
    world: &mut World,
) {
    let type_ids = query_type_ids::<Q>();

    let Some(typed) = world.get_resource::<MirrorQuerySignal<Q, F>>() else {
        return;
    };
    let names = Q::type_ids()
        .into_iter()
        .map(|type_id| type_short_path(world, type_id))
        .collect();
    let handle = typed_query_mutation_handle::<Q, F>(typed.signal_cloned(), names);

    let mut registry = world.resource_mut::<ReflectQueryRegistry>();
    let Some(mirror) = registry.map.get_mut(&type_ids) else {
        return;
    };
    mirror.elevated = true;
    mirror.active_count = 0;
    mirror.handle = handle.clone();
    let _ = mirror.handle_tx.send_replace(handle);

    add_systems_through_world(world, DioxusSyncUpdate, bridge_typed_query::<Q, F>);
}

/// Command adopting a reflect query mirror into a concrete typed query.
pub struct AdoptTypedQuery<Q: ReflectMirrorQueryData + 'static, F: QueryFilter + 'static> {
    /// Marker for the typed query data and filter.
    pub _marker: std::marker::PhantomData<fn() -> (Q, F)>,
}

impl<Q: ReflectMirrorQueryData + 'static, F: QueryFilter + 'static> Command
    for AdoptTypedQuery<Q, F>
{
    type Out = ();

    fn apply(self, world: &mut World) {
        notify_typed_query_mirror::<Q, F>(world);
    }
}

/// Adopt a reflect query mirror into a concrete typed query.
pub fn adopt_typed_query<Q: ReflectMirrorQueryData + 'static, F: QueryFilter + 'static>(
    ctx: &CommandQueueSender,
) {
    let mut queue = CommandQueue::default();
    queue.push(AdoptTypedQuery::<Q, F> {
        _marker: std::marker::PhantomData,
    });
    let _ = ctx.tx.send(queue);
}

/// Dioxus handle for a reflect query mirror.
#[derive(Clone, Copy)]
pub struct ReflectQuerySignal {
    value: Signal<Result<HashMap<Entity, Vec<Arc<dyn Reflect>>>, ReflectQueryNoneState>>,
    health: Signal<HealthStatus>,
    handle: Signal<Option<QueryMutationHandle>>,
}

impl ReflectQuerySignal {
    /// Read the current query snapshot.
    pub fn read(
        &self,
    ) -> SignalReadGuard<'_, Result<HashMap<Entity, Vec<Arc<dyn Reflect>>>, ReflectQueryNoneState>>
    {
        SignalReadGuard::new(self.value.read())
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
        let Some(handle) = self.handle.read().clone() else {
            return Vec::new();
        };
        let count = handle.component_count();
        let names = handle.component_names().to_vec();
        let guard = self.value.read();
        let Ok(map) = &*guard else {
            return Vec::new();
        };
        let mut entities: Vec<Entity> = map.keys().copied().collect();
        entities.sort_unstable();
        entities
            .into_iter()
            .map(|entity| {
                let handles = (0..count)
                    .map(|idx| {
                        let name = names
                            .get(idx)
                            .cloned()
                            .unwrap_or_else(|| format!("component {idx}"));
                        (name, handle.component_handle(entity, idx))
                    })
                    .collect();
                (entity, handles)
            })
            .collect()
    }
}

/// Create or fetch a reflect mirror for a query by component names.
pub fn use_bevy_query_dyn<const N: usize>(names: [&str; N]) -> ReflectQuerySignal {
    let names: Vec<String> = names.iter().map(|s| s.to_string()).collect();
    let ctx = use_context::<CommandQueueSender>();

    let mut value_signal: Signal<
        Result<HashMap<Entity, Vec<Arc<dyn Reflect>>>, ReflectQueryNoneState>,
    > = use_signal(|| Err(ReflectQueryNoneState::NotInitialized));
    let health_signal = use_signal(|| HealthStatus::Healthy);
    let mut handle_signal: Signal<Option<QueryMutationHandle>> = use_signal(|| None);

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
                    value_signal.set(Err(ReflectQueryNoneState::NameError(message)));
                    return;
                }
                Err(e) => {
                    value_signal.set(Err(ReflectQueryNoneState::NameError(e)));
                    return;
                }
            };

            // Forward the snapshot and seed the initial value.
            let current = handles.signal.read().as_ref().clone();
            value_signal.set(Ok(current));
            handles
                .signal
                .state
                .forward_to(value_signal, health_signal, |arc| Ok((*arc).clone()));

            handle_signal.set(Some(handles.handle.clone()));

            // Re-bind mutation routing whenever the handle is replaced.
            let mut rx = handles.handle_rx;
            while rx.changed().await.is_ok() {
                handle_signal.set(Some(rx.borrow().clone()));
            }
        }
    });

    ReflectQuerySignal {
        value: value_signal,
        health: health_signal,
        handle: handle_signal,
    }
}
