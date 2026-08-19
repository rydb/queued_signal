//! Reflect-driven component and query mirroring.

use std::{any::TypeId, collections::HashMap, sync::Arc};

use bevy_ecs::archetype::{ArchetypeGeneration, ArchetypeId};
use bevy_ecs::change_detection::Tick;
use bevy_ecs::component::ComponentId;
use bevy_ecs::entity::Entity;
use bevy_ecs::prelude::*;
use bevy_ecs::query::{FilteredAccess, FilteredAccessSet};
use bevy_ecs::reflect::AppTypeRegistry;
use bevy_ecs::system::{
    ParamBuilder, SystemMeta, SystemParam, SystemParamBuilder, SystemParamValidationError,
};
use bevy_ecs::world::unsafe_world_cell::UnsafeWorldCell;
use bevy_ecs::world::CommandQueue;
use bevy_reflect::{Reflect, ReflectFromPtr};
use dioxus_hooks::{use_context, use_future, use_signal};
use dioxus_signals::{ReadableExt, Signal, WritableExt};
use queued_signal::state::{HealthStatus, QueuedSignal, SignalReadGuard, WriterDriver};
use parking_lot::Mutex;
use queued_signal_tracing::error;
use tokio::sync::oneshot;

use crate::schedules::{DioxusSyncPostUpdate, DioxusSyncUpdate};
use crate::{CommandQueueSender, add_systems_through_world};

use super::{clone_into_arc, enumerate_reflect_types, resolve_name};

/// Error state for a reflect query signal that has not initialized yet.
#[derive(Clone, Debug, PartialEq)]
pub enum ReflectQueryNoneState {
    /// The mirror request has not resolved yet.
    NotInitialized,
    /// One or more names could not be resolved.
    NameError(String),
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
}

/// Registry of reflect query mirrors.
#[derive(Resource, Default)]
pub struct ReflectQueryRegistry {
    /// Mirrors keyed by the sorted TypeId list of the query.
    pub map: HashMap<Vec<TypeId>, ReflectQueryMirror>,
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

/// Command requesting a reflect mirror for a query by component names.
pub struct RequestBevyQueryDyn {
    response_tx: oneshot::Sender<Result<QueuedSignal<HashMap<Entity, Vec<Arc<dyn Reflect>>>>, String>>,
    names: Vec<String>,
}

impl Command for RequestBevyQueryDyn {
    type Out = ();

    fn apply(self, world: &mut World) {
        let result = register_or_get_query_dyn(world, &self.names);
        let _ = self.response_tx.send(result);
    }
}

/// Registers or returns the reflect mirror for a query by names.
pub fn register_or_get_query_dyn(
    world: &mut World,
    names: &[String],
) -> Result<QueuedSignal<HashMap<Entity, Vec<Arc<dyn Reflect>>>>, String> {
    let type_registry = world
        .get_resource::<AppTypeRegistry>()
        .ok_or("AppTypeRegistry is missing")?
        .clone();
    let infos = enumerate_reflect_types(&type_registry);

    let mut type_ids = Vec::with_capacity(names.len());
    for name in names {
        let type_id = resolve_name(&infos, name).map_err(|e| format!("{e:?}"))?;
        type_ids.push(type_id);
    }
    type_ids.sort_unstable();

    let query_registry = world.resource::<ReflectQueryRegistry>();
    if let Some(mirror) = query_registry.map.get(&type_ids) {
        return Ok(mirror.signal.clone());
    }

    let registry = type_registry.read();
    let mut component_ids = Vec::with_capacity(type_ids.len());
    let mut reflect_from_ptrs = Vec::with_capacity(type_ids.len());
    for type_id in &type_ids {
        let registration = registry.get(*type_id).ok_or("type not in registry")?;
        let reflect_from_ptr = registration
            .data::<ReflectFromPtr>()
            .ok_or("type missing ReflectFromPtr")?
            .clone();
        let component_id = world
            .components()
            .get_id(*type_id)
            .ok_or("component has no ComponentId")?;
        component_ids.push(component_id);
        reflect_from_ptrs.push(reflect_from_ptr);
    }
    drop(registry);

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

    let mirror = ReflectQueryMirror {
        component_ids: component_ids.clone(),
        reflect_from_ptrs: reflect_from_ptrs.clone(),
        signal: signal.clone(),
        active_count: 1,
        elevated: false,
        last_written_version: 0,
    };

    let read_key = type_ids.clone();
    let write_key = type_ids.clone();

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
                let mut out: HashMap<Entity, Vec<Arc<dyn Reflect>>> = HashMap::new();
                for &archetype_id in components.matched_archetypes() {
                    let archetype = &components.world().archetypes()[archetype_id];
                    for archetype_entity in archetype.entities() {
                        let entity = archetype_entity.id();
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
                mirror.signal.set_value(out);
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
                        let mut reflect = untyped.map_unchanged(|ptr| unsafe {
                            reflect_from_ptr.as_reflect_mut(ptr)
                        });
                        reflect.apply(value.as_ref());
                        reflect.set_changed();
                    }
                }
                mirror.last_written_version = version;
            },
        );

    add_systems_through_world(world, DioxusSyncUpdate, read_system);
    add_systems_through_world(world, DioxusSyncPostUpdate, write_system);

    world
        .resource_mut::<ReflectQueryRegistry>()
        .map
        .insert(type_ids, mirror);

    Ok(signal)
}

/// Dioxus handle for a reflect query mirror.
#[derive(Clone, Copy)]
pub struct ReflectQuerySignal {
    value: Signal<Result<HashMap<Entity, Vec<Arc<dyn Reflect>>>, ReflectQueryNoneState>>,
    health: Signal<HealthStatus>,
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
}

/// Create or fetch a reflect mirror for a query by component names.
pub fn use_bevy_query_dyn<const N: usize>(names: [&str; N]) -> ReflectQuerySignal {
    let names: Vec<String> = names.iter().map(|s| s.to_string()).collect();
    let ctx = use_context::<CommandQueueSender>();

    let mut value_signal: Signal<
        Result<HashMap<Entity, Vec<Arc<dyn Reflect>>>, ReflectQueryNoneState>,
    > = use_signal(|| Err(ReflectQueryNoneState::NotInitialized));
    let health_signal = use_signal(|| HealthStatus::Healthy);

    let ctx_clone = ctx.clone();
    use_future(move || {
        let ctx = ctx_clone.clone();
        let names = names.clone();
        async move {
            match ctx
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
                Ok(Ok(signal)) => {
                    let current = signal.read().as_ref().clone();
                    value_signal.set(Ok(current));
                    signal.state.forward_to(value_signal, health_signal, |arc| {
                        Ok((*arc).clone())
                    });
                }
                Ok(Err(e)) => {
                    value_signal.set(Err(ReflectQueryNoneState::NameError(e)));
                }
                Err(e) => {
                    value_signal.set(Err(ReflectQueryNoneState::NameError(e)));
                }
            }
        }
    });

    ReflectQuerySignal {
        value: value_signal,
        health: health_signal,
    }
}
