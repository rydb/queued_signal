//! Reflect-driven asset mirroring.

use std::{
    any::TypeId,
    collections::HashMap,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    ptr::NonNull,
    sync::Arc,
    sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};

use bevy_asset::{AssetId, ReflectAsset, UntypedAssetId};
use bevy_ecs::change_detection::Tick;
use bevy_ecs::component::ComponentId;
use bevy_ecs::prelude::*;
use bevy_ecs::query::{FilteredAccess, FilteredAccessSet};
use bevy_ecs::reflect::AppTypeRegistry;
use bevy_ecs::system::{
    ParamBuilder, SystemMeta, SystemParam, SystemParamBuilder, SystemParamValidationError,
};
use bevy_ecs::world::CommandQueue;
use bevy_ecs::world::unsafe_world_cell::UnsafeWorldCell;
use bevy_ptr::{Ptr, PtrMut};
use bevy_reflect::{Reflect, ReflectFromPtr};
use dioxus_core::{Task, use_drop};
use dioxus_hooks::{use_context, use_future, use_memo, use_signal};
use dioxus_signals::{Memo, ReadableExt, Signal, WritableExt};
use kovan::{pin, Atom, Guard};
use queued_signal::atom_coerce::{
    Arc as QsArc, AtomCoerceHandle, CoerceShared, Value, drop_coerce_shared_alloc,
};
use queued_signal::atom_coerce_dyn::{AtomCoerceDyn, AtomCoerceDynHandle, ErasedShared};
use queued_signal::state::{
    HealthStatus, QueuedSignal, QueuedStateDyn, ReaderRegistry, TrackedReadGuardDyn,
};
use tokio::sync::{oneshot, watch};

use crate::asset::{
    AssetMirrorMap, AssetMirrorRequestResponse, AssetNoneState, AssetUpdateExtraInfo, DioxusAssetSync,
    RequestBevyAssetMirror, UpdateTrackingAssets,
};
use crate::macros::*;
use crate::schedules::{DioxusSyncPostUpdate, DioxusSyncUpdate};
use crate::{CommandQueueSender, add_systems_through_world};

use super::{ErasedMutation, clone_into_arc};

/// Sentinel reflect value used while a typed asset has no loaded value.
#[derive(bevy_reflect::Reflect)]
struct EmptyAsset;

static EMPTY_ASSET: EmptyAsset = EmptyAsset;

/// Type-erased handle to the currently active asset signal.
#[derive(Clone)]
pub struct AssetSignalHandle {
    /// Rebindable zero-copy read view over the current asset value.
    view: QueuedStateDyn<dyn Reflect>,
    /// Whether the current bound value is loaded.
    is_loaded: Arc<dyn Fn() -> bool + Send + Sync>,
    /// Enqueue a relative reflect mutation.
    mutate: Arc<dyn Fn(ErasedMutation) + Send + Sync>,
    /// Enqueue an authoritative reflect mutation.
    mutate_set: Arc<dyn Fn(ErasedMutation) + Send + Sync>,
    /// Replace the signal value.
    set_value: Arc<dyn Fn(Arc<dyn Reflect>) + Send + Sync>,
    /// Forward version and health into dioxus signals, returning the forward task.
    forward_to: Arc<dyn Fn(Signal<u64>, Signal<HealthStatus>) -> Task + Send + Sync>,
}

impl AssetSignalHandle {
    /// Whether the current bound value is loaded.
    pub fn is_loaded(&self) -> bool {
        (self.is_loaded)()
    }

    /// A zero-copy read guard over the current asset value.
    pub fn read_view(&self) -> TrackedReadGuardDyn<dyn Reflect> {
        self.view.read()
    }

    /// Enqueue a relative reflect mutation.
    pub fn mutate(&self, f: ErasedMutation) {
        (self.mutate)(f);
    }

    /// Enqueue an authoritative reflect mutation.
    pub fn mutate_set(&self, f: ErasedMutation) {
        (self.mutate_set)(f);
    }

    /// Replace the signal value.
    pub fn set_value(&self, value: Arc<dyn Reflect>) {
        (self.set_value)(value);
    }

    /// Forward version and health into dioxus signals, returning the forward task.
    pub fn forward_to(&self, version: Signal<u64>, health: Signal<HealthStatus>) -> Task {
        (self.forward_to)(version, health)
    }
}

/// Erased asset mirror keyed by untyped asset id.
pub struct ReflectAssetMirror {
    /// The untyped asset id this mirror tracks.
    pub id: UntypedAssetId,
    /// Untyped asset access type data.
    pub reflect_asset: ReflectAsset,
    /// ComponentId of the `Assets<A>` resource.
    pub assets_component_id: ComponentId,
    /// The single value source, owning a reflected value or bound to a typed T.
    pub holder: AtomCoerceDyn<dyn Reflect>,
    /// Whether a loaded value is currently available.
    pub loaded: Arc<AtomicBool>,
    /// Monotonic version, bumped on every value change.
    pub version: Arc<AtomicU64>,
    /// Notification channel for value changes.
    pub notify_tx: watch::Sender<u64>,
    /// Health notification channel.
    pub health_tx: watch::Sender<HealthStatus>,
    /// Active selection count.
    pub active_count: i32,
    /// Whether a typed mirror has taken over.
    pub elevated: bool,
    /// Last version written back to bevy.
    pub last_written_version: u64,
    /// Last world change tick observed by the read system.
    pub last_change_tick: Tick,
    /// Handle to the currently active signal.
    pub handle: AssetSignalHandle,
    /// Sender for replacing the active handle when elevation happens.
    pub handle_tx: watch::Sender<AssetSignalHandle>,
}

/// Registry of reflect asset mirrors.
#[derive(Resource, Default)]
pub struct ReflectAssetRegistry {
    /// Mirrors keyed by untyped asset id.
    pub map: HashMap<UntypedAssetId, ReflectAssetMirror>,
}

/// Handles for a reflect asset mirror returned to dioxus.
#[derive(Clone)]
pub struct ReflectAssetHandles {
    /// Handle to the currently active signal.
    pub handle: AssetSignalHandle,
    /// Receiver observing replacements of the active handle.
    pub handle_rx: watch::Receiver<AssetSignalHandle>,
}

/// Future returned by a typed asset spawner.
pub type TypedSpawnFuture = Pin<Box<dyn Future<Output = Result<(), String>>>>;

/// Type-erased handle for spinning up a typed asset and sharing its count.
#[derive(Clone)]
pub struct TypedAssetSpawner {
    /// Spins up the typed asset, adopts the reflect mirror, and increments the count.
    pub spawn: Arc<dyn Fn(CommandQueueSender, UntypedAssetId) -> TypedSpawnFuture + Send + Sync>,
    /// Decrements the shared count when the untyped asset unmounts.
    pub despawn: Arc<dyn Fn(&CommandQueueSender, UntypedAssetId) + Send + Sync>,
}

impl TypedAssetSpawner {
    /// Spins up the typed asset and shares its count.
    pub fn spawn(&self, ctx: CommandQueueSender, id: UntypedAssetId) -> TypedSpawnFuture {
        (self.spawn)(ctx, id)
    }

    /// Decrements the shared count.
    pub fn despawn(&self, ctx: &CommandQueueSender, id: UntypedAssetId) {
        (self.despawn)(ctx, id)
    }
}

/// Spawners keyed by asset TypeId.
#[derive(Resource, Default)]
pub struct TypedAssetSpawnerRegistry {
    /// Registered typed asset spawners.
    pub spawners: HashMap<TypeId, TypedAssetSpawner>,
}

/// Command requesting a reflect mirror for an asset by untyped id.
pub struct RequestBevyAssetDyn {
    response_tx: oneshot::Sender<Result<ReflectAssetHandles, String>>,
    id: UntypedAssetId,
}

impl Command for RequestBevyAssetDyn {
    type Out = ();

    fn apply(self, world: &mut World) {
        let result = register_or_get_asset_dyn(world, self.id);
        let _ = self.response_tx.send(result);
    }
}

/// Command fetching a spawner for an asset TypeId.
pub struct GetTypedAssetSpawner {
    /// Asset TypeId to look up.
    pub type_id: TypeId,
    /// Response channel.
    pub response_tx: oneshot::Sender<Option<TypedAssetSpawner>>,
}

impl Command for GetTypedAssetSpawner {
    type Out = ();

    fn apply(self, world: &mut World) {
        let spawner = world
            .resource::<TypedAssetSpawnerRegistry>()
            .spawners
            .get(&self.type_id)
            .cloned();
        let _ = self.response_tx.send(spawner);
    }
}

/// Command adopting a reflect asset mirror into a concrete typed asset mirror.
pub struct AdoptTypedAsset<A: DioxusAssetSync> {
    /// Asset id to adopt.
    pub asset_id: AssetId<A>,
    /// Marker for the typed asset.
    pub _marker: PhantomData<fn() -> A>,
}

impl<A: DioxusAssetSync> Command for AdoptTypedAsset<A> {
    type Out = ();

    fn apply(self, world: &mut World) {
        notify_typed_asset_mirror::<A>(world, self.asset_id);
    }
}

/// Bumps the mirror version and notifies watchers.
fn bump_version(version: &Arc<AtomicU64>, notify_tx: &watch::Sender<u64>) {
    let next = version.fetch_add(1, Ordering::Relaxed) + 1;
    let _ = notify_tx.send(next);
}

/// Builds an owning erased holder around an Arc-backed reflected asset value.
fn asset_shared_owned(value: Arc<dyn Reflect>) -> ErasedShared<dyn Reflect> {
    let shared = QsArc::new(CoerceShared::new(Atom::new(Value::new(value))));
    let data = (&*shared as *const CoerceShared<Arc<dyn Reflect>, dyn Reflect>) as *mut ();
    let strong =
        NonNull::new(shared.as_ptr() as *mut AtomicUsize).expect("arc pointer is non-null");
    let erased = ErasedShared::from_raw_parts(
        NonNull::new(data).expect("arc pointer is non-null"),
        strong,
        drop_coerce_shared_alloc::<Arc<dyn Reflect>, dyn Reflect>,
        load_asset_owned,
        try_store_asset_owned,
        Arc::new(()),
    );
    std::mem::forget(shared);
    erased
}

/// Loads an owned asset snapshot as a zero-copy reflected view.
unsafe fn load_asset_owned(shared: &ErasedShared<dyn Reflect>) -> (*const dyn Reflect, Guard) {
    // SAFETY: data points at the live CoerceShared for the owned view.
    let typed = unsafe { &*(shared.data().as_ptr() as *const CoerceShared<Arc<dyn Reflect>, dyn Reflect>) };
    let epoch = pin();
    let atom_guard = typed.atom().load();
    let value: &Value<Arc<dyn Reflect>, dyn Reflect> = &*atom_guard;
    let arc: &Arc<dyn Reflect> = value.value();
    let ptr = arc.as_ref() as *const dyn Reflect;
    (ptr, epoch)
}

/// Stores a new owned asset snapshot.
unsafe fn try_store_asset_owned(
    shared: &ErasedShared<dyn Reflect>,
    value: Box<dyn std::any::Any + Send + Sync>,
) -> Result<(), Box<dyn std::any::Any + Send + Sync>> {
    // SAFETY: data points at the live CoerceShared for the owned view.
    let typed = unsafe { &*(shared.data().as_ptr() as *const CoerceShared<Arc<dyn Reflect>, dyn Reflect>) };
    match value.downcast::<Arc<dyn Reflect>>() {
        Ok(boxed) => {
            typed.atom().store(Value::new(*boxed));
            Ok(())
        }
        Err(boxed) => Err(boxed),
    }
}

/// Builds an erased shared reference bound to a typed asset cell.
fn asset_bound_shared<A: DioxusAssetSync>(
    typed: &AtomCoerceHandle<Result<A, AssetNoneState>, dyn std::any::Any>,
    reflect_from_ptr: ReflectFromPtr,
) -> ErasedShared<dyn Reflect> {
    let shared = typed.shared();
    let data =
        (&**shared as *const CoerceShared<Result<A, AssetNoneState>, dyn std::any::Any>) as *mut ();
    let strong =
        NonNull::new(shared.as_ptr() as *mut AtomicUsize).expect("arc pointer is non-null");
    // Adopt one strong count for this erased reference.
    unsafe {
        strong.as_ref().fetch_add(1, Ordering::Relaxed);
    }
    ErasedShared::from_raw_parts(
        NonNull::new(data).expect("arc pointer is non-null"),
        strong,
        drop_coerce_shared_alloc::<Result<A, AssetNoneState>, dyn std::any::Any>,
        load_asset_bound::<A>,
        try_store_asset_bound,
        Arc::new(reflect_from_ptr),
    )
}

/// Loads a typed asset cell's Ok value as a runtime reflect view.
unsafe fn load_asset_bound<A: DioxusAssetSync>(
    shared: &ErasedShared<dyn Reflect>,
) -> (*const dyn Reflect, Guard) {
    // SAFETY: data points at the live CoerceShared for the bound typed cell.
    let typed = unsafe {
        &*(shared.data().as_ptr() as *const CoerceShared<Result<A, AssetNoneState>, dyn std::any::Any>)
    };
    let epoch = pin();
    let atom_guard = typed.atom().load();
    let value: &Value<Result<A, AssetNoneState>, dyn std::any::Any> = &*atom_guard;
    match value.value() {
        Ok(a) => {
            let reflect_from_ptr = shared
                .context()
                .downcast_ref::<ReflectFromPtr>()
                .expect("asset bind missing ReflectFromPtr");
            let raw = std::ptr::from_ref(a).cast::<u8>() as *mut u8;
            // SAFETY: raw points at the live A and the pointer mirrors A.
            let ptr = unsafe { Ptr::new(NonNull::new_unchecked(raw)) };
            let reflect = unsafe { reflect_from_ptr.as_reflect(ptr) };
            (reflect as *const dyn Reflect, epoch)
        }
        Err(_) => {
            let sentinel: &dyn Reflect = &EMPTY_ASSET;
            (sentinel as *const dyn Reflect, epoch)
        }
    }
}

/// Rejects writes through a reflect-bound view, which shares the typed cell.
unsafe fn try_store_asset_bound(
    _shared: &ErasedShared<dyn Reflect>,
    value: Box<dyn std::any::Any + Send + Sync>,
) -> Result<(), Box<dyn std::any::Any + Send + Sync>> {
    Err(value)
}

/// Builds a type-erased handle wrapping the owned reflect snapshot.
fn reflect_asset_handle(
    holder: AtomCoerceDyn<dyn Reflect>,
    loaded: Arc<AtomicBool>,
    version: Arc<AtomicU64>,
    notify_tx: watch::Sender<u64>,
    notify_rx: watch::Receiver<u64>,
    health_rx: watch::Receiver<HealthStatus>,
    registry: Arc<ReaderRegistry>,
) -> AssetSignalHandle {
    let view = QueuedStateDyn {
        view: holder.handle_dyn(),
        notify_rx,
        health_rx,
        registry,
    };

    let h = holder.clone();
    let v = version.clone();
    let nt = notify_tx.clone();
    let mutate = Arc::new(move |f: ErasedMutation| {
        let guard = h.get();
        let Ok(mut cloned) = guard.as_dyn().reflect_clone() else {
            error!("reflect clone failed");
            return;
        };
        f(&mut *cloned);
        if h.try_store_boxed(Box::new(Arc::<dyn Reflect>::from(cloned)))
            .is_ok()
        {
            bump_version(&v, &nt);
        }
    });

    let h = holder.clone();
    let v = version.clone();
    let nt = notify_tx.clone();
    let mutate_set = Arc::new(move |f: ErasedMutation| {
        let guard = h.get();
        let Ok(mut cloned) = guard.as_dyn().reflect_clone() else {
            error!("reflect clone failed");
            return;
        };
        f(&mut *cloned);
        if h.try_store_boxed(Box::new(Arc::<dyn Reflect>::from(cloned)))
            .is_ok()
        {
            bump_version(&v, &nt);
        }
    });

    let h = holder.clone();
    let v = version.clone();
    let nt = notify_tx.clone();
    let set_value = Arc::new(move |value: Arc<dyn Reflect>| {
        if h.try_store_boxed(Box::new(value)).is_ok() {
            bump_version(&v, &nt);
        }
    });

    let is_loaded = Arc::new(move || loaded.load(Ordering::Relaxed));

    let view_fwd = view.clone();
    let forward_to = Arc::new(
        move |version_signal: Signal<u64>, health_signal: Signal<HealthStatus>| {
            view_fwd.forward_to(version_signal, health_signal)
        },
    );

    AssetSignalHandle {
        view,
        is_loaded,
        mutate,
        mutate_set,
        set_value,
        forward_to,
    }
}

/// Notifies bevy that a typed asset signal was mutated.
fn notify_changed<A: DioxusAssetSync>(extra_info: &QueuedSignal<AssetUpdateExtraInfo<A>>) {
    let info = extra_info.read();
    let _ = info.changed_sender.send(info.asset_id);
}

/// Builds a type-erased handle wrapping the typed asset signal.
fn typed_asset_handle<A: DioxusAssetSync>(
    view: AtomCoerceDynHandle<dyn Reflect>,
    state: QueuedSignal<Result<A, AssetNoneState>>,
    extra_info: QueuedSignal<AssetUpdateExtraInfo<A>>,
    reflect_from_ptr: ReflectFromPtr,
) -> AssetSignalHandle {
    let s = state.clone();
    let e = extra_info.clone();
    let rfp = reflect_from_ptr.clone();
    let mutate = Arc::new(move |f: ErasedMutation| {
        let rfp = rfp.clone();
        s.mutate(move |value: &mut Result<A, AssetNoneState>| {
            if let Ok(a) = value {
                let raw = std::ptr::from_mut(a).cast::<u8>() as *mut u8;
                // SAFETY: a is a live A and rfp mirrors A.
                let ptr = unsafe { PtrMut::new(NonNull::new_unchecked(raw)) };
                let reflect = unsafe { rfp.as_reflect_mut(ptr) };
                f(reflect);
            }
        });
        notify_changed(&e);
    });

    let s = state.clone();
    let e = extra_info.clone();
    let rfp = reflect_from_ptr.clone();
    let mutate_set = Arc::new(move |f: ErasedMutation| {
        let rfp = rfp.clone();
        s.mutate_set(move |value: &mut Result<A, AssetNoneState>| {
            if let Ok(a) = value {
                let raw = std::ptr::from_mut(a).cast::<u8>() as *mut u8;
                // SAFETY: a is a live A and rfp mirrors A.
                let ptr = unsafe { PtrMut::new(NonNull::new_unchecked(raw)) };
                let reflect = unsafe { rfp.as_reflect_mut(ptr) };
                f(reflect);
            }
        });
        notify_changed(&e);
    });

    let s = state.clone();
    let e = extra_info.clone();
    let rfp = reflect_from_ptr.clone();
    let set_value = Arc::new(move |value: Arc<dyn Reflect>| {
        let rfp = rfp.clone();
        s.mutate_set(move |typed: &mut Result<A, AssetNoneState>| {
            if let Ok(a) = typed {
                let raw = std::ptr::from_mut(a).cast::<u8>() as *mut u8;
                // SAFETY: a is a live A and rfp mirrors A.
                let ptr = unsafe { PtrMut::new(NonNull::new_unchecked(raw)) };
                let reflect = unsafe { rfp.as_reflect_mut(ptr) };
                if let Err(err) = reflect.try_apply(value.as_ref()) {
                    error!("reflect apply failed: {}", err);
                }
            }
        });
        notify_changed(&e);
    });

    let state_view = state.clone();
    let view = QueuedStateDyn {
        view,
        notify_rx: state_view.state.notify_rx(),
        health_rx: state_view.state.health_rx.clone(),
        registry: state_view.state.registry.clone(),
    };

    let s = state.clone();
    let is_loaded = Arc::new(move || matches!(s.read().as_ref(), Ok(_)));

    let state_fwd = state.clone();
    let forward_to = Arc::new(
        move |version_signal: Signal<u64>, health_signal: Signal<HealthStatus>| {
            state_fwd.state.forward_to(version_signal, health_signal)
        },
    );

    AssetSignalHandle {
        view,
        is_loaded,
        mutate,
        mutate_set,
        set_value,
        forward_to,
    }
}

/// Looks up the untyped asset access type data for a type.
fn reflect_asset_for(world: &World, type_id: TypeId) -> Option<ReflectAsset> {
    let registry = world.resource::<AppTypeRegistry>();
    let registry = registry.read();
    registry.get(type_id)?.data::<ReflectAsset>().cloned()
}

/// Looks up the pointer conversion type data for a type.
fn reflect_from_ptr_for(world: &World, type_id: TypeId) -> Option<ReflectFromPtr> {
    let registry = world.resource::<AppTypeRegistry>();
    let registry = registry.read();
    registry.get(type_id)?.data::<ReflectFromPtr>().cloned()
}

/// Registers or returns the reflect mirror for an untyped asset id.
pub fn register_or_get_asset_dyn(
    world: &mut World,
    id: UntypedAssetId,
) -> Result<ReflectAssetHandles, String> {
    let reflect_asset = reflect_asset_for(world, id.type_id())
        .ok_or_else(|| "asset type missing ReflectAsset data, call register_asset_reflect".to_owned())?;

    let registry = world.resource::<ReflectAssetRegistry>();
    if let Some(mirror) = registry.map.get(&id) {
        return Ok(ReflectAssetHandles {
            handle: mirror.handle.clone(),
            handle_rx: mirror.handle_tx.subscribe(),
        });
    }

    let assets_component_id = world
        .components()
        .get_id(reflect_asset.assets_resource_type_id())
        .ok_or_else(|| "Assets resource has no ComponentId".to_owned())?;

    let loaded = Arc::new(AtomicBool::new(false));
    let holder = match reflect_asset.get(world, id) {
        Some(value) => {
            let arc = clone_into_arc(value).map_err(|err| err.to_string())?;
            loaded.store(true, Ordering::Relaxed);
            AtomCoerceDyn::from_shared(asset_shared_owned(arc))
        }
        None => {
            let empty: Arc<dyn Reflect> = Arc::new(EmptyAsset);
            AtomCoerceDyn::from_shared(asset_shared_owned(empty))
        }
    };

    let version = Arc::new(AtomicU64::new(0));
    let (notify_tx, notify_rx) = watch::channel(0u64);
    let (health_tx, health_rx) = watch::channel(HealthStatus::Healthy);

    let handle = reflect_asset_handle(
        holder.clone(),
        loaded.clone(),
        version.clone(),
        notify_tx.clone(),
        notify_rx,
        health_rx,
        Arc::new(ReaderRegistry::default()),
    );
    let (handle_tx, handle_rx) = watch::channel(handle.clone());

    let mirror = ReflectAssetMirror {
        id,
        reflect_asset: reflect_asset.clone(),
        assets_component_id,
        holder: holder.clone(),
        loaded: loaded.clone(),
        version: version.clone(),
        notify_tx: notify_tx.clone(),
        health_tx,
        active_count: 1,
        elevated: false,
        last_written_version: 0,
        last_change_tick: Tick::new(0),
        handle: handle.clone(),
        handle_tx,
    };

    let read_key = id;
    let write_key = id;

    let read_system = (
        ReflectedAssetBuilder {
            reflect_asset: reflect_asset.clone(),
            asset_id: id,
            assets_component_id,
            write: false,
        },
        ParamBuilder::resource_mut::<ReflectAssetRegistry>(),
    )
        .build_state(world)
        .build_system(
            move |reflected: ReflectedAsset, mut registry: ResMut<ReflectAssetRegistry>| {
                let Some(mirror) = registry.map.get_mut(&read_key) else {
                    return;
                };
                if mirror.elevated || mirror.active_count <= 0 {
                    return;
                }
                let world_cell = reflected.world();
                let this_run = world_cell.change_tick();
                let last_run = mirror.last_change_tick;
                mirror.last_change_tick = this_run;
                // SAFETY: read access to Assets<A> was declared in init_access.
                let changed = unsafe { world_cell.world() }
                    .get_resource_change_ticks_by_id(mirror.assets_component_id)
                    .is_some_and(|ticks| ticks.is_changed(last_run, this_run));

                if !changed && mirror.loaded.load(Ordering::Relaxed) {
                    return;
                }

                match reflected.read() {
                    Some(value) => {
                        if let Ok(arc) = clone_into_arc(value) {
                            if mirror
                                .holder
                                .try_store_boxed(Box::new(arc))
                                .is_ok()
                            {
                                mirror.loaded.store(true, Ordering::Relaxed);
                                bump_version(&mirror.version, &mirror.notify_tx);
                            }
                        }
                    }
                    None => {
                        if mirror.loaded.swap(false, Ordering::Relaxed) {
                            bump_version(&mirror.version, &mirror.notify_tx);
                        }
                    }
                }
            },
        );

    let write_system = (
        ReflectedAssetBuilder {
            reflect_asset: reflect_asset.clone(),
            asset_id: id,
            assets_component_id,
            write: true,
        },
        ParamBuilder::resource_mut::<ReflectAssetRegistry>(),
    )
        .build_state(world)
        .build_system(
            move |reflected: ReflectedAsset, mut registry: ResMut<ReflectAssetRegistry>| {
                let Some(mirror) = registry.map.get_mut(&write_key) else {
                    return;
                };
                if mirror.elevated || mirror.active_count <= 0 {
                    return;
                }
                let version = mirror.version.load(Ordering::Relaxed);
                if version == mirror.last_written_version {
                    return;
                }
                let Some(target) = reflected.write_mut() else {
                    mirror.last_written_version = version;
                    return;
                };
                let snapshot = mirror.holder.get();
                if let Err(err) = target.try_apply(snapshot.as_dyn()) {
                    error!("reflect apply failed: {}", err);
                }
                mirror.last_written_version = version;
            },
        );

    add_systems_through_world(world, DioxusSyncUpdate, read_system);
    add_systems_through_world(world, DioxusSyncPostUpdate, write_system);

    world
        .resource_mut::<ReflectAssetRegistry>()
        .map
        .insert(id, mirror);

    Ok(ReflectAssetHandles { handle, handle_rx })
}

/// Registers a spawner for a typed asset when the type reflects.
pub fn register_typed_asset_spawner_runtime<A: DioxusAssetSync>(world: &mut World) {
    let reflectable = {
        let Some(registry) = world.get_resource::<AppTypeRegistry>() else {
            return;
        };
        let registry = registry.read();
        registry
            .get(TypeId::of::<A>())
            .is_some_and(|registration| {
                registration.data::<ReflectAsset>().is_some()
                    && registration.data::<ReflectFromPtr>().is_some()
            })
    };
    if !reflectable {
        return;
    }

    let mut spawners = world.get_resource_or_init::<TypedAssetSpawnerRegistry>();
    if spawners.spawners.contains_key(&TypeId::of::<A>()) {
        return;
    }

    let spawner = TypedAssetSpawner {
        spawn: Arc::new(|ctx: CommandQueueSender, id: UntypedAssetId| -> TypedSpawnFuture {
            Box::pin(spawn_typed_asset::<A>(ctx, id))
        }),
        despawn: Arc::new(|ctx: &CommandQueueSender, id: UntypedAssetId| {
            let mut q = CommandQueue::default();
            q.push(UpdateTrackingAssets::<A> {
                delta: -1,
                asset_id: id.typed_debug_checked::<A>(),
                _phantom: PhantomData,
            });
            let _ = ctx.tx.send(q);
        }),
    };

    spawners.spawners.insert(TypeId::of::<A>(), spawner);
}

async fn spawn_typed_asset<A: DioxusAssetSync>(
    ctx: CommandQueueSender,
    id: UntypedAssetId,
) -> Result<(), String> {
    let typed_id = id.typed_debug_checked::<A>();

    let _: AssetMirrorRequestResponse<A> = ctx
        .send_command_async(|tx| {
            let mut q = CommandQueue::default();
            q.push(RequestBevyAssetMirror::<A> {
                response_tx: tx,
                asset_id: typed_id,
            });
            q
        })
        .await?;

    let mut q = CommandQueue::default();
    q.push(AdoptTypedAsset::<A> {
        asset_id: typed_id,
        _marker: PhantomData,
    });
    let _ = ctx.tx.send(q);

    let mut q = CommandQueue::default();
    q.push(UpdateTrackingAssets::<A> {
        delta: 1,
        asset_id: typed_id,
        _phantom: PhantomData,
    });
    let _ = ctx.tx.send(q);

    Ok(())
}

/// Hook called from the typed asset request after the typed mirror exists.
/// Binds the erased mirror to the typed asset signal and notifies dioxus.
pub fn notify_typed_asset_mirror<A: DioxusAssetSync>(world: &mut World, asset_id: AssetId<A>) {
    let Some(reflect_from_ptr) = reflect_from_ptr_for(world, TypeId::of::<A>()) else {
        return;
    };
    if reflect_asset_for(world, TypeId::of::<A>()).is_none() {
        return;
    }

    let untyped = asset_id.untyped();

    let (state, extra_info) = {
        let map = world.resource::<AssetMirrorMap<A>>();
        let Some(entry) = map.assets.get(&asset_id) else {
            return;
        };
        (entry.state.clone(), entry.extra_update_info.clone())
    };

    let mut registry = world.resource_mut::<ReflectAssetRegistry>();
    let Some(mirror) = registry.map.get_mut(&untyped) else {
        return;
    };
    if mirror.elevated {
        return;
    }

    let handle = typed_asset_handle::<A>(
        mirror.holder.handle_dyn(),
        state.clone(),
        extra_info.clone(),
        reflect_from_ptr.clone(),
    );

    mirror
        .holder
        .bind_shared(asset_bound_shared::<A>(&state.state.cell, reflect_from_ptr));
    mirror.elevated = true;
    mirror.active_count = 0;
    mirror.handle = handle.clone();
    let _ = mirror.handle_tx.send_replace(handle);
}

/// State for the [`ReflectedAsset`] parameter.
pub struct ReflectedAssetState {
    /// Untyped asset access type data.
    pub reflect_asset: Option<ReflectAsset>,
    /// The untyped asset id this parameter may access.
    pub asset_id: Option<UntypedAssetId>,
    /// ComponentId of the `Assets<A>` resource.
    pub assets_component_id: Option<ComponentId>,
    /// Whether access is write.
    pub write: bool,
}

impl Default for ReflectedAssetState {
    fn default() -> Self {
        Self {
            reflect_asset: None,
            asset_id: None,
            assets_component_id: None,
            write: false,
        }
    }
}

/// System parameter granting data-driven access to one untyped asset.
pub struct ReflectedAsset<'w, 's> {
    world: UnsafeWorldCell<'w>,
    reflect_asset: &'s ReflectAsset,
    asset_id: &'s UntypedAssetId,
}

impl<'w> ReflectedAsset<'w, '_> {
    /// The world cell backing this parameter.
    pub fn world(&self) -> UnsafeWorldCell<'w> {
        self.world
    }

    /// Reads the current asset value.
    pub fn read(&self) -> Option<&'w dyn Reflect> {
        // SAFETY: read access to Assets<A> was declared in init_access.
        let world = unsafe { self.world.world() };
        self.reflect_asset.get(world, *self.asset_id)
    }

    /// Writes the current asset value.
    pub fn write_mut(&self) -> Option<&'w mut dyn Reflect> {
        // SAFETY: write access to Assets<A> was declared in init_access.
        unsafe { self.reflect_asset.get_unchecked_mut(self.world, *self.asset_id) }
    }
}

// SAFETY: init_access declares exactly the access in state, and get_param only
// reads or writes the declared Assets<A> resource through UnsafeWorldCell.
unsafe impl SystemParam for ReflectedAsset<'_, '_> {
    type State = ReflectedAssetState;
    type Item<'w, 's> = ReflectedAsset<'w, 's>;

    fn init_state(_world: &mut World) -> Self::State {
        ReflectedAssetState::default()
    }

    fn init_access(
        state: &Self::State,
        _system_meta: &mut SystemMeta,
        component_access_set: &mut FilteredAccessSet,
        _world: &mut World,
    ) {
        let Some(assets_component_id) = state.assets_component_id else {
            return;
        };
        let mut filtered = FilteredAccess::default();
        if state.write {
            filtered.add_write(assets_component_id);
        } else {
            filtered.add_read(assets_component_id);
        }
        component_access_set.add(filtered);
    }

    unsafe fn get_param<'w, 's>(
        state: &'s mut Self::State,
        _system_meta: &SystemMeta,
        world: UnsafeWorldCell<'w>,
        _change_tick: Tick,
    ) -> Result<Self::Item<'w, 's>, SystemParamValidationError> {
        Ok(ReflectedAsset {
            world,
            reflect_asset: state
                .reflect_asset
                .as_ref()
                .expect("ReflectedAsset state missing reflect_asset"),
            asset_id: state
                .asset_id
                .as_ref()
                .expect("ReflectedAsset state missing asset_id"),
        })
    }
}

/// Builder producing [`ReflectedAssetState`] for a concrete asset.
pub struct ReflectedAssetBuilder {
    /// Untyped asset access type data.
    pub reflect_asset: ReflectAsset,
    /// The untyped asset id to grant access to.
    pub asset_id: UntypedAssetId,
    /// ComponentId of the `Assets<A>` resource.
    pub assets_component_id: ComponentId,
    /// Whether access is write.
    pub write: bool,
}

// SAFETY: build produces a state that matches the access declared in init_access.
unsafe impl<'w, 's> SystemParamBuilder<ReflectedAsset<'w, 's>> for ReflectedAssetBuilder {
    fn build(self, _world: &mut World) -> ReflectedAssetState {
        ReflectedAssetState {
            reflect_asset: Some(self.reflect_asset),
            asset_id: Some(self.asset_id),
            assets_component_id: Some(self.assets_component_id),
            write: self.write,
        }
    }
}

/// Dioxus handle for a reflect asset mirror.
#[derive(Clone, Copy)]
pub struct ReflectAssetSignal {
    version: Signal<u64>,
    health: Signal<HealthStatus>,
    handle: Signal<Option<AssetSignalHandle>>,
}

impl ReflectAssetSignal {
    /// Read the current asset value as a zero-copy reflect guard.
    pub fn read(&self) -> Result<TrackedReadGuardDyn<dyn Reflect>, AssetNoneState> {
        let _ = self.version.read();
        let handle = self.handle.read();
        let Some(handle) = handle.as_ref() else {
            return Err(AssetNoneState::Fetching);
        };
        if !handle.is_loaded() {
            return Err(AssetNoneState::Loading);
        }
        Ok(handle.read_view())
    }

    /// Current health status of the underlying signal.
    pub fn health(&self) -> HealthStatus {
        *self.health.read()
    }

    /// Enqueue a relative mutation applied to the reflected value.
    pub fn mutate<F>(&self, f: F)
    where
        F: Fn(&mut dyn Reflect) + Send + Sync + 'static,
    {
        let Some(handle) = self.handle.read().clone() else {
            warn!("handle not yet available");
            return;
        };
        handle.mutate(Arc::new(f));
    }

    /// Enqueue an authoritative mutation applied to the reflected value.
    pub fn mutate_set<F>(&self, f: F)
    where
        F: Fn(&mut dyn Reflect) + Send + Sync + 'static,
    {
        let Some(handle) = self.handle.read().clone() else {
            warn!("handle not yet available");
            return;
        };
        handle.mutate_set(Arc::new(f));
    }

    /// Enqueue a full replacement of the reflected value.
    pub fn set_value(&self, value: Arc<dyn Reflect>) {
        let Some(handle) = self.handle.read().clone() else {
            warn!("handle not yet available");
            return;
        };
        handle.set_value(value);
    }
}

/// Create or fetch a reflect mirror for an asset by untyped id.
pub fn use_bevy_asset_dyn(
    id: Memo<Result<UntypedAssetId, AssetNoneState>>,
) -> ReflectAssetSignal {
    let ctx = use_context::<CommandQueueSender>();

    let version = use_signal(|| 0u64);
    let health = use_signal(|| HealthStatus::Healthy);
    let mut handle_signal: Signal<Option<AssetSignalHandle>> = use_signal(|| None);
    let mut spawner_signal: Signal<Option<(TypedAssetSpawner, UntypedAssetId)>> =
        use_signal(|| None);

    let (id_tx, id_rx) =
        tokio::sync::mpsc::channel::<Result<UntypedAssetId, AssetNoneState>>(100);

    let id_tx = use_signal(|| Some(id_tx));

    // Forward asset id changes into the fetch task.
    use_memo(move || {
        let id = id.read().clone();
        if let Some(tx) = id_tx.read().as_ref() {
            let _ = tx
                .try_send(id)
                .inspect_err(|_err| warn!("asset id channel rejected update: {}", _err));
        }
    });

    let ctx_r = ctx.clone();
    let mut id_rx = Some(id_rx);
    use_future(move || {
        let ctx = ctx_r.clone();
        let mut id_rx = id_rx.take().expect("asset id receiver taken twice");
        async move {
            let mut state_forward: Option<Task> = None;
            let mut handle_rx: Option<watch::Receiver<AssetSignalHandle>> = None;

            loop {
                tokio::select! {
                    next = id_rx.recv() => {
                        let Some(mut next) = next else {
                            break;
                        };
                        // Drain stale ids so only the newest is processed.
                        while let Ok(newer) = id_rx.try_recv() {
                            next = newer;
                        }

                        if let Some(task) = state_forward.take() {
                            task.cancel();
                        }
                        let previous = spawner_signal.read().clone();
                        if let Some((spawner, old)) = previous {
                            spawner.despawn(&ctx, old);
                            spawner_signal.set(None);
                        }
                        handle_rx = None;
                        handle_signal.set(None);

                        let id = match next {
                            Ok(id) => id,
                            Err(state) => {
                                debug!("asset id unavailable: {}", state);
                                continue;
                            }
                        };

                        let handles = match ctx
                            .send_command_async(|tx| {
                                let mut q = CommandQueue::default();
                                q.push(RequestBevyAssetDyn {
                                    response_tx: tx,
                                    id,
                                });
                                q
                            })
                            .await
                        {
                            Ok(Ok(handles)) => handles,
                            Ok(Err(e)) => {
                                error!("reflect asset request failed: {}", e);
                                continue;
                            }
                            Err(e) => {
                                warn!("send_command_async failed: {}", e);
                                continue;
                            }
                        };

                        // Spin up and share the typed asset when a spawner is registered.
                        let spawner = ctx
                            .send_command_async(|tx| {
                                let mut q = CommandQueue::default();
                                q.push(GetTypedAssetSpawner {
                                    type_id: id.type_id(),
                                    response_tx: tx,
                                });
                                q
                            })
                            .await
                            .ok()
                            .flatten();
                        if let Some(spawner) = spawner {
                            if let Err(err) = spawner.spawn(ctx.clone(), id).await {
                                warn!("typed asset spawn failed: {}", err);
                            } else {
                                spawner_signal.set(Some((spawner, id)));
                            }
                        }

                        handle_signal.set(Some(handles.handle.clone()));
                        state_forward = Some(handles.handle.forward_to(version, health));
                        handle_rx = Some(handles.handle_rx);
                    }
                    changed = wait_handle_change(&mut handle_rx) => {
                        if let Ok(()) = changed {
                            let handle = handle_rx
                                .as_ref()
                                .expect("handle receiver exists after change")
                                .borrow()
                                .clone();
                            if let Some(task) = state_forward.take() {
                                task.cancel();
                            }
                            handle_signal.set(Some(handle.clone()));
                            state_forward = Some(handle.forward_to(version, health));
                        }
                    }
                }
            }
        }
    });

    let ctx_drop = ctx.clone();
    use_drop(move || {
        if let Some((spawner, id)) = spawner_signal.read().as_ref() {
            spawner.despawn(&ctx_drop, *id);
        }
    });

    ReflectAssetSignal {
        version,
        health,
        handle: handle_signal,
    }
}

/// Waits for a handle replacement on the active receiver.
async fn wait_handle_change(
    rx: &mut Option<watch::Receiver<AssetSignalHandle>>,
) -> Result<(), watch::error::RecvError> {
    match rx.as_mut() {
        Some(rx) => rx.changed().await,
        None => std::future::pending().await,
    }
}
