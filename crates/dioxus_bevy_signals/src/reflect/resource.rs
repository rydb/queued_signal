//! Reflect-driven resource mirroring.

use std::{any::TypeId, collections::HashMap, ptr::NonNull, sync::Arc};
use std::sync::atomic::{AtomicU64, Ordering};

use bevy_ecs::component::ComponentId;
use bevy_ecs::prelude::*;
use bevy_ecs::ptr::{Ptr, PtrMut};
use bevy_ecs::reflect::AppTypeRegistry;
use bevy_ecs::system::{
    FilteredResourcesMutParamBuilder, FilteredResourcesParamBuilder, ParamBuilder,
    SystemParamBuilder,
};
use bevy_ecs::world::{CommandQueue, FilteredResources, FilteredResourcesMut};
use bevy_reflect::{Reflect, ReflectFromPtr};
use dioxus_core::{spawn, Task};
use dioxus_hooks::{use_context, use_future, use_signal};
use dioxus_signals::{ReadableExt, Signal, WritableExt};
use queued_signal::atom_coerce_dyn::AtomCoerceDyn;
use queued_signal::state::{HealthStatus, QueuedSignal, SignalReadGuard};
use tokio::sync::{oneshot, watch};

use crate::macros::*;
use crate::resource::{ResourceDioxusSync, ResourceQueuedSignalMirror};
use crate::schedules::{DioxusSyncPostUpdate, DioxusSyncUpdate};
use crate::{CommandQueueSender, add_systems_through_world};

use super::{
    AtomCoerceDynReflectExt, ErasedMutation, clone_into_arc, enumerate_reflect_types,
    reflect_holder_owned, resolve_name,
};

/// Error state for a reflect resource signal that has not initialized yet.
#[derive(Clone, Debug, PartialEq)]
pub enum ReflectResourceNoneState {
    /// The mirror request has not resolved yet.
    NotInitialized,
    /// The name could not be resolved to exactly one resource.
    NameError(String),
    /// The reflected value could not be cloned for the dioxus side.
    CloneError(String),
}

/// Type-erased handle to the currently active resource signal.
#[derive(Clone)]
pub struct ResourceSignalHandle {
    /// Enqueue a relative reflect mutation.
    mutate: Arc<dyn Fn(ErasedMutation) + Send + Sync>,
    /// Enqueue an authoritative reflect mutation.
    mutate_set: Arc<dyn Fn(ErasedMutation) + Send + Sync>,
    /// Replace the signal value.
    set_value: Arc<dyn Fn(Arc<dyn Reflect>) + Send + Sync>,
    /// Read the current value as a reflected value.
    read_reflect: Arc<dyn Fn() -> Option<Arc<dyn Reflect>> + Send + Sync>,
    /// Forward value and health into dioxus signals, returning the forward task.
    forward_to: Arc<
        dyn Fn(
                Signal<Result<Arc<dyn Reflect>, ReflectResourceNoneState>>,
                Signal<HealthStatus>,
            ) -> Task
            + Send
            + Sync,
    >,
}

impl ResourceSignalHandle {
    /// Enqueue a relative reflect mutation into the active signal.
    pub fn mutate(&self, f: ErasedMutation) {
        (self.mutate)(f);
    }

    /// Enqueue an authoritative reflect mutation into the active signal.
    pub fn mutate_set(&self, f: ErasedMutation) {
        (self.mutate_set)(f);
    }

    /// Replace the active signal value.
    pub fn set_value(&self, value: Arc<dyn Reflect>) {
        (self.set_value)(value);
    }

    /// Read the current value as a reflected value.
    pub fn read_reflect(&self) -> Option<Arc<dyn Reflect>> {
        (self.read_reflect)()
    }

    /// Forward value and health into dioxus signals, returning the forward task.
    pub fn forward_to(
        &self,
        value: Signal<Result<Arc<dyn Reflect>, ReflectResourceNoneState>>,
        health: Signal<HealthStatus>,
    ) -> Task {
        (self.forward_to)(value, health)
    }
}

/// Erased resource mirror keyed by TypeId.
pub struct ReflectResourceMirror {
    /// ComponentId of the resource.
    pub component_id: ComponentId,
    /// Type data for pointer conversion.
    pub reflect_from_ptr: ReflectFromPtr,
    /// The single value source, owning a reflected value or bound to a typed T.
    pub holder: AtomCoerceDyn<dyn Reflect>,
    /// Monotonic version, bumped on every value change.
    pub version: Arc<AtomicU64>,
    /// Notification channel for value changes.
    pub notify_tx: watch::Sender<u64>,
    /// Active selection count.
    pub active_count: i32,
    /// Whether a typed mirror has taken over.
    pub elevated: bool,
    /// Last version written back to bevy.
    pub last_written_version: u64,
    /// Handle to the currently active signal.
    pub handle: ResourceSignalHandle,
    /// Sender for replacing the active handle when elevation happens.
    pub handle_tx: watch::Sender<ResourceSignalHandle>,
}

/// Registry of reflect resource mirrors.
#[derive(Resource, Default)]
pub struct ReflectResourceRegistry {
    /// Mirrors keyed by TypeId.
    pub map: HashMap<TypeId, ReflectResourceMirror>,
}

/// Drives reflect resource signals.
pub fn drive_reflect_resource_signals(_registry: ResMut<ReflectResourceRegistry>) {}

/// Reads the current owning value of a holder as a shared reflected arc.
fn read_holder_arc(holder: &AtomCoerceDyn<dyn Reflect>) -> Option<Arc<dyn Reflect>> {
    let view = holder.get();
    view.as_dyn().downcast_ref::<Arc<dyn Reflect>>().cloned()
}

/// Bumps the mirror version and notifies watchers.
fn bump_version(version: &Arc<AtomicU64>, notify_tx: &watch::Sender<u64>) {
    let next = version.fetch_add(1, Ordering::Relaxed) + 1;
    let _ = notify_tx.send(next);
}

/// Builds a type-erased handle wrapping the reflect holder.
fn reflect_signal_handle(
    holder: AtomCoerceDyn<dyn Reflect>,
    version: Arc<AtomicU64>,
    notify_tx: watch::Sender<u64>,
    notify_rx: watch::Receiver<u64>,
) -> ResourceSignalHandle {
    let h = holder.clone();
    let v = version.clone();
    let nt = notify_tx.clone();
    let mutate = Arc::new(move |f: ErasedMutation| {
        let Some(current) = read_holder_arc(&h) else {
            return;
        };
        let Ok(mut cloned) = current.reflect_clone() else {
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
        let Some(current) = read_holder_arc(&h) else {
            return;
        };
        let Ok(mut cloned) = current.reflect_clone() else {
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

    let h = holder.clone();
    let read_reflect = Arc::new(move || read_holder_arc(&h));

    let h = holder.clone();
    let forward_to = Arc::new(
        move |value: Signal<Result<Arc<dyn Reflect>, ReflectResourceNoneState>>,
              health: Signal<HealthStatus>| {
            let h = h.clone();
            let mut notify_rx = notify_rx.clone();
            spawn(async move {
                let mut value = value;
                let mut health = health;
                health.set(HealthStatus::Healthy);
                loop {
                    if notify_rx.changed().await.is_err() {
                        break;
                    }
                    if let Some(arc) = read_holder_arc(&h) {
                        value.set(Ok(arc));
                    }
                    tokio::task::yield_now().await;
                }
            })
        },
    );

    ResourceSignalHandle {
        mutate,
        mutate_set,
        set_value,
        read_reflect,
        forward_to,
    }
}

/// Builds a type-erased handle wrapping the typed signal using pointer conversion.
fn typed_signal_handle<T: ResourceDioxusSync>(
    signal: QueuedSignal<T>,
    reflect_from_ptr: ReflectFromPtr,
) -> ResourceSignalHandle {
    let s = signal.clone();
    let rfp = reflect_from_ptr.clone();
    let mutate = Arc::new(move |f: ErasedMutation| {
        let rfp = rfp.clone();
        s.mutate(move |value: &mut T| {
            let raw = std::ptr::from_mut(value).cast::<u8>() as *mut u8;
            // SAFETY: value is a live T and rfp mirrors T.
            let ptr = unsafe { PtrMut::new(NonNull::new_unchecked(raw)) };
            let reflect = unsafe { rfp.as_reflect_mut(ptr) };
            f(reflect);
        });
    });

    let s = signal.clone();
    let rfp = reflect_from_ptr.clone();
    let mutate_set = Arc::new(move |f: ErasedMutation| {
        let rfp = rfp.clone();
        s.mutate_set(move |value: &mut T| {
            let raw = std::ptr::from_mut(value).cast::<u8>() as *mut u8;
            // SAFETY: value is a live T and rfp mirrors T.
            let ptr = unsafe { PtrMut::new(NonNull::new_unchecked(raw)) };
            let reflect = unsafe { rfp.as_reflect_mut(ptr) };
            f(reflect);
        });
    });

    let s = signal.clone();
    let rfp = reflect_from_ptr.clone();
    let set_value = Arc::new(move |value: Arc<dyn Reflect>| {
        let rfp = rfp.clone();
        s.mutate_set(move |typed: &mut T| {
            let raw = std::ptr::from_mut(typed).cast::<u8>() as *mut u8;
            // SAFETY: typed is a live T and rfp mirrors T.
            let ptr = unsafe { PtrMut::new(NonNull::new_unchecked(raw)) };
            let reflect = unsafe { rfp.as_reflect_mut(ptr) };
            if let Err(err) = reflect.try_apply(value.as_ref()) {
                error!("reflect apply failed: {}", err);
            }
        });
    });

    let s = signal.clone();
    let rfp = reflect_from_ptr.clone();
    let read_reflect = Arc::new(move || {
        let guard = s.read();
        let value = guard.as_ref();
        let raw = std::ptr::from_ref(value).cast::<u8>() as *mut u8;
        // SAFETY: value is a live T and rfp mirrors T.
        let ptr = unsafe { Ptr::new(NonNull::new_unchecked(raw)) };
        let reflect = unsafe { rfp.as_reflect(ptr) };
        clone_into_arc(reflect).ok()
    });

    let state = signal.state.clone();
    let rfp = reflect_from_ptr.clone();
    let forward_to = Arc::new(move |value, health| {
        let rfp = rfp.clone();
        state.forward_value_to(value, health, move |arc_t: Arc<T>| {
            let raw = std::ptr::from_ref(arc_t.as_ref()).cast::<u8>() as *mut u8;
            // SAFETY: arc_t is live and rfp mirrors T.
            let ptr = unsafe { Ptr::new(NonNull::new_unchecked(raw)) };
            let reflect = unsafe { rfp.as_reflect(ptr) };
            match clone_into_arc(reflect) {
                Ok(arc) => Ok(arc),
                Err(err) => Err(ReflectResourceNoneState::CloneError(err.to_string())),
            }
        })
    });

    ResourceSignalHandle {
        mutate,
        mutate_set,
        set_value,
        read_reflect,
        forward_to,
    }
}

/// Handles for the reflect resource mirror returned to dioxus.
#[derive(Clone)]
pub struct ReflectResourceHandles {
    /// Handle to the currently active signal.
    pub handle: ResourceSignalHandle,
    /// Receiver observing replacements of the active handle.
    pub handle_rx: watch::Receiver<ResourceSignalHandle>,
}

/// Command requesting a reflect mirror for a resource by name.
pub struct RequestBevyResourceDyn {
    response_tx: oneshot::Sender<Result<ReflectResourceHandles, String>>,
    name: String,
}

impl Command for RequestBevyResourceDyn {
    type Out = ();

    fn apply(self, world: &mut World) {
        let result = register_or_get_resource_dyn(world, &self.name);
        let _ = self.response_tx.send(result);
    }
}

/// Registers or returns the reflect mirror for a resource name.
pub fn register_or_get_resource_dyn(
    world: &mut World,
    name: &str,
) -> Result<ReflectResourceHandles, String> {
    let type_registry = world
        .get_resource::<AppTypeRegistry>()
        .ok_or("AppTypeRegistry is missing")?
        .clone();
    let infos = enumerate_reflect_types(&type_registry);
    let type_id = resolve_name(&infos, name).map_err(|e| format!("{e:?}"))?;

    let registry = world.resource::<ReflectResourceRegistry>();
    if let Some(mirror) = registry.map.get(&type_id) {
        return Ok(ReflectResourceHandles {
            handle: mirror.handle.clone(),
            handle_rx: mirror.handle_tx.subscribe(),
        });
    }

    let registration = type_registry
        .read()
        .get(type_id)
        .ok_or("type not in registry")?
        .clone();
    let reflect_from_ptr = registration
        .data::<ReflectFromPtr>()
        .ok_or("type missing ReflectFromPtr")?
        .clone();
    let component_id = world
        .components()
        .get_id(type_id)
        .ok_or("resource has no ComponentId")?;

    let initial = {
        let ptr = world
            .get_resource_by_id(component_id)
            .ok_or("resource does not exist")?;
        // SAFETY: ptr holds the type mirrored by reflect_from_ptr.
        let value = unsafe { reflect_from_ptr.as_reflect(ptr) };
        match clone_into_arc(value) {
            Ok(arc) => arc,
            Err(err) => return Err(err.to_string()),
        }
    };

    let holder = reflect_holder_owned(initial);
    let version = Arc::new(AtomicU64::new(0));
    let (notify_tx, notify_rx) = watch::channel(0u64);

    let handle = reflect_signal_handle(holder.clone(), version.clone(), notify_tx.clone(), notify_rx);
    let (handle_tx, handle_rx) = watch::channel(handle.clone());

    let mirror = ReflectResourceMirror {
        component_id,
        reflect_from_ptr: reflect_from_ptr.clone(),
        holder: holder.clone(),
        version: version.clone(),
        notify_tx: notify_tx.clone(),
        active_count: 1,
        elevated: false,
        last_written_version: 0,
        handle: handle.clone(),
        handle_tx,
    };

    let read_system = (
        FilteredResourcesParamBuilder::new(move |b| {
            b.add_read_by_id(component_id);
        }),
        ParamBuilder::resource_mut::<ReflectResourceRegistry>(),
    )
        .build_state(world)
        .build_system(
            move |resources: FilteredResources, mut registry: ResMut<ReflectResourceRegistry>| {
                let Some(mirror) = registry.map.get_mut(&type_id) else {
                    return;
                };
                if mirror.elevated || mirror.active_count <= 0 {
                    return;
                }
                let Ok(ptr) = resources.get_by_id(mirror.component_id) else {
                    return;
                };
                // SAFETY: ptr holds the type mirrored by reflect_from_ptr.
                let value = unsafe { mirror.reflect_from_ptr.as_reflect(ptr) };
                if let Ok(arc) = clone_into_arc(value)
                    && mirror
                        .holder
                        .try_store_boxed(Box::new(arc))
                        .is_ok()
                    {
                        bump_version(&mirror.version, &mirror.notify_tx);
                    }
            },
        );

    let write_system = (
        FilteredResourcesMutParamBuilder::new(move |b| {
            b.add_write_by_id(component_id);
        }),
        ParamBuilder::resource_mut::<ReflectResourceRegistry>(),
    )
        .build_state(world)
        .build_system(
            move |mut resources: FilteredResourcesMut,
                  mut registry: ResMut<ReflectResourceRegistry>| {
                let Some(mirror) = registry.map.get_mut(&type_id) else {
                    return;
                };
                if mirror.elevated || mirror.active_count <= 0 {
                    return;
                }
                let version = mirror.version.load(Ordering::Relaxed);
                if version == mirror.last_written_version {
                    return;
                }
                let Some(arc) = read_holder_arc(&mirror.holder) else {
                    return;
                };
                let Ok(untyped) = resources.get_mut_by_id(mirror.component_id) else {
                    return;
                };
                // SAFETY: untyped holds the type mirrored by reflect_from_ptr.
                let mut reflect = untyped
                    .map_unchanged(|ptr| unsafe { mirror.reflect_from_ptr.as_reflect_mut(ptr) });
                reflect.apply(arc.as_ref());
                reflect.set_changed();
                mirror.last_written_version = version;
            },
        );

    add_systems_through_world(world, DioxusSyncUpdate, read_system);
    add_systems_through_world(world, DioxusSyncPostUpdate, write_system);

    world
        .resource_mut::<ReflectResourceRegistry>()
        .map
        .insert(type_id, mirror);

    Ok(ReflectResourceHandles { handle, handle_rx })
}

/// Hook called from the typed resource request after the typed mirror exists.
/// Binds the erased mirror to the typed signal value and notifies dioxus.
pub fn notify_typed_resource_mirror<T: ResourceDioxusSync>(world: &mut World) {
    let type_id = TypeId::of::<T>();

    let Some(typed) = world.get_resource::<ResourceQueuedSignalMirror<T>>() else {
        return;
    };
    let typed_signal = typed.0.clone();

    let reflect_from_ptr = {
        let registry = world.resource::<ReflectResourceRegistry>();
        let Some(mirror) = registry.map.get(&type_id) else {
            return;
        };
        mirror.reflect_from_ptr.clone()
    };

    let handle = typed_signal_handle::<T>(typed_signal.clone(), reflect_from_ptr.clone());

    let mut registry = world.resource_mut::<ReflectResourceRegistry>();
    let Some(mirror) = registry.map.get_mut(&type_id) else {
        return;
    };
    // Bind the erased holder to the typed cell so both share the one T value.
    mirror
        .holder
        .bind_reflect(&typed_signal.state.cell, reflect_from_ptr);
    mirror.elevated = true;
    mirror.active_count = 0;
    mirror.handle = handle.clone();
    let _ = mirror.handle_tx.send_replace(handle);
}

/// Dioxus handle for a reflect resource mirror.
#[derive(Clone, Copy)]
pub struct ReflectResourceSignal {
    value: Signal<Result<Arc<dyn Reflect>, ReflectResourceNoneState>>,
    health: Signal<HealthStatus>,
    handle: Signal<Option<ResourceSignalHandle>>,
}

impl ReflectResourceSignal {
    /// Read the current reflected value.
    pub fn read(&self) -> SignalReadGuard<'_, Result<Arc<dyn Reflect>, ReflectResourceNoneState>> {
        SignalReadGuard::new(self.value.read())
    }

    /// Current health status of the underlying signal.
    pub fn health(&self) -> HealthStatus {
        *self.health.read()
    }

    /// Downcast the current value to a concrete type.
    pub fn read_as<T: Reflect + Clone>(&self) -> Option<Arc<T>> {
        let guard = self.value.read();
        let result: &Result<Arc<dyn Reflect>, ReflectResourceNoneState> = &guard;
        let arc = result.as_ref().ok()?;
        let value: &T = arc.downcast_ref::<T>()?;
        Some(Arc::new(value.clone()))
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

/// Create or fetch a reflect mirror for a resource by name.
pub fn use_bevy_resource_dyn(name: impl Into<String>) -> ReflectResourceSignal {
    let name = name.into();
    let ctx = use_context::<CommandQueueSender>();

    let mut value_signal: Signal<Result<Arc<dyn Reflect>, ReflectResourceNoneState>> =
        use_signal(|| Err(ReflectResourceNoneState::NotInitialized));
    let health_signal = use_signal(|| HealthStatus::Healthy);
    let mut handle_signal: Signal<Option<ResourceSignalHandle>> = use_signal(|| None);

    let ctx_clone = ctx.clone();
    let name_clone = name.clone();
    use_future(move || {
        let ctx = ctx_clone.clone();
        let name = name_clone.clone();
        async move {
            let handles = match ctx
                .send_command_async(|tx| {
                    let mut q = CommandQueue::default();
                    q.push(RequestBevyResourceDyn {
                        response_tx: tx,
                        name: name.clone(),
                    });
                    q
                })
                .await
            {
                Ok(Ok(handles)) => handles,
                Ok(Err(e)) => {
                    value_signal.set(Err(ReflectResourceNoneState::NameError(e)));
                    return;
                }
                Err(e) => {
                    value_signal.set(Err(ReflectResourceNoneState::NameError(e)));
                    return;
                }
            };

            // Bind the initial handle and forward its value.
            handle_signal.set(Some(handles.handle.clone()));
            if let Some(arc) = handles.handle.read_reflect() {
                value_signal.set(Ok(arc));
            } else {
                value_signal.set(Err(ReflectResourceNoneState::CloneError(
                    "clone failed".to_owned(),
                )));
            }
            let mut task = handles.handle.forward_to(value_signal, health_signal);

            // Re-bind whenever bevy replaces the handle on elevation.
            let mut rx = handles.handle_rx;
            while rx.changed().await.is_ok() {
                let handle = rx.borrow().clone();
                task.cancel();
                handle_signal.set(Some(handle.clone()));
                if let Some(arc) = handle.read_reflect() {
                    value_signal.set(Ok(arc));
                }
                task = handle.forward_to(value_signal, health_signal);
            }
        }
    });

    ReflectResourceSignal {
        value: value_signal,
        health: health_signal,
        handle: handle_signal,
    }
}
