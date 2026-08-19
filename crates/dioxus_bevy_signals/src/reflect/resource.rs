//! Reflect-driven resource mirroring.

use std::{any::TypeId, collections::HashMap, ptr::NonNull, sync::Arc, time::Duration};

use bevy_ecs::component::ComponentId;
use bevy_ecs::prelude::*;
use bevy_ecs::ptr::{Ptr, PtrMut};
use bevy_ecs::reflect::AppTypeRegistry;
use bevy_ecs::system::{
    FilteredResourcesMutParamBuilder, FilteredResourcesParamBuilder, ParamBuilder, SystemParamBuilder,
};
use bevy_ecs::world::{FilteredResources, FilteredResourcesMut, CommandQueue};
use bevy_reflect::{Reflect, ReflectFromPtr};
use dioxus_core::Task;
use dioxus_hooks::{use_context, use_future, use_signal};
use dioxus_signals::{ReadableExt, Signal, WritableExt};
use parking_lot::Mutex;
use queued_signal::state::{HealthStatus, QueuedSignal, SignalReadGuard, WriterDriver};
use tokio::sync::{oneshot, watch};

use crate::macros::*;
use crate::resource::{ResourceDioxusSync, ResourceQueuedSignalMirror};
use crate::schedules::{DioxusSyncPostUpdate, DioxusSyncUpdate};
use crate::{CommandQueueSender, add_systems_through_world};

use super::{ErasedValue, clone_into_arc, enumerate_reflect_types, resolve_name};

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

/// Type-erased mutation operating on a reflected value.
pub type ErasedMutation = Arc<dyn Fn(&mut dyn Reflect) + Send + Sync>;

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
    /// The erased signal mirror.
    pub signal: QueuedSignal<ErasedValue>,
    /// Active selection count.
    pub active_count: i32,
    /// Whether a typed mirror has taken over.
    pub elevated: bool,
    /// Last signal version written back to bevy.
    pub last_written_version: u64,
    /// Driver that publishes queued mutations into the signal read buffer.
    pub driver: Arc<Mutex<WriterDriver<ErasedValue>>>,
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

/// Ticks every active reflect resource driver so queued mutations publish.
pub fn drive_reflect_resource_signals(mut registry: ResMut<ReflectResourceRegistry>) {
    for mirror in registry.map.values_mut() {
        if mirror.elevated {
            continue;
        }
        let mut guard = mirror.driver.lock();
        guard.tick(Duration::ZERO);
    }
}

/// Builds a type-erased handle wrapping the reflect signal.
fn reflect_signal_handle(signal: QueuedSignal<ErasedValue>) -> ResourceSignalHandle {
    let s = signal.clone();
    let mutate = Arc::new(move |f: ErasedMutation| {
        s.mutate(move |erased: &mut ErasedValue| match erased.0.reflect_clone() {
            Ok(mut cloned) => {
                f(&mut *cloned);
                erased.0 = Arc::from(cloned);
            }
            Err(err) => error!("reflect clone failed: {}", err),
        });
    });

    let s = signal.clone();
    let mutate_set = Arc::new(move |f: ErasedMutation| {
        s.mutate_set(move |erased: &mut ErasedValue| match erased.0.reflect_clone() {
            Ok(mut cloned) => {
                f(&mut *cloned);
                erased.0 = Arc::from(cloned);
            }
            Err(err) => error!("reflect clone failed: {}", err),
        });
    });

    let s = signal.clone();
    let set_value = Arc::new(move |value: Arc<dyn Reflect>| {
        if let Ok(erased) = ErasedValue::new(value.as_ref()) {
            s.set_value(erased);
        }
    });

    let s = signal.clone();
    let read_reflect = Arc::new(move || {
        let guard = s.read();
        clone_into_arc(guard.as_reflect()).ok()
    });

    let state = signal.state.clone();
    let forward_to = Arc::new(move |value, health| {
        state.forward_to(value, health, |arc: Arc<ErasedValue>| match arc.0.reflect_clone() {
            Ok(boxed) => Ok(Arc::from(boxed)),
            Err(err) => Err(ReflectResourceNoneState::CloneError(err.to_string())),
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
        state.forward_to(value, health, move |arc_t: Arc<T>| {
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
        match ErasedValue::new(value) {
            Ok(erased) => erased,
            Err(err) => return Err(err.to_string()),
        }
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

    let handle = reflect_signal_handle(signal.clone());
    let (handle_tx, handle_rx) = watch::channel(handle.clone());

    let mirror = ReflectResourceMirror {
        component_id,
        reflect_from_ptr: reflect_from_ptr.clone(),
        signal: signal.clone(),
        active_count: 1,
        elevated: false,
        last_written_version: 0,
        driver: driver_arc.clone(),
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
                if let Ok(erased) = ErasedValue::new(value) {
                    mirror.signal.set_value(erased);
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
            move |mut resources: FilteredResourcesMut, mut registry: ResMut<ReflectResourceRegistry>| {
                let Some(mirror) = registry.map.get_mut(&type_id) else {
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
                let pending: &dyn Reflect = guard.as_reflect();
                let Ok(untyped) = resources.get_mut_by_id(mirror.component_id) else {
                    return;
                };
                // SAFETY: untyped holds the type mirrored by reflect_from_ptr.
                let mut reflect = untyped
                    .map_unchanged(|ptr| unsafe { mirror.reflect_from_ptr.as_reflect_mut(ptr) });
                reflect.apply(pending);
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
/// Replaces the erased mirror handle with the typed signal handle and notifies dioxus.
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

    let handle = typed_signal_handle::<T>(typed_signal, reflect_from_ptr);

    let mut registry = world.resource_mut::<ReflectResourceRegistry>();
    let Some(mirror) = registry.map.get_mut(&type_id) else {
        return;
    };
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
