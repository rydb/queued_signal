//! Bevy resource mirroring via QueuedSignals.
//!
//! Provides [`use_bevy_resource`] to create dioxus-side signal mirrors
//! of bevy resources, with automatic bidirectional synchronization.

use crate::schedules::DioxusSyncPostUpdate;
use bevy_ecs::component::Mutable;
use bevy_ecs::prelude::*;
use bevy_ecs::world::CommandQueue;
use dioxus_core::{IntoAttributeValue, IntoDynNode};
use dioxus_hooks::{use_context, use_future, use_memo, use_signal};
use dioxus_signals::{Memo, ReadableExt, Signal, WritableExt};
use parking_lot::Mutex;
use queued_signal::state::{
    HealthStatus, QueuedSignal, SetValueOp, TrackedReadGuard, WriterDriver,
};
use std::any::{TypeId, type_name};
use std::collections::HashSet;
use std::fmt::Display;
use std::sync::Arc;
use tokio::sync::oneshot;
use trait_set::trait_set;

use crate::macros::*;

use crate::{CommandQueueSender, add_systems_through_world};

/// Convenience re-export of the standard Result type.
pub type Result<T, E> = std::result::Result<T, E>;

trait_set! {
    /// Resource that can be synced with dioxus.
    pub trait ResourceDioxusSync = bevy_ecs::resource::Resource + Component<Mutability = Mutable> + Clone + Send + Sync + 'static;
}

/// Error state for a resource signal that hasn't been initialized yet.
#[derive(Clone, Debug, PartialEq)]
pub enum ResourceNoneState {
    /// The resource mirror has not been requested from bevy yet.
    NotInitialized,
}

impl Display for ResourceNoneState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            ResourceNoneState::NotInitialized => "Not Initialized",
        };
        write!(f, "{}", value)
    }
}

/// Write driver for ticking resource signal updates.
#[derive(Resource)]
pub struct ResourceWriteDriver<T: ResourceDioxusSync>(pub Arc<Mutex<WriterDriver<T>>>);

struct RequestBevyResource<T: ResourceDioxusSync> {
    response_tx: oneshot::Sender<QueuedSignal<T>>,
}

/// The queued signal mirroring a bevy resource.
#[derive(Resource)]
pub struct ResourceQueuedSignalMirror<T: ResourceDioxusSync>(pub QueuedSignal<T>);

/// Set of [`TypeId`]s for resources that have registered sync systems.
#[derive(Resource, Default)]
pub struct RegisteredResourceSyncs(HashSet<TypeId>);

impl<T: ResourceDioxusSync> Command for RequestBevyResource<T> {
    type Out = ();

    fn apply(self, world: &mut World) {
        let signal_to_send = match world.get_resource::<ResourceQueuedSignalMirror<T>>() {
            Some(signal) => signal.0.clone(),
            None => {
                // put synced resources in registry for tracking
                world
                    .get_resource_or_init::<RegisteredResourceSyncs>()
                    .0
                    .insert(TypeId::of::<T>());

                let Some(resource) = world.get_resource::<T>().cloned() else {
                    warn!(
                        "Cannot initialize dioxus-bevy sync for {} as this resource does not exist at the time of this sync request.",
                        type_name::<T>()
                    );
                    return;
                };

                let driver = WriterDriver::new(resource.clone());
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
                world.insert_resource(ResourceWriteDriver(driver_arc));

                add_systems_through_world(world, DioxusSyncPostUpdate, sync_resource::<T>);
                let mut map = world.get_resource_or_init::<RegisteredResourceSyncs>();
                map.0.insert(TypeId::of::<T>());
                world.insert_resource(ResourceQueuedSignalMirror(signal.clone()));

                // Elevate any erased reflect mirror for this type.
                #[cfg(feature = "reflect")]
                crate::reflect::resource::notify_typed_resource_mirror::<T>(world);

                signal
            }
        };
        let _ = self.response_tx.send(signal_to_send);
    }
}

/// Sync bevy <-> dioxus resource values
fn sync_resource<T: ResourceDioxusSync>(
    mut resource: ResMut<T>,
    mut driver: ResMut<ResourceWriteDriver<T>>,
) {
    let bevy_changed = resource.is_changed();
    let mut guard = driver.0.lock();
    let (set_values, sets, adds) = guard.drain_ops();
    let has_dioxus_ops = !set_values.is_empty() || !sets.is_empty() || !adds.is_empty();

    if !bevy_changed && !has_dioxus_ops {
        guard.update_health();
        return;
    }

    if bevy_changed {
        // Bevy wins authoritative sets; relative adds compose on top.
        for f in adds {
            f(&mut *resource);
        }
        if let Ok(()) = guard.try_swap(&mut *resource) {
            let value = (*guard.read()).clone();
            *resource = value;
            guard.publish();
        }
    } else {
        // Dioxus wins; apply operations to the read buffer.
        match guard.get_mut() {
            Ok(slot) => {
                for value in set_values {
                    *slot = value;
                }
                for f in sets {
                    f(slot);
                }
                for f in adds {
                    f(slot);
                }
            }
            Err(_count) => {
                warn!("readers active, deferring mutations: {}", _count);
                for value in set_values {
                    let _ = guard.set_value_tx.send(SetValueOp(value));
                }
                for f in sets {
                    let _ = guard.set_tx.send(f);
                }
                for f in adds {
                    let _ = guard.add_tx.send(f);
                }
                guard.update_health();
                return;
            }
        }
        if let Ok(()) = guard.try_swap(&mut *resource) {
            let value = resource.clone();
            if let Ok(slot) = guard.get_mut() {
                *slot = value;
            }
            guard.publish();
        }
    }

    guard.update_health();
}

/// Dioxus signal for managing bevy resource synchronization.
pub struct ResourceMirrorSignal<R, U, E>
where
    R: Clone + Send + Sync + 'static,
    U: 'static,
    E: 'static,
{
    version: Signal<u64>,
    health: Signal<HealthStatus>,
    /// None until the bevy round-trip completes.
    /// Writes are silently ignored while pending.
    writer: Signal<Option<QueuedSignal<R>>>,
    /// Memoized mapped value for reactive display in RSX.
    display: Memo<Result<U, E>>,
}

impl<R: Clone + Send + Sync + 'static, U, E> Clone for ResourceMirrorSignal<R, U, E> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<R: Clone + Send + Sync + 'static, U, E> Copy for ResourceMirrorSignal<R, U, E> {}

impl<R, U, E> Display for ResourceMirrorSignal<R, U, E>
where
    R: Clone + Send + Sync + 'static,
    U: Display + PartialEq + 'static,
    E: Display + PartialEq + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.display.read().as_ref() {
            Ok(u) => write!(f, "{u}"),
            Err(e) => write!(f, "{e}"),
        }
    }
}

impl<R, U, E> IntoDynNode for ResourceMirrorSignal<R, U, E>
where
    R: Clone + Send + Sync + 'static,
    U: Display + PartialEq + Clone + 'static,
    E: Display + PartialEq + Clone + 'static,
{
    fn into_dyn_node(self) -> dioxus_core::DynamicNode {
        self.to_string().into_dyn_node()
    }
}

impl<R, U, E> IntoAttributeValue for ResourceMirrorSignal<R, U, E>
where
    R: Clone + Send + Sync + 'static,
    U: Display + PartialEq + Clone + 'static,
    E: Display + PartialEq + Clone + 'static,
{
    fn into_value(self) -> dioxus_core::AttributeValue {
        self.to_string().into_value()
    }
}

impl<R: Clone + Send + Sync + 'static, U, E> ResourceMirrorSignal<R, U, E> {
    /// Enqueue a relative mutation.
    pub fn mutate<F>(&self, f: F)
    where
        F: Fn(&mut R) + Send + Sync + 'static,
    {
        if let Some(w) = self.writer.read().as_ref() {
            w.mutate(f);
        } else {
            warn!(
                "ResourceMirrorSignal::mutate dropped: writer not yet available (Bevy round-trip pending)"
            );
        }
    }

    /// Enqueue an authoritative mutation.
    pub fn mutate_set<F>(&self, f: F)
    where
        F: Fn(&mut R) + Send + Sync + 'static,
    {
        if let Some(w) = self.writer.read().as_ref() {
            w.mutate_set(f);
        } else {
            warn!(
                "ResourceMirrorSignal::mutate_set dropped: writer not yet available (Bevy round-trip pending)"
            );
        }
    }

    /// Enqueue a full-value replacement.
    pub fn set_value(&self, value: R) {
        if let Some(w) = self.writer.read().as_ref() {
            w.set_value(value);
        } else {
            warn!(
                "ResourceMirrorSignal::set_value dropped: writer not yet available (Bevy round-trip pending)"
            );
        }
    }

    /// Current health status of the underlying signal.
    pub fn health(&self) -> HealthStatus {
        *self.health.read()
    }

    /// Read resource, subscribing to version updates.
    pub fn read(&self) -> Result<TrackedReadGuard<R>, ResourceNoneState> {
        let _ = self.version.read();
        let writer = self.writer.read();
        match writer.as_ref() {
            Some(signal) => Ok(signal.read()),
            None => Err(ResourceNoneState::NotInitialized),
        }
    }

    /// .read() + .map()
    pub fn read_ok<O>(&self, f: impl FnOnce(&R) -> O) -> Result<O, ResourceNoneState> {
        let _ = self.version.read();
        let writer = self.writer.read();
        match writer.as_ref() {
            Some(signal) => Ok(f(&signal.read())),
            None => Err(ResourceNoneState::NotInitialized),
        }
    }
}

/// Create or fetch a signal mirror for a bevy resource.
pub fn use_bevy_resource<T, U, E>(
    map_fn: impl Fn(&T) -> U + Clone + 'static,
    err_fn: impl Fn(ResourceNoneState) -> E + Clone + 'static,
) -> ResourceMirrorSignal<T, U, E>
where
    T: ResourceDioxusSync,
    U: PartialEq,
    E: PartialEq,
{
    let ctx = use_context::<CommandQueueSender>();

    let mut version: Signal<u64> = use_signal(|| 0);
    let health_signal = use_signal(|| HealthStatus::Healthy);
    let mut writer: Signal<Option<QueuedSignal<T>>> = use_signal(|| None);

    let display = {
        let version = version;
        let writer = writer;
        let map_fn = map_fn.clone();
        let err_fn = err_fn.clone();
        use_memo(move || {
            let _ = version.read();
            let writer = writer.read();
            match writer.as_ref() {
                Some(signal) => Ok(map_fn(&signal.read())),
                None => Err(err_fn(ResourceNoneState::NotInitialized)),
            }
        })
    };

    let ctx_clone = ctx.clone();
    use_future(move || {
        let ctx = ctx_clone.clone();
        async move {
            match ctx
                .send_command_async(|tx| {
                    let mut command_queue = CommandQueue::default();
                    command_queue.push(RequestBevyResource::<T> { response_tx: tx });
                    command_queue
                })
                .await
            {
                Ok(signal) => {
                    signal.state.forward_to(version, health_signal);
                    writer.set(Some(signal));
                }
                Err(_err) => warn!("use_bevy_resource: {}", _err),
            }
        }
    });

    ResourceMirrorSignal {
        version,
        health: health_signal,
        writer,
        display,
    }
}
