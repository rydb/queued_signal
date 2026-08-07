//! Bevy resource mirroring via QueuedSignals.
//!
//! Provides [`use_bevy_resource`] to create dioxus-side signal mirrors
//! of bevy resources, with automatic bidirectional synchronization.

use crate::schedules::{DioxusSyncPostUpdate, DioxusSyncPreUpdate, DioxusSyncUpdate};
use bevy_ecs::component::Mutable;
use bevy_ecs::prelude::*;
use bevy_ecs::world::CommandQueue;
use dioxus_core::IntoDynNode;
use dioxus_hooks::{use_context, use_future, use_memo, use_signal};
use dioxus_signals::{Memo, ReadableExt, Signal, WritableExt};
use parking_lot::Mutex;
use queued_signal::state::{HealthStatus, QueuedSignal, SignalReadGuard, WriterDriver};
use std::any::{TypeId, type_name};
use std::collections::HashSet;
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;
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

                add_systems_through_world(world, DioxusSyncUpdate, drive_signal::<T>);
                add_systems_through_world(
                    world,
                    DioxusSyncPreUpdate,
                    sync_mirror_to_resource::<T>.run_if(resource_changed::<T>),
                );
                add_systems_through_world(
                    world,
                    DioxusSyncPostUpdate,
                    sync_resource_to_mirror::<T>,
                );
                let mut map = world.get_resource_or_init::<RegisteredResourceSyncs>();
                map.0.insert(TypeId::of::<T>());
                world.insert_resource(ResourceQueuedSignalMirror(signal.clone()));
                signal
            }
        };
        let _ = self.response_tx.send(signal_to_send);
    }
}

/// Synchronizes the authoritative bevy resource into the signal mirror.
/// Runs when the bevy resource has changed. When bevy and dioxus both
/// modify the resource in the same frame, the bevy change takes precedence.
fn sync_mirror_to_resource<T: ResourceDioxusSync>(
    resource: Res<T>,
    mut mirror: ResMut<ResourceQueuedSignalMirror<T>>,
) {
    let new_value = resource.clone();
    // Send authoritative full replacement.
    mirror.bypass_change_detection().0.set_value(new_value);
}

/// Synchronizes a dioxus-side signal mutation back into the bevy resource.
/// Only writes when the dioxus signal version advanced, avoiding unnecessary
/// writes every frame.
fn sync_resource_to_mirror<T: ResourceDioxusSync>(
    mut resource: ResMut<T>,
    mirror: Res<ResourceQueuedSignalMirror<T>>,
    mut last_version: Local<u64>,
) {
    let current = mirror.0.peek_version();
    if current == *last_version {
        return; // dioxus side hasn't published a new version
    }
    *last_version = current;
    let new_value = mirror.0.read().as_ref().clone();
    *resource.bypass_change_detection() = new_value;
}

fn drive_signal<T: ResourceDioxusSync>(driver: Res<ResourceWriteDriver<T>>) {
    let mut guard = driver.0.lock();
    guard.tick(Duration::ZERO);
}

/// Dioxus signal for managing bevy resource synchronization.
pub struct ResourceMirrorSignal<R, U, E>
where
    R: Clone + Send + Sync + 'static,
{
    signal: Signal<Result<Arc<R>, ResourceNoneState>>,
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

    /// Read resource
    pub fn read(&self) -> SignalReadGuard<'_, Result<Arc<R>, ResourceNoneState>> {
        SignalReadGuard::new(self.signal.read())
    }

    /// .read() + .map()
    pub fn read_ok<O>(&self, f: impl FnOnce(&R) -> O) -> Result<O, ResourceNoneState> {
        let guard = self.signal.read();
        match &*guard {
            Ok(arc_r) => Ok(f(arc_r.as_ref())),
            Err(e) => Err(e.clone()),
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
    U: PartialEq + 'static,
    E: PartialEq + 'static,
{
    let ctx = use_context::<CommandQueueSender>();

    let mut value_signal: Signal<Result<Arc<T>, ResourceNoneState>> =
        use_signal(|| Err(ResourceNoneState::NotInitialized));
    let health_signal = use_signal(|| HealthStatus::Healthy);
    let mut writer: Signal<Option<QueuedSignal<T>>> = use_signal(|| None);

    let display = {
        let value_signal = value_signal;
        let map_fn = map_fn.clone();
        let err_fn = err_fn.clone();
        use_memo(move || {
            let guard = value_signal.read();
            match &*guard {
                Ok(arc_r) => Ok(map_fn(arc_r.as_ref())),
                Err(e) => Err(err_fn(e.clone())),
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
                    // Eagerly forward the current mirrored value so
                    // static resources are available immediately.
                    let current = signal.read().clone();
                    value_signal.set(Ok(current));
                    signal.state.forward_to(value_signal, health_signal, Ok);
                    writer.set(Some(signal));
                }
                Err(err) => warn!("use_bevy_resource: {}", err),
            }
        }
    });

    ResourceMirrorSignal {
        signal: value_signal,
        health: health_signal,
        writer,
        display,
    }
}
