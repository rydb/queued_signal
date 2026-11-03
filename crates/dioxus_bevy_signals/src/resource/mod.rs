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
use queued_signal::state::{HealthStatus, QueuedSignal, TrackedReadGuard, WriterDriver};
use std::any::TypeId;
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
                        std::any::type_name::<T>()
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
    driver: Res<ResourceWriteDriver<T>>,
) {
    let bevy_changed = resource.is_changed();
    let mut guard = driver.0.lock();
    let (mut set_values, mut sets, adds) = guard.drain_ops();
    let has_dioxus_ops = !set_values.is_empty() || !sets.is_empty() || !adds.is_empty();

    if !bevy_changed && !has_dioxus_ops {
        guard.update_health();
        return;
    }

    // One subscriber pass per publish, so a scope that stops reading is pruned after
    // PRUNE_AFTER_PASSES publishes instead of on every frame.
    guard.queued_state.begin_pass();

    if bevy_changed {
        // Bevy wins authoritative ops; only relative adds compose.
        set_values.clear();
        sets.clear();
        for op in adds {
            op.apply(&mut *resource);
        }
        // Publish bevy's value to the cell.
        guard.publish_value(resource.clone());
    } else {
        // Apply the oplog to the bevy resource, the write buffer.
        for op in &set_values {
            *resource = (*op.0).clone();
        }
        for op in &sets {
            op.apply(&mut *resource);
        }
        for op in &adds {
            op.apply(&mut *resource);
        }

        // Publish the bevy resource as the new snapshot.
        guard.publish_value(resource.clone());
    }

    // Submit what the publish retired, so the count read below is the deferral window and not
    // this frame's unplaced retires.
    guard.submit_pending();
    guard.update_health();
}

trait_set! {
    /// Bounds required for mirrored resource values.
    pub trait UBounds = Send + Sync + PartialEq + 'static
}

/// Borrowing projection used to display one field of a resource.
type Projection<R, U> = Arc<dyn for<'a> Fn(&'a R) -> &'a U>;

/// Dioxus signal for managing bevy resource synchronization.
pub struct ResourceMirrorSignal<R, U, E>
where
    R: Clone + Send + Sync + 'static,
    U: UBounds,
    E: 'static,
{
    version: Signal<u64>,
    health: Signal<HealthStatus>,
    /// None until the bevy round-trip completes.
    /// Writes are silently ignored while pending.
    writer: Signal<Option<QueuedSignal<R>>>,
    /// Projection applied on each read, allocated once when the hook runs.
    map_fn: Signal<Projection<R, U>>,
    /// Latest version and error state, which holds no read pin.
    display: Memo<Result<(), E>>,
}

impl<R: Clone + Send + Sync + 'static, U: UBounds, E> Clone for ResourceMirrorSignal<R, U, E> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<R: Clone + Send + Sync + 'static, U: UBounds, E> Copy for ResourceMirrorSignal<R, U, E> {}

impl<R, U: UBounds, E> Display for ResourceMirrorSignal<R, U, E>
where
    R: Clone + Send + Sync + 'static,
    U: Display + 'static,
    E: Display + PartialEq + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Reading the state subscribes this scope. The guard below is transient.
        // The version changes on every publish, so it is what re-renders this scope.
        let _ = self.version.read();
        let display = self.display.read();
        if let Err(err) = display.as_ref() {
            return write!(f, "{err}");
        }
        let writer = self.writer.peek();
        let Some(signal) = writer.as_ref() else {
            return Ok(());
        };
        let map = self.map_fn.peek();
        let projection: &Projection<R, U> = &map;
        // A tracked read joins the slot array, so a publish marks this scope directly while the
        // guard stays transient and drops before the render returns.
        let value = signal.read().project(|resource| projection(resource));
        write!(f, "{}", *value)
    }
}

impl<R, U: UBounds, E> IntoDynNode for ResourceMirrorSignal<R, U, E>
where
    R: Clone + Send + Sync + 'static,
    U: Display + 'static,
    E: Display + PartialEq + 'static,
{
    fn into_dyn_node(self) -> dioxus_core::DynamicNode {
        self.to_string().into_dyn_node()
    }
}

impl<R, U: UBounds, E> IntoAttributeValue for ResourceMirrorSignal<R, U, E>
where
    R: Clone + Send + Sync + 'static,
    U: Display + 'static,
    E: Display + PartialEq + 'static,
{
    fn into_value(self) -> dioxus_core::AttributeValue {
        self.to_string().into_value()
    }
}

impl<R: Clone + Send + Sync + 'static, U: UBounds, E> ResourceMirrorSignal<R, U, E> {
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
        let _ = self.health.read();
        let writer = self.writer.peek();
        match writer.as_ref() {
            Some(signal) => signal.health(),
            None => *self.health.peek(),
        }
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
///
/// `map_fn` projects a borrowed field of the resource for display and equality.
pub fn use_bevy_resource<T, U, E>(
    map_fn: impl for<'a> Fn(&'a T) -> &'a U + 'static,
    err_fn: impl Fn(ResourceNoneState) -> E + Clone + 'static,
) -> ResourceMirrorSignal<T, U, E>
where
    T: ResourceDioxusSync,
    U: UBounds,
    E: PartialEq,
{
    let ctx = use_context::<CommandQueueSender>();

    let version: Signal<u64> = use_signal(|| 0);
    let health_signal = use_signal(|| HealthStatus::Healthy);
    let mut writer: Signal<Option<QueuedSignal<T>>> = use_signal(|| None);
    // The projection is allocated once and borrowed on every read.
    let map_fn: Signal<Projection<T, U>> = use_signal(move || Arc::new(map_fn) as Projection<T, U>);

    let display: Memo<Result<(), E>> = {
        let err_fn = err_fn.clone();
        use_memo(move || {
            let _ = version.read();
            let writer = writer.read();
            match writer.as_ref() {
                Some(_) => Ok(()),
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
        map_fn,
        display,
    }
}
