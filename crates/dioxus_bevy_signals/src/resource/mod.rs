pub(crate) use crate::macros::*;
use bevy_app::prelude::*;
use bevy_ecs::world::CommandQueue;
use bevy_ecs::prelude::*;
use dioxus::prelude::*;
use dioxus_hooks::{use_context, use_signal};
use dioxus_signals::{ReadableExt, Signal};
use parking_lot::Mutex;
use queued_signal::signal::{HealthStatus, QueuedSignal, WriterDriver};
use std::any::{TypeId, type_name};
use std::collections::HashSet;
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use trait_set::trait_set;

use crate::{CommandQueueSender, add_systems_through_world, SignalReadGuard};

pub type Result<T, E> = std::result::Result<T, E>;

trait_set! {
    /// Resource that is syncable with dioxus
    pub trait ResourceDioxusSync = bevy_ecs::resource::Resource + Clone + Send + Sync + 'static;
}

/// Error state for a resource signal that hasn't been initialized yet.
#[derive(Clone, Debug)]
pub enum ResourceNoneState {
    NotInitialized,
}

impl From<ResourceNoneState> for String {
    fn from(value: ResourceNoneState) -> Self {
        match value {
            ResourceNoneState::NotInitialized => "Not Initialized".into(),
        }
    }
}

#[derive(Resource)]
pub struct ResourceWriteDriver<T: ResourceDioxusSync>(pub Arc<Mutex<WriterDriver<T>>>);

struct RequestBevyResource<T: ResourceDioxusSync> {
    response_tx: oneshot::Sender<QueuedSignal<T>>,
}

#[derive(Resource)]
pub struct ResourceQueuedSignalMirror<T: ResourceDioxusSync>(pub QueuedSignal<T>);

#[derive(Resource, Default)]
pub struct RegisteredResourceSyncs(HashSet<TypeId>);

impl<T: ResourceDioxusSync> Command for RequestBevyResource<T> {
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

                // tick the resources 60 times a second (If uninitialized) by default to match standard FPS settings.
                world.get_resource_or_insert_with(|| {
                    ResourceSyncTickRate(Duration::from_millis(16))
                });

                add_systems_through_world(world, Update, drive_signal::<T>);
                // Also add the authoritative sync system, but **after** command processing.
                add_systems_through_world(
                    world,
                    PostUpdate,
                    sync_mirror_to_resource::<T>.run_if(resource_changed::<T>),
                );
                add_systems_through_world(
                    world,
                    PostUpdate,
                    sync_resource_to_mirror::<T>.run_if(not(resource_changed::<T>)),
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

/// Minimum time to pass til queued mutations from QueuedSignal are published.
/// The time to publish may be longer then this duration, but no shorter then this duration.
#[derive(Resource)]
pub struct ResourceSyncTickRate(Duration);

/// System that synchronises the authoritative Bevy resource into the signal mirror.
/// It uses `set_value`, which results in exactly two clones (one into the operation,
/// one for the second internal buffer).
pub fn sync_mirror_to_resource<T: ResourceDioxusSync>(
    resource: Res<T>,
    mut mirror: ResMut<ResourceQueuedSignalMirror<T>>,
) {
    if resource.is_changed() {
        // 1 clone: Bevy resource -> new_value
        let new_value = resource.clone();
        // send authoritative full replacement (2nd clone happens internally)
        mirror
            .bypass_change_detection()
            .0
            .set_value(new_value.into());
    }
}

pub fn sync_resource_to_mirror<T: ResourceDioxusSync>(
    mut resource: ResMut<T>,
    mirror: Res<ResourceQueuedSignalMirror<T>>,
) {
    let new_value = mirror.0.read().as_ref().clone();
    *resource.bypass_change_detection() = new_value
}

fn drive_signal<T: ResourceDioxusSync>(
    driver: Res<ResourceWriteDriver<T>>,
    tick_rate: Res<ResourceSyncTickRate>,
) {
    let mut guard = driver.0.lock();
    guard.tick(tick_rate.0);
}

/// Dioxus signal for managing bevy resource <-> dioxus interop.
#[derive(Clone)]
pub struct ResourceMirrorSignal<R: Clone + Send + Sync + 'static> {
    pub signal: Signal<Result<Arc<R>, ResourceNoneState>>,
    pub health: Signal<HealthStatus>,
    /// `None` until the Bevy round-trip completes (non-blocking). Writes are
    /// silently ignored while the writer is still pending.
    pub writer: Signal<Option<QueuedSignal<R>>>,
}

impl<R: Clone + Send + Sync + 'static> Copy for ResourceMirrorSignal<R> {}

impl<R: Clone + Send + Sync + 'static + Display> Display for ResourceMirrorSignal<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.read_ok(|n| format!("{}", n)).unwrap_or_else(|n| n.into()))
    }
}

impl<R: Clone + Send + Sync + 'static> ResourceMirrorSignal<R> {
    pub fn mutate<F>(&self, f: F)
    where
        F: Fn(&mut R) + Send + Sync + 'static,
    {
        if let Some(w) = self.writer.read().as_ref() {
            w.mutate(f);
        }
    }

    pub fn mutate_set<F>(&self, f: F)
    where
        F: Fn(&mut R) + Send + Sync + 'static,
    {
        if let Some(w) = self.writer.read().as_ref() {
            w.mutate_set(f);
        }
    }

    pub fn set_value(&self, value: Arc<R>) {
        if let Some(w) = self.writer.read().as_ref() {
            w.set_value(value);
        }
    }

    pub fn health(&self) -> HealthStatus {
        *self.health.read()
    }

    /// Read resource
    pub fn read(&self) -> SignalReadGuard<'_, Result<Arc<R>, ResourceNoneState>> {
        SignalReadGuard::new(self.signal.read())
    }

    /// Read and map the `Ok` variant of the resource, or pass through the error.
    pub fn read_ok<U>(&self, f: impl FnOnce(&R) -> U) -> Result<U, ResourceNoneState> {
        let guard = self.signal.read();
        match &*guard {
            Ok(arc_r) => Ok(f(arc_r.as_ref())),
            Err(e) => Err(e.clone()),
        }
    }
}

/// Create or fetch a QueuedSignal mirror to a bevy resource.
///
/// This hook is **non-blocking**: it returns immediately with the value
/// signal set to [`ResourceNoneState::NotInitialized`] and the writer
/// set to `None`.  The real [`QueuedSignal`] is fetched asynchronously
/// via [`CommandQueueSender::send_command_async`] and installed when
/// the Bevy world processes the request.
pub fn use_bevy_resource<T>() -> ResourceMirrorSignal<T>
where
    T: ResourceDioxusSync,
{
    let ctx = use_context::<CommandQueueSender>();

    let mut value_signal = use_signal(|| Err(ResourceNoneState::NotInitialized));
    let mut health_signal = use_signal(|| HealthStatus::Healthy);
    let mut writer: Signal<Option<QueuedSignal<T>>> = use_signal(|| None);

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
                    signal.state.forward_to(
                        value_signal,
                        health_signal,
                        |arc| Ok(arc),
                    );
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
    }
}
