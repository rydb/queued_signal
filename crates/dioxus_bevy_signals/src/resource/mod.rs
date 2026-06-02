pub(crate) use crate::macros::*;
use bevy_app::prelude::*;
use bevy_ecs::world::CommandQueue;
use bevy_ecs::prelude::*;
use dioxus::prelude::*;
use dioxus_core::use_hook;
use dioxus_hooks::{use_context, use_signal};
use dioxus_signals::{ReadableExt, Signal};
use flume::Sender;
use parking_lot::Mutex;
use queued_signal::signal::{HealthStatus, QueuedSignal, WriterDriver};
use std::any::{TypeId, type_name};
use std::collections::HashSet;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use trait_set::trait_set;

use crate::{CommandQueueSender, add_systems_through_world};

pub type Result<T, E> = std::result::Result<T, E>;

trait_set! {
    /// Resource that is syncable with dioxus
    pub trait ResourceDioxusSync = Resource + Clone + Send + Sync + 'static;
}

#[derive(Resource)]
pub struct ResourceWriteDriver<T: ResourceDioxusSync>(pub Arc<Mutex<WriterDriver<T>>>);

struct RequestBevyResource<T: ResourceDioxusSync> {
    response_tx: Sender<QueuedSignal<T>>,
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
    pub signal: Signal<Option<Arc<R>>>,
    pub health: Signal<HealthStatus>,
    pub writer: Signal<QueuedSignal<R>>,
}

impl<R: Clone + Send + Sync + 'static> Copy for ResourceMirrorSignal<R> {}

impl<R: Clone + Send + Sync + 'static> ResourceMirrorSignal<R> {
    pub fn mutate<F>(&self, f: F)
    where
        F: Fn(&mut R) + Send + Sync + 'static,
    {
        self.writer.read().mutate(f);
    }

    pub fn mutate_set<F>(&self, f: F)
    where
        F: Fn(&mut R) + Send + Sync + 'static,
    {
        self.writer.read().mutate_set(f);
    }

    pub fn set_value(&self, value: Arc<R>) {
        self.writer.read().set_value(value);
    }

    pub fn health(&self) -> HealthStatus {
        *self.health.read()
    }

    pub fn read(&self) -> Option<Arc<R>> {
        let value = self.signal.deref();

        let value = value();
        value
    }
}

impl<T: Clone + Send + Sync + 'static> Deref for ResourceMirrorSignal<T> {
    type Target = Signal<Option<Arc<T>>>;
    fn deref(&self) -> &Self::Target {
        &self.signal
    }
}

pub fn use_bevy_resource<T>() -> ResourceMirrorSignal<T>
where
    T: ResourceDioxusSync,
{
    let ctx = use_context::<CommandQueueSender>();
    let signal = use_hook(|| {
        trace!("sending signal {}", type_name::<T>());
        ctx.send_command(|tx| {
            let mut command_queue = CommandQueue::default();
            let command = RequestBevyResource::<T> { response_tx: tx };
            command_queue.push(command);
            command_queue
        })
        .inspect_err(|err| warn!("{}", err))
    })
    .unwrap();

    let (value_signal, health_signal) = signal.use_hook();

    let signal = use_signal(|| signal);
    ResourceMirrorSignal {
        signal: value_signal,
        health: health_signal,
        writer: signal,
    }
}
