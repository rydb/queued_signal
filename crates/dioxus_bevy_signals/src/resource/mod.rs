use bevy_app::prelude::*;
use bevy_ecs::world::CommandQueue;
use bevy_ecs::{prelude::*, schedule::ScheduleLabel, system::ScheduleSystem};
use bevy_log::prelude::*;
use dioxus_core::use_hook;
use dioxus_hooks::use_context;
use dioxus::prelude::*;
use flume::Sender;
use queued_signal::signal::{HealthStatus, QueuedSignal, QueuedSignalHandle, WriterDriver, SetValueOp};
use std::any::{TypeId, type_name};
use std::collections::HashSet;
use std::sync::Mutex;
use trait_set::trait_set;
use std::time::Duration;

use crate::CommandQueueSender;

pub type Result<T, E> = std::result::Result<T, E>;

trait_set! {
    /// Resource that is syncable with dioxus
    pub trait ResourceDioxusSync = Resource + Clone + Send + Sync + 'static;
}

#[derive(Resource)]
pub struct ResourceWriteDriver<T: ResourceDioxusSync>(pub Mutex<WriterDriver<T>>);

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
                world.get_resource_or_init::<RegisteredResourceSyncs>().0.insert(TypeId::of::<T>());

                let Some(resource) = world.get_resource::<T>().cloned() else {
                    warn!("Cannot initialize dioxus-bevy sync for {} as this resource does not exist at the time of this sync request.", type_name::<T>());
                    return;
                };

                let driver = WriterDriver::new(resource.clone());
                let signal = QueuedSignal::from_parts(
                    driver.queued_state.clone(),
                    driver.add_tx.clone(),
                    driver.set_tx.clone(),
                    driver.set_value_tx.clone(), 
                );
                world.insert_resource(ResourceWriteDriver(Mutex::new(driver)));

                // tick the resources 60 times a second (If uninitialized) by default to match standard FPS settings.
                world.get_resource_or_insert_with(|| ResourceSyncTickRate(Duration::from_millis(16)));

                add_systems_through_world(world, Update, drive_signal::<T>);
                // Also add the authoritative sync system, but **after** command processing.
                add_systems_through_world(world, PostUpdate, sync_mirror_to_resource::<T>.run_if(resource_changed::<T>));
                add_systems_through_world(world, PostUpdate, sync_resource_to_mirror::<T>.run_if(not(resource_changed::<T>)));
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
        mirror.bypass_change_detection().0.set_value(new_value.into());
        
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
    if let Ok(mut guard) = driver.0.lock().inspect_err(|err| warn!("UNABLE TO AQUIRE LOCK: {}", err)) {
        guard.tick(tick_rate.0);
    }
}

pub fn add_systems_through_world<T>(
    world: &mut World,
    schedule: impl ScheduleLabel,
    systems: impl IntoScheduleConfigs<ScheduleSystem, T>,
) {
    let mut schedules = world.get_resource_mut::<Schedules>().unwrap();
    let schedule = schedules.entry(schedule);
    schedule.add_systems(systems);
}

pub fn use_bevy_resource<T>() -> QueuedSignalHandle<T>
where
    T: ResourceDioxusSync,
{
    let ctx = use_context::<CommandQueueSender>();
    let signal = use_hook(|| {
        println!("sending signal");
        ctx.send_command(|tx| {
            let mut command_queue = CommandQueue::default();
            let command = RequestBevyResource::<T> { response_tx: tx };
            command_queue.push(command);
            command_queue
        }).inspect_err(|err| warn!("{}", err))
    }).unwrap();

    let (value_signal, health_signal) = signal.use_hook();

    QueuedSignalHandle {
        signal: value_signal,
        health: health_signal,
        writer: signal,
    }
}