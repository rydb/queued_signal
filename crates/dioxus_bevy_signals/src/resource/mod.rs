use bevy_app::{ScheduleRunnerPlugin, prelude::*};
use bevy_ecs::world::CommandQueue;
use bevy_ecs::{prelude::*, schedule::ScheduleLabel, system::ScheduleSystem};
use bevy_log::prelude::*;
use dioxus_core::{Element, use_hook};
use dioxus_hooks::{use_context, use_future};
use dioxus_signals::*;
use dioxus::prelude::*;
use flume::{Receiver, Sender, unbounded};
use queued_signal::signal::{HealthStatus, QueuedSignal, QueuedSignalHandle, WriterDriver};
use std::any::{TypeId, type_name};
use std::collections::{HashSet};
use std::sync::Mutex;

use std::{result, thread};
use std::time::Duration;

use crate::CommandQueueSender;

pub type Result<T, E> = result::Result<T, E>;

#[derive(Resource)]
pub struct ResourceWriteDriver<T: Clone + Send + Sync + 'static>(pub Mutex<WriterDriver<T>>);

struct RequestBevyResource<T: Resource + Clone + Send + Sync + 'static> {
    response_tx: Sender<QueuedSignal<T>>,
}

#[derive(Resource)]
pub struct ResourceQueuedSignalMirror<T: Clone + Send + Sync + Resource>(pub QueuedSignal<T>);

#[derive(Resource, Default)]
pub struct RegisteredResourceSyncs(HashSet<TypeId>);

impl<T: Resource + Clone + Send + Sync + 'static> Command for RequestBevyResource<T> {
    fn apply(self, world: &mut World) {
        let signal_to_send = match world.get_resource::<ResourceQueuedSignalMirror<T>>() {
            Some(signal) => signal.0.clone(),
            None => {
                
                // put synced resources in registry for tracking
                world.get_resource_or_init::<RegisteredResourceSyncs>().0.insert(TypeId::of::<T>());

                let Some(resource) = world.get_resource::<T>().cloned() else {
                    warn!("Cannot initialize dioxus-bevy sync for {} as this resource does not exist at the time of this sync request.", type_name::<T>());
                    return
                };

                let driver = WriterDriver::new(resource.clone());
                let signal = QueuedSignal::from_parts(driver.queued_state.clone(), driver.command_tx.clone());
                world.insert_resource(ResourceWriteDriver(Mutex::new(driver)));

                // tick the resources 60 times a second (If uninitialized) by default to match standard FPS settings.
                world.get_resource_or_insert_with(|| ResourceSyncTickRate(Duration::from_millis(16)));

                add_systems_through_world(world, Update, drive_signal::<T>);
                let mut map = world.get_resource_or_init::<RegisteredResourceSyncs>();

                map.0.insert(TypeId::of::<T>());
                world.insert_resource(ResourceQueuedSignalMirror(signal.clone()));
                signal
            }
        };
        let _ = self.response_tx.send(signal_to_send);
    }
}

/// Minimum time to pass til queued mutations from QueuedSignal are published. The time to publish may be longer then this duration, but no shorter then this duration.
#[derive(Resource)]
pub struct ResourceSyncTickRate(Duration);

fn drive_signal<T: Clone + Send + Sync + 'static>(
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
    T: Resource + Clone + Send + Sync + 'static,
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