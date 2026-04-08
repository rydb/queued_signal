use std::any::TypeId;
use std::marker::PhantomData;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use bevy_app::prelude::*;
use bevy_ecs::{prelude::*, schedule::ScheduleLabel, system::ScheduleSystem};
use dioxus::prelude::*;
use flume::{Receiver, Sender};
use queued_signal::signal::{QueuedResource, QueuedSignal, QueuedSignalHandle};


/// A command that can be sent from Dioxus to Bevy.
pub trait BevyCommand: Send + 'static {
    fn apply(self: Box<Self>, world: &mut World);
}

pub type CommandSender = Sender<Box<dyn BevyCommand>>;
pub type CommandReceiver = Receiver<Box<dyn BevyCommand>>;

/// Context passed to Dioxus. Holds a sender to the Bevy command queue.
#[derive(Clone)]
pub struct CommandQueueContext {
    tx: CommandSender,
}

impl CommandQueueContext {
    fn send_command<R: Send + 'static>(
        &self,
        make_command: impl FnOnce(Sender<R>) -> Box<dyn BevyCommand>,
    ) -> Option<R> {
        let (response_tx, response_rx) = flume::bounded(1);
        let cmd = make_command(response_tx);
        self.tx.send(cmd).ok()?;
        response_rx.recv_timeout(Duration::from_millis(100)).ok()
    }
}

#[derive(Resource, Clone)]
#[repr(transparent)]
pub struct BevyResourceClone<T: Send + Sync + 'static + Clone> {
    pub inner: QueuedResource<T>,
}

impl<T: Send + Sync + 'static + Clone> BevyResourceClone<T> {
    fn new_uninitialized() -> Self {
        Self {
            inner: QueuedResource {
                signal: QueuedSignal::new_uninitialized(),
            },
        }
    }
}

struct RequestBevyResource<T: Resource + Send + Sync + 'static + Clone> {
    response_tx: Sender<BevyResourceClone<T>>,
}

impl<T: Resource + Send + Sync + 'static + Clone> BevyCommand for RequestBevyResource<T> {
    fn apply(self: Box<Self>, world: &mut World) {
        let clone = if let Some(existing) = world.get_resource::<BevyResourceClone<T>>() {
            existing.clone()
        } else {
            let new_clone = BevyResourceClone::<T>::new_uninitialized();
            world.insert_resource(new_clone.clone());
            new_clone
        };

        if !clone.inner.signal.is_initialized() {
            if let Some(resource) = world.get_resource::<T>() {
                let initial = resource.clone();
                let _ = clone
                    .inner
                    .signal
                    .initialize(initial, Duration::from_millis(SYNC_INTERVAL_MS));
            }
        }

        if !world.contains_resource::<SyncMarker<T>>() {
            world.insert_resource(SyncMarker::<T>(PhantomData));
            add_systems_through_world(
                world,
                Update,
                (
                    sync_resource_to_signal::<T>,
                    sync_signal_to_resource::<T>,
                ),
            );
        }

        let _ = self.response_tx.send(clone);
    }
}

#[derive(Resource)]
struct SyncMarker<T>(PhantomData<T>);

const SYNC_INTERVAL_MS: u64 = 16;


fn sync_resource_to_signal<T: Resource + Send + Sync + 'static + Clone>(
    resource: Res<T>,
    clone: Res<BevyResourceClone<T>>,
) {
    if resource.is_changed() && clone.inner.signal.is_initialized() {
        let _ = clone.inner.signal.set_value(resource.clone());
    }
}

fn sync_signal_to_resource<T: Resource + Send + Sync + 'static + Clone>(
    clone: Res<BevyResourceClone<T>>,
    mut resource: ResMut<T>,
) {
    if let Ok(latest) = clone.inner.read() {
        *resource = (*latest).clone();
    }
}

pub fn add_systems_through_world<T>(
    world: &mut World,
    schedule: impl ScheduleLabel,
    systems: impl IntoScheduleConfigs<ScheduleSystem, T>,
) {
    let mut schedules = world.get_resource_mut::<Schedules>().unwrap();
    if let Some(schedule) = schedules.get_mut(schedule) {
        schedule.add_systems(systems);
    }
}



#[derive(Resource)]
struct CommandReceiverResource {
    rx: CommandReceiver,
}

fn process_commands(world: &mut World) {
    let rx = world.resource::<CommandReceiverResource>().rx.clone();
    while let Ok(cmd) = rx.try_recv() {
        cmd.apply(world);
    }
}

pub fn use_queued_resource<T: Resource + Send + Sync + 'static + Clone>(
) -> QueuedSignalHandle<T> {
    let ctx = use_context::<CommandQueueContext>();

    let resource_clone = use_hook(|| {
        ctx.send_command(|tx| Box::new(RequestBevyResource::<T> { response_tx: tx }))
            .expect("Failed to request resource from Bevy")
    });

    QueuedSignalHandle::new(resource_clone.inner)
}

#[derive(Clone, Resource)]
pub struct Counter {
    pub value: i32,
}

fn main() {
    let (cmd_tx, cmd_rx) = flume::unbounded::<Box<dyn BevyCommand>>();
    let (shutdown_tx, shutdown_rx) = flume::bounded(1);

    thread::spawn(move || {
        let mut app = App::new();
        app.insert_resource(Counter { value: 0 });
        app.insert_resource(CommandReceiverResource { rx: cmd_rx });
        app.add_systems(Update, process_commands);

        loop {
            if shutdown_rx.try_recv().is_ok() {
                break;
            }
            app.update();
            std::thread::sleep(Duration::from_millis(SYNC_INTERVAL_MS));
        }
    });

    let cmd_ctx = CommandQueueContext { tx: cmd_tx };

    dioxus::LaunchBuilder::new()
        .with_context(cmd_ctx)
        .launch(dx_app);

    let _ = shutdown_tx.send(());
}

fn dx_app() -> Element {
    let counter = use_queued_resource::<Counter>();
    let counter_value = counter.read().as_ref().map(|n| n.value).unwrap_or(0);

    let r = Arc::new(counter.clone());
    use_future(move || {
        let counter = r.clone();
        async move {
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                counter.mutate(|c| c.value += 1);
            }
        }
    });

    rsx! {
        div {
            h1 { "Counter: {counter_value}" }
            button {
                onclick: move |_| counter.mutate(|c| c.value += 100),
                "Increment"
            }
        }
    }
}