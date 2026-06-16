#![warn(missing_docs)]
//! Crate for mirroring bevy state to and from dioxus using QueuedSignals.

use std::{any::type_name_of_val, sync::Arc, time::Duration};

use bevy_app::{ScheduleRunnerPlugin, prelude::*};
use bevy_ecs::prelude::*;
use bevy_ecs::{
    schedule::{IntoScheduleConfigs, Schedule, ScheduleLabel, Schedules},
    system::ScheduleSystem,
    world::{CommandQueue, World},
};
use dioxus_hooks::use_context;
use dioxus_signals::Signal;
use flume::{Receiver, Sender, unbounded};
use tokio::sync::oneshot;

pub(crate) mod macros;
pub(crate) use crate::macros::{debug, error, info, trace, warn};

/// Fixed-timestep schedules for dioxus-bevy synchronization.
pub mod schedules;

/// Bevy query mirroring (feature = "query").
#[cfg(feature = "query")]
pub mod query;

/// Bevy resource mirroring (feature = "resource").
#[cfg(feature = "resource")]
pub mod resource;

/// Bevy asset mirroring (feature = "asset").
#[cfg(feature = "asset")]
pub mod asset;

/// Sender for bevy command queues across thread boundaries.
pub type CommandSender = Sender<CommandQueue>;
/// Receiver for bevy command queues across thread boundaries.
pub type CommandReceiver = Receiver<CommandQueue>;

/// Bevy resource holding the receiving end of the command channel.
#[derive(Resource)]
pub struct CommandQueueReceiver {
    /// The flume receiver for incoming command queues.
    pub rx: CommandReceiver,
}
/// Adds a system to bevy through `&mut World` instead of `&mut App`.
///
/// !!! Do not add systems that run before `PreUpdate` via this method, or they will not run. !!!
pub fn add_systems_through_world<T>(
    world: &mut World,
    schedule: impl ScheduleLabel,
    systems: impl IntoScheduleConfigs<ScheduleSystem, T>,
) {
    let mut schedules = world.get_resource_mut::<Schedules>().unwrap();

    // schedules that don't work when added via world.
    let interned = schedule.intern();
    if interned == PreUpdate.intern() || interned == PreStartup.intern() {
        panic!("
                Systems that run before update do not run when added through world.
                Re-structure your command plugin to not use systems that run before update in the mean time.
                TODO: fix this
                
                Offending Schedule:
                {schedule:#?}

                Offending system set:
                {systems:#?}
                ",
            schedule = format!("{:#?}", interned),
            systems = type_name_of_val(&systems)
        );
    }

    let schedule = schedules.entry(schedule);

    schedule.add_systems(systems);
}

/// Sending end of the bevy command channel, usable from dioxus hooks.
#[derive(Clone, Resource)]
pub struct CommandQueueSender {
    /// The flume sender for outgoing command queues.
    pub tx: CommandSender,
}

impl CommandQueueSender {
    /// Synchronously sends a command to bevy and blocks for the response, with a
    /// 10 second timeout. Prefer [`send_command_async`] in async contexts, such as
    /// dioxus hooks, to avoid blocking the runtime.
    pub fn send_command<R: Send + 'static>(
        &self,
        make_command: impl FnOnce(Sender<R>) -> CommandQueue,
    ) -> Result<R, String> {
        let (response_tx, response_rx) = flume::bounded(1);
        let cmd = make_command(response_tx);
        self.tx
            .send(cmd)
            .map_err(|err| format!("{}, {}, {}", err.to_string(), file!(), line!()))?;
        response_rx
            .recv_timeout(Duration::from_millis(10000))
            .map_err(|err| format!("{}, {}, {}", err.to_string(), file!(), line!()))
    }

    /// Async variant of [`send_command`]. Uses a oneshot channel so the caller can
    /// `.await` the response without blocking any OS thread. No explicit timeout is
    /// used; the oneshot resolves once bevy processes the command, typically on the
    /// next frame.
    pub async fn send_command_async<R: Send + 'static>(
        &self,
        make_command: impl FnOnce(oneshot::Sender<R>) -> CommandQueue,
    ) -> Result<R, String> {
        let (response_tx, response_rx) = oneshot::channel();
        let cmd = make_command(response_tx);
        self.tx.send(cmd).map_err(|err| format!("{}", err))?;

        response_rx
            .await
            .map_err(|_| format!("sender dropped before responding"))
    }
}

/// Bevy command channels that can be shared with dioxus for cross-thread communication.
#[derive(Clone)]
pub struct BevyCommandChannels {
    tx: CommandSender,
    rx: CommandReceiver,
}

impl Default for BevyCommandChannels {
    fn default() -> Self {
        let (tx, rx) = unbounded::<CommandQueue>();
        Self { tx, rx }
    }
}
impl BevyCommandChannels {
    /// Returns a clone of the sender.
    pub fn tx(&self) -> CommandSender {
        self.tx.clone()
    }
}

/// Plugin for mirroring state from the bevy world into dioxus.
pub struct DioxusBevyMirrorPlugin {
    /// The bidirectional command channels for bevy-dioxus communication.
    pub bevy_command_txrx: BevyCommandChannels,
    /// The target frames per second for the dioxus sync schedules.
    /// Determines how many times per second the sync systems run.
    /// Default: 60.
    pub dioxus_sync_fps: u32,
}

impl Default for DioxusBevyMirrorPlugin {
    fn default() -> Self {
        Self {
            bevy_command_txrx: Default::default(),
            dioxus_sync_fps: 60,
        }
    }
}

impl Plugin for DioxusBevyMirrorPlugin {
    fn build(&self, app: &mut App) {
        use schedules::*;

        // Register the dioxus sync fixed-timestep schedules.
        app.add_schedule(Schedule::new(DioxusSyncMain))
            .add_schedule(Schedule::new(DioxusSyncPreUpdate))
            .add_schedule(Schedule::new(DioxusSyncUpdate))
            .add_schedule(Schedule::new(DioxusSyncPostUpdate))
            .add_schedule(Schedule::new(DioxusSyncLast))
            .insert_resource(DioxusSyncConfig::from_fps(self.dioxus_sync_fps))
            .insert_resource(DioxusSyncAccumulator::default())
            .init_resource::<DioxusSyncMainScheduleOrder>()
            .add_systems(PostUpdate, DioxusSyncMain::run_dioxus_sync_main);

        app.insert_resource(CommandQueueReceiver {
            rx: self.bevy_command_txrx.rx.clone(),
        })
        .insert_resource(CommandQueueSender {
            tx: self.bevy_command_txrx.tx.clone(),
        });
        // Process commands inside the dioxus sync main runner so that
        // dynamically registered systems from command processing are available
        // for the current frame.
    }
}

/// Dioxus-accessible command queue for sending commands to bevy.
#[derive(Clone, Copy)]
pub struct BevyCommandsSignal {
    /// Signal holding the command queue sender.
    pub command_queue_sender: Signal<CommandQueueSender>,
}

/// Macro for pushing and sending a group of commands to bevy.
///
/// Usage:
/// ```ignore
/// push_and_send!(bevy_command_signal: BevyCommandsSignal, (command1_, command_2, .. command_n))
/// ```
#[macro_export]
macro_rules! push_and_send {
    ($signal:expr, ($($cmd:expr),* $(,)?)) => {{
        let mut q = CommandQueue::default();
        let tx = $signal.command_queue_sender.clone();
        $( q.push($cmd); )*
        let _ = tx.read().tx.send(q).inspect_err(|err| println!("couldn't send command_queue to bevy {}", err));
    }};
}

/// Signal providing a convenience struct for sending commands to bevy.
pub fn use_bevy_command_queue() -> BevyCommandsSignal {
    let command_queue = use_context::<CommandQueueSender>();

    BevyCommandsSignal {
        command_queue_sender: Signal::new(command_queue),
    }
}

