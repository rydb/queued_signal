use std::{any::{type_name, type_name_of_val}, time::Duration};

use bevy_ecs::{schedule::{IntoScheduleConfigs, ScheduleLabel, Schedules}, system::ScheduleSystem, world::{CommandQueue, World}};
use bevy_ecs::prelude::*;
use bevy_app::{ScheduleRunnerPlugin, plugin_group, prelude::*};
use dioxus_hooks::use_context;
use dioxus_signals::{ReadableExt, Signal};
use flume::{Receiver, Sender, unbounded};

#[cfg(feature = "query")]
pub mod query;

#[cfg(feature = "resource")]
pub mod resource;

#[cfg(feature = "asset")]
pub mod asset;

pub type CommandSender = Sender<CommandQueue>;
pub type CommandReceiver = Receiver<CommandQueue>;

#[derive(Resource)]
pub struct CommandQueueReciever {
    pub rx: CommandReceiver,
}

pub fn add_systems_through_world<T>(
    world: &mut World,
    schedule: impl ScheduleLabel,
    systems: impl IntoScheduleConfigs<ScheduleSystem, T>,
) {
    let type_name = type_name_of_val(&schedule);
    let mut schedules = world.get_resource_mut::<Schedules>().unwrap();
    let schedule = schedules.entry(schedule);

    schedule.add_systems(systems);
}


pub fn process_commands(
    command_rx: ResMut<CommandQueueReciever>,
    mut commands: Commands,
) {
    while let Ok(mut cmd) = command_rx.rx.try_recv() {
        commands.append(&mut cmd);
    }
}

/// command queue to send commands to bevy
#[derive(Clone, Resource)]
pub struct CommandQueueSender {
    pub tx: CommandSender,
}

impl CommandQueueSender {
    pub fn send_command<R: Send + 'static>(
        &self,
        make_command: impl FnOnce(Sender<R>) -> CommandQueue,
    ) -> Result<R, String> {
        let (response_tx, response_rx) = flume::bounded(1);
        let cmd = make_command(response_tx);
        self.tx.send(cmd).map_err(|err| format!("{}, {}, {}", err.to_string(), file!(), line!()))?;
        response_rx.recv_timeout(Duration::from_millis(100)).map_err(|err|  format!("{}, {}, {}", err.to_string(), file!(), line!()))
    }
}

/// bevy commands tx/rx channels need to be visible outside of the app thread in order to be inserted into dioxus, so this exists to create them
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
    pub fn tx(&self) -> CommandSender {
        self.tx.clone()
    }
}


/// plugin for mirroring state from the bevy world into the dioxus world
pub struct DioxusBevyMirrorPlugin { pub bevy_command_txrx: BevyCommandChannels}

impl Plugin for DioxusBevyMirrorPlugin {
    fn build(&self, app: &mut App) {        
        // let (cmd_tx, cmd_rx) = unbounded::<CommandQueue>();
        
        if !app.is_plugin_added::<ScheduleRunnerPlugin>() {
            app.add_plugins(ScheduleRunnerPlugin::default());
        }

        app
        .insert_resource(CommandQueueReciever {rx: self.bevy_command_txrx.rx.clone()})
        .insert_resource(CommandQueueSender { tx: self.bevy_command_txrx.tx.clone()})
        .add_systems(PreUpdate, process_commands)
        ;
        
    }
}

/// Dioxus accessible command queue for bevy commands
#[derive(Clone, Copy)]
pub struct BevyCommandsSignal {
    pub command_queue_sender: Signal<CommandQueueSender>,
}

/// Macro to ergonomically push and send a group of bevy commands to bevy
/// 
/// Usage:
/// ```rust
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

/// signal to get a convienience struct for sending commands
pub fn use_bevy_command_queue() -> BevyCommandsSignal {
    let command_queue = use_context::<CommandQueueSender>();

    BevyCommandsSignal { command_queue_sender: Signal::new(command_queue) }
}