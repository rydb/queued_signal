use std::{any::{type_name, type_name_of_val}, time::Duration};

use bevy_ecs::{schedule::{IntoScheduleConfigs, ScheduleLabel, Schedules}, system::ScheduleSystem, world::{CommandQueue, World}};
use bevy_ecs::prelude::*;
use bevy_app::{ScheduleRunnerPlugin, plugin_group, prelude::*};
use flume::{Receiver, Sender, unbounded};

// pub mod query;
pub mod resource;

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
        ;
    }
}