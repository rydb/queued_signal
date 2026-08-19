//! Reflect-driven bevy mirroring tests.

use std::thread;

use bevy_app::{App, ScheduleRunnerPlugin};
use dioxus::prelude::*;
use dioxus::LaunchBuilder;
use dioxus_bevy_signals::{BevyCommandChannels, CommandQueueSender, DioxusBevyMirrorPlugin};
use dioxus_hooks::{use_context, use_context_provider};

pub mod query;
pub mod resource;

/// Plugin wiring bevy and dioxus for the reflect tests.
#[derive(Clone)]
pub struct ReflectionTestsPlugin {
    cmd_channels: BevyCommandChannels,
}

impl Default for ReflectionTestsPlugin {
    fn default() -> Self {
        Self {
            cmd_channels: BevyCommandChannels::default(),
        }
    }
}

impl bevy_app::Plugin for ReflectionTestsPlugin {
    fn build(&self, app: &mut App) {
        app.add_plugins(bevy_time::TimePlugin);
        app.add_plugins(DioxusBevyMirrorPlugin {
            bevy_command_txrx: self.cmd_channels.clone(),
            ..Default::default()
        });
        app.add_plugins(query::QueryDynPlugin);
        app.add_plugins(resource::ResourceDynPlugin);
    }
}

/// Run the reflect tests in a headless bevy app plus a dioxus UI.
pub fn run_reflection_tests() {
    let plugin = ReflectionTestsPlugin::default();

    let bevy_plugin = plugin.clone();
    let bevy_thread = thread::spawn(move || {
        let mut app = App::new();
        app.add_plugins(ScheduleRunnerPlugin::default())
            .add_plugins(bevy_plugin)
            .run();
    });

    LaunchBuilder::new()
        .with_context(plugin)
        .launch(reflection_app);

    bevy_thread.join().unwrap();
}

/// Root dioxus element providing the command queue context.
pub fn reflection_app() -> Element {
    let plugin = use_context::<ReflectionTestsPlugin>();

    let command_queue_sender = CommandQueueSender {
        tx: plugin.cmd_channels.clone().tx(),
    };
    use_context_provider(|| command_queue_sender);

    rsx! {
        div {
            // query::QueryDynDemo {}
            resource::ReflectElevationTest {}
        }
    }
}
