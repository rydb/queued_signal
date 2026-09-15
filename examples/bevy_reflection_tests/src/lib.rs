//! Reflect-driven bevy mirroring tests.

use std::io;
use std::thread;

use bevy_app::{App, ScheduleRunnerPlugin};
use dioxus::LaunchBuilder;
use dioxus::prelude::*;
use dioxus_bevy_signals::{BevyCommandChannels, CommandQueueSender, DioxusBevyMirrorPlugin};
use dioxus_hooks::{use_context, use_context_provider};
use tracing_chrome::ChromeLayerBuilder;
use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry;
use tracing_subscriber::util::SubscriberInitExt;

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
    // Filter OUT noisy crate tracing
    // metadata.target() is the module path, e.g. "dioxus_core::scope_arena"
    let filter = filter_fn(|metadata| {
        !metadata.target().starts_with("dioxus_core")
            && !metadata.target().starts_with("dioxus_signals")
            && !metadata.target().starts_with("tungstenite")
            && !metadata.target().starts_with("bevy_ecs")
            && !metadata.target().starts_with("bevy_app")
            && !metadata.target().starts_with("warnings")
        // true
    });

    let stdout_layer = fmt::layer().with_writer(io::stdout);

    let (chrome_layer, _chrome_guard) = ChromeLayerBuilder::new()
        .file("./target/bevy_signal_tests_trace.json")
        .include_args(true)
        .build();

    let subscriber = registry()
        .with(filter)
        .with(stdout_layer)
        .with(chrome_layer);

    subscriber.init();

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
            query::QueryElevationTest {}
            // resource::ReflectElevationTest {}
        }
    }
}
