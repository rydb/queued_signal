use std::{io, rc::Rc, thread, time::Duration};

use bevy_app::{App, Plugin, ScheduleRunnerPlugin};
use bevy_ecs::prelude::*;
use bevy_log::debug;
use bevy_time::{Timer, TimerMode};
use dioxus::prelude::*;
use dioxus::{LaunchBuilder, prelude::rsx};
use dioxus_bevy_signals::{BevyCommandChannels, CommandQueueSender, DioxusBevyMirrorPlugin};
use dioxus_hooks::{use_context, use_context_provider};
use tracing_chrome::ChromeLayerBuilder;
use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::{fmt, prelude::*, registry, util::SubscriberInitExt};

cfg_if::cfg_if! {
    if #[cfg(feature = "resource_tests")] {
        use crate::resource::ResourceTestsPlugin;
        pub mod resource;
    }
}

cfg_if::cfg_if! {
    if #[cfg(feature = "query_tests")] {
        use crate::query::QueryTestsPlugin;
        pub mod query;

    }
}

cfg_if::cfg_if! {
    if #[cfg(feature = "asset_tests")] {
        pub mod asset;
        use crate::asset::AssetTestPlugin;
    }
}

/// Plugin that includes a dioxus UI element.
pub trait DioxusTestPlugin: Plugin + 'static {
    fn included_element(&self) -> Element;

    fn register_plugin(&self, app: &mut App);
}

/// Shared tick timer resource driven by time delta.
#[derive(Resource)]
pub struct TickTimer(pub Timer);

impl Default for TickTimer {
    fn default() -> Self {
        Self(Timer::new(Duration::from_secs(1), TimerMode::Repeating))
    }
}

/// Plugin that sets up the infrastructure for running all tests.
#[derive(Clone)]
pub struct SignalTestsPlugin {
    /// Creates a minimal dioxus context for running test plugins.
    cmd_channels: BevyCommandChannels,

    /// Collection of all plugins containing tests.
    pub test_plugin_list: fn() -> Vec<Box<dyn DioxusTestPlugin>>,
}

impl Default for SignalTestsPlugin {
    fn default() -> Self {
        let cmd_channels = BevyCommandChannels::default();
        Self {
            cmd_channels,
            test_plugin_list: || {
                let mut list: Vec<Box<dyn DioxusTestPlugin>> = Vec::default();

                #[cfg(feature = "resource_tests")]
                {
                    debug!("resource tests added");
                    list.push(Box::new(ResourceTestsPlugin::default()));
                }

                #[cfg(feature = "query_tests")]
                {
                    debug!("query tests added");
                    list.push(Box::new(QueryTestsPlugin::default()));
                }

                #[cfg(feature = "asset_tests")]
                {
                    debug!("asset_tests added");
                    list.push(Box::new(AssetTestPlugin::default()));
                }
                list
            },
        }
    }
}

impl Plugin for SignalTestsPlugin {
    fn build(&self, app: &mut bevy_app::App) {
        app.add_plugins(bevy_time::TimePlugin);
        app.insert_resource(TickTimer::default());
        app.add_plugins(DioxusBevyMirrorPlugin {
            bevy_command_txrx: self.cmd_channels.clone(),
            ..Default::default()
        });

        let plugins = (self.test_plugin_list)();

        for plugin in plugins {
            plugin.register_plugin(app);
        }
    }
}

pub fn run_signal_tests() {
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

    let signal_tests_plugin = SignalTestsPlugin::default();

    let r = signal_tests_plugin.clone();
    let bevy_thread = thread::spawn(move || {
        let mut app = App::new();
        app
            // run app in headless mode
            .add_plugins(ScheduleRunnerPlugin::default())
            .add_plugins(r)
            .run();
    });

    LaunchBuilder::new()
        .with_context(signal_tests_plugin)
        .launch(signal_tests_app);

    bevy_thread.join().unwrap();
}

#[derive(Clone)]
pub struct AppDocument(pub Signal<Rc<dyn dioxus_document::Document>>);

pub fn signal_tests_app() -> Element {
    let tests_plugin = use_context::<SignalTestsPlugin>();

    let command_queue_sender = CommandQueueSender {
        tx: tests_plugin.cmd_channels.clone().tx(),
    };

    let _document = use_context_provider(|| AppDocument(Signal::new(dioxus_document::document())));

    use_context_provider(|| command_queue_sender);

    let mut elements = Vec::new();
    for plugin in (tests_plugin.test_plugin_list)() {
        elements.push(plugin.included_element());
    }

    rsx! {
        div {
            div { style: "margin-top: 32px; padding: 16px;", // space for the fixed overlay
                for element in elements {
                    {element}
                }
            }
        }
    }
}
