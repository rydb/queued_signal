use std::thread;

use bevy_app::{App, Plugin};
use dioxus::{LaunchBuilder, prelude::rsx};
use dioxus_bevy_signals::{BevyCommandChannels, CommandQueueSender, DioxusBevyMirrorPlugin};
use dioxus_core::Element;
use dioxus_hooks::{use_context, use_context_provider};

use crate::asset::AssetTestPlugin;
#[cfg(feature = "query_tests")]
use crate::query::QueryTestsPlugin;
#[cfg(feature = "resource_tests")]
use crate::resource::ResourceTestsPlugin;

#[cfg(feature = "resource_tests")]
pub mod resource;

#[cfg(feature = "query_tests")]
pub mod query;

#[cfg(feature = "asset_tests")]
pub mod asset;

/// plugin that also includes a dioxus ui element relative to it
pub trait DioxusTestPlugin: Plugin + 'static {
    fn included_element(&self) -> Element;

    fn register_plugin(&self, app: &mut App);
}


/// plugin that setups infastructure to run all tests
#[derive(Clone)]
pub struct SignalTestsPlugin {
    /// call this to create minimal dioxus context to run plugins
    cmd_channels: BevyCommandChannels,

    /// collection of all plugins that hold tests
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
                list.push(Box::new(ResourceTestsPlugin::default()));

                #[cfg(feature = "query_tests")]
                list.push(Box::new(QueryTestsPlugin::default()));

                list.push(Box::new(AssetTestPlugin::default()));
                list
            },
        }
    }
}

impl Plugin for SignalTestsPlugin {
    fn build(&self, app: &mut bevy_app::App) {
        app
        .add_plugins(DioxusBevyMirrorPlugin { bevy_command_txrx: self.cmd_channels.clone() })
        ;

        let plugins = (self.test_plugin_list)();

        for plugin in plugins {
            plugin.register_plugin(app);
        }
    }
}

pub fn signal_tests_app() -> Element{
    let tests_plguin = use_context::<SignalTestsPlugin>();
    
    
    let command_queue_sender = CommandQueueSender {
        tx: tests_plguin.cmd_channels.clone().tx()
    };

    use_context_provider(|| command_queue_sender);
    
    let mut elements = Vec::new();

    for plugin in (tests_plguin.test_plugin_list)(){
        elements.push(plugin.included_element());
    }
    rsx! { 
        for element in elements {
            {element}
        }
    }
}

/// runs signal tests. Tests are enabled by example feature flags. see src for /bevy_signal_tests for test impls.
pub fn run_signal_tests() {
    let signal_tests_plguin = SignalTestsPlugin::default();
    
    let r = signal_tests_plguin.clone();
    let bevy_thread = thread::spawn(move || {
        let mut app = App::new();
        app
        .add_plugins(r)
        .run();
    } );


    LaunchBuilder::new()
    .with_context(signal_tests_plguin)
    .launch(signal_tests_app);

    bevy_thread.join().unwrap();
}