//! Tests for queued_signal bevy integration


use bevy_app::{ScheduleRunnerPlugin, prelude::*};
use bevy_ecs::world::CommandQueue;
use bevy_ecs::{prelude::*, schedule::ScheduleLabel, system::ScheduleSystem};
use bevy_log::prelude::*;
use dioxus_bevy_signals::resource::use_bevy_resource;
use dioxus_bevy_signals::{BevyCommandChannels, CommandQueueSender, DioxusBevyMirrorPlugin};
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


#[derive(Clone, Resource, Debug)]
pub struct Counter {
    pub value: i32,
}

fn main() {
    
    let cmd_channels = BevyCommandChannels::default();
    // Spawn Bevy headless in a background thread

    let r = cmd_channels.clone();
    let bevy_handle = thread::spawn(move || {
        // let _epoch_guard = crossbeam_epoch::pin();

        let mut app = App::new();
        
        app
        .add_plugins(DioxusBevyMirrorPlugin {bevy_command_txrx: r} )
        .insert_resource(Counter { value: 0 });

        app.run()
    });

    // Run Dioxus on the main thread
    // let cmd_ctx = CommandQueueSender { tx: cmd_tx };
    dioxus::LaunchBuilder::new()
        .with_context(CommandQueueSender {
            tx: cmd_channels.tx()
        })
        .launch(dx_app);

    bevy_handle.join().unwrap();
}

/// bevy resource <-> dioxus sync test
#[component]
pub fn CounterResource() -> Element {
    let counter = use_bevy_resource::<Counter>();


    let value = counter().map(|c| c.value).unwrap_or(0);

    let health = counter.health();
    let health_text = match health {
        HealthStatus::Healthy => "Healthy",
        HealthStatus::Degraded { .. } => "Degraded",
        HealthStatus::Stalled { .. } => "Stalled",
    };
    let r = counter.clone();
    use_future(move ||{
        let value = r.clone();
        async move {
            loop {
                value.mutate(|c| c.value += 1);
                let _ = tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    });

    let onclick = move |_|  {
        counter.mutate(|c| c.value += 1);
    };

    rsx! {
        div {
            style: "border: 2px solid black; background-color: #f0f0f0; padding: 16px; border-radius: 8px;",
            h2 {" bevy resource <-> dioxus sync"}
            
            h1 { "Counter: {value}" }
            p { "Health: {health_text}" }
            button {
                onclick,
                "Increment"
            }
        }
    }
}

fn dx_app() -> Element {


    rsx! {
        div {
            CounterResource {  } 
        }
    }
}