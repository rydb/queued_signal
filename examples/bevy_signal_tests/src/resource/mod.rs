use bevy_app::prelude::*;
use bevy_ecs::prelude::*;
use dioxus_bevy_signals::resource::use_bevy_resource;
use dioxus_core::Element;
use dioxus::prelude::*;
use queued_signal::signal::HealthStatus;
use std::time::{Duration, Instant};


use crate::DioxusTestPlugin;


#[derive(Resource)]
pub struct TickTimer {
    last_tick: Instant,
}

impl Default for TickTimer {
    fn default() -> Self {
        Self {
            last_tick: Instant::now(),
        }
    }
}

#[derive(Clone, Resource, Debug)]
pub struct Counter {
    pub value: i32,
}

pub fn bevy_tick_counter(
    mut counter: ResMut<Counter>,
    mut timer: ResMut<TickTimer>,
) {
    if timer.last_tick.elapsed() >= Duration::from_millis(1000) {
        counter.value += 1;
        timer.last_tick = Instant::now();   
    }
}

/// plugin for intiailizing bevy resource <-> dioxus QueuedSignal syncronization test. 
pub struct CounterResourceTestPlugin;

impl Plugin for CounterResourceTestPlugin {
    fn build(&self, app: &mut App) {
        app
        .add_systems(Update, bevy_tick_counter)
        .insert_resource(Counter { value: 0 })
        .insert_resource(TickTimer::default());   // <
    }
}

/// setup for all resource signal interop tests 
#[derive(Clone)]
pub struct ResourceTestsPlugin;

impl Default for ResourceTestsPlugin {
    fn default() -> Self {
        Self {  }
    }
}


impl Plugin for ResourceTestsPlugin {
    fn build(&self, app: &mut App) {
        app
        .add_plugins(CounterResourceTestPlugin)
        ;
    }
}

impl DioxusTestPlugin for ResourceTestsPlugin {
    fn included_element(&self) -> Element {
        rsx! {
            CounterResource {  }
        }
    }
    
    fn register_plugin(&self, app: &mut App) {
        app.add_plugins(self.clone());
    }
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
    
    let onclick = move |_| {
        counter.mutate(|c| c.value += 10);
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

#[component]
pub fn ResourceDemos() -> Element {
    rsx! {
        CounterResource {  }
    }
}