use bevy_app::prelude::*;
use bevy_ecs::prelude::*;
use bevy_time::{Real, Time};
use dioxus::prelude::*;
use dioxus_bevy_signals::resource::use_bevy_resource;
use dioxus_core::Element;
use dioxus_hooks::use_memo;
use queued_signal::state::HealthStatus;

use crate::{DioxusTestPlugin, TickTimer};

#[derive(Clone, Resource, Debug)]
pub struct Counter {
    pub value: i32,
}

pub fn bevy_tick_counter(
    mut counter: ResMut<Counter>,
    mut timer: ResMut<TickTimer>,
    time: Res<Time<Real>>,
) {
    timer.0.tick(time.delta());
    if timer.0.just_finished() {
        counter.value += 1;
    }
}

/// Plugin for initializing the bevy resource synchronization test.
pub struct CounterResourceTestPlugin;

impl Plugin for CounterResourceTestPlugin {
    fn build(&self, app: &mut App) {
        app.add_systems(Update, bevy_tick_counter)
            .insert_resource(Counter { value: 0 });
    }
}

/// Setup for all resource signal interop tests.
#[derive(Clone, Default)]
pub struct ResourceTestsPlugin;

impl Plugin for ResourceTestsPlugin {
    fn build(&self, app: &mut App) {
        app.add_plugins(CounterResourceTestPlugin);
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

/// Bevy resource and dioxus sync test.
#[component]
pub fn CounterResource() -> Element {
    let counter = use_bevy_resource::<Counter>();

    let value = use_memo(move || counter.read_ok(|c| c.value).unwrap_or(0));

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
            h2 {" bevy resource <-> dioxus"}

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
