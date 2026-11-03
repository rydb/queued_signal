use std::time::Duration;

use bevy_app::prelude::*;
use bevy_ecs::prelude::*;
use bevy_time::{Real, Time, Timer, TimerMode};
use dioxus::prelude::*;
use dioxus_bevy_signals::resource::use_bevy_resource;
use dioxus_core::Element;

#[derive(Resource)]
pub struct ResourceTickTimer(pub Timer);

impl Default for ResourceTickTimer {
    fn default() -> Self {
        Self(Timer::new(
            Duration::from_millis(1000),
            TimerMode::Repeating,
        ))
    }
}

pub fn tick_resource_timer(mut timer: ResMut<ResourceTickTimer>, time: Res<Time<Real>>) {
    timer.0.tick(time.delta());
}

pub fn tick_resource_counter(timer: Res<ResourceTickTimer>, mut counter: ResMut<ResourceCounter>) {
    if timer.0.just_finished() {
        counter.value += 1;
    }
}

use crate::DioxusTestPlugin;

#[derive(Clone, Resource, Debug)]
pub struct ResourceCounter {
    pub value: i32,
}

/// Plugin for initializing the bevy resource synchronization test.
pub struct CounterResourceTestPlugin;

impl Plugin for CounterResourceTestPlugin {
    fn build(&self, app: &mut App) {
        app.add_systems(Update, (tick_resource_timer, tick_resource_counter))
            .insert_resource(ResourceCounter { value: 0 })
            .insert_resource(ResourceTickTimer::default());
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
    let counter = use_bevy_resource::<ResourceCounter, _, _>(|n| &n.value, |err| err);

    let onclick = move |_| {
        counter.mutate(|c| c.value += 10);
    };

    rsx! {
        div {
            style: "border: 2px solid black; background-color: #f0f0f0; padding: 16px; border-radius: 8px;",
            h2 {" bevy resource <-> dioxus"}

            h1 { "Counter: {counter}" }
            p { "Health: {counter.health()}" }
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
