use std::time::Instant;

use bevy_app::prelude::*;
use bevy_ecs::prelude::*;
use dioxus::{desktop::wry::cookie::time::Duration, prelude::*};
use dioxus_bevy_signals::query::{DioxusMirror, use_bevy_query};
use dioxus_hooks::use_signal;
use dioxus_signals::{ReadableExt, WritableExt};

use crate::DioxusTestPlugin;

#[derive(Component, Clone, Default)]
pub struct Greets {
    value: i32
}

#[derive(Resource)]
pub struct DebugPrintTimer {
    last_tick: Instant
}

pub fn add_ten_names(
    mut commands: Commands
) {
    commands.spawn(
        (
            Name::new("Name1"),
            Greets::default()
        )
    );
    commands.spawn(
        (
            Name::new("Name2"),
            Greets::default()
        )
    );
    commands.spawn(
        (
            Name::new("Name3"),
            Greets::default()
        )
    );
    commands.spawn(
        (
            Name::new("Name4"),
            Greets::default()
        )
    );
    commands.spawn(
        (
            Name::new("Name5"),
            Greets::default()
        )
    );
    commands.spawn(
        (
            Name::new("Name6"),
            Greets::default()
        )
    );
    commands.spawn(
        (
            Name::new("Name7"),
            Greets::default()
        )
    );
    commands.spawn(
        (
            Name::new("Name8"),
            Greets::default()

        )
    );
    commands.spawn(
        (
            Name::new("Name9"),
            Greets::default()
        )
    );
    commands.spawn(
        (
            Name::new("Name10"),
            Greets::default()
        )
    );
}

pub fn print_mirrors(
    mut timer: ResMut<DebugPrintTimer>,

    query: Query<(&DioxusMirror<Name>, &DioxusMirror<Greets>)>
) {
    if timer.last_tick.elapsed() >= core::time::Duration::from_millis(1000) {
        println!("matching query entries for mirrors: {}", query.iter().len());
        timer.last_tick = Instant::now();   

    }
}
pub struct TenNamesTestPlugin;

impl Plugin for TenNamesTestPlugin {
    fn build(&self, app: &mut App) {
        app
        .insert_resource(DebugPrintTimer {
            last_tick: Instant::now()
        })
        .add_systems(Startup, add_ten_names)
        .add_systems(PostUpdate, print_mirrors)
        ;
    }
}

#[derive(Clone, Default)]
pub struct QueryTestsPlugin;

impl Plugin for QueryTestsPlugin {
    fn build(&self, app: &mut App) {
        app
        .add_plugins(TenNamesTestPlugin)
        ;
    }
}

impl DioxusTestPlugin for QueryTestsPlugin {
    fn included_element(&self) -> Element {
        rsx! {
            TenNamesQuery {  }
        }
    }

    fn register_plugin(&self, app: &mut App) {
        app.add_plugins(self.clone());
    }
}

/// bevy query <-> dioxus sync test
#[component]
pub fn TenNamesQuery() -> Element {
    let names = use_bevy_query::<(Entity, &mut Name, &mut Greets), ()>();
    
    let mut names_list_str = use_signal(|| "".to_string());

    let onclick = move |evt | {
        println!("names total: {:#?}", names.iter().size_hint());
        *names_list_str.write() = "".to_string();
        for (e, name, greet) in names.iter() {
            *names_list_str.write() += name.value.read().as_ref();
        }
    };
    rsx! {
        h1 {
            "names list:"
        }
        h2 {
            {"current names:".to_string() + &names_list_str.read()}
        }
        button {
            onclick,
            "append latest names"
        }
    }
}