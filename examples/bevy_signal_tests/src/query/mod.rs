use std::time::Instant;

use bevy_app::prelude::*;
use bevy_ecs::{prelude::*, world::CommandQueue};
use dioxus::{desktop::wry::cookie::time::Duration, prelude::*};
use dioxus_bevy_signals::{CommandQueueSender, push_and_send, query::{DioxusComponentSync, DioxusMirror, use_bevy_query}, use_bevy_command_queue};
use dioxus_hooks::{use_context, use_memo, use_signal};
use dioxus_signals::{ReadableExt, WritableExt};

use crate::DioxusTestPlugin;

#[derive(Component, Clone, Default, PartialEq, PartialOrd, Eq, Ord, Debug)]
pub struct Greets {
    value: i32
}

#[derive(Resource)]
pub struct DebugPrintTimer {
    last_tick: Instant
}

#[derive(Component)]
pub struct Examined;

#[derive(Component)]
pub struct Marker;

pub struct AddTenNames;

impl Command for AddTenNames {
    fn apply(self, world: &mut World) -> () {
        world.commands().spawn(
            (
                Name::new("Name1"),
                Greets::default(),
                Examined
            )
        );
        world.commands().spawn(
            (
                Name::new("Name2"),
                Greets::default(),
            )
        );
        world.commands().spawn(
            (
                Name::new("Name3"),
                Greets::default()
            )
        );
        world.commands().spawn(
            (
                Name::new("Name4"),
                Greets::default()
            )
        );
        world.commands().spawn(
            (
                Name::new("Name5"),
                Greets::default()
            )
        );
        world.commands().spawn(
            (
                Name::new("Name6"),
                Greets::default()
            )
        );
        world.commands().spawn(
            (
                Name::new("Name7"),
                Greets::default()
            )
        );
        world.commands().spawn(
            (
                Name::new("Name8"),
                Greets::default()

            )
        );
        world.commands().spawn(
            (
                Name::new("Name9"),
                Greets::default()
            )
        );
        world.commands().spawn(
            (
                Name::new("Name10"),
                Greets::default()
            )
        );
    }
}

pub struct RemoveNames;

impl Command for RemoveNames {
    fn apply(self, world: &mut World) -> () {
        let mut names = world.query::<(Entity, &Name, &Greets)>();

        let mut remove_list = Vec::new();
        for (e, ..) in names.iter(world) {
            remove_list.push(e);
        }
        
        for e in remove_list {
            world.commands().entity(e).despawn();
        }
    }
}

/// test removing names from query to see if they reflect in QueryMirror
pub fn remove_names(
    mut commands: Commands,
    names: Query<(Entity, &Name, &Greets), ()>
) {
    for (entity, ..) in names {
        commands.entity(entity).despawn();
    }
}

pub fn print_mirrors(
    mut timer: ResMut<DebugPrintTimer>,

    query: Query<(&DioxusMirror<Name>, &DioxusMirror<Greets>), With<Examined>>
) {
    if timer.last_tick.elapsed() >= core::time::Duration::from_millis(1000) {
        // println!("matching query entries for mirrors: {}", query.iter().len());
        timer.last_tick = Instant::now();   

        let Some((a, b)) = query.iter().next() else {
            return
        };
        let debug_state = format!("{:#?}", a);

        std::fs::write("./target/ten_names_first_debug.txt", debug_state).unwrap();
    }
}
pub struct TenNamesTestPlugin;

impl Plugin for TenNamesTestPlugin {
    fn build(&self, app: &mut App) {
        app.world_mut().commands().queue(AddTenNames {});
        app
        .insert_resource(DebugPrintTimer {
            last_tick: Instant::now()
        })
        .add_systems(PostUpdate, print_mirrors)
        // .add_systems(PostUpdate, debug_dioxus_mirror_changed::<Name, Greets>);
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

#[component]
pub fn ToggleableQuery() -> Element {
    let second_query = use_bevy_query::<(Entity, &mut Name, &mut Greets), ()>();

    rsx! {
        h1 {
            "second query active"
        }
    }
}


/// bevy query <-> dioxus sync test
#[component]
pub fn TenNamesQuery() -> Element {
    let names = use_bevy_query::<(Entity, &mut Name, &mut Greets), ()>();
    let bevy_commands = use_bevy_command_queue();

    let mut conditional_query_hidden = use_signal(|| true);

    let names = use_signal(|| names.clone());
    
    let names_list_str = use_memo(move || {
        let mut value = "".to_string();

        println!("changed names list str");
        let mut new_str_list = Vec::new();
        for (e, name, greet) in names.read().iter() {
            let name_arc = name.value. read().clone();
            let greet_arc = greet.value.read().clone();

            new_str_list.push((name_arc, greet_arc))
        }
        let mut new_str_list = new_str_list.iter().map(|n| (n.0.as_ref(), n.1.as_ref())).collect::<Vec<_>>();
        new_str_list.sort();
        value += &format!("{:#?}", new_str_list);
        value
    });
    
    let greet_button_onclick = move |_evt | {
        for (_e, _n, greet) in names.read().iter() {
            println!("sending greet");
            greet.value.mutate(|n| n.value += 1);
        }
    };
    
    let add_names_onclick = move |evt | {
        push_and_send!(bevy_commands, (AddTenNames {}));
    };
    let remove_names_on_click = move |evt | {
        push_and_send!(bevy_commands, (RemoveNames {}))
    };

    let toggle_conditional_query = move |evt | {
        *conditional_query_hidden.write() ^= true;
    };

    rsx! {
        h1 {
            "bevy query <-> dioxus sync test"
        }
        h1 {
            "names list:"
        }
        h2 {
            {"current names:".to_string() + &names_list_str.read()}
        }
        button {
            onclick: greet_button_onclick,
            "greet"
        }
        button {
            onclick: add_names_onclick,
            "add ten names"
        }
        button {
            onclick: remove_names_on_click,
            "remove names"
        }
        button {
            onclick: toggle_conditional_query,
            "toggle conditional query",
        }
        div {
            hidden: conditional_query_hidden,
            ToggleableQuery {}
        }
    }

}