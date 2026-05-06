use std::{fmt::Display, time::Instant};

use bevy_app::prelude::*;
use bevy_ecs::{prelude::*, world::CommandQueue};
use dioxus::{desktop::wry::cookie::time::Duration, prelude::*};
use dioxus_bevy_signals::{CommandQueueSender, push_and_send, query::{DioxusComponentSync, DioxusMirror, MirrorQueryData, use_bevy_query}, use_bevy_command_queue};
use dioxus_hooks::{use_context, use_memo, use_signal};
use dioxus_signals::{ReadableExt, WritableExt};

use crate::DioxusTestPlugin;

#[derive(Component, Clone, Default, PartialEq, PartialOrd, Eq, Ord, Debug)]
pub struct Greets {
    value: i32
}

impl Display for Greets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}

#[derive(Resource)]
pub struct DebugPrintTimer {
    last_tick: Instant
}

#[derive(Resource)]
pub struct ToggleQueryDebugTimer {
    last_tick: Instant,
}

#[derive(Component)]
pub struct Examined;

/// marks entity as a part of the query toggle on/off test for query artifact cleanup
#[derive(Component, Clone)]
pub struct ToggleTestMarker;

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
                Name::new("Name10_toogle_test"),
                Greets::default(),
                ToggleTestMarker,
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
        .insert_resource(ToggleQueryDebugTimer {
            last_tick: Instant::now()
        })
        .add_systems(PostUpdate, print_mirrors)
        .add_systems(PostUpdate, toggleable_query_mirror_output)
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

type ToggleableQueryData = (Entity, &'static mut Name, &'static mut ToggleTestMarker);
type ToggleableQueryFilter = ();

pub fn toggleable_query_mirror_output(
    query: Query<<ToggleableQueryData as MirrorQueryData>::MirrorSignalsQueryDataImMut, ToggleableQueryFilter>,
    mut timer: ResMut<ToggleQueryDebugTimer>,
) {
    if timer.last_tick.elapsed() >= core::time::Duration::from_millis(1000) {
        timer.last_tick = Instant::now();   

        println!("toggleable query dioxus mirrors: {}", query.count())
    }
}

#[component]
pub fn ToggleableQuery() -> Element {
    let second_query = use_bevy_query::<ToggleableQueryData, ToggleableQueryFilter>();
    let second_query = use_signal(|| second_query.clone());

    let second_query_str = use_memo(move || {
        let mut value = "".to_owned();
        let Some((_e, 
            name, 
            _
        )) = second_query.read().iter().next() else {
            return value
        };
        value = format!("Name: {}", **name.read());
        value
    });

    rsx! {
        h1 {
            "second query active"
        }
        h2 {
            "query: "
        }
        h3 {
            {second_query_str}
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

        for (e, name, greets) in names.read().iter() {
            new_str_list.push((name.read().clone(), greets.read().clone()))
        }
        let mut new_str_list = new_str_list.iter().map(|n| (n.0.as_ref(), n.1.as_ref())).collect::<Vec<_>>();
        new_str_list.sort();
        value += &format!("{:#?}", new_str_list);
        value
    });
    
    let greet_button_onclick = move |_evt | {
        println!("sending greet");

        for (_e, _n, greet) in names.read().iter() {
            println!("greeting");
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
            {
                if *conditional_query_hidden.read() {
                    rsx! {
                        div {
                            
                        }
                    }
                } else {
                    rsx! {
                        ToggleableQuery {}
                    }
                }
            }
        }
    }

}