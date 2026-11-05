use std::{fmt::Display, time::Duration};

use bevy_app::prelude::*;
use bevy_ecs::{prelude::*, world::CommandQueue};
use bevy_time::{Real, Time, Timer, TimerMode};
use dioxus::prelude::*;
use dioxus_bevy_signals::{
    push_and_send,
    query::{single::use_bevy_single, use_bevy_query},
    use_bevy_command_queue,
};
use dioxus_hooks::{use_memo, use_signal};
use dioxus_signals::{ReadableExt, WritableExt};

use crate::DioxusTestPlugin;

#[derive(Component, Clone, Default, PartialEq, PartialOrd, Eq, Ord, Debug)]
pub struct Greets {
    value: i32,
}

impl Display for Greets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}

#[derive(Component)]
pub struct Examined;

/// Marks an entity as part of the query toggle test for artifact cleanup.
#[derive(Component, Clone)]
pub struct ToggleTestMarker;

pub struct AddTenNames;

impl Command for AddTenNames {
    type Out = ();

    fn apply(self, world: &mut World) {
        world
            .commands()
            .spawn((Name::new("Name1"), Greets::default(), Examined));
        world
            .commands()
            .spawn((Name::new("Name2"), Greets::default()));
        world
            .commands()
            .spawn((Name::new("Name3"), Greets::default()));
        world
            .commands()
            .spawn((Name::new("Name4"), Greets::default()));
        world
            .commands()
            .spawn((Name::new("Name5"), Greets::default()));
        world
            .commands()
            .spawn((Name::new("Name6"), Greets::default()));
        world
            .commands()
            .spawn((Name::new("Name7"), Greets::default()));
        world
            .commands()
            .spawn((Name::new("Name8"), Greets::default()));
        world
            .commands()
            .spawn((Name::new("Name9"), Greets::default()));
        world.commands().spawn((
            Name::new("Name10_toggle_test"),
            Greets::default(),
            ToggleTestMarker,
        ));
    }
}

pub struct RemoveNames;

impl Command for RemoveNames {
    type Out = ();

    fn apply(self, world: &mut World) {
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

/// Tests removal of names from a query to verify reflection in the mirror.
pub fn remove_names(mut commands: Commands, names: Query<(Entity, &Name, &Greets), ()>) {
    for (entity, ..) in names {
        commands.entity(entity).despawn();
    }
}

pub struct TenNamesTestPlugin;

impl Plugin for TenNamesTestPlugin {
    fn build(&self, app: &mut App) {
        app.world_mut().commands().queue(AddTenNames {});
    }
}

#[derive(Clone, Default)]
pub struct QueryTestsPlugin;

impl Plugin for QueryTestsPlugin {
    fn build(&self, app: &mut App) {
        app.add_plugins(TenNamesTestPlugin);
        app.add_plugins(SingleQuerySetup);
    }
}

impl DioxusTestPlugin for QueryTestsPlugin {
    fn included_element(&self) -> Element {
        rsx! {
            TenNamesQuery {  }
            SingleQuery {}
        }
    }

    fn register_plugin(&self, app: &mut App) {
        app.add_plugins(self.clone());
    }
}

type ToggleableQueryData = (Entity, &'static mut Name, &'static mut ToggleTestMarker);
type ToggleableQueryFilter = ();

#[component]
pub fn ToggleableQuery() -> Element {
    let second_query = use_bevy_query::<ToggleableQueryData, ToggleableQueryFilter>();

    let second_query_str = use_memo(move || {
        let mut value = "".to_owned();
        let Some((_e, name, _)) = second_query.iter().next() else {
            return value;
        };
        value = format!("Name: {}", *name.read());
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

/// Bevy query <-> dioxus sync test.
#[component]
pub fn TenNamesQuery() -> Element {
    let names = use_bevy_query::<(Entity, &mut Name, &mut Greets), ()>();
    let bevy_commands = use_bevy_command_queue();

    let mut conditional_query_hidden = use_signal(|| true);

    let names_list_str = use_memo(move || {
        let mut value = "".to_string();
        let mut new_str_list = Vec::new();

        for (_e, name, greets) in names.iter() {
            new_str_list.push((name.read().clone(), greets.read().clone()))
        }
        new_str_list.sort();
        value += &format!("{:#?}", new_str_list);
        value
    });

    let greet_button_onclick = move |_evt| {
        for (_e, _n, greet) in names.iter() {
            greet.value.mutate(|n| n.value += 1);
        }
    };

    let add_names_onclick = move |_evt| {
        push_and_send!(bevy_commands, (AddTenNames {}));
    };
    let remove_names_on_click = move |_evt| push_and_send!(bevy_commands, (RemoveNames {}));

    let toggle_conditional_query = move |_evt| {
        *conditional_query_hidden.write() ^= true;
    };

    rsx! {
        div {
            style: "border: 2px solid black; background-color: #f0f0f0; padding: 16px; border-radius: 8px;",
            h1 {
                "bevy query <-> dioxus"
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
}

#[derive(Resource)]
pub struct QuerySingleTimer(pub Timer);

impl Default for QuerySingleTimer {
    fn default() -> Self {
        Self(Timer::new(
            Duration::from_millis(1000),
            TimerMode::Repeating,
        ))
    }
}

#[derive(Component, Clone)]
pub struct QuerySingleCounter {
    pub value: i32,
}

pub fn tick_query_single_timer(mut timer: ResMut<QuerySingleTimer>, time: Res<Time<Real>>) {
    timer.0.tick(time.delta());
}

pub fn tick_query_single_counter(
    timer: Res<QuerySingleTimer>,
    mut counter: Query<&mut QuerySingleCounter>,
) {
    let Ok(mut counter) = counter.single_mut() else {
        return;
    };

    if timer.0.just_finished() {
        counter.value += 1;
    }
}

#[derive(Component, Clone)]
pub struct NoMatch;

#[derive(Component, Clone)]
pub struct MoreThenOneMatch;

pub fn setup_singleton_test(mut commands: Commands) {
    // single match
    commands.spawn((QuerySingleCounter { value: 0 }, MoreThenOneMatch));

    // no matches (due to filter)
    commands.spawn(NoMatch);

    // more then one match
    commands.spawn(MoreThenOneMatch);
    commands.spawn(MoreThenOneMatch);
}

pub struct SingleQuerySetup;

impl Plugin for SingleQuerySetup {
    fn build(&self, app: &mut App) {
        app.insert_resource(QuerySingleTimer::default())
            .add_systems(Startup, setup_singleton_test)
            .add_systems(Update, (tick_query_single_timer, tick_query_single_counter));
    }
}

/// Test for the use_bevy_single mirror.
#[component]
pub fn SingleQuery() -> Element {
    let (_, counter) = use_bevy_single::<(Entity, &mut QuerySingleCounter), ()>();
    let increment_counter = move |_evt| {
        counter.mutate(|n| n.value += 10);
    };

    let (_, no_match) = use_bevy_single::<(Entity, &mut MoreThenOneMatch), With<NoMatch>>();
    let (_, more_then_one_match) = use_bevy_single::<(Entity, &mut MoreThenOneMatch), ()>();

    rsx! {
        div {
            style: "border: 2px solid black; background-color: #f0f0f0; padding: 16px; border-radius: 8px;",
            h1 {
                "Single query sync test"
            }
            div {
                h2 {
                    {format!("single match: {}", counter.use_display(|n| n.value.to_string()))}
                }
                button {
                    onclick: increment_counter,
                    "increment counter"
                }
            }

            h2 {
                {format!("no match: {}", no_match.use_display(|_n| "ERROR: there is a match".into()))}
            }
            h2 {
                {format!("more then one match: {}", more_then_one_match.use_display(|_n| "ERROR: There is more then one match".into()))}
            }
        }
    }
}
