//! Example: Using `use_bevy_query` to interact with Bevy ECS from Dioxus.

use bevy_app::App;
use bevy_ecs::prelude::*;
use bevy_transform::components::Transform;
use dioxus::prelude::*;
use flume::{Receiver, Sender};
use std::time::Duration;
use queued_signal::query_core::{
    use_bevy_query, BevyCommand, BevyQuerySyncPlugin, QueryCommand, QueryCommandSender,
    QueryRequestContext,
};

#[derive(Component, Clone, PartialEq)]
struct Marker;

fn run_bevy(
    cmd_tx: Sender<QueryCommand>,
    cmd_rx: Receiver<QueryCommand>,
    request_rx: Receiver<Box<dyn BevyCommand>>,
) {
    let mut app = App::new();
    app.add_plugins(BevyQuerySyncPlugin::new(cmd_rx, request_rx))
        .insert_resource(QueryCommandSender { tx: cmd_tx });

    for i in 0..100 {
        app.world_mut().spawn((
            Transform::from_xyz(i as f32 * 2.0, 0.0, 0.0),
            Marker,
            Name::new(format!("Cube {i}")),
        ));
    }

    app.run();
}

fn main() {
    let (cmd_tx, cmd_rx) = flume::unbounded::<QueryCommand>();
    let (request_tx, request_rx) = flume::unbounded::<Box<dyn BevyCommand>>();

    std::thread::spawn(move || run_bevy(cmd_tx, cmd_rx, request_rx));

    let query_ctx = QueryRequestContext { tx: request_tx };

    dioxus::launch(move || {
        rsx! { QueryExample {} }
    });
}

#[component]
fn QueryExample() -> Element {
    let cubes = use_bevy_query::<(Entity, &mut Transform), With<Marker>>();

    let cubes_clone = cubes.clone();
    use_future(move || async move {
        loop {
            tokio::time::sleep(Duration::from_millis(16)).await;
            for (entity, mut transform) in cubes_clone.iter() {
                transform.translation.y += 0.01;
                let _ = entity;
            }
        }
    });

    rsx! {
        div {
            h1 { "Bevy Query Demo" }
            p { "Number of cubes: {cubes.len()}" }
            p { "Cubes are moving upward automatically." }
        }
    }
}