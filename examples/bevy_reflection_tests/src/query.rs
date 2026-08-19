//! Typed and untyped query demo with an elevation button.

use std::any::TypeId;

use bevy_app::prelude::*;
use bevy_ecs::prelude::*;
use bevy_ecs::world::CommandQueue;
use bevy_reflect::Reflect;
use dioxus::prelude::*;
use dioxus_bevy_signals::CommandQueueSender;
use dioxus_bevy_signals::query::use_bevy_query;
use dioxus_bevy_signals::reflect::query::{ElevateReflectQuery, use_bevy_query_dyn};
use dioxus_hooks::{use_context, use_memo, use_signal};

#[derive(Component, Clone, Reflect, Default)]
#[reflect(Component)]
pub struct Name(pub String);

#[derive(Component, Clone, Reflect, Default)]
#[reflect(Component)]
pub struct Transform(pub f32);

/// Plugin spawning entities and registering reflect types.
pub struct QueryDynPlugin;

impl Plugin for QueryDynPlugin {
    fn build(&self, app: &mut App) {
        app.register_type::<Name>();
        app.register_type::<Transform>();
        app.world_mut().commands().spawn((Name("A".into()), Transform(1.0)));
        app.world_mut().commands().spawn((Name("B".into()), Transform(2.0)));
    }
}

/// Demo component showing both query forms and an upgrade button.
#[component]
pub fn QueryDynDemo() -> Element {
    let typed = use_bevy_query::<(Entity, &mut Name, &mut Transform), ()>();
    let dyn_query = use_bevy_query_dyn(["Name", "Transform"]);

    let command_queue = use_context::<CommandQueueSender>();
    let mut upgraded = use_signal(|| false);

    let typed_count = use_memo(move || typed.iter().count());
    let dyn_count = use_memo(move || match &*dyn_query.read() {
        Ok(map) => map.len(),
        Err(_) => 0,
    });

    rsx! {
        div {
            h2 { "typed query entities: {typed_count}" }
            h2 { "untyped query entities: {dyn_count}" }
            button {
                onclick: move |_| {
                    upgraded.set(true);
                    let mut queue = CommandQueue::default();
                    queue.push(ElevateReflectQuery {
                        type_ids: vec![TypeId::of::<Name>(), TypeId::of::<Transform>()],
                    });
                    let _ = command_queue.tx.send(queue);
                },
                "upgrade untyped to typed"
            }
        }
    }
}
