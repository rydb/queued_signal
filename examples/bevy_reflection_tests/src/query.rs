//! Typed and untyped query demo with an elevation button.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bevy_app::prelude::*;
use bevy_ecs::prelude::*;
use bevy_reflect::Reflect;
use bevy_time::{Real, Time};
use dioxus::prelude::*;
use dioxus_bevy_signals::query::use_bevy_query;
use dioxus_bevy_signals::reflect::path::{PrimitiveValue, collect_primitive_leaves, write_at_path};
use dioxus_bevy_signals::reflect::query::{ReflectComponentHandle, use_bevy_query_dyn};
use dioxus_hooks::{use_memo, use_signal};
use queued_signal_tracing::*;

#[derive(Component, Clone, Default, PartialEq, PartialOrd, Eq, Ord, Debug, Reflect)]
#[reflect(Component)]
pub struct Greets(u32);

/// Plugin spawning entities and registering reflect types.
pub struct QueryDynPlugin;

impl Plugin for QueryDynPlugin {
    fn build(&self, app: &mut App) {
        app.register_type::<Name>();
        app.register_type::<Greets>();
        app.world_mut().register_component::<Name>();
        app.world_mut().register_component::<Greets>();
        app.world_mut()
            .commands()
            .spawn((Name("A".into()), Greets(0)));
        app.world_mut()
            .commands()
            .spawn((Name("B".into()), Greets(0)));
        app.add_systems(Update, print_values);
    }
}

/// Print values on bevy side to see if updates registered
pub fn print_values(
    query: Query<(&Name, &Greets)>,
    mut last_tick: Local<Duration>,
    time: Res<Time<Real>>,
) {
    if (time.elapsed().as_secs() - last_tick.as_secs()) > 1 {
        for (name, greets) in query.iter() {
            trace!("{}, {}", name, greets.0);
        }
        *last_tick = time.elapsed()
    }
}

#[component]
pub fn TypedQuery() -> Element {
    let query = use_bevy_query::<(Entity, &mut Name, &mut Greets), ()>();

    let fields = use_memo(move || {
        let mut fields = vec![];
        for (_e, name, greets) in query.iter() {
            let name_value = name.read().as_str().to_owned();
            let greets_value = greets.read().0.to_string();

            let field = rsx! {
                div {
                    input {
                        value: name_value,
                        oninput: move |evt: FormEvent| {
                            let text = evt.value();
                            name.mutate(move |n: &mut Name| n.set(text.clone()));
                        },
                    }
                    input {
                        value: greets_value,
                        oninput: move |evt: FormEvent| {
                            if let Ok(parsed) = evt.value().parse::<u32>() {
                                greets.mutate(move |g: &mut Greets| g.0 = parsed);
                            }
                        },
                    }
                }
            };
            fields.push(field);
        }
        fields
    });

    rsx! {
        h1 { "Typed Query Values "}
        for field in fields.read().iter() {
            {field}
        }
    }
}

#[component]
pub fn UntypedQuery() -> Element {
    let query = use_bevy_query_dyn(["Name", "Greets"]);

    let fields = use_memo(move || {
        let mut fields = vec![];
        let snapshot = query.read();

        let map = match &*snapshot {
            Ok(map) => map,
            Err(err) => {
                error!("{:#?}", err);
                return vec![];
            }
        };

        for (item_idx, (entity, handles)) in query.iter().into_iter().enumerate() {
            let values = map.get(&entity).cloned().unwrap_or_default();
            let mut rows = vec![];
            for (idx, (name, handle)) in handles.iter().enumerate() {
                let value = match values.get(idx) {
                    Some(arc) => Ok(arc),
                    None => Err(format!(
                        "component {idx} value is missing for entity {entity:?}"
                    )),
                };
                rows.extend(render_from(handle, name, value));
            }
            fields.push(rsx! {
                div {
                    h3 { "item_{item_idx + 1}" }
                    for row in rows.iter() {
                        {row}
                    }
                }
            });
        }
        fields
    });

    rsx! {
        h1 {" Untyped Query Values"}
        for field in fields.read().iter() {
            {field}
        }
    }
}

fn render_from(
    handle: &ReflectComponentHandle,
    name: &str,
    value: std::result::Result<&Arc<dyn Reflect>, String>,
) -> Vec<Element> {
    let arc = match value {
        Ok(arc) => arc,
        Err(err) => return vec![rsx! { div { "{name}: {err}" } }],
    };

    let leaves = collect_primitive_leaves(arc.as_ref());
    if leaves.is_empty() {
        return vec![rsx! { div { "{name}: no editable fields" } }];
    }

    let single = leaves.len() == 1;
    leaves
        .into_iter()
        .map(|(path, result)| {
            let label = if single {
                name.to_owned()
            } else {
                format!("{name}.{}", path.label())
            };
            match result {
                Ok(primitive) => {
                    let kind = primitive.kind();
                    let text = primitive.to_string_repr();
                    let handle = handle.clone();
                    let path_for_write = path.clone();
                    rsx! {
                        div {
                            "{label}: "
                            input {
                                value: text,
                                onchange: move |evt: FormEvent| {
                                    if let Some(parsed) = PrimitiveValue::parse(&evt.value(), kind) {
                                        let path = path_for_write.clone();
                                        handle.mutate(Arc::new(move |component: &mut dyn Reflect| {
                                            let _ = write_at_path(component, &path, &parsed);
                                        }));
                                    }
                                },
                            }
                        }
                    }
                }
                Err(err) => rsx! { div { "{label}: {err}" } },
            }
        })
        .collect()
}

/// Test elevating untyped query into a typed query
#[component]
pub fn QueryElevationTest() -> Element {
    let mut typed_query =
        use_signal(|| rsx! { h1 {" ...waiting second(s) before starting TypedQuery"}});
    use_future(move || async move {
        for _ in 0..1 {
            let _ = tokio::time::sleep(Duration::from_secs(1)).await;
        }
        *typed_query.write() = rsx! {
            TypedQuery {  }
        }
    });

    rsx! {
        UntypedQuery {  },
        {typed_query.read().clone()},
    }
}

// /// Demo component showing both query forms and an upgrade button.
// #[component]
// pub fn QueryDynDemo() -> Element {
//     let typed = use_bevy_query::<(Entity, &mut Name, &mut Transform), ()>();
//     let dyn_query = use_bevy_query_dyn(["Name", "Transform"]);

//     let command_queue = use_context::<CommandQueueSender>();
//     let mut upgraded = use_signal(|| false);

//     let typed_count = use_memo(move || typed.iter().count());
//     let dyn_count = use_memo(move || match &*dyn_query.read() {
//         Ok(map) => map.len(),
//         Err(_) => 0,
//     });

//     rsx! {
//         div {
//             h2 { "typed query entities: {typed_count}" }
//             h2 { "untyped query entities: {dyn_count}" }
//             button {
//                 onclick: move |_| {
//                     upgraded.set(true);
//                     let mut queue = CommandQueue::default();
//                     queue.push(ElevateReflectQuery {
//                         type_ids: vec![TypeId::of::<Name>(), TypeId::of::<Transform>()],
//                     });
//                     let _ = command_queue.tx.send(queue);
//                 },
//                 "upgrade untyped to typed"
//             }
//         }
//     }
// }
