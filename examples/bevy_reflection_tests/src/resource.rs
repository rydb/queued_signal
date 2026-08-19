use std::time::Duration;

use bevy_app::prelude::*;
use bevy_ecs::prelude::*;
use bevy_reflect::{Reflect, ReflectRef};
use dioxus::prelude::*;
use dioxus_bevy_signals::reflect::path::{
    PrimitiveValue, ReflectPath, reflect_to_primitive, write_at_path,
};
use dioxus_bevy_signals::reflect::resource::{ReflectResourceSignal, use_bevy_resource_dyn};
use dioxus_bevy_signals::resource::use_bevy_resource;

#[derive(Clone, Resource, Debug, Reflect)]
#[reflect(Resource)]
pub struct Counter {
    pub value: i32,
}

/// Plugin registering the reflect resource for the demo.
pub struct ResourceDynPlugin;

impl Plugin for ResourceDynPlugin {
    fn build(&self, app: &mut App) {
        app.register_type::<Counter>();
        app.world_mut().insert_resource(Counter { value: 0 });
    }
}

#[component]
pub fn TypedCounter() -> Element {
    let counter = use_bevy_resource::<Counter, _, _>(|n| n.value, |err| err);

    let onclick = move |_| {
        counter.mutate(|n| n.value += 1);
    };
    rsx! {
        div {
            h1 {"{counter}"}
            button {
                onclick,
                "increment"
            }

        }
    }
}

/// Demo editing a reflect resource through the field walking layer.
#[component]
pub fn ReflectCounter() -> Element {
    let counter = use_bevy_resource_dyn("Counter");
    let value = counter.read();

    match &*value {
        Ok(root) => rsx! {
            div {
                h1 { "Reflect Counter" }
                { render_reflect(counter, ReflectPath::root(), root.as_ref()) }
            }
        },
        Err(_) => rsx! {
            div {
                h1 { "Reflect Counter" }
                span { "not initialized" }
            }
        },
    }
}

/// Demo demonstrating elevating a reflect resource into a typed resource pointer
#[component]
pub fn ReflectElevationTest() -> Element {
    let mut typed_counter = use_signal(|| rsx! {
        h1 {
            "...waiting for countdown"
        }
    });

    use_future(move || async move {
        for i in 0..1 {
            let _ = tokio::time::sleep(Duration::from_secs(1)).await;
        }
        *typed_counter.write() = rsx! {
            TypedCounter {  }
        }
    });

    rsx! {
        ReflectCounter {  }
        {typed_counter.read().clone()}
    }

}

/// Render a reflected value as a flat list of editable primitive leaves.
fn render_reflect(signal: ReflectResourceSignal, path: ReflectPath, value: &dyn Reflect) -> Element {
    let leaves = collect_leaves(&path, value);

    rsx! {
        div { class: "reflect-struct",
            for (leaf_path, primitive) in leaves {
                { render_leaf(signal, leaf_path, primitive) }
            }
        }
    }
}

/// Walk a reflected struct tree with an explicit stack, collecting primitive leaves.
fn collect_leaves(
    root_path: &ReflectPath,
    root_value: &dyn Reflect,
) -> Vec<(ReflectPath, PrimitiveValue)> {
    let mut leaves = Vec::new();
    let mut stack: Vec<(ReflectPath, &dyn Reflect)> = vec![(root_path.clone(), root_value)];

    while let Some((path, value)) = stack.pop() {
        if let Some(primitive) = reflect_to_primitive(value) {
            leaves.push((path, primitive));
            continue;
        }

        if let ReflectRef::Struct(s) = value.reflect_ref() {
            let mut children: Vec<(ReflectPath, &dyn Reflect)> = s
                .iter_fields()
                .filter_map(|(name, field)| field.try_as_reflect().map(|refl| (path.field(name), refl)))
                .collect();
            children.reverse();
            stack.extend(children);
        }
    }

    leaves
}

/// Render a primitive leaf as a labeled, editable input.
fn render_leaf(signal: ReflectResourceSignal, path: ReflectPath, current: PrimitiveValue) -> Element {
    let kind = current.kind();
    let initial = current.to_string_repr();
    let label = path
        .segments()
        .last()
        .map(|s| s.as_str().to_owned())
        .unwrap_or_default();

    rsx! {
        div { class: "reflect-leaf",
            span { "{label}" }
            input {
                value: initial,
                onchange: move |evt: FormEvent| {
                    let text = evt.value();
                    if let Some(parsed) = PrimitiveValue::parse(&text, kind) {
                        let path = path.clone();
                        signal.mutate(move |root: &mut dyn Reflect| {
                            let _ = write_at_path(root, &path, &parsed);
                        });
                    }
                },
            }
        }
    }
}
