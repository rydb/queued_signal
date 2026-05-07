use std::time::Instant;

use bevy_app::ctrlc::Signal;
use bevy_app::prelude::*;
use bevy_asset::{Assets, Handle};
use bevy_color::palettes::basic::RED;
use bevy_color::palettes::css::GREEN;
use bevy_color::{Color, LinearRgba};
use bevy_ecs::prelude::*;
use bevy_log::warn;
use bevy_pbr::{MeshMaterial3d, StandardMaterial};
use dioxus::prelude::*;
use dioxus_bevy_signals::asset::use_bevy_asset;
use dioxus_bevy_signals::query::use_bevy_query;
use dioxus_bevy_signals::{CommandQueueSender, push_and_send};
use dioxus_hooks::{use_memo, use_signal};
use dioxus_signals::{ReadableExt, WritableExt};

use crate::DioxusTestPlugin;


#[derive(Component, Clone)]
pub struct Marker;

pub fn spawn_color_entity(
    mut commands: Commands,
    mut materials: ResMut<Assets<StandardMaterial>>
) {
    commands.spawn(
        (
            Marker,
            MeshMaterial3d(materials.add(StandardMaterial {
                base_color: RED.into(),
                ..Default::default()
            }))
        )
    );
}

#[derive(Default)]
pub struct AssetTestPlugin;

impl Plugin for AssetTestPlugin {
    fn build(&self, app: &mut App) {
        app
        .add_systems(Startup, spawn_color_entity);

    }
}

impl DioxusTestPlugin for AssetTestPlugin {
    fn included_element(&self) -> Element {
        rsx! {
            AssetColorPicker {}
        }
    }

    fn register_plugin(&self, app: &mut App) {
        app.add_plugins(AssetTestPlugin);
    }
}

#[component]
pub fn AssetColorPicker() -> Element {
    let colors = use_bevy_query::<(Entity, &mut MeshMaterial3d<StandardMaterial>, &mut Marker), ()>();
    let colors = use_signal(|| colors);

    let mut material = use_signal(|| None);

    let color_text = use_memo(move || {
        let mut color_text = "".to_owned();
        let Some((e, handle, ..)) = colors.read().iter().next() else {
            warn!("no color found for colors");
            return color_text;
        };

        let handle = handle.read().0.clone();
        let color = use_bevy_asset(handle);

        *material.write() = Some(color.clone());

        let color = color.get();
        let color = color.as_ref().clone();
        let text = match color {
            Ok(color) => format!("{:?}", color.base_color),
            Err(_) => "Loading...".to_owned(),
        };
        color_text += &text;
        color_text
    });

    let on_click_white = move |_evt | {
        let binding = material.read();
        let Some(material) = binding.as_ref() else {
            return
        };
        material.mutate(|mat| mat.base_color = Color::WHITE);
    };

    let on_click_green = move |_evt| {
        let binding = material.read();
        let Some(material) = binding.as_ref() else {
            return
        };
        material.mutate(|mat| mat.base_color = GREEN.into());
    };

    rsx! {
        div {
            style: "border: 2px solid black; padding: 16px; margin: 8px;",
            h2 { "Asset Mirror Demo" }
            p { "Material base color: {color_text}" }
            button {
                onclick: on_click_white,
                "Set White"
            }
            button {
                onclick: on_click_green,
                "Set Green"
            }
        }
    }
}