use std::time::Instant;

use bevy_app::ctrlc::Signal;
use bevy_app::prelude::*;
use bevy_asset::{AssetApp, AssetPlugin, Assets, Handle};
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
        if !app.is_plugin_added::<AssetPlugin>() {
            app.add_plugins(AssetPlugin::default());
        }
        app
        
        .init_asset::<StandardMaterial>()
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

    // Derive the optional handle reactively – use_memo to prevent re‑grabbing the signal too often
    let handle = use_memo(move || {
        colors.iter().next().map(|(_, mat, _)| mat.read().0.clone())
    });

    let material = use_bevy_asset::<StandardMaterial>((handle.read()).clone());

    // Now we use the reactive signal: `material()` returns Option<Arc<Result<…>>>
    let r = material.clone();
    let color_text = use_memo(move || {
        let mut text = "material uninitialized".to_owned();
        let Some(mat) = r() else {
            return text
        };
        text = match mat.as_ref() {
            Ok(asset) => format!("{:?}", asset.base_color),
            Err(_) => "Loading…".to_owned(),
        };
        text
    });

    // Button handlers unchanged – they use material.mutate()
    let r = material.clone();
    let on_click_white = move |_| r.mutate(|m| m.base_color = Color::WHITE);
    let r = material.clone();
    let on_click_green = move |_| r.mutate(|m| m.base_color = GREEN.into());

    rsx! {
        div {
            style: "border: 2px solid black; padding: 16px; margin: 8px;",
            h2 { "Asset Mirror Demo" }
            p { "Material base color: {color_text}" }
            button { onclick: on_click_white, "Set White" }
            button { onclick: on_click_green, "Set Green" }
        }
    }
}