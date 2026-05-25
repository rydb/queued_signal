use std::sync::Arc;
use std::time::Instant;

use bevy_app::ctrlc::Signal;
use bevy_app::prelude::*;
use bevy_asset::{AssetApp, AssetPlugin, Assets, Handle};
use bevy_color::palettes::basic::RED;
use bevy_color::palettes::css::{BLUE, GREEN};
use bevy_color::{Color, LinearRgba, Srgba};
use bevy_ecs::prelude::*;
use bevy_log::warn;
use bevy_pbr::{MeshMaterial3d, StandardMaterial};
use dioxus::desktop::wry::cookie::time::Duration;
use dioxus::prelude::*;
use dioxus_bevy_signals::asset::{AssetState, use_bevy_asset};
// use dioxus_bevy_signals::asset::use_bevy_asset;
use dioxus_bevy_signals::query::use_bevy_query;
use dioxus_bevy_signals::{CommandQueueSender, push_and_send};
use dioxus_hooks::{use_memo, use_signal};
use dioxus_signals::{ReadableExt, WritableExt};

use crate::DioxusTestPlugin;

#[derive(Resource)]
pub struct AssetDebugTimer(Instant);

#[derive(Component, Clone)]
pub struct Marker;

#[derive(Component, Clone)]
pub struct ToggleMarker;

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
    commands.spawn(
        (
            ToggleMarker,
            MeshMaterial3d(materials.add(StandardMaterial {
                base_color: BLUE.into(),
                ..Default::default()
            }))
        )
    );
}
pub fn color_value_print(
    mats: Res<Assets<StandardMaterial>>,
    colors: Query<&MeshMaterial3d<StandardMaterial>>,
    mut timer: ResMut<AssetDebugTimer>
) {
    if timer.0.elapsed() >= core::time::Duration::from_millis(1000) {
        timer.0 = Instant::now();
        for color in colors {
            let Some(color) = mats.get(&color.0) else {
                println!("handle but no asset?");
                continue
            };
        }
    }
}

#[derive(Default)]
pub struct AssetTestPlugin;

impl Plugin for AssetTestPlugin {
    fn build(&self, app: &mut App) {
        if !app.is_plugin_added::<AssetPlugin>() {
            app.add_plugins(AssetPlugin::default());
        }
        app
        .insert_resource(AssetDebugTimer(Instant::now()))
        .init_asset::<StandardMaterial>()
        .add_systems(Startup, spawn_color_entity)
        .add_systems(Update, color_value_print);

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
pub fn ToggleableAsset() -> Element {
    let colors = use_bevy_query::<(Entity, &mut MeshMaterial3d<StandardMaterial>, &mut ToggleMarker), ()>();
    
    let handle = use_memo(move || {
        colors.iter().next().map(|n| n.1.read().0.clone().id())
    });

    let color = use_bevy_asset(handle);

    let status_string = use_memo(move || {
        let mut text = "Loading...".to_owned();

        let Ok(color) = &*color.read() else {
            return text
        };
        text = format!("{:?}", color.base_color);
        text
    });

    rsx! {
        div {
            h2 { {format!("current toggle color: {}", status_string.read())}}
        }
    }
}

/// component to test removing unmonitored assets from AssetsMirror
#[component]
pub fn AssetCleanupDemo() -> Element {
    let mut color_toggled = use_signal(|| true);
    let _toggle_handle = move |_| {
        *color_toggled.write() ^= true;
    };
    rsx! {
        h2 {
            "unwatched assets cleanup test:"
        }
        button {  
            onclick: _toggle_handle,
            "toggle secondary color"
        }
        if *color_toggled.read() {
            ToggleableAsset {  }
        } else {
            div {
                "color disabled"
            }
        }
    }
}

#[component]
pub fn AssetColorPicker() -> Element {
    let colors = use_bevy_query::<(Entity, &mut MeshMaterial3d<StandardMaterial>, &mut Marker), ()>();

    // Reactive handle to the first material (if any)
    let handle = use_memo(move || {
        colors.iter().next().map(|(_, mat, _)| mat.read().0.clone())
    });

    let asset_id = use_memo(move || handle.read().as_ref().map(|h| h.id()));
    let asset_state = use_bevy_asset(asset_id);

    // Derive the colour text reactively
    let color_text = use_memo(move || {
        let mut text = "Loading...".to_string();
        let Ok(color) = &*asset_state.read() else {
           return text
        };
        let text = format!("{:?}", color.base_color);
        text

    });
    let on_click_white = move |_| {
        asset_state.mutate(|n| n.base_color = Color::srgb(1.0, 1.0, 1.0));
    };
    let make_more_green = move |_| {
        asset_state.mutate(|n| {
            let color_srgb = n.base_color.to_srgba();
            let new_green = color_srgb.green + 10.0;
            n.base_color = Color::Srgba(Srgba::rgb(color_srgb.red, new_green, color_srgb.blue))
        });
    };

    rsx! {
        div {
            h2 { "bevy asset <-> dioxus" }
            p { "Material base color: {color_text}" }
            button { onclick: on_click_white, "Set White" }
            button { onclick: make_more_green, "Make more green" }
        }
    }
}

/// demos to test bevy asset <-> dioxus sync
#[component]
pub fn AssetDemos() -> Element {


    rsx! {
        div {
            style: "border: 2px solid black; padding: 16px; margin: 8px;",
            h2 {
                "Bevy Asset <-> Dioxus sync demos"
            }
            AssetColorPicker {}
            AssetCleanupDemo {}
        }
    }
}