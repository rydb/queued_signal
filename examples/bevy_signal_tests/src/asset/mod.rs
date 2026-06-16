use std::time::Instant;

use bevy_app::prelude::*;
use bevy_asset::{AssetApp, AssetPlugin, Assets};
use bevy_color::palettes::basic::RED;
use bevy_color::palettes::css::BLUE;
use bevy_color::{Color, Srgba};
use bevy_ecs::prelude::*;
use bevy_pbr::{MeshMaterial3d, StandardMaterial};
use dioxus::prelude::*;
use dioxus_bevy_signals::asset::{AssetNoneState, use_bevy_asset};
use dioxus_bevy_signals::query::use_bevy_query;
use dioxus_hooks::{use_memo, use_signal};
use dioxus_signals::{ReadableExt, WritableExt};

use crate::DioxusTestPlugin;

#[derive(Resource)]
pub struct AssetDebugTimer(Instant);

#[derive(Component, Clone)]
pub struct Marker;

#[derive(Component, Clone)]
pub struct ToggleMarker;

pub fn spawn_color_entity(mut commands: Commands, mut materials: ResMut<Assets<StandardMaterial>>) {
    commands.spawn((
        Marker,
        MeshMaterial3d(materials.add(StandardMaterial {
            base_color: RED.into(),
            ..Default::default()
        })),
    ));
    commands.spawn((
        ToggleMarker,
        MeshMaterial3d(materials.add(StandardMaterial {
            base_color: BLUE.into(),
            ..Default::default()
        })),
    ));
}
pub fn color_value_print(
    mats: Res<Assets<StandardMaterial>>,
    colors: Query<&MeshMaterial3d<StandardMaterial>>,
    mut timer: ResMut<AssetDebugTimer>,
) {
    if timer.0.elapsed() >= core::time::Duration::from_millis(1000) {
        timer.0 = Instant::now();
        for color in colors {
            let Some(_color) = mats.get(&color.0) else {
                println!("handle but no asset?");
                continue;
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
        app.insert_resource(AssetDebugTimer(Instant::now()))
            .init_asset::<StandardMaterial>()
            .add_systems(Startup, spawn_color_entity)
            .add_systems(Update, color_value_print);
    }
}

impl DioxusTestPlugin for AssetTestPlugin {
    fn included_element(&self) -> Element {
        rsx! {
            AssetDemos {}
        }
    }

    fn register_plugin(&self, app: &mut App) {
        app.add_plugins(AssetTestPlugin);
    }
}

#[component]
pub fn ToggleableAsset() -> Element {
    let colors = use_bevy_query::<
        (
            Entity,
            &mut MeshMaterial3d<StandardMaterial>,
            &mut ToggleMarker,
        ),
        (),
    >();

    let handle = use_memo(move || {
        colors
            .iter()
            .next()
            .map(|n| n.1.read().0.clone().id())
            .ok_or(AssetNoneState::NonAsset)
    });

    let color = use_bevy_asset(handle);

    let status_string = use_memo(move || {
        color
            .read_ok(|c| format!("{:?}", c.base_color))
            .unwrap_or_else(|err| err.as_string())
    });

    let make_white = move |_| {
        color.mutate(|n| n.base_color = Color::srgb(1.0, 1.0, 1.0));
    };
    let make_more_green = move |_| {
        color.mutate(|n| {
            let color_srgb = n.base_color.to_srgba();
            let new_green = color_srgb.green + 10.0;
            n.base_color = Color::Srgba(Srgba::rgb(color_srgb.red, new_green, color_srgb.blue))
        });
    };

    rsx! {
        div {
            h2 { {format!("current toggle color: {}", status_string.read())}}
            button {
                onclick: make_more_green,
                "make more green"
            }
            button {
                onclick: make_white,
                "make white"
            }
        }
    }
}

/// Component for testing cleanup of unmonitored asset mirrors.
#[component]
pub fn AssetCleanupDemo() -> Element {
    let mut color_toggled = use_signal(|| true);
    let _toggle_handle = move |_| {
        *color_toggled.write() ^= true;
    };

    rsx! {
        h2 {
            "Unwatched assets cleanup test:"
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
    let colors =
        use_bevy_query::<(Entity, &mut MeshMaterial3d<StandardMaterial>, &mut Marker), ()>();

    // Reactive handle to the first material (if any)
    let handle = use_memo(move || colors.iter().next().map(|(_, mat, _)| mat.read().0.clone()));

    let asset_id = use_memo(move || {
        handle
            .read()
            .as_ref()
            .map(|h| h.id())
            .ok_or(AssetNoneState::NonAsset)
    });
    let asset_state = use_bevy_asset(asset_id);

    // Derive the colour text reactively
    let color_text = use_memo(move || {
        asset_state
            .read_ok(|c| format!("{:?}", c.base_color))
            .unwrap_or_else(|err| err.as_string())
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

/// Demos for testing bevy asset synchronization with dioxus.
#[component]
pub fn AssetDemos() -> Element {
    rsx! {
        div {
            style: "border: 2px solid black; background-color: #f0f0f0; padding: 16px; border-radius: 8px;",
            AssetColorPicker {}
            AssetCleanupDemo {}
        }
    }
}
