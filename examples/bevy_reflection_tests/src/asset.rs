//! Reflect-driven asset demo using the untyped asset id hook.

use bevy_app::prelude::*;
use bevy_asset::{Asset, AssetApp, AssetPlugin, Assets, Handle};
use bevy_ecs::prelude::*;
use bevy_reflect::Reflect;
use dioxus::prelude::*;
use dioxus_bevy_signals::asset::{AssetNoneState, use_bevy_asset};
use dioxus_bevy_signals::reflect::asset::use_bevy_asset_dyn;
use dioxus_bevy_signals::reflect::path::{
    PrimitiveKind, PrimitiveValue, ReflectPath, read_at_path, reflect_to_primitive, write_at_path,
};
use dioxus_bevy_signals::resource::use_bevy_resource;
use dioxus_hooks::use_memo;

#[derive(Asset, Reflect, Clone, Debug)]
/// Demo asset edited through the reflect path.
pub struct DemoAsset {
    /// Editable value.
    pub value: i32,
}

#[derive(Resource, Clone, Debug)]
/// Resource holding the handle of the demo asset to keep it alive.
pub struct DemoAssetHandle(pub Handle<DemoAsset>);

/// Plugin registering the reflect asset demo.
pub struct AssetDynPlugin;

impl Plugin for AssetDynPlugin {
    fn build(&self, app: &mut App) {
        if !app.is_plugin_added::<AssetPlugin>() {
            app.add_plugins(AssetPlugin::default());
        }
        app.init_asset::<DemoAsset>()
            .register_asset_reflect::<DemoAsset>();
        app.add_systems(Startup, spawn_asset);
    }
}

/// Adds the demo asset and stores its handle to keep it alive.
fn spawn_asset(mut commands: Commands, mut assets: ResMut<Assets<DemoAsset>>) {
    let handle = assets.add(DemoAsset { value: 3 });
    commands.insert_resource(DemoAssetHandle(handle));
}

/// Test elevating an untyped asset mirror into the typed asset mirror.
#[component]
pub fn AssetElevationTest() -> Element {
    let resource =
        use_bevy_resource::<DemoAssetHandle, _, _>(|n| n.0.id(), |err| err);

    let untyped_id = use_memo(move || match resource.read() {
        Ok(guard) => Ok(guard.0.id().untyped()),
        Err(err) => Err(AssetNoneState::Fetching),
    });

    let typed_id = use_memo(move || match resource.read() {
        Ok(guard) => Ok(guard.0.id()),
        Err(err) => Err(AssetNoneState::Fetching),
    });

    let typed = use_bevy_asset::<DemoAsset>(typed_id);
    let untyped = use_bevy_asset_dyn(untyped_id);

    let typed_value = use_memo(move || {
        typed
            .read_ok(|asset| asset.value.to_string())
            .unwrap_or_else(|e| e.to_string())
    });

    let untyped_value = use_memo(move || match untyped.read() {
        Ok(guard) => {
            let path = ReflectPath::root().field("value");
            read_at_path(guard.as_ref(), &path)
                .and_then(reflect_to_primitive)
                .map(|primitive| primitive.to_string_repr())
                .unwrap_or_else(|| "missing value field".to_owned())
        }
        Err(e) => e.to_string(),
    });

    let typed_onchange = move |evt: FormEvent| {
        if let Ok(value) = evt.value().parse::<i32>() {
            typed.mutate(move |asset: &mut DemoAsset| asset.value = value);
        }
    };

    let untyped_onchange = move |evt: FormEvent| {
        if let Some(value) = PrimitiveValue::parse(&evt.value(), PrimitiveKind::I32) {
            untyped.mutate(move |root: &mut dyn Reflect| {
                let path = ReflectPath::root().field("value");
                let _ = write_at_path(root, &path, &value);
            });
        }
    };

    rsx! {
        div {
            label { "untyped value: " }
            input {
                value: untyped_value.read().clone(),
                onchange: untyped_onchange,
            }
            label { "typed value: " }
            input {
                value: typed_value.read().clone(),
                onchange: typed_onchange,
            }
        }
    }
}
