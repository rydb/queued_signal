use std::{
    any::{TypeId, type_name}, collections::{HashMap, HashSet}, marker::PhantomData, ops::Deref, sync::{Arc, Mutex, atomic::{AtomicU64, Ordering}}, time::Duration
};
use std::collections::hash_map::Entry;
use bevy_app::{PreUpdate, PostUpdate, Update};
use bevy_asset::{Asset, AssetEvent, AssetId, Assets, Handle};
use bevy_ecs::{
    prelude::*,
    world::CommandQueue,
};
use bevy_log::warn;
use dioxus_core::{use_drop, use_hook};
use dioxus_hooks::{use_context, use_effect, use_future, use_signal};
use dioxus_signals::{ReadableExt, Signal, WritableExt};
use queued_signal::signal::{HealthStatus, QueuedSignal, QueuedSignalHandle, WriterDriver};
use trait_set::trait_set;

use crate::{CommandQueueSender, add_systems_through_world};

trait_set! {
    pub trait DioxusAssetSync = Asset + Clone + Send + Sync + 'static;
}

#[derive(Clone)]
pub enum AssetLoadState<A: DioxusAssetSync> {
    Loaded(A),
    Loading,
}

pub struct AssetMirror<A: DioxusAssetSync> {
    value: QueuedSignal<A>,
    writer: Mutex<WriterDriver<A>>,
}

pub struct AssetsMirrorWriteDriver<A: DioxusAssetSync>(Mutex<WriterDriver<AssetsMirror<A>>>);

/// handle to an [`AssetMirror`]
#[derive(Clone)]
pub struct AssetMirrorHandle<A: DioxusAssetSync> {
    value: QueuedSignal<A>,
}

/// the mirrored version of [Assets<A>]
#[derive(Resource, Clone)]
pub struct AssetsMirror<A: DioxusAssetSync> {
    values: HashMap<AssetId<A>, AssetMirrorHandle<A>>,
}

pub fn sync_mirrors_to_assets<A: DioxusAssetSync>() {

}

/// the mirrored assets of A
#[derive(Resource)]
pub struct AssetMirrors<A: DioxusAssetSync> {
    assets: HashMap<AssetId<A>, AssetMirror<A>>
}

#[derive(Resource)]
pub struct AssetMirrorsTickRate(Duration);

fn drive_asset_server_signal<A: DioxusAssetSync>(
    tick_rate: Res<AssetMirrorsTickRate>,
    mirrors: ResMut<AssetMirrors<A>>
) {
    for writer in mirrors {}
}

fn drive_asset_signals<A: DioxusAssetSync>(mirrors: Res<MirrorAssets<A>>) {
    for entry in mirrors.entries.values() {
        if let Ok(mut d) = entry.driver.lock() { d.tick(Duration::ZERO); }
    }
}


fn sync_assets_to_mirrors<A: DioxusAssetSync>(
    mut events: MessageReader<AssetEvent<A>>,
    assets: Res<Assets<A>>,
    mirrors: Res<MirrorAssets<A>>,
) {
    for event in events.read() {
        let id = match event {
            AssetEvent::Modified { id } | AssetEvent::LoadedWithDependencies { id } => id,
            _ => continue,
        };
        if let Some(entry) = mirrors.values.get(id) {
            if let Some(a) = assets.get(*id) {
                entry.value.set_value(Arc::new(a.clone()));
            }
        }
    }
}
