use std::{
    any::{TypeId, type_name}, collections::{HashMap, HashSet}, future::Pending, marker::PhantomData, sync::{Arc, Mutex, atomic::{AtomicU64, Ordering}}, time::Duration
};

use bevy_app::{PreUpdate, PostUpdate, Update};
use bevy_asset::{Asset, AssetEvent, AssetId, Assets, Handle};
use bevy_ecs::{
    prelude::*,
    world::CommandQueue,
};
use bevy_log::warn;
use dioxus_core::{use_drop, use_hook};
use dioxus_hooks::{use_context, use_effect};
use queued_signal::signal::{HealthStatus, QueuedSignal, QueuedSignalHandle, WriterDriver};
use trait_set::trait_set;

use crate::{CommandQueueSender, add_systems_through_world};

trait_set! {
    pub trait DioxusAssetSync = Asset + Clone + Send + Sync + 'static;
}

/// Represents why an asset isn't available yet.
#[derive(Debug, Clone, PartialEq)]
pub enum AssetNoneState<A: DioxusAssetSync> {
    Loading(PhantomData<A>),
}

/// All active mirror entries of a given asset type.
#[derive(Resource)]
pub struct MirrorAssets<A: DioxusAssetSync> {
    entries: HashMap<AssetId<A>, AssetMirrorEntry<A>>,
}

impl<A: DioxusAssetSync> Default for MirrorAssets<A> {
    fn default() -> Self {
        Self { entries: HashMap::new() }
    }
}

struct AssetMirrorEntry<A: DioxusAssetSync> {
    signal: QueuedSignal<Result<A, AssetNoneState<A>>>,
    driver: Mutex<WriterDriver<Result<A, AssetNoneState<A>>>>,
    version: Arc<AtomicU64>,
    request_count: u32,
}

/// Pending requests/releases for assets
#[derive(Resource)]
struct PendingAssetRequests<A: DioxusAssetSync> {
    items: Vec<(AssetId<A>, flume::Sender<QueuedSignal<Result<A, AssetNoneState<A>>>>, i8)>,
}
impl<A: DioxusAssetSync> Default for PendingAssetRequests<A> {
    fn default() -> Self {
        Self { items: Default::default() }
    }
}

/// Tracks which asset types already have their sync systems registered.
#[derive(Resource, Default)]
pub struct RegisteredAssetSyncs(HashSet<TypeId>);

pub struct MirrorAssetHandle<A: DioxusAssetSync> {
    inner: QueuedSignalHandle<Result<A, AssetNoneState<A>>>,
}

impl<A: DioxusAssetSync> Clone for MirrorAssetHandle<A> {
    fn clone(&self) -> Self { Self { inner: self.inner.clone() } }
}

impl<A: DioxusAssetSync> MirrorAssetHandle<A> {
    pub fn get(&self) -> queued_signal::signal::TrackedReadGuard<'_, Result<A, AssetNoneState<A>>> {
        self.inner.writer.read()
    }

    pub fn mutate<F>(&self, f: F)
    where
        F: Fn(&mut A) + Send + Sync + 'static,
    {
        self.inner.mutate(move |res| {
            if let Ok(asset) = res {
                f(asset);
            }
        });
    }

    pub fn health(&self) -> HealthStatus { self.inner.health() }
}

pub struct RequestAssetSync<A: DioxusAssetSync> { _phantom: PhantomData<A> }
impl<A: DioxusAssetSync> Command for RequestAssetSync<A> {
    fn apply(self, world: &mut World) {
        let registered = world.get_resource_or_insert_with(|| {
            let mut new_map = RegisteredAssetSyncs::default();
            new_map.0.insert(TypeId::of::<A>());
            new_map
        });
        if registered.0.contains(&TypeId::of::<A>()) == false {
            println!("inserting assets for {}", type_name::<A>());
            add_systems_through_world(world, PreUpdate, process_pending_asset_requests::<A>);
            add_systems_through_world(world, Update, drive_asset_signals::<A>);
            add_systems_through_world(world, PostUpdate, sync_assets_to_mirrors::<A>);
            add_systems_through_world(world, PostUpdate, sync_mirrors_to_assets::<A>);

        }
    }
}


pub struct RequestAssetHandle<A: DioxusAssetSync> {
    pub handle: Handle<A>,
    pub response_tx: flume::Sender<QueuedSignal<Result<A, AssetNoneState<A>>>>,
}
impl<A: DioxusAssetSync> Command for RequestAssetHandle<A> {
    fn apply(self, world: &mut World) {
        let mut pending = world.get_resource_or_init::<PendingAssetRequests<A>>();
        pending.items.push((self.handle.id(), self.response_tx, 1));
    }
}

pub struct ReleaseAssetHandle<A: DioxusAssetSync> { pub handle: Handle<A> }
impl<A: DioxusAssetSync> Command for ReleaseAssetHandle<A> {
    fn apply(self, world: &mut World) {
        let mut pending = world.get_resource_or_init::<PendingAssetRequests<A>>();
        pending.items.push((self.handle.id(), flume::bounded(0).0, -1));
    }
}

fn process_pending_asset_requests<A: DioxusAssetSync>(
    mut mirrors: ResMut<MirrorAssets<A>>,
    mut pending: ResMut<PendingAssetRequests<A>>,
) {
    for (id, tx, delta) in std::mem::take(&mut pending.items) {
        match delta {
            1 => {
                let entry = mirrors.entries.entry(id).or_insert_with(|| {
                    let mut driver = WriterDriver::new(Err(AssetNoneState::Loading(PhantomData)));
                    let version = Arc::new(AtomicU64::new(0));
                    driver.set_publish_counter(version.clone());
                    let signal = QueuedSignal::from_parts(
                        driver.queued_state.clone(),
                        driver.add_tx.clone(),
                        driver.set_tx.clone(),
                        driver.set_value_tx.clone(),
                    );
                    AssetMirrorEntry { signal: signal.clone(), driver: Mutex::new(driver), version, request_count: 0 }
                });
                entry.request_count += 1;
                let _ = tx.send(entry.signal.clone());
            }
            -1 => {
                if let Some(entry) = mirrors.entries.get_mut(&id) {
                    entry.request_count = entry.request_count.saturating_sub(1);
                    if entry.request_count == 0 { mirrors.entries.remove(&id); }
                }
            }
            _ => unreachable!(),
        }
    }
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
        if let Some(entry) = mirrors.entries.get(id) {
            if let Some(a) = assets.get(*id) {
                entry.signal.set_value(Arc::new(Ok(a.clone())));
            }
        }
    }
}

fn sync_mirrors_to_assets<A: DioxusAssetSync>(
    mut assets: ResMut<Assets<A>>,
    mirrors: Res<MirrorAssets<A>>,
    mut last_versions: Local<HashMap<AssetId<A>, u64>>,
) {
    for (id, entry) in &mirrors.entries {
        let cur = entry.version.load(Ordering::Acquire);
        let last = last_versions.get(id).copied().unwrap_or(0);
        if cur != last {
            if let Some(asset) = assets.get_mut(*id) {
                let guard = entry.signal.read();
                if let Ok(a) = guard.as_ref() {
                    *asset = a.clone();
                }
            }
            last_versions.insert(*id, cur);
        }
    }
}

pub fn use_bevy_asset<A: DioxusAssetSync>(handle: Handle<A>) -> MirrorAssetHandle<A> {
    let ctx = use_context::<CommandQueueSender>();

    let r = ctx.clone();
    use_effect(move || {
        let mut q = CommandQueue::default();
        q.push(RequestAssetSync::<A> { _phantom: PhantomData });
        let _ = r.tx.send(q);
    });

    let signal = use_hook(|| {
        ctx.send_command(|tx| {
            let mut q = CommandQueue::default();
            q.push(RequestAssetHandle::<A> { handle: handle.clone(), response_tx: tx });
            q
        }).inspect_err(|e| warn!("{}", e))
    }).unwrap();

    let h = handle.clone();
    let r = ctx.clone();
    use_drop(move || {
        let mut q = CommandQueue::default();
        q.push(ReleaseAssetHandle::<A> { handle: h });
        let _ = r.tx.send(q);
    });

    let (value_signal, health) = signal.use_hook();
    MirrorAssetHandle { inner: QueuedSignalHandle { signal: value_signal, health, writer: signal } }
}