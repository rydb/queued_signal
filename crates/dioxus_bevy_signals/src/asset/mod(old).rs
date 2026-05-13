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

#[derive(Debug, Clone, PartialEq)]
pub enum AssetNoneState<A: DioxusAssetSync> {
    Loading(PhantomData<A>),
}

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
    driver: Arc<Mutex<WriterDriver<Result<A, AssetNoneState<A>>>>>,   // shared with the hook
    version: Arc<AtomicU64>,
    request_count: u32,
}

#[derive(Resource)]
struct PendingAssetRequests<A: DioxusAssetSync> {
    items: Vec<(
        AssetId<A>,
        flume::Sender<QueuedSignal<Result<A, AssetNoneState<A>>>>,   // not used anymore but kept for compatibility
        i8,
        Option<Arc<Mutex<WriterDriver<Result<A, AssetNoneState<A>>>>>>,
        Option<QueuedSignal<Result<A, AssetNoneState<A>>>>,
    )>,
}

impl<A: DioxusAssetSync> Default for PendingAssetRequests<A> {
    fn default() -> Self {
        Self { items: Default::default() }
    }
}

#[derive(Resource, Default)]
pub struct RegisteredAssetSyncs(HashSet<TypeId>);
pub struct MirrorAssetHandle<A: DioxusAssetSync> {
    inner: QueuedSignalHandle<Result<A, AssetNoneState<A>>>,
}

impl<A: DioxusAssetSync> Deref for MirrorAssetHandle<A> {
    type Target = Signal<Option<Arc<Result<A, AssetNoneState<A>>>>>;

    fn deref(&self) -> &Self::Target {
        &self.inner.signal
    }
}

impl<A: DioxusAssetSync> Clone for MirrorAssetHandle<A> {
    fn clone(&self) -> Self { Self { inner: self.inner.clone() } }
}

impl<A: DioxusAssetSync> MirrorAssetHandle<A> {
    pub fn get(&self) -> queued_signal::signal::TrackedReadGuard<'_, Result<A, AssetNoneState<A>>> {
        self.inner.writer.read()
    }

    pub fn current(&self) -> Option<Arc<Result<A, AssetNoneState<A>>>> {
        self.inner.signal.read().clone()
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
pub struct RequestAssetHandle<A: DioxusAssetSync> {
    pub handle: Handle<A>,
    pub response_tx: flume::Sender<QueuedSignal<Result<A, AssetNoneState<A>>>>,
}

impl<A: DioxusAssetSync> Command for RequestAssetHandle<A> {
    fn apply(self, world: &mut World) {
        // --- Registration (first call only) ---
        let mut registered = world.get_resource_or_init::<RegisteredAssetSyncs>();
        if !registered.0.contains(&TypeId::of::<A>()) {
            registered.0.insert(TypeId::of::<A>());
            world.init_resource::<MirrorAssets<A>>();
            world.init_resource::<PendingAssetRequests<A>>();
            add_systems_through_world(world, PreUpdate, process_pending_asset_requests::<A>);
            add_systems_through_world(world, Update, drive_asset_signals::<A>);
            add_systems_through_world(world, PostUpdate, sync_assets_to_mirrors::<A>);
            add_systems_through_world(world, PostUpdate, sync_mirrors_to_assets::<A>);
        }

        let id = self.handle.id();

        // Fetch the asset *before* borrowing mirrors, to avoid borrow conflict.
        let maybe_asset = world.resource::<Assets<A>>().get(id).cloned();

        // --- Enqueue the request (increment reference count) ---
        let mut mirrors = world.resource_mut::<MirrorAssets<A>>();
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
            AssetMirrorEntry {
                signal: signal.clone(),
                driver: Arc::new(Mutex::new(driver)),
                version,
                request_count: 0,
            }
        });

        entry.request_count += 1;

        // If the asset was already loaded, push it immediately.
        if let Some(asset) = maybe_asset {
            entry.signal.set_value(Arc::new(Ok(asset)));
            // Force a tick so the subscriber sees the value right away.
            if let Ok(mut d) = entry.driver.lock() {
                d.tick(Duration::ZERO);
            }
        }

        let _ = self.response_tx.send(entry.signal.clone());
    }
}

pub struct ReleaseAssetHandle<A: DioxusAssetSync> { pub handle: Handle<A> }
impl<A: DioxusAssetSync> Command for ReleaseAssetHandle<A> {
    fn apply(self, world: &mut World) {
        let mut pending = world.get_resource_or_init::<PendingAssetRequests<A>>();
        pending.items.push((
            self.handle.id(),
            flume::bounded(0).0,
            -1,
            None,
            None,
        ));
    }
}

fn process_pending_asset_requests<A: DioxusAssetSync>(
    mut mirrors: ResMut<MirrorAssets<A>>,
    mut pending: ResMut<PendingAssetRequests<A>>,
    assets: Res<Assets<A>>,
) {
    for (id, _tx, delta, driver_opt, signal_opt) in std::mem::take(&mut pending.items) {
        match delta {
            1 => {
                let entry: &mut AssetMirrorEntry<A> = match mirrors.entries.entry(id) {
                    Entry::Occupied(occupied) => {
                        let entry = occupied.into_mut();
                        if let Some(d) = driver_opt {
                            entry.driver = d;
                        }
                        if let Some(s) = signal_opt {
                            entry.signal = s;
                        }
                        entry.request_count += 1;
                        entry
                    }
                    Entry::Vacant(vacant) => {
                        let driver = driver_opt.unwrap_or_else(|| {
                            Arc::new(Mutex::new(WriterDriver::new(Err(AssetNoneState::Loading(PhantomData)))))
                        });
                        let version = Arc::new(AtomicU64::new(0));
                        let signal = signal_opt.unwrap_or_else(|| {
                            let d = driver.lock().unwrap();
                            QueuedSignal::from_parts(
                                d.queued_state.clone(),
                                d.add_tx.clone(),
                                d.set_tx.clone(),
                                d.set_value_tx.clone(),
                            )
                        });
                        vacant.insert(AssetMirrorEntry {
                            signal: signal.clone(),
                            driver,
                            version,
                            request_count: 1,
                        })
                    }
                };

                if let Some(asset) = assets.get(id) {
                    entry.signal.set_value(Arc::new(Ok(asset.clone())));

                    // Force publish so the Dioxus hook sees the update.
                    if let Ok(mut driver) = entry.driver.lock() {
                        driver.tick(Duration::ZERO);
                    }
                }
            }
            -1 => {
                if let Some(entry) = mirrors.entries.get_mut(&id) {
                    entry.request_count = entry.request_count.saturating_sub(1);
                    if entry.request_count == 0 {
                        mirrors.entries.remove(&id);
                    }
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

pub fn use_bevy_asset<A: DioxusAssetSync>(handle: Option<Handle<A>>) -> MirrorAssetHandle<A> {
    let ctx = use_context::<CommandQueueSender>();

    // Track the handle so we can request/release it.
    let mut current_handle = use_signal(|| handle.clone());

    // Request the signal on mount and when the handle changes.
    let signal = {
        let ctx = ctx.clone();
        let h = handle.clone();
        use_hook(|| {
            ctx.send_command(|tx| {
                let mut queue = CommandQueue::default();
                // The ReleaseAssetHandle for the old handle will have been sent before
                // this hook re-runs (see needs_update logic below).
                // We only request if we have a handle.
                if let Some(h) = h.clone() {
                    queue.push(RequestAssetHandle::<A> {
                        handle: h,
                        response_tx: tx,
                    });
                }
                queue
            })
            .inspect_err(|err| warn!("{}", err))
        })
        .unwrap() // Will be None if handle was None; handled below.
    };

    // When handle changes: release old, request new.
    let needs_update = {
        let cur = current_handle.read();
        *cur != handle
    };
    if needs_update {
        let mut cur = current_handle.write();
        // Release old handle (if any).
        if let Some(old) = cur.take() {
            let mut q = CommandQueue::default();
            q.push(ReleaseAssetHandle::<A> { handle: old });
            let _ = ctx.tx.send(q);
        }
        // The new handle is requested by the `use_hook` above which will re‑run
        // because we changed `current_handle`.
        *cur = handle.clone();
    }

    // Release on unmount.
    {
        let h = handle.clone();
        let r = ctx.clone();
        use_drop(move || {
            if let Some(h) = h {
                let mut q = CommandQueue::default();
                q.push(ReleaseAssetHandle::<A> { handle: h });
                let _ = r.tx.send(q);
            }
        });
    }

    // If no handle was provided, we still need a valid signal to satisfy the
    // use_hook rules. We create a dummy loading signal that stays in Loading forever.
    // This keeps the hook count stable without flooding Bevy with fake requests.
    let signal = signal.unwrap_or_else(|| {
        use_hook(|| {
            let mut driver = WriterDriver::new(Err(AssetNoneState::Loading(PhantomData)));
            QueuedSignal::from_parts(
                driver.queued_state.clone(),
                driver.add_tx.clone(),
                driver.set_tx.clone(),
                driver.set_value_tx.clone(),
            )
        })
        .clone()
    });

    let (value_signal, health) = signal.use_hook();

    MirrorAssetHandle {
        inner: QueuedSignalHandle {
            signal: value_signal,
            health,
            writer: signal,
        },
    }
}