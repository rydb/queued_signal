pub use std::{
    any::{TypeId, type_name},
    collections::{HashMap, HashSet},
    marker::PhantomData,
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

pub(crate) use crate::macros::*;
use crate::schedules::{DioxusSyncLast, DioxusSyncPostUpdate, DioxusSyncUpdate};
use bevy_asset::{Asset, AssetEvent, AssetId, AssetServer, Assets, LoadState};
use bevy_ecs::{prelude::*, world::CommandQueue};
use dioxus_core::{spawn, use_drop, use_hook};
use dioxus_hooks::{use_context, use_effect, use_signal};
use dioxus_signals::{Memo, ReadableExt, Signal, WritableExt};
use flume::{Receiver, Sender};
use parking_lot::Mutex;
use queued_signal::signal::{HealthStatus, QueuedSignal, WriterDriver};
use tokio::sync::oneshot;
use trait_set::trait_set;

use crate::{CommandQueueSender, SignalReadGuard, add_systems_through_world};

trait_set! {
    pub trait DioxusAssetSync = Asset + Clone + Send + Sync + 'static;
}

/// current state of asset in the asset server
#[derive(Clone, Debug)]
pub enum AssetState<A: DioxusAssetSync> {
    Loaded(A),
    Loading,
}

impl<A: DioxusAssetSync> AssetState<A> {
    /// for printing asset state without Debug on A
    pub fn as_string(&self) -> &'static str {
        match self {
            AssetState::Loaded(_n) => "Loaded",
            AssetState::Loading => "Loading",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AssetNoneState {
    Loading,
    NotLoaded,
    NonAsset,
    Fetching,
    Error(String),
}

impl AssetNoneState {
    pub fn as_string(&self) -> String {
        let value = match self {
            AssetNoneState::Loading => "Loading",
            AssetNoneState::NonAsset => "NonAsset",
            AssetNoneState::Error(err) => err,
            AssetNoneState::NotLoaded => "NotLoaded",
            AssetNoneState::Fetching => "Fetching",
        };
        value.into()
    }
}

impl From<AssetNoneState> for String {
    fn from(value: AssetNoneState) -> Self {
        value.as_string()
    }
}

#[derive(Clone, Debug)]
pub struct AssetUpdateExtraInfo<A: DioxusAssetSync> {
    changed_sender: Sender<AssetId<A>>,
    asset_id: AssetId<A>,
}

/// stores a dioxus mirror of what may be a mirror to a real bevy asset.
///
/// due to dioxus hook rules, hooks cannot optionally exist, but assets may or may not exist when requested(they were despawned, asset id is wrong, etc..)
///
/// so, when an AssetId is requested you might get an asset, or you'll get an error that it doesn't exist/despawned, etc..
pub struct AssetMaybeMirror<A: DioxusAssetSync> {
    pub state: QueuedSignal<Result<A, AssetNoneState>>,
    /// second signal for extra info needed for updating assets (change detection, asset id, etc..)
    /// kept seperate to not clone channels on update
    pub extra_update_info: QueuedSignal<AssetUpdateExtraInfo<A>>,
    state_driver: Arc<Mutex<WriterDriver<Result<A, AssetNoneState>>>>,
    extra_update_info_driver: Arc<Mutex<WriterDriver<AssetUpdateExtraInfo<A>>>>,
    /// number of signals that are actively reading this asset mirror.
    ///
    /// once this hits zero(last dioxus component reading this is dropped), the asset mirror map clears this entry from it self.
    tracking_signals: i32,
}

#[derive(Resource)]
pub struct AssetMirrorMap<A: DioxusAssetSync> {
    assets: HashMap<AssetId<A>, AssetMaybeMirror<A>>,
    init_requests: HashSet<AssetId<A>>,
}

impl<A: DioxusAssetSync> Default for AssetMirrorMap<A> {
    fn default() -> Self {
        Self {
            assets: Default::default(),
            init_requests: Default::default(),
        }
    }
}

#[derive(Resource)]
pub struct ChangedAssetMirrors<A: DioxusAssetSync>(HashSet<AssetId<A>>);

impl<A: DioxusAssetSync> Default for ChangedAssetMirrors<A> {
    fn default() -> Self {
        Self(Default::default())
    }
}

#[derive(Resource)]
pub struct ChangedIdsReceiver<A: DioxusAssetSync>(Receiver<AssetId<A>>);

#[derive(Resource, Clone)]
pub struct ChangedIdsSender<A: DioxusAssetSync>(Sender<AssetId<A>>);

pub fn drive_maybe_assets<A: DioxusAssetSync>(mut mirrors: ResMut<AssetMirrorMap<A>>) {
    for (_id, asset) in &mut mirrors.assets {
        let mut guard = asset.state_driver.lock();
        guard.tick(Duration::ZERO);

        let mut guard = asset.extra_update_info_driver.lock();
        guard.tick(Duration::ZERO);
    }
}

pub fn clear_asset_init_requests<A: DioxusAssetSync>(mut mirrors: ResMut<AssetMirrorMap<A>>) {
    mirrors.init_requests.clear();
}

/// initialize asset from requested id.
pub fn init_requested_asset_mirrors<A: DioxusAssetSync>(
    mut mirrors: ResMut<AssetMirrorMap<A>>,
    asset_server: Res<AssetServer>,
    assets: Res<Assets<A>>,
) {
    let requests = mirrors.init_requests.clone();
    for id in requests {
        trace!("init_requested_asset_mirrors processing {:?}", id);

        let Some(entry) = mirrors.assets.get_mut(&id) else {
            error!(
                "how was an asset requested, but not have its uninitialized value exist in the assets list? {}",
                type_name::<A>()
            );
            continue;
        };
        let fetch = match asset_server.get_load_state(id) {
            Some(state) => {
                let asset_fetch = match state {
                    LoadState::NotLoaded => Err(AssetNoneState::NotLoaded),
                    LoadState::Loading => Err(AssetNoneState::Loading),
                    LoadState::Loaded => {
                        let asset_opt = assets.get(id);
                        match asset_opt {
                            Some(asset) => Ok(asset.clone()),
                            None => {
                                error!(
                                    "how was this asset marked as loaded without Assets<A> holding the asset? {}",
                                    type_name::<A>()
                                );
                                Err(AssetNoneState::Error(
                                    "asset marked as loaded, but Assets<A> didn't have the asset"
                                        .into(),
                                ))
                            }
                        }
                    }
                    LoadState::Failed(asset_load_error) => {
                        Err(AssetNoneState::Error(asset_load_error.to_string()))
                    }
                };
                asset_fetch
            }
            // get_load_state returns None for procedurally-generated assets
            // (those added via Assets::add() rather than loaded through the
            // AssetServer). The asset may still exist in Assets<A> — check
            // that directly before concluding it's invalid.
            None => match assets.get(id) {
                Some(asset) => {
                    trace!(
                        "init_requested_asset_mirrors: get_load_state=None but found in Assets<A> for {:?}",
                        id
                    );
                    Ok(asset.clone())
                }
                None => {
                    trace!("init_requested_asset_mirrors: asset not found for {:?}", id);
                    Err(AssetNoneState::NonAsset)
                }
            },
        };
        trace!(
            "init_requested_asset_mirrors result for {:?}: {}",
            id,
            match &fetch {
                Ok(_) => "Ok(asset)".to_string(),
                Err(e) => e.as_string(),
            }
        );
        entry.state.set_value(Arc::new(fetch));
    }
}

pub fn sync_mirrors_to_assets<A: DioxusAssetSync>(
    mut events: MessageReader<AssetEvent<A>>,
    assets: Res<Assets<A>>,
    mirrors: Res<AssetMirrorMap<A>>,
    changed: Res<ChangedAssetMirrors<A>>,
) {
    for event in events.read() {
        trace!("recieved event trace for {:#?}", event);
        let id = match event {
            AssetEvent::Modified { id } | AssetEvent::LoadedWithDependencies { id } => id,
            _ => continue,
        };
        // dont re-set the asset on the same frame that it was set to a new value to stop infinite loops
        if changed.0.contains(id) == true {
            trace!("chhanged includes {}, skipping", id);
            continue;
        }
        if let Some(entry) = mirrors.assets.get(id) {
            if let Some(a) = assets.get(*id) {
                entry.state.set_value(Arc::new(Ok(a.clone())));
            }
        }
    }
}

pub fn sync_assets_to_mirrors<A: DioxusAssetSync>(
    mut assets: ResMut<Assets<A>>,
    changed: Res<ChangedAssetMirrors<A>>,
    mirrors: Res<AssetMirrorMap<A>>,
) {
    for id in &changed.0 {
        let Some(asset) = assets.get_mut(*id) else {
            continue;
        };
        let Some(handle) = mirrors.assets.get(id) else {
            continue;
        };
        let state = handle.state.read();

        if let Ok(mirror) = state.as_ref() {
            trace!("syncing asset {:#?} to mirror", type_name::<A>());
            *asset = mirror.clone();
        }
    }
}

pub fn collect_changed_ids<A: DioxusAssetSync>(
    rx: Res<ChangedIdsReceiver<A>>,
    mut changed: ResMut<ChangedAssetMirrors<A>>,
) {
    while let Ok(id) = rx.0.try_recv() {
        trace!(
            "id {} marked as changed for asset type {:#?}",
            id,
            type_name::<A>()
        );
        changed.0.insert(id);
    }
}

pub fn clear_changed_flags<A: DioxusAssetSync>(mut changed: ResMut<ChangedAssetMirrors<A>>) {
    trace!("clearing changed assets");
    changed.bypass_change_detection().0.clear();
}

#[derive(Resource)]
pub struct AssetSyncInitialized<A: DioxusAssetSync> {
    _phantom: PhantomData<A>,
}

#[derive(Resource, Clone, Debug)]
pub struct AssetMirrorRequestResponse<A: DioxusAssetSync> {
    asset_state: QueuedSignal<Result<A, AssetNoneState>>,
    extra_info: QueuedSignal<AssetUpdateExtraInfo<A>>,
}

pub struct RequestBevyAssetMirror<A: DioxusAssetSync> {
    response_tx: oneshot::Sender<AssetMirrorRequestResponse<A>>,
    asset_id: AssetId<A>,
}

impl<A: DioxusAssetSync> Command for RequestBevyAssetMirror<A> {
    fn apply(self, world: &mut World) -> () {
        match world.get_resource::<AssetSyncInitialized<A>>() {
            Some(_) => {}
            None => {
                let (changed_tx, changed_rx) = flume::unbounded();

                world.insert_resource(AssetMirrorMap::<A>::default());
                world.insert_resource(ChangedAssetMirrors::<A>::default());
                world.insert_resource(ChangedIdsReceiver::<A>(changed_rx));
                world.insert_resource(ChangedIdsSender::<A>(changed_tx));
                world.insert_resource(PendingAssetTrackingDeltas::<A>::default());

                add_systems_through_world(world, DioxusSyncUpdate, collect_changed_ids::<A>);
                add_systems_through_world(world, DioxusSyncUpdate, drive_maybe_assets::<A>);
                add_systems_through_world(
                    world,
                    DioxusSyncPostUpdate,
                    init_requested_asset_mirrors::<A>,
                );
                add_systems_through_world(world, DioxusSyncPostUpdate, sync_mirrors_to_assets::<A>);
                add_systems_through_world(
                    world,
                    DioxusSyncPostUpdate,
                    sync_assets_to_mirrors::<A>.run_if(resource_changed::<ChangedAssetMirrors<A>>),
                );
                add_systems_through_world(
                    world,
                    DioxusSyncPostUpdate,
                    apply_tracking_queries_delta::<A>
                        .run_if(resource_changed::<PendingAssetTrackingDeltas<A>>),
                );
                add_systems_through_world(
                    world,
                    DioxusSyncLast,
                    clear_changed_flags::<A>.run_if(resource_changed::<ChangedAssetMirrors<A>>),
                );
                add_systems_through_world(world, DioxusSyncLast, clear_asset_init_requests::<A>);

                world.insert_resource(AssetSyncInitialized {
                    _phantom: PhantomData::<A>,
                });
            }
        }
        let changed_tx = world.resource::<ChangedIdsSender<A>>().0.clone();

        let mut map = world.resource_mut::<AssetMirrorMap<A>>();

        let (asset_state, extra_info) = match map.assets.get_mut(&self.asset_id) {
            Some(asset) => {
                trace!(
                    "requested new signal and set new tracking value for {} -> {}",
                    asset.tracking_signals,
                    asset.tracking_signals + 1
                );
                (asset.state.clone(), asset.extra_update_info.clone())
            }
            None => {
                let asset_state_driver = WriterDriver::new(Err(AssetNoneState::Fetching));

                let set_value_tx = asset_state_driver.set_value_tx.clone();
                let set_tx = asset_state_driver.set_tx.clone();
                let add_tx = asset_state_driver.add_tx.clone();
                let queued_state = asset_state_driver.queued_state.clone();

                let asset_state_driver_arc = Arc::new(Mutex::new(asset_state_driver));

                let asset_state = QueuedSignal::from_parts(
                    queued_state,
                    Some(asset_state_driver_arc.clone()),
                    add_tx,
                    set_tx,
                    set_value_tx,
                );

                let extra_info_driver = WriterDriver::new(AssetUpdateExtraInfo {
                    changed_sender: changed_tx,
                    asset_id: self.asset_id,
                });

                let set_value_tx = extra_info_driver.set_value_tx.clone();
                let set_tx = extra_info_driver.set_tx.clone();
                let add_tx = extra_info_driver.add_tx.clone();
                let queued_state = extra_info_driver.queued_state.clone();

                let extra_info_driver_arc = Arc::new(Mutex::new(extra_info_driver));

                let extra_info = QueuedSignal::from_parts(
                    queued_state,
                    Some(extra_info_driver_arc.clone()),
                    add_tx,
                    set_tx,
                    set_value_tx,
                );
                let mirror = AssetMaybeMirror {
                    state: asset_state.clone(),
                    state_driver: asset_state_driver_arc,
                    extra_update_info: extra_info.clone(),
                    extra_update_info_driver: extra_info_driver_arc,
                    tracking_signals: 0,
                };
                map.assets.insert(self.asset_id, mirror);
                map.init_requests.insert(self.asset_id);
                (asset_state, extra_info)
            }
        };

        trace!("sending back signal response for {}", type_name::<A>());
        let _ = self.response_tx.send(AssetMirrorRequestResponse {
            asset_state,
            extra_info,
        });
    }
}

#[derive(Resource)]
pub struct PendingAssetTrackingDeltas<A: DioxusAssetSync> {
    pending: Vec<(AssetId<A>, i32)>,
    _phantom: PhantomData<A>,
}

impl<A: DioxusAssetSync> Default for PendingAssetTrackingDeltas<A> {
    fn default() -> Self {
        Self {
            pending: Default::default(),
            _phantom: Default::default(),
        }
    }
}

fn apply_tracking_queries_delta<A: DioxusAssetSync>(
    mut mirrors: ResMut<AssetMirrorMap<A>>,
    mut tracking_delta: ResMut<PendingAssetTrackingDeltas<A>>,
) {
    for (id, delta) in tracking_delta.pending.drain(..) {
        trace!("processing increment: {}  {}", id, delta);
        let clear = {
            let Some(entry) = mirrors.assets.get_mut(&id) else {
                error!(
                    "delta change request recieved by asset doesn't exist in map? How did this happen?"
                );
                continue;
            };
            trace!(
                "ENTRY NEW TRACKING DELTA FOR ASSET ID {}: {} -> {}",
                id,
                entry.tracking_signals,
                entry.tracking_signals + delta
            );

            entry.tracking_signals += delta;

            if entry.tracking_signals <= 0 {
                true
            } else {
                false
            }
        };
        if clear {
            trace!(
                "last signal referencing {}, dropped. removing un-used asset mirror from map",
                id
            );
            mirrors.assets.remove(&id);
        }
    }
}

/// update number of signals tracking an asset(for cleanup on un-monitored assets)
pub struct UpdateTrackingAssets<A: DioxusAssetSync> {
    delta: i32,
    asset_id: AssetId<A>,
    _phantom: PhantomData<A>,
}

impl<A: DioxusAssetSync> Command for UpdateTrackingAssets<A> {
    fn apply(self, world: &mut World) -> () {
        let mut pending_delta = world.get_resource_or_init::<PendingAssetTrackingDeltas<A>>();
        pending_delta.pending.push((self.asset_id, self.delta));
    }
}

/// an asset that may or may not exist when requested.
///
/// due to hook rules, hooks cannot conditionally exist,
///
/// so this signal will either will return an underlying asset or an error that the provided asset id for it doesn't exist
#[derive(Clone)]
pub struct AssetMaybeMirrorSignal<A: DioxusAssetSync> {
    value: Signal<Arc<Result<A, AssetNoneState>>>,
    /// `None` until the Bevy round-trip completes (non-blocking). Writes are
    /// silently ignored while the signal is still pending.
    signal: Signal<Option<QueuedSignal<Result<A, AssetNoneState>>>>,
    extra_info: Signal<Result<Arc<AssetUpdateExtraInfo<A>>, AssetNoneState>>,
    health: Signal<HealthStatus>,
}

impl<A: DioxusAssetSync> Copy for AssetMaybeMirrorSignal<A> {}

impl<A: DioxusAssetSync> AssetMaybeMirrorSignal<A> {
    pub fn mutate<F>(&self, f: F)
    where
        F: Fn(&mut A) + Send + Sync + 'static,
    {
        let signal_guard = self.signal.read();
        let Some(signal) = signal_guard.as_ref() else {
            return;
        };
        signal.mutate(move |state| {
            if let Ok(asset) = state {
                f(asset)
            }
        });
        drop(signal_guard);
        let Ok(extra_info) = &*self.extra_info.read() else {
            return;
        };
        // notify bevy that the inner value was changed
        let _ = extra_info
            .changed_sender
            .send(extra_info.asset_id)
            .inspect_err(|err| warn!("{err}"));
    }

    /// Read asset state
    pub fn read(&self) -> SignalReadGuard<'_, Arc<Result<A, AssetNoneState>>> {
        SignalReadGuard::new(self.value.read())
    }
    pub fn health(&self) -> HealthStatus {
        *self.health.read()
    }

    /// Read + map asset state
    pub fn read_ok<U>(&self, f: impl FnOnce(&A) -> U) -> Result<U, AssetNoneState> {
        let guard = self.value.read();
        match guard.as_ref() {
            Ok(t) => Ok(f(t)),
            Err(e) => Err(e.clone()),
        }
    }
}

use std::fmt::Debug;

/// Returns immediately with `Fetching` state.  The real mirror is fetched
/// asynchronously once the asset ID becomes available via the `id` memo,
/// and installed when the Bevy world processes the request.
///
/// Unlike the previous implementation, this does NOT use a placeholder UUID
/// and swap mechanism — it defers creation of the Bevy-side mirror until
/// the real [`AssetId`] is known, eliminating a class of race conditions
/// where the mirror could get stuck in a `NonAsset` state.
pub fn use_bevy_asset<A: DioxusAssetSync + Debug>(
    id: Memo<Result<AssetId<A>, AssetNoneState>>,
) -> AssetMaybeMirrorSignal<A> {
    let ctx = use_context::<CommandQueueSender>();

    // Value/health signals start in Fetching state — same as before.
    let mut asset_value_signal = use_signal(|| Arc::new(Err(AssetNoneState::Fetching)));
    let mut health_signal = use_signal(|| HealthStatus::Healthy);
    let mut extra_info_signal: Signal<Result<Arc<AssetUpdateExtraInfo<A>>, AssetNoneState>> =
        use_signal(|| Err(AssetNoneState::Fetching));
    let mut writer_signal: Signal<Option<QueuedSignal<Result<A, AssetNoneState>>>> =
        use_signal(|| None);

    // Tracks which asset ID we have sent a mirror request for,
    // and whether the +1 tracking command has been dispatched.
    let mut sent_id = use_signal(|| None::<AssetId<A>>);
    let mut tracking_active = use_signal(|| false);

    // ---- Deferred mirror creation: wait for a real AssetId ----
    //
    // Previously this used a random UUID placeholder + swap mechanism
    // (InitializeSignalAssetIdRequest + update_signals_with_initialized_ids).
    // That created a race window where init_requested_asset_mirrors would
    // process the placeholder UUID, set NonAsset, and the subsequent swap
    // to the real ID could leave the asset stuck at NonAsset.
    //
    // Now we simply wait until `id` resolves to Ok(real_asset_id) before
    // creating the mirror — no placeholder, no swap, no race.
    let ctx2 = ctx.clone();
    use_effect(move || {
        let Ok(asset_id) = id.read().clone() else {
            trace!("use_bevy_asset effect: id not ready yet");
            return; // ID not yet available — stay in Fetching
        };

        // Avoid re-requesting if we're already working on this ID
        // or have already set up the mirror.
        if *sent_id.read() == Some(asset_id) || writer_signal.read().is_some() {
            trace!(
                "use_bevy_asset effect: already sent for {:?}, skipping",
                asset_id
            );
            return;
        }
        sent_id.set(Some(asset_id));
        trace!(
            "use_bevy_asset effect: spawning mirror request for {:?}",
            asset_id
        );

        let ctx = ctx2.clone();
        spawn(async move {
            let resp = match ctx
                .send_command_async(|tx| {
                    let mut q = CommandQueue::default();
                    q.push(RequestBevyAssetMirror::<A> {
                        response_tx: tx,
                        asset_id,
                    });
                    q
                })
                .await
            {
                Ok(r) => r,
                Err(_) => {
                    trace!(
                        "use_bevy_asset spawn: send_command_async failed for {:?}",
                        asset_id
                    );
                    return;
                }
            };

            // Eagerly set signals from current queued state values so they
            // are immediately available (e.g. extra_info_signal needed by
            // mutate() to send change notifications back to Bevy), then
            // subscribe to future changes via forward_to.
            //
            // forward_to uses nr.changed() which only fires on the *next*
            // publish; by the time we subscribe here the driver may already
            // have published the initial value, so we'd miss it and the
            // signals would stay at their initial Fetching/Err state.
            let extra_info_arc = resp.extra_info.read().clone();
            extra_info_signal.set(Ok(extra_info_arc));
            let asset_arc = resp.asset_state.read().clone();
            asset_value_signal.set(asset_arc);

            trace!(
                "use_bevy_asset spawn: response received, asset_value={:?}",
                *asset_value_signal.read()
            );

            // Forward future changes into the hook signals
            resp.asset_state
                .state
                .forward_to(asset_value_signal, health_signal, |arc| arc);
            resp.extra_info
                .state
                .forward_to(extra_info_signal, health_signal, |arc| Ok(arc));

            // Mount: send +1 tracking
            let asset_id = resp.extra_info.read().asset_id;
            trace!("asset signal mounted, sending +1 for {:?}", asset_id);
            let mut q = CommandQueue::default();
            q.push(UpdateTrackingAssets::<A> {
                delta: 1,
                asset_id,
                _phantom: PhantomData,
            });
            let _ = ctx.tx.send(q);

            tracking_active.set(true);
            writer_signal.set(Some(resp.asset_state.clone()));
            trace!("use_bevy_asset spawn: setup complete for {:?}", asset_id);
        });
    });

    // Unmount: send -1 tracking (only if +1 was sent)
    let r = ctx.clone();
    use_drop(move || {
        if !*tracking_active.read() {
            return;
        }
        if let Some(asset_id) = *sent_id.read() {
            trace!("asset signal dropped, sending -1 for {:?}", asset_id);
            let mut q = CommandQueue::default();
            q.push(UpdateTrackingAssets::<A> {
                delta: -1,
                asset_id,
                _phantom: PhantomData,
            });
            let _ = r.tx.send(q);
        }
    });

    AssetMaybeMirrorSignal {
        value: asset_value_signal,
        health: health_signal,
        signal: writer_signal,
        extra_info: extra_info_signal,
    }
}
