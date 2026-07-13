//! Bevy asset mirroring via QueuedSignals.
//!
//! Provides [`use_bevy_asset`] to create dioxus-side signal mirrors
//! of bevy assets, with automatic bidirectional synchronization and
//! cleanup of unused mirrors.

pub use std::{
    any::{TypeId, type_name},
    collections::{HashMap, HashSet},
    marker::PhantomData,
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

pub(crate) use crate::macros::*;
use crate::schedules::{
    DioxusSyncLast, DioxusSyncPostUpdate, DioxusSyncPreUpdate, DioxusSyncUpdate,
};
use bevy_asset::{Asset, AssetEvent, AssetId, AssetServer, Assets, LoadState};
use bevy_ecs::{prelude::*, world::CommandQueue};
use dioxus_core::{spawn, use_drop};
use dioxus_hooks::{use_context, use_effect, use_signal};
use dioxus_signals::{Memo, ReadableExt, Signal, WritableExt};
use flume::{Receiver, Sender};
use parking_lot::Mutex;
use queued_signal::state::{HealthStatus, QueuedSignal, SignalReadGuard, WriterDriver};
use tokio::sync::oneshot;
use trait_set::trait_set;

use crate::{CommandQueueSender, add_systems_through_world};

trait_set! {
    /// Trait alias for assets that can be synced with dioxus.
    pub trait DioxusAssetSync = Asset + Clone + Send + Sync + 'static;
}

/// Current state of an asset in the asset server.
#[derive(Clone, Debug)]
pub enum AssetState<A: DioxusAssetSync> {
    /// The asset is fully loaded.
    Loaded(A),
    /// The asset is still loading.
    Loading,
}

impl<A: DioxusAssetSync> AssetState<A> {
    /// Returns a string describing the asset state.
    pub fn as_string(&self) -> &'static str {
        match self {
            AssetState::Loaded(_n) => "Loaded",
            AssetState::Loading => "Loading",
        }
    }
}

/// Error/loading states for an asset that may not be available.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AssetNoneState {
    /// Asset is still loading.
    Loading,
    /// Asset was not found in the asset server.
    NotLoaded,
    /// The requested handle does not correspond to an asset.
    NonAsset,
    /// The mirror request has been sent but not yet responded to.
    Fetching,
    /// An error occurred while fetching the asset.
    Error(String),
}

impl Display for AssetNoneState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            AssetNoneState::Loading => "Loading",
            AssetNoneState::NonAsset => "NonAsset",
            AssetNoneState::Error(err) => err,
            AssetNoneState::NotLoaded => "NotLoaded",
            AssetNoneState::Fetching => "Fetching",
        };
        write!(f, "{}", value)
    }
}

/// Extra metadata for asset update propagation.
#[derive(Clone, Debug)]
pub struct AssetUpdateExtraInfo<A: DioxusAssetSync> {
    changed_sender: Sender<AssetId<A>>,
    asset_id: AssetId<A>,
}

/// Stores a dioxus signal that mirrors a bevy asset.
/// Dioxus hooks cannot conditionally exist, so this returns either
/// the asset or a none-state when the asset is unavailable.
pub struct AssetMaybeMirror<A: DioxusAssetSync> {
    state: QueuedSignal<Result<A, AssetNoneState>>,
    extra_update_info: QueuedSignal<AssetUpdateExtraInfo<A>>,
    state_driver: Arc<Mutex<WriterDriver<Result<A, AssetNoneState>>>>,
    extra_update_info_driver: Arc<Mutex<WriterDriver<AssetUpdateExtraInfo<A>>>>,
    /// Number of signals actively reading this asset mirror.
    ///
    /// Asset mirror is cleaned up when this hits zero.
    tracking_signals: i32,
}

/// Maps asset IDs to their dioxus mirror state.
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

/// Set of asset IDs whose dioxus-side mirrors have changed.
#[derive(Resource)]
pub struct ChangedAssetMirrors<A: DioxusAssetSync>(HashSet<AssetId<A>>);

impl<A: DioxusAssetSync> Default for ChangedAssetMirrors<A> {
    fn default() -> Self {
        Self(Default::default())
    }
}

/// Flume receiver for changed asset IDs.
#[derive(Resource)]
pub struct ChangedIdsReceiver<A: DioxusAssetSync>(Receiver<AssetId<A>>);

/// Flume sender for changed asset IDs.
#[derive(Resource, Clone)]
pub struct ChangedIdsSender<A: DioxusAssetSync>(Sender<AssetId<A>>);

/// Tick all asset mirror drivers.
pub fn drive_maybe_assets<A: DioxusAssetSync>(mut mirrors: ResMut<AssetMirrorMap<A>>) {
    for asset in mirrors.assets.values_mut() {
        let mut guard = asset.state_driver.lock();
        guard.tick(Duration::ZERO);

        let mut guard = asset.extra_update_info_driver.lock();
        guard.tick(Duration::ZERO);
    }
}

/// Clear pending asset initialization requests.
pub fn clear_asset_init_requests<A: DioxusAssetSync>(mut mirrors: ResMut<AssetMirrorMap<A>>) {
    mirrors.init_requests.clear();
}

/// Initialize an asset mirror from a requested ID.
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
            Some(state) => match state {
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
            },
            // get_load_state returns None for procedurally-generated assets
            // (those added via Assets::add() rather than loaded through the
            // AssetServer). The asset may still exist in Assets<A> check
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
                Err(e) => e.to_string(),
            }
        );
        entry.state.set_value(fetch);
    }
}

/// Synchronize bevy-side asset changes to the dioxus mirrors.
pub fn sync_mirrors_to_assets<A: DioxusAssetSync>(
    mut events: MessageReader<AssetEvent<A>>,
    assets: Res<Assets<A>>,
    mirrors: Res<AssetMirrorMap<A>>,
    changed: Res<ChangedAssetMirrors<A>>,
) {
    for event in events.read() {
        let id = match event {
            AssetEvent::Modified { id } | AssetEvent::LoadedWithDependencies { id } => id,
            _ => continue,
        };
        // don't sync mirror asset to asset on the same frame it was set in order to stop an infinite change loop
        if changed.0.contains(id) {
            trace!("changed includes {}, skipping", id);
            continue;
        }

        if let Some(entry) = mirrors.assets.get(id)
            && let Some(a) = assets.get(*id)
        {
            trace!("setting new asset value for {} on event: {:#?}", id, event);

            entry.state.set_value(Ok(a.clone()));
        }
    }
}

/// Synchronize dioxus-side mirror changes back to the bevy assets.
pub fn sync_assets_to_mirrors<A: DioxusAssetSync>(
    mut assets: ResMut<Assets<A>>,
    changed: Res<ChangedAssetMirrors<A>>,
    mirrors: Res<AssetMirrorMap<A>>,
) {
    for id in &changed.0 {
        let Some(mut asset) = assets.get_mut(*id) else {
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

/// Drain the changed-ID receiver into [`ChangedAssetMirrors`].
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

/// Clear the changed-asset flags after processing.
pub fn clear_changed_flags<A: DioxusAssetSync>(mut changed: ResMut<ChangedAssetMirrors<A>>) {
    trace!("clearing changed assets");
    changed.bypass_change_detection().0.clear();
}

#[derive(Resource)]
/// Marker resource indicating asset sync systems have been registered.
pub struct AssetSyncInitialized<A: DioxusAssetSync> {
    _phantom: PhantomData<A>,
}

#[derive(Resource, Clone, Debug)]
/// Response from a bevy asset mirror request, containing the signals.
pub struct AssetMirrorRequestResponse<A: DioxusAssetSync> {
    /// The asset's current state (loaded value or error).
    pub asset_state: QueuedSignal<Result<A, AssetNoneState>>,
    /// Extra update metadata for the asset.
    pub extra_info: QueuedSignal<AssetUpdateExtraInfo<A>>,
    /// `true` when the mirror was just created (the initial reader
    /// is pre-counted and does not need to send a +1 tracking delta).
    pub first_reader: bool,
}

/// Command requesting a mirror for a specific bevy asset.
pub struct RequestBevyAssetMirror<A: DioxusAssetSync> {
    response_tx: oneshot::Sender<AssetMirrorRequestResponse<A>>,
    asset_id: AssetId<A>,
}

impl<A: DioxusAssetSync> Command for RequestBevyAssetMirror<A> {
    type Out = ();

    fn apply(self, world: &mut World) {
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
                add_systems_through_world(world, DioxusSyncPreUpdate, sync_mirrors_to_assets::<A>);
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

        let (asset_state, extra_info, first_reader) = match map.assets.get_mut(&self.asset_id) {
            Some(asset) => {
                trace!(
                    "requested new signal and set new tracking value for {} -> {}",
                    asset.tracking_signals,
                    asset.tracking_signals + 1
                );
                (asset.state.clone(), asset.extra_update_info.clone(), false)
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
                    // Seed at 1 so a concurrent unmount delta cannot
                    // drop the count below zero before the initial
                    // reader's +1 command is processed.
                    tracking_signals: 1,
                };
                map.assets.insert(self.asset_id, mirror);
                map.init_requests.insert(self.asset_id);
                (asset_state, extra_info, true)
            }
        };

        trace!("sending back signal response for {}", type_name::<A>());
        let _ = self.response_tx.send(AssetMirrorRequestResponse {
            asset_state,
            extra_info,
            first_reader,
        });
    }
}

#[derive(Resource)]
/// Accumulated tracking delta requests for batch processing.
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
                    "delta change request received by asset doesn't exist in map? How did this happen?"
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

            entry.tracking_signals <= 0
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
    type Out = ();

    fn apply(self, world: &mut World) {
        let mut pending_delta = world.get_resource_or_init::<PendingAssetTrackingDeltas<A>>();
        pending_delta.pending.push((self.asset_id, self.delta));
    }
}

/// An asset that may or may not exist when requested.
/// Returns either the underlying asset or an none-state.
#[derive(Clone)]
pub struct AssetMaybeMirrorSignal<A: DioxusAssetSync> {
    value: Signal<Arc<Result<A, AssetNoneState>>>,
    /// None until the bevy round-trip completes.
    /// Writes are silently ignored while pending.
    signal: Signal<Option<QueuedSignal<Result<A, AssetNoneState>>>>,
    extra_info: Signal<Result<Arc<AssetUpdateExtraInfo<A>>, AssetNoneState>>,
    health: Signal<HealthStatus>,
}

impl<A: DioxusAssetSync> Copy for AssetMaybeMirrorSignal<A> {}

impl<A: DioxusAssetSync> AssetMaybeMirrorSignal<A> {
    /// Enqueues a relative mutation
    pub fn mutate<F>(&self, f: F)
    where
        F: Fn(&mut A) + Send + Sync + 'static,
    {
        let signal_guard = self.signal.read();
        let Some(signal) = signal_guard.as_ref() else {
            warn!(
                "AssetMaybeMirrorSignal::mutate dropped: writer not yet available (Bevy round-trip pending)"
            );
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
    /// Returns signal health
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

use std::fmt::{Debug, Display};

/// Returns immediately with a `Fetching` state.
/// The real mirror is fetched asynchronously once the
/// asset ID becomes available and installed when bevy
/// processes the request.
pub fn use_bevy_asset<A: DioxusAssetSync + Debug>(
    id: Memo<Result<AssetId<A>, AssetNoneState>>,
) -> AssetMaybeMirrorSignal<A> {
    let ctx = use_context::<CommandQueueSender>();

    // Value/health signals start in Fetching state, same as before.
    let mut asset_value_signal = use_signal(|| Arc::new(Err(AssetNoneState::Fetching)));
    let health_signal = use_signal(|| HealthStatus::Healthy);
    let mut extra_info_signal: Signal<Result<Arc<AssetUpdateExtraInfo<A>>, AssetNoneState>> =
        use_signal(|| Err(AssetNoneState::Fetching));
    let mut writer_signal: Signal<Option<QueuedSignal<Result<A, AssetNoneState>>>> =
        use_signal(|| None);

    // Check for if a mirror request is in flight to bevy. Prevents
    // spawning duplicate tasks when the effect re-runs before
    // the previous response arrives.
    let mut in_flight = use_signal(|| false);
    let mut tracking_active = use_signal(|| false);

    // Wait for a real AssetId before creating the mirror.
    let ctx2 = ctx.clone();
    use_effect(move || {
        let Ok(asset_id) = id.read().clone() else {
            trace!("id not ready yet");
            return;
        };

        let writer_signal_setup = writer_signal.read().is_some();
        let in_flight_status = *in_flight.read();
        // Skip if the mirror is already set up or a request is in flight. 
        if writer_signal_setup || in_flight_status {
            trace!("asset mirror setup skipped: signal is some: {}, in flight: {}", writer_signal_setup, in_flight_status);
            return;
        }
        in_flight.set(true);
        trace!(
            "spawning mirror request for {:?}",
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
                        "send_command_async failed for {:?}",
                        asset_id
                    );
                    in_flight.set(false);
                    return;
                }
            };

            // Eagerly set signals from current values so they
            // are immediately available, then subscribe to
            // future changes.
            let extra_info_arc = resp.extra_info.read().clone();
            extra_info_signal.set(Ok(extra_info_arc));
            let asset_arc = resp.asset_state.read().clone();
            asset_value_signal.set(asset_arc);

            trace!(
                "response received, asset_value={:?}",
                *asset_value_signal.read()
            );

            // Forward future changes into the hook signals.
            resp.asset_state
                .state
                .forward_to(asset_value_signal, health_signal, |arc| arc);
            resp.extra_info
                .state
                .forward_to(extra_info_signal, health_signal, Ok);

            // Mount: send tracking increment only for subsequent
            // readers the initial reader is pre-counted.
            if !resp.first_reader {
                let asset_id = resp.extra_info.read().asset_id;
                trace!("asset signal mounted, sending +1 for {:?}", asset_id);
                let mut q = CommandQueue::default();
                q.push(UpdateTrackingAssets::<A> {
                    delta: 1,
                    asset_id,
                    _phantom: PhantomData,
                });
                let _ = ctx.tx.send(q);
            }

            tracking_active.set(true);
            writer_signal.set(Some(resp.asset_state.clone()));
            in_flight.set(false);
            trace!("setup complete for {:?}", asset_id);
        });
    });

    // Unmount: send tracking decrement.
    let r = ctx.clone();
    use_drop(move || {
        if !*tracking_active.read() {
            return;
        }
        let extra_info = extra_info_signal.read();
        if let Ok(ref extra) = *extra_info {
            let asset_id = extra.asset_id;
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
