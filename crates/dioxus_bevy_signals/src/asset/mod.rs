use std::ops::Deref;
pub use std::{
    any::{TypeId, type_name},
    collections::{HashMap, HashSet},
    marker::PhantomData,
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

pub(crate) use crate::macros::*;
use bevy_app::{Last, PostUpdate, Update};
use bevy_asset::{Asset, AssetEvent, AssetId, AssetServer, Assets, LoadState, uuid::Uuid};
use bevy_ecs::{prelude::*, world::CommandQueue};
use bytemuck::{TransparentWrapper, TransparentWrapperAlloc};
use dioxus_core::{use_drop, use_hook};
use dioxus_hooks::{use_context, use_effect, use_memo, use_signal};
use dioxus_signals::{Memo, ReadableExt, Signal, WritableExt};
use flume::{Receiver, Sender};
use linked_hash_set::LinkedHashSet;
use parking_lot::Mutex;
use queued_signal::signal::{HealthStatus, QueuedSignal, WriterDriver};
use trait_set::trait_set;

use crate::{CommandQueueSender, add_systems_through_world};

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

#[derive(Clone, Debug)]
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

#[derive(TransparentWrapper, Clone, Debug)]
#[repr(transparent)]
pub struct AssetFetch<A: DioxusAssetSync>(Result<A, AssetNoneState>);

impl<A: DioxusAssetSync> AssetFetch<A> {
    pub fn as_string(&self) -> String {
        let value = match &self.0 {
            Ok(_asset) => "Loaded".to_string(),
            Err(err) => err.as_string(),
        };
        value
    }
}

#[derive(TransparentWrapper, Clone, Debug)]
#[repr(transparent)]
pub struct AssetMaybeMirrorState<A: DioxusAssetSync> {
    state: Result<A, AssetNoneState>,
}

impl<A: DioxusAssetSync> Deref for AssetMaybeMirrorState<A> {
    type Target = Result<A, AssetNoneState>;

    fn deref(&self) -> &Self::Target {
        &self.state
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
    pub state: QueuedSignal<AssetMaybeMirrorState<A>>,
    /// second signal for extra info needed for updating assets (change detection, asset id, etc..)
    /// kept seperate to not clone channels on update, and to allow transparent wrapper condense if let somes with arc transmute
    pub extra_update_info: QueuedSignal<AssetUpdateExtraInfo<A>>,
    state_driver: Arc<Mutex<WriterDriver<AssetMaybeMirrorState<A>>>>,
    extra_update_info_driver: Arc<Mutex<WriterDriver<AssetUpdateExtraInfo<A>>>>,
    /// number of signals that are actively reading this asset mirror.
    ///
    /// once this hits zero(last dioxus component reading this is dropped), the asset mirror map clears this entry from it self.
    tracking_signals: i32,
}

#[derive(Resource)]
pub struct AssetMirrorMap<A: DioxusAssetSync> {
    assets: HashMap<AssetId<A>, AssetMaybeMirror<A>>,
    asset_id_initialize_tickets: LinkedHashSet<RequestAssetIdTicket>,
    init_requests: HashSet<AssetId<A>>,
}

impl<A: DioxusAssetSync> Default for AssetMirrorMap<A> {
    fn default() -> Self {
        Self {
            assets: Default::default(),
            init_requests: Default::default(),
            asset_id_initialize_tickets: Default::default(),
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
        trace!("processing asset init request");

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
                        let fetch = match assets.get(id) {
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
                        };
                        fetch
                    }
                    LoadState::Failed(asset_load_error) => {
                        Err(AssetNoneState::Error(asset_load_error.to_string()))
                    }
                };
                asset_fetch
            }
            None => {
                // println!("NO ASSET found for {}", id);
                Err(AssetNoneState::NonAsset)
            }
        };
        // println!("ASSET INIT REQUEST RESULT: {:#?}", fetch);
        // let old_state = entry.state.read();
        entry
            .state
            .set_value(Arc::new(AssetMaybeMirrorState { state: fetch }));
    }
}

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
        // dont re-set the asset on the same frame that it was set to a new value to stop infinite loops
        if changed.0.contains(id) == true {
            trace!("chhanged includes {}, skipping", id);
            continue;
        }
        if let Some(entry) = mirrors.assets.get(id) {
            if let Some(a) = assets.get(*id) {
                // let old_state = entry.state.read();
                entry.state.set_value(Arc::new(AssetMaybeMirrorState {
                    state: Ok(a.clone()),
                }));
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
        // println!("attempting to change value to new mirror value");

        let Some(asset) = assets.get_mut(*id) else {
            // println!("asset {:#?} not found, contained asset ids: {:#?}", id, assets.ids().map(|n| n.untyped()).collect::<Vec<_>>());
            continue;
        };
        let Some(handle) = mirrors.assets.get(id) else {
            // println!("changed asset {id} has no mirror handle");
            continue;
        };
        let state = handle.state.read();

        match &state.as_ref().state {
            Ok(mirror) => {
                *asset = mirror.clone();
            }
            Err(_) => {}
        }
    }
}

pub fn collect_changed_ids<A: DioxusAssetSync>(
    rx: Res<ChangedIdsReceiver<A>>,
    mut changed: ResMut<ChangedAssetMirrors<A>>,
) {
    while let Ok(id) = rx.0.try_recv() {
        changed.0.insert(id);
    }
}

pub fn clear_changed_flags<A: DioxusAssetSync>(mut changed: ResMut<ChangedAssetMirrors<A>>) {
    changed.0.clear();
}

pub struct AssetInitResponse<A: DioxusAssetSync> {
    signal: QueuedSignal<AssetMaybeMirrorState<A>>,
}

#[derive(Resource)]
pub struct AssetSyncInitialized<A: DioxusAssetSync> {
    _phantom: PhantomData<A>,
}

/// id of asset signal. used for initializing signals.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct AssetSignalId {
    id: u32,
}

#[derive(Resource)]
pub struct RegisteredAssetSignals<A: DioxusAssetSync> {
    ids: LinkedHashSet<AssetSignalId>,
    _phantom: PhantomData<A>,
}

impl<A: DioxusAssetSync> Default for RegisteredAssetSignals<A> {
    fn default() -> Self {
        Self {
            ids: Default::default(),
            _phantom: Default::default(),
        }
    }
}

#[derive(Hash, PartialEq, Eq, Clone, Copy)]
pub struct RequestAssetIdTicket {
    ticket_id: u32,
}

pub struct InitializeSignalAssetIdRequest<A: DioxusAssetSync> {
    // signal: QueuedSignal<AssetMaybeMirrorState<A>>,
    // signal_extra_info: QueuedSignal<AssetUpdateExtraInfo<A>>,
    new_id: AssetId<A>,
    // requesting_signal_ticket: RequestAssetIdTicket,
    old_id: AssetId<A>,
}

#[derive(Resource)]
pub struct UpdateAssetSignalSender<A: DioxusAssetSync> {
    tx: Sender<InitializeSignalAssetIdRequest<A>>,
}

#[derive(Resource)]
pub struct UpdateAssetSignalReciever<A: DioxusAssetSync> {
    rx: Receiver<InitializeSignalAssetIdRequest<A>>,
}

/// take currently signals that have uninitailized asset ids, point them to their new initialized asset id value
fn update_signals_with_initialized_ids<A: DioxusAssetSync>(
    requests: Res<UpdateAssetSignalReciever<A>>,
    mut map: ResMut<AssetMirrorMap<A>>,
) {
    while let Ok(request) = requests.rx.try_recv() {
        // don't remove mirror thats already been removed to stop use after free
        let old_id = request.old_id;
        if !map.assets.contains_key(&old_id) {
            continue;
        }

        // let old_id = request.signal_extra_info.read().asset_id.clone();

        let Some((_, mirror)) = map.assets.remove_entry(&old_id) else {
            error!("requested asset id for update is invalid? {}", old_id);
            continue;
        };

        // let signal = &mirror.state;
        let signal = &mirror.extra_update_info;

        signal.mutate(move |n| n.asset_id = request.new_id);
        // force send changed in order to refresh asset. Otherwise, asset reports as still none.
        let _ = signal
            .read()
            .changed_sender
            .send(request.new_id)
            .inspect_err(|err| println!("{err}"));

        map.init_requests.insert(request.new_id);
        map.assets.insert(request.new_id, mirror);
    }
}

#[derive(Resource, Clone)]
pub struct AssetMirrorRequestResponse<A: DioxusAssetSync> {
    asset_state: QueuedSignal<AssetMaybeMirrorState<A>>,
    extra_info: QueuedSignal<AssetUpdateExtraInfo<A>>,
    // asset_id_initialize_ticket: RequestAssetIdTicket,
    initialize_request_tx: Sender<InitializeSignalAssetIdRequest<A>>,
}

pub struct RequestBevyAssetMirror<A: DioxusAssetSync> {
    response_tx: Sender<AssetMirrorRequestResponse<A>>,
    asset_id: AssetId<A>,
}

impl<A: DioxusAssetSync> Command for RequestBevyAssetMirror<A> {
    fn apply(self, world: &mut World) -> () {
        // let mut ticket_id = None;

        match world.get_resource::<AssetSyncInitialized<A>>() {
            Some(_) => {}
            None => {
                let (changed_tx, changed_rx) = flume::unbounded();
                let (initialize_asset_id_tx, initialize_asset_id_rx) = flume::unbounded();

                world.insert_resource(AssetMirrorMap::<A>::default());
                world.insert_resource(ChangedAssetMirrors::<A>::default());
                world.insert_resource(ChangedIdsReceiver::<A>(changed_rx));
                world.insert_resource(ChangedIdsSender::<A>(changed_tx));
                world.insert_resource(UpdateAssetSignalReciever::<A> {
                    rx: initialize_asset_id_rx,
                });
                world.insert_resource(UpdateAssetSignalSender::<A> {
                    tx: initialize_asset_id_tx,
                });
                world.insert_resource(PendingAssetTrackingDeltas::<A>::default());
                world.insert_resource(RegisteredAssetSignals::<A>::default());

                add_systems_through_world(world, Update, collect_changed_ids::<A>);
                add_systems_through_world(world, Update, drive_maybe_assets::<A>);
                add_systems_through_world(world, Update, update_signals_with_initialized_ids::<A>);
                add_systems_through_world(world, PostUpdate, init_requested_asset_mirrors::<A>);
                add_systems_through_world(world, PostUpdate, sync_mirrors_to_assets::<A>);
                add_systems_through_world(
                    world,
                    PostUpdate,
                    sync_assets_to_mirrors::<A>.run_if(resource_changed::<ChangedAssetMirrors<A>>),
                );
                add_systems_through_world(
                    world,
                    PostUpdate,
                    apply_tracking_queries_delta::<A>
                        .run_if(resource_changed::<PendingAssetTrackingDeltas<A>>),
                );
                add_systems_through_world(
                    world,
                    Last,
                    clear_changed_flags::<A>.run_if(resource_changed::<ChangedAssetMirrors<A>>),
                );
                add_systems_through_world(world, Last, clear_asset_init_requests::<A>);

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
                let asset_state_driver = WriterDriver::new(AssetMaybeMirrorState {
                    state: Err(AssetNoneState::Fetching),
                });

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

        let initialize_request_tx = world.resource::<UpdateAssetSignalSender<A>>();

        trace!("SENDING BACK NEW SIGNAL RESPONSE");
        let _ = self.response_tx.send(AssetMirrorRequestResponse {
            asset_state,
            initialize_request_tx: initialize_request_tx.tx.clone(),
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
        trace!("PROCESSING INCREMENT FOR: {}:  {}", id, delta);
        let clear = {
            let Some(entry) = mirrors.assets.get_mut(&id) else {
                println!(
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
        // pending_delta.pending += self.delta
    }
}

/// an asset that may or may not exist when requested.
///
/// due to hook rules, hooks cannot conditionally exist,
///
/// so this signal will either will return an underlying asset or an error that the provided asset id for it doesn't exist
#[derive(Clone)]
pub struct AssetMaybeMirrorSignal<A: DioxusAssetSync> {
    value: Signal<Option<Arc<AssetMaybeMirrorState<A>>>>,
    signal: Signal<QueuedSignal<AssetMaybeMirrorState<A>>>,
    extra_info: Signal<Option<Arc<AssetUpdateExtraInfo<A>>>>,
    health: Signal<HealthStatus>,
    /// dummy always Err value for allowing reading asset mirror without cloning an arc.
    dummy_value: Signal<Arc<Result<A, AssetNoneState>>>, // signal_id: AssetSignalId
}

impl<A: DioxusAssetSync> Copy for AssetMaybeMirrorSignal<A> {}

impl<A: DioxusAssetSync> AssetMaybeMirrorSignal<A> {
    pub fn mutate<F>(&self, f: F)
    where
        F: Fn(&mut A) + Send + Sync + 'static,
    {
        self.signal.read().mutate(move |state| {
            if let Ok(asset) = &mut state.state {
                f(asset)
            }
        });
        let Some(extra_info) = &*self.extra_info.read() else {
            return;
        };
        // notify bevy that the inner value was changed
        let _ = extra_info
            .changed_sender
            .send(extra_info.asset_id)
            .inspect_err(|err| warn!("{err}"));
    }
    /// read inner value
    pub fn read(&self) -> Arc<Result<A, AssetNoneState>> {
        let value = self.value.deref();
        let value = value();

        let value = match value {
            Some(value) => {
                let value = TransparentWrapperAlloc::peel_arc(value);

                value
            }
            None => self.dummy_value.deref()(),
        };
        value
    }
}
use std::fmt::Debug;

/// return the asset or a dummy
pub fn use_bevy_asset<A: DioxusAssetSync + Debug>(
    id: Memo<Option<AssetId<A>>>,
) -> AssetMaybeMirrorSignal<A> {
    let ctx = use_context::<CommandQueueSender>();

    let response = use_hook(|| {
        let initial_id = id.read().unwrap_or_else(|| {
            let random_uuid = Uuid::new_v4();
            AssetId::<A>::from(random_uuid)
        });
        ctx.send_command(|tx| {
            let mut queue = CommandQueue::default();
            queue.push(RequestBevyAssetMirror::<A> {
                response_tx: tx,
                asset_id: initial_id,
            });
            queue
        })
        .unwrap()
    });

    let (asset_value_signal, health) = response.asset_state.clone().use_hook();
    let (extra_info_signal, _) = response.extra_info.clone().use_hook();

    let mut current_asset_id = use_signal(|| response.extra_info.read().asset_id);
    use_memo(move || {
        if let Some(info) = extra_info_signal.read().as_ref() {
            current_asset_id.set(info.asset_id);
        }
    });

    let ctx_clone = ctx.clone();
    use_effect(move || {
        let asset_id = response.extra_info.read().asset_id;
        trace!("ASSET SIGNAL MOUNTED, SENDING +1 for {:?}", asset_id);
        let mut queue = CommandQueue::default();
        queue.push(UpdateTrackingAssets::<A> {
            delta: 1,
            asset_id,
            _phantom: PhantomData,
        });
        let _ = ctx_clone.tx.send(queue);
    });

    let r = ctx.clone();
    use_drop(move || {
        let asset_id = *current_asset_id.read();
        trace!("ASSET SIGNAL DROPPED, SENDING -1 for {:?}", asset_id);
        let mut queue = CommandQueue::default();
        queue.push(UpdateTrackingAssets::<A> {
            delta: -1,
            asset_id,
            _phantom: PhantomData,
        });
        let _ = r.tx.send(queue);
    });

    let mut last_requested_id = use_signal(|| *id.read());
    use_effect(move || {
        let wanted = *id.read();
        let last = *last_requested_id.read();
        if wanted != last {
            last_requested_id.set(wanted);
            if let Some(new_id) = wanted {
                let old_id = *current_asset_id.read();
                if old_id != new_id {
                    let _ = response
                        .initialize_request_tx
                        .send(InitializeSignalAssetIdRequest { new_id, old_id });
                }
            }
        }
    });

    let writer = use_signal(|| response.asset_state);

    AssetMaybeMirrorSignal {
        value: asset_value_signal,
        health,
        signal: writer,
        dummy_value: use_signal(|| Arc::new(Err(AssetNoneState::Error("…".into())))),
        extra_info: extra_info_signal,
    }
}
