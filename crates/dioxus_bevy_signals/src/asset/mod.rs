use std::{any::type_name_of_val, ops::Deref};
pub use std::{
    any::{type_name, TypeId},
    collections::{HashMap, HashSet},
    marker::PhantomData,
    sync::{Arc, Mutex},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use bevy_app::{Last, PostUpdate, Update};
use bevy_asset::{Asset, AssetEvent, AssetId, AssetServer, Assets, Handle, LoadState};
use bevy_ecs::{prelude::*, world::CommandQueue};
use bevy_log::warn;
use dioxus_core::{needs_update, use_hook};
use dioxus_hooks::{use_context, use_effect, use_future, use_memo, use_signal};
use dioxus_signals::{Memo, Readable, ReadableExt, Signal, WritableExt};
use flume::{Receiver, Sender};
use generational_box::GenerationalRef;
use linked_hash_set::LinkedHashSet;
use queued_signal::signal::{HealthStatus, QueuedSignal, TrackedReadGuard, WriterDriver};
use trait_set::trait_set;

use crate::{add_systems_through_world, CommandQueueSender};


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
            AssetState::Loaded(n) => "Loaded",
            AssetState::Loading => "Loading",
        }
    }
}

/// A fetch for an asset state. 
#[derive(Clone, Debug)]
pub enum AssetFetch<A: DioxusAssetSync> {
    Fetching,
    /// asset doesn't actually exist (removed from world, bad id, etc...)
    NonAsset,
    Fetched(AssetState<A>),
    Error(String)
}

impl<A: DioxusAssetSync> AssetFetch<A> {
    /// for printing asset fetch state without Debug on A
    pub fn as_string(&self) -> &'static str {
        match self {
            AssetFetch::Fetching => "Fetching",
            AssetFetch::NonAsset => "NonAsset",
            AssetFetch::Fetched(asset_state) => asset_state.as_string(),
            AssetFetch::Error(_) => todo!(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct AssetMaybeMirrorState<A: DioxusAssetSync> {
    state: AssetFetch<A>,
    changed_sender: Sender<AssetId<A>>,
    asset_id: AssetId<A>
}

/// stores a dioxus mirror of what may be a mirror to a real bevy asset.
/// 
/// due to dioxus hook rules, hooks cannot optionally exist, but assets may or may not exist when requested(they were despawned, asset id is wrong, etc..)
/// 
/// so, when an AssetId is requested you might get an asset, or you'll get an error that it doesn't exist/despawned, etc..
pub struct AssetMaybeMirror<A: DioxusAssetSync> {
    pub state: QueuedSignal<AssetMaybeMirrorState<A>>,
    driver: Mutex<WriterDriver<AssetMaybeMirrorState<A>>>,
    poke_tx: Sender<()>,
}


// #[derive(Clone)]
// pub struct ChangeDetectorSignal<A: DioxusAssetSync>(QueuedSignal<AssetMaybeMirrorState<A>>);

// impl<A: DioxusAssetSync> ChangeDetectorSignal<A> {
//     /// forces signal to proc change detection for sync systems when activated
//     pub fn set_value(&self, value: Arc<AssetMaybeMirrorState<A>>) {
//         self.0.set_value(value);
//         let sender = &self.0.read().changed_sender;
//         let asset_id = self.0.read().asset_id;
//         sender.send(asset_id);
    
//     }
// }

// impl<A: DioxusAssetSync> Deref for ChangeDetectorSignal<A> {
//     type Target = QueuedSignal<AssetMaybeMirrorState<A>>;

//     fn deref(&self) -> &Self::Target {
//         &self.0
//     }
// }

#[derive(Resource)]
pub struct AssetMirrorMap<A: DioxusAssetSync> {
    assets: HashMap<AssetId<A>, AssetMaybeMirror<A>>,
    asset_id_initialize_tickets: LinkedHashSet<RequestAssetIdTicket>,
    init_requests: HashSet<AssetId<A>>
}

impl<A: DioxusAssetSync> Default for AssetMirrorMap<A> {
    fn default() -> Self {
        Self { assets: Default::default(), init_requests: Default::default(), asset_id_initialize_tickets: Default::default()}
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


pub fn drive_maybe_assets<A: DioxusAssetSync>(
    mut mirrors: ResMut<AssetMirrorMap<A>>,
) {
    for (id, asset) in &mut mirrors.assets {
        if let Ok(mut guard) = asset.driver.lock().inspect_err(|err| warn!("UNABLE TO AQUIRE LOCK FOR {} FOR {}: {}", id, type_name::<A>(), err)) {
            guard.tick(Duration::ZERO);
        }
    }
}

pub fn clear_asset_init_requests<A: DioxusAssetSync>(
    mut mirrors: ResMut<AssetMirrorMap<A>>
) {
    mirrors.init_requests.clear();
}

/// initialize asset from requested id.
pub fn init_requested_asset_mirrors<A: DioxusAssetSync>(
    mut mirrors: ResMut<AssetMirrorMap<A>>,
    asset_server: Res<AssetServer>,
    assets: Res<Assets<A>>
) {
    let requests = mirrors.init_requests.clone();
    for id in requests {
        let Some(entry) = mirrors.assets.get_mut(&id) else {
            println!("how was an asset requested, but not have its uninitialized value exist in the assets list? {}", type_name::<A>());
            continue
        };
        let fetch = match asset_server.get_load_state(id) {
            Some(state) => {
                let asset_fetch = match state {
                    LoadState::NotLoaded => AssetFetch::Fetched(AssetState::Loading),
                    LoadState::Loading =>  AssetFetch::Fetched(AssetState::Loading),
                    LoadState::Loaded => {
                        let fetch = match assets.get(id) {
                            Some(asset) => {
                                AssetFetch::Fetched(AssetState::Loaded(asset.clone()))
                            },
                            None => {
                                println!("how was this asset marked as loaded without Assets<A> holding the asset? {}", type_name::<A>());
                                AssetFetch::Error("asset marked as loaded, but Assets<A> didn't have the asset".into())
                            },
                            
                        };
                        fetch
                    },
                    LoadState::Failed(asset_load_error) => AssetFetch::Error(asset_load_error.to_string()),
                };
                asset_fetch
            },
            None => {
                // println!("NO ASSET found for {}", id);
                AssetFetch::NonAsset
            },
        };
    
        let old_state = entry.state.read();
        entry.state.set_value(Arc::new(AssetMaybeMirrorState { state: fetch, changed_sender: old_state.changed_sender.clone(), asset_id: old_state.asset_id }));

    }
}

pub fn sync_mirrors_to_assets<A: DioxusAssetSync>(
    mut events: MessageReader<AssetEvent<A>>,
    assets: Res<Assets<A>>,
    mirrors: Res<AssetMirrorMap<A>>,
    changed: Res<ChangedAssetMirrors<A>>
) {
    for event in events.read() {
        let id = match event {
            AssetEvent::Modified { id } | AssetEvent::LoadedWithDependencies { id } => id,
            _ => continue,
        };
        // dont re-set the asset on the same frame that it was set to a new value to stop infinite loops
        if changed.0.contains(id) == true {
            println!("CHANGED INCLUDEDS {}, SKIPPING", id);
            continue
        }
        if let Some(entry) = mirrors.assets.get(id) {
            if let Some(a) = assets.get(*id) {
                let old_state = entry.state.read();
                entry.state.set_value(
                    Arc::new(
                        AssetMaybeMirrorState { state: AssetFetch::Fetched(AssetState::Loaded(a.clone())), changed_sender: old_state.changed_sender.clone(), asset_id: old_state.asset_id }
                    )
                );
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
            AssetFetch::Fetched(asset_state) => {
                match asset_state {
                    AssetState::Loaded(mirrored) => {
                        *asset = mirrored.clone();
                    },
                    AssetState::Loading => {
                        // println!("asset not loaded, skipping")
                    },
                }
            },
            AssetFetch::Fetching => {
                // println!("asset is fetching");
            },
            AssetFetch::NonAsset => {
                // println!("asset is non-asset")
            },
            AssetFetch::Error(err) => {
                println!("could not sync asset: {}",err);
            },
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
    // if changed.0.len() > 0 {
    //     println!("cleared changed: {:#?}", changed.0);
    // }
    changed.0.clear();
}

pub struct AssetInitResponse<A: DioxusAssetSync> {
    signal: QueuedSignal<AssetMaybeMirrorState<A>>,
    // signal_id: AssetSignalId,
}

#[derive(Resource)]
pub struct AssetSyncInitialized<A: DioxusAssetSync> {
    _phantom: PhantomData<A>
}

/// id of asset signal. used for initializing signals.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct AssetSignalId {
    id: u32
}

#[derive(Resource)]
pub struct RegisteredAssetSignals<A: DioxusAssetSync> {
    ids: LinkedHashSet<AssetSignalId>,
    _phantom: PhantomData<A>
}

impl<A: DioxusAssetSync> Default for RegisteredAssetSignals<A> {
    fn default() -> Self {
        Self { ids: Default::default(), _phantom: Default::default() }
    }
}

#[derive(Hash, PartialEq, Eq, Clone, Copy)]
pub struct RequestAssetIdTicket {
    ticket_id: u32
}


pub struct InitializeSignalAssetIdRequest<A: DioxusAssetSync> {
    signal: QueuedSignal<AssetMaybeMirrorState<A>>,
    new_id: AssetId<A>,
    requesting_signal_ticket: RequestAssetIdTicket
}

#[derive(Resource)]
pub struct UpdateAssetSignalSender<A: DioxusAssetSync> {
    tx: Sender<InitializeSignalAssetIdRequest<A>>,
}

#[derive(Resource)]
pub struct UpdateAssetSignalReciever<A: DioxusAssetSync> {
    rx: Receiver<InitializeSignalAssetIdRequest<A>>
    
}

/// take currently signals that have uninitailized asset ids, point them to their new initialized asset id value
fn update_signals_with_initialized_ids<A: DioxusAssetSync>(
    requests: Res<UpdateAssetSignalReciever<A>>,
    mut map: ResMut<AssetMirrorMap<A>>
) {
    while let Ok(request) = requests.rx.try_recv() {

        let old_id = request.signal.read().asset_id.clone();

        let Some((_, mirror)) = map.assets.remove_entry(&old_id) else {
            println!("requested asset id for update is invalid? {}", old_id);
            continue;
        };

        let signal = &mirror.state;

        signal.mutate(move |n| {
            n.asset_id = request.new_id
        });
        // force send changed in order to refresh asset. Otherwise, asset reports as still none.
        let _ = signal.read().changed_sender.send(request.new_id).inspect_err(|err| println!("{err}"));

        map.init_requests.insert(request.new_id);
        map.assets.insert(request.new_id, mirror);

    }
}

#[derive(Resource,Clone)]
pub struct AssetMirrorRequestResponse<A: DioxusAssetSync> {
    signal: QueuedSignal<AssetMaybeMirrorState<A>>,
    asset_id_initialize_ticket: Option<RequestAssetIdTicket>,
    initialize_request_tx: Sender<InitializeSignalAssetIdRequest<A>>,
    poke_rx: Receiver<()>,
}

#[derive(Resource)]
pub struct DioxusAssetUpdatePokeSender(Sender<()>);

#[derive(Resource)]
pub struct DioxusAssetUpdatePokeReceiver(Receiver<()>);

pub struct RequestBevyAssetMirror<A: DioxusAssetSync> {
    response_tx: Sender<AssetMirrorRequestResponse<A>>,
    asset_id: AssetId<A>
}

impl<A: DioxusAssetSync> Command for RequestBevyAssetMirror<A> {
    fn apply(self, world: &mut World) -> () {
        
        let mut ticket_id = None;

        match world.get_resource::<AssetSyncInitialized<A>>() {
            Some(_) => {},
            None => {
                let (changed_tx, changed_rx) = flume::unbounded();
                let (initialize_asset_id_tx, initialize_asset_id_rx) = flume::unbounded();
                let (poke_tx, poke_rx) = flume::unbounded::<()>();

                world.insert_resource(DioxusAssetUpdatePokeReceiver(poke_rx));
                world.insert_resource(DioxusAssetUpdatePokeSender(poke_tx));
                world.insert_resource(AssetMirrorMap::<A>::default());
                world.insert_resource(ChangedAssetMirrors::<A>::default());
                world.insert_resource(ChangedIdsReceiver::<A>(changed_rx));
                world.insert_resource(ChangedIdsSender::<A>(changed_tx));
                world.insert_resource(UpdateAssetSignalReciever::<A> {rx: initialize_asset_id_rx });
                world.insert_resource(UpdateAssetSignalSender::<A> {tx: initialize_asset_id_tx });

                world.insert_resource(RegisteredAssetSignals::<A>::default());

                add_systems_through_world(world, Update, collect_changed_ids::<A>);
                add_systems_through_world(world, Update, drive_maybe_assets::<A>);
                add_systems_through_world(world, Update, update_signals_with_initialized_ids::<A>);
                add_systems_through_world(world, PostUpdate, init_requested_asset_mirrors::<A>);
                add_systems_through_world(world, PostUpdate, sync_mirrors_to_assets::<A>);
                add_systems_through_world(world, PostUpdate, sync_assets_to_mirrors::<A>);
                add_systems_through_world(world, Last, clear_changed_flags::<A>);
                add_systems_through_world(world, Last, clear_asset_init_requests::<A>);

                world.insert_resource(AssetSyncInitialized {
                    _phantom: PhantomData::<A>,
                });
            },
        }
        let changed_tx = world.resource::<ChangedIdsSender<A>>().0.clone();
        let poke_rx = world.resource::<DioxusAssetUpdatePokeReceiver>().0.clone();
        let poke_tx = world.resource::<DioxusAssetUpdatePokeSender>().0.clone();


        let mut map = world.resource_mut::<AssetMirrorMap<A>>();

        let signal = match map.assets.get(&self.asset_id) {
            Some(asset) => {
                asset.state.clone()
            },
            None => {
                let driver = WriterDriver::new(AssetMaybeMirrorState {
                    state: AssetFetch::Fetching,
                    changed_sender: changed_tx,
                    asset_id: self.asset_id,
                });

                let queued_signal = QueuedSignal::from_parts(
                    driver.queued_state.clone(),
                    driver.add_tx.clone(),
                    driver.set_tx.clone(),
                    driver.set_value_tx.clone(),
                );


                let mirror = AssetMaybeMirror {
                    state: queued_signal.clone(),
                    driver: Mutex::new(driver),
                    poke_tx
                };
                let latest_ticket_id = map.asset_id_initialize_tickets.back().unwrap_or(&RequestAssetIdTicket { ticket_id: 0 });
                ticket_id = Some(RequestAssetIdTicket {ticket_id: latest_ticket_id.ticket_id + 1 });
                map.assets.insert(self.asset_id, mirror);
                map.init_requests.insert(self.asset_id);
                queued_signal

            
            },
        };

        let initialize_request_tx = world.resource::<UpdateAssetSignalSender<A>>();
        // let signal_ids = world.resource::<RegisteredAssetSignals<A>>();
        // let signal_id = signal_ids.ids.contains(self.)
        let _ = self.response_tx.send(AssetMirrorRequestResponse {
            signal,
            asset_id_initialize_ticket: ticket_id,
            initialize_request_tx: initialize_request_tx.tx.clone(),
            poke_rx,
        });
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
    pub signal: Signal<QueuedSignal<AssetMaybeMirrorState<A>>>,
    health: Signal<HealthStatus>,
    // signal_id: AssetSignalId
}

impl<A: DioxusAssetSync> Copy for AssetMaybeMirrorSignal<A> {}

impl<A: DioxusAssetSync> AssetMaybeMirrorSignal<A> {
    pub fn mutate<F>(&self, f: F)
    where
        F: Fn(&mut A) + Send + Sync + 'static,
    {
        self.signal.read().mutate(move |state| {
            if let AssetFetch::Fetched(state) = &mut state.state 
            && let AssetState::Loaded(asset) = state {
                f(asset)
            }
            let _ = state.changed_sender.send(state.asset_id).inspect_err(|err| warn!("{err}"));
        });        
    }
    pub fn with_asset<R>(&self, f: impl FnOnce(Option<&A>) -> R) -> R {
        let maybe_state = self.value.read();           // reactive subscription
        match maybe_state.as_deref() {
            Some(state) => match &state.state {
                AssetFetch::Fetched(AssetState::Loaded(asset)) => f(Some(asset)),
                _ => f(None),
            },
            None => f(None),
        }
    }
}

pub struct AssetReadGuard<'a, A: DioxusAssetSync> {
    _signal_guard: GenerationalRef<core::cell::Ref<'a, QueuedSignal<AssetMaybeMirrorState<A>>>>,
    inner: TrackedReadGuard<'a, AssetMaybeMirrorState<A>>,
}

impl<'a, A: DioxusAssetSync> AssetReadGuard<'a, A> {
    /// Returns a reference to the asset if it is fully loaded.
    pub fn get(&self) -> Option<&A> {
        match &self.inner.state {
            AssetFetch::Fetched(AssetState::Loaded(asset)) => Some(asset),
            _ => None,
        }
    }
}

use std::fmt::Debug;

/// return the asset or a dummy 
pub fn use_bevy_asset<A: DioxusAssetSync + Debug>(
    id: Memo<Option<AssetId<A>>>
) -> AssetMaybeMirrorSignal<A> {
    let ctx = use_context::<CommandQueueSender>();



    let response  = use_hook(|| {
        let id = *id.read();
        let signal = ctx.send_command(|tx| {
            let mut queue = CommandQueue::default();
            queue.push(RequestBevyAssetMirror::<A> {
                response_tx: tx,
                asset_id: id.unwrap_or_default(),
            });
            queue

        }).unwrap();
        signal
        
    });

    // force dioxus to re-render when the first asset value is recieved, otherwise, it stays as none until dioxus updates it.
    //let r = response.signal.clone();

    use_future(move || {
        let poke_rx = response.poke_rx.clone();
        {
        // let value = r.clone();
        async move {
            while let Ok(()) = poke_rx.recv_async().await {
                // println!("force poke RECIEVED, forcing re-render, asset state now: {:#?}", &value.read().as_ref().state);
                // needs_update();
            }
        }
        }
    });

    // update the AssetId<A> of the signal if it has been changed
    let r = response.signal.clone();
    use_memo(move || {
        let id = *id.read();
        if let Some(id) = id {
            if let Some(ticket) = response.asset_id_initialize_ticket {
                // println!("current asset id is: {:#?}", id);

                // println!("id updated, getting new id");
                let status = response.initialize_request_tx.send(InitializeSignalAssetIdRequest { signal: r.clone(), new_id: id, requesting_signal_ticket: ticket }).inspect_err(|err| println!("{err}"));
                // println!("send status: {:#?}", status);
                // println!("ASSET STATE IS NOW: {:#?}", r.read().as_ref());

            } else {
                println!("how was a request for a new asset id made, but no response ticket found?");
            }
        }

    });
    let (value_signal, health) = response.signal.clone().use_hook();

    let writer = use_signal(|| response.signal);
    AssetMaybeMirrorSignal {
        value: value_signal,
        health: health,
        signal: writer,
    }
}
