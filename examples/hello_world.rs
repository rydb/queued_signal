// main.rs
use std::any::TypeId;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use bevy_app::prelude::*;
use bevy_ecs::prelude::*;
use dioxus::prelude::*;
use flume::{Receiver, Sender};
use queued_signal::signal::{QueuedResource, QueuedSignal};

type SignalRequest = (TypeId, Sender<Box<dyn std::any::Any + Send>>);

/// Context passed to Dioxus. It allows requesting a `QueuedResource<T>` without
/// locking or accessing the Bevy world directly.
#[derive(Clone)]
pub struct SignalRequestContext {
    request_tx: Sender<SignalRequest>,
}

impl SignalRequestContext {
    /// Request a `QueuedResource<T>` for a given resource type.
    /// Returns `None` if the type is not registered or the request times out.
    pub fn request_signal<T: Send + Sync + 'static + Clone>(
        &self,
    ) -> Option<QueuedResource<T>> {
        let (tx, rx) = flume::bounded(1);
        let type_id = TypeId::of::<T>();
        self.request_tx.send((type_id, tx)).ok()?;

        let boxed = rx.recv_timeout(Duration::from_millis(100)).ok()?;
        let resource = boxed.downcast::<QueuedResource<T>>().ok()?;
        Some(*resource)
    }
}

#[derive(Resource)]
struct SyncHandle<T: Send + Sync + 'static + Clone> {
    resource: QueuedResource<T>,
}

impl<T: Send + Sync + 'static + Clone> SyncHandle<T> {
    fn new() -> Self {
        Self {
            resource: QueuedResource {
                signal: QueuedSignal::new_uninitialized(),
            },
        }
    }
}

const SYNC_INTERVAL_MS: u64 = 16; // ~60 FPS

pub struct ResourceSyncPlugin<T: Resource + Send + Sync + 'static + Clone> {
    _marker: std::marker::PhantomData<T>,
}

impl<T: Resource + Send + Sync + 'static + Clone> ResourceSyncPlugin<T> {
    pub fn new() -> Self {
        Self {
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T: Resource + Send + Sync + 'static + Clone> Plugin for ResourceSyncPlugin<T> {
    fn build(&self, app: &mut App) {
        println!("building sync plugin for: {:?}", TypeId::of::<T>());
        app.insert_resource(SyncHandle::<T>::new())
            .add_systems(Update, (
                initialize_signal::<T>,
                sync_signal_to_resource::<T>,
                handle_signal_requests::<T>,
            ));
    }
}

/// Initialises the `QueuedSignal` with a clone of the current authoritative resource.
fn initialize_signal<T: Resource + Send + Sync + 'static + Clone>(
    mut handle: ResMut<SyncHandle<T>>,
    res: Res<T>,
) {
    if !handle.resource.signal.is_initialized() {
        let initial = res.clone();
        let _ = handle.resource.signal.initialize(initial, Duration::from_millis(SYNC_INTERVAL_MS));
    }
}

/// Writes the latest value from the `QueuedSignal` back to the authoritative resource.
fn sync_signal_to_resource<T: Resource + Send + Sync + 'static + Clone>(
    handle: Res<SyncHandle<T>>,
    mut res: ResMut<T>,
) {
    if let Ok(latest) = handle.resource.read() {
        *res = (*latest).clone();
    }
}

/// Processes incoming requests from Dioxus. Matches by `TypeId`.
/// If the request is not for this type, it is forwarded back to the main queue.
fn handle_signal_requests<T: Send + Sync + 'static + Clone>(
    handle: Res<SyncHandle<T>>,
    receiver: Res<SignalRequestReceiver>,
    forwarder: Res<SignalRequestForwarder>,
) {
    while let Ok((type_id, reply_tx)) = receiver.rx.try_recv() {
        if type_id == TypeId::of::<T>() {
            let boxed: Box<dyn std::any::Any + Send> = Box::new(handle.resource.clone());
            let _ = reply_tx.send(boxed);
        } else {
            // Not our type – put it back for other plugins.
            let _ = forwarder.tx.send((type_id, reply_tx));
        }
    }
}

#[derive(Resource)]
struct SignalRequestReceiver {
    rx: Receiver<SignalRequest>,
}

#[derive(Resource, Clone)]
struct SignalRequestForwarder {
    tx: Sender<SignalRequest>,
}

fn run_bevy_world(
    request_rx: Receiver<SignalRequest>,
    shutdown_rx: Receiver<()>,
    initial_resources: Vec<Box<dyn std::any::Any + Send>>,
    request_tx: Sender<SignalRequest>, // <-- Pass the sender explicitly
) {
    let mut app = App::new();

    // Add plugins for each resource type we want to sync.
    app.add_plugins(ResourceSyncPlugin::<Counter>::new());

    // Insert initial resources.
    for resource in initial_resources {
        if let Some(counter) = resource.downcast_ref::<Counter>() {
            app.insert_resource(counter.clone());
        }
    }

    app.insert_resource(SignalRequestReceiver { rx: request_rx });
    app.insert_resource(SignalRequestForwarder { tx: request_tx });

    loop {
        if shutdown_rx.try_recv().is_ok() {
            break;
        }
        app.update();
        std::thread::sleep(Duration::from_millis(SYNC_INTERVAL_MS));
    }
}

pub fn use_queued_resource<T: Send + Sync + 'static + Clone>(
) -> (Signal<Option<Arc<T>>>, impl Fn(Box<dyn FnOnce(&mut T) + Send + 'static>) + Clone + 'static) {
    let ctx = use_context::<SignalRequestContext>();
    // Store the QueuedResource in a use_signal so it persists across renders.
    let mut resource_signal = use_signal(|| {
        ctx.request_signal::<T>()
            .expect("Resource type not registered with Bevy")
    });

    let resource = resource_signal.read().clone();
    let read_signal = resource.attach();

    // Create a mutate function that captures the resource.
    let mutate = {
        let resource = resource.clone();
        move |f: Box<dyn FnOnce(&mut T) + Send + 'static>| {
            let _ = resource.mutate(move |data| f(data));
        }
    };

    (read_signal, mutate)
}

#[derive(Clone, Resource)]
pub struct Counter {
    pub value: i32,
}

fn main() {
    let (request_tx, request_rx) = flume::unbounded::<SignalRequest>();
    let (shutdown_tx, shutdown_rx) = flume::bounded(1);

    let initial_resources: Vec<Box<dyn std::any::Any + Send>> = vec![
        Box::new(Counter { value: 0 }),
    ];

    // Clone the sender to pass into the bevy thread.
    let request_tx_for_bevy = request_tx.clone();
    thread::spawn(move || {
        run_bevy_world(request_rx, shutdown_rx, initial_resources, request_tx_for_bevy);
    });

    let request_ctx = SignalRequestContext { request_tx };

    dioxus::LaunchBuilder::new()
        .with_context(request_ctx)
        .launch(dx_app);

    let _ = shutdown_tx.send(());
}

fn dx_app() -> Element {
    let (counter, mutate) = use_queued_resource::<Counter>();
    let value = counter.read().as_ref().map(|arc| arc.value).unwrap_or(0);

    // Auto‑increment every 10ms
    let incr_clone = mutate.clone();
    use_future(move || {
        let mutate = incr_clone.clone();
        async move {
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                mutate(Box::new(|c: &mut Counter| c.value += 1));
            }
        }
    });

    rsx! {
        div {
            h1 { "Counter: {value}" }
            button {
                onclick: move |_| {
                    mutate(Box::new(|c: &mut Counter| c.value += 100));
                },
                "Increment"
            }
        }
    }
}