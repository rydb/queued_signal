use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use dioxus::prelude::*;
use dioxus_signals::Signal;
use flume::{Receiver, Sender};
use parking_lot::Mutex;
use tokio::sync::oneshot;

use crate::state::{HealthStatus, QueuedSignal, SignalReadGuard, WriterDriver};

/// Error state for a queued signal that has not yet resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueuedSignalNoneState {
    /// No registration call has been made for this type.
    NotRegistered,
    /// A request has been sent to the hub and the response is pending.
    Fetching,
}

impl From<QueuedSignalNoneState> for String {
    fn from(value: QueuedSignalNoneState) -> Self {
        match value {
            QueuedSignalNoneState::NotRegistered => "Not Registered".into(),
            QueuedSignalNoneState::Fetching => "Fetching".into(),
        }
    }
}

/// Handle to a globally registered queued signal.
pub struct QueuedSignalHandle<T: Clone + Send + Sync + 'static> {
    /// Current value, or an error state if not yet resolved.
    pub value: Signal<Result<Arc<T>, QueuedSignalNoneState>>,
    /// Health status of the underlying signal.
    pub health: Signal<HealthStatus>,
    writer: Signal<Option<QueuedSignal<T>>>,
}

impl<T: Clone + Send + Sync + 'static> Copy for QueuedSignalHandle<T> {}

impl<T: Clone + Send + Sync + 'static> Clone for QueuedSignalHandle<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: Clone + Send + Sync + 'static> QueuedSignalHandle<T> {
    /// Enqueue a relative mutation, applied after all authoritative operations.
    pub fn mutate<F>(&self, f: F)
    where
        F: Fn(&mut T) + Send + Sync + 'static,
    {
        if let Some(w) = self.writer.read().as_ref() {
            w.mutate(f);
        }
    }

    /// Enqueue an authoritative closure mutation.
    pub fn mutate_set<F>(&self, f: F)
    where
        F: Fn(&mut T) + Send + Sync + 'static,
    {
        if let Some(w) = self.writer.read().as_ref() {
            w.mutate_set(f);
        }
    }

    /// Enqueue an authoritative full value replacement.
    pub fn set_value(&self, value: T) {
        if let Some(w) = self.writer.read().as_ref() {
            w.set_value(value);
        }
    }

    /// Current health status of the underlying signal.
    pub fn health(&self) -> HealthStatus {
        *self.health.read()
    }

    /// Read the current value through a [`SignalReadGuard`].
    pub fn read(&self) -> SignalReadGuard<'_, Result<Arc<T>, QueuedSignalNoneState>> {
        SignalReadGuard::new(self.value.read())
    }

    /// Read the `Ok` variant, mapping the inner value through `f`.
    pub fn read_ok<U>(&self, f: impl FnOnce(&T) -> U) -> Result<U, QueuedSignalNoneState> {
        let guard = self.value.read();
        match &*guard {
            Ok(arc) => Ok(f(arc.as_ref())),
            Err(e) => Err(*e),
        }
    }
}

/// Type-erased command processed by the background hub.
trait HubCommand: Send + 'static {
    fn execute(self: Box<Self>, hub: &mut InnerHub);
}

/// Command to register a new signal type with its initial value.
struct RegisterCommand<T: Clone + Send + Sync + 'static> {
    initial: T,
}

impl<T: Clone + Send + Sync + 'static> HubCommand for RegisterCommand<T> {
    fn execute(self: Box<Self>, hub: &mut InnerHub) {
        let this = *self;
        let type_id = TypeId::of::<T>();

        if hub.signals.contains_key(&type_id) {
            return;
        }

        let driver = WriterDriver::new(this.initial);
        let set_value_tx = driver.set_value_tx.clone();
        let set_tx = driver.set_tx.clone();
        let add_tx = driver.add_tx.clone();
        let queued_state = driver.queued_state.clone();

        let driver_arc = Arc::new(Mutex::new(driver));

        let signal = QueuedSignal::from_parts(
            queued_state,
            Some(driver_arc.clone()),
            add_tx,
            set_tx,
            set_value_tx,
        );

        hub.signals.insert(type_id, Box::new(signal));

        let tick_driver = driver_arc;
        hub.drivers.push(Box::new(move || {
            tick_driver.lock().tick(Duration::ZERO);
        }));

        // Replay any requests that were parked while waiting for
        // this registration. They will now find the signal.
        if let Some(pending) = hub.pending.remove(&type_id) {
            for cmd in pending {
                cmd.execute(hub);
            }
        }
    }
}

/// Command to request an existing signal from the hub.
struct GetSignalCommand<T: Clone + Send + Sync + 'static> {
    response: oneshot::Sender<QueuedSignal<T>>,
}

impl<T: Clone + Send + Sync + 'static> HubCommand for GetSignalCommand<T> {
    fn execute(self: Box<Self>, hub: &mut InnerHub) {
        let type_id = TypeId::of::<T>();

        match hub.signals.get(&type_id) {
            Some(boxed) => {
                let signal = boxed
                    .downcast_ref::<QueuedSignal<T>>()
                    .expect("signal stored under TypeId::of::<T>() must be QueuedSignal<T>")
                    .clone();
                let _ = self.response.send(signal);
            }
            None => {
                // Signal not registered yet. Park this request so it
                // resolves when a RegisterCommand arrives for this type.
                hub.pending.entry(type_id).or_default().push(self);
            }
        }
    }
}

/// Handle for communicating with the background signal hub.
#[derive(Clone)]
pub struct QueuedSignalSender {
    tx: Sender<Box<dyn HubCommand>>,
}

impl QueuedSignalSender {
    /// Register a signal type with its initial value.
    pub fn register<T: Clone + Send + Sync + 'static>(&self, initial: T) {
        let cmd = RegisterCommand { initial };
        let _ = self.tx.send(Box::new(cmd));
    }

    /// Request a signal handle for type `T` from the hub.
    pub(crate) fn request<T: Clone + Send + Sync + 'static>(
        &self,
    ) -> oneshot::Receiver<QueuedSignal<T>> {
        let (tx, rx) = oneshot::channel();
        let cmd = GetSignalCommand { response: tx };
        let _ = self.tx.send(Box::new(cmd));
        rx
    }
}

/// Internal hub owning all registered signals and driver tickers.
struct InnerHub {
    signals: HashMap<TypeId, Box<dyn Any + Send>>,
    drivers: Vec<Box<dyn FnMut() + Send>>,
    requests: Receiver<Box<dyn HubCommand>>,
    shutdown: Arc<AtomicBool>,
    /// Requests parked until a matching registration arrives.
    pending: HashMap<TypeId, Vec<Box<dyn HubCommand>>>,
}

/// Create a new queued signal hub running in a background thread.
pub fn create_queued_signal_hub() -> QueuedSignalSender {
    let (tx, rx) = flume::unbounded::<Box<dyn HubCommand>>();
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();

    let weak_tx = tx.downgrade();
    std::thread::spawn(move || {
        let mut hub = InnerHub {
            signals: HashMap::new(),
            drivers: Vec::new(),
            requests: rx,
            shutdown: shutdown_clone,
            pending: HashMap::new(),
        };

        while !hub.shutdown.load(Ordering::Relaxed) {
            let pending: Vec<Box<dyn HubCommand>> = hub.requests.drain().collect();
            for cmd in pending {
                cmd.execute(&mut hub);
            }

            for ticker in &mut hub.drivers {
                ticker();
            }

            if weak_tx.upgrade().is_none() {
                hub.shutdown.store(true, Ordering::Release);
                break;
            }

            std::thread::sleep(Duration::from_millis(16));
        }
    });

    QueuedSignalSender { tx }
}

/// Hook to obtain a [`QueuedSignalHandle`] for type `T`.
///
/// Returns either the signal or a [`QueuedNoneState] until it exists
///
/// # Panics
///
/// Panics if no [`QueuedSignalSender`] was provided to the dioxus context.
pub fn use_queued_signal<T: Clone + Send + Sync + 'static>() -> QueuedSignalHandle<T> {
    let sender = use_context::<QueuedSignalSender>();

    let mut value: Signal<Result<Arc<T>, QueuedSignalNoneState>> =
        use_signal(|| Err(QueuedSignalNoneState::NotRegistered));
    let health: Signal<HealthStatus> = use_signal(|| HealthStatus::Healthy);
    let mut writer: Signal<Option<QueuedSignal<T>>> = use_signal(|| None);

    let sender = sender.clone();
    use_future(move || {
        let sender = sender.clone();
        async move {
            value.set(Err(QueuedSignalNoneState::Fetching));

            let rx = sender.request::<T>();
            match rx.await {
                Ok(signal) => {
                    let current = signal.read().clone();
                    value.set(Ok(current));

                    signal.state.forward_to(value, health, Ok);
                    writer.set(Some(signal));
                }
                Err(_) => {
                    value.set(Err(QueuedSignalNoneState::NotRegistered));
                }
            }
        }
    });

    QueuedSignalHandle {
        value,
        health,
        writer,
    }
}
