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
use parking_lot::Mutex;

use crate::state::{HealthStatus, QueuedSignal, SignalReadGuard, WriterDriver};

/// Error state for an uninitialized queued signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueuedSignalNoneState {
    /// The signal has been requested but the upstream provider hasn't registered it yet.
    Fetching,
}

impl From<QueuedSignalNoneState> for String {
    fn from(value: QueuedSignalNoneState) -> Self {
        match value {
            QueuedSignalNoneState::Fetching => "Fetching".into(),
        }
    }
}

/// Handle to a globally registered queued signal.
pub struct QueuedSignalHandle<T: Clone + Send + Sync + 'static> {
    /// The current value (or `Err(Fetching)` if not yet registered).
    pub value: Signal<Result<Arc<T>, QueuedSignalNoneState>>,
    /// The health status of the underlying signal.
    pub health: Signal<HealthStatus>,
    writer: Signal<Option<QueuedSignal<T>>>,
}

impl<T: Clone + Send + Sync + 'static> Copy for QueuedSignalHandle<T> {}

impl<T: Clone + Send + Sync + 'static> Clone for QueuedSignalHandle<T> {
    fn clone(&self) -> Self {
        Self {
            value: self.value,
            health: self.health,
            writer: self.writer,
        }
    }
}

impl<T: Clone + Send + Sync + 'static> QueuedSignalHandle<T> {
    /// Relative mutation. Applied after all authoritative operations.
    pub fn mutate<F>(&self, f: F)
    where
        F: Fn(&mut T) + Send + Sync + 'static,
    {
        if let Some(w) = self.writer.read().as_ref() {
            w.mutate(f);
        }
    }

    /// Authoritative mutation (closure). May be overridden by a later [`set_value`](Self::set_value).
    pub fn mutate_set<F>(&self, f: F)
    where
        F: Fn(&mut T) + Send + Sync + 'static,
    {
        if let Some(w) = self.writer.read().as_ref() {
            w.mutate_set(f);
        }
    }

    /// Authoritative full value replacement.
    pub fn set_value(&self, value: T) {
        if let Some(w) = self.writer.read().as_ref() {
            w.set_value(Arc::new(value));
        }
    }

    /// Returns the current [`HealthStatus`] of the underlying signal.
    pub fn health(&self) -> HealthStatus {
        *self.health.read()
    }

    pub fn read(&self) -> SignalReadGuard<'_, Result<Arc<T>, QueuedSignalNoneState>> {
        SignalReadGuard::new(self.value.read())
    }
    
    /// Read and map the `Ok` variant, or pass through the error.
    pub fn read_ok<U>(&self, f: impl FnOnce(&T) -> U) -> Result<U, QueuedSignalNoneState> {
        let guard = self.value.read();
        match &*guard {
            Ok(arc) => Ok(f(arc.as_ref())),
            Err(e) => Err(e.clone()),
        }
    }
}

/// Context hub for standalone queued signals.
/// Register types and provide it via dioxus launch context.
/// Components call [`use_queued_signal::<T>()`] to obtain a handle.
pub struct QueuedSignalHub {
    signals: Arc<Mutex<HashMap<TypeId, Box<dyn Any + Send>>>>,
    tickers: Arc<Mutex<Vec<Box<dyn FnMut() + Send>>>>,
    shutdown: Arc<AtomicBool>,
}

impl Clone for QueuedSignalHub {
    fn clone(&self) -> Self {
        Self {
            signals: self.signals.clone(),
            tickers: self.tickers.clone(),
            shutdown: self.shutdown.clone(),
        }
    }
}

impl QueuedSignalHub {
    /// Create a new hub and start the background tick thread.
    ///
    /// The tick thread runs at ~60 Hz and stops when the last clone of the hub
    /// is dropped.
    pub fn new() -> Self {
        let tickers: Arc<Mutex<Vec<Box<dyn FnMut() + Send>>>> =
            Arc::new(Mutex::new(Vec::new()));

        let shutdown = Arc::new(AtomicBool::new(false));

        let tickers_clone = tickers.clone();
        let shutdown_clone = shutdown.clone();
        std::thread::spawn(move || {
            while !shutdown_clone.load(Ordering::Relaxed) {
                {
                    let mut guard = tickers_clone.lock();
                    for ticker in guard.iter_mut() {
                        ticker();
                    }
                }
                std::thread::sleep(Duration::from_millis(16));
            }
        });

        Self {
            signals: Arc::new(Mutex::new(HashMap::new())),
            tickers,
            shutdown,
        }
    }

    /// Register a queued signal with its initial value.
    ///
    /// Register a signal type with its initial value.
    /// Must be called before the dioxus app launches.
    pub fn register<T: Clone + Send + Sync + 'static>(&self, initial: T) {
        let driver = WriterDriver::new(initial);
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

        self.signals
            .lock()
            .insert(TypeId::of::<T>(), Box::new(signal));

        let tick_driver = driver_arc;
        self.tickers.lock().push(Box::new(move || {
            tick_driver.lock().tick(Duration::ZERO);
        }));
    }

    /// Retrieve a previously registered [`QueuedSignal`] by type.
    pub(crate) fn get<T: Clone + Send + Sync + 'static>(&self) -> Option<QueuedSignal<T>> {
        self.signals
            .lock()
            .get(&TypeId::of::<T>())
            .and_then(|boxed| boxed.downcast_ref::<QueuedSignal<T>>())
            .cloned()
    }
}

impl Drop for QueuedSignalHub {
    fn drop(&mut self) {
        // Only shut down the background ticker thread when the last
        // user-facing clone is dropped. 
        if Arc::strong_count(&self.shutdown) == 2 {
            self.shutdown.store(true, Ordering::Release);
        }
    }
}

/// Hook to obtain a [`QueuedSignalHandle`] for a globally-registered type `T`. 
/// Returns a none-state if not initialized
///
/// # Panics
/// Panics if no [`QueuedSignalHub`] was provided to the dioxus context.
pub fn use_queued_signal<T: Clone + Send + Sync + 'static>() -> QueuedSignalHandle<T> {
    let hub = use_context::<QueuedSignalHub>();

    match hub.get::<T>() {
        Some(signal) => {
            // Eagerly set the initial value so the first
            // render shows the real data.
            let (mut value_signal, health_signal) =
                signal.use_hook(QueuedSignalNoneState::Fetching);
            let current = signal.read().clone();
            value_signal.set(Ok(current));

            let writer: Signal<Option<QueuedSignal<T>>> = use_signal(|| Some(signal));

            QueuedSignalHandle {
                value: value_signal,
                health: health_signal,
                writer,
            }
        }
        None => {
            let value: Signal<Result<Arc<T>, QueuedSignalNoneState>> =
                use_signal(|| Err(QueuedSignalNoneState::Fetching));
            let health: Signal<HealthStatus> = use_signal(|| HealthStatus::Healthy);
            let writer: Signal<Option<QueuedSignal<T>>> = use_signal(|| None);

            QueuedSignalHandle {
                value,
                health,
                writer,
            }
        }
    }
}
