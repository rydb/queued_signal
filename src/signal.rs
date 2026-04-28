use bevy_log::tracing;
use dioxus::prelude::*;
use dioxus_signals::*;
use dioxus_hooks::*;
use flume::{Receiver, Sender};
use left_right::{Absorb, ReadHandle, WriteHandle};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;


/// Health status of QueuedSignal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    Healthy,
    Degraded { pinned_buffers: usize },
    Stalled { pinned_buffers: usize },
}

struct ReaderRegistry {
    readers: Mutex<HashMap<u64, Instant>>,
    next_id: AtomicU64,
}

impl ReaderRegistry {
    fn new() -> Self {
        Self {
            readers: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        }
    }

    fn register(&self) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.readers.lock().insert(id, Instant::now());
        id
    }

    fn unregister(&self, id: u64) {
        self.readers.lock().remove(&id);
    }

    fn heartbeat(&self, id: u64) {
        if let Some(last_seen) = self.readers.lock().get_mut(&id) {
            *last_seen = Instant::now();
        }
    }

    fn check_stalled(&self, timeout: Duration) -> Vec<u64> {
        let now = Instant::now();
        self.readers
            .lock()
            .iter()
            .filter_map(|(&id, &last_seen)| {
                if now.duration_since(last_seen) > timeout {
                    Some(id)
                } else {
                    None
                }
            })
            .collect()
    }
}

/// Closure mutation (used by Dioxus `mutate` and `mutate_set`)
type MutationOp<T> = Arc<dyn Fn(&mut T) + Send + Sync>;

/// Full-value replacement op
#[derive(Clone)]
pub struct SetValueOp<T>(pub Arc<T>);

/// Unified operation for left‑right – always operates on `Arc<T>` inside `Absorbable`.
#[derive(Clone)]
enum SignalOp<T: Clone + Send + Sync> {
    Fn(MutationOp<T>),
    Set(SetValueOp<T>),
}

/// 
#[derive(Clone)]
#[repr(transparent)]
pub struct Absorbable<T: Clone>(pub Arc<T>);

impl<T: Clone> Deref for Absorbable<T> {
    type Target = Arc<T>;
    fn deref(&self) -> &Self::Target { &self.0 }
}
impl<T: Clone> DerefMut for Absorbable<T> {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.0 }
}

impl<T: Clone + Send + Sync> Absorb<SignalOp<T>> for Absorbable<T> {
    fn absorb_first(&mut self, operation: &mut SignalOp<T>, _other: &Self) {
        match operation {
            SignalOp::Fn(op) => {
                let inner = Arc::make_mut(&mut self.0);
                op(inner);
            }
            SignalOp::Set(op) => {
                self.0 = op.0.clone();
            }
        }
    }

    fn absorb_second(&mut self, operation: SignalOp<T>, _other: &Self) {
        match operation {
            SignalOp::Fn(op) => {
                let inner = Arc::make_mut(&mut self.0);
                op(inner);
            }
            SignalOp::Set(op) => {
                self.0 = op.0;
            }
        }
    }

    fn sync_with(&mut self, first: &Self) {
        self.0 = first.0.clone();
    }
}

/// Inner state of QueuedSignal
pub struct QueuedState<T: Clone + Send + Sync> {
    read_handle: ReadHandle<Absorbable<T>>,
    notify_rx: watch::Receiver<()>,
    health_rx: watch::Receiver<HealthStatus>,
    registry: Arc<ReaderRegistry>,
}

impl<T: Clone + Send + Sync> Clone for QueuedState<T> {
    fn clone(&self) -> Self {
        Self {
            read_handle: self.read_handle.clone(),
            notify_rx: self.notify_rx.clone(),
            health_rx: self.health_rx.clone(),
            registry: self.registry.clone(),
        }
    }
}

impl<T: Clone + Send + Sync> QueuedState<T> {
    /// Returns a tracked guard that dereferences to `Arc<T>`.
    pub fn read(&self) -> TrackedReadGuard<'_, T> {
        let guard = self.read_handle.enter().unwrap();
        TrackedReadGuard::new(guard, self.registry.clone())
    }
    pub fn health(&self) -> HealthStatus { *self.health_rx.borrow() }
    fn notify_rx(&self) -> watch::Receiver<()> { self.notify_rx.clone() }
}

/// QueuedSignal Read guard. 
pub struct TrackedReadGuard<'a, T: Clone + Send + Sync> {
    guard: left_right::ReadGuard<'a, Absorbable<T>>,
    registry: Arc<ReaderRegistry>,
    reader_id: u64,
}

impl<'a, T: Clone + Send + Sync> TrackedReadGuard<'a, T> {
    fn new(guard: left_right::ReadGuard<'a, Absorbable<T>>, registry: Arc<ReaderRegistry>) -> Self {
        let reader_id = registry.register();
        Self { guard, registry, reader_id }
    }
    pub fn heartbeat(&self) { self.registry.heartbeat(self.reader_id); }
}

impl<'a, T: Clone + Send + Sync> Drop for TrackedReadGuard<'a, T> {
    fn drop(&mut self) { self.registry.unregister(self.reader_id); }
}

impl<'a, T: Clone + Send + Sync> Deref for TrackedReadGuard<'a, T> {
    type Target = Arc<T>;
    fn deref(&self) -> &Self::Target { &self.guard.0 }
}

/// Driver for how reads/writes are managed between QueuedSignal and a given state front end (Dioxus, and others)
pub struct WriterDriver<T: Clone + Send + Sync> {
    write_handle: WriteHandle<Absorbable<T>, SignalOp<T>>,
    set_value_rx: Receiver<SetValueOp<T>>,
    set_rx: Receiver<MutationOp<T>>,
    add_rx: Receiver<MutationOp<T>>,
    abs_slot: Arc<Mutex<Option<Arc<T>>>>, 
    notify_tx: watch::Sender<()>,
    health_tx: watch::Sender<HealthStatus>,
    registry: Arc<ReaderRegistry>,
    watchdog_timeout: Duration,
    last_publish: Instant,
    pub set_value_tx: Sender<SetValueOp<T>>,
    pub set_tx: Sender<MutationOp<T>>,
    pub add_tx: Sender<MutationOp<T>>,
    pub queued_state: QueuedState<T>,
}

impl<T: Clone + Send + Sync + 'static> WriterDriver<T> {
    pub fn new(initial: T) -> Self {
        let initial_wrapped = Absorbable(Arc::new(initial));
        let (write_handle, read_handle) =
            left_right::new_from_empty::<Absorbable<T>, SignalOp<T>>(initial_wrapped);

        let (notify_tx, notify_rx) = watch::channel(());
        let (health_tx, health_rx) = watch::channel(HealthStatus::Healthy);

        let (set_value_tx, set_value_rx) = flume::unbounded();
        let (set_tx, set_rx) = flume::unbounded();
        let (add_tx, add_rx) = flume::unbounded();

        let registry = Arc::new(ReaderRegistry::new());

        let state = QueuedState {
            read_handle,
            notify_rx,
            health_rx,
            registry: registry.clone(),
        };

        Self {
            write_handle,
            set_value_rx,
            set_rx,
            add_rx,
            abs_slot: Arc::new(Mutex::new(None)),
            notify_tx,
            health_tx,
            registry,
            watchdog_timeout: Duration::from_millis(500),
            last_publish: Instant::now(),
            set_value_tx: set_value_tx.clone(),
            set_tx: set_tx.clone(),
            add_tx: add_tx.clone(),
            queued_state: state,
        }
    }

    /// Absolute override – drains all buffers and replaces with the given `T`.
    pub fn write_absolute(&self, value: T) {
        let arc = Arc::new(value);
        let mut slot = self.abs_slot.lock();
        if slot.is_some() {
            tracing::warn!("Absolute value overwritten before being applied.");
        }
        *slot = Some(arc);
    }

    pub fn tick(&mut self, publish_interval: Duration) {
        let mut did_work = false;

        let mut abs_guard = self.abs_slot.lock();
        if let Some(abs_val) = abs_guard.take() {
            drop(abs_guard);
            // Wrap as SetValueOp and append as Set.
            self.write_handle.append(SignalOp::Set(SetValueOp(abs_val)));
            self.set_value_rx.drain().for_each(|_| {});
            self.set_rx.drain().for_each(|_| {});
            self.add_rx.drain().for_each(|_| {});
            did_work = true;
        } else {
            drop(abs_guard);
            // Authoritative full replacements (SetValueOp)
            for op in self.set_value_rx.drain() {
                self.write_handle.append(SignalOp::Set(op));
                did_work = true;
            }
            // Authoritative closure mutations (mutate_set)
            for op in self.set_rx.drain() {
                self.write_handle.append(SignalOp::Fn(op));
                did_work = true;
            }
            // Relative closure mutations (mutate)
            for op in self.add_rx.drain() {
                self.write_handle.append(SignalOp::Fn(op));
                did_work = true;
            }
        }

        if did_work && self.last_publish.elapsed() >= publish_interval {
            self.write_handle.publish();
            self.last_publish = Instant::now();
            let _ = self.notify_tx.send(());
        }

        self.update_health();
    }

    fn update_health(&self) {
        let stalled = self.registry.check_stalled(self.watchdog_timeout);
        let pinned = stalled.len();
        let status = match pinned {
            0 => HealthStatus::Healthy,
            1 => HealthStatus::Degraded { pinned_buffers: pinned },
            _ => HealthStatus::Stalled { pinned_buffers: pinned },
        };
        let _ = self.health_tx.send(status);
    }
}

// -----------------------------------------------------------------------------
// QueuedSignal (public handle)
// -----------------------------------------------------------------------------
#[derive(Clone)]
pub struct QueuedSignal<T: Clone + Send + Sync> {
    state: QueuedState<T>,
    add_tx: Sender<MutationOp<T>>,
    set_tx: Sender<MutationOp<T>>,
    set_value_tx: Sender<SetValueOp<T>>,
}

impl<T: Clone + Send + Sync + 'static> QueuedSignal<T> {
    pub fn from_parts(
        state: QueuedState<T>,
        add_tx: Sender<MutationOp<T>>,
        set_tx: Sender<MutationOp<T>>,
        set_value_tx: Sender<SetValueOp<T>>,
    ) -> Self {
        Self { state, add_tx, set_tx, set_value_tx }
    }

    pub fn read(&self) -> TrackedReadGuard<'_, T> { self.state.read() }

    /// Relative mutation. Applied after all authoritative operations.
    pub fn mutate<F>(&self, f: F)
    where F: Fn(&mut T) + Send + Sync + 'static
    {
        let _ = self.add_tx.send(Arc::new(f));
    }

    /// Authoritative mutation (closure), may be overridden by a later `set_value`.
    pub fn mutate_set<F>(&self, f: F)
    where F: Fn(&mut T) + Send + Sync + 'static
    {
        let _ = self.set_tx.send(Arc::new(f));
    }

    /// Authoritative full value replacement.
    pub fn set_value(&self, value: Arc<T>) {
        let _ = self.set_value_tx.send(SetValueOp(value));
    }

    pub fn health(&self) -> HealthStatus { self.state.health() }

    pub fn use_hook(&self) -> (Signal<Option<Arc<T>>>, Signal<HealthStatus>) {
        use_queued_signal(self.state.clone())
    }
}

pub fn use_queued_signal<T: Clone + Send + Sync + 'static>(
    state: QueuedState<T>,
) -> (Signal<Option<Arc<T>>>, Signal<HealthStatus>) {
    let mut value_signal = use_signal(|| None);
    let mut health_signal = use_signal(|| HealthStatus::Healthy);

    use_future(move || {
        let mut notify_rx = state.notify_rx();
        let mut health_rx = state.health_rx.clone();
        let read_handle = state.read_handle.clone();
        let registry = state.registry.clone();

        async move {
            loop {
                tokio::select! {
                    Ok(()) = notify_rx.changed() => {
                        let guard = read_handle.enter().unwrap();
                        let reader_id = registry.register();
                        registry.heartbeat(reader_id);
                        // Clone the Arc<T>, not T itself – just a refcount bump.
                        value_signal.set(Some(guard.0.clone()));
                        registry.unregister(reader_id);
                    }
                    Ok(()) = health_rx.changed() => {
                        health_signal.set(*health_rx.borrow());
                    }
                    else => break,
                }
            }
        }
    });

    (value_signal, health_signal)
}

/// Handle to underyling QueuedSignal, made reactive for Dioxus.
#[derive(Clone)]
pub struct QueuedSignalHandle<T: Clone + Send + Sync + 'static> {
    pub signal: Signal<Option<Arc<T>>>,
    pub health: Signal<HealthStatus>,
    pub writer: QueuedSignal<T>,
}

impl<T: Clone + Send + Sync + 'static> QueuedSignalHandle<T> {
    pub fn mutate<F>(&self, f: F)
    where F: Fn(&mut T) + Send + Sync + 'static
    {
        self.writer.mutate(f);
    }

    pub fn mutate_set<F>(&self, f: F)
    where F: Fn(&mut T) + Send + Sync + 'static
    {
        self.writer.mutate_set(f);
    }

    pub fn set_value(&self, value: Arc<T>) {
        self.writer.set_value(value);
    }

    pub fn health(&self) -> HealthStatus { *self.health.read() }
}

impl<T: Clone + Send + Sync + 'static> Deref for QueuedSignalHandle<T> {
    type Target = Signal<Option<Arc<T>>>;
    fn deref(&self) -> &Self::Target { &self.signal }
}

unsafe impl<T: Clone + Send + Sync> Send for Absorbable<T> {}
unsafe impl<T: Clone + Send + Sync> Sync for Absorbable<T> {}
unsafe impl<T: Clone + Send + Sync> Send for QueuedState<T> {}
unsafe impl<T: Clone + Send + Sync> Sync for QueuedState<T> {}