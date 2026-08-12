use dioxus::prelude::*;
use dioxus::signals::Signal;
use dioxus_core::Task;
use flume::{Receiver, Sender};
use left_right::{Absorb, ReadHandle, WriteHandle};
use parking_lot::Mutex;
use queued_signal_tracing::error;
use std::any::type_name;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::watch;

use crate::macros::warn;

/// Health status of QueuedSignal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    /// All readers are responding within the watchdog timeout.
    Healthy,
    /// One reader has stalled beyond the watchdog timeout.
    Degraded {
        /// Number of stalled (pinned) read buffers.
        pinned_buffers: usize,
    },
    /// Two or more readers have stalled.
    Stalled {
        /// Number of stalled (pinned) read buffers.
        pinned_buffers: usize,
    },
}

impl Display for HealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HealthStatus::Healthy => write!(f, "Healthy"),
            HealthStatus::Degraded { pinned_buffers } => {
                write!(f, "Degraded: pinned_buffers {}", pinned_buffers)
            }
            HealthStatus::Stalled { pinned_buffers } => {
                write!(f, "Stalled: pinned_buffers {}", pinned_buffers)
            }
        }
    }
}

/// Registry tracking active readers for stall detection.
#[derive(Debug)]
pub struct ReaderRegistry {
    readers: Mutex<HashMap<u64, Instant>>,
    next_id: AtomicU64,
}

impl Default for ReaderRegistry {
    fn default() -> Self {
        Self {
            readers: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        }
    }
}

impl ReaderRegistry {
    /// Register a new reader and return its unique ID.
    pub fn register(&self) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.readers.lock().insert(id, Instant::now());
        id
    }

    /// Remove a reader from the registry.
    pub fn unregister(&self, id: u64) {
        self.readers.lock().remove(&id);
    }

    /// Record a heartbeat for the given reader, resetting its stall timer.
    pub fn heartbeat(&self, id: u64) {
        if let Some(last_seen) = self.readers.lock().get_mut(&id) {
            *last_seen = Instant::now();
        }
    }

    /// Return IDs of all readers whose last heartbeat exceeds `timeout`.
    pub fn check_stalled(&self, timeout: Duration) -> Vec<u64> {
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

/// Closure mutation operation.
pub type MutationOp<T> = Arc<dyn Fn(&mut T) + Send + Sync>;

/// Full-value replacement operation.
#[derive(Clone)]
pub struct SetValueOp<T>(pub Arc<T>);

/// Unified operation that always operates on `Arc<T>`.
#[derive(Clone)]
pub enum SignalOp<T: Clone + Send + Sync> {
    /// Apply a closure mutation to the value.
    Fn(MutationOp<T>),
    /// Replace the entire value.
    Set(SetValueOp<T>),
}

impl<T: Debug + Clone + Send + Sync> Debug for SignalOp<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fn(_arg0) => f.debug_tuple("Fn").field(&"{anonymous function}").finish(),
            Self::Set(_arg0) => f.debug_tuple("Set").field(&"{anonymous function}").finish(),
        }
    }
}

/// Newtype wrapper over `Arc<T>` that implements [`left_right::Absorb`]
/// for [`SignalOp<T>`].
#[derive(Clone)]
pub struct Absorbable<T: Clone>(pub Arc<T>);

impl<T: Clone + Debug> Debug for Absorbable<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Absorbable").field(&self.0).finish()
    }
}

impl<T: Clone> Deref for Absorbable<T> {
    type Target = Arc<T>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<T: Clone> DerefMut for Absorbable<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
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
        // Clone T into a fresh Arc so that subsequent absorb_first/absorb_second
        // calls see strong_count == 1, making Arc::make_mut a no-op.
        self.0 = Arc::new((*first.0).clone());
    }
}

/// Inner state of a QueuedSignal.
pub struct QueuedState<T: Clone + Send + Sync> {
    /// The left-right read handle for wait-free snapshot reads.
    pub read_handle: ReadHandle<Absorbable<T>>,
    /// Version notification channel. Readers await changes here.
    pub notify_rx: watch::Receiver<u64>,
    /// Health status notification channel.
    pub health_rx: watch::Receiver<HealthStatus>,
    /// Shared reader registry for stall detection.
    pub registry: Arc<ReaderRegistry>,
}

impl<T: Clone + Send + Sync> Debug for QueuedState<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueuedState")
            .field("read_handle", &self.read_handle)
            .field("notify_rx", &self.notify_rx)
            .field("health_rx", &self.health_rx)
            .field("registry", &self.registry)
            .finish()
    }
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
    /// Returns a tracked read guard.
    pub fn read(&self) -> TrackedReadGuard<'_, T> {
        let guard = self.read_handle.enter().unwrap();
        TrackedReadGuard::new(guard, self.registry.clone())
    }
    /// Current health status of the underlying signal.
    pub fn health(&self) -> HealthStatus {
        *self.health_rx.borrow()
    }
    /// Clone of the version notification receiver.
    pub fn notify_rx(&self) -> watch::Receiver<u64> {
        self.notify_rx.clone()
    }

    /// Peek the current version without entering the read side.
    pub fn peek_version(&self) -> u64 {
        *self.notify_rx.borrow()
    }
}

impl<T: Clone + Send + Sync + 'static> QueuedState<T> {
    /// Spawn a background task forwarding values into existing dioxus signals.
    ///
    /// Returns the spawned task so callers can cancel forwarding when
    /// rebinding the target signals to a different source.
    pub fn forward_to<V: 'static>(
        &self,
        value_signal: Signal<V>,
        health_signal: Signal<HealthStatus>,
        map: impl Fn(Arc<T>) -> V + 'static,
    ) -> Task {
        let state = self.clone();
        // Use dioxus local spawn since Signal is not Send.
        spawn(async move {
            let mut value_signal = value_signal;
            let mut health_signal = health_signal;
            let mut nr = state.notify_rx();
            let mut hr = state.health_rx.clone();
            loop {
                tokio::select! {
                    Ok(()) = nr.changed() => {
                        // Retry enter() while a concurrent publish is in
                        // progress, skipping this notification after a
                        // bounded wait.
                        let deadline = Instant::now() + Duration::from_millis(100);
                        let mut entered = false;
                        loop {
                            if let Some(g) = state.read_handle.enter() {
                                let reader_id = state.registry.register();
                                state.registry.heartbeat(reader_id);
                                value_signal.set(map(g.0.clone()));
                                state.registry.unregister(reader_id);
                                entered = true;
                                break;
                            }
                            if Instant::now() >= deadline {
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                        if !entered {
                            error!("read handle stayed pinned for {}, skipping update", type_name::<T>());
                        }
                    }
                    Ok(()) = hr.changed() => {
                        health_signal.set(*hr.borrow());
                    }
                    else => break,
                }
                tokio::task::yield_now().await;
            }
        })
    }
}

/// Read guard for a QueuedSignal.
pub struct TrackedReadGuard<'a, T: Clone + Send + Sync> {
    guard: left_right::ReadGuard<'a, Absorbable<T>>,
    registry: Arc<ReaderRegistry>,
    reader_id: u64,
}

impl<'a, T: Clone + Send + Sync> TrackedReadGuard<'a, T> {
    fn new(guard: left_right::ReadGuard<'a, Absorbable<T>>, registry: Arc<ReaderRegistry>) -> Self {
        let reader_id = registry.register();
        Self {
            guard,
            registry,
            reader_id,
        }
    }
    /// Record a heartbeat, resetting this reader's stall timer.
    pub fn heartbeat(&self) {
        self.registry.heartbeat(self.reader_id);
    }
}

impl<'a, T: Clone + Send + Sync> Drop for TrackedReadGuard<'a, T> {
    fn drop(&mut self) {
        self.registry.unregister(self.reader_id);
    }
}

impl<'a, T: Clone + Send + Sync> Deref for TrackedReadGuard<'a, T> {
    type Target = Arc<T>;
    fn deref(&self) -> &Self::Target {
        &self.guard.0
    }
}

/// Driver for managing reads and writes for a QueuedSignal.
///
/// Owns the left-right [`WriteHandle`] and channels for
/// receiving mutations. Call [`tick`](Self::tick) regularly (e.g.
/// every frame or in a background thread) to drain pending
/// operations, publish to the read side, and update health.
pub struct WriterDriver<T: Clone + Send + Sync> {
    write_handle: WriteHandle<Absorbable<T>, SignalOp<T>>,
    set_value_rx: Receiver<SetValueOp<T>>,
    set_rx: Receiver<MutationOp<T>>,
    add_rx: Receiver<MutationOp<T>>,
    abs_slot: Arc<Mutex<Option<Arc<T>>>>,
    notify_tx: watch::Sender<u64>,
    version: u64,
    health_tx: watch::Sender<HealthStatus>,
    last_health: HealthStatus,
    registry: Arc<ReaderRegistry>,
    /// Timeout for how long signal health updates will be waited for until marking a signal as stalled.
    pub watchdog_timeout: Duration,
    last_publish: Instant,
    /// Sender for authoritative full-value replacements.
    /// Sent values overwrite the entire signal state.
    pub set_value_tx: Sender<SetValueOp<T>>,
    /// Sender for authoritative closure mutations.
    /// These are applied before relative mutations and may be
    /// overridden by a later [`set_value_tx`](Self::set_value_tx) send.
    pub set_tx: Sender<MutationOp<T>>,
    /// Sender for relative closure mutations.
    /// These are applied after all authoritative operations.
    pub add_tx: Sender<MutationOp<T>>,
    /// The read-side state that consumers subscribe to.
    pub queued_state: QueuedState<T>,
    publish_counter: Option<Arc<AtomicU64>>,
}

impl<T: Debug + Clone + Send + Sync> Debug for WriterDriver<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriterDriver")
            .field("write_handle", &self.write_handle)
            .field("set_value_rx", &self.set_value_rx)
            .field("set_rx", &self.set_rx)
            .field("add_rx", &self.add_rx)
            .field("abs_slot", &self.abs_slot)
            .field("notify_tx", &self.notify_tx)
            .field("version", &self.version)
            .field("last_health", &self.last_health)
            .field("health_tx", &self.health_tx)
            .field("registry", &self.registry)
            .field("watchdog_timeout", &self.watchdog_timeout)
            .field("last_publish", &self.last_publish)
            .field("set_value_tx", &self.set_value_tx)
            .field("set_tx", &self.set_tx)
            .field("add_tx", &self.add_tx)
            .field("queued_state", &self.queued_state)
            .finish()
    }
}

impl<T: Clone + Send + Sync + 'static> WriterDriver<T> {
    /// Create a new driver with the given initial value.
    pub fn new(initial: T) -> Self {
        let initial_health = HealthStatus::Healthy;
        let initial_wrapped = Absorbable(Arc::new(initial));
        let (write_handle, read_handle) =
            left_right::new_from_empty::<Absorbable<T>, SignalOp<T>>(initial_wrapped);

        let (notify_tx, notify_rx) = watch::channel(0u64);
        let (health_tx, health_rx) = watch::channel(HealthStatus::Healthy);

        let (set_value_tx, set_value_rx) = flume::unbounded();
        let (set_tx, set_rx) = flume::unbounded();
        let (add_tx, add_rx) = flume::unbounded();

        let registry = Arc::new(ReaderRegistry::default());

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
            publish_counter: None,
            version: 0,
            last_health: initial_health,
        }
    }
    /// Append an operation directly to the write handle (bypasses channels).
    pub fn append(&mut self, op: SignalOp<T>) {
        self.write_handle.append(op);
    }
    /// Attach a counter that will be incremented on each publish.
    pub fn set_publish_counter(&mut self, counter: Arc<AtomicU64>) {
        self.publish_counter = Some(counter);
    }

    /// Drains all buffers and replaces with the given value.
    ///
    /// All pending mutation channels are drained so that the absolute
    /// write is not interleaved with in-flight relative or authoritative
    /// operations.
    pub fn write_absolute(&self, value: T) {
        let arc = Arc::new(value);
        let mut slot = self.abs_slot.lock();
        if slot.is_some() {
            warn!("Absolute value overwritten before being applied.");
        }
        *slot = Some(arc);
        // Drain pending mutation channels so that stale ops don't
        // compete with the absolute write on the next tick.
        self.set_value_rx.drain();
        self.set_rx.drain();
        self.add_rx.drain();
    }

    /// Drain pending operations, publish if the interval has elapsed,
    /// and update the health status.
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
            self.version += 1;
            let _ = self.notify_tx.send(self.version);
            if let Some(ref counter) = self.publish_counter {
                counter.fetch_add(1, Ordering::Relaxed);
            }
        }

        self.update_health();
    }

    fn update_health(&mut self) {
        let stalled = self.registry.check_stalled(self.watchdog_timeout);
        let pinned = stalled.len();
        let status = match pinned {
            0 => HealthStatus::Healthy,
            1 => HealthStatus::Degraded {
                pinned_buffers: pinned,
            },
            _ => HealthStatus::Stalled {
                pinned_buffers: pinned,
            },
        };
        // Don't update health every frame when no changes are made.
        if status != self.last_health {
            self.last_health = status;
            let _ = self.health_tx.send(status);
        }
    }
}

/// A signal providing wait-free reads and queued writes via left-right
/// double buffering.
#[derive(Clone)]
pub struct QueuedSignal<T: Clone + Send + Sync> {
    /// The read-side state that consumers subscribe to.
    pub state: QueuedState<T>,
    // keep the writer alive as long as this signal exists
    _driver: Option<Arc<Mutex<WriterDriver<T>>>>,
    add_tx: Sender<MutationOp<T>>,
    set_tx: Sender<MutationOp<T>>,
    set_value_tx: Sender<SetValueOp<T>>,
}

impl<T: Clone + Send + Sync> Deref for QueuedSignal<T> {
    type Target = QueuedState<T>;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl<T: Clone + Send + Sync + Debug> Debug for QueuedSignal<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueuedSignal")
            .field("state", &self.state)
            .field("add_tx", &self.add_tx)
            .field("set_tx", &self.set_tx)
            .field("set_value_tx", &self.set_value_tx)
            .finish()
    }
}

impl<T: Clone + Send + Sync + 'static> QueuedSignal<T> {
    /// Assemble a QueuedSignal from its constituent parts.
    pub fn from_parts(
        state: QueuedState<T>,
        driver: Option<Arc<Mutex<WriterDriver<T>>>>,
        add_tx: Sender<MutationOp<T>>,
        set_tx: Sender<MutationOp<T>>,
        set_value_tx: Sender<SetValueOp<T>>,
    ) -> Self {
        Self {
            state,
            _driver: driver,
            add_tx,
            set_tx,
            set_value_tx,
        }
    }

    /// Acquire a tracked read guard for the current snapshot.
    pub fn read(&self) -> TrackedReadGuard<'_, T> {
        self.state.read()
    }

    /// Enqueue a relative mutation. Applied after all authoritative
    /// operations within the same tick.
    ///
    /// Use for non-critical adjustments (e.g., incrementing a counter)
    /// that should compose with authoritative changes.
    pub fn mutate<F>(&self, f: F)
    where
        F: Fn(&mut T) + Send + Sync + 'static,
    {
        let _ = self.add_tx.send(Arc::new(f));
    }

    /// Enqueue an authoritative mutation (closure). Applied before
    /// relative mutations but may be overridden by a later
    /// [`set_value`](Self::set_value) in the same tick.
    pub fn mutate_set<F>(&self, f: F)
    where
        F: Fn(&mut T) + Send + Sync + 'static,
    {
        let _ = self.set_tx.send(Arc::new(f));
    }

    /// Enqueue an authoritative full-value replacement. Within a
    /// single tick, `set_value` ops are applied first, followed by
    /// [`mutate_set`](Self::mutate_set), then [`mutate`](Self::mutate).
    ///
    /// To discard all pending mutations and force a pure overwrite,
    /// use [`WriterDriver::write_absolute`] instead.
    pub fn set_value(&self, value: T) {
        let _ = self.set_value_tx.send(SetValueOp(Arc::new(value)));
    }

    /// Current health status.
    pub fn health(&self) -> HealthStatus {
        self.state.health()
    }

    /// Subscribe dioxus signals to this queued signal's value and health,
    /// wrapping each value in `Ok(Arc<T>)`.
    pub fn use_hook<E: 'static>(
        &self,
        error_state: E,
    ) -> (Signal<Result<Arc<T>, E>>, Signal<HealthStatus>) {
        use_queued_state(self.state.clone(), error_state)
    }

    /// Like [`use_hook`], but passes `Arc<T>` directly without wrapping in `Ok()`.
    pub fn use_hook_direct(&self, initial: T) -> (Signal<Arc<T>>, Signal<HealthStatus>) {
        use_queued_state_direct(self.state.clone(), initial)
    }
}

/// Shared helper that subscribes a [`Signal`] to a [`QueuedState`],
/// mapping each published value through `map`.
fn use_queued_state_inner<T: Clone + Send + Sync + 'static, V: 'static>(
    state: QueuedState<T>,
    initial: V,
    map: impl Fn(Arc<T>) -> V + 'static,
) -> (Signal<V>, Signal<HealthStatus>) {
    let mut value_signal = use_signal(|| initial);
    let mut health_signal = use_signal(|| HealthStatus::Healthy);
    let map = Arc::new(map);

    use_future(move || {
        let mut notify_rx = state.notify_rx();
        let mut health_rx = state.health_rx.clone();
        let read_handle = state.read_handle.clone();
        let registry = state.registry.clone();
        let map = map.clone();

        async move {
            loop {
                tokio::select! {
                    Ok(()) = notify_rx.changed() => {
                        // Retry enter() while a concurrent publish is in
                        // progress.
                        let deadline = Instant::now() + Duration::from_millis(100);
                        let mut entered = false;
                        loop {
                            if let Some(guard) = read_handle.enter() {
                                let reader_id = registry.register();
                                registry.heartbeat(reader_id);
                                value_signal.set(map(guard.0.clone()));
                                registry.unregister(reader_id);
                                entered = true;
                                break;
                            }
                            if Instant::now() >= deadline {
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                        if !entered {
                            warn!("use_queued_state: read handle stayed pinned, skipping update");
                        }
                    }
                    Ok(()) = health_rx.changed() => {
                        health_signal.set(*health_rx.borrow());
                    }
                    else => break,
                }
                tokio::task::yield_now().await;
            }
        }
    });

    (value_signal, health_signal)
}

/// Subscribe a [`Signal`] to a [`QueuedState`], wrapping each value in `Ok(Arc<T>)`.
pub fn use_queued_state<T: Clone + Send + Sync + 'static, E: 'static>(
    state: QueuedState<T>,
    error_state: E,
) -> (Signal<Result<Arc<T>, E>>, Signal<HealthStatus>) {
    use_queued_state_inner(state, Err(error_state), |arc| Ok(arc))
}

/// Subscribe a [`Signal`] to a [`QueuedState`], passing `Arc<T>` directly.
pub fn use_queued_state_direct<T: Clone + Send + Sync + 'static>(
    state: QueuedState<T>,
    initial: T,
) -> (Signal<Arc<T>>, Signal<HealthStatus>) {
    let arc = Arc::new(initial);
    use_queued_state_inner(state, arc, |arc| arc)
}

// SAFETY: Absorbable<T> is a newtype over Arc<T>. Arc<T> is Send when
// T: Send + Sync, and our bound enforces both. The left-right crate's
// ReadHandle/WriteHandle internally use Arc<AtomicCell<…>> which are
// also Send + Sync given T: Send.
unsafe impl<T: Clone + Send + Sync> Send for Absorbable<T> {}

// SAFETY: Absorbable<T> is a newtype over Arc<T>. Arc<T> is Sync when
// T: Send + Sync, and our bound enforces both. Shared references to
// Absorbable<T> only permit cloning the inner Arc, which is safe to
// share across threads.
unsafe impl<T: Clone + Send + Sync> Sync for Absorbable<T> {}

// SAFETY: QueuedState<T> contains:
//   - ReadHandle<Absorbable<T>>: internally Arc<AtomicCell<…>>, Send when T: Send
//   - watch::Receiver<u64>: Send + Sync
//   - watch::Receiver<HealthStatus>: Send + Sync
//   - Arc<ReaderRegistry>: Send + Sync (all fields are atomics or Mutex<HashMap<…>>)
// Our bound T: Send + Sync satisfies all transitive requirements.
unsafe impl<T: Clone + Send + Sync> Send for QueuedState<T> {}

// SAFETY: QueuedState<T>'s fields are all Sync-safe:
//   - ReadHandle uses Arc internally, reads are lock-free via atomics
//   - watch::Receiver::borrow() takes a shared reference
//   - ReaderRegistry uses Mutex for interior mutability
// Shared access to QueuedState<T> is therefore sound when T: Sync.
unsafe impl<T: Clone + Send + Sync> Sync for QueuedState<T> {}

/// Read guard for returning refs to inner signal values.
pub struct SignalReadGuard<
    'a,
    T: 'static,
    R: dioxus_signals::Readable<Target = T> + 'static = dioxus_signals::Signal<T>,
> {
    guard: dioxus_signals::ReadableRef<'a, R>,
}

impl<'a, T: 'static, R: dioxus_signals::Readable<Target = T> + 'static> SignalReadGuard<'a, T, R> {
    /// Wrap a dioxus `ReadableRef` into a `SignalReadGuard`.
    pub fn new(guard: dioxus_signals::ReadableRef<'a, R>) -> Self {
        Self { guard }
    }
}

impl<'a, T: 'static, R: dioxus_signals::Readable<Target = T> + 'static> std::ops::Deref
    for SignalReadGuard<'a, T, R>
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn reader_registry_stall_detection() {
        let registry = ReaderRegistry::default();
        let id = registry.register();
        registry.heartbeat(id);
        assert!(
            registry
                .check_stalled(Duration::from_millis(100))
                .is_empty()
        );

        // Simulate a stalled reader by registering but never heartbeating,
        // then waiting past the timeout.
        let id2 = registry.register();
        std::thread::sleep(Duration::from_millis(50));
        let stalled = registry.check_stalled(Duration::from_millis(10));
        assert!(stalled.contains(&id2));

        // id is still heartbeating, should not be stalled.
        registry.heartbeat(id);
        assert!(
            registry
                .check_stalled(Duration::from_millis(100))
                .is_empty()
                || registry.check_stalled(Duration::from_millis(100)) == vec![id2]
        );
    }

    #[test]
    fn reader_registry_unregister() {
        let registry = ReaderRegistry::default();
        let id = registry.register();
        assert!(!registry.check_stalled(Duration::from_millis(0)).is_empty());

        registry.unregister(id);
        assert!(registry.check_stalled(Duration::ZERO).is_empty());
    }

    /// Verify that within a single tick, operations are applied in
    /// priority order: set_value first, then mutate_set, then mutate.
    /// Full-value replacements do not drain the mutation channels
    /// unless the `write_absolute` path is used.
    #[test]
    fn writer_driver_tick_ordering() {
        let mut driver = WriterDriver::new(0i32);

        // All three op types in one tick.
        // Priority order: set_value_rx → set_rx → add_rx
        driver.set_value_tx.send(SetValueOp(Arc::new(42))).unwrap();
        driver
            .set_tx
            .send(Arc::new(|v: &mut i32| *v += 10))
            .unwrap();
        driver.add_tx.send(Arc::new(|v: &mut i32| *v += 1)).unwrap();

        driver.tick(Duration::ZERO);

        // set_value(42) → mutate_set(+10) → mutate(+1) = 53
        let val = driver.queued_state.read().clone();
        assert_eq!(*val, 53i32);
    }

    /// A lone set_value should set the value directly.
    #[test]
    fn writer_driver_tick_set_value_alone() {
        let mut driver = WriterDriver::new(0i32);

        driver.set_value_tx.send(SetValueOp(Arc::new(99))).unwrap();

        driver.tick(Duration::ZERO);

        let val = driver.queued_state.read().clone();
        assert_eq!(*val, 99i32);
    }

    /// Verify that mutate_set wins over mutate when no set_value is
    /// present.
    #[test]
    fn writer_driver_tick_mutate_set_wins_over_mutate() {
        let mut driver = WriterDriver::new(0i32);

        // Relative mutation (applied second)
        driver.add_tx.send(Arc::new(|v: &mut i32| *v += 1)).unwrap();
        // Authoritative mutation (applied first, so result is 10 + 1 = 11)
        driver.set_tx.send(Arc::new(|v: &mut i32| *v = 10)).unwrap();

        driver.tick(Duration::ZERO);

        let val = driver.queued_state.read().clone();
        assert_eq!(*val, 11i32);
    }

    /// Verify health transitions: Healthy → Degraded → Stalled.
    #[test]
    fn health_status_transitions() {
        let mut driver = WriterDriver::new(0i32);

        // Initially healthy
        assert_eq!(driver.queued_state.health(), HealthStatus::Healthy);

        // Register one reader and let it go stale, should be Degraded
        let _id = driver.registry.register();
        // Set a very short watchdog timeout
        driver.watchdog_timeout = Duration::from_millis(1);
        std::thread::sleep(Duration::from_millis(10));

        driver.tick(Duration::ZERO);
        assert_eq!(
            driver.queued_state.health(),
            HealthStatus::Degraded { pinned_buffers: 1 }
        );

        // Register two more stale readers, should be Stalled
        let _id2 = driver.registry.register();
        let _id3 = driver.registry.register();
        std::thread::sleep(Duration::from_millis(10));

        driver.tick(Duration::ZERO);
        assert_eq!(
            driver.queued_state.health(),
            HealthStatus::Stalled { pinned_buffers: 3 }
        );
    }

    /// Absorbable absorb_first should clone the inner Arc for mutation
    /// (via Arc::make_mut) and apply the operation.
    #[test]
    fn absorbable_absorb_first_fn() {
        let mut absorbable = Absorbable(Arc::new(vec![1, 2, 3]));
        let other = Absorbable(Arc::new(vec![]));

        let mut op = SignalOp::Fn(Arc::new(|v: &mut Vec<i32>| v.push(4)));
        absorbable.absorb_first(&mut op, &other);

        assert_eq!(*absorbable.0, vec![1, 2, 3, 4]);
    }

    #[test]
    fn absorbable_absorb_first_set() {
        let mut absorbable = Absorbable(Arc::new(vec![1, 2, 3]));
        let other = Absorbable(Arc::new(vec![]));

        let mut op = SignalOp::Set(SetValueOp(Arc::new(vec![9, 9, 9])));
        absorbable.absorb_first(&mut op, &other);

        assert_eq!(*absorbable.0, vec![9, 9, 9]);
    }

    #[test]
    fn absorbable_sync_with_clones_inner() {
        let first = Absorbable(Arc::new(42i32));
        let mut second = Absorbable(Arc::new(0i32));

        second.sync_with(&first);
        assert_eq!(*second.0, 42i32);
    }

    /// Compile-time Send/Sync verification for the unsafe impl blocks.
    #[test]
    fn absorbable_is_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<Absorbable<i32>>();
        assert_sync::<Absorbable<i32>>();
        assert_send::<QueuedState<i32>>();
        assert_sync::<QueuedState<i32>>();
    }
}
