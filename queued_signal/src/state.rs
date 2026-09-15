use dioxus::prelude::*;
use dioxus::signals::Signal;
use dioxus_core::Task;
use flume::{Receiver, Sender};
use parking_lot::Mutex;
use queued_signal_tracing::error;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::watch;

use crate::macros::warn;
use crate::swap_cell::{ReadGuard, ReadHandle, SwapCellSync};

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

/// Full-value replacement operation carrying an owned value.
#[derive(Debug)]
pub struct SetValueOp<T>(pub T);

/// Inner state of a QueuedSignal.
pub struct QueuedState<T: Clone + Send + Sync> {
    /// Shared read handle over the read buffer.
    pub cell: ReadHandle<T>,
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
            .field("notify_rx", &self.notify_rx)
            .field("health_rx", &self.health_rx)
            .field("registry", &self.registry)
            .finish()
    }
}

impl<T: Clone + Send + Sync> Clone for QueuedState<T> {
    fn clone(&self) -> Self {
        Self {
            cell: self.cell.clone(),
            notify_rx: self.notify_rx.clone(),
            health_rx: self.health_rx.clone(),
            registry: self.registry.clone(),
        }
    }
}

impl<T: Clone + Send + Sync> QueuedState<T> {
    /// Returns a tracked read guard over the current read buffer.
    pub fn read(&self) -> TrackedReadGuard<T> {
        let guard = self.cell.read();
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
    /// Spawn a background task forwarding the version into a dioxus signal.
    pub fn forward_to(
        &self,
        version_signal: Signal<u64>,
        health_signal: Signal<HealthStatus>,
    ) -> Task {
        let state = self.clone();
        // Use dioxus local spawn since Signal is not Send.
        spawn(async move {
            let mut version_signal = version_signal;
            let mut health_signal = health_signal;
            let mut nr = state.notify_rx();
            let mut hr = state.health_rx.clone();
            loop {
                tokio::select! {
                    Ok(()) = nr.changed() => {
                        version_signal.set(*nr.borrow());
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
pub struct TrackedReadGuard<T: Clone + Send + Sync> {
    guard: ReadGuard<T>,
    registry: Arc<ReaderRegistry>,
    reader_id: u64,
}

impl<T: Clone + Send + Sync> TrackedReadGuard<T> {
    fn new(guard: ReadGuard<T>, registry: Arc<ReaderRegistry>) -> Self {
        let reader_id = registry.register();
        registry.heartbeat(reader_id);
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

impl<T: Clone + Send + Sync> Drop for TrackedReadGuard<T> {
    fn drop(&mut self) {
        self.registry.unregister(self.reader_id);
    }
}

impl<T: Clone + Send + Sync> Deref for TrackedReadGuard<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

/// Driver for managing reads and writes for a QueuedSignal.
///
/// Owns the read buffer and the channels for receiving mutations. Call
/// [`tick`](Self::tick) regularly to drain pending operations, publish
/// to readers, and update health.
pub struct WriterDriver<T: Clone + Send + Sync> {
    cell: SwapCellSync<T>,
    set_value_rx: Receiver<SetValueOp<T>>,
    set_rx: Receiver<MutationOp<T>>,
    add_rx: Receiver<MutationOp<T>>,
    abs_slot: Arc<Mutex<Option<T>>>,
    notify_tx: watch::Sender<u64>,
    version: u64,
    health_tx: watch::Sender<HealthStatus>,
    last_health: HealthStatus,
    registry: Arc<ReaderRegistry>,
    /// Timeout for how long signal health updates will be waited for until marking a signal as stalled.
    pub watchdog_timeout: Duration,
    last_publish: Instant,
    /// Sender for authoritative full-value replacements.
    pub set_value_tx: Sender<SetValueOp<T>>,
    /// Sender for authoritative closure mutations.
    pub set_tx: Sender<MutationOp<T>>,
    /// Sender for relative closure mutations.
    pub add_tx: Sender<MutationOp<T>>,
    /// The read-side state that consumers subscribe to.
    pub queued_state: QueuedState<T>,
    publish_counter: Option<Arc<AtomicU64>>,
}

impl<T: Debug + Clone + Send + Sync> Debug for WriterDriver<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriterDriver")
            .field("version", &self.version)
            .field("last_health", &self.last_health)
            .field("registry", &self.registry)
            .field("watchdog_timeout", &self.watchdog_timeout)
            .field("last_publish", &self.last_publish)
            .field("queued_state", &self.queued_state)
            .finish()
    }
}

impl<T: Clone + Send + Sync + 'static> WriterDriver<T> {
    fn build(cell: SwapCellSync<T>) -> Self {
        let (notify_tx, notify_rx) = watch::channel(0u64);
        let (health_tx, health_rx) = watch::channel(HealthStatus::Healthy);

        let (set_value_tx, set_value_rx) = flume::unbounded();
        let (set_tx, set_rx) = flume::unbounded();
        let (add_tx, add_rx) = flume::unbounded();

        let registry = Arc::new(ReaderRegistry::default());

        let state = QueuedState {
            cell: cell.share(),
            notify_rx,
            health_rx,
            registry: registry.clone(),
        };

        Self {
            cell,
            set_value_rx,
            set_rx,
            add_rx,
            abs_slot: Arc::new(Mutex::new(None)),
            notify_tx,
            health_tx,
            registry,
            watchdog_timeout: Duration::from_millis(500),
            last_publish: Instant::now(),
            publish_counter: None,
            version: 0,
            last_health: HealthStatus::Healthy,
            set_value_tx,
            set_tx,
            add_tx,
            queued_state: state,
        }
    }

    /// Create a new driver with an initial read buffer value.
    pub fn new(initial: T) -> Self {
        Self::build(SwapCellSync::new(initial))
    }

    /// Attach a counter that will be incremented on each publish.
    pub fn set_publish_counter(&mut self, counter: Arc<AtomicU64>) {
        self.publish_counter = Some(counter);
    }

    /// Drains all channels and returns the pending operations.
    pub fn drain_ops(&mut self) -> (Vec<T>, Vec<MutationOp<T>>, Vec<MutationOp<T>>) {
        let set_values = self.set_value_rx.drain().map(|op| op.0).collect();
        let sets = self.set_rx.drain().collect();
        let adds = self.add_rx.drain().collect();
        (set_values, sets, adds)
    }

    /// Borrows the read buffer value.
    pub fn read(&self) -> ReadGuard<T> {
        self.cell.read()
    }

    /// Returns mutable access to the read buffer when no readers exist.
    pub fn get_mut(&mut self) -> Result<&mut T, usize> {
        self.cell.get_mut()
    }

    /// Swaps the read buffer value with `with`, zero clone.
    pub fn try_swap(&mut self, with: &mut T) -> Result<(), usize> {
        self.cell.try_swap(with)
    }

    /// Publishes the current version to readers.
    pub fn publish(&mut self) {
        self.version += 1;
        let _ = self.notify_tx.send(self.version);
        if let Some(ref counter) = self.publish_counter {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Drains all buffers and replaces with the given value.
    pub fn write_absolute(&self, value: T) {
        let mut slot = self.abs_slot.lock();
        if slot.is_some() {
            warn!("Absolute value overwritten before being applied.");
        }
        *slot = Some(value);
        self.set_value_rx.drain();
        self.set_rx.drain();
        self.add_rx.drain();
    }

    /// Drain pending operations, publish if the interval has elapsed,
    /// and update the health status.
    pub fn tick(&mut self, publish_interval: Duration) {
        let abs_taken = { self.abs_slot.lock().take() };

        let did_work;

        if let Some(abs_val) = abs_taken {
            match self.cell.get_mut() {
                Ok(slot) => {
                    *slot = abs_val;
                    self.set_value_rx.drain();
                    self.set_rx.drain();
                    self.add_rx.drain();
                    did_work = true;
                }
                Err(_count) => {
                    error!("readers active, deferring absolute write: {}", _count);
                    *self.abs_slot.lock() = Some(abs_val);
                    did_work = false;
                }
            }
        } else {
            let (set_values, sets, adds) = self.drain_ops();
            let has_ops = !set_values.is_empty() || !sets.is_empty() || !adds.is_empty();
            if !has_ops {
                self.update_health();
                return;
            }
            match self.cell.get_mut() {
                Ok(slot) => {
                    for value in set_values {
                        *slot = value;
                    }
                    for f in sets {
                        f(slot);
                    }
                    for f in adds {
                        f(slot);
                    }
                    did_work = true;
                }
                Err(_count) => {
                    error!("readers active, deferring mutations: {}", _count);
                    for value in set_values {
                        let _ = self.set_value_tx.send(SetValueOp(value));
                    }
                    for f in sets {
                        let _ = self.set_tx.send(f);
                    }
                    for f in adds {
                        let _ = self.add_tx.send(f);
                    }
                    did_work = false;
                }
            }
        }

        if did_work && self.last_publish.elapsed() >= publish_interval {
            self.publish();
            self.last_publish = Instant::now();
        }

        self.update_health();
    }

    /// Recomputes and publishes the current health status.
    pub fn update_health(&mut self) {
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
        if status != self.last_health {
            self.last_health = status;
            let _ = self.health_tx.send(status);
        }
    }
}

/// A signal providing borrow reads and queued writes.
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

    /// Acquire a tracked read guard for the current value.
    pub fn read(&self) -> TrackedReadGuard<T> {
        self.state.read()
    }

    /// Enqueue a relative mutation.
    pub fn mutate<F>(&self, f: F)
    where
        F: Fn(&mut T) + Send + Sync + 'static,
    {
        let _ = self.add_tx.send(Arc::new(f));
    }

    /// Enqueue an authoritative mutation.
    pub fn mutate_set<F>(&self, f: F)
    where
        F: Fn(&mut T) + Send + Sync + 'static,
    {
        let _ = self.set_tx.send(Arc::new(f));
    }

    /// Enqueue an authoritative full-value replacement.
    pub fn set_value(&self, value: T) {
        let _ = self.set_value_tx.send(SetValueOp(value));
    }

    /// Current health status.
    pub fn health(&self) -> HealthStatus {
        self.state.health()
    }

    /// Subscribe dioxus signals to this queued signal's value and health.
    pub fn use_hook<E: 'static>(
        &self,
        error_state: E,
    ) -> (Signal<Result<ReadGuard<T>, E>>, Signal<HealthStatus>) {
        use_queued_state(self.state.clone(), error_state)
    }

    /// Like [`use_hook`], but passes the read guard directly.
    pub fn use_hook_direct(&self, initial: T) -> (Signal<ReadGuard<T>>, Signal<HealthStatus>) {
        use_queued_state_direct(self.state.clone(), initial)
    }
}

/// Shared helper that subscribes a [`Signal`] to a [`QueuedState`].
fn use_queued_state_inner<T: Clone + Send + Sync + 'static, V: 'static>(
    state: QueuedState<T>,
    initial: V,
    map: impl Fn(ReadGuard<T>) -> V + 'static,
) -> (Signal<V>, Signal<HealthStatus>) {
    let mut value_signal = use_signal(|| initial);
    let mut health_signal = use_signal(|| HealthStatus::Healthy);
    let map = Arc::new(map);

    use_future(move || {
        let mut notify_rx = state.notify_rx();
        let mut health_rx = state.health_rx.clone();
        let cell = state.cell.clone();
        let map = map.clone();

        async move {
            loop {
                tokio::select! {
                    Ok(()) = notify_rx.changed() => {
                        value_signal.set(map(cell.read()));
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

/// Subscribe a [`Signal`] to a [`QueuedState`], wrapping each read in `Ok`.
pub fn use_queued_state<T: Clone + Send + Sync + 'static, E: 'static>(
    state: QueuedState<T>,
    error_state: E,
) -> (Signal<Result<ReadGuard<T>, E>>, Signal<HealthStatus>) {
    use_queued_state_inner(state, Err(error_state), |guard| Ok(guard))
}

/// Subscribe a [`Signal`] to a [`QueuedState`], passing the guard directly.
pub fn use_queued_state_direct<T: Clone + Send + Sync + 'static>(
    state: QueuedState<T>,
    _initial: T,
) -> (Signal<ReadGuard<T>>, Signal<HealthStatus>) {
    let guard = state.cell.read();
    use_queued_state_inner(state, guard, |guard| guard)
}

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

        let id2 = registry.register();
        std::thread::sleep(Duration::from_millis(50));
        let stalled = registry.check_stalled(Duration::from_millis(10));
        assert!(stalled.contains(&id2));

        registry.heartbeat(id);
        let stalled = registry.check_stalled(Duration::from_millis(100));
        assert!(stalled.is_empty() || stalled == vec![id2]);
    }

    #[test]
    fn reader_registry_unregister() {
        let registry = ReaderRegistry::default();
        let id = registry.register();
        assert!(!registry.check_stalled(Duration::from_millis(0)).is_empty());

        registry.unregister(id);
        assert!(registry.check_stalled(Duration::ZERO).is_empty());
    }

    #[test]
    fn writer_driver_tick_ordering() {
        let mut driver = WriterDriver::new(0i32);

        driver.set_value_tx.send(SetValueOp(42)).unwrap();
        driver
            .set_tx
            .send(Arc::new(|v: &mut i32| *v += 10))
            .unwrap();
        driver.add_tx.send(Arc::new(|v: &mut i32| *v += 1)).unwrap();

        driver.tick(Duration::ZERO);

        let val = driver.queued_state.read().clone();
        assert_eq!(val, 53i32);
    }

    #[test]
    fn writer_driver_tick_set_value_alone() {
        let mut driver = WriterDriver::new(0i32);

        driver.set_value_tx.send(SetValueOp(99)).unwrap();

        driver.tick(Duration::ZERO);

        let val = driver.queued_state.read().clone();
        assert_eq!(val, 99i32);
    }

    #[test]
    fn writer_driver_tick_mutate_set_wins_over_mutate() {
        let mut driver = WriterDriver::new(0i32);

        driver.add_tx.send(Arc::new(|v: &mut i32| *v += 1)).unwrap();
        driver.set_tx.send(Arc::new(|v: &mut i32| *v = 10)).unwrap();

        driver.tick(Duration::ZERO);

        let val = driver.queued_state.read().clone();
        assert_eq!(val, 11i32);
    }

    #[test]
    fn health_status_transitions() {
        let mut driver = WriterDriver::new(0i32);

        assert_eq!(driver.queued_state.health(), HealthStatus::Healthy);

        let _id = driver.registry.register();
        driver.watchdog_timeout = Duration::from_millis(1);
        std::thread::sleep(Duration::from_millis(10));

        driver.update_health();
        assert_eq!(
            driver.queued_state.health(),
            HealthStatus::Degraded { pinned_buffers: 1 }
        );

        let _id2 = driver.registry.register();
        let _id3 = driver.registry.register();
        std::thread::sleep(Duration::from_millis(10));

        driver.update_health();
        assert_eq!(
            driver.queued_state.health(),
            HealthStatus::Stalled { pinned_buffers: 3 }
        );
    }

    /// Sentinel whose Clone counts invocations, to prove the write path
    /// never clones the inner value.
    #[derive(Debug)]
    struct CountClones {
        counter: Arc<AtomicU64>,
        value: i32,
    }

    impl Clone for CountClones {
        fn clone(&self) -> Self {
            self.counter.fetch_add(1, Ordering::Relaxed);
            Self {
                counter: self.counter.clone(),
                value: self.value,
            }
        }
    }

    #[test]
    fn writer_driver_write_path_does_not_clone() {
        let counter = Arc::new(AtomicU64::new(0));
        let initial = CountClones {
            counter: counter.clone(),
            value: 0,
        };
        let mut driver = WriterDriver::new(initial);

        assert_eq!(counter.load(Ordering::Relaxed), 0);

        driver
            .set_value_tx
            .send(SetValueOp(CountClones {
                counter: counter.clone(),
                value: 7,
            }))
            .unwrap();
        driver
            .add_tx
            .send(Arc::new(|v: &mut CountClones| v.value += 1))
            .unwrap();

        driver.tick(Duration::ZERO);

        assert_eq!(counter.load(Ordering::Relaxed), 0);
        let val = driver.queued_state.read().clone();
        assert_eq!(val.value, 8);
    }

    #[test]
    fn queued_state_is_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<QueuedState<i32>>();
        assert_sync::<QueuedState<i32>>();
    }
}
