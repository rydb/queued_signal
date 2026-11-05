//! Read and write state for a queued signal.
//!
//! A read pins the current generation and loads it, so it never waits on the writer.
//! The writer driver applies queued mutations, publishes a new generation, and wakes the
//! reactive contexts recorded in the subscriber slots.
//!
//! The driver begins one subscriber pass per frame and publishes inside that pass. A slot is
//! pruned after more than [`PRUNE_AFTER_PASSES`](crate::subscriber_slots::PRUNE_AFTER_PASSES)
//! passes without a read, so a component that stops reading stops rendering.
//!
//! Retained generations drive [`HealthStatus`](crate::state::HealthStatus). `Healthy` means every
//! replaced generation was dropped. `Degraded` means one is still retained while reclamation is
//! still making progress, and `Stalled` means a whole window of publishes released nothing, which
//! points at a reader starving reclamation.

use dioxus::prelude::*;
use dioxus::signals::Signal;
use dioxus_core::Task;
use flume::{Receiver, Sender};
use kovan::{Guard, pin};
use parking_lot::Mutex;
use std::any::Any;
use std::fmt::{Debug, Display};
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::watch;

use dioxus_core::ReactiveContext;

use crate::atom_coerce::{AtomCoerce, AtomCoerceHandle, RcuGuard, RcuGuardTyped};
use crate::atom_coerce_dyn::{AtomCoerceDynHandle, AtomCoerceDynSlotGuard, EraseMut};
use crate::subscriber_slots::{PassEpoch, SubscriberSlots, VisitStats};

/// Health status of QueuedSignal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    /// Every replaced generation has been reclaimed.
    Healthy,
    /// Generations are still retained while reclamation is making progress.
    Degraded {
        /// Number of retained generations.
        pinned_buffers: usize,
    },
    /// A window of publishes released nothing and the backlog outgrew that window.
    Stalled {
        /// Number of retained generations.
        pinned_buffers: usize,
    },
}

impl HealthStatus {
    /// Maps a retained generation count onto its status, with no progress information.
    pub fn from_pinned(pinned: usize) -> Self {
        match pinned {
            0 => HealthStatus::Healthy,
            _ => HealthStatus::Degraded {
                pinned_buffers: pinned,
            },
        }
    }
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

/// Version and health words shared by the writer and every reader.
///
/// The writer stores both words with `Release` and readers load them with `Acquire`.
/// Watch channels remain for async forwarders, and the value read path never touches them.
pub struct ReadAtomics {
    version: AtomicU64,
    pinned: AtomicUsize,
    status: AtomicU64,
    /// Receivers held by state instances, which never wait on the version channel.
    ///
    /// Comparing this with the sender's receiver count tells the writer whether any wait
    /// exists, so a publish with no async observer skips the channel update.
    owned_receivers: AtomicUsize,
}

/// Bits reserved for the status tag, with the retained count above them.
const STATUS_TAG_BITS: u32 = 2;

const fn encode_health(status: HealthStatus) -> u64 {
    match status {
        HealthStatus::Healthy => 0,
        HealthStatus::Degraded { pinned_buffers } => {
            1 | ((pinned_buffers as u64) << STATUS_TAG_BITS)
        }
        HealthStatus::Stalled { pinned_buffers } => {
            2 | ((pinned_buffers as u64) << STATUS_TAG_BITS)
        }
    }
}

const fn decode_health(word: u64) -> HealthStatus {
    let pinned_buffers = (word >> STATUS_TAG_BITS) as usize;
    match word & 0b11 {
        0 => HealthStatus::Healthy,
        1 => HealthStatus::Degraded { pinned_buffers },
        _ => HealthStatus::Stalled { pinned_buffers },
    }
}

impl ReadAtomics {
    /// Creates the words at version zero with no retained generation.
    ///
    /// The first state instance contributes one owned receiver.
    pub fn new() -> Self {
        Self {
            version: AtomicU64::new(0),
            pinned: AtomicUsize::new(0),
            status: AtomicU64::new(0),
            owned_receivers: AtomicUsize::new(1),
        }
    }

    /// Records one state instance holding its own version receiver.
    pub(crate) fn own_receiver(&self) {
        self.owned_receivers.fetch_add(1, Ordering::Relaxed);
    }

    /// Releases the record for a state instance that is dropping.
    pub(crate) fn release_receiver(&self) {
        self.owned_receivers.fetch_sub(1, Ordering::Relaxed);
    }

    /// Number of version receivers held by state instances.
    pub fn owned_receivers(&self) -> usize {
        self.owned_receivers.load(Ordering::Relaxed)
    }

    /// Publishes a version to readers.
    pub fn store_version(&self, version: u64) {
        self.version.store(version, Ordering::Release);
    }

    /// Publishes the number of retained generations to readers.
    pub fn store_pinned(&self, pinned: usize) {
        self.pinned.store(pinned, Ordering::Release);
    }

    /// Publishes a health status to readers.
    pub fn store_health(&self, status: HealthStatus) {
        self.status.store(encode_health(status), Ordering::Release);
    }

    /// Reads the latest published version.
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// Reads the latest retained generation count.
    pub fn pinned(&self) -> usize {
        self.pinned.load(Ordering::Acquire)
    }

    /// Reads the latest health status.
    pub fn health(&self) -> HealthStatus {
        decode_health(self.status.load(Ordering::Acquire))
    }
}

impl Default for ReadAtomics {
    fn default() -> Self {
        Self::new()
    }
}

/// A queued mutation operating on either the typed value or the erased view.
pub enum MutationOp<T, D: ?Sized + 'static = dyn Any>
where
    T: Clone + Send + Sync + 'static,
{
    /// Mutation through the typed value.
    Typed(Arc<dyn Fn(&mut T) + Send + Sync>),
    /// Mutation through the erased view, coerced during application.
    Untyped(Arc<dyn Fn(&mut D) + Send + Sync>),
}

impl<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> MutationOp<T, D> {
    /// Applies this mutation to a typed value.
    pub fn apply(&self, value: &mut T)
    where
        T: EraseMut<D>,
    {
        match self {
            MutationOp::Typed(f) => f(value),
            MutationOp::Untyped(f) => {
                let ptr = (value as *mut T) as *mut ();
                // SAFETY: value is a live T and EraseMut unsizes it to D.
                let view = unsafe { &mut *<T as EraseMut<D>>::erase_mut(ptr) };
                f(view);
            }
        }
    }
}

/// Full-value replacement operation carrying an owned value.
#[derive(Debug)]
pub struct SetValueOp<T>(T);

impl<T> SetValueOp<T> {
    /// Wraps a value for the next resolve.
    pub fn new(value: T) -> Self {
        Self(value)
    }

    /// Borrows the value from the write buffer.
    pub fn value(&self) -> &T {
        &self.0
    }

    /// Moves the value into the read buffer.
    pub(crate) fn into_value(self) -> T {
        self.0
    }
}

/// A queued write, applied in arrival order.
pub enum Op<T: Clone + Send + Sync + 'static, D: ?Sized + 'static = dyn Any> {
    /// Replaces the value.
    Set(SetValueOp<T>),
    /// Mutates the value through the typed or erased view.
    SetInPlace(MutationOp<T, D>),
    /// Composes a relative change onto the value.
    Add(MutationOp<T, D>),
    /// Replaces the value at the submit point in arrival order.
    Absolute(SetValueOp<T>),
}

/// Inner state of a QueuedSignal.
pub struct QueuedState<T: Clone + Send + Sync + 'static, D: ?Sized + 'static = dyn Any> {
    /// Shared read handle over the read buffer.
    pub cell: AtomCoerceHandle<T, D>,
    /// Version notification channel. Readers await changes here.
    pub notify_rx: watch::Receiver<u64>,
    /// Health status notification channel.
    pub health_rx: watch::Receiver<HealthStatus>,
    /// Subscriber slots used to wake reactive contexts.
    pub slots: SubscriberSlots<ReactiveContext>,
    /// Lock free version and health words.
    pub atomics: Arc<ReadAtomics>,
}

impl<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> Debug for QueuedState<T, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueuedState")
            .field("notify_rx", &self.notify_rx)
            .field("health_rx", &self.health_rx)
            .finish()
    }
}

impl<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> Clone for QueuedState<T, D> {
    fn clone(&self) -> Self {
        self.atomics.own_receiver();
        Self {
            cell: self.cell.clone(),
            notify_rx: self.notify_rx.clone(),
            health_rx: self.health_rx.clone(),
            slots: self.slots.clone(),
            atomics: self.atomics.clone(),
        }
    }
}

impl<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> Drop for QueuedState<T, D> {
    fn drop(&mut self) {
        self.atomics.release_receiver();
    }
}

impl<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> QueuedState<T, D> {
    /// Returns a tracked read guard over the current read buffer.
    pub fn read(&self) -> TrackedReadGuard<T> {
        let guard = self.cell.read_typed();
        if let Some(context) = ReactiveContext::current() {
            self.slots.join(context, guard.version());
        }
        TrackedReadGuard::new(guard)
    }

    /// Pins the current generation once and reuses the pin across a frame.
    pub fn frame_read(&self) -> FrameRead<T, D> {
        FrameRead {
            state: self.clone(),
            epoch: None,
            ptr: std::ptr::null(),
            version: 0,
        }
    }

    /// Projects the current value without registering a reader.
    pub fn read_cached<U, F>(&self, f: F) -> ProjectedReadGuard<U>
    where
        U: Send + Sync + 'static,
        F: FnOnce(&T) -> &U,
    {
        ProjectedReadGuard::from_pinned(self.cell.read_typed().project(f))
    }

    /// The typed cell viewed through the erased trait object.
    pub fn cell_dyn(&self) -> &AtomCoerceHandle<T, D> {
        &self.cell
    }

    /// Current health status of the underlying signal.
    pub fn health(&self) -> HealthStatus {
        self.atomics.health()
    }

    /// Starts a new subscriber pass and returns its number.
    ///
    /// The built in driver calls this once per [`tick`](WriterDriver::tick). A custom driver
    /// must call it once per frame, and readers stay subscribed while their pass gap is small.
    pub fn begin_pass(&self) -> u64 {
        self.slots.begin_pass()
    }

    /// Clone of the version notification receiver.
    pub fn notify_rx(&self) -> watch::Receiver<u64> {
        self.notify_rx.clone()
    }

    /// Peek the current version without entering the read side.
    pub fn peek_version(&self) -> u64 {
        self.atomics.version()
    }
}

impl<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> QueuedState<T, D> {
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

    /// Spawns a task forwarding the mapped value and health into dioxus signals.
    pub fn forward_value_to<V, E>(
        &self,
        value_signal: Signal<Result<V, E>>,
        health_signal: Signal<HealthStatus>,
        map: impl Fn(Arc<T>) -> Result<V, E> + Send + Sync + 'static,
    ) -> Task
    where
        V: 'static,
        E: 'static,
    {
        let state = self.clone();
        let map = Arc::new(map);
        spawn(async move {
            let mut value_signal = value_signal;
            let mut health_signal = health_signal;
            let mut nr = state.notify_rx();
            let mut hr = state.health_rx.clone();
            let cell = state.cell.clone();
            let map = map.clone();
            loop {
                tokio::select! {
                    Ok(()) = nr.changed() => {
                        let guard = cell.read_owned();
                        let arc = Arc::new((*guard).clone());
                        value_signal.set(map(arc));
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

/// Read-side state for an erased queued signal view.
pub struct QueuedStateDyn<D: ?Sized + 'static> {
    /// Rebindable erased read handle over the current value.
    pub view: AtomCoerceDynHandle<D>,
    /// Version notification channel. Readers await changes here.
    pub notify_rx: watch::Receiver<u64>,
    /// Health status notification channel.
    pub health_rx: watch::Receiver<HealthStatus>,
    /// Subscriber slots used to wake reactive contexts.
    pub slots: SubscriberSlots<ReactiveContext>,
    /// Lock free version and health words.
    pub atomics: Arc<ReadAtomics>,
}

impl<D: ?Sized + 'static> Debug for QueuedStateDyn<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueuedStateDyn")
            .field("notify_rx", &self.notify_rx)
            .field("health_rx", &self.health_rx)
            .finish()
    }
}

impl<D: ?Sized + 'static> Clone for QueuedStateDyn<D> {
    fn clone(&self) -> Self {
        self.atomics.own_receiver();
        Self {
            view: self.view.clone(),
            notify_rx: self.notify_rx.clone(),
            health_rx: self.health_rx.clone(),
            slots: self.slots.clone(),
            atomics: self.atomics.clone(),
        }
    }
}

impl<D: ?Sized + 'static> Drop for QueuedStateDyn<D> {
    fn drop(&mut self) {
        self.atomics.release_receiver();
    }
}

impl<D: ?Sized + 'static> QueuedStateDyn<D> {
    /// Returns a tracked read guard over the erased view.
    pub fn read(&self) -> TrackedReadGuardDyn<D> {
        let guard = self.view.get();
        if let Some(context) = ReactiveContext::current() {
            self.slots.join(context, guard.version());
        }
        TrackedReadGuardDyn::new(guard)
    }

    /// Current health status of the underlying signal.
    pub fn health(&self) -> HealthStatus {
        self.atomics.health()
    }

    /// Starts a new subscriber pass and returns its number.
    ///
    /// Call this once per frame from the driver that publishes this signal.
    pub fn begin_pass(&self) -> u64 {
        self.slots.begin_pass()
    }

    /// Clone of the version notification receiver.
    pub fn notify_rx(&self) -> watch::Receiver<u64> {
        self.notify_rx.clone()
    }

    /// Peek the current version without entering the read side.
    pub fn peek_version(&self) -> u64 {
        self.atomics.version()
    }

    /// Spawn a background task forwarding the version into a dioxus signal.
    pub fn forward_to(
        &self,
        version_signal: Signal<u64>,
        health_signal: Signal<HealthStatus>,
    ) -> Task {
        let state = self.clone();
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

/// A pin held across one frame.
///
/// The scope takes one pin when it first reads and keeps it until the scope drops. Each refresh
/// loads the published node once, and the node holds the version with the value, so a frame read
/// is one dereference into one line rather than a version word plus a node.
pub struct FrameRead<T, D = dyn Any>
where
    T: Clone + Send + Sync + 'static,
    D: ?Sized + 'static,
{
    state: QueuedState<T, D>,
    epoch: Option<Guard>,
    ptr: *const T,
    version: u64,
}

impl<T, D> FrameRead<T, D>
where
    T: Clone + Send + Sync + 'static,
    D: ?Sized + 'static,
{
    /// Reads the newest value under the scope's existing pin.
    pub fn read(&mut self) -> &T {
        self.refresh();
        // SAFETY: the scope keeps the epoch pinned, and the node behind the pointer was loaded
        // under that pin, so the value stays alive for the borrow.
        unsafe { &*self.ptr }
    }

    /// Reads the newest value and records the reader in the slot array.
    pub fn read_tracked(&mut self) -> &T {
        self.refresh();
        if let Some(context) = ReactiveContext::current() {
            self.state.slots.join(context, self.version);
        }
        // SAFETY: as in `read`.
        unsafe { &*self.ptr }
    }

    /// Version of the generation this scope holds.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Pins once for the scope and reads the published node.
    fn refresh(&mut self) {
        if self.epoch.is_none() {
            self.epoch = Some(pin());
        }
        let guard = self
            .epoch
            .as_ref()
            .expect("a frame read pins before it loads");
        let (ptr, version) = self.state.cell.load_typed_under(guard);
        self.ptr = ptr;
        self.version = version;
    }
}

/// Read guard for a QueuedSignal.
pub struct TrackedReadGuard<T: Send + Sync + 'static> {
    guard: RcuGuardTyped<T>,
}

impl<T: Send + Sync + 'static + PartialEq> PartialEq for TrackedReadGuard<T> {
    fn eq(&self, other: &Self) -> bool {
        self.guard == other.guard
    }
}

impl<T: Send + Sync + 'static> TrackedReadGuard<T> {
    fn new(guard: RcuGuardTyped<T>) -> Self {
        Self { guard }
    }

    /// Kept for API compatibility. Retention is tracked per generation.
    pub fn heartbeat(&self) {}

    /// Projects the guard onto a value inside the pinned item.
    ///
    /// The read pin moves into the projected guard, so no value is cloned and no shared count is touched.
    pub fn project<U: Send + Sync + 'static>(
        self,
        f: impl FnOnce(&T) -> &U,
    ) -> ProjectedReadGuard<U> {
        ProjectedReadGuard {
            source: ProjectedSource::Pinned {
                guard: self.guard.project(f),
            },
        }
    }
}

/// Storage backing a projected read guard.
enum ProjectedSource<U: Send + Sync + 'static> {
    /// A projection into a pinned item.
    Pinned { guard: RcuGuardTyped<U> },
    /// An owned value with no read pin.
    Owned(U),
}

/// Read guard projected onto a value inside the pinned item.
///
/// A guard may also wrap an owned value when no signal source exists yet.
/// Equality compares the projected values, which lets consumers deduplicate updates by field.
pub struct ProjectedReadGuard<U: Send + Sync + 'static> {
    source: ProjectedSource<U>,
}

impl<U: Send + Sync + PartialEq + 'static> PartialEq for ProjectedReadGuard<U> {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl<U: Send + Sync + Eq + 'static> Eq for ProjectedReadGuard<U> {}

impl<U: Send + Sync + Debug + 'static> Debug for ProjectedReadGuard<U> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ProjectedReadGuard")
            .field(self.as_ref())
            .finish()
    }
}

impl<U: Send + Sync + 'static> ProjectedReadGuard<U> {
    /// Wraps an owned value without a read pin.
    ///
    /// Use this when no signal source is available yet.
    pub fn from_owned(value: U) -> Self {
        Self {
            source: ProjectedSource::Owned(value),
        }
    }

    /// Wraps a pinned projection that registers no reader.
    pub fn from_pinned(guard: RcuGuardTyped<U>) -> Self {
        Self {
            source: ProjectedSource::Pinned { guard },
        }
    }

    /// Kept for API compatibility. Retention is tracked per generation.
    pub fn heartbeat(&self) {}
}

impl<U: Send + Sync + 'static> Deref for ProjectedReadGuard<U> {
    type Target = U;

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

/// Read guard for an erased view over a queued signal.
pub struct TrackedReadGuardDyn<D: ?Sized + 'static> {
    guard: AtomCoerceDynSlotGuard<D>,
}

impl<D: ?Sized + 'static> TrackedReadGuardDyn<D> {
    /// Wraps an erased slot guard.
    pub fn new(guard: AtomCoerceDynSlotGuard<D>) -> Self {
        Self { guard }
    }

    /// Kept for API compatibility. Retention is tracked per generation.
    pub fn heartbeat(&self) {}
}

impl<D: ?Sized + 'static> Deref for TrackedReadGuardDyn<D> {
    type Target = D;

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<T: Clone + Send + Sync + 'static> Deref for TrackedReadGuard<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<T: Send + Sync + 'static> AsRef<T> for TrackedReadGuard<T> {
    /// Borrows the underlying value.
    fn as_ref(&self) -> &T {
        &self.guard
    }
}

impl<U: Send + Sync + 'static> AsRef<U> for ProjectedReadGuard<U> {
    /// Borrows the projected value.
    fn as_ref(&self) -> &U {
        match &self.source {
            ProjectedSource::Pinned { guard } => guard,
            ProjectedSource::Owned(value) => value,
        }
    }
}

impl<D: ?Sized + 'static> AsRef<D> for TrackedReadGuardDyn<D> {
    /// Borrows the erased value.
    fn as_ref(&self) -> &D {
        self.guard.as_dyn()
    }
}

/// Pending operations returned by a driver drain.
pub type DrainedOps<T, D> = Vec<Op<T, D>>;

/// Publishes with no reclamation progress that report a stall.
///
/// A replaced generation is released by a later retire, so a couple of publishes with nothing freed
/// is the normal lag. A whole window with nothing freed means a reader is starving the reclaimer.
const STALL_WINDOW_PUBLISHES: u64 = 4;

/// Ticks between health scans.
///
/// Health is a diagnostic, so it is sampled on a short cadence rather than on every tick. A stall
/// is still reported within this many frames.
const HEALTH_INTERVAL_TICKS: u32 = 8;

/// Driver for managing reads and writes for a QueuedSignal.
///
/// Owns the read buffer and the channels for receiving mutations. Call
/// [`tick`](Self::tick) regularly to drain pending operations, publish
/// to readers, and update health.
pub struct WriterDriver<
    T: Clone + Send + Sync + 'static + EraseMut<D>,
    D: ?Sized + 'static = dyn Any,
> {
    cell: AtomCoerce<T, D>,
    op_rx: Receiver<Op<T, D>>,
    notify_tx: watch::Sender<u64>,
    version: u64,
    current_version: u64,
    health_tx: watch::Sender<HealthStatus>,
    last_health: HealthStatus,
    /// Ticks since the last health scan.
    ticks_since_health: u32,
    last_publish: Instant,
    /// Reclaimed count recorded at each of the last publishes, newest last.
    reclaim_history: std::collections::VecDeque<u64>,
    /// Whether the last window of publishes saw no reclamation progress.
    starved: bool,
    /// Sender for queued writes
    pub op_tx: Sender<Op<T, D>>,
    /// The read-side state that consumers subscribe to.
    pub queued_state: QueuedState<T, D>,
    publish_counter: Option<Arc<AtomicU64>>,
}

impl<T: Debug + Clone + Send + Sync + 'static + EraseMut<D>, D: ?Sized + 'static> Debug
    for WriterDriver<T, D>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriterDriver")
            .field("version", &self.version)
            .field("last_health", &self.last_health)
            .field("last_publish", &self.last_publish)
            .field("queued_state", &self.queued_state)
            .finish()
    }
}

impl<T: Clone + Send + Sync + 'static + EraseMut<D>, D: ?Sized + 'static> WriterDriver<T, D> {
    fn build(cell: AtomCoerce<T, D>) -> Self {
        let (notify_tx, notify_rx) = watch::channel(0u64);
        let (health_tx, health_rx) = watch::channel(HealthStatus::Healthy);

        let (op_tx, op_rx) = flume::unbounded();

        let pass = PassEpoch::new();
        let slots = SubscriberSlots::new(pass);
        let atomics = Arc::new(ReadAtomics::new());

        let state: QueuedState<T, D> = QueuedState {
            cell: cell.handle(),
            notify_rx,
            health_rx,
            slots,
            atomics,
        };

        Self {
            cell,
            op_rx,
            notify_tx,
            health_tx,
            ticks_since_health: 0,
            // Above two publish intervals, which is the lag a replaced generation has before a
            // later retire releases it.
            last_publish: Instant::now(),
            // Room for the push that precedes the pop, so a steady publish stream never grows it.
            reclaim_history: std::collections::VecDeque::with_capacity(
                STALL_WINDOW_PUBLISHES as usize + 2,
            ),
            starved: false,
            publish_counter: None,
            version: 0,
            current_version: 0,
            last_health: HealthStatus::Healthy,
            op_tx,
            queued_state: state,
        }
    }

    /// Create a new driver with an initial read buffer value.
    pub fn new(initial: T) -> Self {
        Self::build(AtomCoerce::new(initial))
    }

    /// Attach a counter that will be incremented on each publish.
    pub fn set_publish_counter(&mut self, counter: Arc<AtomicU64>) {
        self.publish_counter = Some(counter);
    }

    /// Drains the queue and returns the pending operations in arrival order.
    pub fn drain_ops(&mut self) -> DrainedOps<T, D> {
        self.op_rx.drain().collect()
    }

    /// Reads the current value through an owned guard.
    pub fn read(&self) -> RcuGuard<T, D> {
        self.cell.read_owned()
    }

    /// Publishes the current version to readers.
    ///
    /// Call this in the same pass as [`begin_pass`](QueuedState::begin_pass), so a reader that
    /// read during the previous pass still receives the mark.
    pub fn publish(&mut self) {
        let _ = self.publish_counted();
    }

    /// Publishes the current version to readers and returns the visit counters.
    ///
    /// The counters expose the number of marks a publish issued, which timing alone cannot show.
    pub fn publish_counted(&mut self) -> VisitStats {
        self.version += 1;
        let visit = self.publish_current();
        crate::atom_coerce::submit_if_pending();
        visit
    }

    /// Publishes the version and the snapshot the caller already stored.
    fn publish_current(&mut self) -> VisitStats {
        self.publish_current_inner(true)
    }

    /// Publishes the epilogue, optionally recording reclaim progress.
    fn publish_current_inner(&mut self, record_reclaim: bool) -> VisitStats {
        self.queued_state.atomics.store_version(self.version);
        // Every state instance holds one version receiver that never waits, so only a count above
        // that means an async forwarder or a manual waiter is listening.
        if self.notify_tx.receiver_count() > self.queued_state.atomics.owned_receivers() {
            let _ = self.notify_tx.send(self.version);
        }
        if let Some(ref counter) = self.publish_counter {
            counter.fetch_add(1, Ordering::Relaxed);
        }
        // A signal with no subscriber block has no slot to mark, so it skips the visit entirely.
        let visit = if self.queued_state.slots.has_blocks() {
            self.queued_state
                .slots
                .visit(self.current_version, |context| context.mark_dirty())
        } else {
            VisitStats::default()
        };
        if record_reclaim {
            self.record_reclaim_progress();
        }
        visit
    }

    /// Records reclamation progress for the publish that just happened.
    ///
    /// Kovan releases a replaced generation from a later retire, so a publish that frees nothing is
    /// normal for a couple of publishes. A whole window of publishes with nothing freed is what
    /// reports a stall, which needs no clock.
    fn record_reclaim_progress(&mut self) {
        let retained = self.cell.reclaim_health().retained();
        self.reclaim_history.push_back(retained);
        if self.reclaim_history.len() > STALL_WINDOW_PUBLISHES as usize + 1 {
            self.reclaim_history.pop_front();
        }
        let window_full = self.reclaim_history.len() > STALL_WINDOW_PUBLISHES as usize;
        // Progress means the backlog shrank since the oldest publish in the window.
        self.starved = window_full
            && self
                .reclaim_history
                .front()
                .is_some_and(|oldest| retained >= *oldest);
    }

    /// Stores a value into the read buffer and publishes a new version.
    pub fn publish_value(&mut self, value: T) {
        let _ = self.publish_value_counted(value);
    }

    /// Stores a value, publishes a new version, and returns the visit counters.
    pub fn publish_value_counted(&mut self, value: T) -> VisitStats {
        self.version += 1;
        self.current_version = self.version;
        self.cell.store_versioned(value, self.version);
        let visit = self.publish_current();
        crate::atom_coerce::submit_if_pending();
        visit
    }

    /// Submits whatever this thread has retired, so the next health read is not counting it.
    ///
    /// A retire lands in kovan's accumulating batch, which the health count treats as retained
    /// until the batch is placed. A write path that publishes without submitting therefore reports
    /// a backlog of its own unplaced retires.
    pub fn submit_pending(&self) {
        crate::atom_coerce::submit_if_pending();
    }

    /// Enqueues a full-value replacement.
    pub fn set_value(&self, value: T) {
        let _ = self.op_tx.send(Op::Set(SetValueOp::new(value)));
    }

    /// Enqueues an authoritative mutation.
    pub fn mutate_set<F>(&self, f: F)
    where
        F: Fn(&mut T) + Send + Sync + 'static,
    {
        let _ = self
            .op_tx
            .send(Op::SetInPlace(MutationOp::Typed(Arc::new(f))));
    }

    /// Enqueues a relative mutation.
    pub fn mutate<F>(&self, f: F)
    where
        F: Fn(&mut T) + Send + Sync + 'static,
    {
        let _ = self.op_tx.send(Op::Add(MutationOp::Typed(Arc::new(f))));
    }

    /// Drops queued writes and enqueues a full replacement.
    ///
    /// A replacement overrides every write already queued, so the queue is emptied here
    /// rather than walked by the next resolve.
    pub fn write_absolute(&mut self, value: T) {
        self.op_rx.drain();
        let _ = self.op_tx.send(Op::Absolute(SetValueOp::new(value)));
    }

    /// Resolves one signal and leaves reclamation and health to the caller.
    fn tick_inner(&mut self, publish_interval: Duration, record_reclaim: bool) {
        // The queue is the pending state, so a submit that lands after this drain waits for the
        // next frame. A separate flag could clear in that gap and strand the write.
        let mut working: Option<T> = None;
        let mut saw_op = false;
        while let Ok(op) = self.op_rx.try_recv() {
            saw_op = true;
            match op {
                Op::Set(value) | Op::Absolute(value) => {
                    working = Some(value.into_value());
                }
                Op::SetInPlace(op) | Op::Add(op) => {
                    let value = working.get_or_insert_with(|| (*self.cell.read_typed()).clone());
                    op.apply(value);
                }
            }
        }
        if !saw_op {
            return;
        }

        // A pass advances only on a frame with work, and only while a subscriber block exists.
        if self.queued_state.slots.has_blocks() {
            self.queued_state.begin_pass();
        }

        self.version += 1;
        self.current_version = self.version;
        let value = match working {
            Some(value) => value,
            None => (*self.cell.read_typed()).clone(),
        };
        self.cell.store_versioned(value, self.version);

        // A zero interval publishes every resolve, so it needs no clock read at all.
        let always_publish = publish_interval.is_zero();
        if always_publish || self.last_publish.elapsed() >= publish_interval {
            self.publish_current_inner(record_reclaim);
            if !always_publish {
                self.last_publish = Instant::now();
            }
        }
    }

    /// Drains one signal, publishes if due, flushes reclamation, and updates health.
    ///
    /// Call this once per frame for a single signal. A batch of signals should use
    /// [`tick_batch`](Self::tick_batch), which shares one flush and one health step.
    pub fn tick(&mut self, publish_interval: Duration) {
        self.tick_inner(publish_interval, true);
        crate::atom_coerce::submit_if_pending();
        self.refresh_health_on_cadence();
    }

    /// Resolves a batch of signals with one flush and one health step.
    ///
    /// The batch advances the health cadence once, so a frame pays one flush and one scan
    /// regardless of how many signals it resolves.
    pub fn tick_batch(drivers: &mut [Self], publish_interval: Duration) {
        for driver in drivers.iter_mut() {
            driver.tick_inner(publish_interval, false);
        }
        crate::atom_coerce::submit_if_pending();
        let Some(first) = drivers.first_mut() else {
            return;
        };
        // Reclamation is global, so the batch samples one window and shares the verdict.
        first.record_reclaim_progress();
        let starved = first.starved;
        first.ticks_since_health += 1;
        let health_due = first.ticks_since_health >= HEALTH_INTERVAL_TICKS;
        if health_due {
            first.ticks_since_health = 0;
        }
        for driver in drivers.iter_mut() {
            driver.starved = starved;
        }
        if health_due {
            for driver in drivers.iter_mut() {
                driver.update_health();
            }
        }
    }

    /// Refreshes health when the tick cadence says it is due.
    fn refresh_health_on_cadence(&mut self) {
        self.ticks_since_health += 1;
        if self.ticks_since_health >= HEALTH_INTERVAL_TICKS {
            self.ticks_since_health = 0;
            self.update_health();
        }
    }

    /// Recomputes and publishes the current health status.
    ///
    /// `Stalled` means a whole window of publishes released nothing and the backlog also outgrew
    /// that window, which is a reader starving the reclaimer. `Degraded` means generations are
    /// retained while progress is still happening, and `Healthy` means nothing is retained.
    pub fn update_health(&mut self) {
        // Submitting belongs to the write path and the destructors belong to the receiver, so a
        // scan only reads the counters and publishes the status.
        let retained = self.cell.reclaim_health().retained();
        let pinned = usize::try_from(retained).unwrap_or(usize::MAX);
        let status = if retained == 0 {
            HealthStatus::Healthy
        } else if self.starved && retained > STALL_WINDOW_PUBLISHES {
            // A backlog on its own is not a stall, because a burst of publishes holds a few
            // generations until the next era advance. Stalled needs a window that released
            // nothing and a backlog that outgrew that window.
            HealthStatus::Stalled {
                pinned_buffers: pinned,
            }
        } else {
            HealthStatus::Degraded {
                pinned_buffers: pinned,
            }
        };
        self.queued_state.atomics.store_pinned(pinned);
        self.queued_state.atomics.store_health(status);
        // The count is readable from the atomics, so notify only when the severity changes.
        // A count only change would repaint every reader for no visible reason.
        if core::mem::discriminant(&status) != core::mem::discriminant(&self.last_health) {
            self.last_health = status;
            let _ = self.health_tx.send(status);
        }
    }
}

/// A signal providing borrow reads and queued writes.
pub struct QueuedSignal<
    T: Clone + Send + Sync + 'static + EraseMut<D>,
    D: ?Sized + 'static = dyn Any,
> {
    /// The read-side state that consumers subscribe to.
    pub state: QueuedState<T, D>,
    // keep the writer alive as long as this signal exists
    _driver: Option<Arc<Mutex<WriterDriver<T, D>>>>,
    op_tx: Sender<Op<T, D>>,
}

impl<T: Clone + Send + Sync + 'static + EraseMut<D>, D: ?Sized + 'static> Clone
    for QueuedSignal<T, D>
{
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            _driver: self._driver.clone(),
            op_tx: self.op_tx.clone(),
        }
    }
}

impl<T: Clone + Send + Sync + 'static + EraseMut<D>, D: ?Sized + 'static> Deref
    for QueuedSignal<T, D>
{
    type Target = QueuedState<T, D>;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl<T: Clone + Send + Sync + Debug + 'static + EraseMut<D>, D: ?Sized + 'static> Debug
    for QueuedSignal<T, D>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueuedSignal")
            .field("state", &self.state)
            .field("op_tx", &self.op_tx)
            .finish()
    }
}

impl<T: Clone + Send + Sync + 'static + EraseMut<D>, D: ?Sized + 'static> QueuedSignal<T, D> {
    /// Assemble a QueuedSignal from its constituent parts.
    pub fn from_parts(
        state: QueuedState<T, D>,
        driver: Option<Arc<Mutex<WriterDriver<T, D>>>>,
        op_tx: Sender<Op<T, D>>,
    ) -> Self {
        Self {
            state,
            _driver: driver,
            op_tx,
        }
    }

    /// Acquire a tracked read guard for the current value.
    pub fn read(&self) -> TrackedReadGuard<T> {
        self.state.read()
    }

    /// Projects the current value without registering a reader.
    pub fn read_cached<U, F>(&self, f: F) -> ProjectedReadGuard<U>
    where
        U: Send + Sync + 'static,
        F: FnOnce(&T) -> &U,
    {
        self.state.read_cached(f)
    }

    /// Clone of the write sender.
    pub(crate) fn op_tx(&self) -> Sender<Op<T, D>> {
        self.op_tx.clone()
    }

    /// Enqueue a relative mutation.
    pub fn mutate<F>(&self, f: F)
    where
        F: Fn(&mut T) + Send + Sync + 'static,
    {
        let _ = self.op_tx.send(Op::Add(MutationOp::Typed(Arc::new(f))));
    }

    /// Enqueue an authoritative mutation.
    pub fn mutate_set<F>(&self, f: F)
    where
        F: Fn(&mut T) + Send + Sync + 'static,
    {
        let _ = self
            .op_tx
            .send(Op::SetInPlace(MutationOp::Typed(Arc::new(f))));
    }

    /// Enqueue an authoritative full-value replacement.
    pub fn set_value(&self, value: T) {
        let _ = self.op_tx.send(Op::Set(SetValueOp::new(value)));
    }

    /// Current health status.
    pub fn health(&self) -> HealthStatus {
        self.state.health()
    }

    /// Subscribe dioxus signals to this queued signal's value and health.
    pub fn use_hook<E: 'static>(
        &self,
        error_state: E,
    ) -> (Signal<Result<RcuGuardTyped<T>, E>>, Signal<HealthStatus>) {
        use_queued_state(self.state.clone(), error_state)
    }

    /// Like [`use_hook`], but passes the read guard directly.
    pub fn use_hook_direct(&self, initial: T) -> (Signal<RcuGuardTyped<T>>, Signal<HealthStatus>) {
        use_queued_state_direct(self.state.clone(), initial)
    }
}

/// Shared helper that subscribes a [`Signal`] to a [`QueuedState`].
fn use_queued_state_inner<T: Clone + Send + Sync + 'static, D: ?Sized + 'static, V: 'static>(
    state: QueuedState<T, D>,
    initial: V,
    map: impl Fn(RcuGuardTyped<T>) -> V + 'static,
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
                        value_signal.set(map(cell.read_typed()));
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
pub fn use_queued_state<T: Clone + Send + Sync + 'static, D: ?Sized + 'static, E: 'static>(
    state: QueuedState<T, D>,
    error_state: E,
) -> (Signal<Result<RcuGuardTyped<T>, E>>, Signal<HealthStatus>) {
    use_queued_state_inner(state, Err(error_state), |guard| Ok(guard))
}

/// Subscribe a [`Signal`] to a [`QueuedState`], passing the guard directly.
pub fn use_queued_state_direct<T: Clone + Send + Sync + 'static, D: ?Sized + 'static>(
    state: QueuedState<T, D>,
    _initial: T,
) -> (Signal<RcuGuardTyped<T>>, Signal<HealthStatus>) {
    let guard = state.cell.read_typed();
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
    fn writer_driver_tick_ordering() {
        let mut driver = WriterDriver::new(0i32);

        driver.set_value(42);
        driver.mutate_set(|v: &mut i32| *v += 10);
        driver.mutate(|v: &mut i32| *v += 1);

        driver.tick(Duration::ZERO);

        let val = *driver.queued_state.read();
        assert_eq!(val, 53i32);
    }

    #[test]
    fn writer_driver_tick_set_value_alone() {
        let mut driver = WriterDriver::new(0i32);

        driver.set_value(99);

        driver.tick(Duration::ZERO);

        let val = *driver.queued_state.read();
        assert_eq!(val, 99i32);
    }

    /// A value that counts its clones, so a write path can prove the value moved.
    #[derive(Debug)]
    struct Counted {
        clones: Arc<AtomicUsize>,
        payload: Vec<u8>,
    }

    impl Counted {
        fn new() -> Self {
            Self {
                clones: Arc::new(AtomicUsize::new(0)),
                payload: vec![7; 64],
            }
        }
    }

    impl Clone for Counted {
        fn clone(&self) -> Self {
            self.clones.fetch_add(1, Ordering::Relaxed);
            Self {
                clones: Arc::clone(&self.clones),
                payload: self.payload.clone(),
            }
        }
    }

    #[test]
    fn an_unshared_set_value_moves_without_a_clone() {
        let mut driver = WriterDriver::new(Counted::new());
        let value = Counted::new();
        let clones = Arc::clone(&value.clones);
        driver.set_value(value);
        clones.store(0, Ordering::Relaxed);

        driver.tick(Duration::ZERO);

        assert_eq!(clones.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn writer_driver_tick_applies_in_arrival_order() {
        let mut driver = WriterDriver::new(0i32);

        driver.mutate(|v: &mut i32| *v += 1);
        driver.mutate_set(|v: &mut i32| *v = 10);
        driver.tick(Duration::ZERO);
        assert_eq!(
            *driver.queued_state.read(),
            10i32,
            "a later set replaces earlier adds"
        );

        driver.mutate_set(|v: &mut i32| *v = 10);
        driver.mutate(|v: &mut i32| *v += 1);
        driver.tick(Duration::ZERO);
        assert_eq!(
            *driver.queued_state.read(),
            11i32,
            "an add after a set composes onto it"
        );
    }

    #[test]
    fn writer_driver_tick_absolute_discards_earlier_writes() {
        let mut driver = WriterDriver::new(0i32);

        driver.set_value(1);
        driver.write_absolute(5);
        driver.mutate(|v: &mut i32| *v += 2);
        driver.tick(Duration::ZERO);

        assert_eq!(*driver.queued_state.read(), 7i32);
    }

    #[test]
    fn a_submit_racing_a_resolve_is_never_stranded() {
        let mut driver = WriterDriver::new(0u64);
        let state = driver.queued_state.clone();
        let op_tx = driver.op_tx.clone();
        let writer = std::thread::spawn(move || {
            for value in 1..=2_000u64 {
                let _ = op_tx.send(Op::Set(SetValueOp::new(value)));
            }
        });

        // The last write must arrive without another submit to wake the resolver, which a
        // separate pending flag cannot promise once it clears between a send and the drain.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            driver.tick(Duration::ZERO);
            if *state.read() == 2_000 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the last write was never resolved"
            );
        }
        writer.join().expect("the writer thread");
    }

    #[test]
    fn idle_ticks_leave_the_version_alone() {
        let mut driver = WriterDriver::new(0i32);
        let state = driver.queued_state.clone();
        for _ in 0..16 {
            driver.tick(Duration::ZERO);
        }
        assert_eq!(state.peek_version(), 0, "an idle frame must not publish");
    }

    #[test]
    fn publish_notifies_an_external_version_receiver() {
        let mut driver = WriterDriver::new(0i32);
        let state = driver.queued_state.clone();
        // A receiver taken outside the state counts as a listener, so a publish must wake it.
        let mut rx = state.notify_rx();
        driver.publish_value(1);
        assert!(rx.has_changed().expect("the sender is alive"));
        assert_eq!(*rx.borrow_and_update(), 1);
    }

    #[test]
    fn health_status_transitions() {
        let mut driver = WriterDriver::new(0i32);
        let state = driver.queued_state.clone();
        assert_eq!(state.health(), HealthStatus::Healthy);
        assert_eq!(state.peek_version(), 0);

        let held = driver.queued_state.read();
        driver.publish_value(1);
        driver.update_health();
        assert_eq!(state.peek_version(), 1);
        assert_eq!(state.health(), HealthStatus::Degraded { pinned_buffers: 1 });

        let held_two = driver.queued_state.read();
        driver.publish_value(2);
        driver.update_health();
        assert_eq!(state.peek_version(), 2);
        assert_eq!(
            state.health(),
            HealthStatus::Degraded { pinned_buffers: 2 },
            "young retained generations are degraded, not stalled"
        );

        // A window of publishes with nothing reclaimed is what reports a stall, so every
        // generation is pinned by a guard taken before it is replaced.
        let mut pinned = Vec::new();
        for value in 3..=7 {
            pinned.push(driver.queued_state.read());
            driver.publish_value(value);
            driver.update_health();
        }
        assert!(
            matches!(state.health(), HealthStatus::Stalled { .. }),
            "publishes with no reclaim must report a stall, got {:?}",
            state.health()
        );

        drop(held);
        drop(held_two);
        drop(pinned);
        assert_eq!(settled_health(&mut driver), HealthStatus::Healthy);
    }

    /// Reports health after reclamation settles.
    ///
    /// Drives a reclamation cycle on this thread and gives the receiver time to collect.
    fn settled_health(driver: &mut WriterDriver<i32>) -> HealthStatus {
        for _ in 0..2_000 {
            crate::atom_coerce::flush_reclamation();
            driver.update_health();
            let status = driver.queued_state.health();
            if status == HealthStatus::Healthy {
                return status;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        driver.queued_state.health()
    }

    #[test]
    fn writer_driver_old_snapshot_stays_valid() {
        let mut driver = WriterDriver::new(0i32);

        let old_guard = driver.read();
        assert_eq!(*old_guard, 0);

        driver.set_value(42);
        driver.tick(Duration::ZERO);

        assert_eq!(*old_guard, 0);
        let val = *driver.queued_state.read();
        assert_eq!(val, 42);
    }

    #[test]
    fn queued_state_is_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<QueuedState<i32>>();
        assert_sync::<QueuedState<i32>>();
    }
}
