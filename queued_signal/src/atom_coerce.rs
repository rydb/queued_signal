//! Typed read-copy-update cell with wait-free reads.

use std::any::Any;
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::ptr;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering, fence};

use kovan::{
    Atomic, Guard, Reclaimable, RetiredNode, Shared, flush, flush_pending, pin, retire_reclaimable,
};

use crate::atom_coerce_dyn::Erase;

/// Shared allocation header holding one strong count and the data.
#[repr(C)]
pub struct ArcData<T> {
    pub(crate) strong: AtomicUsize,
    pub(crate) data: UnsafeCell<ManuallyDrop<T>>,
}

/// A reference-counted pointer with a single shared count.
pub struct Arc<T> {
    pub(crate) ptr: NonNull<ArcData<T>>,
}

impl<T> Arc<T> {
    /// Raw pointer to the shared allocation header.
    pub fn as_ptr(&self) -> *mut ArcData<T> {
        self.ptr.as_ptr()
    }

    pub(crate) fn data(&self) -> &ArcData<T> {
        // SAFETY: the allocation stays live while this Arc exists.
        unsafe { self.ptr.as_ref() }
    }

    /// The number of strong references to this allocation.
    pub fn strong_count(this: &Self) -> usize {
        this.data().strong.load(Ordering::Acquire)
    }

    /// Allocates a new counted reference to `data`.
    pub fn new(data: T) -> Arc<T> {
        Arc {
            ptr: NonNull::from(Box::leak(Box::new(ArcData {
                strong: AtomicUsize::new(1),
                data: UnsafeCell::new(ManuallyDrop::new(data)),
            }))),
        }
    }
}

impl<T> Deref for Arc<T> {
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: the Arc keeps the data alive and shareable.
        unsafe { &*self.data().data.get() }
    }
}

impl<T> Clone for Arc<T> {
    fn clone(&self) -> Self {
        if self.data().strong.fetch_add(1, Ordering::Relaxed) > usize::MAX / 2 {
            std::process::abort();
        }
        Arc { ptr: self.ptr }
    }
}

impl<T> Drop for Arc<T> {
    fn drop(&mut self) {
        if self.data().strong.fetch_sub(1, Ordering::Release) == 1 {
            fence(Ordering::Acquire);
            // SAFETY: the strong count hit zero, so nothing else reads the data.
            unsafe {
                ManuallyDrop::drop(&mut *self.data().data.get());
            }
            // SAFETY: this is the exact allocation produced by Arc::new.
            unsafe {
                drop(Box::from_raw(self.ptr.as_ptr()));
            }
        }
    }
}

// SAFETY: the count synchronizes ownership like the standard library Arc.
unsafe impl<T: Send + Sync> Send for Arc<T> {}
unsafe impl<T: Send + Sync> Sync for Arc<T> {}

/// Outstanding node count for one atom.
///
/// The count lives in one word, shifted left by one, with the low bit set once the atom is dropped.
/// Publishing a node is one add and releasing one is one subtract, and the value a release observes
/// tells it whether it is the last node of a dropped atom and must also release the counters. The
/// counters are owned outright rather than shared through a refcount, which the writer would
/// otherwise touch on every publish.
#[derive(Debug)]
pub struct ReclaimHealth {
    pending: AtomicU64,
    /// Nodes freed by the reclaimer and ready for reuse, so neither side reaches the allocator.
    ///
    /// Only the receiver pushes and only the writing thread pops, so the list needs no tag: a node
    /// can only be pushed after it was popped, and a popped node is not on the list.
    pool: AtomicPtr<()>,
    /// Nodes the writing thread took from the pool and has not used yet.
    ///
    /// The receiver never touches this list, so a publication normally takes a node with one
    /// exchange that no other thread ever contends for.
    cache: AtomicPtr<()>,
}

impl ReclaimHealth {
    /// One node, shifted left by one so the low bit can mark a dropped atom.
    const ONE: u64 = 2;
    /// Low bit, set once the owning atom is dropped.
    const DROPPED: u64 = 1;

    /// Starts the counters for an atom that owns one published node.
    pub fn new() -> Self {
        Self {
            pending: AtomicU64::new(Self::ONE),
            pool: AtomicPtr::new(ptr::null_mut()),
            cache: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Nodes returned by the reclaimer.
    fn pool(&self) -> &AtomicPtr<()> {
        &self.pool
    }

    /// Nodes the writing thread has already taken for itself.
    fn cache(&self) -> &AtomicPtr<()> {
        &self.cache
    }

    /// Published nodes awaiting reclamation, excluding the live node.
    pub fn retained(&self) -> u64 {
        (self.pending.load(Ordering::Acquire) >> 1).saturating_sub(1)
    }

    /// Records one more published node.
    fn publish(&self) {
        self.pending.fetch_add(Self::ONE, Ordering::AcqRel);
    }

    /// Records one released node and reports whether the counters can be freed with it.
    ///
    /// That is the case for the final node of an atom that has already been dropped.
    fn release(&self) -> bool {
        self.pending.fetch_sub(Self::ONE, Ordering::AcqRel) == Self::ONE | Self::DROPPED
    }

    /// Marks the atom dropped and reports whether nothing is left to report into the counters.
    fn drop_atom(&self) -> bool {
        self.pending.fetch_or(Self::DROPPED, Ordering::AcqRel) == 0
    }
}

/// One node published into an atom.
///
/// The reclamation header must stay the first field so the node can be
/// retired through the epoch scheme.
///
/// The counters are reached through a plain pointer rather than a shared count, so publishing a node
/// does not touch an atomic the drainer also touches on every free.
#[repr(C)]
struct AtomNode<T> {
    header: RetiredNode,
    deferred: crate::deferred::DeferredHeader,
    version: u64,
    health: *const ReclaimHealth,
    val: ManuallyDrop<T>,
}

impl<T: Send + Sync + 'static> AtomNode<T> {
    /// Takes a node for `val` at the given publication version, reusing a pooled one when there is.
    fn boxed(val: T, version: u64, counters: &ReclaimHealth) -> *mut Self {
        let pooled = take_pooled(counters);
        if pooled.is_null() {
            return Box::into_raw(Box::new(Self {
                header: RetiredNode::new(),
                deferred: crate::deferred::DeferredHeader::new(),
                version,
                health: counters as *const ReclaimHealth,
                val: ManuallyDrop::new(val),
            }));
        }
        let node = pooled as *mut Self;
        // SAFETY: the node came from this pool, so it is an allocation of this type whose value was
        // dropped before it was pooled.
        unsafe {
            (*node).header = RetiredNode::new();
            (*node).deferred = crate::deferred::DeferredHeader::new();
            (*node).version = version;
            (*node).health = counters;
            (*node).val = ManuallyDrop::new(val);
        }
        node
    }

    /// Points at the value inside a node.
    ///
    /// The caller must keep the node alive.
    unsafe fn val_ptr(node: *const Self) -> *const T {
        unsafe { std::ptr::addr_of!((*node).val) as *const T }
    }
}

/// Pops one node from a list, or null when it is empty.
fn pop_node<T: Send + Sync + 'static>(head: &AtomicPtr<()>) -> *mut AtomNode<T> {
    let mut current = head.load(Ordering::Acquire);
    while !current.is_null() {
        let node = current as *mut AtomNode<T>;
        // SAFETY: a pooled pointer is this node type, and its link holds the next pooled node.
        let next = unsafe { (*node).deferred.link() };
        match head.compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return node,
            Err(seen) => current = seen,
        }
    }
    ptr::null_mut()
}

/// Takes one node for reuse, or null when there is none and the caller must allocate.
///
/// The private cache is tried first, so a publication normally exchanges a line no other thread
/// writes. Only an empty cache reaches the shared pool, and it takes the whole pool in one exchange
/// so the receiver is not contended with for every node in its next burst.
fn take_pooled<T: Send + Sync + 'static>(counters: &ReclaimHealth) -> *mut AtomNode<T> {
    let cached = pop_node::<T>(counters.cache());
    if !cached.is_null() {
        return cached;
    }
    let chain = counters.pool().swap(ptr::null_mut(), Ordering::AcqRel);
    if chain.is_null() {
        return ptr::null_mut();
    }
    // Only the writing thread uses the cache, and it is empty here.
    counters.cache().store(chain, Ordering::Release);
    pop_node::<T>(counters.cache())
}

/// Returns one node to the pool so the next publication can reuse it.
fn give_pooled<T: Send + Sync + 'static>(node: *mut AtomNode<T>, counters: &ReclaimHealth) {
    let pool = counters.pool();
    let mut head = pool.load(Ordering::Relaxed);
    loop {
        // SAFETY: the node was just freed and nothing else reads its link.
        unsafe { (*node).deferred.set_link(head) };
        match pool.compare_exchange_weak(
            head,
            node as *mut (),
            Ordering::Release,
            Ordering::Relaxed,
        ) {
            Ok(_) => return,
            Err(seen) => head = seen,
        }
    }
}

/// Frees every pooled node, using the type only `Atom<T>` and its destructor know.
unsafe fn free_pooled<T: Send + Sync + 'static>(counters: &ReclaimHealth) {
    for list in [counters.cache(), counters.pool()] {
        let mut head = list.swap(ptr::null_mut(), Ordering::AcqRel);
        while !head.is_null() {
            let node = head as *mut AtomNode<T>;
            // SAFETY: every pooled node is an allocation of this type, never published again.
            head = unsafe { (*node).deferred.link() };
            drop(unsafe { Box::from_raw(node) });
        }
    }
}

/// Drops the value on the drainer and returns the node to the atom's pool.
unsafe fn free_node<T: Send + Sync + 'static>(ptr: *mut ()) {
    let node = ptr as *mut AtomNode<T>;
    unsafe {
        ManuallyDrop::drop(&mut (*node).val);
        // SAFETY: the counters outlive every node, see `Atom::drop`.
        let counters_ptr = (*node).health;
        let counters = &*counters_ptr;
        if counters.release() {
            // The atom is gone and this was its last node, so the pool and the counters go with it.
            free_pooled::<T>(counters);
            drop(Box::from_raw(node));
            drop(Box::from_raw(counters_ptr as *mut ReclaimHealth));
        } else {
            give_pooled(node, counters);
        }
    }
}

// SAFETY: RetiredNode is the first field of a repr(C) node, and dealloc hands
// the whole allocation to the drainer without dropping the value.
unsafe impl<T: Send + Sync + 'static> Reclaimable for AtomNode<T> {
    fn retired_node(&self) -> &RetiredNode {
        &self.header
    }

    fn retired_node_mut(&mut self) -> &mut RetiredNode {
        &mut self.header
    }

    unsafe fn dealloc(ptr: *mut Self) {
        // Kovan owns the node until this call and never touches it again, so the embedded header
        // can be linked into the queue.
        let header = unsafe { &mut (*ptr).deferred };
        header.prepare(ptr as *mut (), free_node::<T>);
        crate::deferred::defer(header);
    }
}

/// A borrow of the current atom value.
///
/// The guard pins the epoch, which keeps the node alive until it drops.
pub struct AtomGuard<'a, T: Send + Sync + 'static> {
    _epoch: Guard,
    ptr: *const T,
    version: u64,
    marker: PhantomData<&'a T>,
}

impl<T: Send + Sync + 'static> AtomGuard<'_, T> {
    /// Publication version of the node this guard pinned.
    pub fn version(&self) -> u64 {
        self.version
    }
}

impl<T: Send + Sync + 'static> Deref for AtomGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: the epoch guard keeps the node alive and ptr points into it.
        unsafe { &*self.ptr }
    }
}

/// Atom whose nodes are reclaimed by the epoch scheme.
///
/// Reads take no shared count, so a read is a pin plus one pointer load.
/// Dropping the atom retires the current node instead of freeing it, which
/// keeps guards that outlive the atom valid. Every node is still released
/// once the readers pinned before its retirement have moved on.
pub struct Atom<T: Send + Sync + 'static> {
    root: Atomic<AtomNode<T>>,
    /// Counters owned by this atom, reached by every node through a pointer.
    counters: NonNull<ReclaimHealth>,
}

impl<T: Send + Sync + 'static> Atom<T> {
    /// Publishes a first node holding `val`.
    pub fn new(val: T) -> Self {
        let counters = Box::into_raw(Box::new(ReclaimHealth::new()));
        Self {
            // The first published version is the driver's, so the initial node is version zero.
            // SAFETY: the allocation above is owned by this atom.
            root: Atomic::new(AtomNode::boxed(val, 0, unsafe { &*counters })),
            // SAFETY: the allocation above is owned by this atom.
            counters: unsafe { NonNull::new_unchecked(counters) },
        }
    }

    /// Reclamation counters for this atom.
    pub fn reclaim_health(&self) -> &ReclaimHealth {
        // SAFETY: the counters outlive this atom.
        unsafe { self.counters.as_ref() }
    }

    /// Loads the current value behind an epoch guard.
    pub fn load(&self) -> AtomGuard<'_, T> {
        let (epoch, ptr, version) = self.load_parts();
        AtomGuard {
            _epoch: epoch,
            ptr,
            version,
            marker: PhantomData,
        }
    }

    /// Replaces the value at the caller's version, deferring the destruction of the old node.
    ///
    /// The caller supplies the version, so publishing takes no counter of its own. A caller without
    /// a version of its own passes zero.
    pub fn store(&self, val: T, version: u64) {
        let epoch = pin();
        // SAFETY: the node is a fresh allocation that is published below.
        // SAFETY: the counters outlive this atom.
        let new =
            unsafe { Shared::from_raw(AtomNode::boxed(val, version, self.counters.as_ref())) };
        let old = self.root.swap(new, Ordering::AcqRel, &epoch);
        // SAFETY: the counters outlive this atom.
        unsafe { self.counters.as_ref() }.publish();
        self.retire_node(old);
    }

    /// Applies a read-copy-update transformation at the caller's version.
    pub fn rcu<F>(&self, version: u64, mut f: F)
    where
        F: FnMut(&T) -> T,
    {
        let epoch = pin();
        // SAFETY: the counters outlive this atom.
        let counters = unsafe { self.counters.as_ref() };
        loop {
            let current = self.root.load(Ordering::Acquire, &epoch);
            let node = current.as_raw();
            // SAFETY: the atom always holds a live node.
            let old_value: &T = Deref::deref(unsafe { &(*node).val });
            let next_ptr = AtomNode::boxed(f(old_value), version, counters);
            // SAFETY: the node is a fresh allocation that is published below.
            let next = unsafe { Shared::from_raw(next_ptr) };
            match self.root.compare_exchange(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
                &epoch,
            ) {
                Ok(old) => {
                    // SAFETY: the counters outlive this atom.
                    unsafe { self.counters.as_ref() }.publish();
                    self.retire_node(old);
                    return;
                }
                // The exchange failed, so the node was never published and goes back to the pool.
                Err(_) => unsafe {
                    ManuallyDrop::drop(&mut (*next_ptr).val);
                    give_pooled(next_ptr, counters);
                },
            }
        }
    }

    /// Pins the epoch and returns the guard with the current value pointer.
    pub(crate) fn load_parts(&self) -> (Guard, *const T, u64) {
        let epoch = pin();
        let node = self.root.load(Ordering::Acquire, &epoch);
        let raw = node.as_raw();
        // SAFETY: the atom always holds a published node.
        let ptr = unsafe { AtomNode::val_ptr(raw) };
        // SAFETY: the node stays alive under the pin.
        let version = unsafe { (*raw).version };
        (epoch, ptr, version)
    }

    /// Hands a replaced node to the reclaimer and records when that happened.
    fn retire_node(&self, node: Shared<'_, AtomNode<T>>) {
        if node.is_null() {
            return;
        }
        // SAFETY: the node was allocated here and is retired exactly once.
        unsafe { retire_reclaimable(node.as_raw()) };
        // The receiver collects it even when the calling thread holds a guard.
        crate::deferred::wake();
    }
}

impl<T: Send + Sync + 'static> Drop for Atom<T> {
    fn drop(&mut self) {
        {
            let epoch = pin();
            // SAFETY: null is a valid shared pointer for an empty atom.
            let empty = unsafe { Shared::from_raw(std::ptr::null_mut()) };
            let old = self.root.swap(empty, Ordering::AcqRel, &epoch);
            self.retire_node(old);
        }
        // Prompt reclamation now that this thread holds no guard.
        flush();
        drain_deferred_frees();

        // The counters outlive this atom while a node can still report into them. Whichever side
        // finds the count at zero releases them, and the other side leaves them alone.
        // SAFETY: the counters are only released once, by the side that observes the last node.
        let counters = unsafe { self.counters.as_ref() };
        if counters.drop_atom() {
            // Nothing can reach the pool any more, so it and the counters are released here.
            unsafe { free_pooled::<T>(counters) };
            drop(unsafe { Box::from_raw(self.counters.as_ptr()) });
        }
    }
}

// SAFETY: nodes are shared through the epoch scheme and hold Send + Sync values.
unsafe impl<T: Send + Sync> Send for Atom<T> {}
unsafe impl<T: Send + Sync> Sync for Atom<T> {}

/// The value stored inline in the atom node.
pub struct Value<T: Send + Sync + 'static, D: ?Sized + 'static> {
    pub(crate) value: T,
    pub(crate) marker: PhantomData<fn() -> D>,
}

impl<T: Send + Sync + 'static, D: ?Sized + 'static> Value<T, D> {
    /// Wraps a value with the target view marker.
    pub fn new(value: T) -> Self {
        Self {
            value,
            marker: PhantomData,
        }
    }

    /// Shared access to the stored value.
    pub fn value(&self) -> &T {
        &self.value
    }
}

/// One shared state, the atom whose node holds the value inline.
pub struct CoerceShared<T: Send + Sync + 'static, D: ?Sized + 'static> {
    pub(crate) atom: Atom<Value<T, D>>,
}

impl<T: Send + Sync + 'static, D: ?Sized + 'static> CoerceShared<T, D> {
    /// Wraps an atom holding the current value node.
    pub fn new(atom: Atom<Value<T, D>>) -> Self {
        Self { atom }
    }

    /// The atom holding the current value node.
    pub fn atom(&self) -> &Atom<Value<T, D>> {
        &self.atom
    }
}

/// A typed guard that dereferences to the value.
pub struct ValueGuard<'a, T: Send + Sync + 'static, D: ?Sized + 'static> {
    inner: AtomGuard<'a, Value<T, D>>,
}

impl<T: Send + Sync + 'static, D: ?Sized + 'static> Deref for ValueGuard<'_, T, D> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.inner.value
    }
}

/// An owned read guard that dereferences to the current value.
///
/// The guard pins the epoch, and the atom retires the node it points at
/// instead of freeing it, so no shared count is taken to keep it alive.
pub struct RcuGuard<T: Send + Sync + 'static, D: ?Sized + 'static> {
    _epoch: Guard,
    ptr: *const Value<T, D>,
}

impl<T: Send + Sync + 'static, D: ?Sized + 'static> RcuGuard<T, D> {
    fn new(shared: &Arc<CoerceShared<T, D>>) -> Self {
        let (epoch, ptr, _version) = shared.atom.load_parts();
        RcuGuard { _epoch: epoch, ptr }
    }
}

impl<T: Send + Sync + 'static, D: ?Sized + 'static> Deref for RcuGuard<T, D> {
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: the epoch guard keeps the node alive and the shared
        // allocation keeps the atom alive.
        unsafe { &(*self.ptr).value }
    }
}

/// Drops the CoerceShared and frees its ArcData allocation.
///
/// # Safety
/// The caller must own the final strong count of the allocation and `header`
/// must point at the data of an `ArcData` produced by `Arc::new`.
pub unsafe fn drop_coerce_shared_alloc<T, D>(header: NonNull<()>)
where
    T: Send + Sync + 'static,
    D: ?Sized + 'static,
{
    let ptr = header.as_ptr() as *mut ArcData<CoerceShared<T, D>>;
    // SAFETY: the caller owns the final strong count, so no aliases remain.
    unsafe {
        ManuallyDrop::drop(&mut *(*ptr).data.get());
    }
    // SAFETY: ptr is the exact allocation produced by Arc::new.
    unsafe {
        drop(Box::from_raw(ptr));
    }
}

/// A typed read guard that dereferences directly to the value.
pub struct RcuGuardTyped<T: Send + Sync + 'static> {
    _epoch: Guard,
    ptr: *const T,
    version: u64,
}

impl<T: Send + Sync + PartialEq + 'static> PartialEq for RcuGuardTyped<T> {
    fn eq(&self, other: &Self) -> bool {
        self.ptr == other.ptr
    }
}

impl<T: Send + Sync + 'static> RcuGuardTyped<T> {
    pub(crate) fn new<D: ?Sized + 'static>(shared: &Arc<CoerceShared<T, D>>) -> Self {
        let (epoch, value, version) = shared.atom.load_parts();
        // SAFETY: the pin keeps the node that holds the value alive.
        let ptr = unsafe { std::ptr::addr_of!((*value).value) };
        RcuGuardTyped {
            _epoch: epoch,
            ptr,
            version,
        }
    }

    /// Publication version of the node this guard pinned.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Projects this guard onto a value inside the pinned item.
    ///
    /// The epoch pin moves into the projected guard, so the pointer stays valid for the guard lifetime.
    pub(crate) fn project<U: Send + Sync + 'static>(
        self,
        f: impl FnOnce(&T) -> &U,
    ) -> RcuGuardTyped<U> {
        let RcuGuardTyped {
            _epoch,
            ptr,
            version,
        } = self;
        // SAFETY: the epoch guard keeps the node that holds the value alive, and
        // the closure returns a reference derived from that pinned value.
        let value = unsafe { &*ptr };
        let projected: *const U = f(value);
        RcuGuardTyped {
            _epoch,
            ptr: projected,
            version,
        }
    }
}

impl<T: Send + Sync + 'static> Deref for RcuGuardTyped<T> {
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: the epoch guard keeps the node alive and ptr points into it.
        unsafe { &*self.ptr }
    }
}

/// A shared, mutable value with typed and erased access.
pub struct AtomCoerce<T: Send + Sync + 'static, D: ?Sized + 'static = dyn Any> {
    pub(crate) shared: Arc<CoerceShared<T, D>>,
}

impl<T: Send + Sync + 'static, D: ?Sized + 'static> AtomCoerce<T, D> {
    /// Shared reference to the allocation backing this cell.
    pub fn shared(&self) -> &Arc<CoerceShared<T, D>> {
        &self.shared
    }

    /// Reads the current value through an owned guard.
    pub fn read_owned(&self) -> RcuGuard<T, D> {
        RcuGuard::new(&self.shared)
    }

    /// Reads the current value through a guard that points directly at T.
    pub fn read_typed(&self) -> RcuGuardTyped<T> {
        RcuGuardTyped::new(&self.shared)
    }

    /// Reclamation counters for this cell.
    pub fn reclaim_health(&self) -> &ReclaimHealth {
        self.shared.atom.reclaim_health()
    }

    /// Wraps a value, using the built-in metadata for the target view.
    pub fn new(val: T) -> AtomCoerce<T, D>
    where
        T: Erase<D>,
    {
        AtomCoerce {
            shared: Arc::new(CoerceShared {
                atom: Atom::new(Value {
                    value: val,
                    marker: PhantomData,
                }),
            }),
        }
    }

    /// Views this cell under a different erased trait object.
    ///
    /// The concrete value layout is independent of the erased view, so the
    /// same allocation is shared across views. The caller must prove the new
    /// view is valid for `T` through `T: Erase<D2>`.
    pub fn coerce<D2: ?Sized + 'static>(&self) -> &AtomCoerce<T, D2>
    where
        T: Erase<D2>,
    {
        // SAFETY: Value<T, D> and Value<T, D2> have identical layout because
        // the erased view is a zero-sized phantom and the value is stored
        // identically. The allocation and strong count are shared.
        unsafe { &*(self as *const AtomCoerce<T, D> as *const AtomCoerce<T, D2>) }
    }

    /// The typed atom, sharing the single strong count.
    pub fn typed(&self) -> &Atom<Value<T, D>> {
        &self.shared.atom
    }

    /// Loads the current value through a typed guard.
    pub fn load(&self) -> ValueGuard<'_, T, D> {
        ValueGuard {
            inner: self.shared.atom.load(),
        }
    }

    /// Atomically replaces the value.
    pub fn store(&self, val: T) {
        self.shared.atom.store(
            Value {
                value: val,
                marker: PhantomData,
            },
            0,
        );
    }

    /// Applies a read-copy-update transformation to the value.
    pub fn rcu<F>(&self, mut f: F)
    where
        F: FnMut(&T) -> T,
    {
        self.shared.atom.rcu(0, |value: &Value<T, D>| Value {
            value: f(&value.value),
            marker: PhantomData,
        });
    }

    /// Atomically replaces the value at the caller's version.
    pub fn store_versioned(&self, val: T, version: u64) {
        self.shared.atom.store(
            Value {
                value: val,
                marker: PhantomData,
            },
            version,
        );
    }

    /// Applies a read-copy-update transformation at the caller's version.
    pub fn rcu_versioned<F>(&self, version: u64, mut f: F)
    where
        F: FnMut(&T) -> T,
    {
        self.shared.atom.rcu(version, |value: &Value<T, D>| Value {
            value: f(&value.value),
            marker: PhantomData,
        });
    }

    /// A typed handle that reads and edits the current value.
    pub fn handle(&self) -> AtomCoerceHandle<T, D> {
        AtomCoerceHandle {
            shared: self.shared.clone(),
        }
    }

    /// The number of strong references to the shared allocation.
    pub fn strong_count(this: &Self) -> usize {
        Arc::strong_count(&this.shared)
    }
}

/// Typed handle that reads and edits the current value.
pub struct AtomCoerceHandle<T: Send + Sync + 'static, D: ?Sized + 'static = dyn Any> {
    pub(crate) shared: Arc<CoerceShared<T, D>>,
}

impl<T: Send + Sync + 'static, D: ?Sized + 'static> AtomCoerceHandle<T, D> {
    /// Shared reference to the allocation backing this handle.
    pub fn shared(&self) -> &Arc<CoerceShared<T, D>> {
        &self.shared
    }

    /// Reads the current value through an owned guard.
    pub fn read_owned(&self) -> RcuGuard<T, D> {
        RcuGuard::new(&self.shared)
    }

    /// Reads the current value through a guard that points directly at T.
    pub fn read_typed(&self) -> RcuGuardTyped<T> {
        RcuGuardTyped::new(&self.shared)
    }

    /// Reclamation counters for this cell.
    pub fn reclaim_health(&self) -> &ReclaimHealth {
        self.shared.atom.reclaim_health()
    }

    /// Views this handle under a different erased trait object.
    ///
    /// The concrete value layout is independent of the erased view, so the
    /// same allocation is shared across views. The caller must prove the new
    /// view is valid for `T` through `T: Erase<D2>`.
    pub fn coerce<D2: ?Sized + 'static>(&self) -> &AtomCoerceHandle<T, D2>
    where
        T: Erase<D2>,
    {
        // SAFETY: AtomCoerceHandle<T, D> and AtomCoerceHandle<T, D2> have
        // identical layout because the erased view is a zero-sized phantom.
        unsafe { &*(self as *const AtomCoerceHandle<T, D> as *const AtomCoerceHandle<T, D2>) }
    }

    /// The typed atom, sharing the single strong count.
    pub fn typed(&self) -> &Atom<Value<T, D>> {
        &self.shared.atom
    }

    /// Loads the current value through a typed guard.
    pub fn load(&self) -> ValueGuard<'_, T, D> {
        ValueGuard {
            inner: self.shared.atom.load(),
        }
    }

    /// Atomically replaces the value.
    pub fn store(&self, val: T) {
        self.shared.atom.store(
            Value {
                value: val,
                marker: PhantomData,
            },
            0,
        );
    }

    /// Applies a read-copy-update transformation to the value.
    pub fn rcu<F>(&self, mut f: F)
    where
        F: FnMut(&T) -> T,
    {
        self.shared.atom.rcu(0, |value: &Value<T, D>| Value {
            value: f(&value.value),
            marker: PhantomData,
        });
    }
}

impl<T: Send + Sync + 'static, D: ?Sized + 'static> Clone for AtomCoerceHandle<T, D> {
    fn clone(&self) -> Self {
        AtomCoerceHandle {
            shared: self.shared.clone(),
        }
    }
}

/// Runs every deferred destructor that reclamation has queued.
pub fn drain_deferred_frees() -> usize {
    crate::deferred::drain()
}

/// Submits this thread's retired batch when it holds one.
///
/// A batch only leaves the thread that retired it, so the write path submits before it returns
/// rather than on a timer. Kovan already tracks whether the batch is empty, so no flag of ours is
/// needed on the retire path.
pub(crate) fn submit_if_pending() {
    flush_pending();
}

/// Runs one reclamation step on the calling thread.
///
/// The era is global while retired batches are per thread, so one call drains every signal this
/// thread retired and lets generations retired elsewhere become eligible. It costs tens of
/// nanoseconds, so an embedder runs it once per frame on each thread that reads.
// TODO: ask the reclamation backend to keep every retirement in the retiring thread's slot, then
// remove this and the per frame call from the embedder.
pub fn flush_thread_reclamation() {
    let guard = pin();
    drop(guard);
    flush();
}

/// Runs a full reclamation cycle on the calling thread and wakes the receiver.
pub fn flush_reclamation() {
    for _ in 0..2 {
        let guard = pin();
        drop(guard);
        flush();
    }
    crate::deferred::wake();
    drain_deferred_frees();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_load_store_rcu() {
        let cell = AtomCoerce::<u64, dyn Any>::new(1u64);
        assert_eq!(*cell.load(), 1);

        cell.store(2);
        assert_eq!(*cell.load(), 2);

        cell.rcu(|v| v + 1);
        assert_eq!(*cell.load(), 3);
    }

    #[test]
    fn typed_handle_shares_state() {
        let cell = AtomCoerce::<u64, dyn Any>::new(1u64);
        let handle = cell.handle();
        assert_eq!(AtomCoerce::strong_count(&cell), 2);

        handle.store(5);
        assert_eq!(*cell.load(), 5);

        handle.rcu(|v| v * 2);
        assert_eq!(*cell.load(), 10);

        drop(handle);
        assert_eq!(AtomCoerce::strong_count(&cell), 1);
    }
}
