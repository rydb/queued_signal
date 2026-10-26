//! Typed read-copy-update cell with wait-free reads.

use std::any::Any;
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::atomic::fence;
use std::sync::atomic::{AtomicUsize, Ordering};

use kovan::{Atomic, Guard, RetiredNode, Shared, flush, pin, retire};

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

/// One node published into an atom.
///
/// The reclamation header must stay the first field so the node can be
/// retired through the epoch scheme.
#[repr(C)]
struct AtomNode<T> {
    header: RetiredNode,
    val: T,
}

impl<T> AtomNode<T> {
    /// Allocates a node holding `val`.
    fn boxed(val: T) -> *mut Self {
        Box::into_raw(Box::new(Self {
            header: RetiredNode::new(),
            val,
        }))
    }

    /// Points at the value inside a node.
    ///
    /// The caller must keep the node alive.
    unsafe fn val_ptr(node: *const Self) -> *const T {
        unsafe { std::ptr::addr_of!((*node).val) }
    }
}

/// A borrow of the current atom value.
///
/// The guard pins the epoch, which keeps the node alive until it drops.
pub struct AtomGuard<'a, T: Send + Sync + 'static> {
    _epoch: Guard,
    ptr: *const T,
    marker: PhantomData<&'a T>,
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
}

impl<T: Send + Sync + 'static> Atom<T> {
    /// Publishes a first node holding `val`.
    pub fn new(val: T) -> Self {
        Self {
            root: Atomic::new(AtomNode::boxed(val)),
        }
    }

    /// Loads the current value behind an epoch guard.
    pub fn load(&self) -> AtomGuard<'_, T> {
        let (epoch, ptr) = self.load_parts();
        AtomGuard {
            _epoch: epoch,
            ptr,
            marker: PhantomData,
        }
    }

    /// Replaces the value, deferring the destruction of the old node.
    pub fn store(&self, val: T) {
        let epoch = pin();
        // SAFETY: the node is a fresh allocation that is published below.
        let new = unsafe { Shared::from_raw(AtomNode::boxed(val)) };
        let old = self.root.swap(new, Ordering::AcqRel, &epoch);
        Self::retire_node(old);
    }

    /// Applies a read-copy-update transformation.
    ///
    /// The closure may run more than once when writers contend.
    pub fn rcu<F>(&self, mut f: F)
    where
        F: FnMut(&T) -> T,
    {
        let epoch = pin();
        loop {
            let current = self.root.load(Ordering::Acquire, &epoch);
            // SAFETY: the atom always holds a live node.
            let next_ptr = AtomNode::boxed(f(unsafe { &(*current.as_raw()).val }));
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
                    Self::retire_node(old);
                    return;
                }
                // SAFETY: the exchange failed, so the node was never published.
                Err(_) => unsafe { drop(Box::from_raw(next_ptr)) },
            }
        }
    }

    /// Pins the epoch and returns the guard with the current value pointer.
    fn load_parts(&self) -> (Guard, *const T) {
        let epoch = pin();
        let node = self.root.load(Ordering::Acquire, &epoch);
        // SAFETY: the atom always holds a published node.
        let ptr = unsafe { AtomNode::val_ptr(node.as_raw()) };
        (epoch, ptr)
    }

    /// Hands a replaced node to the reclaimer.
    fn retire_node(node: Shared<'_, AtomNode<T>>) {
        if node.is_null() {
            return;
        }
        // SAFETY: the node was allocated here and is retired exactly once.
        unsafe { retire(node.as_raw()) };
    }
}

impl<T: Send + Sync + 'static> Drop for Atom<T> {
    fn drop(&mut self) {
        {
            let epoch = pin();
            // SAFETY: null is a valid shared pointer for an empty atom.
            let empty = unsafe { Shared::from_raw(std::ptr::null_mut()) };
            let old = self.root.swap(empty, Ordering::AcqRel, &epoch);
            Self::retire_node(old);
        }
        // Prompt reclamation now that this thread holds no guard.
        flush();
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
        let (epoch, ptr) = shared.atom.load_parts();
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
}

impl<T: Send + Sync + PartialEq + 'static> PartialEq for RcuGuardTyped<T> {
    fn eq(&self, other: &Self) -> bool {
        self.ptr == other.ptr
    }
}

impl<T: Send + Sync + 'static> RcuGuardTyped<T> {
    pub(crate) fn new<D: ?Sized + 'static>(shared: &Arc<CoerceShared<T, D>>) -> Self {
        let (epoch, value) = shared.atom.load_parts();
        // SAFETY: the pin keeps the node that holds the value alive.
        let ptr = unsafe { std::ptr::addr_of!((*value).value) };
        RcuGuardTyped { _epoch: epoch, ptr }
    }

    /// Projects this guard onto a value inside the pinned item.
    ///
    /// The epoch pin moves into the projected guard, so the pointer stays valid for the guard lifetime.
    pub(crate) fn project<U: Send + Sync + 'static>(
        self,
        f: impl FnOnce(&T) -> &U,
    ) -> RcuGuardTyped<U> {
        let RcuGuardTyped { _epoch, ptr } = self;
        // SAFETY: the epoch guard keeps the node that holds the value alive, and
        // the closure returns a reference derived from that pinned value.
        let value = unsafe { &*ptr };
        let projected: *const U = f(value);
        RcuGuardTyped {
            _epoch,
            ptr: projected,
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
        self.shared.atom.store(Value {
            value: val,
            marker: PhantomData,
        });
    }

    /// Applies a read-copy-update transformation to the value.
    pub fn rcu<F>(&self, mut f: F)
    where
        F: FnMut(&T) -> T,
    {
        self.shared.atom.rcu(|value: &Value<T, D>| Value {
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
        self.shared.atom.store(Value {
            value: val,
            marker: PhantomData,
        });
    }

    /// Applies a read-copy-update transformation to the value.
    pub fn rcu<F>(&self, mut f: F)
    where
        F: FnMut(&T) -> T,
    {
        self.shared.atom.rcu(|value: &Value<T, D>| Value {
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
