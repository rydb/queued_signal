//! Typed read-copy-update cell with wait-free reads.

use std::any::Any;
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::atomic::fence;
use std::sync::atomic::{AtomicUsize, Ordering};

use kovan::{pin, Atom, AtomGuard, Guard};

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
/// The guard pins the epoch and holds a clone of the shared allocation, so
/// the node it points at stays valid until the guard drops.
pub struct RcuGuard<T: Send + Sync + 'static, D: ?Sized + 'static> {
    _epoch: Guard,
    ptr: *const Value<T, D>,
    _shared: Arc<CoerceShared<T, D>>,
}

impl<T: Send + Sync + 'static, D: ?Sized + 'static> RcuGuard<T, D> {
    fn new(shared: Arc<CoerceShared<T, D>>) -> Self {
        let epoch = pin();
        let atom_guard = shared.atom.load();
        let value: &Value<T, D> = &*atom_guard;
        let ptr = value as *const Value<T, D>;
        RcuGuard {
            _epoch: epoch,
            ptr,
            _shared: shared,
        }
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

/// Type-erased keep-alive for a typed read guard.
pub(crate) struct TypedShared {
    strong: NonNull<AtomicUsize>,
    drop_alloc: unsafe fn(NonNull<()>),
}

impl TypedShared {
    /// Adopts one strong count from a typed shared reference.
    fn from_arc<T, D>(typed: &Arc<CoerceShared<T, D>>) -> TypedShared
    where
        T: Send + Sync + 'static,
        D: ?Sized + 'static,
    {
        // SAFETY: the ArcData header is repr(C) with strong at offset zero.
        let strong = unsafe { NonNull::new_unchecked(typed.as_ptr() as *mut AtomicUsize) };
        unsafe {
            strong.as_ref().fetch_add(1, Ordering::Relaxed);
        }
        TypedShared {
            strong,
            drop_alloc: drop_coerce_shared_alloc::<T, D>,
        }
    }
}

impl Clone for TypedShared {
    fn clone(&self) -> Self {
        // SAFETY: strong points at a live AtomicUsize for the allocation.
        let strong = unsafe { self.strong.as_ref() };
        if strong.fetch_add(1, Ordering::Relaxed) > usize::MAX / 2 {
            std::process::abort();
        }
        TypedShared {
            strong: self.strong,
            drop_alloc: self.drop_alloc,
        }
    }
}

impl Drop for TypedShared {
    fn drop(&mut self) {
        // SAFETY: strong points at a live AtomicUsize for the allocation.
        let strong = unsafe { self.strong.as_ref() };
        if strong.fetch_sub(1, Ordering::Release) == 1 {
            fence(Ordering::Acquire);
            // SAFETY: this reference owns the final strong count.
            unsafe { (self.drop_alloc)(self.strong.cast::<()>()) };
        }
    }
}

/// A typed read guard that dereferences directly to the value.
pub struct RcuGuardTyped<T: Send + Sync + 'static> {
    _epoch: Guard,
    ptr: *const T,
    _shared: TypedShared,
}

impl<T: Send + Sync + 'static> RcuGuardTyped<T> {
    pub(crate) fn new<D: ?Sized + 'static>(shared: Arc<CoerceShared<T, D>>) -> Self {
        let epoch = pin();
        let atom_guard = shared.atom.load();
        let value: &Value<T, D> = &*atom_guard;
        let ptr = &value.value as *const T;
        let _shared = TypedShared::from_arc(&shared);
        RcuGuardTyped {
            _epoch: epoch,
            ptr,
            _shared,
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
        RcuGuard::new(self.shared.clone())
    }

    /// Reads the current value through a guard that points directly at T.
    pub fn read_typed(&self) -> RcuGuardTyped<T> {
        RcuGuardTyped::new(self.shared.clone())
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
        RcuGuard::new(self.shared.clone())
    }

    /// Reads the current value through a guard that points directly at T.
    pub fn read_typed(&self) -> RcuGuardTyped<T> {
        RcuGuardTyped::new(self.shared.clone())
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
