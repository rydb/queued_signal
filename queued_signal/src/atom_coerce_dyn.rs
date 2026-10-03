//! Type-erased access and rebindable handles for the typed read-copy-update cell.
//!
//! Run the miri checks with:
//! RUSTFLAGS="-C target-feature=+cmpxchg16b" cargo miri test -p queued_signal

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::ptr::NonNull;
use std::sync::atomic::fence;
use std::sync::atomic::{AtomicUsize, Ordering};

use bevy_ptr::Ptr;
use bevy_reflect::{Reflect, ReflectFromPtr};
use kovan::{pin, Atom, Guard};

use crate::atom_coerce::{Arc, ArcData, AtomCoerce, AtomCoerceHandle, CoerceShared, Value};

/// Unsizes a sized pointer to a trait object view.
///
/// A blanket impl covers `dyn Any`. Implement this for other trait objects to
/// make `AtomCoerce::new` work without passing the metadata.
pub trait Erase<D: ?Sized>: Sized {
    /// Unsizes a pointer to this type into the erased view.
    ///
    /// The pointer must be the address of a live `Self` value.
    fn erase(ptr: *const ()) -> *const D;
}

impl<T: Send + Sync + 'static> Erase<dyn Any> for T {
    fn erase(ptr: *const ()) -> *const dyn Any {
        let this: *const T = ptr.cast();
        this
    }
}

impl<T: Reflect + Send + Sync + 'static> Erase<dyn Reflect> for T {
    fn erase(ptr: *const ()) -> *const dyn Reflect {
        let this: *const T = ptr.cast();
        this
    }
}

/// Erased access to the shared state for one trait object type.
trait CoerceSharedDyn<D: ?Sized>: Send + Sync {
    /// Loads the current value as an erased view.
    fn load(&self) -> AtomCoerceDynGuard<'_, D>;

    /// Stores a type-erased owned value when it matches the typed T.
    fn try_store_boxed(
        &self,
        value: Box<dyn Any + Send + Sync>,
    ) -> Result<(), Box<dyn Any + Send + Sync>>;
}

impl<T: Send + Sync + 'static + Erase<D>, D: ?Sized + 'static> CoerceSharedDyn<D>
    for CoerceShared<T, D>
{
    fn load(&self) -> AtomCoerceDynGuard<'_, D> {
        let guard = pin();
        let atom_guard = self.atom.load();
        let value: &Value<T, D> = &*atom_guard;
        let ptr = <T as Erase<D>>::erase((&value.value as *const T) as *const ());
        AtomCoerceDynGuard {
            _guard: guard,
            ptr,
            marker: PhantomData,
        }
    }

    fn try_store_boxed(
        &self,
        value: Box<dyn Any + Send + Sync>,
    ) -> Result<(), Box<dyn Any + Send + Sync>> {
        match value.downcast::<T>() {
            Ok(boxed) => {
                self.atom.store(Value {
                    value: *boxed,
                    marker: PhantomData,
                });
                Ok(())
            }
            Err(boxed) => Err(boxed),
        }
    }
}

/// A guard that dereferences to an erased view.
pub struct AtomCoerceDynGuard<'a, D: ?Sized> {
    _guard: Guard,
    ptr: *const D,
    marker: PhantomData<&'a D>,
}

impl<D: ?Sized> AtomCoerceDynGuard<'_, D> {
    /// The erased view of the current value.
    pub fn as_dyn(&self) -> &D {
        // SAFETY: the guard keeps the node alive and ptr points into it.
        unsafe { &*self.ptr }
    }
}

impl AtomCoerceDynGuard<'_, dyn Any> {
    /// The erased Any view of the current value.
    pub fn as_any(&self) -> &dyn Any {
        self.as_dyn()
    }

    /// Downcasts the value to a concrete type.
    pub fn downcast_ref<U: Any>(&self) -> Option<&U> {
        self.as_dyn().downcast_ref::<U>()
    }
}

/// A guard from a rebindable slot that dereferences to an erased view and
/// keeps the bound shared allocation alive.
pub struct AtomCoerceDynSlotGuard<'a, D: ?Sized + 'static> {
    _guard: Guard,
    ptr: *const D,
    _shared: ErasedShared<D>,
    marker: PhantomData<&'a D>,
}

impl<D: ?Sized + 'static> AtomCoerceDynSlotGuard<'_, D> {
    /// The erased view of the value this guard captured.
    pub fn as_dyn(&self) -> &D {
        // SAFETY: the guard keeps the shared allocation alive and the epoch guard keeps the node alive.
        unsafe { &*self.ptr }
    }
}

impl AtomCoerceDynSlotGuard<'_, dyn Any> {
    /// The erased Any view of the value this guard captured.
    pub fn as_any(&self) -> &dyn Any {
        self.as_dyn()
    }

    /// Downcasts the captured value to a concrete type.
    pub fn downcast_ref<U: Any>(&self) -> Option<&U> {
        self.as_dyn().downcast_ref::<U>()
    }
}

/// A type-erased counted reference to a CoerceShared allocation.
struct ErasedShared<D: ?Sized + 'static> {
    /// Points at the CoerceShared value, past the ArcData header.
    data: NonNull<()>,
    /// Strong count in the ArcData header, which sits at offset zero.
    strong: NonNull<AtomicUsize>,
    /// Drops the CoerceShared and frees the ArcData allocation.
    drop_alloc: unsafe fn(NonNull<()>),
    /// Pins the epoch and loads the erased view pointer.
    load: unsafe fn(&ErasedShared<D>) -> (*const D, Guard),
    /// Stores a type-erased owned value when the view supports it.
    try_store: unsafe fn(
        &ErasedShared<D>,
        Box<dyn Any + Send + Sync>,
    ) -> Result<(), Box<dyn Any + Send + Sync>>,
    /// Runtime reflect conversion, present only for a reflect-bound view.
    reflect_from_ptr: Option<ReflectFromPtr>,
    marker: PhantomData<fn() -> D>,
}

impl<D: ?Sized + 'static> ErasedShared<D> {
    /// Erases a typed shared reference without changing the count.
    fn from_arc<T>(typed: &Arc<CoerceShared<T, D>>) -> ErasedShared<D>
    where
        T: Send + Sync + 'static + Erase<D>,
    {
        let data = (&**typed as *const CoerceShared<T, D>) as *mut ();
        // SAFETY: the ArcData header is repr(C) with strong at offset zero.
        let strong = unsafe { NonNull::new_unchecked(typed.ptr.as_ptr() as *mut AtomicUsize) };
        ErasedShared {
            data: NonNull::new(data).expect("Arc pointer is non-null"),
            strong,
            drop_alloc: drop_coerce_shared_alloc::<T, D>,
            load: load_erased::<T, D>,
            try_store: try_store_erased::<T, D>,
            reflect_from_ptr: None,
            marker: PhantomData,
        }
    }

    /// Erases a typed shared reference, bumping the count.
    fn owned<T>(typed: &Arc<CoerceShared<T, D>>) -> ErasedShared<D>
    where
        T: Send + Sync + 'static + Erase<D>,
    {
        let view = Self::from_arc(typed);
        let bumped = view.clone();
        std::mem::forget(view);
        bumped
    }
}

impl ErasedShared<dyn Reflect> {
    /// Binds a typed cell's value as a runtime reflect view.
    fn reflect_bound<T: Send + Sync + 'static>(
        typed: &Arc<CoerceShared<T, dyn Any>>,
        reflect_from_ptr: ReflectFromPtr,
    ) -> ErasedShared<dyn Reflect> {
        let data = (&**typed as *const CoerceShared<T, dyn Any>) as *mut ();
        // SAFETY: the ArcData header is repr(C) with strong at offset zero.
        let strong = unsafe { NonNull::new_unchecked(typed.ptr.as_ptr() as *mut AtomicUsize) };
        // Bump the shared strong count so this reference owns the allocation.
        unsafe {
            strong.as_ref().fetch_add(1, Ordering::Relaxed);
        }
        ErasedShared {
            data: NonNull::new(data).expect("Arc pointer is non-null"),
            strong,
            drop_alloc: drop_reflect_alloc::<T>,
            load: load_reflect_bound::<T>,
            try_store: try_store_reflect,
            reflect_from_ptr: Some(reflect_from_ptr),
            marker: PhantomData,
        }
    }
}

impl<D: ?Sized + 'static> Clone for ErasedShared<D> {
    fn clone(&self) -> Self {
        // SAFETY: strong points at a live AtomicUsize for the allocation.
        let strong = unsafe { self.strong.as_ref() };
        if strong.fetch_add(1, Ordering::Relaxed) > usize::MAX / 2 {
            std::process::abort();
        }
        ErasedShared {
            data: self.data,
            strong: self.strong,
            drop_alloc: self.drop_alloc,
            load: self.load,
            try_store: self.try_store,
            reflect_from_ptr: self.reflect_from_ptr.clone(),
            marker: PhantomData,
        }
    }
}

impl<D: ?Sized + 'static> Drop for ErasedShared<D> {
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

// SAFETY: the erased CoerceShared<T, D> is Send and Sync because every
// constructor requires T to be Send and Sync.
unsafe impl<D: ?Sized + 'static> Send for ErasedShared<D> {}
unsafe impl<D: ?Sized + 'static> Sync for ErasedShared<D> {}

/// Drops the CoerceShared and frees its ArcData allocation.
unsafe fn drop_coerce_shared_alloc<T, D>(header: NonNull<()>)
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

/// Drops a reflect-bound CoerceShared and frees its ArcData allocation.
unsafe fn drop_reflect_alloc<T: Send + Sync + 'static>(header: NonNull<()>) {
    let ptr = header.as_ptr() as *mut ArcData<CoerceShared<T, dyn Any>>;
    // SAFETY: the caller owns the final strong count, so no aliases remain.
    unsafe {
        ManuallyDrop::drop(&mut *(*ptr).data.get());
    }
    // SAFETY: ptr is the exact allocation produced by Arc::new.
    unsafe {
        drop(Box::from_raw(ptr));
    }
}

/// Loads the erased view through the compile-time Erase coercion.
unsafe fn load_erased<T, D>(shared: &ErasedShared<D>) -> (*const D, Guard)
where
    T: Send + Sync + 'static + Erase<D>,
    D: ?Sized + 'static,
{
    // SAFETY: data points at the live CoerceShared for the erased view.
    let typed = unsafe { &*(shared.data.as_ptr() as *const CoerceShared<T, D>) };
    let epoch = pin();
    let atom_guard = typed.atom.load();
    let value: &Value<T, D> = &*atom_guard;
    let ptr = <T as Erase<D>>::erase((&value.value as *const T) as *const ());
    (ptr, epoch)
}

/// Stores a type-erased owned value when it matches the typed T.
unsafe fn try_store_erased<T, D>(
    shared: &ErasedShared<D>,
    value: Box<dyn Any + Send + Sync>,
) -> Result<(), Box<dyn Any + Send + Sync>>
where
    T: Send + Sync + 'static + Erase<D>,
    D: ?Sized + 'static,
{
    // SAFETY: data points at the live CoerceShared for the erased view.
    let typed = unsafe { &*(shared.data.as_ptr() as *const CoerceShared<T, D>) };
    match value.downcast::<T>() {
        Ok(boxed) => {
            typed.atom.store(Value {
                value: *boxed,
                marker: PhantomData,
            });
            Ok(())
        }
        Err(boxed) => Err(boxed),
    }
}

/// Loads a typed cell's value as a runtime reflect view.
unsafe fn load_reflect_bound<T: Send + Sync + 'static>(
    shared: &ErasedShared<dyn Reflect>,
) -> (*const dyn Reflect, Guard) {
    // SAFETY: data points at the live CoerceShared for the bound typed cell.
    let typed = unsafe { &*(shared.data.as_ptr() as *const CoerceShared<T, dyn Any>) };
    let epoch = pin();
    let atom_guard = typed.atom.load();
    let value: &Value<T, dyn Any> = &*atom_guard;
    let raw = (&value.value as *const T).cast::<u8>() as *mut u8;
    // SAFETY: raw points at the live T value and the pointer mirrors T.
    let ptr = unsafe { Ptr::new(NonNull::new_unchecked(raw)) };
    let reflect = shared
        .reflect_from_ptr
        .as_ref()
        .expect("reflect bind missing ReflectFromPtr");
    // SAFETY: ptr holds the type mirrored by reflect_from_ptr.
    let reflect = unsafe { reflect.as_reflect(ptr) };
    (reflect as *const dyn Reflect, epoch)
}

/// Rejects writes through a reflect-bound view, which shares the typed cell.
unsafe fn try_store_reflect<D: ?Sized + 'static>(
    _shared: &ErasedShared<D>,
    value: Box<dyn Any + Send + Sync>,
) -> Result<(), Box<dyn Any + Send + Sync>> {
    Err(value)
}

impl<T: Send + Sync + 'static + Erase<D>, D: ?Sized + 'static> AtomCoerce<T, D> {
    /// The erased view of the current value.
    pub fn untyped(&self) -> AtomCoerceDynGuard<'_, D> {
        self.shared.load()
    }

    /// Stores a type-erased owned value when it matches T.
    pub fn try_store_boxed(
        &self,
        value: Box<dyn Any + Send + Sync>,
    ) -> Result<(), Box<dyn Any + Send + Sync>> {
        self.shared.try_store_boxed(value)
    }

    fn shared_erased(&self) -> ErasedShared<D> {
        ErasedShared::owned(&self.shared)
    }
}

impl<T: Send + Sync + 'static + Clone, D: ?Sized + 'static> AtomCoerce<T, D> {
    /// Stores a borrowed type-erased value when it is a T.
    pub fn try_store_dyn(&self, value: &dyn Any) -> bool {
        let Some(typed) = value.downcast_ref::<T>() else {
            return false;
        };
        self.store(typed.clone());
        true
    }
}

impl<T: Send + Sync + 'static + Erase<D>, D: ?Sized + 'static> AtomCoerceHandle<T, D> {
    fn shared_erased(&self) -> ErasedShared<D> {
        ErasedShared::owned(&self.shared)
    }
}

/// A rebindable erased handle that reads the currently bound value.
pub struct AtomCoerceDynHandle<D: ?Sized + 'static> {
    slot: Arc<Atom<ErasedShared<D>>>,
}

impl<D: ?Sized + 'static> AtomCoerceDynHandle<D> {
    /// The erased view of the currently bound value.
    pub fn get(&self) -> AtomCoerceDynSlotGuard<'_, D> {
        let slot_guard = self.slot.load();
        let shared: &ErasedShared<D> = &*slot_guard;
        // SAFETY: data points at the live CoerceShared and the load function matches.
        let (ptr, epoch) = unsafe { (shared.load)(shared) };
        // Keep the bound shared allocation alive for the guard's lifetime. The
        // epoch guard defers reclamation of the value node inside it.
        AtomCoerceDynSlotGuard {
            _guard: epoch,
            ptr,
            _shared: shared.clone(),
            marker: PhantomData,
        }
    }

    /// Stores a type-erased owned value through the currently bound source.
    pub fn try_store_boxed(
        &self,
        value: Box<dyn Any + Send + Sync>,
    ) -> Result<(), Box<dyn Any + Send + Sync>> {
        let slot_guard = self.slot.load();
        let shared: &ErasedShared<D> = &*slot_guard;
        // SAFETY: data points at the live CoerceShared and the store function matches.
        unsafe { (shared.try_store)(shared, value) }
    }
}

impl<D: ?Sized + 'static> Clone for AtomCoerceDynHandle<D> {
    fn clone(&self) -> Self {
        AtomCoerceDynHandle {
            slot: self.slot.clone(),
        }
    }
}

/// An erased holder that gives rebindable untyped handles.
pub struct AtomCoerceDyn<D: ?Sized + 'static> {
    slot: Arc<Atom<ErasedShared<D>>>,
}

impl<D: ?Sized + 'static> AtomCoerceDyn<D> {
    /// Creates a holder owning its own value.
    pub fn new<T>(value: T) -> AtomCoerceDyn<D>
    where
        T: Send + Sync + 'static + Erase<D>,
    {
        let shared = Arc::new(CoerceShared {
            atom: Atom::new(Value {
                value,
                marker: PhantomData,
            }),
        });
        let erased = ErasedShared::from_arc(&shared);
        // Transfer the single strong count to the erased view.
        std::mem::forget(shared);
        AtomCoerceDyn {
            slot: Arc::new(Atom::new(erased)),
        }
    }

    /// Binds every handle to the typed instance's untyped view.
    pub fn bind<T>(&self, typed: &AtomCoerce<T, D>)
    where
        T: Send + Sync + 'static + Erase<D>,
    {
        self.slot.store(typed.shared_erased());
    }

    /// Binds every handle to a typed read handle's untyped view.
    pub fn bind_handle<T>(&self, typed: &AtomCoerceHandle<T, D>)
    where
        T: Send + Sync + 'static + Erase<D>,
    {
        self.slot.store(typed.shared_erased());
    }

    /// A rebindable erased handle.
    pub fn handle_dyn(&self) -> AtomCoerceDynHandle<D> {
        AtomCoerceDynHandle {
            slot: self.slot.clone(),
        }
    }

    /// The erased view of the currently bound value.
    pub fn get(&self) -> AtomCoerceDynSlotGuard<'_, D> {
        let slot_guard = self.slot.load();
        let shared: &ErasedShared<D> = &*slot_guard;
        // SAFETY: data points at the live CoerceShared and the load function matches.
        let (ptr, epoch) = unsafe { (shared.load)(shared) };
        // Keep the bound shared allocation alive for the guard's lifetime. The
        // epoch guard defers reclamation of the value node inside it.
        AtomCoerceDynSlotGuard {
            _guard: epoch,
            ptr,
            _shared: shared.clone(),
            marker: PhantomData,
        }
    }

    /// Stores a type-erased owned value through the currently bound source.
    pub fn try_store_boxed(
        &self,
        value: Box<dyn Any + Send + Sync>,
    ) -> Result<(), Box<dyn Any + Send + Sync>> {
        let slot_guard = self.slot.load();
        let shared: &ErasedShared<D> = &*slot_guard;
        // SAFETY: data points at the live CoerceShared and the store function matches.
        unsafe { (shared.try_store)(shared, value) }
    }
}

impl AtomCoerceDyn<dyn Reflect> {
    /// Binds the holder to a typed cell, viewing its value as a reflected value.
    ///
    /// The reflect pointer converts the typed cell's value in place, so the
    /// erased view shares the typed cell's single allocation.
    pub fn bind_reflect<T: Send + Sync + 'static>(
        &self,
        typed: &AtomCoerceHandle<T, dyn Any>,
        reflect_from_ptr: ReflectFromPtr,
    ) {
        self.slot
            .store(ErasedShared::reflect_bound(&typed.shared, reflect_from_ptr));
    }
}

impl<D: ?Sized + 'static> Clone for AtomCoerceDyn<D> {
    fn clone(&self) -> Self {
        AtomCoerceDyn {
            slot: self.slot.clone(),
        }
    }
}


/// A registry mapping type names to typed identifiers.
pub struct TypeRegistry {
    entries: HashMap<String, TypeId>,
}

impl TypeRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        TypeRegistry {
            entries: HashMap::new(),
        }
    }

    /// Registers a type under its short name.
    pub fn register<T>(&mut self)
    where
        T: Send + Sync + 'static,
    {
        self.entries
            .insert(short_type_name::<T>(), TypeId::of::<T>());
    }

    /// The registered type id for a name, if any.
    pub fn type_id_of(&self, name: &str) -> Option<TypeId> {
        self.entries.get(name).copied()
    }
}

/// The final path segment of a type's name.
fn short_type_name<T: ?Sized>() -> String {
    std::any::type_name::<T>()
        .rsplit("::")
        .next()
        .unwrap_or("")
        .to_string()
}

/// A demo value registered by its short type name.
pub struct Counter(u32);

#[test]
fn test_atom_coerce() {
    let coerce = AtomCoerce::<u64, dyn Any>::new(42u64);

    assert_eq!(*coerce.load(), 42);
    assert_eq!(coerce.typed().load().value, 42);
    assert_eq!(*coerce.untyped().downcast_ref::<u64>().unwrap(), 42);

    let handle = coerce.handle();
    assert_eq!(*handle.load(), 42);

    let holder = AtomCoerceDyn::<dyn Any>::new(0u64);
    holder.bind(&coerce);
    assert_eq!(*holder.get().downcast_ref::<u64>().unwrap(), 42);

    coerce.store(100);
    assert_eq!(*coerce.load(), 100);
    assert_eq!(*coerce.untyped().downcast_ref::<u64>().unwrap(), 100);
    assert_eq!(*handle.load(), 100);
    assert_eq!(*holder.get().downcast_ref::<u64>().unwrap(), 100);

    coerce.rcu(|v| v + 1);
    assert_eq!(*coerce.load(), 101);
    assert_eq!(*handle.load(), 101);

    handle.store(200);
    assert_eq!(*coerce.load(), 200);

    drop(handle);
}

#[test]
fn test_erase_reflect_view() {
    let typed = AtomCoerce::<u64, dyn Reflect>::new(42u64);
    assert_eq!(*typed.load(), 42);

    let arc: std::sync::Arc<dyn Reflect> = std::sync::Arc::new(7u64);
    let holder = AtomCoerceDyn::<dyn Reflect>::new(arc);
    let view = holder.get();
    let erased = view
        .as_dyn()
        .downcast_ref::<std::sync::Arc<dyn Reflect>>()
        .unwrap();
    assert_eq!(erased.as_ref().downcast_ref::<u64>().unwrap(), &7u64);

    holder.bind(&typed);
    let rebound = holder.get();
    assert_eq!(rebound.as_dyn().downcast_ref::<u64>().unwrap(), &42u64);
}

#[test]
fn test_handle_bumps_refcount() {
    let coerce = AtomCoerce::<u64, dyn Any>::new(1u64);
    assert_eq!(AtomCoerce::strong_count(&coerce), 1);

    let handle = coerce.handle();
    assert_eq!(AtomCoerce::strong_count(&coerce), 2);

    let holder = AtomCoerceDyn::<dyn Any>::new(0u64);
    holder.bind(&coerce);
    assert_eq!(AtomCoerce::strong_count(&coerce), 3);

    drop(handle);
    assert_eq!(AtomCoerce::strong_count(&coerce), 2);

    drop(holder);
    assert_eq!(AtomCoerce::strong_count(&coerce), 1);
}

#[test]
fn test_handle_outlives_owner() {
    let handle = AtomCoerce::<u64, dyn Any>::new(7u64).handle();
    assert_eq!(*handle.load(), 7);
    handle.store(8);
    assert_eq!(*handle.load(), 8);

    let holder = AtomCoerceDyn::<dyn Any>::new(0u64);
    holder.bind(&AtomCoerce::<u64, dyn Any>::new(9u64));
    assert_eq!(*holder.get().downcast_ref::<u64>().unwrap(), 9);
}

#[test]
fn test_handle_in_other_thread() {
    let coerce = AtomCoerce::<u64, dyn Any>::new(42u64);
    let handle = coerce.handle();
    let holder = AtomCoerceDyn::<dyn Any>::new(0u64);
    holder.bind(&coerce);
    let dyn_handle = holder.handle_dyn();

    let worker = std::thread::spawn(move || {
        assert_eq!(*handle.load(), 42);
        assert_eq!(*dyn_handle.get().downcast_ref::<u64>().unwrap(), 42);
    });

    worker.join().unwrap();
}

#[test]
fn test_try_store_dyn() {
    let coerce = AtomCoerce::<u64, dyn Any>::new(42u64);

    let any: &dyn Any = &100u64;
    assert!(coerce.try_store_dyn(any));
    assert_eq!(*coerce.load(), 100);
    assert_eq!(*coerce.untyped().downcast_ref::<u64>().unwrap(), 100);

    // A wrong concrete type is rejected and leaves the value unchanged.
    let wrong: &dyn Any = &"hello";
    assert!(!coerce.try_store_dyn(wrong));
    assert_eq!(*coerce.load(), 100);
}

#[test]
fn test_try_store_boxed() {
    let coerce = AtomCoerce::<u64, dyn Any>::new(7u64);

    // A matching type stores and the typed view reflects it.
    let boxed: Box<dyn Any + Send + Sync> = Box::new(99u64);
    assert!(coerce.try_store_boxed(boxed).is_ok());
    assert_eq!(*coerce.load(), 99);

    // A wrong type returns the box unchanged.
    let wrong: Box<dyn Any + Send + Sync> = Box::new(String::from("nope"));
    let err = coerce.try_store_boxed(wrong).unwrap_err();
    assert_eq!(*err.downcast_ref::<String>().unwrap(), "nope");
    assert_eq!(*coerce.load(), 99);

    // The erased holder stores too, reflected on the typed side.
    let holder = AtomCoerceDyn::<dyn Any>::new(0u64);
    holder.bind(&coerce);
    let boxed: Box<dyn Any + Send + Sync> = Box::new(123u64);
    assert!(holder.try_store_boxed(boxed).is_ok());
    assert_eq!(*coerce.load(), 123);
    assert_eq!(*holder.get().downcast_ref::<u64>().unwrap(), 123);
}

#[test]
fn test_pointer_elevation() {
    let mut registry = TypeRegistry::new();
    registry.register::<Counter>();
    assert_eq!(registry.type_id_of("Counter"), Some(TypeId::of::<Counter>()));

    let holder = AtomCoerceDyn::<dyn Any>::new(Counter(0));
    assert_eq!(holder.get().downcast_ref::<Counter>().unwrap().0, 0);

    let typed = AtomCoerce::new(Counter(42));
    assert_eq!(AtomCoerce::strong_count(&typed), 1);
    holder.bind(&typed);
    assert_eq!(AtomCoerce::strong_count(&typed), 2);

    assert_eq!(holder.get().downcast_ref::<Counter>().unwrap().0, 42);

    typed.store(Counter(99));
    assert_eq!(holder.get().downcast_ref::<Counter>().unwrap().0, 99);

    let clone = holder.clone();
    assert_eq!(clone.get().downcast_ref::<Counter>().unwrap().0, 99);
}

#[test]
fn test_atom_coerce_dyn_handle() {
    let holder = AtomCoerceDyn::<dyn Any>::new(Counter(0));
    let handle = holder.handle_dyn();
    assert_eq!(handle.get().downcast_ref::<Counter>().unwrap().0, 0);

    let typed = AtomCoerce::new(Counter(7));
    holder.bind(&typed);
    assert_eq!(handle.get().downcast_ref::<Counter>().unwrap().0, 7);
}

#[test]
fn test_atom_coerce_dyn_concurrent_rebind() {
    let holder = AtomCoerceDyn::<dyn Any>::new(Counter(0));

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let reader_stop = stop.clone();
    let reader_holder = holder.clone();
    let reader = std::thread::spawn(move || {
        while !reader_stop.load(Ordering::Relaxed) {
            let view = reader_holder.get();
            std::hint::black_box(view.downcast_ref::<Counter>());
        }
    });

    for i in 1..=100u32 {
        let typed = AtomCoerce::new(Counter(i));
        holder.bind(&typed);
    }
    stop.store(true, Ordering::Relaxed);
    reader.join().unwrap();
}

#[test]
fn test_slot_guard_keeps_value_across_rebind() {
    let holder = AtomCoerceDyn::<dyn Any>::new(Counter(7));
    let view = holder.get();
    let typed = AtomCoerce::new(Counter(99));
    holder.bind(&typed);
    assert_eq!(view.downcast_ref::<Counter>().unwrap().0, 7);
    assert_eq!(holder.get().downcast_ref::<Counter>().unwrap().0, 99);
}
