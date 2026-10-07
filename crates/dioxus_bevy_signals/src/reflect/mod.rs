//! Reflect-driven type-erased mirroring of bevy resources and components.
//!
//! The typed mirror path in this crate stays monomorphized and reflection free.
//! This module provides a separate, feature-gated path that lets an in-app UI
//! discover and edit arbitrary resources and components by name at runtime.

pub mod asset;
pub mod path;
pub mod query;
pub mod resource;

use std::{any::TypeId, collections::HashSet, ops::Deref, sync::Arc};
use std::any::Any;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc as StdArc;

use bevy_app::prelude::*;
use bevy_ecs::reflect::{AppTypeRegistry, ReflectComponent, ReflectResource};
use bevy_ptr::Ptr;
use bevy_reflect::{Reflect, ReflectCloneError, ReflectFromPtr};
use kovan::{pin, Atom, Guard};
use queued_signal::atom_coerce::{
    Arc as QsArc, AtomCoerceHandle, CoerceShared, Value, drop_coerce_shared_alloc,
};
use queued_signal::atom_coerce_dyn::{AtomCoerceDyn, ErasedShared};

/// Type-erased mutation operating on a reflected value.
pub type ErasedMutation = Arc<dyn Fn(&mut dyn Reflect) + Send + Sync>;

/// Kinds of reflectable bevy state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReflectKind {
    /// A bevy resource.
    Resource,
    /// A bevy component.
    Component,
}

/// Info describing one reflectable type.
#[derive(Clone, Copy, Debug)]
pub struct ReflectTypeInfo {
    /// Runtime type id.
    pub type_id: TypeId,
    /// Full type path.
    pub full_path: &'static str,
    /// Short type name.
    pub short_path: &'static str,
    /// Whether the type is a resource or a component.
    pub kind: ReflectKind,
}

impl PartialEq for ReflectTypeInfo {
    fn eq(&self, other: &Self) -> bool {
        self.type_id == other.type_id
    }
}

impl Eq for ReflectTypeInfo {}

impl std::hash::Hash for ReflectTypeInfo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.type_id.hash(state);
    }
}

/// Error returned when a name cannot be resolved to exactly one type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NameResolutionError {
    /// No registered type matched the name.
    NotFound(String),
    /// Multiple types matched the short name.
    Ambiguous(String, Vec<&'static str>),
}

/// Enumerate all reflectable resources and components in the type registry.
pub fn enumerate_reflect_types(type_registry: &AppTypeRegistry) -> HashSet<ReflectTypeInfo> {
    let registry = type_registry.read();
    let mut out = HashSet::new();

    for registration in registry.iter() {
        let type_id = registration.type_id();
        let full_path = registration.type_info().type_path();
        let short_path = registration.type_info().type_path_table().short_path();

        if registration.data::<ReflectResource>().is_some() {
            out.insert(ReflectTypeInfo {
                type_id,
                full_path,
                short_path,
                kind: ReflectKind::Resource,
            });
        }

        if registration.data::<ReflectComponent>().is_some() {
            out.insert(ReflectTypeInfo {
                type_id,
                full_path,
                short_path,
                kind: ReflectKind::Component,
            });
        }
    }

    out
}

/// Resolve a name with three tiers of matching.
///
/// Full path matches first, then a unique short name, then ambiguity.
pub fn resolve_name(
    infos: &HashSet<ReflectTypeInfo>,
    name: &str,
) -> Result<TypeId, NameResolutionError> {
    if let Some(info) = infos.iter().find(|i| i.full_path == name) {
        return Ok(info.type_id);
    }

    let matches: Vec<&ReflectTypeInfo> = infos.iter().filter(|i| i.short_path == name).collect();

    match matches.len() {
        1 => Ok(matches[0].type_id),
        0 => Err(NameResolutionError::NotFound(name.to_owned())),
        _ => Err(NameResolutionError::Ambiguous(
            name.to_owned(),
            matches.iter().map(|i| i.full_path).collect(),
        )),
    }
}

/// Clone a reflected value into a shared erased pointer.
pub fn clone_into_arc(value: &dyn Reflect) -> Result<Arc<dyn Reflect>, ReflectCloneError> {
    match value.reflect_clone() {
        Ok(boxed) => Ok(Arc::from(boxed)),
        Err(err) => Err(err),
    }
}

/// Owned erased reflect value
pub struct ErasedValue(pub Arc<dyn Reflect>);

impl Clone for ErasedValue {
    fn clone(&self) -> Self {
        ErasedValue(self.0.clone())
    }
}

impl ErasedValue {
    /// Create an owned erased value by deep cloning a reflected value.
    pub fn new(value: &dyn Reflect) -> Result<ErasedValue, ReflectCloneError> {
        clone_into_arc(value).map(ErasedValue)
    }

    /// Shared access to the reflected value.
    pub fn as_reflect(&self) -> &dyn Reflect {
        self.0.as_ref()
    }

    /// The shared erased pointer for read paths.
    pub fn as_arc(&self) -> &Arc<dyn Reflect> {
        &self.0
    }
}

impl Deref for ErasedValue {
    type Target = dyn Reflect;

    fn deref(&self) -> &dyn Reflect {
        self.0.as_ref()
    }
}

/// Registers reflect-driven mirroring registries and systems.
pub fn setup(app: &mut App) {
    app.init_resource::<resource::ReflectResourceRegistry>();
    app.init_resource::<query::ReflectComponentRegistry>();
    app.init_resource::<query::ReflectQueryRegistry>();
    app.init_resource::<query::ReflectActiveTypedQueries>();
    app.init_resource::<query::TypedQuerySpawnerRegistry>();
    app.init_resource::<asset::ReflectAssetRegistry>();
    app.init_resource::<asset::TypedAssetSpawnerRegistry>();

    app.add_systems(
        crate::schedules::DioxusSyncUpdate,
        resource::drive_reflect_resource_signals,
    );
    app.add_systems(
        crate::schedules::DioxusSyncUpdate,
        query::drive_reflect_query_signals,
    );
}

/// Builds an owning erased holder around an Arc-backed reflected value.
pub fn reflect_holder_owned(value: StdArc<dyn Reflect>) -> AtomCoerceDyn<dyn Reflect> {
    AtomCoerceDyn::from_shared(reflect_shared_owned(value))
}

/// Extension for binding a reflected holder to a typed cell.
pub trait AtomCoerceDynReflectExt {
    /// Binds the holder to a typed cell, viewing its value as a reflected value.
    fn bind_reflect<T: Send + Sync + 'static>(
        &self,
        typed: &AtomCoerceHandle<T, dyn Any>,
        reflect_from_ptr: ReflectFromPtr,
    );
}

impl AtomCoerceDynReflectExt for AtomCoerceDyn<dyn Reflect> {
    fn bind_reflect<T: Send + Sync + 'static>(
        &self,
        typed: &AtomCoerceHandle<T, dyn Any>,
        reflect_from_ptr: ReflectFromPtr,
    ) {
        self.bind_shared(reflect_bound_shared(typed, reflect_from_ptr));
    }
}

/// Owned erased shared reference around an Arc-backed reflected value.
fn reflect_shared_owned(value: StdArc<dyn Reflect>) -> ErasedShared<dyn Reflect> {
    let shared = QsArc::new(CoerceShared::new(Atom::new(Value::new(value))));
    let data = (&*shared as *const CoerceShared<StdArc<dyn Reflect>, dyn Reflect>) as *mut ();
    let strong =
        NonNull::new(shared.as_ptr() as *mut AtomicUsize).expect("arc pointer is non-null");
    let erased = ErasedShared::from_raw_parts(
        NonNull::new(data).expect("arc pointer is non-null"),
        strong,
        drop_coerce_shared_alloc::<StdArc<dyn Reflect>, dyn Reflect>,
        load_arc_reflect::<StdArc<dyn Reflect>>,
        try_store_arc_reflect::<StdArc<dyn Reflect>>,
        StdArc::new(()),
    );
    std::mem::forget(shared);
    erased
}

/// Builds an erased shared reference bound to a typed cell.
fn reflect_bound_shared<T: Send + Sync + 'static>(
    typed: &AtomCoerceHandle<T, dyn Any>,
    reflect_from_ptr: ReflectFromPtr,
) -> ErasedShared<dyn Reflect> {
    let shared = typed.shared();
    let data = (&**shared as *const CoerceShared<T, dyn Any>) as *mut ();
    let strong =
        NonNull::new(shared.as_ptr() as *mut AtomicUsize).expect("arc pointer is non-null");
    // Adopt one strong count for this erased reference.
    unsafe {
        strong.as_ref().fetch_add(1, Ordering::Relaxed);
    }
    ErasedShared::from_raw_parts(
        NonNull::new(data).expect("arc pointer is non-null"),
        strong,
        drop_coerce_shared_alloc::<T, dyn Any>,
        load_reflect_bound::<T>,
        try_store_reflect,
        StdArc::new(reflect_from_ptr),
    )
}

/// Loads an Arc-backed reflected value as a reflected view.
unsafe fn load_arc_reflect<T>(shared: &ErasedShared<dyn Reflect>) -> (*const dyn Reflect, Guard)
where
    T: Reflect + Send + Sync + 'static,
{
    // SAFETY: data points at the live CoerceShared for the erased view.
    let typed = unsafe { &*(shared.data().as_ptr() as *const CoerceShared<T, dyn Reflect>) };
    let epoch = pin();
    let atom_guard = typed.atom().load();
    let value: &Value<T, dyn Reflect> = &*atom_guard;
    let ptr = value.value() as *const T;
    (ptr as *const dyn Reflect, epoch)
}

/// Stores an Arc-backed reflected value when the boxed value matches.
unsafe fn try_store_arc_reflect<T>(
    shared: &ErasedShared<dyn Reflect>,
    value: Box<dyn Any + Send + Sync>,
) -> Result<(), Box<dyn Any + Send + Sync>>
where
    T: Send + Sync + 'static,
{
    // SAFETY: data points at the live CoerceShared for the erased view.
    let typed = unsafe { &*(shared.data().as_ptr() as *const CoerceShared<T, dyn Reflect>) };
    match value.downcast::<T>() {
        Ok(boxed) => {
            typed.atom().store(Value::new(*boxed));
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
    let typed = unsafe { &*(shared.data().as_ptr() as *const CoerceShared<T, dyn Any>) };
    let epoch = pin();
    let atom_guard = typed.atom().load();
    let value: &Value<T, dyn Any> = &*atom_guard;
    let raw = (value.value() as *const T).cast::<u8>() as *mut u8;
    // SAFETY: raw points at the live T value and the pointer mirrors T.
    let ptr = unsafe { Ptr::new(NonNull::new_unchecked(raw)) };
    let reflect = shared
        .context()
        .downcast_ref::<ReflectFromPtr>()
        .expect("reflect bind missing ReflectFromPtr");
    // SAFETY: ptr holds the type mirrored by reflect_from_ptr.
    let reflect = unsafe { reflect.as_reflect(ptr) };
    (reflect as *const dyn Reflect, epoch)
}

/// Rejects writes through a reflect-bound view, which shares the typed cell.
unsafe fn try_store_reflect(
    _shared: &ErasedShared<dyn Reflect>,
    value: Box<dyn Any + Send + Sync>,
) -> Result<(), Box<dyn Any + Send + Sync>> {
    Err(value)
}
