//! Reflect-driven type-erased mirroring of bevy resources and components.
//!
//! The typed mirror path in this crate stays monomorphized and reflection free.
//! This module provides a separate, feature-gated path that lets an in-app UI
//! discover and edit arbitrary resources and components by name at runtime.

pub mod asset;
pub mod path;
pub mod query;
pub mod resource;

use std::{
    any::TypeId,
    collections::HashSet,
    ops::Deref,
    sync::Arc,
};

use bevy_app::prelude::*;
use bevy_ecs::reflect::{AppTypeRegistry, ReflectComponent, ReflectResource};
use bevy_reflect::{Reflect, ReflectCloneError};

use crate::macros::*;

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

    let matches: Vec<&ReflectTypeInfo> =
        infos.iter().filter(|i| i.short_path == name).collect();

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
        Err(err) => {
            Err(err)
        }
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

    app.add_systems(
        crate::schedules::DioxusSyncUpdate,
        resource::drive_reflect_resource_signals,
    );
}
