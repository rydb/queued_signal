//! Mirror signal for one mirrored component.

use std::fmt::{self, Display};

use dioxus_core::{IntoAttributeValue, IntoDynNode};
use dioxus_hooks::{use_memo, use_signal};
use dioxus_signals::{Memo, ReadableExt, Signal};
use queued_signal::state::{HealthStatus, ProjectedReadGuard, TrackedReadGuard};
use trait_set::trait_set;

use crate::Projection;
use crate::macros::*;

use super::single::SingleQueryError;
use super::{ComponentMirrorHandle, DioxusComponentSync};

#[cfg(feature = "asset")]
use crate::asset::{AssetMaybeMirrorSignal, AssetNoneState, DioxusAssetSync, use_bevy_asset};
#[cfg(feature = "asset")]
use bevy_asset::Handle;

trait_set! {
    /// Bounds required for projected component values.
    pub trait ComponentBounds = Send + Sync + 'static;
}

/// Read signal for one mirrored component.
pub struct ComponentMirrorSignal<T, U, E>
where
    T: DioxusComponentSync,
    U: ComponentBounds,
    E: 'static,
{
    /// Resolution of the mirrored component handle.
    handle: Signal<Result<ComponentMirrorHandle<T>, SingleQueryError>>,
    /// Projection applied on each read.
    map_fn: Projection<T, U>,
    /// Error state for display.
    display: Memo<Result<(), E>>,
}

impl<T, U, E> Clone for ComponentMirrorSignal<T, U, E>
where
    T: DioxusComponentSync,
    U: ComponentBounds,
    E: 'static,
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<T, U, E> Copy for ComponentMirrorSignal<T, U, E>
where
    T: DioxusComponentSync,
    U: ComponentBounds,
    E: 'static,
{
}

impl<T, U, E> PartialEq for ComponentMirrorSignal<T, U, E>
where
    T: DioxusComponentSync,
    U: ComponentBounds,
    E: 'static,
{
    /// Mirrors compare by handle identity and projection.
    fn eq(&self, other: &Self) -> bool {
        self.handle == other.handle && std::ptr::fn_addr_eq(self.map_fn, other.map_fn)
    }
}

impl<T, U, E> Display for ComponentMirrorSignal<T, U, E>
where
    T: DioxusComponentSync,
    U: ComponentBounds + Display,
    E: Display + PartialEq + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Reading the memo subscribes this scope to resolution changes.
        let display = self.display.read();
        if let Err(err) = display.as_ref() {
            return write!(f, "{err}");
        }
        let handle = self.handle.read();
        let Ok(handle) = &*handle else {
            return Ok(());
        };
        // A tracked read joins the slot array, so a publish marks this scope directly while the
        // guard stays transient and drops before the render returns.
        let value = handle.value.read().project(self.map_fn);
        write!(f, "{}", *value)
    }
}

impl<T, U, E> IntoDynNode for ComponentMirrorSignal<T, U, E>
where
    T: DioxusComponentSync,
    U: ComponentBounds + Display,
    E: Display + PartialEq + 'static,
{
    fn into_dyn_node(self) -> dioxus_core::DynamicNode {
        self.to_string().into_dyn_node()
    }
}

impl<T, U, E> IntoAttributeValue for ComponentMirrorSignal<T, U, E>
where
    T: DioxusComponentSync,
    U: ComponentBounds + Display,
    E: Display + PartialEq + 'static,
{
    fn into_value(self) -> dioxus_core::AttributeValue {
        self.to_string().into_value()
    }
}

impl<T, U, E> ComponentMirrorSignal<T, U, E>
where
    T: DioxusComponentSync,
    U: ComponentBounds,
    E: 'static,
{
    /// Builds a mirror over a fallible handle signal.
    pub(crate) fn from_resolved(
        handle: Signal<Result<ComponentMirrorHandle<T>, SingleQueryError>>,
        map_fn: Projection<T, U>,
        err_fn: impl Fn(SingleQueryError) -> E + 'static,
    ) -> Self
    where
        E: PartialEq,
    {
        let display = use_memo(move || {
            let resolved = handle.read();
            match &*resolved {
                Ok(_) => Ok(()),
                Err(err) => Err(err_fn(err.clone())),
            }
        });
        Self {
            handle,
            map_fn,
            display,
        }
    }

    /// Enqueues a relative mutation.
    pub fn mutate<F>(&self, f: F)
    where
        F: Fn(&mut T) + Send + Sync + 'static,
    {
        match &*self.handle.peek() {
            Ok(handle) => handle.value.mutate(f),
            Err(_err) => {
                warn!("ComponentMirrorSignal::mutate dropped: component not resolved")
            }
        }
    }

    /// Enqueues an authoritative mutation.
    pub fn mutate_set<F>(&self, f: F)
    where
        F: Fn(&mut T) + Send + Sync + 'static,
    {
        match &*self.handle.peek() {
            Ok(handle) => handle.value.mutate_set(f),
            Err(_err) => {
                warn!("ComponentMirrorSignal::mutate_set dropped: component not resolved")
            }
        }
    }

    /// Enqueues a full value replacement.
    pub fn set_value(&self, value: T) {
        match &*self.handle.peek() {
            Ok(handle) => handle.value.set_value(value),
            Err(_err) => {
                warn!("ComponentMirrorSignal::set_value dropped: component not resolved")
            }
        }
    }

    /// Current health of the underlying signal.
    pub fn health(&self) -> HealthStatus {
        match &*self.handle.peek() {
            Ok(handle) => handle.value.health(),
            Err(_err) => HealthStatus::Healthy,
        }
    }

    /// Reads the component and subscribes to resolution changes and publishes.
    pub fn read(&self) -> Result<TrackedReadGuard<T>, SingleQueryError> {
        let handle = self.handle.read();
        match &*handle {
            Ok(handle) => Ok(handle.value.read()),
            Err(err) => Err(err.clone()),
        }
    }

    /// .read() or .map() if the component is not resolved.
    pub fn read_ok<O>(&self, f: impl FnOnce(&T) -> O) -> Result<O, SingleQueryError> {
        let handle = self.handle.read();
        match &*handle {
            Ok(handle) => Ok(f(&handle.value.read())),
            Err(err) => Err(err.clone()),
        }
    }

    /// Reads the component or returns its default value while unresolved.
    pub fn read_or_default(&self) -> ProjectedReadGuard<T>
    where
        T: Default,
    {
        // Preserve the scope subscription even when the component is missing.
        let handle = self.handle.read();
        match &*handle {
            Ok(handle) => handle.value.read().project(|component| component),
            Err(_err) => ProjectedReadGuard::from_owned(T::default()),
        }
    }

    /// Creates a memoized display string for the component.
    pub fn use_display(&self, f: impl Fn(&T) -> String + 'static) -> Memo<String> {
        let this = *self;
        use_memo(move || match this.read_ok(|component| f(component)) {
            Ok(value) => value,
            Err(err) => String::from(err),
        })
    }
}

#[cfg(feature = "asset")]
impl<T, U, E, A> ComponentMirrorSignal<T, U, E>
where
    T: DioxusComponentSync + std::ops::Deref<Target = Handle<A>>,
    U: ComponentBounds,
    E: 'static,
    A: DioxusAssetSync,
{
    /// Converts a handle bearing component into an asset mirror.
    pub fn use_asset(&self) -> AssetMaybeMirrorSignal<A> {
        let this = *self;
        // Only an unresolved component withholds the id. The asset mirror keeps reporting its own
        // loading and error state for every id it receives.
        let id_memo = use_memo(move || match this.read() {
            Ok(component) => Ok(component.id()),
            Err(_err) => Err(AssetNoneState::Fetching),
        });
        use_bevy_asset(id_memo)
    }
}

impl<T: DioxusComponentSync> ComponentMirrorHandle<T> {
    /// Converts a resolved handle into a mirror signal.
    pub fn use_signal<U>(
        self,
        map_fn: Projection<T, U>,
    ) -> ComponentMirrorSignal<T, U, SingleQueryError>
    where
        U: ComponentBounds,
    {
        let handle: Signal<Result<ComponentMirrorHandle<T>, SingleQueryError>> =
            use_signal(|| Ok(self));
        ComponentMirrorSignal::from_resolved(handle, map_fn, |err| err)
    }
}
