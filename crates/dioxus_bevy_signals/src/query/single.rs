//! Implementation for [`Single<Q, F>`] mirror

use std::marker::PhantomData;

#[cfg(feature = "asset")]
use bevy_asset::Handle;
use bevy_ecs::{prelude::*, query::QueryFilter};
use dioxus_hooks::{use_memo, use_signal};
use dioxus_signals::{Memo, ReadableExt, Signal, WritableExt};
use queued_signal::state::{HealthStatus, QueuedSignal};

#[cfg(feature = "asset")]
use crate::asset::{AssetMaybeMirrorSignal, AssetNoneState, DioxusAssetSync, use_bevy_asset};

use super::{DioxusComponentSync, DioxusQuerySync, MirrorQuery, MirrorQueryData, MirrorQuerySignalHandle, SignalReadGuard, use_bevy_query};

/// Error when a single-entity query does not resolve to exactly one match.
#[derive(Clone, Debug, PartialEq)]
pub enum SingleQueryError {
    /// The mirror signal has not been initialized yet.
    NotInitialized,
    /// The query matched zero entities.
    NoMatchingEntity,
    /// The query matched more than one entity. Contains all matching [`Entity`] IDs.
    MoreThanOneEntity(Vec<Entity>),
    /// No upper bound found on the size hint for the query.
    NoUpperBound,
}

impl From<SingleQueryError> for String {
    fn from(value: SingleQueryError) -> Self {
        match value {
            SingleQueryError::NotInitialized => "Not initialized".into(),
            SingleQueryError::NoMatchingEntity => "No matching entities".into(),
            SingleQueryError::MoreThanOneEntity(entities) => {
                format!("more than one matching entity: {:#?}", entities)
            }
            SingleQueryError::NoUpperBound => "No upper bound for query size found".into(),
        }
    }
}

/// Resolves a query signal handle into a single matched entity's handles.
pub fn resolve_single<Q: DioxusQuerySync + 'static, F: QueryFilter + 'static>(
    query_handle: MirrorQuerySignalHandle<Q, F>,
) -> Signal<Result<Q::MirrorItemHandles, SingleQueryError>> {
    let mut item: Signal<Result<Q::MirrorItemHandles, SingleQueryError>> =
        use_signal(|| Err(SingleQueryError::NotInitialized));

    use_memo(move || {
        let dep = query_handle.read();
        let new_val = match &*dep {
            Err(_) => Err(SingleQueryError::NotInitialized),
            Ok(mq) => match mq.value.len() {
                0 => Err(SingleQueryError::NoMatchingEntity),
                1 => {
                    let single = mq.value.values().next().unwrap().clone();
                    Ok(single)
                }
                _ => {
                    let entities: Vec<Entity> = mq.value.keys().copied().collect();
                    Err(SingleQueryError::MoreThanOneEntity(entities))
                }
            },
        };
        item.set(new_val);
    });

    item
}

/// Handle to a single-entity mirrored bevy query.
pub struct MirrorQuerySingleHandle<Q: MirrorQueryData, F: QueryFilter> {
    /// The single matched item, or an error describing why resolution failed.
    item: Signal<Result<Q::MirrorItemHandles, SingleQueryError>>,
    /// Health of the underlying signal.
    pub health: Signal<HealthStatus>,
    /// None until the bevy round-trip completes.
    pub writer: Signal<Option<QueuedSignal<MirrorQuery<Q, F>>>>,
    _filter: PhantomData<F>,
}

impl<Q: MirrorQueryData, F: QueryFilter> Clone for MirrorQuerySingleHandle<Q, F> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Q: MirrorQueryData, F: QueryFilter> Copy for MirrorQuerySingleHandle<Q, F> {}

impl<Q: MirrorQueryData + 'static, F: QueryFilter + 'static> MirrorQuerySingleHandle<Q, F> {    
    /// Aquires a read guard on the inner read value of the signal
    pub fn read(&self) -> SignalReadGuard<'_, Result<Q::MirrorItemHandles, SingleQueryError>> {
        SignalReadGuard::new(self.item.read())
    }

    /// .read() + .map() convienience method.
    pub fn read_ok<U>(
        &self,
        f: impl FnOnce(&Q::MirrorItemHandles) -> U,
    ) -> Result<U, SingleQueryError> {
        let guard = self.item.read();
        match &*guard {
            Ok(item) => Ok(f(item)),
            Err(e) => Err(e.clone()),
        }
    }
}

/// Handle for the Entity field of a single-entity query.
#[derive(Clone, Copy)]
pub struct SingleEntityHandle {
    pub(crate) signal: Signal<Result<Entity, SingleQueryError>>,
}

impl SingleEntityHandle {
    /// Read the current entity or error state.
    pub fn read(&self) -> SignalReadGuard<'_, Result<Entity, SingleQueryError>> {
        SignalReadGuard::new(self.signal.read())
    }

    /// .read() + .map() convienience method.
    pub fn read_ok<U>(&self, f: impl FnOnce(&Entity) -> U) -> Result<U, SingleQueryError> {
        let guard = self.signal.read();
        match &*guard {
            Ok(entity) => Ok(f(entity)),
            Err(e) => Err(e.clone()),
        }
    }
}

/// Handle for a single component mirrored from a single-entity bevy query.
#[derive(Clone)]
pub struct SingleComponentHandle<T: DioxusComponentSync> {
    pub(crate) signal: Signal<Result<super::DioxusMirrorHandle<T>, SingleQueryError>>,
}

impl<T: DioxusComponentSync> Copy for SingleComponentHandle<T> {}


impl<T: DioxusComponentSync> SingleComponentHandle<T> {
    /// Read the current component handle or error state.
    pub fn read(&self) -> SignalReadGuard<'_, Result<super::DioxusMirrorHandle<T>, SingleQueryError>> {
        SignalReadGuard::new(self.signal.read())
    }

    /// .read() + .map() convienience method.
    pub fn read_ok<U>(
        &self,
        f: impl FnOnce(&super::DioxusMirrorHandle<T>) -> U,
    ) -> Result<U, SingleQueryError> {
        let guard = self.signal.read();
        match &*guard {
            Ok(handle) => Ok(f(handle)),
            Err(e) => Err(e.clone()),
        }
    }

    /// Mutate the underlying component value on the bevy side.
    pub fn mutate<F>(&self, f: F)
    where
        F: Fn(&mut T) + Send + Sync + 'static,
    {
        let guard = self.signal.read();
        if let Ok(handle) = &*guard {
            handle.value.mutate(f);
        }
    }

    /// Create a memoized display string from the component value.
    pub fn use_display(&self, f: impl Fn(&T) -> String + 'static) -> Memo<String> {
        let this = *self;
        use_memo(move || {
            this.read_ok(|handle| {
                let guard = handle.read();
                f(&guard)
            })
            .unwrap_or_else(|err| err.into())
        })
    }
}

#[cfg(feature = "asset")]
impl<T, A> SingleComponentHandle<T>
where
    T: DioxusComponentSync + std::ops::Deref<Target = Handle<A>>,
    A: DioxusAssetSync,
{
    /// Converts a handle-bearing component into an asset mirror signal.
    pub fn use_asset(&self) -> AssetMaybeMirrorSignal<A> {
        let this = *self;
        let id_memo = use_memo(move || {
            this.read_ok(|handle| Ok(handle.read().id()))
                .unwrap_or(Err(AssetNoneState::Fetching))
        });
        use_bevy_asset(id_memo)
    }
}

/// Maps a [`MirrorQueryData`] implementor to its destructured per-field handle tuple.
pub trait SingleQueryParts: MirrorQueryData {
    /// The destructured tuple of per-field handles.
    type Output;

    /// Create the per-field handles from a resolved single signal.
    fn create_parts<F: QueryFilter + 'static>(
        resolved: Signal<Result<Self::MirrorItemHandles, SingleQueryError>>,
    ) -> Self::Output;
}

/// Query for a single mirrored bevy entity, returning per-field handles.
pub fn use_bevy_single<Q: DioxusQuerySync + SingleQueryParts + 'static, F: QueryFilter + 'static>()
-> Q::Output {
    let query_handle = use_bevy_query::<Q, F>();
    let resolved = resolve_single::<Q, F>(query_handle);
    Q::create_parts::<F>(resolved)
}
