//! Implementation for [`Single<Q, F>`] mirror

use std::fmt::Display;
use std::marker::PhantomData;

use bevy_ecs::{prelude::*, query::QueryFilter};
use dioxus_hooks::{use_memo, use_signal};
use dioxus_signals::{ReadableExt, Signal, WritableExt};
use queued_signal::state::{HealthStatus, QueuedSignal};

use super::{
    DioxusQuerySync, MirrorQuery, MirrorQueryData, MirrorQuerySignalHandle, SignalReadGuard,
    use_bevy_query,
};

/// Error when a single-entity query does not resolve to exactly one match.
#[derive(Clone, Debug, PartialEq)]
pub enum SingleQueryError {
    /// The mirror signal has not been initialized yet.
    NotInitialized,
    /// The query matched zero entities.
    NoMatchingEntity,
    /// The query matched more than one entity. Contains all matching [`Entity`] IDs.
    MoreThanOneEntity(Vec<Entity>),
}

impl From<SingleQueryError> for String {
    fn from(value: SingleQueryError) -> Self {
        match value {
            SingleQueryError::NotInitialized => "Not initialized".into(),
            SingleQueryError::NoMatchingEntity => "No matching entities".into(),
            SingleQueryError::MoreThanOneEntity(entities) => {
                format!("more than one matching entity: {:#?}", entities)
            }
        }
    }
}

impl Display for SingleQueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SingleQueryError::NotInitialized => write!(f, "Not initialized"),
            SingleQueryError::NoMatchingEntity => write!(f, "No matching entities"),
            SingleQueryError::MoreThanOneEntity(entities) => {
                write!(f, "more than one matching entity: {:#?}", entities)
            }
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
        let new_val = match &dep {
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

/// Handle to a single-entity mirrored query.
pub struct MirrorQuerySingleHandle<Q: MirrorQueryData + 'static, F: QueryFilter + 'static> {
    /// The single matched item, or an error describing why resolution failed.
    item: Signal<Result<Q::MirrorItemHandles, SingleQueryError>>,
    /// Health of the underlying signal.
    pub health: Signal<HealthStatus>,
    /// None until the round trip completes.
    pub writer: Signal<Option<QueuedSignal<MirrorQuery<Q, F>>>>,
    _filter: PhantomData<F>,
}

impl<Q: MirrorQueryData + 'static, F: QueryFilter + 'static> Clone
    for MirrorQuerySingleHandle<Q, F>
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<Q: MirrorQueryData + 'static, F: QueryFilter + 'static> Copy
    for MirrorQuerySingleHandle<Q, F>
{
}

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

/// Maps a [`MirrorQueryData`] implementor to its per-field mirror output.
pub trait SingleQueryParts: MirrorQueryData {
    /// The resolved entity memo and one mirror per field.
    type Output;

    /// Creates the per-field mirrors from a resolved single signal.
    fn create_parts<F: QueryFilter + 'static>(
        resolved: Signal<Result<Self::MirrorItemHandles, SingleQueryError>>,
    ) -> Self::Output;
}

/// Query for a single mirrored entity, returning per-field mirrors.
pub fn use_bevy_single<
    Q: DioxusQuerySync + SingleQueryParts + 'static,
    F: QueryFilter + 'static,
>() -> Q::Output {
    let query_handle = use_bevy_query::<Q, F>();
    let resolved = resolve_single::<Q, F>(query_handle);
    Q::create_parts::<F>(resolved)
}
