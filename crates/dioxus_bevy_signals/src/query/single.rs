//! Implementation for [`Single<Q, F>`] mirror

use std::marker::PhantomData;

use bevy_ecs::{prelude::*, query::QueryFilter};
use dioxus_hooks::{use_memo, use_signal};
use dioxus_signals::{ReadableExt, Signal, WritableExt};
use queued_signal::state::{HealthStatus, QueuedSignal};

use super::{DioxusQuerySync, MirrorQuery, MirrorQueryData, SignalReadGuard, use_bevy_query};

/// Error when a single-entity query doesn't resolve to exactly one match.
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

    /// Combination of .read() + .map()
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

/// Query for a single mirrored bevy entity.
/// Resolves to an error if there is not exactly one matching result.
pub fn use_bevy_single<Q: DioxusQuerySync + 'static, F: QueryFilter + 'static>()
-> MirrorQuerySingleHandle<Q, F> {
    let query_handle = use_bevy_query::<Q, F>();

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

    MirrorQuerySingleHandle {
        item,
        health: query_handle.health,
        writer: query_handle.writer,
        _filter: PhantomData,
    }
}
