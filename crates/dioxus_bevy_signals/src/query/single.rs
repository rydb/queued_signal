use std::marker::PhantomData;

use bevy_ecs::{
    prelude::*,
    query::QueryFilter,
};
use dioxus_hooks::{use_effect, use_memo, use_signal};
use dioxus_signals::{Memo, ReadableExt, Signal, WritableExt};
use queued_signal::signal::{HealthStatus, QueuedSignal};

use super::{
    DioxusQuerySync, MirrorQuery, MirrorQueryData, QueryNoneState,
    SignalReadGuard, use_bevy_query,
};

/// Error when a single-entity query doesn't resolve to exactly one match.
#[derive(Clone, Debug, PartialEq)]
pub enum SingleQueryError {
    /// The mirror signal hasn't been initialized yet (Bevy round-trip pending).
    NotInitialized,
    /// The query matched zero entities.
    NoMatchingEntity,
    /// The query matched more than one entity. Contains all matching [`Entity`] IDs.
    MoreThanOneEntity { entities: Vec<Entity> },
    /// no upper bound found on the size hint for the query size
    NoUpperBound
}

impl From<SingleQueryError> for String {
    fn from(value: SingleQueryError) -> Self {
        match value {
            SingleQueryError::NotInitialized => "Not initialized".into(),
            SingleQueryError::NoMatchingEntity => "No matching entities".into(),
            SingleQueryError::MoreThanOneEntity { entities } => format!("more then one matching entities: {:#?}", entities),
            SingleQueryError::NoUpperBound => "No upper bound for query size found".into(),
        }
    }
}

/// Handle to a single-entity mirrored Bevy query, 
pub struct MirrorQuerySingleHandle<Q: MirrorQueryData, F: QueryFilter> {
    /// The single matched item, or an error describing why resolution failed.
    item: Signal<Result<Q::MirrorItemHandles, SingleQueryError>>,
    /// Health of the underlying signal.
    pub health: Signal<HealthStatus>,
    /// `None` until the Bevy round-trip completes (non-blocking).
    pub writer: Signal<Option<QueuedSignal<MirrorQuery<Q, F>>>>,
    _filter: PhantomData<F>,
}

impl<Q: MirrorQueryData, F: QueryFilter> Clone for MirrorQuerySingleHandle<Q, F> {
    fn clone(&self) -> Self {
        Self {
            item: self.item.clone(),
            health: self.health.clone(),
            writer: self.writer.clone(),
            _filter: self._filter.clone(),
        }
    }
}

impl<Q: MirrorQueryData, F: QueryFilter> Copy for MirrorQuerySingleHandle<Q, F> {}

impl<Q: MirrorQueryData + 'static, F: QueryFilter + 'static> MirrorQuerySingleHandle<Q, F> {
    pub fn read(&self) -> SignalReadGuard<'_, Result<Q::MirrorItemHandles, SingleQueryError>> {
        SignalReadGuard::new(self.item.read())
    }

    pub fn read_ok<U>(&self, f: impl FnOnce(&Q::MirrorItemHandles) -> U) -> Result<U, SingleQueryError> {
        let guard = self.item.read();
        match &*guard {
            Ok(item) => Ok(f(item)),
            Err(e) => Err(e.clone()),
        }
    }
}

/// Query for a mirror bevy [`Single<Q>`], resolves an error when read if there more or less then one result.
pub fn use_bevy_single<Q: DioxusQuerySync + 'static, F: QueryFilter + 'static>()
-> MirrorQuerySingleHandle<Q, F> {
    let query_handle = use_bevy_query::<Q, F>();

    let mut item: Signal<Result<Q::MirrorItemHandles, SingleQueryError>> = use_signal(|| Err(SingleQueryError::NotInitialized));

    let mut last_version: Signal<u64> = use_signal(|| 0);

    use_memo(move || {
        // Always read to register the reactive dependency on the query signal,
        // even when we early-return. Without this, Dioxus's reset_and_run_in
        // clears the subscriber registration and subsequent updates from Bevy
        // never trigger this memo
        let dep = query_handle.read();

        // Check if the query signal has published a new version
        let writer = query_handle.writer.read();
        let current_version = writer.as_ref()
            .map(|s| s.peek_version())
            .unwrap_or(0);
        if current_version == *last_version.read() {
            return;
        }
        last_version.set(current_version);

        let new_val = match &*dep {
            Err(_) => Err(SingleQueryError::NotInitialized),
            Ok(mq) => {
                match mq.value.len() {
                    0 => Err(SingleQueryError::NoMatchingEntity),
                    1 => {
                        let single = mq.value.values().next().unwrap().clone();
                        Ok(single)
                    }
                    _ => {
                        let entities: Vec<Entity> =
                            mq.value.keys().copied().collect();
                        Err(SingleQueryError::MoreThanOneEntity { entities })
                    }
                }
            }
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
