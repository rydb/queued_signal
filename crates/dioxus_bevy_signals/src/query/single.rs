use std::marker::PhantomData;

use bevy_ecs::{
    prelude::*,
    query::QueryFilter,
};
use dioxus_hooks::{use_effect, use_signal};
use dioxus_signals::{ReadableExt, Signal, WritableExt};
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
}

impl From<SingleQueryError> for String {
    fn from(value: SingleQueryError) -> Self {
        match value {
            SingleQueryError::NotInitialized => "Not initialized".into(),
            SingleQueryError::NoMatchingEntity => "No matching entities".into(),
            SingleQueryError::MoreThanOneEntity { entities } => format!("more then one matching entities: {:#?}", entities),
        }
    }
}

/// Handle to a single-entity mirrored Bevy query, 
pub struct MirrorQuerySingleHandle<Q: MirrorQueryData, F: QueryFilter> {
    /// The single matched item, or an error describing why resolution failed.
    pub item: Signal<Result<Q::MirrorItemHandles, SingleQueryError>>,
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
    /// Read the single item with zero refcount bump.
    ///
    /// Returns a [`SignalReadGuard`] that derefs to `Result<Q::MirrorItemHandles, SingleQueryError>`,
    /// so callers write `&*self.read()` to get `&Result<Q::MirrorItemHandles, SingleQueryError>`.
    pub fn read(&self) -> SignalReadGuard<'_, Result<Q::MirrorItemHandles, SingleQueryError>> {
        SignalReadGuard::new(self.item.read())
    }

    /// Read and map the `Ok` variant of the single item, or pass through the error.
    ///
    /// Avoids the refcount bump of a full [`SignalReadGuard`] when you only need
    /// a projected value from the matched entity.
    pub fn read_ok<U>(&self, f: impl FnOnce(&Q::MirrorItemHandles) -> U) -> Result<U, SingleQueryError> {
        let guard = self.item.read();
        match &*guard {
            Ok(item) => Ok(f(item)),
            Err(e) => Err(e.clone()),
        }
    }
}

/// Create or fetch a single-entity [`MirrorQuery`] signal — **non-blocking**.
///
/// Composes on [`use_bevy_query`], reusing all its infrastructure (tracking,
/// Bevy round-trip, health). The derived signal validates that exactly one
/// entity matches and errors with [`SingleQueryError`] otherwise.
pub fn use_bevy_single<Q: DioxusQuerySync + 'static, F: QueryFilter + 'static>()
-> MirrorQuerySingleHandle<Q, F> {
    let query_handle = use_bevy_query::<Q, F>();

    let mut item: Signal<Result<Q::MirrorItemHandles, SingleQueryError>> =
        use_signal(|| Err(SingleQueryError::NotInitialized));

    // Derive the single-item signal from the underlying query signal.
    {
        let query_signal = query_handle.signal;
        use_effect(move || {
            let new_val = match &*query_signal.read() {
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
    }

    MirrorQuerySingleHandle {
        item,
        health: query_handle.health,
        writer: query_handle.writer,
        _filter: PhantomData,
    }
}
