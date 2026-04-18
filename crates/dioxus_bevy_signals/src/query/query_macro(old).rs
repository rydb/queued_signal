//! Macro-generated implementations for IntoStaticItem and QuerySnapshotItem.

use crate::query_core::*;
use bevy_ecs::{entity::Entity, query::QueryData};
use variadics_please::all_tuples;

// -----------------------------------------------------------------------------
// Generate IntoStaticItem for tuple arities 1..=15
// -----------------------------------------------------------------------------

macro_rules! impl_into_static_item {
    ($($T:ident),+) => {
        impl<$($T: MirrorComponent),+> IntoStaticItem for ($($T,)+) {
            type Static = ($($T::Cache,)+);

            fn into_static(self) -> Self::Static {
                #[allow(non_snake_case)]
                let ($($T,)+) = self;
                ($($T.into_cache(),)+)
            }

            fn register_components(registry: &mut ComponentMutationRegistry) {
                $(<$T as MirrorComponent>::register_component(registry);)+
            }
        }
    };
}

all_tuples!(impl_into_static_item, 1, 15, T);

// -----------------------------------------------------------------------------
// Generate QuerySnapshotItem for (Entity, ...) tuples
// -----------------------------------------------------------------------------

macro_rules! impl_query_snapshot_item {
    ($($T:ident),+) => {
        impl<$($T: MirrorComponent),+> QuerySnapshotItem for (Entity, $($T::Cache),+) {
            type IterItem = (Entity, $($T::Iter),+);

            fn into_iter_item(self, entity: Entity, sender: QueryCommandSender) -> Self::IterItem {
                #[allow(non_snake_case)]
                let (_, $($T),+) = self;
                (entity, $($T::iter_from_cache(&$T, entity, sender.clone())),+)
            }
        }
    };
}

all_tuples!(impl_query_snapshot_item, 1, 15, T);