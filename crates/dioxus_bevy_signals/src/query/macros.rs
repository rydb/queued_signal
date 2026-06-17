/// Implements [`MirrorQueryData`](super::MirrorQueryData) for tuples of
/// `(Entity, &mut T1, &mut T2, ..., &mut TN)`.

macro_rules! impl_mirror_query_data {
    ($T0:ident, $T1:ident) => {};

    // Single-element arm (1 component)

    ($T:ident) => {
        impl<$T: DioxusComponentSync> MirrorQueryData for (Entity, &mut $T) {
            type MirrorItem = (Entity, DioxusMirror<$T>);

            type MirrorSignalsQueryDataImMut = (Entity, &'static DioxusMirror<$T>);

            // Or does not implement QueryFilter for 1-tuples, so use bare filters.
            type MirrorSignalsWithoutFilter = Without<DioxusMirror<$T>>;

            type MirrorItemHandles = (Entity, DioxusMirrorHandle<$T>);

            type MirrorSignalsChangedFilter = Changed<DioxusMirror<$T>>;

            type TrackingQueriesQuerydataMut =
                (Entity, &'static mut DioxusTrackingQueries<$T>);

            fn register_mirror_sync_systems<F: QueryFilter>(world: &mut World) {
                world
                    .commands()
                    .queue(RequestComponentsMirror::<$T>::default());
            }

            fn get_mirror_entity<'w, 's>(
                item: &<<Self::MirrorSignalsQueryDataImMut as QueryData>::ReadOnly as QueryData>::Item<
                    'w,
                    's,
                >,
            ) -> Entity {
                item.0
            }

            fn get_query_entity<'w, 's>(item: &Self::Item<'w, 's>) -> Entity {
                item.0
            }

            fn get_mirror_bundle<'w, 's, F: QueryFilter + 'static>(
                item: Self::Item<'w, 's>,
            ) -> impl Bundle {
                #[allow(non_snake_case)]
                let (_, $T) = item;
                (DioxusMirror::init_and_decompose::<Self, F>($T.clone()),)
            }

            fn clone_dioxus_signals<'w, 's>(
                item: &<<Self::MirrorSignalsQueryDataImMut as QueryData>::ReadOnly as QueryData>::Item<
                    'w,
                    's,
                >,
            ) -> Self::MirrorItemHandles {
                #[allow(non_snake_case)]
                let (_, $T) = item;
                (item.0, $T.handle())
            }

            fn extract_version<'w, 's>(
                item: &<<Self::MirrorSignalsQueryDataImMut as QueryData>::ReadOnly as QueryData>::Item<
                    'w,
                    's,
                >,
            ) -> u64 {
                #[allow(non_snake_case)]
                let (_, $T) = item;
                $T.version.load(std::sync::atomic::Ordering::Relaxed)
            }

            fn apply_tracking_delta<'w, 's, F: QueryFilter + 'static>(
                mut item: <Self::TrackingQueriesQuerydataMut as QueryData>::Item<'w, 's>,
                delta: i32,
            ) {
                #[allow(non_snake_case)]
                let (_, $T) = &mut item;

                let id = query_to_tracking_id::<Self, F>();
                let current_delta = $T.tracking_counts.entry(id).or_insert(0);
                *current_delta += delta;
                if *current_delta <= 0 {
                    $T.tracking_counts.remove(&id);
                }
            }
        }
    };

    // Multi-element arm (3+ components)

    ($first:ident, $second:ident, $third:ident $(, $rest:ident)*) => {
        impl<
            $first: DioxusComponentSync,
            $second: DioxusComponentSync,
            $third: DioxusComponentSync,
            $($rest: DioxusComponentSync),*
        > MirrorQueryData
            for (Entity, &mut $first, &mut $second, &mut $third $(, &mut $rest)*)
        {
            type MirrorItem = (
                Entity,
                DioxusMirror<$first>,
                DioxusMirror<$second>,
                DioxusMirror<$third>
                $(, DioxusMirror<$rest>)*
            );

            type MirrorSignalsQueryDataImMut = (
                Entity,
                &'static DioxusMirror<$first>,
                &'static DioxusMirror<$second>,
                &'static DioxusMirror<$third>
                $(, &'static DioxusMirror<$rest>)*
            );

            type MirrorSignalsWithoutFilter = Or<(
                Without<DioxusMirror<$first>>,
                Without<DioxusMirror<$second>>,
                Without<DioxusMirror<$third>>
                $(, Without<DioxusMirror<$rest>>)*
            )>;

            type MirrorItemHandles = (
                Entity,
                DioxusMirrorHandle<$first>,
                DioxusMirrorHandle<$second>,
                DioxusMirrorHandle<$third>
                $(, DioxusMirrorHandle<$rest>)*
            );

            type MirrorSignalsChangedFilter = Or<(
                Changed<DioxusMirror<$first>>,
                Changed<DioxusMirror<$second>>,
                Changed<DioxusMirror<$third>>
                $(, Changed<DioxusMirror<$rest>>)*
            )>;

            type TrackingQueriesQuerydataMut = (
                Entity,
                &'static mut DioxusTrackingQueries<$first>,
                &'static mut DioxusTrackingQueries<$second>,
                &'static mut DioxusTrackingQueries<$third>
                $(, &'static mut DioxusTrackingQueries<$rest>)*
            );

            fn register_mirror_sync_systems<F: QueryFilter>(world: &mut World) {
                world.commands().queue(RequestComponentsMirror::<$first>::default());
                world.commands().queue(RequestComponentsMirror::<$second>::default());
                world.commands().queue(RequestComponentsMirror::<$third>::default());
                $(
                    world.commands().queue(RequestComponentsMirror::<$rest>::default());
                )*
            }

            fn get_mirror_entity<'w, 's>(
                item: &<<Self::MirrorSignalsQueryDataImMut as QueryData>::ReadOnly as QueryData>::Item<
                    'w,
                    's,
                >,
            ) -> Entity {
                item.0
            }

            fn get_query_entity<'w, 's>(item: &Self::Item<'w, 's>) -> Entity {
                item.0
            }

            fn get_mirror_bundle<'w, 's, F: QueryFilter + 'static>(
                item: Self::Item<'w, 's>,
            ) -> impl Bundle {
                #[allow(non_snake_case)]
                let (_, $first, $second, $third $(, $rest)*) = item;
                (
                    DioxusMirror::init_and_decompose::<Self, F>($first.clone()),
                    DioxusMirror::init_and_decompose::<Self, F>($second.clone()),
                    DioxusMirror::init_and_decompose::<Self, F>($third.clone())
                    $(, DioxusMirror::init_and_decompose::<Self, F>($rest.clone()))*
                )
            }

            fn clone_dioxus_signals<'w, 's>(
                item: &<<Self::MirrorSignalsQueryDataImMut as QueryData>::ReadOnly as QueryData>::Item<
                    'w,
                    's,
                >,
            ) -> Self::MirrorItemHandles {
                #[allow(non_snake_case)]
                let (_, $first, $second, $third $(, $rest)*) = item;
                (
                    item.0,
                    $first.handle(),
                    $second.handle(),
                    $third.handle()
                    $(, $rest.handle())*
                )
            }

            fn extract_version<'w, 's>(
                item: &<<Self::MirrorSignalsQueryDataImMut as QueryData>::ReadOnly as QueryData>::Item<
                    'w,
                    's,
                >,
            ) -> u64 {
                #[allow(non_snake_case)]
                let (_, $first, $second, $third $(, $rest)*) = item;
                let v0 = $first.version.load(std::sync::atomic::Ordering::Relaxed);
                let v1 = $second.version.load(std::sync::atomic::Ordering::Relaxed);
                let v2 = $third.version.load(std::sync::atomic::Ordering::Relaxed);
                // Combine via XOR and rotate to avoid collisions between version tuples.
                // rust-analyzer complains about this being mut without unused_mut?
                #[allow(unused_mut)]
                let mut combined = v0 ^ v1.rotate_left(21) ^ v2.rotate_left(42);
                $(
                    let v = $rest.version.load(std::sync::atomic::Ordering::Relaxed);
                    combined ^= v.rotate_left(11);
                )*
                combined
            }

            fn apply_tracking_delta<'w, 's, F: QueryFilter + 'static>(
                mut item: <Self::TrackingQueriesQuerydataMut as QueryData>::Item<'w, 's>,
                delta: i32,
            ) {
                #[allow(non_snake_case)]
                let (_, $first, $second, $third $(, $rest)*) = &mut item;

                {
                    let id = query_to_tracking_id::<Self, F>();
                    let current_delta = $first.tracking_counts.entry(id).or_insert(0);
                    *current_delta += delta;
                    if *current_delta <= 0 {
                        $first.tracking_counts.remove(&id);
                    }
                }
                {
                    let id = query_to_tracking_id::<Self, F>();
                    let current_delta = $second.tracking_counts.entry(id).or_insert(0);
                    *current_delta += delta;
                    if *current_delta <= 0 {
                        $second.tracking_counts.remove(&id);
                    }
                }
                {
                    let id = query_to_tracking_id::<Self, F>();
                    let current_delta = $third.tracking_counts.entry(id).or_insert(0);
                    *current_delta += delta;
                    if *current_delta <= 0 {
                        $third.tracking_counts.remove(&id);
                    }
                }
                $(
                    {
                        let id = query_to_tracking_id::<Self, F>();
                        let current_delta = $rest.tracking_counts.entry(id).or_insert(0);
                        *current_delta += delta;
                        if *current_delta <= 0 {
                            $rest.tracking_counts.remove(&id);
                        }
                    }
                )*
            }
        }
    };
}

use super::*;
use variadics_please::all_tuples;

all_tuples!(impl_mirror_query_data, 1, 13, T);
