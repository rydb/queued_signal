//! Macro impls for library macros.

macro_rules! impl_mirror_query_data {
    ($T0:ident, $T1:ident) => {};

    // Single-element arm (1 component)

    ($T:ident) => {
        impl<$T: DioxusComponentSync> MirrorQueryData for (Entity, &mut $T) {
            type MirrorItem = (Entity, DioxusMirror<$T>);

            type MirrorSignalsQueryDataImMut = (Entity, &'static DioxusMirror<$T>);

            // Or does not implement QueryFilter for 1-tuples, so use bare filters.
            type MirrorSignalsWithoutFilter = Without<DioxusMirror<$T>>;

            type MirrorItemHandles = (Entity, ComponentMirrorHandle<$T>);

            type MirrorSignalsChangedFilter = Changed<DioxusMirror<$T>>;

            type TrackingQueriesQuerydataMut =
                (Entity, &'static mut DioxusTrackingQueries<$T>);

            fn register_mirror_sync_systems<F: QueryFilter>(world: &mut World) {
                world
                    .commands()
                    .queue(RequestComponentsMirror::<$T>::default());
                #[cfg(feature = "reflect")]
                crate::reflect::query::register_typed_query_active(
                    world,
                    vec![TypeId::of::<$T>()],
                );
            }

            fn component_type_ids() -> Vec<TypeId> {
                vec![TypeId::of::<$T>()]
            }

            fn handles_entity(handles: &Self::MirrorItemHandles) -> Entity {
                handles.0
            }

            fn read_component_erased(
                handles: &Self::MirrorItemHandles,
                idx: usize,
            ) -> Option<ErasedTypedReadGuard> {
                if idx == 0 {
                    let guard = handles.1.read();
                    let ptr = NonNull::from(&*guard).cast::<u8>();
                    Some(ErasedTypedReadGuard {
                        owner: Box::new(guard),
                        ptr,
                    })
                } else {
                    None
                }
            }

            fn with_component_mut(
                handles: &Self::MirrorItemHandles,
                idx: usize,
                f: Box<dyn Fn(NonNull<u8>) + Send + Sync>,
            ) {
                if idx == 0 {
                    handles
                        .1
                        .mutate(move |a: &mut $T| f(NonNull::from(&mut *a).cast::<u8>()));
                }
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
                ComponentMirrorHandle<$first>,
                ComponentMirrorHandle<$second>,
                ComponentMirrorHandle<$third>
                $(, ComponentMirrorHandle<$rest>)*
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
                #[cfg(feature = "reflect")]
                crate::reflect::query::register_typed_query_active(
                    world,
                    vec![
                        TypeId::of::<$first>(),
                        TypeId::of::<$second>(),
                        TypeId::of::<$third>()
                        $(, TypeId::of::<$rest>())*
                    ],
                );
            }

            fn component_type_ids() -> Vec<TypeId> {
                vec![
                    TypeId::of::<$first>(),
                    TypeId::of::<$second>(),
                    TypeId::of::<$third>()
                    $(, TypeId::of::<$rest>())*
                ]
            }

            fn handles_entity(handles: &Self::MirrorItemHandles) -> Entity {
                handles.0
            }

            fn read_component_erased(
                _handles: &Self::MirrorItemHandles,
                _idx: usize,
            ) -> Option<ErasedTypedReadGuard> {
                None
            }

            fn with_component_mut(
                _handles: &Self::MirrorItemHandles,
                _idx: usize,
                f: Box<dyn Fn(NonNull<u8>) + Send + Sync>,
            ) {
                drop(f);
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

macro_rules! impl_single_query_parts {
    // 2-component case is a no-op (handled manually in query/mod.rs).
    ($T0:ident, $T1:ident) => {};

    // Single-element arm (1 component)
    ($T:ident) => {
        impl<$T: DioxusComponentSync> SingleQueryParts for (Entity, &mut $T) {
            type Output = (
                Memo<Result<Entity, SingleQueryError>>,
                ComponentMirrorSignal<$T, $T, SingleQueryError>,
            );

            fn create_parts<F: QueryFilter + 'static>(
                resolved: Signal<Result<Self::MirrorItemHandles, SingleQueryError>>,
            ) -> Self::Output {
                let entity = use_memo(move || match resolved.read().as_ref() {
                    Ok(handles) => Ok(Self::handles_entity(handles)),
                    Err(err) => Err(err.clone()),
                });

                let mut t_signal: Signal<Result<ComponentMirrorHandle<$T>, SingleQueryError>> =
                    use_signal(|| Err(SingleQueryError::NotInitialized));

                use_memo(move || match resolved.read().as_ref() {
                    Ok(handles) => {
                        let handles = handles.clone();
                        #[allow(non_snake_case)]
                        let (_, $T) = handles;
                        t_signal.set(Ok($T));
                    }
                    Err(err) => {
                        t_signal.set(Err(err.clone()));
                    }
                });

                (
                    entity,
                    ComponentMirrorSignal::from_resolved(
                        t_signal,
                        |component: &$T| component,
                        |err| err,
                    ),
                )
            }
        }
    };

    // Multi-element arm (3+ components)
    ($first:ident, $second:ident, $third:ident $(, $rest:ident)*) => {
        pastey::paste! {
            impl<
                $first: DioxusComponentSync,
                $second: DioxusComponentSync,
                $third: DioxusComponentSync,
                $($rest: DioxusComponentSync),*
            > SingleQueryParts
                for (Entity, &mut $first, &mut $second, &mut $third $(, &mut $rest)*)
            {
                type Output = (
                    Memo<Result<Entity, SingleQueryError>>,
                    ComponentMirrorSignal<$first, $first, SingleQueryError>,
                    ComponentMirrorSignal<$second, $second, SingleQueryError>,
                    ComponentMirrorSignal<$third, $third, SingleQueryError>
                    $(, ComponentMirrorSignal<$rest, $rest, SingleQueryError>)*
                );

                fn create_parts<F: QueryFilter + 'static>(
                    resolved: Signal<Result<Self::MirrorItemHandles, SingleQueryError>>,
                ) -> Self::Output {
                    let entity = use_memo(move || match resolved.read().as_ref() {
                        Ok(handles) => Ok(Self::handles_entity(handles)),
                        Err(err) => Err(err.clone()),
                    });

                    let mut [< $first:snake _signal >]: Signal<Result<ComponentMirrorHandle<$first>, SingleQueryError>> =
                        use_signal(|| Err(SingleQueryError::NotInitialized));
                    let mut [< $second:snake _signal >]: Signal<Result<ComponentMirrorHandle<$second>, SingleQueryError>> =
                        use_signal(|| Err(SingleQueryError::NotInitialized));
                    let mut [< $third:snake _signal >]: Signal<Result<ComponentMirrorHandle<$third>, SingleQueryError>> =
                        use_signal(|| Err(SingleQueryError::NotInitialized));
                    $(
                        let mut [< $rest:snake _signal >]: Signal<Result<ComponentMirrorHandle<$rest>, SingleQueryError>> =
                            use_signal(|| Err(SingleQueryError::NotInitialized));
                    )*

                    use_memo(move || match resolved.read().as_ref() {
                        Ok(handles) => {
                            let handles = handles.clone();
                            #[allow(non_snake_case)]
                            let (_, $first, $second, $third $(, $rest)*) = handles;
                            [< $first:snake _signal >].set(Ok($first));
                            [< $second:snake _signal >].set(Ok($second));
                            [< $third:snake _signal >].set(Ok($third));
                            $(
                                [< $rest:snake _signal >].set(Ok($rest));
                            )*
                        }
                        Err(err) => {
                            [< $first:snake _signal >].set(Err(err.clone()));
                            [< $second:snake _signal >].set(Err(err.clone()));
                            [< $third:snake _signal >].set(Err(err.clone()));
                            $(
                                [< $rest:snake _signal >].set(Err(err.clone()));
                            )*
                        }
                    });

                    (
                        entity,
                        ComponentMirrorSignal::from_resolved(
                            [< $first:snake _signal >],
                            |component: &$first| component,
                            |err| err,
                        ),
                        ComponentMirrorSignal::from_resolved(
                            [< $second:snake _signal >],
                            |component: &$second| component,
                            |err| err,
                        ),
                        ComponentMirrorSignal::from_resolved(
                            [< $third:snake _signal >],
                            |component: &$third| component,
                            |err| err,
                        )
                        $(
                            , ComponentMirrorSignal::from_resolved(
                                [< $rest:snake _signal >],
                                |component: &$rest| component,
                                |err| err,
                            )
                        )*
                    )
                }
            }
        }
    };
}

use super::component::*;
use super::single::*;
use super::*;
use variadics_please::all_tuples;

all_tuples!(impl_mirror_query_data, 1, 13, T);
all_tuples!(impl_single_query_parts, 1, 13, T);
