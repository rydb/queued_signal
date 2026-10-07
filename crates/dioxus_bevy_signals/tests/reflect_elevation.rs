//! Verifies that an untyped reflect query elevates to a typed query on request.

use std::any::TypeId;

use bevy_app::App;
use bevy_ecs::prelude::*;
use bevy_ecs::world::CommandQueue;
use bevy_reflect::Reflect;
use dioxus_bevy_signals::reflect::query::{
    ElevateReflectQuery, ReflectQueryRegistry, TypedQuerySpawnerRegistry, register_or_get_query_dyn,
    register_typed_query_spawner_runtime,
};
use dioxus_bevy_signals::reflect::resource::{
    ReflectResourceRegistry, notify_typed_resource_mirror, register_or_get_resource_dyn,
};
use dioxus_bevy_signals::resource::ResourceQueuedSignalMirror;
use parking_lot::Mutex;
use queued_signal::state::{QueuedSignal, WriterDriver};
use std::sync::Arc;

#[derive(Component, Reflect, Default, Clone)]
#[reflect(Component)]
struct Name(String);

#[derive(Component, Reflect, Default)]
#[reflect(Component)]
struct Transform;

#[derive(Component, Default, Clone)]
struct NonReflect;

#[test]
fn untyped_query_elevates_to_typed() {
    let mut app = App::new();
    app.register_type::<Name>();
    app.register_type::<Transform>();
    app.init_resource::<ReflectQueryRegistry>();

    {
        let world = app.world_mut();
        world.register_component::<Name>();
        world.register_component::<Transform>();
    }

    let mut type_ids = vec![TypeId::of::<Name>(), TypeId::of::<Transform>()];
    type_ids.sort_unstable();

    {
        let world = app.world_mut();
        let signal =
            register_or_get_query_dyn(world, &["Name".to_string(), "Transform".to_string()])
                .expect("reflect query should register");
        drop(signal);

        assert!(
            !world.resource::<ReflectQueryRegistry>().map[&type_ids].elevated,
            "mirror starts unelevated"
        );
    }

    {
        let world = app.world_mut();
        let mut queue = CommandQueue::default();
        queue.push(ElevateReflectQuery {
            type_ids: type_ids.clone(),
        });
        queue.apply(world);
    }

    {
        let world = app.world_mut();
        let mirror = &world.resource::<ReflectQueryRegistry>().map[&type_ids];
        assert!(mirror.elevated, "mirror should be elevated after request");
        assert_eq!(mirror.active_count, 0, "reflect sync should be disabled");
    }
}

#[derive(Resource, Reflect, Clone, Default)]
#[reflect(Resource)]
struct Counter(i32);

#[test]
fn untyped_resource_elevates_to_typed() {
    let mut app = App::new();
    app.register_type::<Counter>();
    app.init_resource::<ReflectResourceRegistry>();

    {
        let world = app.world_mut();
        world.register_component::<Counter>();
        world.insert_resource(Counter(7));
    }

    {
        let world = app.world_mut();
        let signal = register_or_get_resource_dyn(world, "Counter")
            .expect("reflect resource should register");
        drop(signal);

        assert!(
            !world.resource::<ReflectResourceRegistry>().map[&TypeId::of::<Counter>()].elevated,
            "mirror starts unelevated"
        );
    }

    {
        let world = app.world_mut();
        let driver = WriterDriver::new(Counter(7));
        let set_value_tx = driver.set_value_tx.clone();
        let set_tx = driver.set_tx.clone();
        let add_tx = driver.add_tx.clone();
        let queued_state = driver.queued_state.clone();
        let driver_arc = Arc::new(Mutex::new(driver));
        let signal =
            QueuedSignal::from_parts(queued_state, Some(driver_arc), add_tx, set_tx, set_value_tx);
        world.insert_resource(ResourceQueuedSignalMirror(signal));
        notify_typed_resource_mirror::<Counter>(world);
    }

    {
        let world = app.world_mut();
        let mirror = &world.resource::<ReflectResourceRegistry>().map[&TypeId::of::<Counter>()];
        assert!(
            mirror.elevated,
            "mirror should be elevated after typed request"
        );
        assert_eq!(mirror.active_count, 0, "reflect sync should be disabled");
    }
}

#[test]
fn mixed_query_registers_with_none_reflect_slot() {
    let mut app = App::new();
    app.register_type::<Name>();
    app.init_resource::<ReflectQueryRegistry>();

    {
        let world = app.world_mut();
        world.register_component::<Name>();
        world.register_component::<NonReflect>();
    }

    let mut key = vec![TypeId::of::<Name>(), TypeId::of::<NonReflect>()];
    key.sort_unstable();

    {
        let world = app.world_mut();
        let handles = register_or_get_query_dyn(
            world,
            &["Name".to_string(), "NonReflect".to_string()],
        )
        .expect("mixed query should register");
        drop(handles);

        let mirror = &world.resource::<ReflectQueryRegistry>().map[&key];
        assert_eq!(mirror.reflect_from_ptrs.len(), 2, "two component slots");
        assert!(mirror.reflect_from_ptrs[0].is_some(), "Name reflects");
        assert!(
            mirror.reflect_from_ptrs[1].is_none(),
            "NonReflect has no reflect data"
        );
    }
}

#[test]
fn typed_spawner_registers_for_mixed_query() {
    let mut app = App::new();
    app.register_type::<Name>();
    app.init_resource::<TypedQuerySpawnerRegistry>();

    {
        let world = app.world_mut();
        world.register_component::<Name>();
        world.register_component::<NonReflect>();
    }

    let mut key = vec![TypeId::of::<Name>(), TypeId::of::<NonReflect>()];
    key.sort_unstable();

    {
        let world = app.world_mut();
        register_typed_query_spawner_runtime::<(Entity, &mut Name, &mut NonReflect), ()>(world);
        assert!(
            world
                .resource::<TypedQuerySpawnerRegistry>()
                .spawners
                .contains_key(&key),
            "spawner registered for the mixed component set"
        );
    }
}

#[test]
fn unknown_query_name_errors() {
    let mut app = App::new();
    app.register_type::<Name>();
    app.init_resource::<ReflectQueryRegistry>();

    {
        let world = app.world_mut();
        world.register_component::<Name>();
        let result = register_or_get_query_dyn(world, &["DoesNotExist".to_string()]);
        assert!(result.is_err(), "unknown component names fail");
    }
}
