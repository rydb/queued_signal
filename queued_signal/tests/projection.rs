//! Tests for zero-copy projection of tracked read guards.

use queued_signal::state::{ProjectedReadGuard, WriterDriver};

#[derive(Clone, Debug)]
struct Resource {
    name: String,
    count: i32,
}

#[derive(Clone, Debug, PartialEq)]
struct Snapshot {
    count: i32,
}

#[test]
fn projection_ignores_unrelated_changes() {
    let mut driver = WriterDriver::new(Resource {
        name: "first".to_owned(),
        count: 1,
    });
    let state = driver.queued_state.clone();
    assert_eq!(state.read().name, "first");

    let first = state.read().project(|resource| &resource.count);
    assert_eq!(*first, 1);

    driver.publish_value(Resource {
        name: "second".to_owned(),
        count: 1,
    });
    let second = state.read().project(|resource| &resource.count);
    assert_eq!(first, second);

    driver.publish_value(Resource {
        name: "second".to_owned(),
        count: 2,
    });
    let third = state.read().project(|resource| &resource.count);
    assert_ne!(second, third);
}

#[test]
fn self_projection_compares_resource_values() {
    let driver = WriterDriver::new(Snapshot { count: 1 });
    let state = driver.queued_state.clone();

    let guard = state.read().project(|snapshot| snapshot);
    guard.heartbeat();

    assert_eq!(*guard, Snapshot { count: 1 });
    assert_eq!(guard.as_ref(), &Snapshot { count: 1 });
}

#[test]
fn tracked_guard_keeps_snapshot_equality() {
    let mut driver = WriterDriver::new(Snapshot { count: 1 });
    let state = driver.queued_state.clone();

    let first = state.read();
    let second = state.read();
    assert!(first == second);

    driver.publish_value(Snapshot { count: 1 });
    let third = state.read();
    assert!(first != third);
}

#[test]
fn owned_guard_compares_with_pinned_guard() {
    let driver = WriterDriver::new(Snapshot { count: 1 });
    let state = driver.queued_state.clone();

    let pinned = state.read().project(|snapshot| snapshot);
    let owned = ProjectedReadGuard::from_owned(Snapshot { count: 1 });

    owned.heartbeat();
    assert_eq!(*owned, Snapshot { count: 1 });
    assert_eq!(pinned, owned);
}
