//! Health semantics for retained snapshot generations.
//!
//! A guard keeps the generation it read alive. Health reports the retained count while publishes
//! keep releasing older generations, and reports a stall once a whole window of publishes released
//! nothing. Reclamation runs on the receiver thread, so every check settles first.

use std::time::Duration;

use queued_signal::state::{HealthStatus, WriterDriver};

#[derive(Clone, Debug, PartialEq)]
struct Counter {
    value: i32,
}

/// Drives a reclamation cycle and health scans until the status reports healthy.
fn settle(driver: &mut WriterDriver<Counter>) -> HealthStatus {
    for _ in 0..2_000 {
        queued_signal::atom_coerce::flush_reclamation();
        driver.update_health();
        let status = driver.queued_state.health();
        if status == HealthStatus::Healthy {
            return status;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    driver.queued_state.health()
}

#[test]
fn health_suite() {
    released_cached_projection_returns_health_to_healthy();
    cached_projection_reports_the_generation_it_pins();
    held_guard_reports_degraded_after_a_publish();
    publishes_without_a_reclaim_report_stalled();
}

fn released_cached_projection_returns_health_to_healthy() {
    let mut driver = WriterDriver::new(Counter { value: 0 });
    let state = driver.queued_state.clone();

    let cached = state.read_cached(|counter| &counter.value);
    driver.publish_value(Counter { value: 1 });
    drop(cached);

    assert_eq!(settle(&mut driver), HealthStatus::Healthy);

    let refreshed = state.read_cached(|counter| &counter.value);
    assert_eq!(*refreshed, 1);
}

fn cached_projection_reports_the_generation_it_pins() {
    let mut driver = WriterDriver::new(Counter { value: 0 });
    let state = driver.queued_state.clone();

    let cached = state.read_cached(|counter| &counter.value);
    driver.publish_value(Counter { value: 7 });

    assert_eq!(*cached, 0);
    driver.update_health();
    assert_eq!(
        driver.queued_state.health(),
        HealthStatus::Degraded { pinned_buffers: 1 }
    );

    drop(cached);
    assert_eq!(settle(&mut driver), HealthStatus::Healthy);

    let refreshed = state.read_cached(|counter| &counter.value);
    assert_eq!(*refreshed, 7);
}

fn held_guard_reports_degraded_after_a_publish() {
    let mut driver = WriterDriver::new(Counter { value: 0 });
    let state = driver.queued_state.clone();

    let held = state.read();
    driver.publish_value(Counter { value: 1 });

    assert_eq!(held.value, 0);
    driver.update_health();
    assert_eq!(
        driver.queued_state.health(),
        HealthStatus::Degraded { pinned_buffers: 1 }
    );

    drop(held);
    assert_eq!(settle(&mut driver), HealthStatus::Healthy);
}

fn publishes_without_a_reclaim_report_stalled() {
    let mut driver = WriterDriver::new(Counter { value: 0 });
    let state = driver.queued_state.clone();

    let first = state.read();
    driver.publish_value(Counter { value: 1 });
    let second = state.read();
    driver.publish_value(Counter { value: 2 });

    assert_eq!(first.value, 0);
    assert_eq!(second.value, 1);
    driver.update_health();
    assert_eq!(
        driver.queued_state.health(),
        HealthStatus::Degraded { pinned_buffers: 2 },
        "retained generations with progress still happening are degraded"
    );

    // A guard taken before each publish keeps that generation, so the window reports a stall.
    let mut pinned = Vec::new();
    for value in 3..=7 {
        pinned.push(state.read());
        driver.publish_value(Counter { value });
        driver.update_health();
    }
    assert!(
        matches!(driver.queued_state.health(), HealthStatus::Stalled { .. }),
        "publishes with no reclaim must report a stall, got {:?}",
        driver.queued_state.health()
    );

    drop(first);
    drop(second);
    drop(pinned);
    assert_eq!(settle(&mut driver), HealthStatus::Healthy);
}
