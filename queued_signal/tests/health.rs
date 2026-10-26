//! Health watchdog semantics for snapshot pinning.
//!
//! Values cached across frames must not register a reader, and readers that
//! keep accessing their snapshot must not be reported as stalled.

use queued_signal::state::{HealthStatus, WriterDriver};
use std::time::{Duration, Instant};

#[derive(Clone, Debug, PartialEq)]
struct Counter {
    value: i32,
}

#[test]
fn cached_projection_keeps_health_healthy_between_publishes() {
    let mut driver = WriterDriver::new(Counter { value: 0 });
    driver.watchdog_timeout = Duration::from_millis(50);
    let state = driver.queued_state.clone();

    let publish_every = Duration::from_millis(100);
    let mut value = 0;
    let mut cached = state.read_cached(|counter| &counter.value);
    assert_eq!(*cached, 0);

    let start = Instant::now();
    let mut last_publish = Instant::now();
    while start.elapsed() < Duration::from_millis(350) {
        if last_publish.elapsed() >= publish_every {
            value += 1;
            driver.publish_value(Counter { value });
            last_publish = Instant::now();
            cached = state.read_cached(|counter| &counter.value);
        }
        driver.update_health();
        assert_eq!(
            driver.queued_state.health(),
            HealthStatus::Healthy,
            "a cached projection must not be counted as a reader"
        );
        std::thread::sleep(Duration::from_millis(16));
    }
    assert_eq!(*cached, value);
}

#[test]
fn cached_projection_pins_without_registering() {
    let mut driver = WriterDriver::new(Counter { value: 0 });
    // Any registered reader would be reported immediately with a zero timeout.
    driver.watchdog_timeout = Duration::ZERO;
    let state = driver.queued_state.clone();

    let cached = state.read_cached(|counter| &counter.value);
    driver.publish_value(Counter { value: 7 });

    // The cached guard still pins the generation it was taken from.
    assert_eq!(*cached, 0);
    driver.update_health();
    assert_eq!(driver.queued_state.health(), HealthStatus::Healthy);

    drop(cached);
    let refreshed = state.read_cached(|counter| &counter.value);
    assert_eq!(*refreshed, 7);
}

#[test]
fn tracked_reader_without_access_reports_degraded() {
    let mut driver = WriterDriver::new(Counter { value: 0 });
    driver.watchdog_timeout = Duration::from_millis(20);
    let state = driver.queued_state.clone();

    let _held = state.read();
    std::thread::sleep(Duration::from_millis(80));

    driver.update_health();
    assert_eq!(
        driver.queued_state.health(),
        HealthStatus::Degraded { pinned_buffers: 1 }
    );
}

#[test]
fn tracked_reader_access_refreshes_the_stall_timer() {
    let mut driver = WriterDriver::new(Counter { value: 0 });
    driver.watchdog_timeout = Duration::from_millis(40);
    let state = driver.queued_state.clone();

    let held = state.read();
    let start = Instant::now();
    while start.elapsed() < Duration::from_millis(260) {
        assert_eq!(held.value, 0);
        driver.update_health();
        assert_eq!(
            driver.queued_state.health(),
            HealthStatus::Healthy,
            "a reader that keeps accessing its snapshot is making progress"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn stalled_readers_are_counted_after_access_stops() {
    let mut driver = WriterDriver::new(Counter { value: 0 });
    driver.watchdog_timeout = Duration::from_millis(20);
    let state = driver.queued_state.clone();

    let first = state.read();
    let second = state.read();
    assert_eq!(first.value, 0);
    assert_eq!(second.value, 0);
    std::thread::sleep(Duration::from_millis(80));

    driver.update_health();
    assert_eq!(
        driver.queued_state.health(),
        HealthStatus::Stalled { pinned_buffers: 2 }
    );
}
