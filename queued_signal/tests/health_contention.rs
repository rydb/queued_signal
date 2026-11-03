//! Retained generations while readers hold or refresh guards.
//!
//! A publish on a thread that holds a guard cannot complete the final reclamation step, so a
//! small window of generations stays retained. These probes pin that window down and prove it
//! closes once the guards are gone.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use queued_signal::state::{HealthStatus, WriterDriver};

/// Retained generations reported by the signal.
fn retained(driver: &WriterDriver<u32>) -> u64 {
    driver.queued_state.atomics.pinned() as u64
}

/// Drives a reclamation cycle and scans health until the count reaches zero.
fn settle_to_zero(driver: &mut WriterDriver<u32>) {
    for _ in 0..2000 {
        queued_signal::atom_coerce::flush_reclamation();
        driver.update_health();
        if retained(driver) == 0 {
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn retained_generations_stay_bounded() {
    held_guard_on_the_publishing_thread();
    refreshing_reader_thread();
    parked_reader_thread();
    frequent_health_updates();
    writer_pinned_across_many_publishes();
    render_loop_projects_without_holding_a_guard();
    publishes_await_a_submit();
}

/// A render loop that projects and drops the guard on every frame.
///
/// This is the pattern the resource mirror uses, and it must not leave a pin behind.
fn render_loop_projects_without_holding_a_guard() {
    let mut driver = WriterDriver::new(0u32);
    let state = driver.queued_state.clone();

    for value in 1..=50u32 {
        let projected = state.read_cached(|value| value);
        std::hint::black_box(*projected);
        drop(projected);
        driver.publish_value(value);
        driver.update_health();
    }

    settle_to_zero(&mut driver);
    assert_eq!(
        retained(&driver),
        0,
        "projecting per frame must not hold a pin, got {}",
        retained(&driver)
    );
    assert_eq!(state.health(), HealthStatus::Healthy);
}

/// Publishes many times while a guard stays held on the publishing thread.
fn writer_pinned_across_many_publishes() {
    let mut driver = WriterDriver::new(0u32);
    let state = driver.queued_state.clone();
    let held = state.read();

    for value in 1..=50u32 {
        driver.publish_value(value);
        driver.update_health();
    }

    for _ in 0..100_000 {
        driver.update_health();
        if retained(&driver) <= 50 {
            break;
        }
        thread::yield_now();
    }
    assert!(
        retained(&driver) <= 50,
        "the count must stay bounded by the publishes, got {}",
        retained(&driver)
    );
    // The guard's own generation cannot be released, so the signal is never Healthy while it
    // lives, even though the older generations are still reclaimed.
    assert!(
        !matches!(state.health(), HealthStatus::Healthy),
        "a writer pinned across publishes keeps a generation, got {:?}",
        state.health()
    );

    drop(held);
    settle_to_zero(&mut driver);
    assert_eq!(retained(&driver), 0);
    assert_eq!(state.health(), HealthStatus::Healthy);
}

/// A guard held on the thread that publishes and scans health, as a memo value would be.
///
/// A writer holding a guard owes one traversal per retired generation, so the count tracks the
/// publishes made while the guard is held and drops to zero once it is released.
fn held_guard_on_the_publishing_thread() {
    let mut driver = WriterDriver::new(0u32);
    let state = driver.queued_state.clone();
    let held = state.read();

    for value in 1..=5u32 {
        driver.publish_value(value);
        driver.update_health();
    }
    assert!(
        retained(&driver) <= 5,
        "a held guard retains at most one generation per publish, got {}",
        retained(&driver)
    );

    drop(held);
    settle_to_zero(&mut driver);
    assert_eq!(retained(&driver), 0);
    assert_eq!(state.health(), HealthStatus::Healthy);
}

/// A reader thread that keeps replacing its guard, as a render loop would.
fn refreshing_reader_thread() {
    let mut driver = WriterDriver::new(0u32);
    let state = driver.queued_state.clone();
    let stop = Arc::new(AtomicBool::new(false));

    let reader = {
        let state = state.clone();
        let stop = stop.clone();
        thread::spawn(move || {
            let mut guard = state.read();
            while !stop.load(Ordering::Acquire) {
                drop(guard);
                guard = state.read();
                thread::yield_now();
            }
        })
    };

    for value in 1..=50u32 {
        driver.publish_value(value);
        driver.update_health();
        thread::yield_now();
    }
    stop.store(true, Ordering::Release);
    reader.join().unwrap();
    driver.update_health();

    // The receiver drains the generations the reader's thread freed on exit.
    settle_to_zero(&mut driver);
    assert_eq!(
        retained(&driver),
        0,
        "a refreshing reader must not leave generations behind, got {}",
        retained(&driver)
    );
}

/// A reader on another thread that holds one guard through every publish.
fn parked_reader_thread() {
    let mut driver = WriterDriver::new(0u32);
    let state = driver.queued_state.clone();
    let release = Arc::new(AtomicBool::new(false));
    let parked = Arc::new(AtomicBool::new(false));

    let reader = {
        let state = state.clone();
        let release = release.clone();
        let parked = parked.clone();
        thread::spawn(move || {
            let guard = state.read();
            parked.store(true, Ordering::Release);
            while !release.load(Ordering::Acquire) {
                thread::sleep(Duration::from_millis(1));
            }
            drop(guard);
        })
    };

    while !parked.load(Ordering::Acquire) {
        thread::yield_now();
    }
    for value in 1..=50u32 {
        driver.publish_value(value);
        driver.update_health();
        thread::sleep(Duration::from_millis(1));
    }
    // The reader's slot accumulates one generation per publish while it stays parked.
    driver.update_health();
    assert!(
        retained(&driver) <= 50,
        "a parked reader must stay bounded by the publishes, got {}",
        retained(&driver)
    );
    assert_ne!(
        state.health(),
        HealthStatus::Healthy,
        "a parked reader must not report healthy"
    );

    release.store(true, Ordering::Release);
    reader.join().unwrap();
    for _ in 0..2000 {
        driver.update_health();
        if retained(&driver) == 0 {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        retained(&driver),
        0,
        "reclamation must catch up after the reader releases"
    );
    assert_eq!(state.health(), HealthStatus::Healthy);
}

/// Health scans at frame rate with publishes at a slower rate.
fn frequent_health_updates() {
    let mut driver = WriterDriver::new(0u32);

    for value in 1..=5u32 {
        driver.publish_value(value);
        for _ in 0..60 {
            driver.update_health();
        }
    }
    settle_to_zero(&mut driver);
    assert_eq!(retained(&driver), 0);
    assert_eq!(driver.queued_state.health(), HealthStatus::Healthy);
}

/// Publishes that are never submitted are counted as retained until the next submit.
///
/// The resource mirror publishes from its sync system, so without a submit the retires sit in
/// kovan's accumulating batch and the health count reports them as a backlog of their own.
fn publishes_await_a_submit() {
    let mut driver = WriterDriver::new(0u32);
    for value in 1..=12u32 {
        driver.publish_value(value);
    }
    driver.update_health();
    assert!(
        retained(&driver) >= 10,
        "unsubmitted retires must count as retained, got {}",
        retained(&driver)
    );

    driver.submit_pending();
    let mut released = false;
    for _ in 0..2000 {
        driver.update_health();
        if retained(&driver) == 0 {
            released = true;
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert!(
        released,
        "a submit must release the retires counted above, got {}",
        retained(&driver)
    );
}
