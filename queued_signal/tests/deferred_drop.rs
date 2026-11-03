//! Deferred destruction and reclamation counters.
//!
//! The drainer queue is process wide, so this file keeps every check in one
//! test function and never runs two drains in parallel.

use queued_signal::atom_coerce::{AtomCoerce, drain_deferred_frees, flush_reclamation};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A value that counts how many times it was dropped.
struct DropCounter(Arc<AtomicUsize>);

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn deferred_drop_suite() {
    held_guard_keeps_the_replaced_value_alive();
    drain_runs_the_destructor_once();
    reclaim_health_tracks_retained_generations();
}

/// A guarded generation survives reclamation cycles and is dropped once after release.
fn held_guard_keeps_the_replaced_value_alive() {
    let drops = Arc::new(AtomicUsize::new(0));
    let cell = AtomCoerce::<DropCounter>::new(DropCounter(drops.clone()));
    let guard = cell.read_typed();
    cell.store(DropCounter(drops.clone()));
    std::hint::black_box(&*guard);

    for _ in 0..50 {
        flush_reclamation();
        if drops.load(Ordering::SeqCst) != 0 {
            break;
        }
        std::thread::yield_now();
    }
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "a pinned generation must not be dropped"
    );

    drop(guard);
    for _ in 0..10_000 {
        flush_reclamation();
        if drops.load(Ordering::SeqCst) == 1 {
            break;
        }
        std::thread::yield_now();
    }
    assert_eq!(drops.load(Ordering::SeqCst), 1, "release drops it once");
}

/// The drainer runs each queued destructor exactly once.
fn drain_runs_the_destructor_once() {
    let drops = Arc::new(AtomicUsize::new(0));
    let cell = AtomCoerce::<DropCounter>::new(DropCounter(drops.clone()));
    cell.store(DropCounter(drops.clone()));

    // Reclamation runs on the receiver thread, so yield while it catches up.
    for _ in 0..10_000 {
        flush_reclamation();
        drain_deferred_frees();
        if drops.load(Ordering::SeqCst) == 1 {
            break;
        }
        std::thread::yield_now();
    }
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    drain_deferred_frees();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

/// Retention counts the published generations still awaiting release.
fn reclaim_health_tracks_retained_generations() {
    let cell = AtomCoerce::<u32>::new(1);
    assert_eq!(cell.reclaim_health().retained(), 0);

    cell.store(2);
    assert_eq!(cell.reclaim_health().retained(), 1);

    flush_reclamation();
    drain_deferred_frees();
    assert_eq!(cell.reclaim_health().retained(), 0);
}
