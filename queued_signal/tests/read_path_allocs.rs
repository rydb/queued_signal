//! Allocation checks for the wait free read path.
//!
//! The measuring dependency wraps the global allocator of this test binary and counts
//! allocations on the calling thread. A read pins the current generation and loads it, so each
//! measured loop reports zero allocations.

use std::any::Any;
use std::hint::black_box;

use allocation_counter::measure;
use queued_signal::atom_coerce::flush_reclamation;
use queued_signal::atom_coerce_dyn::AtomCoerceDyn;
use queued_signal::state::WriterDriver;

#[test]
fn read_path_does_not_allocate() {
    let mut driver = WriterDriver::new(1u32);
    let state = driver.queued_state.clone();
    let erased = AtomCoerceDyn::<dyn Any>::new(7u32);
    let handle = erased.handle_dyn();

    // Warm up the thread local reclamation state, the erased slot, and the thread slot.
    for _ in 0..64 {
        black_box(*state.read());
        black_box(handle.get().as_dyn() as *const dyn Any);
        driver.publish_value(1);
    }
    flush_reclamation();
    queued_signal::atom_coerce::drain_deferred_frees();

    let tracked = measure(|| {
        for _ in 0..256 {
            let guard = state.read();
            black_box(*guard);
        }
    });
    assert_eq!(tracked.count_total, 0, "tracked read allocated");

    let owned = measure(|| {
        for _ in 0..256 {
            let guard = driver.read();
            black_box(*guard);
        }
    });
    assert_eq!(owned.count_total, 0, "owned read allocated");

    let cached = measure(|| {
        for _ in 0..256 {
            let guard = state.read_cached(|value: &u32| value);
            black_box(*guard);
        }
    });
    assert_eq!(cached.count_total, 0, "cached projection allocated");

    let erased_reads = measure(|| {
        for _ in 0..256 {
            let guard = handle.get();
            black_box(guard.as_dyn() as *const dyn Any);
        }
    });
    assert_eq!(erased_reads.count_total, 0, "erased read allocated");
}

#[test]
fn first_publishes_take_seeded_nodes() {
    let mut driver = WriterDriver::new(0u32);
    let state = driver.queued_state.clone();
    // Warm the thread local pin state, the watch send, and the reclaimer thread, and spend one of
    // the eight seeded nodes.
    drop(state.read());
    driver.publish_value(0);

    // The seven remaining seeded nodes cover these publishes without a fresh allocation.
    let measured = measure(|| {
        for value in 1..8u32 {
            driver.publish_value(value);
        }
    });
    assert_eq!(
        measured.count_total, 0,
        "the seeded nodes must cover the first publishes, allocated {}",
        measured.count_total
    );
}

#[test]
fn publish_reuses_seeded_generations() {
    let mut driver = WriterDriver::new(0u32);

    // Warm the seed, the reclaimer, and the watch channel.
    for value in 0..64u32 {
        driver.publish_value(value);
        driver.submit_pending();
        flush_reclamation();
    }

    // Reclamation returns each replaced generation to the pool, so the publishes below take a
    // seeded or returned node rather than a fresh allocation.
    let measured = measure(|| {
        for value in 0..64u32 {
            driver.publish_value(value);
            driver.submit_pending();
            flush_reclamation();
        }
    });
    assert!(
        measured.count_total <= 8,
        "64 publishes with reclamation allocated {} times",
        measured.count_total
    );
}
