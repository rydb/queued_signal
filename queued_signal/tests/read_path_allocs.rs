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
