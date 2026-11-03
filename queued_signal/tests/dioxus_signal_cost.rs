//! Native dioxus signal costs beside the queued read and write paths.
//!
//! The native signal runs inside a live `VirtualDom`, so its scope is subscribed and a write
//! takes the full notify path. Each row is a median of batch averages on one thread.

use dioxus::prelude::*;
use dioxus_core::{VNode, VirtualDom};
use queued_signal::state::WriterDriver;
use std::cell::RefCell;
use std::hint::black_box;
use std::time::Instant;

/// Number of timed batches per row.
const SAMPLES: usize = 100;
/// Operations per batch.
const BATCH: u64 = 500;

/// Copy type carried by the queued driver.
#[derive(Clone, Copy, Debug)]
struct Counter(u64);

thread_local! {
    /// Signals created by the harness scope, handed back to the test thread.
    static SIGNALS: RefCell<Option<(Signal<u64>, Signal<u64>)>> = const { RefCell::new(None) };
}

/// Creates one signal this scope reads and one it never reads.
fn harness() -> Element {
    let watched = use_hook(|| Signal::new(0u64));
    let quiet = use_hook(|| Signal::new(0u64));
    // Reading here subscribes this scope, so a write to `watched` has to notify it.
    black_box(*watched.read());
    SIGNALS.with(|slot| *slot.borrow_mut() = Some((watched, quiet)));
    VNode::empty()
}

fn median(mut f: impl FnMut()) -> f64 {
    for _ in 0..BATCH {
        f();
    }
    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let start = Instant::now();
        for _ in 0..BATCH {
            f();
        }
        samples.push(start.elapsed().as_nanos() as f64 / BATCH as f64);
    }
    samples.sort_by(|a, b| a.total_cmp(b));
    samples[samples.len() / 2]
}

#[cfg_attr(miri, ignore = "measures wall clock time")]
#[test]
fn native_signal_costs_beside_the_queued_path() {
    let mut dom = VirtualDom::new(harness);
    dom.rebuild_in_place();
    let (mut watched, mut quiet) = SIGNALS
        .with(|slot| slot.borrow_mut().take())
        .expect("the harness scope created the signals");

    let read = median(|| {
        black_box(*watched.read());
    });
    let peek = median(|| {
        black_box(*watched.peek());
    });
    let write_quiet = median(|| {
        *quiet.write() = black_box(1);
    });
    let write_watched = median(|| {
        *watched.write() = black_box(1);
    });

    let mut driver = WriterDriver::<Counter>::new(Counter(0));
    let state = driver.queued_state.clone();
    let queued_read = median(|| {
        let guard = state.read();
        black_box(guard.0);
    });
    let queued_pin_and_load = median(|| {
        let guard = state.cell.read_typed();
        black_box(guard.0);
    });
    let queued_publish = median(|| driver.publish_value(Counter(black_box(1))));

    println!(
        "dioxus signal read {read:.2} ns, peek {peek:.2} ns, write with no subscriber \
{write_quiet:.2} ns, write with one subscriber {write_watched:.2} ns"
    );
    println!(
        "queued read tracked {queued_read:.2} ns, pin and load {queued_pin_and_load:.2} ns, \
publish value {queued_publish:.2} ns"
    );
    assert!(read > 0.0 && queued_read > 0.0);
}
