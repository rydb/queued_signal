//! Frame resolve attribution for the queued signal.
//!
//! Splits the E5 resolver frame into the calls a driver makes, so a change can aim at the part that
//! costs. This runs on one thread, so the store does not compete with a writer that is submitting.

use std::hint::black_box;
use std::time::Duration;

use queued_signal::state::{QueuedSignal, QueuedState, WriterDriver};
use queued_signal::write_queues::WriteQueues;
use queued_signal_benches::stats::{self, Sampling};

/// Signals on the surface, matching the largest E5 row.
const SIGNALS: usize = 2_000;
/// Timed frames per part.
const SAMPLES: usize = 200;

/// One surface, with the handles the writer and the resolver each hold.
struct Surface {
    drivers: Vec<WriterDriver<u64>>,
    signals: Vec<QueuedSignal<u64>>,
    states: Vec<QueuedState<u64>>,
    senders: Vec<WriteQueues<u64>>,
}

fn build() -> Surface {
    let mut drivers = Vec::with_capacity(SIGNALS);
    let mut signals = Vec::with_capacity(SIGNALS);
    let mut states = Vec::with_capacity(SIGNALS);
    let mut senders = Vec::with_capacity(SIGNALS);
    for _ in 0..SIGNALS {
        let driver = WriterDriver::<u64>::new(0);
        let state = driver.queued_state.clone();
        let op_tx = driver.op_tx.clone();
        signals.push(QueuedSignal::from_parts(state.clone(), None, op_tx));
        states.push(state);
        senders.push(driver.op_tx.clone());
        drivers.push(driver);
    }
    Surface {
        drivers,
        signals,
        states,
        senders,
    }
}

fn main() {
    let Surface {
        mut drivers,
        signals,
        states,
        senders,
    } = build();
    let sampling = Sampling {
        batch: 1,
        samples: SAMPLES,
        warmup_batches: 20,
    };

    let walk = stats::measure(sampling, || {
        for driver in drivers.iter_mut() {
            black_box(&mut *driver);
        }
    })
    .p50;

    let submit = stats::measure(sampling, || {
        for signal in signals.iter() {
            signal.set_value(black_box(7));
        }
    })
    .p50;

    let tick_idle = stats::measure(sampling, || {
        for driver in drivers.iter_mut() {
            driver.tick(Duration::ZERO);
        }
    })
    .p50;

    let submit_and_tick = stats::measure(sampling, || {
        for signal in signals.iter() {
            signal.set_value(black_box(7));
        }
        for driver in drivers.iter_mut() {
            driver.tick(Duration::ZERO);
        }
    })
    .p50;

    let tick_idle_batch = stats::measure(sampling, || {
        WriterDriver::tick_batch(&mut drivers, Duration::ZERO);
    })
    .p50;

    let submit_and_tick_batch = stats::measure(sampling, || {
        for signal in signals.iter() {
            signal.set_value(black_box(7));
        }
        WriterDriver::tick_batch(&mut drivers, Duration::ZERO);
    })
    .p50;

    let submit_and_drain = stats::measure(sampling, || {
        for signal in signals.iter() {
            signal.set_value(black_box(7));
        }
        for driver in drivers.iter_mut() {
            black_box(driver.drain_ops());
        }
    })
    .p50;

    let publish = stats::measure(sampling, || {
        for driver in drivers.iter_mut() {
            driver.publish();
        }
    })
    .p50;

    let publish_value = stats::measure(sampling, || {
        for driver in drivers.iter_mut() {
            driver.publish_value(black_box(7));
        }
    })
    .p50;

    let arc_alloc = stats::measure(sampling, || {
        for _ in 0..SIGNALS {
            black_box(std::sync::Arc::new(black_box(7u64)));
        }
    })
    .p50;

    let probe = stats::measure(sampling, || {
        for sender in senders.iter() {
            black_box(sender.queue_count());
        }
    })
    .p50;

    let cell_store = stats::measure(sampling, || {
        for state in states.iter() {
            state.cell.store(black_box(7));
        }
    })
    .p50;

    let version_store = stats::measure(sampling, || {
        for state in states.iter() {
            state.atomics.store_version(black_box(7));
        }
    })
    .p50;

    let tick_busy = submit_and_tick - submit;
    let tick_idle_batch_ns = tick_idle_batch;
    let tick_busy_batch = submit_and_tick_batch - submit;
    let drain = submit_and_drain - submit;
    let store = publish_value - publish;
    let rest = tick_busy - drain - publish_value;

    let parts = [
        ("walk-empty", walk),
        ("submit-all", submit),
        ("tick-idle", tick_idle),
        ("tick-idle-batch", tick_idle_batch_ns),
        ("tick-busy", tick_busy),
        ("tick-busy-batch", tick_busy_batch),
        ("arc-alloc", arc_alloc),
        ("queue-count", probe),
        ("cell-store", cell_store),
        ("version-store", version_store),
        ("drain-ops", drain),
        ("publish", publish),
        ("publish-value", publish_value),
        ("store", store),
        ("rest-of-tick", rest),
    ];

    println!("{:>14} {:>14} {:>12}", "part", "ns/frame", "ns/signal");
    for (name, ns) in parts {
        println!("{name:>14} {:>14.1} {:>12.2}", ns, ns / SIGNALS as f64);
    }

    let per_signal = |ns: f64| ns / SIGNALS as f64;
    println!(
        "\ntick-busy is {:.2} ns per signal, of which drain-ops {:.2}, store {:.2}, and the rest {:.2}",
        per_signal(tick_busy),
        per_signal(drain),
        per_signal(store),
        per_signal(rest)
    );
    println!(
        "an idle frame is {:.2} ns per signal, so the work a queued op adds is {:.2} ns",
        per_signal(tick_idle),
        per_signal(tick_busy - tick_idle)
    );
}
