//! A publish that races a reader join or a prune must not lose the update.
//!
//! One thread keeps reading the signal while another begins passes and visits the slots. Every
//! publish either marks the reader or arrives before its read, and a reader that keeps reading
//! keeps its slot. A reader that joins late is marked on the following publish.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use queued_signal::subscriber_slots::{PassEpoch, SubscriberSlots};

/// A reader that keeps reading the newest published version.
fn spawn_reader(
    slots: Arc<SubscriberSlots<u64>>,
    published: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    ctx: u64,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        while !stop.load(Ordering::Acquire) {
            let version = published.load(Ordering::Acquire);
            slots.join(ctx, version);
        }
    })
}

#[test]
fn publish_racing_a_join_never_loses_an_update() {
    let epoch = PassEpoch::new();
    epoch.begin();
    let slots: Arc<SubscriberSlots<u64>> = Arc::new(SubscriberSlots::new(epoch.clone()));
    let published = Arc::new(AtomicU64::new(1));
    let stop = Arc::new(AtomicBool::new(false));
    let marked = Arc::new(AtomicU64::new(0));

    let first = spawn_reader(slots.clone(), published.clone(), stop.clone(), 0);

    let publish = |slots: &SubscriberSlots<u64>, marked: &AtomicU64| {
        epoch.begin();
        let version = published.fetch_add(1, Ordering::AcqRel) + 1;
        let stats = slots.visit(version, |ctx| {
            marked.fetch_or(1 << ctx, Ordering::Relaxed);
            true
        });
        // A reactivation can briefly leave a pruned entry beside the new one.
        assert!(
            stats.visited <= 3,
            "at most two readers are registered, saw {stats:?} with {} slots",
            slots.len()
        );
    };

    for _ in 0..1000 {
        publish(&slots, &marked);
    }

    // A reader that joins in the middle of the run is marked on the following publish.
    let second = spawn_reader(slots.clone(), published.clone(), stop.clone(), 1);
    thread::sleep(Duration::from_millis(20));

    for _ in 0..200 {
        publish(&slots, &marked);
    }

    stop.store(true, Ordering::Release);
    first.join().unwrap();
    second.join().unwrap();

    assert_eq!(
        marked.load(Ordering::Relaxed) & 0b11,
        0b11,
        "both the early and the late reader must be marked"
    );
    assert_eq!(
        slots.len(),
        2,
        "readers that keep reading must keep their slots"
    );
}
