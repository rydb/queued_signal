//! Probe: a writer that holds a guard retires nodes, and a separate receiver thread frees them.
//!
//! The writer never releases its guard, so its own flush cannot drain its slot. The receiver only
//! pins, drops, and flushes, which is the role a helper thread would play.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use kovan::{Atomic, Guard, Reclaimable, RetiredNode, Shared, flush, pin, retire_reclaimable};

#[repr(C)]
struct Node {
    retired: RetiredNode,
    drops: Arc<AtomicUsize>,
}

impl Drop for Node {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

unsafe impl Reclaimable for Node {
    fn retired_node(&self) -> &RetiredNode {
        &self.retired
    }

    fn retired_node_mut(&mut self) -> &mut RetiredNode {
        &mut self.retired
    }
}

fn new_node(drops: &Arc<AtomicUsize>) -> *mut Node {
    Box::into_raw(Box::new(Node {
        retired: RetiredNode::new(),
        drops: drops.clone(),
    }))
}

/// Publishes a replacement and retires the old node, all under the caller's guard.
fn publish(atom: &Atomic<Node>, guard: &Guard, drops: &Arc<AtomicUsize>) {
    // SAFETY: the new node is a fresh allocation that is published here.
    let next = unsafe { Shared::from_raw(new_node(drops)) };
    let old = atom.swap(next, Ordering::AcqRel, guard);
    // SAFETY: the old node was unlinked and is retired exactly once.
    unsafe { retire_reclaimable(old.as_raw()) };
}

#[test]
fn a_receiver_thread_frees_nodes_retired_under_a_held_guard() {
    const ROUNDS: usize = 40;
    // Publish slowly, as an application does, so the receiver has to keep its slot eligible
    // between collections instead of coincidentally overlapping the writer.
    const PUBLISH_EVERY: Duration = Duration::from_millis(10);

    let drops = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let receiver = {
        let stop = stop.clone();
        thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                // Hold a published epoch for most of the cycle.
                {
                    let guard = pin();
                    thread::sleep(Duration::from_millis(5));
                    drop(guard);
                }
                for _ in 0..2 {
                    let guard = pin();
                    drop(guard);
                    flush();
                }
                thread::sleep(Duration::from_millis(1));
            }
        })
    };

    let writer = {
        let drops = drops.clone();
        thread::spawn(move || {
            let atom = Atomic::new(new_node(&drops));
            // The long lived guard stands in for a memo value held across frames.
            let held = pin();
            for _ in 0..ROUNDS {
                publish(&atom, &held, &drops);
                // The writer submits its batch but cannot drain its own slot while pinned.
                flush();
                thread::sleep(PUBLISH_EVERY);
            }
            drop(held);
            // The atom owns the last node; leaking the box keeps the drop count exact.
            let _ = Box::leak(Box::new(atom));
        })
    };

    writer.join().unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    while drops.load(Ordering::SeqCst) < ROUNDS && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }
    stop.store(true, Ordering::Release);
    receiver.join().unwrap();

    let dropped = drops.load(Ordering::SeqCst);
    println!("receiver probe dropped {dropped} of {ROUNDS}");
    assert_eq!(
        dropped, ROUNDS,
        "every node retired under a held guard must be freed by the receiver thread"
    );
}
