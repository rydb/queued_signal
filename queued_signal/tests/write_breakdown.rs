//! Isolates the parts of the write path so the store's median can be attributed.
//!
//! Each measurement runs the same loop so the numbers are comparable. The reclaimer thread starts
//! on the first retired node and stays for the rest of the test, so the later rows include its
//! interference the same way a running signal does.

use std::hint::black_box;
use std::sync::atomic::Ordering;
use std::time::Instant;

use kovan::{
    Atomic, Reclaimable, RetiredNode, Shared, flush, flush_pending, pin, retire_reclaimable,
};

/// A node shaped like the real one, so the retirement header sits first.
#[repr(C)]
struct Node {
    header: RetiredNode,
    value: u64,
}

impl Node {
    fn boxed(value: u64) -> *mut Self {
        Box::into_raw(Box::new(Self {
            header: RetiredNode::new(),
            value,
        }))
    }
}

unsafe impl Reclaimable for Node {
    fn retired_node(&self) -> &RetiredNode {
        &self.header
    }

    fn retired_node_mut(&mut self) -> &mut RetiredNode {
        &mut self.header
    }

    unsafe fn dealloc(ptr: *mut Self) {
        // SAFETY: kovan hands over the allocation it was given, once.
        drop(unsafe { Box::from_raw(ptr) });
    }
}

const ITERATIONS: u32 = 100_000;

fn ns_per<F: FnMut()>(mut f: F) -> f64 {
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        f();
    }
    start.elapsed().as_nanos() as f64 / ITERATIONS as f64
}

#[cfg_attr(miri, ignore = "measures wall clock time")]
#[test]
fn write_breakdown() {
    // 1. Pin and drop while the global epoch is still, before anything has been retired.
    let pin_still = ns_per(|| {
        let guard = pin();
        drop(guard);
    });

    // 2. Pin and drop after a flush has advanced the epoch.
    flush();
    let pin_moved = ns_per(|| {
        let guard = pin();
        drop(guard);
    });

    // 3. A raw swap with a held guard, so no pin and no retirement are involved.
    let first = Node::boxed(1);
    let second = Node::boxed(2);
    let slot: Atomic<u64> = Atomic::new(first as *mut u64);
    let swap = {
        let guard = pin();
        let mut current = unsafe { Shared::from_raw(first as *mut u64) };
        ns_per(|| {
            let next_raw = if current.as_raw() == first as *mut u64 {
                second as *mut u64
            } else {
                first as *mut u64
            };
            // SAFETY: both nodes are live for the whole measurement.
            let next = unsafe { Shared::from_raw(next_raw) };
            current = black_box(slot.swap(next, Ordering::AcqRel, &guard));
        })
    };

    // 4. A pin per swap, which is what the atom does.
    let swap_with_pin = ns_per(|| {
        let guard = pin();
        // SAFETY: the node is live for the whole measurement.
        let node = unsafe { Shared::from_raw(first as *mut u64) };
        black_box(slot.swap(node, Ordering::AcqRel, &guard));
    });

    // 5. Allocating and freeing a node with no retirement at all.
    let allocate = ns_per(|| {
        let node = Node::boxed(black_box(3));
        // SAFETY: the node was just allocated above and is never published.
        drop(unsafe { Box::from_raw(node) });
    });

    // 6. Allocating a node and retiring it, which is what a store does.
    let allocate_and_retire = ns_per(|| {
        let node = Node::boxed(black_box(4));
        // SAFETY: the node was just allocated and is retired once.
        unsafe { retire_reclaimable(node) };
    });

    // 7. The check the write path makes after every publish.
    let flush_empty = ns_per(|| {
        flush_pending();
    });

    // 8. A full flush, which happens once a batch is submitted.
    let flush_full = ns_per(|| {
        flush();
    });

    // 9. Freeing a node on the thread that allocated it, and on another thread. The real cell
    // allocates on the writer and frees on the receiver, so this is the difference to isolate.
    let owned: Vec<*mut Node> = (0..4096).map(|_| Node::boxed(7)).collect();
    let free_same_thread = {
        let start = Instant::now();
        for node in owned {
            // SAFETY: each node was allocated above and freed once here.
            drop(unsafe { Box::from_raw(node) });
        }
        start.elapsed().as_nanos() as f64 / 4096.0
    };

    let handed: Vec<usize> = (0..4096).map(|_| Node::boxed(8) as usize).collect();
    let free_other_thread = std::thread::scope(|scope| {
        let handle = scope.spawn(|| {
            let start = Instant::now();
            for node in &handed {
                // SAFETY: each node was allocated on the parent thread and freed once here.
                drop(unsafe { Box::from_raw(*node as *mut Node) });
            }
            start.elapsed().as_nanos() as f64 / 4096.0
        });
        handle.join().unwrap()
    });

    // 10. The real value cell, for reference.
    let cell = queued_signal::atom_coerce::AtomCoerce::<u64>::new(0);
    let mut value = 0u64;
    let store = ns_per(|| {
        value += 1;
        cell.store(black_box(value));
    });

    println!("pin, epoch still      {pin_still:>8.2} ns");
    println!("pin, epoch moved      {pin_moved:>8.2} ns");
    println!("swap, guard held      {swap:>8.2} ns");
    println!("swap, pin per call    {swap_with_pin:>8.2} ns");
    println!("allocate a node       {allocate:>8.2} ns");
    println!("allocate and retire   {allocate_and_retire:>8.2} ns");
    println!("flush_pending, empty  {flush_empty:>8.2} ns");
    println!("flush, full           {flush_full:>8.2} ns");
    println!("free same thread      {free_same_thread:>8.2} ns");
    println!("free other thread     {free_other_thread:>8.2} ns");
    println!("cell store            {store:>8.2} ns");
}
