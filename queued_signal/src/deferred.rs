//! Deferred destruction and asynchronous reclamation.
//!
//! A replaced node is retired on the writer thread and collected by a receiver thread. Kovan hands
//! the node to the slot of the thread that flushes, so the receiver, which never holds a read
//! guard, can always drain it. The queued destructors run there too, never on a reader and never
//! on the writer.
//!
//! The queue is a lock free stack of headers embedded in the nodes themselves, so queueing costs one
//! compare and exchange rather than a channel send.

use std::ptr;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use kovan::{flush, pin};

/// Destructor that owns the whole allocation.
pub(crate) type DropFn = unsafe fn(*mut ());

/// Link and destructor for one queued allocation.
///
/// A node embeds this header, so queueing it allocates nothing and the queue cannot grow a backing
/// store of its own.
#[repr(C)]
pub(crate) struct DeferredHeader {
    next: *mut DeferredHeader,
    node: *mut (),
    drop_fn: DropFn,
}

impl DeferredHeader {
    /// Prepares an embedded header for the allocation it belongs to.
    pub(crate) fn new() -> Self {
        Self {
            next: ptr::null_mut(),
            node: ptr::null_mut(),
            drop_fn: noop,
        }
    }

    /// Points the header at its allocation and at the destructor that frees it.
    pub(crate) fn prepare(&mut self, node: *mut (), drop_fn: DropFn) {
        self.node = node;
        self.drop_fn = drop_fn;
    }

    /// Link while the node sits in a per atom pool.
    ///
    /// A node is either queued here or pooled there, so the two links never hold a value at the same
    /// time and one word serves both.
    pub(crate) fn link(&self) -> *mut () {
        self.next as *mut ()
    }

    /// Sets the pool link.
    pub(crate) fn set_link(&mut self, next: *mut ()) {
        self.next = next as *mut DeferredHeader;
    }
}

/// Placeholder destructor for a header that was never queued.
unsafe fn noop(_: *mut ()) {}

/// Number of handing off queues.
///
/// Each thread hands its work to its own queue, so no two threads ever exchange the same head. The
/// receiver walks all of them, which is a fixed cost per drain rather than contention per release.
const QUEUES: usize = 64;

/// One queue head, padded to its own lines.
///
/// The owning thread compares and exchanges it on every released node, while the receiver swaps it
/// once per drain, so sharing a line with another head would cost both sides a coherence miss. The
/// 128 byte width also covers the adjacent line prefetch on x86-64.
#[repr(align(128))]
struct PaddedHead(AtomicPtr<DeferredHeader>);

impl PaddedHead {
    const fn new() -> Self {
        Self(AtomicPtr::new(ptr::null_mut()))
    }
}

/// One queue per handing off thread.
static QUEUE_HEADS: [PaddedHead; QUEUES] = [const { PaddedHead::new() }; QUEUES];

/// Hands out queue indices, one per thread.
static NEXT_QUEUE: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    /// The queue this thread hands work to.
    static MY_QUEUE: usize = NEXT_QUEUE.fetch_add(1, Ordering::Relaxed) % QUEUES;
}

/// Queues an allocation whose destructor must not run on the reclaimer.
pub(crate) fn defer(header: &mut DeferredHeader) {
    let index = MY_QUEUE.with(|index| *index);
    let head = &QUEUE_HEADS[index].0;
    let mut current = head.load(Ordering::Relaxed);
    loop {
        header.next = current;
        match head.compare_exchange_weak(current, header, Ordering::Release, Ordering::Relaxed) {
            Ok(_) => return,
            Err(seen) => current = seen,
        }
    }
}

/// Runs every queued destructor and returns how many ran.
pub fn drain() -> usize {
    let mut drained = 0;
    for queue in QUEUE_HEADS.iter() {
        walk(queue, &mut drained);
    }
    drained
}

/// Empties one queue and runs its destructors.
fn walk(queue: &PaddedHead, drained: &mut usize) {
    let mut head = queue.0.swap(ptr::null_mut(), Ordering::AcqRel);
    while !head.is_null() {
        let mut current = head;
        while !current.is_null() {
            // SAFETY: every header on this list was published by `defer` and the list is ours now.
            let (next, node, drop_fn) =
                unsafe { ((*current).next, (*current).node, (*current).drop_fn) };
            // The destructor owns the allocation from here, so nothing below reads the header.
            unsafe { drop_fn(node) };
            *drained += 1;
            current = next;
        }
        head = queue.0.swap(ptr::null_mut(), Ordering::AcqRel);
    }
}

/// How long an idle receiver waits between drains.
const IDLE: Duration = Duration::from_millis(1);

fn start_receiver() {
    thread::Builder::new()
        .name("queued-signal-reclaimer".to_owned())
        .spawn(|| {
            loop {
                let drained = drain();
                // A pin publishes the epoch and a drop ends the section, which is the state a
                // collection needs.
                let guard = pin();
                drop(guard);
                flush();
                drain();
                // A drain that found work belongs to a burst, so the next pass follows at once.
                // Only an empty queue waits out the idle slice.
                if drained == 0 {
                    thread::sleep(IDLE);
                }
            }
        })
        .expect("spawn the reclamation receiver");
}

// TODO: stop and join the receiver when the last signal is dropped, and start it again on the
// next retirement. Miri then keeps its leak check, since no thread is alive when the harness ends.
/// Ensures the receiver thread runs.
pub(crate) fn wake() {
    static STARTED: OnceLock<()> = OnceLock::new();
    STARTED.get_or_init(start_receiver);
}
