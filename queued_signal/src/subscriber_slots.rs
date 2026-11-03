//! Subscriber slots for trackable reactive contexts.
//!
//! A driver begins one pass per frame and then visits the slots after each publish.
//! A context that stays without a read for more than two passes is pruned, so its slot stops
//! causing renders.
//!
//! TODO(option 4): a `Copy` subscriber handle resolved by `clear_subscribers`
//! would replace the pass epoch and the prune. It needs the subscriber set to
//! keep its allocation across passes and slots to stay reserved. Until then the
//! forwarder prunes a slot whose context skipped a pass.
//!
//! The array grows by prepending 16 slot blocks. An entry is initialized once
//! and never overwritten, so a reader can copy its context without a data race.
//! Pruning flips the entry to EMPTY, and the same context can reactivate it.

use std::array;
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};

const UNINIT: u64 = 0;
const WRITING: u64 = 1;
const EMPTY: u64 = 2;
const OCCUPIED: u64 = 3;
const PHASE_MASK: u64 = 0b11;
const GEN_SHIFT: u32 = 2;

/// Slots per block.
pub const BLOCK_SLOTS: usize = 16;

/// Passes without a read before a slot is pruned.
///
/// A publish begins a pass and then visits the slots, so a reader that read during the
/// previous pass must still receive the mark. Two passes of slack keep a reader alive
/// while its render rate stays above a third of the pass rate.
pub const PRUNE_AFTER_PASSES: u64 = 2;

const fn state_word(generation: u64, phase: u64) -> u64 {
    (generation << GEN_SHIFT) | phase
}

const fn phase(state: u64) -> u64 {
    state & PHASE_MASK
}

const fn generation(state: u64) -> u64 {
    state >> GEN_SHIFT
}

/// Monotonic render pass counter shared by every slot array in an app.
#[derive(Clone)]
pub struct PassEpoch {
    counter: Arc<AtomicU64>,
}

impl PassEpoch {
    /// Creates a counter at zero.
    pub fn new() -> Self {
        Self {
            counter: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Starts a new pass and returns its number.
    ///
    /// Call this once per frame, before visiting the slots.
    pub fn begin(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// The current pass number.
    pub fn current(&self) -> u64 {
        self.counter.load(Ordering::Acquire)
    }
}

impl Default for PassEpoch {
    fn default() -> Self {
        Self::new()
    }
}

/// One entry in a slot block.
#[repr(align(64))]
struct Slot<T> {
    state: AtomicU64,
    epoch: AtomicU64,
    version: AtomicU64,
    ctx: UnsafeCell<MaybeUninit<T>>,
}

impl<T> Slot<T> {
    const fn new() -> Self {
        Self {
            state: AtomicU64::new(state_word(0, UNINIT)),
            epoch: AtomicU64::new(0),
            version: AtomicU64::new(0),
            ctx: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }

    /// Claims a never used entry and publishes it as occupied.
    fn init(&self, ctx: T, pass: u64, version: u64) -> bool {
        let state = self.state.load(Ordering::Acquire);
        if phase(state) != UNINIT {
            return false;
        }
        let writing = state_word(generation(state), WRITING);
        if self
            .state
            .compare_exchange(state, writing, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        // SAFETY: this entry is WRITING, so no other thread reads the context.
        unsafe { self.ctx.get().write(MaybeUninit::new(ctx)) };
        self.epoch.store(pass, Ordering::Release);
        self.version.store(version, Ordering::Release);
        self.state
            .store(state_word(generation(state), OCCUPIED), Ordering::Release);
        true
    }

    /// Refreshes an entry owned by `ctx`, or reactivates a pruned one.
    fn touch(&self, ctx: &T, pass: u64, version: u64) -> Touch
    where
        T: Copy + PartialEq,
    {
        let before = self.state.load(Ordering::Acquire);
        match phase(before) {
            UNINIT => return Touch::Vacant,
            WRITING => return Touch::Busy,
            _ => {}
        }
        // SAFETY: the entry was initialized before it left UNINIT, and the
        // context is never overwritten afterwards.
        let stored = unsafe { (*self.ctx.get()).assume_init_ref() };
        let after = self.state.load(Ordering::Acquire);
        if after != before {
            return Touch::Busy;
        }
        if stored != ctx {
            return Touch::Other;
        }
        // Publish the stamp before the generation bump, so a forwarder that
        // observes the new generation also observes the new stamp.
        self.epoch.store(pass, Ordering::Release);
        self.version.store(version, Ordering::Release);
        let next = state_word(generation(before) + 1, OCCUPIED);
        if self
            .state
            .compare_exchange(before, next, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            Touch::Updated
        } else {
            Touch::Busy
        }
    }

    /// Turns an occupied entry into a pruned one.
    fn prune(&self, expected: u64) -> bool {
        let next = state_word(generation(expected) + 1, EMPTY);
        self.state
            .compare_exchange(expected, next, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}

enum Touch {
    Updated,
    Vacant,
    Other,
    Busy,
}

struct Block<T> {
    slots: [Slot<T>; BLOCK_SLOTS],
    next: *mut Block<T>,
}

// SAFETY: access to the slots is governed by the state word, and a context is
// written once before its entry becomes visible.
unsafe impl<T: Send> Send for Block<T> {}
unsafe impl<T: Send> Sync for Block<T> {}

impl<T> Block<T> {
    fn new() -> *mut Self {
        Box::into_raw(Box::new(Self {
            slots: array::from_fn(|_| Slot::new()),
            next: ptr::null_mut(),
        }))
    }
}

struct Inner<T> {
    head: AtomicPtr<Block<T>>,
}

impl<T> Inner<T> {
    fn new() -> Self {
        Self {
            head: AtomicPtr::new(ptr::null_mut()),
        }
    }

    fn grow(&self) {
        let block = Block::new();
        let mut head = self.head.load(Ordering::Acquire);
        loop {
            // SAFETY: the block is unpublished until the CAS below.
            unsafe { (*block).next = head };
            match self
                .head
                .compare_exchange(head, block, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return,
                Err(actual) => head = actual,
            }
        }
    }
}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        let mut block = self.head.load(Ordering::Acquire);
        while !block.is_null() {
            // SAFETY: the last reference to the inner state is going away, so
            // no reader or forwarder can reach these blocks.
            let next = unsafe { (*block).next };
            unsafe { drop(Box::from_raw(block)) };
            block = next;
        }
    }
}

/// Slot array that tracks which reactive contexts read a signal in each pass.
pub struct SubscriberSlots<T> {
    inner: Arc<Inner<T>>,
    epoch: PassEpoch,
}

impl<T> Clone for SubscriberSlots<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            epoch: self.epoch.clone(),
        }
    }
}

/// Counters from one forwarder visit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct VisitStats {
    /// Occupied slots examined.
    pub visited: usize,
    /// Slots whose context was marked dirty.
    pub marked: usize,
    /// Slots pruned because the context skipped the pass.
    pub pruned: usize,
    /// Slots skipped because the context already read the published version.
    pub skipped: usize,
    /// Marks that failed because the context was busy, retried at the next publish.
    pub dropped: usize,
}

impl<T> SubscriberSlots<T> {
    /// Creates an empty array sharing the given pass counter.
    pub fn new(epoch: PassEpoch) -> Self {
        Self {
            inner: Arc::new(Inner::new()),
            epoch,
        }
    }

    /// The shared pass counter.
    pub fn epoch(&self) -> &PassEpoch {
        &self.epoch
    }

    /// Starts a new pass and returns its number.
    ///
    /// Call this once per frame. Every context that does not read until the next pass is pruned.
    pub fn begin_pass(&self) -> u64 {
        self.epoch.begin()
    }
}

impl<T: Copy + PartialEq + Send> SubscriberSlots<T> {
    /// Records that `ctx` read `version` in the current pass.
    ///
    /// The first call for a context claims an entry. A pruned context
    /// reactivates the same entry, so no slot is consumed by a passing skip.
    pub fn join(&self, ctx: T, version: u64) {
        let pass = self.epoch.current();
        loop {
            let mut vacant: Option<*const Slot<T>> = None;
            let mut unstable = false;
            let mut block = self.inner.head.load(Ordering::Acquire);
            while !block.is_null() {
                // SAFETY: blocks live as long as the inner state.
                let slots = unsafe { &(*block).slots };
                for slot in slots {
                    match slot.touch(&ctx, pass, version) {
                        Touch::Updated => return,
                        Touch::Vacant => vacant = Some(slot as *const Slot<T>),
                        Touch::Busy => unstable = true,
                        Touch::Other => {}
                    }
                }
                // SAFETY: as above.
                block = unsafe { (*block).next };
            }
            if unstable {
                // A prune can move a slot between the two state loads `touch` makes, which hides
                // this context's own entry for that scan. Claiming a vacant slot here would give
                // the context a second one, so the scan repeats against the settled state, where
                // the entry is empty and reactivates.
                continue;
            }
            if let Some(slot) = vacant {
                // SAFETY: the pointer comes from a live block.
                if unsafe { &*slot }.init(ctx, pass, version) {
                    return;
                }
                continue;
            }
            self.inner.grow();
        }
    }

    /// Removes a context from the array.
    ///
    /// Returns true when an occupied entry was deactivated.
    pub fn remove(&self, ctx: &T) -> bool {
        let mut block = self.inner.head.load(Ordering::Acquire);
        while !block.is_null() {
            // SAFETY: blocks live as long as the inner state.
            let slots = unsafe { &(*block).slots };
            for slot in slots {
                loop {
                    let state = slot.state.load(Ordering::Acquire);
                    if phase(state) != OCCUPIED {
                        break;
                    }
                    // SAFETY: an occupied entry has an initialized context.
                    let stored = unsafe { (*slot.ctx.get()).assume_init_ref() };
                    let after = slot.state.load(Ordering::Acquire);
                    if after != state {
                        continue;
                    }
                    if stored != ctx {
                        break;
                    }
                    if slot.prune(state) {
                        return true;
                    }
                }
            }
            // SAFETY: as above.
            block = unsafe { (*block).next };
        }
        false
    }

    /// Marks every live context whose version is behind `published`.
    ///
    /// Slots whose context skipped more than [`PRUNE_AFTER_PASSES`] passes are pruned instead.
    /// A mark that fails leaves the slot subscribed, because a busy context is not a dropped one,
    /// and the next publish marks it again.
    pub fn visit(&self, published: u64, mut mark: impl FnMut(T) -> bool) -> VisitStats {
        let pass = self.epoch.current();
        let mut stats = VisitStats::default();
        let mut block = self.inner.head.load(Ordering::Acquire);
        while !block.is_null() {
            // SAFETY: blocks live as long as the inner state.
            let slots = unsafe { &(*block).slots };
            for slot in slots {
                loop {
                    let before = slot.state.load(Ordering::Acquire);
                    if phase(before) != OCCUPIED {
                        break;
                    }
                    // SAFETY: an occupied entry has an initialized context.
                    let ctx = unsafe { slot.ctx.get().read().assume_init_read() };
                    let epoch = slot.epoch.load(Ordering::Acquire);
                    let version = slot.version.load(Ordering::Acquire);
                    let after = slot.state.load(Ordering::Acquire);
                    if after != before {
                        continue;
                    }
                    if pass.saturating_sub(epoch) > PRUNE_AFTER_PASSES {
                        if slot.prune(before) {
                            stats.visited += 1;
                            stats.pruned += 1;
                            break;
                        }
                        // The slot changed under the prune, so it is examined again from its new
                        // state and counts once, not once per attempt.
                        continue;
                    }
                    stats.visited += 1;
                    if version < published {
                        if mark(ctx) {
                            stats.marked += 1;
                        } else {
                            // Pruning here would drop this reader's wake for good. The slot
                            // keeps its old version, so the next publish marks it again.
                            stats.dropped += 1;
                        }
                    } else {
                        stats.skipped += 1;
                    }
                    break;
                }
            }
            // SAFETY: as above.
            block = unsafe { (*block).next };
        }
        stats
    }

    /// Number of occupied slots.
    pub fn len(&self) -> usize {
        let mut count = 0;
        let mut block = self.inner.head.load(Ordering::Acquire);
        while !block.is_null() {
            // SAFETY: blocks live as long as the inner state.
            for slot in unsafe { &(*block).slots } {
                if phase(slot.state.load(Ordering::Acquire)) == OCCUPIED {
                    count += 1;
                }
            }
            // SAFETY: as above.
            block = unsafe { (*block).next };
        }
        count
    }

    /// Number of entries that have ever been initialized.
    #[cfg(test)]
    fn initialized(&self) -> usize {
        let mut count = 0;
        let mut block = self.inner.head.load(Ordering::Acquire);
        while !block.is_null() {
            // SAFETY: blocks live as long as the inner state.
            for slot in unsafe { &(*block).slots } {
                if phase(slot.state.load(Ordering::Acquire)) != UNINIT {
                    count += 1;
                }
            }
            // SAFETY: as above.
            block = unsafe { (*block).next };
        }
        count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_marks_only_new_versions() {
        let epoch = PassEpoch::new();
        epoch.begin();
        let slots = SubscriberSlots::<u64>::new(epoch.clone());
        slots.join(7, 1);
        assert_eq!(slots.len(), 1);

        let mut marked = Vec::new();
        let stats = slots.visit(1, |ctx| {
            marked.push(ctx);
            true
        });
        assert_eq!(stats.skipped, 1);
        assert!(marked.is_empty());

        let stats = slots.visit(2, |ctx| {
            marked.push(ctx);
            true
        });
        assert_eq!(stats.marked, 1);
        assert_eq!(marked, vec![7]);
    }

    #[test]
    fn a_busy_context_stays_subscribed_and_is_marked_next_publish() {
        let epoch = PassEpoch::new();
        epoch.begin();
        let slots = SubscriberSlots::<u64>::new(epoch);
        slots.join(3, 1);

        // The context is rendering, so the mark fails. It must stay subscribed.
        let stats = slots.visit(2, |_| false);
        assert_eq!(stats.dropped, 1);
        assert_eq!(stats.marked, 0);
        assert_eq!(slots.len(), 1, "a busy context must not be pruned");

        // The next publish marks it, so the reader still receives the update.
        let stats = slots.visit(2, |_| true);
        assert_eq!(stats.marked, 1);
        assert_eq!(slots.len(), 1);
    }

    #[test]
    fn stale_slot_is_pruned_and_reactivates_same_entry() {
        let epoch = PassEpoch::new();
        epoch.begin();
        let slots = SubscriberSlots::<u64>::new(epoch.clone());
        slots.join(7, 1);
        assert_eq!(slots.initialized(), 1);

        // A reader that read in the previous pass still receives the mark.
        epoch.begin();
        let stats = slots.visit(2, |_| true);
        assert_eq!(stats.marked, 1);
        assert_eq!(stats.pruned, 0);

        // Two passes of slack still receive the mark.
        epoch.begin();
        let stats = slots.visit(3, |_| true);
        assert_eq!(stats.marked, 1);
        assert_eq!(stats.pruned, 0);

        // A third pass without a read prunes instead.
        epoch.begin();
        let stats = slots.visit(4, |_| true);
        assert_eq!(stats.marked, 0);
        assert_eq!(stats.pruned, 1);
        assert_eq!(slots.len(), 0);

        epoch.begin();
        slots.join(7, 2);
        assert_eq!(slots.len(), 1);
        assert_eq!(
            slots.initialized(),
            1,
            "reactivation must not consume a slot"
        );
    }

    #[test]
    fn driver_loop_marks_a_reader_that_reads_once_per_pass() {
        let epoch = PassEpoch::new();
        let slots = SubscriberSlots::<u64>::new(epoch.clone());
        let mut version = 1;

        for pass in 0..64 {
            // The driver begins the pass and publishes in the same pass.
            epoch.begin();
            version += 1;
            let stats = slots.visit(version, |_| true);
            if pass > 0 {
                assert_eq!(
                    stats.marked, 1,
                    "a reader that read during the previous pass must be marked"
                );
                assert_eq!(stats.pruned, 0);
            }
            // The reader renders after the mark, then reads in the current pass.
            slots.join(7, version);
        }

        assert_eq!(slots.len(), 1);
    }

    #[test]
    fn refresh_keeps_the_slot_live_across_passes() {
        let epoch = PassEpoch::new();
        epoch.begin();
        let slots = SubscriberSlots::<u64>::new(epoch.clone());
        slots.join(7, 1);

        epoch.begin();
        slots.join(7, 2);
        let stats = slots.visit(2, |_| true);
        assert_eq!(stats.pruned, 0);
        assert_eq!(stats.skipped, 1);
    }

    #[test]
    fn growth_adds_blocks() {
        let epoch = PassEpoch::new();
        epoch.begin();
        let slots = SubscriberSlots::<u64>::new(epoch);
        for ctx in 0..(BLOCK_SLOTS as u64 * 2 + 1) {
            slots.join(ctx, 1);
        }
        assert_eq!(slots.len(), BLOCK_SLOTS * 2 + 1);
        assert_eq!(slots.initialized(), BLOCK_SLOTS * 2 + 1);
    }

    /// Prints what a join and a marking pass cost as the slot array grows.
    ///
    /// Every scenario starts from a freshly filled array, so the columns stay comparable.
    /// Growth prepends blocks, so a returning context near the head stops early.
    #[cfg_attr(miri, ignore = "measures wall clock time")]
    #[test]
    fn new_subscriber_join_cost() {
        const ROUNDS: u64 = 200;
        let filled = |registered: u64| {
            let epoch = PassEpoch::new();
            epoch.begin();
            let slots = SubscriberSlots::<u64>::new(epoch.clone());
            for ctx in 0..registered {
                slots.join(ctx, 1);
            }
            (epoch, slots)
        };
        let time = |f: &mut dyn FnMut()| {
            let start = std::time::Instant::now();
            for _ in 0..ROUNDS {
                f();
            }
            start.elapsed().as_nanos() as f64 / ROUNDS as f64
        };
        for registered in [16u64, 256, 1024] {
            // A context that has never claimed a slot pays for the whole scan. The array grows
            // during the loop, so this averages the scan lengths the loop passes through.
            let (epoch, slots) = filled(registered);
            let mut ctx = registered;
            let ns = time(&mut || {
                epoch.begin();
                slots.join(ctx, 2);
                ctx += 1;
            });
            println!("{registered} registered, join of a new context {ns:.2} ns");
            // A returning context whose slot sits in the oldest block pays for the whole scan.
            let (epoch, slots) = filled(registered);
            let ns = time(&mut || {
                epoch.begin();
                slots.join(0, 2);
            });
            println!("{registered} registered, join of the oldest slot {ns:.2} ns");
            // A returning context whose slot sits in the newest block stops in the first block.
            let (epoch, slots) = filled(registered);
            let ns = time(&mut || {
                epoch.begin();
                slots.join(registered - 1, 2);
            });
            println!("{registered} registered, join of the newest slot {ns:.2} ns");
            // Marking every slot, which the forwarder does once per publish.
            let (_epoch, slots) = filled(registered);
            let ns = time(&mut || {
                slots.visit(2, |_| true);
            });
            println!("{registered} registered, mark every slot {ns:.2} ns");
        }
    }

    #[test]
    fn a_block_costs_about_a_kibibyte() {
        let slot = std::mem::size_of::<Slot<dioxus_core::ReactiveContext>>();
        let block = std::mem::size_of::<Block<dioxus_core::ReactiveContext>>();
        println!("slot {slot} bytes, block {block} bytes, slots per block {BLOCK_SLOTS}");
        assert_eq!(slot, 64, "a slot is one cache line");
        assert!(
            block <= BLOCK_SLOTS * slot + 64,
            "a block is its slots plus the link, saw {block}"
        );
    }

    #[test]
    fn remove_deactivates_and_join_restores() {
        let epoch = PassEpoch::new();
        epoch.begin();
        let slots = SubscriberSlots::<u64>::new(epoch);
        slots.join(9, 1);
        assert!(slots.remove(&9));
        assert_eq!(slots.len(), 0);
        assert!(!slots.remove(&9));

        slots.join(9, 1);
        assert_eq!(slots.len(), 1);
        assert_eq!(slots.initialized(), 1);
    }
}
