//! Per thread write queues for a queued signal.
//!
//! A write resolves the calling thread's queue, so writers never share a head and a handle used on
//! a thread follows that thread's queue. The driver merges the queues when it resolves, and a
//! signal with one queue keeps the single queue path.
//!
//! A queue is a chain of fixed size chunks. The producer links a chunk before it writes the first
//! slot, and the consumer returns a drained chunk to a free stack, so a steady writer allocates
//! only while it runs ahead of the resolver. No step blocks and no step takes a lock.
//!
//! Merge rule: entries from one queue apply in enqueue order. Each non-empty queue is drained as
//! one block, and blocks apply by the monotonic timestamp of the queue's last write, then by queue
//! identity. A write stamps itself from the monotonic clock, so no shared counter is contended and
//! the newest submission resolves last.
//!
//! Lifecycle: a queue is created on the first write from a thread and its producer lives in thread
//! local storage, so every handle used on that thread shares it. The resolver retires a queue once
//! its producer is gone and the queue is drained. A clone used on the same thread joins the queue,
//! and a clone used on another thread attaches that thread's queue.

use std::any::Any;
use std::cell::{Cell, RefCell, UnsafeCell};
use std::fmt;
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::Instant;

use crate::state::Op;

/// Slots in one chunk.
///
/// A chunk holds one value per slot, so the constant trades the size of a first write against the
/// number of chunk turns a writer that outruns the resolver makes.
const CHUNK: usize = 64;

/// Next identity handed to a queue set.
static NEXT_SET_ID: AtomicU64 = AtomicU64::new(1);

/// Next identity handed to a thread queue.
static NEXT_QUEUE_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    /// One producer for every signal this thread has written to.
    static PRODUCERS: RefCell<Vec<ProducerSlot>> = const { RefCell::new(Vec::new()) };
    /// Direct mapped cache of the producers this thread writes most often.
    ///
    /// A write tests one entry before it reaches the directory, so the common case is a thread
    /// local load, an index, and a compare. A miss falls back to the directory and refills the
    /// entry the identity maps to.
    static CACHED: Cell<[CachedProducer; CACHE_SLOTS]> =
        const { Cell::new([CachedProducer::NONE; CACHE_SLOTS]) };
}

/// Entries in the thread local producer cache.
///
/// The cache is direct mapped, so two identities that share an index evict each other. Eight slots
/// cover the signals a thread writes in one frame, and an identity outside that set only pays the
/// directory lookup.
const CACHE_SLOTS: usize = 8;

/// One cached producer pointer, keyed by the identity of its signal.
#[derive(Clone, Copy)]
struct CachedProducer {
    /// Identity of the signal, or zero for an empty entry.
    id: u64,
    /// The producer this thread owns for that signal.
    producer: *mut (),
}

impl CachedProducer {
    /// An entry that matches no signal.
    const NONE: Self = Self {
        id: 0,
        producer: ptr::null_mut(),
    };
}

/// Liveness counter shared by the queues of one signal.
///
/// A producer keeps one of these instead of the queue set, so a retired signal can release its
/// queues without the producer holding them alive.
struct Liveness {
    /// Number of attached queues, so a lone queue can skip the arrival stamp.
    queues: AtomicUsize,
    /// Number of producers that are gone, so the reader knows to retire their queues.
    dead: AtomicUsize,
}

impl Liveness {
    /// Creates the counter for a signal with no queues.
    fn new() -> Self {
        Self {
            queues: AtomicUsize::new(0),
            dead: AtomicUsize::new(0),
        }
    }

    /// Records that one producer is gone.
    fn note_dead(&self) {
        self.dead.fetch_add(1, Ordering::Release);
    }
}

/// Returns the arrival stamp of one write, in nanoseconds on the monotonic clock.
///
/// The clock is machine wide and needs no shared counter, so a write never contends for its stamp.
fn arrival_ns() -> u64 {
    static BASE: OnceLock<Instant> = OnceLock::new();
    BASE.get_or_init(Instant::now).elapsed().as_nanos() as u64
}

/// One slot of a chunk.
struct Slot<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> {
    /// Entry written by the producer and taken by the consumer.
    payload: UnsafeCell<Option<Op<T, D>>>,
}

// SAFETY: A slot is written by the queue's single producer before the queue publishes the slot
// index with a release store, and read by the single consumer only after an acquire load of that
// index. The consumer empties a slot before its chunk is returned to the free stack, so no two
// threads touch a live slot at once.
unsafe impl<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> Send for Slot<T, D> {}
// SAFETY: The same handoff applies, so sharing a slot between threads is bounded by the queue
// protocol rather than by a lock.
unsafe impl<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> Sync for Slot<T, D> {}

impl<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> Slot<T, D> {
    /// Creates an empty slot.
    fn new() -> Self {
        Self {
            payload: UnsafeCell::new(None),
        }
    }
}

/// One fixed size block of slots.
struct Chunk<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> {
    /// Slots of this block.
    slots: Vec<Slot<T, D>>,
    /// Chunk that follows this one in the chain, linked by the producer.
    next: AtomicPtr<Chunk<T, D>>,
    /// Next chunk on the free stack, linked by the consumer.
    free_next: AtomicPtr<Chunk<T, D>>,
}

impl<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> Chunk<T, D> {
    /// Allocates an empty chunk.
    fn new() -> Self {
        Self {
            slots: (0..CHUNK).map(|_| Slot::new()).collect(),
            next: AtomicPtr::new(ptr::null_mut()),
            free_next: AtomicPtr::new(ptr::null_mut()),
        }
    }
}

/// Producer index, kept on its own line so the consumer's flag swap does not share it.
#[repr(align(64))]
struct Tail(AtomicUsize);

/// Non-empty flag, kept on its own line so the consumer's swap invalidates only this word.
#[repr(align(64))]
struct NonEmpty(AtomicBool);

/// One writer thread's queue.
struct ThreadQueue<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> {
    /// Identity of this queue.
    id: u64,
    /// First chunk of the chain, set by the producer on its first write.
    first: AtomicPtr<Chunk<T, D>>,
    /// Next index the producer will write.
    tail: Tail,
    /// Whether anything was published since the consumer last looked.
    nonempty: NonEmpty,
    /// Arrival stamp of the last write published on this queue.
    stamp: AtomicU64,
    /// Stack of drained chunks the producer reuses.
    free: AtomicPtr<Chunk<T, D>>,
    /// Every chunk allocation of this queue.
    ///
    /// The producer pushes here and the queue frees the list when it is released. The list is not
    /// shared live, so a plain cell is enough.
    allocations: UnsafeCell<Vec<*mut Chunk<T, D>>>,
    /// Whether the producer is gone, so the queue can be retired once drained.
    dead: AtomicBool,
}

// SAFETY: The allocation list is touched by the queue's single producer and by the queue release,
// which runs only when no producer can write again. Every other field is atomic or immutable.
unsafe impl<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> Send for ThreadQueue<T, D> {}
// SAFETY: Several threads share the queue through atomics, and the allocation list is confined to
// the producer and the release path as above.
unsafe impl<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> Sync for ThreadQueue<T, D> {}

impl<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> ThreadQueue<T, D> {
    /// Creates an empty queue.
    fn new() -> Self {
        Self {
            id: NEXT_QUEUE_ID.fetch_add(1, Ordering::Relaxed),
            first: AtomicPtr::new(ptr::null_mut()),
            tail: Tail(AtomicUsize::new(0)),
            nonempty: NonEmpty(AtomicBool::new(false)),
            stamp: AtomicU64::new(0),
            free: AtomicPtr::new(ptr::null_mut()),
            allocations: UnsafeCell::new(Vec::new()),
            dead: AtomicBool::new(false),
        }
    }

    /// Releases every chunk this queue allocated and marks it retired.
    ///
    /// A handle keeps the queue set alive for the whole of a write, so no write can run once the
    /// set is releasing its queues.
    fn release_chunks(&self) {
        self.dead.store(true, Ordering::Release);
        // SAFETY: No producer can be inside a write, because every write holds the queue set that
        // is being released. The consumer belongs to the driver, which holds the same set.
        let allocations = unsafe { &mut *self.allocations.get() };
        for chunk in allocations.drain(..) {
            // SAFETY: The chunk was allocated with Box::into_raw and is freed once.
            unsafe { drop(Box::from_raw(chunk)) };
        }
    }
}

impl<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> Drop for ThreadQueue<T, D> {
    fn drop(&mut self) {
        // SAFETY: The queue is dropping, so no producer or consumer remains.
        let allocations = unsafe { &mut *self.allocations.get() };
        for chunk in allocations.drain(..) {
            // SAFETY: The chunk was allocated with Box::into_raw and is freed once.
            unsafe { drop(Box::from_raw(chunk)) };
        }
    }
}

/// One node of the lock free queue registry.
struct QueueNode<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> {
    /// The queue this node lists.
    queue: Arc<ThreadQueue<T, D>>,
    /// Next node in the registry, oldest last.
    next: AtomicPtr<QueueNode<T, D>>,
}

/// The queues of one signal, one per writer thread.
struct QueueSet<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> {
    /// Identity of this set, used to resolve a thread's producer.
    id: u64,
    /// Registry head, pushed by attaching producers and unlinked by the reader.
    head: AtomicPtr<QueueNode<T, D>>,
    /// Number of attached queues, readable without a traversal.
    count: AtomicUsize,
    /// Bumped whenever the registry changes, so the reader can skip a traversal.
    generation: AtomicU64,
    /// Liveness counter shared with the producers.
    liveness: Arc<Liveness>,
    /// Liveness marker, weak handles to which tell a thread that the signal is gone.
    alive: Arc<()>,
}

impl<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> QueueSet<T, D> {
    /// Creates an empty set with a fresh identity.
    fn new() -> Self {
        Self {
            id: NEXT_SET_ID.fetch_add(1, Ordering::Relaxed),
            head: AtomicPtr::new(ptr::null_mut()),
            count: AtomicUsize::new(0),
            generation: AtomicU64::new(0),
            liveness: Arc::new(Liveness::new()),
            alive: Arc::new(()),
        }
    }

    /// Attaches a new queue and returns it.
    fn attach(&self) -> Arc<ThreadQueue<T, D>> {
        let queue = Arc::new(ThreadQueue::new());
        let node = Box::into_raw(Box::new(QueueNode {
            queue: queue.clone(),
            next: AtomicPtr::new(ptr::null_mut()),
        }));
        let mut head = self.head.load(Ordering::Relaxed);
        loop {
            // SAFETY: The node is owned by this thread until the compare and exchange publishes it.
            unsafe { (*node).next.store(head, Ordering::Relaxed) };
            match self
                .head
                .compare_exchange_weak(head, node, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(observed) => head = observed,
            }
        }
        self.count.fetch_add(1, Ordering::Release);
        self.liveness.queues.fetch_add(1, Ordering::Release);
        // The generation follows the publish, so a reader that sees the new generation also sees
        // the node.
        self.generation.fetch_add(1, Ordering::Release);
        queue
    }
}

impl<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> Drop for QueueSet<T, D> {
    fn drop(&mut self) {
        let mut node = self.head.load(Ordering::Acquire);
        let mut queues = Vec::new();
        while !node.is_null() {
            // SAFETY: The set is dropping, so no attach, prune, or write runs concurrently.
            let next = unsafe { (*node).next.load(Ordering::Acquire) };
            let boxed = unsafe { Box::from_raw(node) };
            queues.push(boxed.queue.clone());
            node = next;
        }
        for queue in queues.iter() {
            queue.release_chunks();
        }
    }
}

/// Handle to the per thread write queues of one signal.
///
/// A handle is cheap to clone, and every clone used on one thread resolves that thread's queue. A
/// write from a thread that has not written yet attaches a queue for it, so a clone never creates a
/// second queue on the same thread. A clone used on another thread writes to that thread's queue.
pub struct WriteQueues<T: Clone + Send + Sync + 'static, D: ?Sized + 'static = dyn Any> {
    /// The queues shared by every handle of the signal.
    set: Arc<QueueSet<T, D>>,
}

impl<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> WriteQueues<T, D> {
    /// Creates an empty queue set.
    pub fn new() -> Self {
        Self {
            set: Arc::new(QueueSet::new()),
        }
    }

    /// Enqueues one write on this thread's queue and never blocks or drops it.
    ///
    /// The cache hits on the signal a thread writes most often, and a miss attaches the thread's
    /// queue once and then fills the entry the identity maps to.
    pub fn push(&self, op: Op<T, D>) {
        let slot = (self.set.id as usize) & (CACHE_SLOTS - 1);
        let cached = CACHED.with(|cache| cache.get()[slot]);
        if cached.id == self.set.id && !cached.producer.is_null() {
            // SAFETY: This thread wrote the entry for a signal it holds, and the identity is
            // unique, so the pointer is that signal's producer for this thread.
            unsafe { (&mut *(cached.producer as *mut Producer<T, D>)).push(op) };
            return;
        }

        PRODUCERS.with(|producers| {
            let mut producers = producers.borrow_mut();
            let found = producers
                .iter_mut()
                .position(|entry| entry.id == self.set.id);
            let producer = match found {
                Some(position) => producers[position]
                    .producer
                    .downcast_mut::<Producer<T, D>>()
                    .expect("a queue identity maps to one producer type"),
                None => {
                    // Attaching a queue is a chance to drop producers of signals that are gone, so a
                    // process that creates many short lived signals does not keep their queues alive.
                    let before = producers.len();
                    producers.retain(ProducerSlot::alive);
                    if producers.len() != before {
                        // A dropped producer invalidates the entry that pointed at it.
                        CACHED.with(|cache| cache.set([CachedProducer::NONE; CACHE_SLOTS]));
                    }
                    let queue = self.set.attach();
                    producers.push(ProducerSlot {
                        id: self.set.id,
                        alive: Arc::downgrade(&self.set.alive),
                        producer: Box::new(Producer::new(Arc::clone(&self.set.liveness), queue)),
                    });
                    producers
                        .last_mut()
                        .expect("the producer was just pushed")
                        .producer
                        .downcast_mut::<Producer<T, D>>()
                        .expect("a queue identity maps to one producer type")
                }
            };
            producer.push(op);
            let pointer = producer as *mut Producer<T, D> as *mut ();
            CACHED.with(|cache| {
                let mut entries = cache.get();
                entries[slot] = CachedProducer {
                    id: self.set.id,
                    producer: pointer,
                };
                cache.set(entries);
            });
        });
    }

    /// Number of writer thread queues attached to this signal.
    pub fn queue_count(&self) -> usize {
        self.set.count.load(Ordering::Acquire)
    }
}

impl<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> Default for WriteQueues<T, D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> Clone for WriteQueues<T, D> {
    fn clone(&self) -> Self {
        Self {
            set: Arc::clone(&self.set),
        }
    }
}

impl<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> fmt::Debug for WriteQueues<T, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteQueues")
            .field("queues", &self.queue_count())
            .finish()
    }
}

/// One producer as the thread directory stores it.
struct ProducerSlot {
    /// Identity of the signal the producer writes to.
    id: u64,
    /// Liveness marker of that signal.
    alive: Weak<()>,
    /// The typed producer.
    producer: Box<dyn Any>,
}

impl ProducerSlot {
    /// Whether the signal behind this producer is still alive.
    fn alive(&self) -> bool {
        self.alive.strong_count() > 0
    }
}

/// Writer side of one queue, owned by one thread.
struct Producer<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> {
    /// Liveness counter shared with the other queues of the signal.
    liveness: Arc<Liveness>,
    /// The queue of this thread.
    queue: Arc<ThreadQueue<T, D>>,
    /// Chunk this producer writes, when one is held.
    chunk: *mut Chunk<T, D>,
    /// First index of the chunk this producer writes.
    chunk_start: usize,
    /// Next index this producer will write.
    tail: usize,
}

impl<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> Producer<T, D> {
    /// Creates the writer side of a queue.
    fn new(liveness: Arc<Liveness>, queue: Arc<ThreadQueue<T, D>>) -> Self {
        Self {
            liveness,
            queue,
            chunk: ptr::null_mut(),
            chunk_start: 0,
            tail: 0,
        }
    }

    /// Enqueues one entry and never blocks or drops it.
    fn push(&mut self, op: Op<T, D>) {
        let index = self.tail;
        if self.chunk.is_null() || index == self.chunk_start + CHUNK {
            self.take_chunk(index);
        }
        // A signal with one queue has nothing to merge against, so it keeps the single thread path
        // free of a clock read. Otherwise the write stamps its arrival time without a shared
        // counter that every write would contend for.
        if self.liveness.queues.load(Ordering::Relaxed) > 1 {
            self.queue.stamp.store(arrival_ns(), Ordering::Relaxed);
        }
        // SAFETY: The consumer cannot reach this slot until the index below is published.
        let chunk = unsafe { &*self.chunk };
        let slot = &chunk.slots[index - self.chunk_start];
        // SAFETY: The consumer cannot reach this slot until the index below is published.
        unsafe { *slot.payload.get() = Some(op) };
        self.tail = index + 1;
        self.queue.tail.0.store(self.tail, Ordering::Release);
        self.queue.nonempty.0.store(true, Ordering::Release);
    }

    /// Takes a chunk for the next block of entries.
    fn take_chunk(&mut self, index: usize) {
        let recycled = self.pop_free();
        let chunk = if recycled.is_null() {
            // SAFETY: The allocation is stored in the queue's list and freed when the queue is
            // released, so the raw pointer stays valid for as long as this producer exists.
            let chunk = Box::into_raw(Box::new(Chunk::new()));
            // SAFETY: Only this producer pushes to the allocation list.
            unsafe { (*self.queue.allocations.get()).push(chunk) };
            chunk
        } else {
            recycled
        };
        // SAFETY: The chunk is owned by this producer and is not reachable by the consumer yet.
        unsafe { (*chunk).next.store(ptr::null_mut(), Ordering::Relaxed) };
        if self.chunk.is_null() {
            self.queue.first.store(chunk, Ordering::Release);
        } else {
            // SAFETY: The chunk is owned by this producer, and the link is the publication that
            // makes it reachable.
            unsafe { (*self.chunk).next.store(chunk, Ordering::Release) };
        }
        self.chunk = chunk;
        self.chunk_start = index;
    }

    /// Pops a drained chunk from the free stack, or returns null when the stack is empty.
    fn pop_free(&self) -> *mut Chunk<T, D> {
        let mut head = self.queue.free.load(Ordering::Acquire);
        loop {
            if head.is_null() {
                return ptr::null_mut();
            }
            // SAFETY: Every chunk on the free stack was allocated by this queue and is drained.
            let next = unsafe { (*head).free_next.load(Ordering::Relaxed) };
            match self.queue.free.compare_exchange_weak(
                head,
                next,
                Ordering::Acquire,
                Ordering::Acquire,
            ) {
                Ok(_) => return head,
                Err(observed) => head = observed,
            }
        }
    }
}

impl<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> Drop for Producer<T, D> {
    fn drop(&mut self) {
        self.queue.dead.store(true, Ordering::Release);
        self.liveness.note_dead();
    }
}

/// Reader side of one queue, owned by the resolver thread.
struct Consumer<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> {
    /// The queue this consumer drains.
    queue: Arc<ThreadQueue<T, D>>,
    /// Next index this consumer will take.
    head: usize,
    /// Chunk this consumer reads, when one is held.
    chunk: *mut Chunk<T, D>,
    /// First index of the chunk this consumer reads.
    chunk_start: usize,
    /// Arrival stamp of the queue's last write before this pass.
    stamp: u64,
    /// Entries drained by this pass, in dequeue order.
    ops: Vec<Op<T, D>>,
}

// SAFETY: A cursor is owned by one driver at a time, and the queue it points into is kept alive by
// the cursor's own Arc. Moving the cursor to another thread moves the whole reader with it.
unsafe impl<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> Send for Consumer<T, D> {}

impl<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> Consumer<T, D> {
    /// Creates the reader side of a queue.
    fn new(queue: Arc<ThreadQueue<T, D>>) -> Self {
        Self {
            queue,
            head: 0,
            chunk: ptr::null_mut(),
            chunk_start: 0,
            stamp: 0,
            ops: Vec::new(),
        }
    }

    /// Takes every published entry into `ops`.
    ///
    /// The flag is cleared before the index is read, so a write that lands in that gap is drained
    /// by this pass or leaves the flag set for the next one.
    fn drain(&mut self) {
        if !self.queue.nonempty.0.swap(false, Ordering::Acquire) {
            return;
        }
        let tail = self.queue.tail.0.load(Ordering::Acquire);
        self.stamp = self.queue.stamp.load(Ordering::Acquire);
        while self.head < tail {
            if self.chunk.is_null() || self.head == self.chunk_start + CHUNK {
                // SAFETY: The queue Arc keeps every chunk allocation alive while the cursor exists.
                let next = if self.chunk.is_null() {
                    self.queue.first.load(Ordering::Acquire)
                } else {
                    unsafe { (*self.chunk).next.load(Ordering::Acquire) }
                };
                let Some(next) = NonNull::new(next) else {
                    // The producer has not linked the next chunk yet, so no entry is published.
                    break;
                };
                if !self.chunk.is_null() {
                    self.recycle(self.chunk);
                }
                self.chunk = next.as_ptr();
                self.chunk_start = self.head;
            }
            // SAFETY: The producer published this index before the acquire load above, and the
            // consumer is the only thread that takes a published slot.
            let chunk = unsafe { &*self.chunk };
            let slot = &chunk.slots[self.head - self.chunk_start];
            let op =
                unsafe { (*slot.payload.get()).take() }.expect("a published slot holds an entry");
            self.ops.push(op);
            self.head += 1;
        }
    }

    /// Pushes a drained chunk onto the free stack.
    fn recycle(&self, chunk: *mut Chunk<T, D>) {
        let mut head = self.queue.free.load(Ordering::Relaxed);
        loop {
            // SAFETY: The chunk is drained and owned by this consumer.
            unsafe { (*chunk).free_next.store(head, Ordering::Relaxed) };
            match self.queue.free.compare_exchange_weak(
                head,
                chunk,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(observed) => head = observed,
            }
        }
    }
}

/// Reader side of a signal's queues, owned by the driver.
///
/// The reader keeps one cursor per queue and reorders only a small list of blocks, so a resolve
/// does not allocate once the buffers have grown. Queues are re-read only when the registry
/// generation changes.
pub(crate) struct QueueReader<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> {
    /// One cursor per attached queue.
    consumers: Vec<Consumer<T, D>>,
    /// Cursors with work this pass, in the order their blocks apply.
    order: Vec<usize>,
    /// Registry generation this reader last observed.
    generation: u64,
}

impl<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> QueueReader<T, D> {
    /// Creates a reader with no queues attached.
    pub(crate) fn new() -> Self {
        Self {
            consumers: Vec::new(),
            order: Vec::new(),
            generation: 0,
        }
    }

    /// Drains every queue and applies the entries in merge order.
    ///
    /// A signal with one queue keeps its enqueue order and reads no stamps, so the single writer
    /// path does no merge work.
    pub(crate) fn apply(
        &mut self,
        queues: &WriteQueues<T, D>,
        mut apply: impl FnMut(Op<T, D>),
    ) -> bool {
        self.sync(&queues.set);
        self.order.clear();
        for (index, consumer) in self.consumers.iter_mut().enumerate() {
            consumer.drain();
            if !consumer.ops.is_empty() {
                self.order.push(index);
            }
        }
        if self.order.len() > 1 {
            self.order.sort_unstable_by_key(|&index| {
                let consumer = &self.consumers[index];
                (consumer.stamp, consumer.queue.id)
            });
        }
        let mut saw_op = false;
        let order = std::mem::take(&mut self.order);
        for &index in order.iter() {
            let consumer = &mut self.consumers[index];
            for op in consumer.ops.drain(..) {
                saw_op = true;
                apply(op);
            }
        }
        self.order = order;
        self.prune(&queues.set);
        saw_op
    }

    /// Rebuilds the cursors when the registry changed.
    fn sync(&mut self, set: &QueueSet<T, D>) {
        let generation = set.generation.load(Ordering::Acquire);
        if generation == self.generation {
            return;
        }
        let mut previous = std::mem::take(&mut self.consumers);
        let mut consumers = Vec::new();
        let mut node = set.head.load(Ordering::Acquire);
        while !node.is_null() {
            // SAFETY: Only the reader unlinks nodes, and this reader is the only one.
            let entry = unsafe { &*node };
            match previous
                .iter()
                .position(|cursor| cursor.queue.id == entry.queue.id)
            {
                Some(index) => consumers.push(previous.swap_remove(index)),
                None => consumers.push(Consumer::new(entry.queue.clone())),
            }
            node = entry.next.load(Ordering::Acquire);
        }
        self.consumers = consumers;
        self.generation = generation;
    }

    /// Removes queues whose producer is gone and whose entries are all taken.
    fn prune(&mut self, set: &QueueSet<T, D>) {
        if set.liveness.dead.load(Ordering::Relaxed) == 0 {
            return;
        }
        let mut link: *const AtomicPtr<QueueNode<T, D>> = &set.head;
        let mut removed = 0;
        loop {
            // SAFETY: Only the reader unlinks nodes, so the predecessor link stays valid.
            let node = unsafe { (*link).load(Ordering::Acquire) };
            if node.is_null() {
                break;
            }
            // SAFETY: The node is linked in the registry, which keeps it alive.
            let entry = unsafe { &*node };
            let removable = entry.queue.dead.load(Ordering::Acquire) && {
                let head = self
                    .consumers
                    .iter()
                    .find(|cursor| cursor.queue.id == entry.queue.id)
                    .map_or(0, |cursor| cursor.head);
                head >= entry.queue.tail.0.load(Ordering::Acquire)
            };
            if !removable {
                link = &entry.next;
                continue;
            }
            let next = entry.next.load(Ordering::Acquire);
            // SAFETY: Both the unattached head and an interior link are atomic and are written
            // only here, apart from a head push that makes this compare and exchange retry.
            if unsafe { (*link).compare_exchange(node, next, Ordering::AcqRel, Ordering::Acquire) }
                .is_ok()
            {
                if let Some(index) = self
                    .consumers
                    .iter()
                    .position(|cursor| cursor.queue.id == entry.queue.id)
                {
                    self.consumers.swap_remove(index);
                }
                // SAFETY: The node is unlinked, and no other reader traverses the registry.
                unsafe { drop(Box::from_raw(node)) };
                removed += 1;
                continue;
            }
        }
        if removed == 0 {
            return;
        }
        set.count.fetch_sub(removed, Ordering::Release);
        set.liveness.queues.fetch_sub(removed, Ordering::Release);
        set.liveness.dead.fetch_sub(removed, Ordering::Release);
        self.generation = set.generation.fetch_add(1, Ordering::Release) + 1;
    }
}
