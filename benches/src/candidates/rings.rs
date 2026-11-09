//! B8, one unbounded queue per writer.
//!
//! Each writer handle owns a queue, so writers never share a head. The resolver thread walks the
//! queues at the frame cadence and publishes through the same driver b7 uses, so the read side is
//! identical and only the queue topology differs.
//!
//! A queue is a list of fixed size chunks. The producer writes a slot and publishes its index, and
//! the consumer releases a chunk once it has drained it, so a frame of writes allocates only the
//! chunks it fills.

use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use queued_signal::state::{FrameRead, QueuedSignal, WriterDriver};

use super::{Candidate, Clock, DriveReport, FrameReader, Sample, Submitter};
use crate::publish::{NO_PUBLISH, PublishProbe, PublishSamples, WriterProbe};
use crate::stats::{self, Stats};

/// How often the runner drains and publishes, matching the hub's frame cadence.
const TICK: Duration = Duration::from_millis(16);
/// Slots in one chunk, so a chunk is allocated once per this many writes.
const CHUNK: usize = 1024;
/// Bound on the resolver passes kept for a row.
const PASS_CAP: usize = 65_536;
/// Slot states.
const SLOT_SET: u8 = 1;
const SLOT_MUTATE: u8 = 2;

/// A mutation received through a queue.
type Mutation = Arc<dyn Fn(&mut Sample) + Send + Sync + 'static>;

/// Chunks waiting to be drained, oldest first.
type ChunkQueue = Mutex<VecDeque<Arc<Chunk>>>;

/// One queued write.
enum Entry {
    /// Replaces the value.
    Set(Sample),
    /// Mutates the value in place.
    Mutate(Mutation),
}

/// One queued entry, written by the producer and taken by the consumer.
struct Slot {
    /// Whether the slot holds a set, a mutation, or nothing yet.
    kind: AtomicU8,
    /// Submission sequence of a set.
    seq: AtomicU64,
    /// Submission time of a set, on the row clock.
    submitted_ns: AtomicU64,
    /// Mutation of a mutation entry, written before the kind is published.
    mutation: UnsafeCell<Option<Mutation>>,
}

// SAFETY: one producer writes a slot before publishing its index with a release store, and one
// consumer reads it only after acquiring that index. No two threads touch the same slot at once.
unsafe impl Send for Slot {}
// SAFETY: as above, the slot is handed from producer to consumer and never shared live.
unsafe impl Sync for Slot {}

impl Slot {
    /// Creates an empty slot.
    fn new() -> Self {
        Self {
            kind: AtomicU8::new(0),
            seq: AtomicU64::new(0),
            submitted_ns: AtomicU64::new(0),
            mutation: UnsafeCell::new(None),
        }
    }

    /// Writes one entry and publishes it to the consumer.
    fn put(&self, entry: Entry) {
        match entry {
            Entry::Set(sample) => {
                self.seq.store(sample.seq, Ordering::Relaxed);
                self.submitted_ns
                    .store(sample.submitted_ns, Ordering::Relaxed);
                self.kind.store(SLOT_SET, Ordering::Release);
            }
            Entry::Mutate(op) => {
                // SAFETY: the consumer cannot reach this slot until the index is published below.
                unsafe { *self.mutation.get() = Some(op) };
                self.kind.store(SLOT_MUTATE, Ordering::Release);
            }
        }
    }

    /// Takes the published entry.
    fn take(&self) -> Entry {
        match self.kind.load(Ordering::Acquire) {
            SLOT_SET => Entry::Set(Sample {
                seq: self.seq.load(Ordering::Relaxed),
                submitted_ns: self.submitted_ns.load(Ordering::Relaxed),
            }),
            _ => {
                // SAFETY: the producer is done with this slot once its index is visible here.
                let op = unsafe { (*self.mutation.get()).take() };
                Entry::Mutate(op.expect("a published mutation slot holds an op"))
            }
        }
    }
}

/// One fixed size block of slots.
struct Chunk {
    /// Slots of this block.
    slots: Vec<Slot>,
}

impl Chunk {
    /// Allocates an empty chunk.
    fn new() -> Self {
        Self {
            slots: (0..CHUNK).map(|_| Slot::new()).collect(),
        }
    }
}

/// Producer index, kept on its own line so the consumer's flag swap never steals it.
#[repr(align(64))]
struct Tail(AtomicUsize);

/// Non-empty flag, kept on its own line so the consumer's swap invalidates only this word.
#[repr(align(64))]
struct NonEmpty(AtomicBool);

/// One writer's queue.
struct Ring {
    /// Chunks the consumer has not finished, oldest first.
    chunks: ChunkQueue,
    /// Next index the producer will write.
    tail: Tail,
    /// Whether anything was published since the consumer last looked.
    nonempty: NonEmpty,
}

impl Ring {
    /// Creates an empty ring.
    fn new() -> Self {
        Self {
            chunks: Mutex::new(VecDeque::new()),
            tail: Tail(AtomicUsize::new(0)),
            nonempty: NonEmpty(AtomicBool::new(false)),
        }
    }

    /// Reads the flag the resolver checks before touching this ring.
    fn has_work(&self) -> bool {
        self.nonempty.0.load(Ordering::Acquire)
    }
}

/// Writer side of one ring, owned by one thread.
struct Producer {
    ring: Arc<Ring>,
    tail: usize,
    chunk_start: usize,
    chunk: Option<Arc<Chunk>>,
}

impl Producer {
    /// Creates the writer side of a ring.
    fn new(ring: Arc<Ring>) -> Self {
        Self {
            ring,
            tail: 0,
            chunk_start: 0,
            chunk: None,
        }
    }

    /// Enqueues one entry and never blocks or drops.
    fn push(&mut self, entry: Entry) {
        let index = self.tail;
        if self.chunk.is_none() || index == self.chunk_start + CHUNK {
            let chunk = Arc::new(Chunk::new());
            self.ring.chunks.lock().push_back(chunk.clone());
            self.chunk = Some(chunk);
            self.chunk_start = index;
        }
        if let Some(chunk) = self.chunk.as_ref() {
            chunk.slots[index - self.chunk_start].put(entry);
        }
        self.tail = index + 1;
        self.ring.tail.0.store(self.tail, Ordering::Release);
        self.ring.nonempty.0.store(true, Ordering::Release);
    }
}

/// Reader side of one ring, owned by the resolver thread.
struct Consumer {
    ring: Arc<Ring>,
    head: usize,
    chunk: Option<Arc<Chunk>>,
}

impl Consumer {
    /// Creates the reader side of a ring.
    fn new(ring: Arc<Ring>) -> Self {
        Self {
            ring,
            head: 0,
            chunk: None,
        }
    }

    /// Appends every published entry to `out`.
    ///
    /// The flag is cleared before the entries are read, so a write that lands in that gap is
    /// drained by this pass or leaves the flag set for the next one.
    fn drain_into(&mut self, out: &mut Vec<Entry>) {
        if !self.ring.nonempty.0.swap(false, Ordering::Acquire) {
            return;
        }
        let tail = self.ring.tail.0.load(Ordering::Acquire);
        while self.head < tail {
            if self.chunk.is_none() && self.head.is_multiple_of(CHUNK) {
                self.chunk = self.ring.chunks.lock().pop_front();
            }
            let Some(chunk) = self.chunk.as_ref() else {
                break;
            };
            out.push(chunk.slots[self.head % CHUNK].take());
            self.head += 1;
            if self.head.is_multiple_of(CHUNK) {
                self.chunk = None;
            }
        }
    }
}

/// The queues of one signal, one per writer handle.
#[derive(Default)]
struct RingSet {
    rings: Mutex<Vec<Arc<Ring>>>,
    count: AtomicUsize,
}

impl RingSet {
    /// Attaches a new queue and returns it.
    fn attach(&self) -> Arc<Ring> {
        let ring = Arc::new(Ring::new());
        let mut rings = self.rings.lock();
        rings.push(ring.clone());
        self.count.store(rings.len(), Ordering::Release);
        ring
    }

    /// Number of queues attached so far.
    fn len(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }

    /// The queue at `index`, when it has been attached.
    fn get(&self, index: usize) -> Option<Arc<Ring>> {
        self.rings.lock().get(index).cloned()
    }
}

/// What the resolver observed, for the row to read.
#[derive(Default)]
struct Observed {
    /// Oldest enqueue timestamp covered by each publish.
    published: Mutex<Vec<u64>>,
    /// Duration of each resolver pass that had work.
    passes: Mutex<Vec<u64>>,
}

/// B8 as the ladder sees it: the signal plus what its resolver observed.
pub struct RingsCandidate {
    signal: QueuedSignal<Sample>,
    rings: Arc<RingSet>,
    observed: Arc<Observed>,
    probe: Option<Arc<PublishProbe>>,
}

/// Builds the signal, the queues, and the resolver thread at the frame cadence.
pub fn new_rings() -> RingsCandidate {
    build(None)
}

/// Builds the same design with publish latency records enabled.
pub fn new_rings_probed() -> RingsCandidate {
    build(Some(PublishProbe::new()))
}

/// Builds the signal, the queues, the resolver thread, and an optional publish probe.
fn build(probe: Option<Arc<PublishProbe>>) -> RingsCandidate {
    let driver = WriterDriver::<Sample>::new(Sample {
        seq: 0,
        submitted_ns: 0,
    });
    let state = driver.queued_state.clone();
    let op_tx = driver.op_tx.clone();
    let driver = Arc::new(Mutex::new(driver));
    let signal = QueuedSignal::from_parts(state, Some(driver.clone()), op_tx);
    let rings = Arc::new(RingSet::default());
    let observed = Arc::new(Observed::default());

    let runner_driver = driver.clone();
    let runner_rings = rings.clone();
    let runner_observed = observed.clone();
    let runner_probe = probe.clone();
    std::thread::spawn(move || {
        let mut consumers: Vec<Consumer> = Vec::new();
        let mut entries: Vec<Entry> = Vec::new();
        let mut mutations: Vec<Mutation> = Vec::new();
        let mut last = Instant::now();
        loop {
            // The runner's clone is the last one once every signal clone has dropped.
            if Arc::strong_count(&runner_driver) == 1 {
                break;
            }
            if last.elapsed() >= TICK {
                while consumers.len() < runner_rings.len() {
                    let Some(ring) = runner_rings.get(consumers.len()) else {
                        break;
                    };
                    consumers.push(Consumer::new(ring));
                }
                let begin = Instant::now();
                let work_begin = runner_probe.as_ref().map(|probe| probe.now_ns());
                entries.clear();
                for consumer in consumers.iter_mut() {
                    consumer.drain_into(&mut entries);
                }
                let mut newest: Option<Sample> = None;
                let mut oldest = u64::MAX;
                for entry in entries.drain(..) {
                    match entry {
                        Entry::Set(sample) => {
                            oldest = oldest.min(sample.submitted_ns);
                            if newest.is_none_or(|seen| sample.seq > seen.seq) {
                                newest = Some(sample);
                            }
                        }
                        Entry::Mutate(op) => mutations.push(op),
                    }
                }
                let had_work = newest.is_some() || !mutations.is_empty();
                if had_work {
                    let mut held = runner_driver.lock();
                    let mut value = match newest {
                        Some(sample) => sample,
                        None => *held.queued_state.read(),
                    };
                    for op in mutations.drain(..) {
                        op(&mut value);
                    }
                    let newest_seq = value.seq;
                    held.publish_value(value);
                    let pass_ns = begin.elapsed().as_nanos() as u64;
                    let mut passes = runner_observed.passes.lock();
                    if passes.len() < PASS_CAP {
                        passes.push(pass_ns);
                    }
                    drop(passes);
                    if let Some(probe) = runner_probe.as_ref() {
                        let work_ns = work_begin
                            .map(|begin| probe.now_ns().saturating_sub(begin))
                            .unwrap_or(0);
                        probe.record_generation(newest_seq, work_ns);
                    }
                    if oldest != u64::MAX {
                        runner_observed.published.lock().push(oldest);
                    }
                }
                last = Instant::now();
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    });

    RingsCandidate {
        signal,
        rings,
        observed,
        probe,
    }
}

/// Writer handle that owns its queue.
struct RingWriter {
    producer: Producer,
    probe: Option<WriterProbe>,
}

impl Submitter for RingWriter {
    fn submit(&mut self, sample: Sample) {
        let start_ns = self.probe.as_ref().map(WriterProbe::start_now);
        self.producer.push(Entry::Set(sample));
        if let (Some(probe), Some(start_ns)) = (self.probe.as_mut(), start_ns) {
            probe.record(sample.seq, start_ns, NO_PUBLISH);
        }
    }

    fn submit_mutation(&mut self, sample: Sample) {
        let start_ns = self.probe.as_ref().map(WriterProbe::start_now);
        let mutation = Arc::new(move |value: &mut Sample| *value = sample);
        self.producer.push(Entry::Mutate(mutation));
        if let (Some(probe), Some(start_ns)) = (self.probe.as_mut(), start_ns) {
            probe.record(sample.seq, start_ns, NO_PUBLISH);
        }
    }
}

/// Frame scoped reader over the signal.
struct SignalFrameReader {
    frame: FrameRead<Sample>,
}

impl FrameReader for SignalFrameReader {
    fn read(&mut self) -> u64 {
        self.frame.read().seq
    }
}

impl Candidate for RingsCandidate {
    fn id(&self) -> &'static str {
        "b8"
    }

    fn caveat(&self) -> &'static str {
        "one unbounded queue per writer, one resolver; mutation order across queues is queue order, \
         not global arrival"
    }

    fn submitter(&self) -> Option<Box<dyn Submitter>> {
        let ring = self.rings.attach();
        Some(Box::new(RingWriter {
            producer: Producer::new(ring),
            probe: self.probe.as_ref().map(|probe| probe.writer()),
        }))
    }

    fn drive(&self, clock: &Clock, report: &mut DriveReport) {
        // The resolver publishes on its own thread, so a pass converts the enqueue timestamps it
        // already published into visibility latency on the row clock.
        let published: Vec<u64> = std::mem::take(&mut *self.observed.published.lock());
        for oldest in published {
            report.applied += 1;
            report
                .latencies_ns
                .push(clock.now_ns().saturating_sub(oldest));
        }
        let passes: Vec<u64> = std::mem::take(&mut *self.observed.passes.lock());
        report.drain_ns.extend_from_slice(&passes);
        report.finished_ns = clock.now_ns();
    }

    fn read(&self) -> u64 {
        self.signal.state.read().seq
    }

    fn frame_reader(&self) -> Option<Box<dyn FrameReader>> {
        let reader = SignalFrameReader {
            frame: self.signal.state.frame_read(),
        };
        Some(Box::new(reader))
    }

    fn drain_probe(&self, samples: usize) -> Option<Stats> {
        // The resolver's first act per frame is one flag read per writer, so an idle probe of that
        // loop prices the drain with no entries behind it.
        let samples = samples.max(1);
        let rings = self.rings.rings.lock();
        let mut values = Vec::with_capacity(samples);
        for _ in 0..samples {
            let begin = Instant::now();
            let mut busy = 0usize;
            for ring in rings.iter() {
                if ring.has_work() {
                    busy += 1;
                }
            }
            black_box(busy);
            values.push(begin.elapsed().as_nanos() as f64);
        }
        Some(stats::summarize(values))
    }

    fn retained(&self) -> u64 {
        self.signal.state.cell.reclaim_health().retained()
    }

    fn publish_samples(&self) -> Option<PublishSamples> {
        self.probe.as_ref().map(|probe| probe.samples())
    }
}
