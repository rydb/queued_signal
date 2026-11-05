//! B6, preallocated snapshots that allocate nothing per publish.
//!
//! `Snapshot` uses an odd and even version counter around a preallocated slot, so a reader
//! retries only while a writer is inside the write section. `RotatingSnapshot` rotates three
//! slots instead, so the reader owns a slot the writer cannot touch and never retries at all.
//!
//! A preempted writer inside a seqlock section stalls the reader for the length of the
//! preemption, which is why both variants are reported. The rotating variant is the strongest
//! competitor for read latency: it removes the reader's wait entirely at the cost of one
//! publication overshoot when the reader is slower than the writer.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use super::{Candidate, Clock, DriveReport, Sample, Submitter};

/// Preallocated snapshot with a version counter.
pub struct Snapshot {
    version: AtomicU64,
    value: AtomicU64,
    writer_lock: AtomicBool,
    serialised: bool,
}

impl Snapshot {
    /// Snapshot for one writer, which needs no writer lock.
    pub fn single_writer() -> Self {
        Self {
            version: AtomicU64::new(0),
            value: AtomicU64::new(0),
            writer_lock: AtomicBool::new(false),
            serialised: false,
        }
    }

    /// Snapshot for several writers, which serialise on one compare and exchange.
    pub fn multi_writer() -> Self {
        Self {
            serialised: true,
            ..Self::single_writer()
        }
    }

    /// Publishes one sequence.
    pub fn publish_value(&self, seq: u64) {
        if self.serialised {
            while self
                .writer_lock
                .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
            {
                std::hint::spin_loop();
            }
        }
        self.version.fetch_add(1, Ordering::AcqRel);
        self.value.store(seq, Ordering::Release);
        self.version.fetch_add(1, Ordering::Release);
        if self.serialised {
            self.writer_lock.store(false, Ordering::Release);
        }
    }

    /// Reads the newest complete sequence, retrying across a write section.
    pub fn snapshot(&self) -> u64 {
        loop {
            let before = self.version.load(Ordering::Acquire);
            if before & 1 == 1 {
                std::hint::spin_loop();
                continue;
            }
            let value = self.value.load(Ordering::Acquire);
            let after = self.version.load(Ordering::Acquire);
            if before == after {
                return value;
            }
        }
    }
}

/// B6 with one writer.
pub struct AtomicSnapshot {
    snapshot: Arc<Snapshot>,
}

impl AtomicSnapshot {
    /// Creates the single writer snapshot.
    pub fn single_writer() -> Self {
        Self {
            snapshot: Arc::new(Snapshot::single_writer()),
        }
    }

    /// Creates the multi writer snapshot.
    pub fn multi_writer() -> Self {
        Self {
            snapshot: Arc::new(Snapshot::multi_writer()),
        }
    }

    /// Publishes a sequence.
    pub fn publish_value(&self, seq: u64) {
        self.snapshot.publish_value(seq);
    }

    /// Reads the newest complete sequence.
    pub fn read_value(&self) -> u64 {
        self.snapshot.snapshot()
    }
}

/// Writer handle for B6.
struct SnapshotWriter {
    snapshot: Arc<Snapshot>,
}

impl Submitter for SnapshotWriter {
    fn submit(&mut self, sample: Sample) {
        self.snapshot.publish_value(sample.seq);
    }
}

/// Index mask for the shared word.
const INDEX_MASK: u64 = 0b11;
/// Set when the shared slot has not been consumed yet.
const FRESH: u64 = 0b100;

/// Shared state of the rotation.
struct Rotation {
    slots: [AtomicU64; 3],
    shared: AtomicU64,
    back: AtomicU64,
    front: AtomicU64,
    writer_lock: AtomicBool,
    serialised: bool,
}

impl Rotation {
    /// Starts a rotation with a fresh first publication.
    fn new(serialised: bool) -> Self {
        Self {
            slots: std::array::from_fn(|_| AtomicU64::new(0)),
            shared: AtomicU64::new(2 | FRESH),
            back: AtomicU64::new(0),
            front: AtomicU64::new(1),
            writer_lock: AtomicBool::new(false),
            serialised,
        }
    }

    /// Publishes one sequence.
    fn publish(&self, seq: u64) {
        if self.serialised {
            while self
                .writer_lock
                .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
            {
                std::hint::spin_loop();
            }
        }
        let back = self.back.load(Ordering::Relaxed) as usize;
        self.slots[back].store(seq, Ordering::Release);
        let previous = self.shared.swap(back as u64 | FRESH, Ordering::AcqRel);
        self.back.store(previous & INDEX_MASK, Ordering::Relaxed);
        if self.serialised {
            self.writer_lock.store(false, Ordering::Release);
        }
    }

    /// Reads the newest complete sequence without waiting.
    fn read(&self) -> u64 {
        let shared = self.shared.load(Ordering::Acquire);
        if shared & FRESH != 0 {
            let front = self.front.load(Ordering::Relaxed);
            let previous = self.shared.swap(front, Ordering::AcqRel);
            self.front.store(previous & INDEX_MASK, Ordering::Relaxed);
        }
        let front = self.front.load(Ordering::Relaxed) as usize;
        self.slots[front].load(Ordering::Acquire)
    }
}

/// Three slot snapshot with one writer and one consumer.
///
/// The writer owns the back slot and the consumer owns the front slot, so neither one reads
/// memory the other writes. One atomic word hands a finished slot over and returns the slot
/// the other side is done with. The consumer never spins.
pub struct RotatingSnapshot {
    rotation: Arc<Rotation>,
}

impl RotatingSnapshot {
    /// Rotation for one writer.
    pub fn single_writer() -> Self {
        Self {
            rotation: Arc::new(Rotation::new(false)),
        }
    }

    /// Rotation for several writers, which serialise on one compare and exchange.
    pub fn multi_writer() -> Self {
        Self {
            rotation: Arc::new(Rotation::new(true)),
        }
    }

    /// Publishes one sequence.
    pub fn publish_value(&self, seq: u64) {
        self.rotation.publish(seq);
    }

    /// Reads the newest complete sequence without waiting.
    pub fn snapshot(&self) -> u64 {
        self.rotation.read()
    }
}

impl Candidate for RotatingSnapshot {
    fn id(&self) -> &'static str {
        "b6r"
    }

    fn caveat(&self) -> &'static str {
        "single consumer, no wake path, a slow reader silently drops the intermediate publications"
    }

    fn submitter(&self) -> Option<Box<dyn Submitter>> {
        Some(Box::new(RotatingWriter {
            rotation: self.rotation.clone(),
        }))
    }

    fn drive(&self, _clock: &Clock, _report: &mut DriveReport) {}

    fn read(&self) -> u64 {
        self.snapshot()
    }
}

/// Writer handle for the rotating variant.
struct RotatingWriter {
    rotation: Arc<Rotation>,
}

impl Submitter for RotatingWriter {
    fn submit(&mut self, sample: Sample) {
        self.rotation.publish(sample.seq);
    }
}

impl Candidate for AtomicSnapshot {
    fn id(&self) -> &'static str {
        "b6"
    }

    fn caveat(&self) -> &'static str {
        "fixed participant set, no wake path, readers retry across a write section"
    }

    fn submitter(&self) -> Option<Box<dyn Submitter>> {
        Some(Box::new(SnapshotWriter {
            snapshot: self.snapshot.clone(),
        }))
    }

    fn drive(&self, _clock: &Clock, _report: &mut DriveReport) {}

    fn read(&self) -> u64 {
        self.read_value()
    }
}
