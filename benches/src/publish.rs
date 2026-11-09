//! Publish latency records shared by every candidate.
//!
//! Each writer keeps a ring of submission stamps, and the publishing side records the newest
//! sequence it made readable. A row derives one latency per write from those two records.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

use parking_lot::Mutex;

/// Writes one writer ring holds before it wraps.
const CAP: usize = 8_192;

/// Marks a write whose design publishes it from a resolver rather than inside submit.
pub const NO_PUBLISH: u64 = u64::MAX;

/// One latency per write, split into the part after submit and the whole call.
#[derive(Default)]
pub struct PublishSamples {
    /// Time from the writer's submit returning to readable.
    pub deferral: Vec<u64>,
    /// Time from the writer entering submit to readable.
    pub total: Vec<u64>,
    /// Time the publishing side spent from its pass start to readable.
    pub work: Vec<u64>,
}

/// Clock and records shared by the writers and the publishing side of one candidate.
pub struct PublishProbe {
    /// Base instant of the probe clock.
    base: Instant,
    /// One ring per writer handle.
    writers: Mutex<Vec<Arc<WriterRing>>>,
    /// Publications from a resolver, in time order.
    published: Mutex<Vec<Published>>,
}

/// One publication of a resolver.
#[derive(Clone, Copy)]
struct Published {
    /// Newest sequence made readable by the publication.
    newest_seq: u64,
    /// Publish time on the probe clock.
    publish_ns: u64,
    /// Time the publishing pass spent before the publish.
    work_ns: u64,
}

/// Submission stamps of one writer.
struct WriterRing {
    /// Submission sequence per slot.
    seq: Box<[AtomicU64]>,
    /// Submit entry stamp per slot, on the probe clock.
    start_ns: Box<[AtomicU64]>,
    /// Submit return stamp per slot, on the probe clock.
    submit_ns: Box<[AtomicU64]>,
    /// Immediate publish stamp per slot, or [`NO_PUBLISH`].
    publish_ns: Box<[AtomicU64]>,
    /// Number of writes recorded, which may exceed the ring length.
    count: AtomicUsize,
}

impl WriterRing {
    /// Creates an empty ring.
    fn new() -> Self {
        Self {
            seq: zeros(0),
            start_ns: zeros(0),
            submit_ns: zeros(0),
            publish_ns: zeros(NO_PUBLISH),
            count: AtomicUsize::new(0),
        }
    }
}

/// Boxed array of atomic words filled with one value.
fn zeros(value: u64) -> Box<[AtomicU64]> {
    (0..CAP)
        .map(|_| AtomicU64::new(value))
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

impl PublishProbe {
    /// Creates a probe with its own monotonic clock.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            base: Instant::now(),
            writers: Mutex::new(Vec::new()),
            published: Mutex::new(Vec::new()),
        })
    }

    /// Nanoseconds since the probe started.
    pub fn now_ns(&self) -> u64 {
        self.base.elapsed().as_nanos() as u64
    }

    /// Attaches one writer handle.
    pub fn writer(self: &Arc<Self>) -> WriterProbe {
        let ring = Arc::new(WriterRing::new());
        self.writers.lock().push(ring.clone());
        WriterProbe {
            probe: self.clone(),
            ring,
            cursor: 0,
        }
    }

    /// Records a publication that made `newest_seq` readable.
    ///
    /// `work_ns` is the time the publishing pass spent before the publish, so it excludes the wait
    /// for the pass to start.
    pub fn record_generation(&self, newest_seq: u64, work_ns: u64) {
        let publish_ns = self.now_ns();
        self.published.lock().push(Published {
            newest_seq,
            publish_ns,
            work_ns,
        });
    }

    /// Derives one latency per write, in writer then submission order.
    pub fn samples(&self) -> PublishSamples {
        let published = self.published.lock();
        let writers = self.writers.lock();
        let mut out = PublishSamples::default();
        for ring in writers.iter() {
            let count = ring.count.load(Ordering::Acquire);
            let first = count.saturating_sub(CAP);
            for cursor in first..count {
                let index = cursor % CAP;
                let seq = ring.seq[index].load(Ordering::Relaxed);
                let start_ns = ring.start_ns[index].load(Ordering::Relaxed);
                let submit_ns = ring.submit_ns[index].load(Ordering::Relaxed);
                let immediate = ring.publish_ns[index].load(Ordering::Relaxed);
                let (publish_ns, work_ns) = if immediate != NO_PUBLISH {
                    // The writer is the publishing side, so its whole submit is the work.
                    (immediate, immediate.saturating_sub(start_ns))
                } else {
                    match match_publish(&published, seq) {
                        Some(record) => (record.publish_ns, record.work_ns),
                        None => continue,
                    }
                };
                out.deferral.push(publish_ns.saturating_sub(submit_ns));
                out.total.push(publish_ns.saturating_sub(start_ns));
                out.work.push(work_ns);
            }
        }
        out
    }
}

/// Publication that made `seq` readable, when one covers it.
///
/// A write can be published inside the submit call that enqueued it, so the publication and the
/// submit return can be ordered either way. The start stamp is always before the publication.
fn match_publish(published: &[Published], seq: u64) -> Option<Published> {
    published
        .iter()
        .find(|record| record.newest_seq >= seq)
        .copied()
}

/// One writer's handle into a probe.
pub struct WriterProbe {
    /// Shared clock and publication records.
    probe: Arc<PublishProbe>,
    /// This writer's submission ring.
    ring: Arc<WriterRing>,
    /// Slots this writer has filled.
    cursor: usize,
}

impl WriterProbe {
    /// Time on the probe clock, captured as the writer enters submit.
    pub fn start_now(&self) -> u64 {
        self.probe.now_ns()
    }

    /// Publish time on the probe clock, captured at a publication inside submit.
    pub fn publish_now(&self) -> u64 {
        self.probe.now_ns()
    }

    /// Records one write and the publication that made it readable.
    ///
    /// A design that publishes from a resolver passes [`NO_PUBLISH`], and the row matches the
    /// write to the first publication whose newest sequence covers it. The start stamp is taken
    /// before the design sees the write, so the total includes any wait inside submit.
    pub fn record(&mut self, seq: u64, start_ns: u64, publish_ns: u64) {
        let index = self.cursor % CAP;
        self.ring.seq[index].store(seq, Ordering::Relaxed);
        self.ring.start_ns[index].store(start_ns, Ordering::Relaxed);
        self.ring.submit_ns[index].store(self.probe.now_ns(), Ordering::Relaxed);
        self.ring.publish_ns[index].store(publish_ns, Ordering::Relaxed);
        self.cursor = self.cursor.wrapping_add(1);
        self.ring.count.store(self.cursor, Ordering::Release);
    }
}
