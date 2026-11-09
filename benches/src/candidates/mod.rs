//! The baseline ladder.
//!
//! Every candidate exposes the same two sides: a submitter that a writer thread owns, and a
//! driver pass that a consumer thread owns. A design that publishes inside `submit` leaves the
//! driver pass empty, and says so in its caveat.

use std::time::Instant;

pub mod channels;
pub mod direct;
pub mod locks;
pub mod queued;
pub mod rings;
pub mod triple;

pub use channels::ChannelPerSubscriber;
pub use direct::Direct;
pub use locks::{MutexCell, RwLockCell};
pub use triple::{AtomicSnapshot, RotatingSnapshot};

use crate::publish::PublishSamples;
use crate::stats::Stats;

/// One mutation submitted to a candidate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Sample {
    /// Global submission sequence, which identifies the last write.
    pub seq: u64,
    /// Nanoseconds since the row started when the writer submitted.
    pub submitted_ns: u64,
}

/// Monotonic clock shared by every writer in a row.
#[derive(Clone, Copy, Debug)]
pub struct Clock {
    base: Instant,
}

impl Clock {
    /// Starts the clock for a row.
    pub fn new() -> Self {
        Self {
            base: Instant::now(),
        }
    }

    /// Nanoseconds since the row started.
    pub fn now_ns(&self) -> u64 {
        self.base.elapsed().as_nanos() as u64
    }

    /// Stamps a submission sequence with the current time.
    pub fn stamp(&self, seq: u64) -> Sample {
        Sample {
            seq,
            submitted_ns: self.now_ns(),
        }
    }
}

impl Default for Clock {
    fn default() -> Self {
        Self::new()
    }
}

/// Writer side handle that may move to another thread.
///
/// A handle is owned by one writer thread, so a design whose queue state lives in the handle takes
/// the mutable form and needs no interior lock.
pub trait Submitter: Send + Sync {
    /// Submits one sample.
    fn submit(&mut self, sample: Sample);

    /// Submits one mutation, when the design can tell it from a replacement.
    ///
    /// The default treats a mutation as a replacement, which is what a design without a mutation
    /// path can represent.
    fn submit_mutation(&mut self, sample: Sample) {
        self.submit(sample);
    }
}

/// Reader that keeps one pin and reuses it while the published version is unchanged.
pub trait FrameReader {
    /// Reads the newest sequence.
    fn read(&mut self) -> u64;
}

/// Result of one driver pass.
#[derive(Debug, Default)]
pub struct DriveReport {
    /// Submissions applied by this pass.
    pub applied: usize,
    /// Wake calls issued by this pass.
    pub wakes: usize,
    /// Newest sample applied by this pass.
    pub last: Option<Sample>,
    /// Per write visibility latency, in submission order.
    ///
    /// The vector is reused across passes so a steady driver does not allocate.
    pub latencies_ns: Vec<u64>,
    /// Resolver passes completed since the last report, one duration per pass.
    ///
    /// A design whose resolver runs on its own thread reports the pass it already ran, so a row
    /// can price the drain beside the write side instead of assuming it is free.
    pub drain_ns: Vec<u64>,
    /// Time the pass finished, on the row clock.
    pub finished_ns: u64,
}

impl DriveReport {
    /// Clears the report for the next pass.
    pub fn reset(&mut self) {
        self.applied = 0;
        self.wakes = 0;
        self.last = None;
        self.latencies_ns.clear();
        self.drain_ns.clear();
        self.finished_ns = 0;
    }
}

/// A design in the ladder.
pub trait Candidate: Send + Sync {
    /// Stable identifier, for example `b6`.
    fn id(&self) -> &'static str;

    /// What this design cannot represent, stated once and reported with every row.
    fn caveat(&self) -> &'static str;

    /// Creates a writer handle, or none when the design is single threaded.
    fn submitter(&self) -> Option<Box<dyn Submitter>>;

    /// Runs one driver pass.
    ///
    /// Designs that publish inside `submit` leave the report empty.
    fn drive(&self, clock: &Clock, report: &mut DriveReport);

    /// Reads the newest published sequence.
    fn read(&self) -> u64;

    /// A frame scoped reader, when the design offers one.
    fn frame_reader(&self) -> Option<Box<dyn FrameReader>> {
        None
    }

    /// Times an idle resolver pass, when the design can isolate one.
    ///
    /// A design with a queue per writer reads one flag per queue before touching it, so this probe
    /// prices that scan with no work behind it.
    fn drain_probe(&self, _samples: usize) -> Option<Stats> {
        None
    }

    /// Replaced generations not yet reclaimed, when the design has any.
    fn retained(&self) -> u64 {
        0
    }

    /// Publish latency samples, when the candidate records them.
    ///
    /// A design built without a probe returns nothing, so a fixed burst row keeps its submit path.
    fn publish_samples(&self) -> Option<PublishSamples> {
        None
    }
}

/// Candidate identifiers in ladder order, limited to the designs implemented so far.
///
/// B1 and B2 are absent because neither can accept a write from a thread that does not own the
/// runtime: a dioxus signal is written on the runtime that owns it, and a bevy resource is
/// written through a system on the app's schedule. They still appear in E2, where the row owns the
/// runtime and can drive it from the same thread.
pub const IMPLEMENTED: &[&str] = &["b0", "b3", "b4", "b5", "b6", "b6r", "b7", "b7z", "b8"];

/// Builds one candidate by identifier, with one subscriber and one writer.
pub fn build(id: &str) -> Option<Box<dyn Candidate>> {
    match id {
        "b0" => Some(Box::new(Direct::new())),
        "b3" => Some(Box::new(RwLockCell::new())),
        "b4" => Some(Box::new(MutexCell::new())),
        "b5" => Some(Box::new(ChannelPerSubscriber::new(1))),
        "b6" => Some(Box::new(AtomicSnapshot::single_writer())),
        "b6r" => Some(Box::new(RotatingSnapshot::single_writer())),
        "b7" => Some(Box::new(queued::new_signal())),
        // The same signal with no frame cadence, so a row can separate the policy from the cost.
        "b7z" => Some(Box::new(queued::new_signal_with_tick(
            std::time::Duration::ZERO,
        ))),
        "b8" => Some(Box::new(rings::new_rings())),
        _ => None,
    }
}

/// Builds one candidate with a stated subscriber count.
///
/// Only the fan out experiment varies this. A multi writer row still has one consumer, so
/// the writer count must not become the channel count.
pub fn build_with_subscribers(id: &str, subscribers: usize) -> Option<Box<dyn Candidate>> {
    match id {
        "b5" => Some(Box::new(ChannelPerSubscriber::new(subscribers))),
        _ => build(id),
    }
}

/// Builds one candidate for a multi writer row.
///
/// B6 keeps one writer per section, so several writers need its serialising variant. B8 attaches
/// one queue per writer handle, so it learns its writer count from the handles a row takes.
pub fn build_for_writers(id: &str, _writers: usize) -> Option<Box<dyn Candidate>> {
    match id {
        "b6" => Some(Box::new(AtomicSnapshot::multi_writer())),
        "b6r" => Some(Box::new(RotatingSnapshot::multi_writer())),
        _ => build_with_subscribers(id, 1),
    }
}

/// Builds one candidate with publish latency records enabled.
///
/// Only the duration row uses this, so a fixed burst row keeps its original submit path. Every
/// design records through the same helper, so the comparison is structurally identical.
pub fn build_probed(id: &str, writers: usize) -> Option<Box<dyn Candidate>> {
    match id {
        "b3" => Some(Box::new(RwLockCell::with_probe())),
        "b4" => Some(Box::new(MutexCell::with_probe())),
        "b5" => Some(Box::new(ChannelPerSubscriber::with_probe(1))),
        "b6" => Some(Box::new(if writers > 1 {
            AtomicSnapshot::multi_writer_with_probe()
        } else {
            AtomicSnapshot::single_writer_with_probe()
        })),
        "b6r" => Some(Box::new(if writers > 1 {
            RotatingSnapshot::multi_writer_with_probe()
        } else {
            RotatingSnapshot::single_writer_with_probe()
        })),
        "b7" => Some(Box::new(queued::new_signal_probed())),
        "b7z" => Some(Box::new(queued::new_zero_tick_signal_probed())),
        "b8" => Some(Box::new(rings::new_rings_probed())),
        _ => None,
    }
}
