//! B7, the queued signal itself.
//!
//! The signal is the real one, built with `QueuedSignal::from_parts`. The driver is owned by a
//! runner thread, the same role `create_queued_signal_hub` plays, and the runner exits when the
//! last signal clone drops. Nothing reimplements the signal or its driver.
//!
//! The runner records the oldest enqueue it published, so a row can report how long a writer
//! waited for its generation to become visible. `b7z` is the same signal with a zero cadence, so a
//! row can separate the frame policy from the cost of publishing as soon as a write lands.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use queued_signal::state::{FrameRead, QueuedSignal, WriterDriver};

use super::{Candidate, Clock, DriveReport, FrameReader, Sample, Submitter};
use crate::publish::{NO_PUBLISH, PublishProbe, PublishSamples, WriterProbe};

/// How often the runner drains and publishes, matching the hub's frame cadence.
const TICK: Duration = Duration::from_millis(16);

/// No enqueue is waiting for a publish.
const NO_ENQUEUE: u64 = u64::MAX;

/// Bound on the runner passes kept for a row.
const PASS_CAP: usize = 65_536;

/// State shared between the writers, the runner, and the row that reads the report.
#[derive(Default)]
struct Observed {
    /// Oldest enqueue timestamp since the last publish.
    oldest: AtomicU64,
    /// Enqueue timestamps already published, oldest first.
    published: Mutex<Vec<u64>>,
    /// Duration of each runner pass that published.
    passes: Mutex<Vec<u64>>,
}

impl Observed {
    /// Starts with no enqueue waiting.
    fn new() -> Self {
        Self {
            oldest: AtomicU64::new(NO_ENQUEUE),
            published: Mutex::new(Vec::new()),
            passes: Mutex::new(Vec::new()),
        }
    }
}

/// B7 as the ladder sees it: the signal plus what its runner observed.
pub struct QueuedCandidate {
    signal: QueuedSignal<Sample>,
    observed: Arc<Observed>,
    probe: Option<Arc<PublishProbe>>,
}

/// Builds the real signal and starts its runner at the frame cadence.
pub fn new_signal() -> QueuedCandidate {
    new_signal_with_tick(TICK)
}

/// Builds the real signal and starts its runner at the given cadence.
///
/// A zero cadence ticks as fast as the runner allows, so a write is published on the runner's next
/// loop instead of at the next frame.
pub fn new_signal_with_tick(tick: Duration) -> QueuedCandidate {
    build(tick, None)
}

/// Builds the real signal at the frame cadence with publish latency records enabled.
pub fn new_signal_probed() -> QueuedCandidate {
    build(TICK, Some(PublishProbe::new()))
}

/// Builds the real signal at a zero cadence with publish latency records enabled.
pub fn new_zero_tick_signal_probed() -> QueuedCandidate {
    build(Duration::ZERO, Some(PublishProbe::new()))
}

/// Builds the signal, its runner, and an optional publish probe.
fn build(tick: Duration, probe: Option<Arc<PublishProbe>>) -> QueuedCandidate {
    let driver = WriterDriver::<Sample>::new(Sample {
        seq: 0,
        submitted_ns: 0,
    });
    let state = driver.queued_state.clone();
    let op_tx = driver.op_tx.clone();
    let driver = Arc::new(Mutex::new(driver));
    let observed = Arc::new(Observed::new());
    let signal = QueuedSignal::from_parts(state, Some(driver.clone()), op_tx);

    let runner_observed = observed.clone();
    let runner_probe = probe.clone();
    std::thread::spawn(move || {
        let mut last = Instant::now();
        loop {
            // The runner's clone is the last one once every signal clone has dropped.
            if Arc::strong_count(&driver) == 1 {
                break;
            }
            if tick.is_zero() || last.elapsed() >= tick {
                let begin = Instant::now();
                let work_begin = runner_probe.as_ref().map(|probe| probe.now_ns());
                let mut held = driver.lock();
                let before = held.queued_state.peek_version();
                held.tick(Duration::ZERO);
                let published = held.queued_state.peek_version() != before;
                let pass_ns = begin.elapsed().as_nanos() as u64;
                // A row without a probe keeps the original publish path, so its read side is
                // untouched by the trace.
                let newest_seq = if published && runner_probe.is_some() {
                    held.queued_state.read().seq
                } else {
                    0
                };
                drop(held);
                if published {
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
                    let oldest = runner_observed.oldest.swap(NO_ENQUEUE, Ordering::AcqRel);
                    if oldest != NO_ENQUEUE {
                        runner_observed.published.lock().push(oldest);
                    }
                }
                last = Instant::now();
            }
            if !tick.is_zero() {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    });

    QueuedCandidate {
        signal,
        observed,
        probe,
    }
}

/// Writer handle that writes through the signal.
struct QueuedWriter {
    signal: QueuedSignal<Sample>,
    observed: Arc<Observed>,
    probe: Option<WriterProbe>,
}

impl Submitter for QueuedWriter {
    fn submit(&mut self, sample: Sample) {
        let start_ns = self.probe.as_ref().map(WriterProbe::start_now);
        // The runner publishes a generation per tick and reports the oldest enqueue it covered.
        self.observed
            .oldest
            .fetch_min(sample.submitted_ns, Ordering::AcqRel);
        self.signal.set_value(sample);
        if let (Some(probe), Some(start_ns)) = (self.probe.as_mut(), start_ns) {
            probe.record(sample.seq, start_ns, NO_PUBLISH);
        }
    }

    fn submit_mutation(&mut self, sample: Sample) {
        let start_ns = self.probe.as_ref().map(WriterProbe::start_now);
        self.observed
            .oldest
            .fetch_min(sample.submitted_ns, Ordering::AcqRel);
        self.signal
            .mutate_set(move |value: &mut Sample| *value = sample);
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

impl Candidate for QueuedCandidate {
    fn id(&self) -> &'static str {
        "b7"
    }

    fn caveat(&self) -> &'static str {
        "writes are resolved by the runner at the frame cadence, so a reader sees a frame of latency"
    }

    fn submitter(&self) -> Option<Box<dyn Submitter>> {
        Some(Box::new(QueuedWriter {
            signal: self.signal.clone(),
            observed: self.observed.clone(),
            probe: self.probe.as_ref().map(|probe| probe.writer()),
        }))
    }

    fn drive(&self, clock: &Clock, report: &mut DriveReport) {
        // The runner publishes on its own thread, so a pass only converts the enqueue timestamps it
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

    fn retained(&self) -> u64 {
        self.signal.state.cell.reclaim_health().retained()
    }

    fn publish_samples(&self) -> Option<PublishSamples> {
        self.probe.as_ref().map(|probe| probe.samples())
    }
}
