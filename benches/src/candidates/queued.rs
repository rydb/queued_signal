//! B7, the queued signal itself.
//!
//! The signal is the real one, built with `QueuedSignal::from_parts`. The driver is owned by a
//! runner thread, the same role `create_queued_signal_hub` plays, and the runner exits when the
//! last signal clone drops. Nothing reimplements the signal or its driver.

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use queued_signal::state::{FrameRead, QueuedSignal, WriterDriver};

use super::{Candidate, Clock, DriveReport, FrameReader, Sample, Submitter};

/// How often the runner drains and publishes, matching the hub's frame cadence.
const TICK: Duration = Duration::from_millis(16);

/// Builds the real signal and starts its runner.
pub fn new_signal() -> QueuedSignal<Sample> {
    let driver = WriterDriver::<Sample>::new(Sample {
        seq: 0,
        submitted_ns: 0,
    });
    let state = driver.queued_state.clone();
    let op_tx = driver.op_tx.clone();
    let driver = Arc::new(Mutex::new(driver));
    let signal = QueuedSignal::from_parts(state, Some(driver.clone()), op_tx);

    std::thread::spawn(move || {
        let mut last = Instant::now();
        loop {
            // The runner's clone is the last one once every signal clone has dropped.
            if Arc::strong_count(&driver) == 1 {
                break;
            }
            if last.elapsed() >= TICK {
                driver.lock().tick(Duration::ZERO);
                last = Instant::now();
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    });

    signal
}

/// Writer handle that writes through the signal.
struct QueuedWriter {
    signal: QueuedSignal<Sample>,
}

impl Submitter for QueuedWriter {
    fn submit(&self, sample: Sample) {
        self.signal.set_value(sample);
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

impl Candidate for QueuedSignal<Sample> {
    fn id(&self) -> &'static str {
        "b7"
    }

    fn caveat(&self) -> &'static str {
        "writes are resolved by the runner at the frame cadence, so a reader sees a frame of latency"
    }

    fn submitter(&self) -> Option<Box<dyn Submitter>> {
        Some(Box::new(QueuedWriter {
            signal: self.clone(),
        }))
    }

    fn drive(&self, clock: &Clock, report: &mut DriveReport) {
        // The runner owns the driver, so a reader never drives it.
        report.finished_ns = clock.now_ns();
    }

    fn read(&self) -> u64 {
        self.state.read().seq
    }

    fn frame_reader(&self) -> Option<Box<dyn FrameReader>> {
        let reader = SignalFrameReader {
            frame: self.state.frame_read(),
        };
        Some(Box::new(reader))
    }

    fn retained(&self) -> u64 {
        self.state.cell.reclaim_health().retained()
    }
}
