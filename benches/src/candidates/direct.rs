//! B0, a plain field with no synchronisation.
//!
//! This is the floor of the ladder. Anything slower than a plain field needs a reason, so the
//! row exists to calibrate the machine and the harness rather than to compete.

use std::cell::UnsafeCell;
use std::sync::Arc;

use super::{Candidate, Clock, DriveReport, Sample, Submitter};

/// Plain cell the baseline only touches from one thread.
struct Plain {
    value: UnsafeCell<u64>,
}

// SAFETY: a direct baseline row has one writer and one reader on one thread.
unsafe impl Send for Plain {}
// SAFETY: as above.
unsafe impl Sync for Plain {}

/// B0, the floor.
pub struct Direct {
    value: Arc<Plain>,
}

impl Direct {
    /// Creates the floor baseline at zero.
    pub fn new() -> Self {
        Self {
            value: Arc::new(Plain {
                value: UnsafeCell::new(0),
            }),
        }
    }

    /// Reads the field.
    pub fn read_value(&self) -> u64 {
        // SAFETY: no other thread touches the cell during a direct baseline row.
        unsafe { *self.value.value.get() }
    }

    /// Writes the field.
    pub fn write_value(&self, value: u64) {
        // SAFETY: as above.
        unsafe { *self.value.value.get() = value };
    }
}

impl Default for Direct {
    fn default() -> Self {
        Self::new()
    }
}

/// Writer handle for the direct baseline.
struct DirectWriter {
    value: Arc<Plain>,
}

impl Submitter for DirectWriter {
    fn submit(&mut self, sample: Sample) {
        // SAFETY: the direct baseline is only used with one writer thread.
        unsafe { *self.value.value.get() = sample.seq };
    }
}

impl Candidate for Direct {
    fn id(&self) -> &'static str {
        "b0"
    }

    fn caveat(&self) -> &'static str {
        "single thread only, no synchronisation"
    }

    fn submitter(&self) -> Option<Box<dyn Submitter>> {
        Some(Box::new(DirectWriter {
            value: self.value.clone(),
        }))
    }

    fn drive(&self, _clock: &Clock, _report: &mut DriveReport) {}

    fn read(&self) -> u64 {
        self.read_value()
    }
}
