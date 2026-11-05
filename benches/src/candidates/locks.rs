//! B3 and B4, the two obvious fixes a competent engineer reaches for.
//!
//! B3 takes a read lock once per frame, so a writer can stall every reader. B4 takes a lock on
//! every mutation, so readers contend with each writer directly.

use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use super::{Candidate, Clock, DriveReport, Sample, Submitter};

/// B3, a reference counted read write lock.
pub struct RwLockCell {
    value: Arc<RwLock<u64>>,
}

impl RwLockCell {
    /// Creates the lock at zero.
    pub fn new() -> Self {
        Self {
            value: Arc::new(RwLock::new(0)),
        }
    }

    /// Reads under a shared lock.
    pub fn read_value(&self) -> u64 {
        *self.value.read()
    }

    /// Writes under an exclusive lock.
    pub fn write_value(&self, value: u64) {
        *self.value.write() = value;
    }
}

impl Default for RwLockCell {
    fn default() -> Self {
        Self::new()
    }
}

/// Writer handle for B3.
struct RwLockWriter {
    value: Arc<RwLock<u64>>,
}

impl Submitter for RwLockWriter {
    fn submit(&self, sample: Sample) {
        *self.value.write() = sample.seq;
    }
}

impl Candidate for RwLockCell {
    fn id(&self) -> &'static str {
        "b3"
    }

    fn caveat(&self) -> &'static str {
        "a writer holding the lock blocks every reader"
    }

    fn submitter(&self) -> Option<Box<dyn Submitter>> {
        Some(Box::new(RwLockWriter {
            value: self.value.clone(),
        }))
    }

    fn drive(&self, _clock: &Clock, _report: &mut DriveReport) {}

    fn read(&self) -> u64 {
        self.read_value()
    }
}

/// B4, a reference counted mutex.
pub struct MutexCell {
    value: Arc<Mutex<u64>>,
}

impl MutexCell {
    /// Creates the mutex at zero.
    pub fn new() -> Self {
        Self {
            value: Arc::new(Mutex::new(0)),
        }
    }

    /// Reads under the lock.
    pub fn read_value(&self) -> u64 {
        *self.value.lock()
    }

    /// Writes under the lock.
    pub fn write_value(&self, value: u64) {
        *self.value.lock() = value;
    }
}

impl Default for MutexCell {
    fn default() -> Self {
        Self::new()
    }
}

/// Writer handle for B4.
struct MutexWriter {
    value: Arc<Mutex<u64>>,
}

impl Submitter for MutexWriter {
    fn submit(&self, sample: Sample) {
        *self.value.lock() = sample.seq;
    }
}

impl Candidate for MutexCell {
    fn id(&self) -> &'static str {
        "b4"
    }

    fn caveat(&self) -> &'static str {
        "a writer holding the lock blocks every reader, and writers serialise"
    }

    fn submitter(&self) -> Option<Box<dyn Submitter>> {
        Some(Box::new(MutexWriter {
            value: self.value.clone(),
        }))
    }

    fn drive(&self, _clock: &Clock, _report: &mut DriveReport) {}

    fn read(&self) -> u64 {
        self.read_value()
    }
}
