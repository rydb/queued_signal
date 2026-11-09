//! B3 and B4, the two obvious fixes a competent engineer reaches for.
//!
//! B3 takes a read lock once per frame, so a writer can stall every reader. B4 takes a lock on
//! every mutation, so readers contend with each writer directly.

use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use super::{Candidate, Clock, DriveReport, Sample, Submitter};
use crate::publish::{PublishProbe, PublishSamples, WriterProbe};

/// B3, a reference counted read write lock.
pub struct RwLockCell {
    value: Arc<RwLock<u64>>,
    probe: Option<Arc<PublishProbe>>,
}

impl RwLockCell {
    /// Creates the lock at zero.
    pub fn new() -> Self {
        Self {
            value: Arc::new(RwLock::new(0)),
            probe: None,
        }
    }

    /// Creates the lock with publish latency records enabled.
    pub fn with_probe() -> Self {
        Self {
            value: Arc::new(RwLock::new(0)),
            probe: Some(PublishProbe::new()),
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
    probe: Option<WriterProbe>,
}

impl Submitter for RwLockWriter {
    fn submit(&mut self, sample: Sample) {
        let start_ns = self.probe.as_ref().map(WriterProbe::start_now);
        *self.value.write() = sample.seq;
        if let (Some(probe), Some(start_ns)) = (self.probe.as_mut(), start_ns) {
            let published_ns = probe.publish_now();
            probe.record(sample.seq, start_ns, published_ns);
        }
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
            probe: self.probe.as_ref().map(|probe| probe.writer()),
        }))
    }

    fn drive(&self, _clock: &Clock, _report: &mut DriveReport) {}

    fn read(&self) -> u64 {
        self.read_value()
    }

    fn publish_samples(&self) -> Option<PublishSamples> {
        self.probe.as_ref().map(|probe| probe.samples())
    }
}

/// B4, a reference counted mutex.
pub struct MutexCell {
    value: Arc<Mutex<u64>>,
    probe: Option<Arc<PublishProbe>>,
}

impl MutexCell {
    /// Creates the mutex at zero.
    pub fn new() -> Self {
        Self {
            value: Arc::new(Mutex::new(0)),
            probe: None,
        }
    }

    /// Creates the mutex with publish latency records enabled.
    pub fn with_probe() -> Self {
        Self {
            value: Arc::new(Mutex::new(0)),
            probe: Some(PublishProbe::new()),
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
    probe: Option<WriterProbe>,
}

impl Submitter for MutexWriter {
    fn submit(&mut self, sample: Sample) {
        let start_ns = self.probe.as_ref().map(WriterProbe::start_now);
        *self.value.lock() = sample.seq;
        if let (Some(probe), Some(start_ns)) = (self.probe.as_mut(), start_ns) {
            let published_ns = probe.publish_now();
            probe.record(sample.seq, start_ns, published_ns);
        }
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
            probe: self.probe.as_ref().map(|probe| probe.writer()),
        }))
    }

    fn drive(&self, _clock: &Clock, _report: &mut DriveReport) {}

    fn read(&self) -> u64 {
        self.read_value()
    }

    fn publish_samples(&self) -> Option<PublishSamples> {
        self.probe.as_ref().map(|probe| probe.samples())
    }
}
