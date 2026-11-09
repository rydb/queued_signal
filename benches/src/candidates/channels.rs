//! B5, one unbounded channel per subscriber.
//!
//! Writers send to every subscriber and the consumer drains each channel once per frame. The
//! cost grows with the subscriber count, which is the shape the fan out curve is looking for.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use flume::{Receiver, Sender};
use parking_lot::Mutex;

use super::{Candidate, Clock, DriveReport, Sample, Submitter};
use crate::publish::{NO_PUBLISH, PublishProbe, PublishSamples, WriterProbe};

/// B5, one channel per subscriber.
pub struct ChannelPerSubscriber {
    senders: Arc<Vec<Sender<Sample>>>,
    receivers: Mutex<Vec<Receiver<Sample>>>,
    last: AtomicU64,
    probe: Option<Arc<PublishProbe>>,
}

impl ChannelPerSubscriber {
    /// Creates one channel per subscriber.
    pub fn new(subscribers: usize) -> Self {
        Self::build(subscribers, None)
    }

    /// Creates one channel per subscriber with publish latency records enabled.
    pub fn with_probe(subscribers: usize) -> Self {
        Self::build(subscribers, Some(PublishProbe::new()))
    }

    /// Creates the channels and an optional publish probe.
    fn build(subscribers: usize, probe: Option<Arc<PublishProbe>>) -> Self {
        let mut senders = Vec::with_capacity(subscribers);
        let mut receivers = Vec::with_capacity(subscribers);
        for _ in 0..subscribers {
            let (tx, rx) = flume::unbounded();
            senders.push(tx);
            receivers.push(rx);
        }
        Self {
            senders: Arc::new(senders),
            receivers: Mutex::new(receivers),
            last: AtomicU64::new(0),
            probe,
        }
    }

    /// Subscriber count this candidate was built with.
    pub fn subscribers(&self) -> usize {
        self.senders.len()
    }
}

/// Writer handle for B5.
struct ChannelWriter {
    senders: Arc<Vec<Sender<Sample>>>,
    probe: Option<WriterProbe>,
}

impl Submitter for ChannelWriter {
    fn submit(&mut self, sample: Sample) {
        let start_ns = self.probe.as_ref().map(WriterProbe::start_now);
        for sender in self.senders.iter() {
            let _ = sender.send(sample);
        }
        if let (Some(probe), Some(start_ns)) = (self.probe.as_mut(), start_ns) {
            probe.record(sample.seq, start_ns, NO_PUBLISH);
        }
    }
}

impl Candidate for ChannelPerSubscriber {
    fn id(&self) -> &'static str {
        "b5"
    }

    fn caveat(&self) -> &'static str {
        "one allocation per delivered message, cost grows with the subscriber count"
    }

    fn submitter(&self) -> Option<Box<dyn Submitter>> {
        Some(Box::new(ChannelWriter {
            senders: self.senders.clone(),
            probe: self.probe.as_ref().map(|probe| probe.writer()),
        }))
    }

    fn drive(&self, clock: &Clock, report: &mut DriveReport) {
        let work_begin = self.probe.as_ref().map(|probe| probe.now_ns());
        let receivers = self.receivers.lock();
        for receiver in receivers.iter() {
            for sample in receiver.drain() {
                report.applied += 1;
                report.wakes += 1;
                report
                    .latencies_ns
                    .push(clock.now_ns().saturating_sub(sample.submitted_ns));
                if report.last.is_none_or(|last| sample.seq > last.seq) {
                    report.last = Some(sample);
                }
            }
        }
        if let Some(last) = report.last {
            self.last.store(last.seq, Ordering::Release);
            if let Some(probe) = self.probe.as_ref() {
                let work_ns = work_begin
                    .map(|begin| probe.now_ns().saturating_sub(begin))
                    .unwrap_or(0);
                probe.record_generation(last.seq, work_ns);
            }
        }
        report.finished_ns = clock.now_ns();
    }

    fn read(&self) -> u64 {
        self.last.load(Ordering::Acquire)
    }

    fn publish_samples(&self) -> Option<PublishSamples> {
        self.probe.as_ref().map(|probe| probe.samples())
    }
}
