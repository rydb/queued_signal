//! B5, one unbounded channel per subscriber.
//!
//! Writers send to every subscriber and the consumer drains each channel once per frame. The
//! cost grows with the subscriber count, which is the shape the fan out curve is looking for.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use flume::{Receiver, Sender};
use parking_lot::Mutex;

use super::{Candidate, Clock, DriveReport, Sample, Submitter};

/// B5, one channel per subscriber.
pub struct ChannelPerSubscriber {
    senders: Arc<Vec<Sender<Sample>>>,
    receivers: Mutex<Vec<Receiver<Sample>>>,
    last: AtomicU64,
}

impl ChannelPerSubscriber {
    /// Creates one channel per subscriber.
    pub fn new(subscribers: usize) -> Self {
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
}

impl Submitter for ChannelWriter {
    fn submit(&mut self, sample: Sample) {
        for sender in self.senders.iter() {
            let _ = sender.send(sample);
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
        }))
    }

    fn drive(&self, clock: &Clock, report: &mut DriveReport) {
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
        }
        report.finished_ns = clock.now_ns();
    }

    fn read(&self) -> u64 {
        self.last.load(Ordering::Acquire)
    }
}
