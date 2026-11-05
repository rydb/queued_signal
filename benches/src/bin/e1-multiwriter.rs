//! E1, multi writer contention.
//!
//! Writers submit a timestamped sample and a driver thread resolves the queue. The row reports
//! what a writer pays per submission, what the driver takes to make it visible, the aggregate
//! rate, retained generations, and whether the final value is the newest submission.
//!
//! Every row submits through one queue per signal, so a cross queue order is never assumed. A
//! design that publishes inside `submit` reports no visibility latency, because submission is
//! publication for it.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use queued_signal_benches::candidates::{self, Candidate, Clock, DriveReport};
use queued_signal_benches::csv::{Record, RunSink, Unit};
use queued_signal_benches::isolation::{self, Args};
use queued_signal_benches::manifest;
use queued_signal_benches::pin::{self, PinMap};
use queued_signal_benches::settle;
use queued_signal_benches::stats::{self, Stats};

/// Candidates measured by this experiment.
///
/// B7Z is B7 with a zero runner cadence, so a row separates a frame policy from the cost of
/// publishing as soon as a write lands. B8 keeps B7's signal and runner but attaches one queue per
/// writer, so the two rows differ only in queue topology.
const CANDIDATES: [&str; 8] = ["b3", "b4", "b5", "b6", "b6r", "b7", "b7z", "b8"];
/// Writer placements.
const WORKLOADS: [&str; 2] = ["shared-signal", "separate-signals"];
/// Writer counts.
const WRITERS: [usize; 7] = [1, 2, 4, 8, 16, 32, 64];
/// Repetitions per row.
const REPS: u32 = 30;
/// Submissions per writer per repetition.
const BATCH: usize = 1_000;
/// Submissions discarded before each timed section.
const WARMUP: usize = 100;
/// Bound on visibility samples kept per repetition.
const LATENCY_CAP: usize = 200_000;
/// Bound on the wait for a consumer to observe the newest write.
const OBSERVE_LIMIT: Duration = Duration::from_millis(250);
/// Idle resolver passes timed by the drain probe.
const PROBE_SAMPLES: usize = 2_000;
/// Bound on the wait for the runner to record the pass that ended the newest-wait.
const LATE_WAIT: Duration = Duration::from_millis(2);
/// Sleep between attempts to collect that pass.
const LATE_SLEEP: Duration = Duration::from_micros(100);

/// One unit of work.
struct Row {
    candidate: &'static str,
    workload: &'static str,
    writers: usize,
}

impl Row {
    /// Signals the writers are spread over.
    fn signals(&self) -> usize {
        if self.workload == "separate-signals" {
            self.writers
        } else {
            1
        }
    }
}

/// Result of one repetition.
#[derive(Default)]
struct RepOut {
    submit_ns: Vec<f64>,
    visible_ns: Vec<f64>,
    drain_ns: Vec<f64>,
    drain_idle_ns: Option<f64>,
    drain_idle_p99_ns: Option<f64>,
    throughput: f64,
    retained: u64,
    final_is_newest: f64,
    observe_ns: f64,
}

fn main() {
    let args = Args::parse();
    let rows = build_rows(args.smoke);

    if !isolation::owns_rows(&args) {
        let sink = RunSink::open("e1", args.run_id.as_deref()).expect("results directory");
        write_caveats(&sink);
        let pin_map = PinMap::plan();
        // The parent leaves its affinity alone, because a child inherits it and would only see
        // one core. Each row pins the threads it owns.
        manifest::write(&sink, &pin_map).expect("manifest");
        let ok = isolation::spawn_rows(&args, rows.len(), sink.run_id());
        isolation::finish_parent(ok);
    }

    if !args.smoke
        && let Err(message) = manifest::require_release()
    {
        eprintln!("[e1] {message}");
        std::process::exit(isolation::FAILURE);
    }

    let sink = RunSink::open("e1", args.run_id.as_deref()).expect("results directory");

    let cores = pin::available_cores();
    for index in args.selected(rows.len()) {
        let row = &rows[index];
        eprintln!(
            "[e1] {} {} {} producers, {} signals, one resolver per signal",
            row.candidate,
            row.workload,
            row.writers,
            row.signals()
        );
        let records = run_row(row, &args, &cores);
        sink.write(&records).expect("csv");
    }
}

/// Builds the row list, which fixes the `--row` index order.
fn build_rows(smoke: bool) -> Vec<Row> {
    let candidates: &[&'static str] = if smoke {
        &["b3", "b7", "b8"]
    } else {
        &CANDIDATES
    };
    let workloads: &[&'static str] = if smoke {
        &["shared-signal"]
    } else {
        &WORKLOADS
    };
    let writers: &[usize] = if smoke { &[2] } else { &WRITERS };

    let mut rows = Vec::new();
    for &candidate in candidates {
        for &workload in workloads {
            for &count in writers {
                rows.push(Row {
                    candidate,
                    workload,
                    writers: count,
                });
            }
        }
    }
    rows
}

/// Writes what each design cannot represent beside the results.
fn write_caveats(sink: &RunSink) {
    let mut out = String::new();
    for id in CANDIDATES {
        if let Some(candidate) = candidates::build(id) {
            out.push_str(&format!("{id}: {}\n", candidate.caveat()));
        }
    }
    out.push_str(
        "submit: time a writer spends inside the submit call. For b3, b4, b6 and b6r that call \
         publishes, for b5, b7, b7z and b8 it only enqueues\n",
    );
    out.push_str(
        "newest-wait: one sample per repetition, timed from the writers joining to a consumer \
         reading the newest value, bounded by the row limit. The burst is shorter than a frame, so \
         a runner cadence shows its worst phase here rather than its mean\n",
    );
    out.push_str(
        "b7z: the same signal as b7 with a zero runner cadence, so a row separates the frame policy \
         from the cost of publishing as soon as a write lands\n",
    );
    out.push_str(
        "drain: duration of each resolver pass that published, timed on the row clock by the runner \
         that ran it, so the drain is priced beside the write side\n",
    );
    out.push_str(
        "drain-idle: an idle pass of the b8 resolver loop, which reads one non-empty flag per \
         writer queue, summed over the signals in the row. It prices the flag scan with no entries \
         behind it\n",
    );
    std::fs::write(sink.dir().join("caveats.txt"), out).expect("caveats");
}

/// Runs every repetition of one row.
fn run_row(row: &Row, args: &Args, cores: &[usize]) -> Vec<Record> {
    // A row that outlives this budget has hung, so the run fails instead of waiting on it.
    let _deadline = isolation::Deadline::arm(
        Duration::from_secs(30),
        format!("e1 {} {}", row.candidate, row.writers),
    );
    let reps = args.reps(REPS);
    let (batch, warmup) = if args.smoke { (32, 4) } else { (BATCH, WARMUP) };
    let signals = row.signals();
    let mut records = Vec::new();

    for rep in 0..reps {
        let out = run_rep(row.candidate, signals, row.writers, batch, warmup, cores);
        push_records(&mut records, row, rep, out);
    }
    records
}

/// Runs one repetition and returns what it measured.
fn run_rep(
    candidate: &str,
    signals: usize,
    writers: usize,
    batch: usize,
    warmup: usize,
    cores: &[usize],
) -> RepOut {
    let cells: Vec<Arc<dyn Candidate>> = (0..signals)
        .map(|_| {
            Arc::from(
                candidates::build_for_writers(candidate, writers)
                    .unwrap_or_else(|| panic!("no candidate named {candidate}")),
            )
        })
        .collect();
    let seqs: Vec<Arc<AtomicU64>> = (0..signals).map(|_| Arc::new(AtomicU64::new(0))).collect();
    let clock = Clock::new();
    let start = Arc::new(Barrier::new(writers + 1));
    let stop = Arc::new(AtomicBool::new(false));
    let finished = Arc::new(AtomicUsize::new(0));
    let samples = Arc::new(Mutex::new(Vec::with_capacity(writers)));

    let driver = {
        let cells = cells.clone();
        let start = start.clone();
        let stop = stop.clone();
        let core = cores.first().copied();
        std::thread::spawn(move || {
            if let Some(core) = core {
                let _ = pin::pin_current_thread(core);
            }
            let mut report = DriveReport::default();
            let mut latencies = Vec::new();
            let mut drains = Vec::new();
            start.wait();
            while !stop.load(Ordering::Acquire) {
                for cell in cells.iter() {
                    report.reset();
                    cell.drive(&clock, &mut report);
                    if latencies.len() < LATENCY_CAP {
                        latencies.extend_from_slice(&report.latencies_ns);
                    }
                    drains.extend_from_slice(&report.drain_ns);
                }
                std::hint::spin_loop();
            }
            for cell in cells.iter() {
                report.reset();
                cell.drive(&clock, &mut report);
                if latencies.len() < LATENCY_CAP {
                    latencies.extend_from_slice(&report.latencies_ns);
                }
                drains.extend_from_slice(&report.drain_ns);
            }
            (latencies, drains)
        })
    };

    let mut handles = Vec::with_capacity(writers);
    for writer in 0..writers {
        let signal = writer % signals;
        let cell = cells[signal].clone();
        let seq = seqs[signal].clone();
        let start = start.clone();
        let stop = stop.clone();
        let finished = finished.clone();
        let samples = samples.clone();
        let core = if cores.len() > 1 {
            Some(cores[1 + writer % (cores.len() - 1)])
        } else {
            cores.first().copied()
        };
        handles.push(std::thread::spawn(move || {
            if let Some(core) = core {
                let _ = pin::pin_current_thread(core);
            }
            let mut submitter = cell
                .submitter()
                .unwrap_or_else(|| panic!("{} has no writer handle", cell.id()));
            for _ in 0..warmup {
                let sample = clock.stamp(seq.fetch_add(1, Ordering::Relaxed));
                submitter.submit(sample);
            }
            start.wait();
            let begin = Instant::now();
            for _ in 0..batch {
                let sample = clock.stamp(seq.fetch_add(1, Ordering::Relaxed));
                submitter.submit(sample);
            }
            let elapsed = begin.elapsed();
            samples.lock().push((
                elapsed.as_nanos() as f64 / batch as f64,
                elapsed.as_secs_f64(),
            ));
            if finished.fetch_add(1, Ordering::AcqRel) + 1 == writers {
                stop.store(true, Ordering::Release);
            }
        }));
    }

    for handle in handles {
        handle.join().expect("writer thread");
    }
    let (mut visible, mut drains) = driver.join().expect("driver thread");

    // A design that applies in its pass can still hold the last submit when the writers stop, so
    // one final pass on this thread applies whatever the driver's last loop missed.
    let mut final_report = DriveReport::default();
    for cell in cells.iter() {
        final_report.reset();
        cell.drive(&clock, &mut final_report);
        visible.extend_from_slice(&final_report.latencies_ns);
        drains.extend_from_slice(&final_report.drain_ns);
    }

    // The runner publishes on its own thread, so the wait is bounded and reported instead of
    // slept through. A design that publishes inside submit satisfies it immediately.
    let (observe_ns, newest_ok) = observe_newest(&cells, &seqs, OBSERVE_LIMIT);

    // The wait above can cross a resolver tick, so a late pass collects the publish it waited for.
    // A runner records its pass after the value becomes visible, so the collection waits briefly
    // instead of losing the race with the observation that ended the wait.
    if visible.is_empty() && newest_ok {
        let deadline = Instant::now() + LATE_WAIT;
        let mut late_report = DriveReport::default();
        loop {
            let mut seen = false;
            for cell in cells.iter() {
                late_report.reset();
                cell.drive(&clock, &mut late_report);
                seen |= !late_report.latencies_ns.is_empty() || !late_report.drain_ns.is_empty();
                visible.extend_from_slice(&late_report.latencies_ns);
                drains.extend_from_slice(&late_report.drain_ns);
            }
            if seen || Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(LATE_SLEEP);
        }
    }

    // With the writers stopped, an idle pass only reads the flags the resolver checks each frame.
    // A row over several signals pays one pass per signal, so the costs are summed.
    let mut idle_p50 = 0.0;
    let mut idle_p99 = 0.0;
    let mut probed = false;
    for cell in cells.iter() {
        if let Some(sample) = cell.drain_probe(PROBE_SAMPLES) {
            probed = true;
            idle_p50 += sample.p50;
            idle_p99 += sample.p99;
        }
    }

    let mut out = RepOut::default();
    let mut slowest = 0.0f64;
    {
        let samples = samples.lock();
        out.submit_ns = samples.iter().map(|(ns, _)| *ns).collect();
        for (_, elapsed) in samples.iter() {
            slowest = slowest.max(*elapsed);
        }
    }
    if slowest > 0.0 {
        out.throughput = (writers * batch) as f64 / slowest;
    }
    out.visible_ns = visible.into_iter().map(|ns| ns as f64).collect();
    out.drain_ns = drains.into_iter().map(|ns| ns as f64).collect();
    if probed {
        out.drain_idle_ns = Some(idle_p50);
        out.drain_idle_p99_ns = Some(idle_p99);
    }
    out.retained = cells.iter().map(|cell| cell.retained()).sum();
    out.observe_ns = observe_ns as f64;
    out.final_is_newest = f64::from(newest_ok);
    let _ = settle::settle_default(|| cells.iter().map(|cell| cell.retained()).sum());
    out
}

/// Waits until a consumer observes the newest write, and reports how long it waited.
///
/// The wait is bounded, so a design that never publishes the last write reports a timeout instead
/// of turning a policy delay into a correctness failure.
fn observe_newest(
    cells: &[Arc<dyn Candidate>],
    seqs: &[Arc<AtomicU64>],
    limit: Duration,
) -> (u64, bool) {
    let begin = Instant::now();
    let mut spins = 0u32;
    loop {
        let seen = (0..cells.len()).all(|signal| {
            let newest = seqs[signal].load(Ordering::Acquire).saturating_sub(1);
            cells[signal].read() == newest
        });
        if seen {
            return (begin.elapsed().as_nanos() as u64, true);
        }
        if begin.elapsed() >= limit {
            return (begin.elapsed().as_nanos() as u64, false);
        }
        spins = spins.wrapping_add(1);
        if spins.is_multiple_of(1_024) {
            std::thread::yield_now();
        } else {
            std::hint::spin_loop();
        }
    }
}

/// Turns one repetition into CSV records.
fn push_records(records: &mut Vec<Record>, row: &Row, rep: u32, out: RepOut) {
    let RepOut {
        submit_ns,
        visible_ns,
        drain_ns,
        drain_idle_ns,
        drain_idle_p99_ns,
        throughput,
        retained,
        final_is_newest,
        observe_ns,
    } = out;

    let submit = stats::summarize(submit_ns);

    records.push(Record {
        candidate: row.candidate,
        workload: row.workload,
        param: "producers",
        param_value: row.writers as f64,
        metric: "submit",
        unit: Unit::Nanoseconds,
        stats: submit,
        rep,
    });

    if !visible_ns.is_empty() {
        records.push(Record {
            candidate: row.candidate,
            workload: row.workload,
            param: "producers",
            param_value: row.writers as f64,
            metric: "visible",
            unit: Unit::Nanoseconds,
            stats: stats::summarize(visible_ns),
            rep,
        });
    }

    if !drain_ns.is_empty() {
        records.push(Record {
            candidate: row.candidate,
            workload: row.workload,
            param: "producers",
            param_value: row.writers as f64,
            metric: "drain",
            unit: Unit::Nanoseconds,
            stats: stats::summarize(drain_ns),
            rep,
        });
    }

    for (metric, value) in [
        ("drain-idle", drain_idle_ns),
        ("drain-idle-p99", drain_idle_p99_ns),
    ] {
        let Some(value) = value else {
            continue;
        };
        records.push(Record {
            candidate: row.candidate,
            workload: row.workload,
            param: "producers",
            param_value: row.writers as f64,
            metric,
            unit: Unit::Nanoseconds,
            stats: Stats::single(value),
            rep,
        });
    }

    for (metric, unit, value) in [
        ("throughput", Unit::PerSecond, throughput),
        ("retained", Unit::Count, retained as f64),
        ("final-is-newest", Unit::Count, final_is_newest),
        ("newest-wait", Unit::Nanoseconds, observe_ns),
    ] {
        records.push(Record {
            candidate: row.candidate,
            workload: row.workload,
            param: "producers",
            param_value: row.writers as f64,
            metric,
            unit,
            stats: Stats::single(value),
            rep,
        });
    }
}
