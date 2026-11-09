//! E8, one hot writer beside occasional writers.
//!
//! The usual application shape is several handles to one signal where one handle writes often and
//! the rest only push a mutation now and then. This experiment times the hot handle's submit under
//! that load, so a queue per writer can be judged on the shape it is meant to serve.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use queued_signal_benches::candidates::{self, Candidate, Clock, DriveReport, Submitter};
use queued_signal_benches::csv::{Record, RunSink, Unit};
use queued_signal_benches::isolation::{self, Args};
use queued_signal_benches::manifest;
use queued_signal_benches::pin::{self, PinMap};
use queued_signal_benches::settle;
use queued_signal_benches::stats::{self, Stats};

/// Candidates measured by the hot and rare shapes.
const CANDIDATES: [&str; 2] = ["b7", "b8"];
/// Candidates measured by the paced shape, immediate and deferred together.
const PACED_CANDIDATES: [&str; 8] = ["b3", "b4", "b5", "b6", "b6r", "b7", "b7z", "b8"];
/// Writer shapes.
///
/// The rare writers exist to check whether a hot writer's tail depends on them. `one-thread` puts
/// every handle on one thread, which is what a component tree that writes on the render thread
/// looks like. `paced` spreads one write per millisecond over several frames, so a cadence shows
/// as a distribution instead of one sample.
const SHAPES: [&str; 4] = ["hot-only", "rare-frame", "rare-busy", "one-thread"];
/// Occasional writers beside the hot one.
const RARE_WRITERS: usize = 9;
/// Repetitions per row.
const REPS: u32 = 30;
/// Timed window of one repetition.
const WINDOW: Duration = Duration::from_millis(20);
/// Bound on hot samples kept per repetition.
const HOT_CAP: usize = 250_000;
/// Bound on rare samples kept per writer.
const RARE_CAP: usize = 20_000;
/// Gap between rare bursts, matching the frame cadence.
const RARE_PERIOD: Duration = Duration::from_millis(16);
/// Gap between rare bursts in the stress shape.
const BUSY_PERIOD: Duration = Duration::from_micros(100);
/// A hot submit slower than this counts as a stall, in nanoseconds.
const OVER_BOUND_NS: f64 = 1_000.0;
/// Gap between paced writes, one write per millisecond.
const PACE: Duration = Duration::from_millis(1);
/// Writers in the paced row, so the lock designs see contention.
const PACED_WRITERS: usize = 4;
/// Timed window of the paced row, eight frames at the frame cadence.
const PACED_WINDOW: Duration = Duration::from_millis(8 * 16);
/// Timed window of the paced row in smoke mode.
const PACED_SMOKE_WINDOW: Duration = Duration::from_millis(24);
/// Wait for a deferred design to publish the last paced write.
const PACED_SETTLE: Duration = Duration::from_millis(40);
/// Budget for flushing reclamation between repetitions.
///
/// A candidate that keeps one generation alive would otherwise spend the full default guard on
/// every repetition without reclaiming anything more.
const SETTLE_BUDGET: Duration = Duration::from_millis(100);

/// One unit of work.
struct Row {
    candidate: &'static str,
    shape: &'static str,
}

impl Row {
    /// Number of occasional writers in this shape.
    fn rare(&self) -> usize {
        if self.shape == "hot-only" || self.shape == "paced" {
            0
        } else {
            RARE_WRITERS
        }
    }

    /// Gap between rare bursts.
    fn period(&self) -> Duration {
        if self.shape == "rare-busy" {
            BUSY_PERIOD
        } else {
            RARE_PERIOD
        }
    }

    /// Writers the row drives.
    fn writers(&self) -> usize {
        if self.shape == "paced" {
            PACED_WRITERS
        } else {
            1 + self.rare()
        }
    }
}

/// Result of one repetition.
#[derive(Default)]
struct RepOut {
    hot_ns: Vec<f64>,
    rare_ns: Vec<f64>,
    drain_ns: Vec<f64>,
    over_bound: u64,
    throughput: f64,
    retained: u64,
    publish_ns: Vec<u64>,
    publish_total_ns: Vec<u64>,
    publish_work_ns: Vec<u64>,
}

fn main() {
    let args = Args::parse();
    let rows = build_rows(args.smoke);

    if !isolation::owns_rows(&args) {
        let sink = RunSink::open("e8", args.run_id.as_deref()).expect("results directory");
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
        eprintln!("[e8] {message}");
        std::process::exit(isolation::FAILURE);
    }

    let sink = RunSink::open("e8", args.run_id.as_deref()).expect("results directory");

    let cores = pin::available_cores();
    for index in args.selected(rows.len()) {
        let row = &rows[index];
        if row.shape == "paced" {
            let window = if args.smoke {
                PACED_SMOKE_WINDOW
            } else {
                PACED_WINDOW
            };
            eprintln!(
                "[e8] {} paced, {} writers and one reader, one write each every {:?} for {:?}",
                row.candidate, PACED_WRITERS, PACE, window
            );
        } else {
            eprintln!(
                "[e8] {} {} with one hot writer and {} occasional writers",
                row.candidate,
                row.shape,
                row.rare()
            );
        }
        let records = run_row(row, &args, &cores);
        sink.write(&records).expect("csv");
    }
}

/// Builds the row list, which fixes the `--row` index order.
fn build_rows(smoke: bool) -> Vec<Row> {
    let mut rows = Vec::new();
    if smoke {
        for &candidate in &["b3", "b7", "b8"] {
            rows.push(Row {
                candidate,
                shape: "hot-only",
            });
            rows.push(Row {
                candidate,
                shape: "paced",
            });
        }
        return rows;
    }

    for &candidate in &CANDIDATES {
        for &shape in &SHAPES {
            rows.push(Row { candidate, shape });
        }
    }
    for &candidate in &PACED_CANDIDATES {
        rows.push(Row {
            candidate,
            shape: "paced",
        });
    }
    rows
}

/// Writes what each design cannot represent beside the results.
fn write_caveats(sink: &RunSink) {
    let mut out = String::new();
    for id in PACED_CANDIDATES {
        if let Some(candidate) = candidates::build(id) {
            out.push_str(&format!("{id}: {}\n", candidate.caveat()));
        }
    }
    out.push_str(
        "hot-submit: time inside one submit on the writer that writes at its own maximum rate, \
         timed on its own so interference from an occasional writer stays visible in the tail\n",
    );
    out.push_str(
        "rare-submit: time inside one mutation submit on an occasional writer. Each pushes a \
         mutation, which is the path a component takes when it adjusts a value rather than \
         replacing it\n",
    );
    out.push_str(
        "rare-frame: nine occasional writers burst one mutation together every sixteen \
         milliseconds, the shape a per frame system tick has\n",
    );
    out.push_str(
        "rare-busy: the same nine writers burst every hundred microseconds, a stress case that \
         makes interference frequent\n",
    );
    out.push_str(
        "one-thread: one thread owns the hot handle and the nine occasional handles, so a queue \
         per handle means ten queues on one producer thread\n",
    );
    out.push_str(&format!(
        "over-bound: hot submits slower than {OVER_BOUND_NS:.0} nanoseconds, so a rare stall is \
         counted instead of being averaged away\n"
    ));
    out.push_str(&format!(
        "paced: {PACED_WRITERS} writers release one write each every {PACE:?} for {PACED_WINDOW:?}, \
         with one reader running, so a cadence of {RARE_PERIOD:?} produces about {} publishes per \
         repetition\n",
        PACED_WINDOW.as_millis() / RARE_PERIOD.as_millis()
    ));
    out.push_str(
        "publish-latency: time from the writer's submit returning to the moment the value became \
         readable, recorded by the same probe on every design. An immediate design publishes \
         inside submit and reads as the timer floor, b5 publishes at the consumer drain, and b7, \
         b7z and b8 publish at the runner tick. A lock wait that happens inside submit, such as \
         readers holding a shared lock, is part of the submit column and is excluded here by the \
         definition\n",
    );
    out.push_str(
        "publish-total: time from the writer entering submit to the moment the value became \
         readable, so the lock wait and the enqueue are included. The difference from \
         publish-latency is the part of the write spent before it returned\n",
    );
    out.push_str(
        "publish-work: time the publishing side spent from its pass start to readable, so the wait \
         for the runner tick is excluded. For an immediate design the pass is the submit call, so \
         its whole submit is the work; for a resolver design the pass is the drain or tick that \
         published the write\n",
    );
    out.push_str(
        "probe cost: the trace adds one clock read at submit entry, one at the publish point, and \
         one at the write stamp, with four relaxed stores into a ring the writer alone fills. The \
         hot-submit column of the paced row is the submit cost with that trace enabled\n",
    );
    std::fs::write(sink.dir().join("caveats.txt"), out).expect("caveats");
}

/// Runs every repetition of one row.
fn run_row(row: &Row, args: &Args, cores: &[usize]) -> Vec<Record> {
    // A row that outlives this budget has hung, so the run fails instead of waiting on it.
    let _deadline = isolation::Deadline::arm(
        Duration::from_secs(30),
        format!("e8 {} {}", row.candidate, row.shape),
    );
    let reps = args.reps(REPS);
    let window = if row.shape == "paced" {
        if args.smoke {
            PACED_SMOKE_WINDOW
        } else {
            PACED_WINDOW
        }
    } else if args.smoke {
        Duration::from_millis(2)
    } else {
        WINDOW
    };
    let mut records = Vec::new();

    for rep in 0..reps {
        let out = if row.shape == "paced" {
            run_paced_rep(row, window, cores)
        } else {
            run_rep(row, window, cores)
        };
        push_records(&mut records, row, rep, out);
    }
    records
}

/// Runs one repetition of the paced row and returns what it measured.
fn run_paced_rep(row: &Row, window: Duration, cores: &[usize]) -> RepOut {
    let cell: Arc<dyn Candidate> = Arc::from(
        candidates::build_probed(row.candidate, PACED_WRITERS)
            .unwrap_or_else(|| panic!("no candidate named {}", row.candidate)),
    );
    let clock = Clock::new();
    let seq = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    // The main thread, the driver thread, the reader thread, and every writer.
    let start = Arc::new(Barrier::new(PACED_WRITERS + 3));
    // The main thread releases every writer together once per pace period.
    let round = Arc::new(Barrier::new(PACED_WRITERS + 1));

    let driver = {
        let cell = cell.clone();
        let start = start.clone();
        let stop = stop.clone();
        let core = cores.first().copied();
        std::thread::spawn(move || {
            if let Some(core) = core {
                let _ = pin::pin_current_thread(core);
            }
            let mut report = DriveReport::default();
            let mut drains = Vec::new();
            start.wait();
            while !stop.load(Ordering::Acquire) {
                report.reset();
                cell.drive(&clock, &mut report);
                drains.extend_from_slice(&report.drain_ns);
                std::hint::spin_loop();
            }
            report.reset();
            cell.drive(&clock, &mut report);
            drains.extend_from_slice(&report.drain_ns);
            drains
        })
    };

    let reader = {
        let cell = cell.clone();
        let start = start.clone();
        let stop = stop.clone();
        let core = reader_core(cores);
        std::thread::spawn(move || {
            if let Some(core) = core {
                let _ = pin::pin_current_thread(core);
            }
            start.wait();
            while !stop.load(Ordering::Acquire) {
                std::hint::black_box(cell.read());
                std::hint::spin_loop();
            }
        })
    };

    let mut writers = Vec::with_capacity(PACED_WRITERS);
    for writer in 0..PACED_WRITERS {
        let cell = cell.clone();
        let start = start.clone();
        let stop = stop.clone();
        let round = round.clone();
        let seq = seq.clone();
        let core = writer_core(writer, cores);
        writers.push(std::thread::spawn(move || {
            if let Some(core) = core {
                let _ = pin::pin_current_thread(core);
            }
            let mut submitter = cell
                .submitter()
                .unwrap_or_else(|| panic!("{} has no writer handle", cell.id()));
            start.wait();
            let mut local = Vec::new();
            loop {
                round.wait();
                if stop.load(Ordering::Acquire) {
                    break;
                }
                let sample = clock.stamp(seq.fetch_add(1, Ordering::Relaxed));
                let begin = Instant::now();
                submitter.submit(sample);
                local.push(begin.elapsed().as_nanos() as f64);
            }
            local
        }));
    }

    start.wait();
    let deadline = Instant::now() + window;
    let mut next = Instant::now();
    loop {
        next += PACE;
        let now = Instant::now();
        if next > now {
            std::thread::sleep(next - now);
        }
        let last = Instant::now() >= deadline;
        if last {
            stop.store(true, Ordering::Release);
        }
        round.wait();
        if last {
            break;
        }
    }

    let mut hot = Vec::new();
    for writer in writers {
        hot.extend(writer.join().expect("writer thread"));
    }
    reader.join().expect("reader thread");
    let mut drains = driver.join().expect("driver thread");

    // A design that publishes in its drive applies the tail here.
    let mut final_report = DriveReport::default();
    cell.drive(&clock, &mut final_report);
    drains.extend_from_slice(&final_report.drain_ns);

    // A resolver publishes on its own thread, so wait a frame for the last write.
    if matches!(row.candidate, "b7" | "b7z" | "b8") {
        std::thread::sleep(PACED_SETTLE);
        let mut late_report = DriveReport::default();
        cell.drive(&clock, &mut late_report);
        drains.extend_from_slice(&late_report.drain_ns);
    }

    let samples = cell.publish_samples().unwrap_or_default();
    let publish_ns = samples.deferral;
    let publish_total_ns = samples.total;
    let publish_work_ns = samples.work;
    let over_bound = hot.iter().filter(|ns| **ns > OVER_BOUND_NS).count() as u64;
    let throughput = hot.len() as f64 / window.as_secs_f64();
    let retained = cell.retained();
    let _ = settle::settle(|| cell.retained(), SETTLE_BUDGET);
    RepOut {
        hot_ns: hot,
        rare_ns: Vec::new(),
        drain_ns: drains.into_iter().map(|ns| ns as f64).collect(),
        over_bound,
        throughput,
        retained,
        publish_ns,
        publish_total_ns,
        publish_work_ns,
    }
}

/// Runs one repetition and returns what it measured.
fn run_rep(row: &Row, window: Duration, cores: &[usize]) -> RepOut {
    let cell: Arc<dyn Candidate> = Arc::from(
        candidates::build_for_writers(row.candidate, 1 + row.rare())
            .unwrap_or_else(|| panic!("no candidate named {}", row.candidate)),
    );
    let clock = Clock::new();
    let seq = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let rare = row.rare();
    let period = row.period();
    let writers = if row.shape == "one-thread" || rare == 0 {
        1
    } else {
        rare + 1
    };
    let start = Arc::new(Barrier::new(writers + 2));
    let hot_ns = Arc::new(Mutex::new(Vec::<f64>::new()));
    let rare_ns = Arc::new(Mutex::new(Vec::<f64>::new()));

    let driver = {
        let cell = cell.clone();
        let start = start.clone();
        let stop = stop.clone();
        let core = cores.first().copied();
        std::thread::spawn(move || {
            if let Some(core) = core {
                let _ = pin::pin_current_thread(core);
            }
            let mut report = DriveReport::default();
            let mut drains = Vec::new();
            start.wait();
            while !stop.load(Ordering::Acquire) {
                report.reset();
                cell.drive(&clock, &mut report);
                drains.extend_from_slice(&report.drain_ns);
                std::hint::spin_loop();
            }
            report.reset();
            cell.drive(&clock, &mut report);
            drains.extend_from_slice(&report.drain_ns);
            drains
        })
    };

    // Occasional writers release together on a barrier, so a per frame system tick looks like
    // nine writes that land at the same moment. Two barriers bound every round, so the main
    // thread can publish the stop decision before the release and no writer leaves early.
    let round_start = Arc::new(Barrier::new(rare.max(1) + 1));
    let round_end = Arc::new(Barrier::new(rare.max(1) + 1));

    let mut handles = Vec::with_capacity(writers);
    if row.shape == "one-thread" {
        let cell = cell.clone();
        let start = start.clone();
        let stop = stop.clone();
        let hot_ns = hot_ns.clone();
        let rare_ns = rare_ns.clone();
        let core = writer_core(0, cores);
        handles.push(std::thread::spawn(move || {
            if let Some(core) = core {
                let _ = pin::pin_current_thread(core);
            }
            let mut hot = cell
                .submitter()
                .unwrap_or_else(|| panic!("{} has no writer handle", cell.id()));
            let mut occasional: Vec<Box<dyn Submitter>> = (0..rare)
                .map(|_| {
                    cell.submitter()
                        .unwrap_or_else(|| panic!("{} has no writer handle", cell.id()))
                })
                .collect();
            let mut hot_local = Vec::with_capacity(HOT_CAP);
            let mut rare_local = Vec::new();
            let mut next_rare = Instant::now();
            start.wait();
            while !stop.load(Ordering::Acquire) && hot_local.len() < HOT_CAP {
                let sample = clock.stamp(seq.fetch_add(1, Ordering::Relaxed));
                let begin = Instant::now();
                hot.submit(sample);
                hot_local.push(begin.elapsed().as_nanos() as f64);
                if next_rare.elapsed() >= period {
                    for submitter in occasional.iter_mut() {
                        let sample = clock.stamp(seq.fetch_add(1, Ordering::Relaxed));
                        let begin = Instant::now();
                        submitter.submit_mutation(sample);
                        if rare_local.len() < RARE_CAP {
                            rare_local.push(begin.elapsed().as_nanos() as f64);
                        }
                    }
                    next_rare = Instant::now();
                }
            }
            hot_ns.lock().extend(hot_local);
            rare_ns.lock().extend(rare_local);
        }));
    } else {
        let hot = {
            let cell = cell.clone();
            let start = start.clone();
            let stop = stop.clone();
            let hot_ns = hot_ns.clone();
            let seq = seq.clone();
            let core = writer_core(0, cores);
            std::thread::spawn(move || {
                if let Some(core) = core {
                    let _ = pin::pin_current_thread(core);
                }
                let mut submitter = cell
                    .submitter()
                    .unwrap_or_else(|| panic!("{} has no writer handle", cell.id()));
                let mut local = Vec::with_capacity(HOT_CAP);
                start.wait();
                while !stop.load(Ordering::Acquire) && local.len() < HOT_CAP {
                    let sample = clock.stamp(seq.fetch_add(1, Ordering::Relaxed));
                    let begin = Instant::now();
                    submitter.submit(sample);
                    local.push(begin.elapsed().as_nanos() as f64);
                }
                hot_ns.lock().extend(local);
            })
        };
        handles.push(hot);

        for writer in 0..rare {
            let cell = cell.clone();
            let start = start.clone();
            let stop = stop.clone();
            let rare_ns = rare_ns.clone();
            let round_start = round_start.clone();
            let round_end = round_end.clone();
            let seq = seq.clone();
            let core = writer_core(writer + 1, cores);
            handles.push(std::thread::spawn(move || {
                if let Some(core) = core {
                    let _ = pin::pin_current_thread(core);
                }
                let mut submitter = cell
                    .submitter()
                    .unwrap_or_else(|| panic!("{} has no writer handle", cell.id()));
                let mut local = Vec::new();
                start.wait();
                loop {
                    round_start.wait();
                    let stopping = stop.load(Ordering::Acquire);
                    if !stopping {
                        let sample = clock.stamp(seq.fetch_add(1, Ordering::Relaxed));
                        let begin = Instant::now();
                        submitter.submit_mutation(sample);
                        if local.len() < RARE_CAP {
                            local.push(begin.elapsed().as_nanos() as f64);
                        }
                    }
                    round_end.wait();
                    if stopping {
                        break;
                    }
                }
                rare_ns.lock().extend(local);
            }));
        }
    }

    start.wait();
    if rare > 0 && row.shape != "one-thread" {
        // The main thread paces the bursts, so every round has exactly the rare writers plus this
        // thread. The stop decision is published before the round start, so every writer sees it
        // and they all leave through the round end.
        let deadline = Instant::now() + window;
        loop {
            std::thread::sleep(period);
            let last = Instant::now() >= deadline;
            if last {
                stop.store(true, Ordering::Release);
            }
            round_start.wait();
            round_end.wait();
            if last {
                break;
            }
        }
    } else {
        std::thread::sleep(window + Duration::from_millis(2));
        stop.store(true, Ordering::Release);
    }

    for handle in handles {
        handle.join().expect("writer thread");
    }
    let mut drains = driver.join().expect("driver thread");

    // One late pass collects whatever the driver's last loop missed.
    let mut final_report = DriveReport::default();
    cell.drive(&clock, &mut final_report);
    drains.extend_from_slice(&final_report.drain_ns);

    let hot = hot_ns.lock().clone();
    let over_bound = hot.iter().filter(|ns| **ns > OVER_BOUND_NS).count() as u64;
    let throughput = hot.len() as f64 / window.as_secs_f64();
    let retained = cell.retained();
    let _ = settle::settle(|| cell.retained(), SETTLE_BUDGET);
    RepOut {
        hot_ns: hot,
        rare_ns: rare_ns.lock().clone(),
        drain_ns: drains.into_iter().map(|ns| ns as f64).collect(),
        over_bound,
        throughput,
        retained,
        publish_ns: Vec::new(),
        publish_total_ns: Vec::new(),
        publish_work_ns: Vec::new(),
    }
}

/// Core for the reader, kept away from the driver and the writers.
fn reader_core(cores: &[usize]) -> Option<usize> {
    if cores.len() > 1 {
        Some(cores[1])
    } else {
        cores.first().copied()
    }
}

/// Core for a writer, kept away from the driver.
fn writer_core(writer: usize, cores: &[usize]) -> Option<usize> {
    if cores.len() <= 2 {
        return cores.last().copied();
    }
    Some(cores[2 + writer % (cores.len() - 2)])
}

/// Turns one repetition into CSV records.
fn push_records(records: &mut Vec<Record>, row: &Row, rep: u32, out: RepOut) {
    let RepOut {
        hot_ns,
        rare_ns,
        drain_ns,
        over_bound,
        throughput,
        retained,
        publish_ns,
        publish_total_ns,
        publish_work_ns,
    } = out;
    let writers = row.writers();

    records.push(Record {
        candidate: row.candidate,
        workload: row.shape,
        param: "writers",
        param_value: writers as f64,
        metric: "hot-submit",
        unit: Unit::Nanoseconds,
        stats: stats::summarize(hot_ns),
        rep,
    });

    if !publish_ns.is_empty() {
        records.push(Record {
            candidate: row.candidate,
            workload: row.shape,
            param: "writers",
            param_value: writers as f64,
            metric: "publish-latency",
            unit: Unit::Nanoseconds,
            stats: stats::summarize(publish_ns.into_iter().map(|ns| ns as f64).collect()),
            rep,
        });
    }

    if !publish_total_ns.is_empty() {
        records.push(Record {
            candidate: row.candidate,
            workload: row.shape,
            param: "writers",
            param_value: writers as f64,
            metric: "publish-total",
            unit: Unit::Nanoseconds,
            stats: stats::summarize(publish_total_ns.into_iter().map(|ns| ns as f64).collect()),
            rep,
        });
    }

    if !publish_work_ns.is_empty() {
        records.push(Record {
            candidate: row.candidate,
            workload: row.shape,
            param: "writers",
            param_value: writers as f64,
            metric: "publish-work",
            unit: Unit::Nanoseconds,
            stats: stats::summarize(publish_work_ns.into_iter().map(|ns| ns as f64).collect()),
            rep,
        });
    }

    if !rare_ns.is_empty() {
        records.push(Record {
            candidate: row.candidate,
            workload: row.shape,
            param: "writers",
            param_value: writers as f64,
            metric: "rare-submit",
            unit: Unit::Nanoseconds,
            stats: stats::summarize(rare_ns),
            rep,
        });
    }

    if !drain_ns.is_empty() {
        records.push(Record {
            candidate: row.candidate,
            workload: row.shape,
            param: "writers",
            param_value: writers as f64,
            metric: "drain",
            unit: Unit::Nanoseconds,
            stats: stats::summarize(drain_ns),
            rep,
        });
    }

    for (metric, unit, value) in [
        ("over-bound", Unit::Count, over_bound as f64),
        ("throughput", Unit::PerSecond, throughput),
        ("retained", Unit::Count, retained as f64),
    ] {
        records.push(Record {
            candidate: row.candidate,
            workload: row.shape,
            param: "writers",
            param_value: writers as f64,
            metric,
            unit,
            stats: Stats::single(value),
            rep,
        });
    }
}
