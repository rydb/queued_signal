//! E5, frame budget composition.
//!
//! A surface holds N signals. One writer thread submits one mutation to every signal per frame
//! while four panels read overlapping halves of the surface twice per frame, once for layout and
//! once for paint. The writer runs concurrently with the drain and the reads, so a design that
//! locks the value makes the panels wait. The row reports the write side, the drain, the reads,
//! the cost of both threads, the consumer share of a 16.6 ms budget, and the allocations.

use std::hint::black_box;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use allocation_counter::measure;
use queued_signal::state::{FrameRead, QueuedSignal, QueuedState, WriterDriver};
use queued_signal_benches::FRAME_BUDGET_NS;
use queued_signal_benches::candidates::{self, Candidate, Clock, DriveReport, Sample, Submitter};
use queued_signal_benches::csv::{Record, RunSink, Unit};
use queued_signal_benches::isolation::{self, Args};
use queued_signal_benches::manifest;
use queued_signal_benches::pin::{PinMap, Role};
use queued_signal_benches::stats;

/// Designs in the sweep.
const CANDIDATES: [&str; 4] = ["b4", "b5", "b7", "b7f"];
/// Signals on the surface.
const SIGNALS: [usize; 5] = [1, 10, 100, 500, 2_000];
/// Operations submitted per signal per frame.
///
/// A real app spends most frames with no operation at all, so the resolver cost is recorded for
/// both the busy frame and the idle frame.
const OPS: [usize; 2] = [1, 0];
/// Panels reading the surface.
const PANELS: usize = 4;
/// Reads of each covered signal per panel per frame.
const PASSES: usize = 2;
/// Frames in one repetition.
const FRAMES: usize = 10;
/// Repetitions per row.
const REPS: u32 = 5;

/// One unit of work.
struct Row {
    candidate: &'static str,
    signals: usize,
    ops: usize,
}

/// Writer handle for the signal, which only enqueues.
struct SignalWriter {
    signal: QueuedSignal<Sample>,
}

impl Submitter for SignalWriter {
    fn submit(&self, sample: Sample) {
        self.signal.set_value(sample);
    }
}

/// The write side of one frame.
enum Drain {
    /// Designs that publish inside the write, so a frame has nothing to drain.
    Cells(Vec<Arc<dyn Candidate>>),
    /// Designs that drain a queue once per frame.
    Queued(Vec<WriterDriver<Sample>>),
}

impl Drain {
    /// Drains every signal once.
    fn drain(&mut self, clock: &Clock) {
        match self {
            Drain::Cells(cells) => {
                let mut report = DriveReport::default();
                for cell in cells.iter() {
                    report.reset();
                    cell.drive(clock, &mut report);
                }
            }
            Drain::Queued(drivers) => {
                WriterDriver::tick_batch(drivers, Duration::ZERO);
            }
        }
    }
}

/// The panel side read of one frame.
enum Reader {
    /// Designs where a read takes whatever the design offers.
    Cells(Vec<Arc<dyn Candidate>>),
    /// Designs where every read pins again.
    Queued(Vec<QueuedState<Sample>>),
    /// Designs where a panel keeps one scope per signal and reuses its pin.
    Scoped(Vec<FrameRead<Sample>>),
}

impl Reader {
    /// Reads one signal.
    fn read(&mut self, index: usize) -> u64 {
        match self {
            Reader::Cells(cells) => cells[index].read(),
            Reader::Queued(states) => states[index].read().seq,
            Reader::Scoped(scopes) => scopes[index].read().seq,
        }
    }
}

/// Result of one repetition.
struct RepOut {
    submit_ns: Vec<f64>,
    drain_ns: Vec<f64>,
    read_ns: Vec<f64>,
    sync_ns: Vec<f64>,
    allocs: Vec<f64>,
    reads: u64,
}

fn main() {
    let args = Args::parse();
    let rows = build_rows(args.smoke);

    if !isolation::owns_rows(&args) {
        let sink = RunSink::open("e5", args.run_id.as_deref()).expect("results directory");
        write_caveats(&sink);
        let mut pin_map = PinMap::plan();
        pin_map.pin_here(Role::Reader);
        manifest::write(&sink, &pin_map).expect("manifest");
        let ok = isolation::spawn_rows(&args, rows.len(), sink.run_id());
        isolation::finish_parent(ok);
    }

    if !args.smoke
        && let Err(message) = manifest::require_release()
    {
        eprintln!("[e5] {message}");
        std::process::exit(isolation::FAILURE);
    }

    let sink = RunSink::open("e5", args.run_id.as_deref()).expect("results directory");

    for index in args.selected(rows.len()) {
        let row = &rows[index];
        eprintln!(
            "[e5] {} {} signals, {} operations per signal per frame",
            row.candidate, row.signals, row.ops
        );
        let records = run_row(row, &args);
        sink.write(&records).expect("csv");
    }
}

/// Builds the row list, which fixes the `--row` index order.
fn build_rows(smoke: bool) -> Vec<Row> {
    let signals: &[usize] = if smoke { &SIGNALS[..2] } else { &SIGNALS };
    let mut rows = Vec::new();
    for candidate in CANDIDATES {
        for &signals in signals {
            for &ops in &OPS {
                rows.push(Row {
                    candidate,
                    signals,
                    ops,
                });
            }
        }
    }
    rows
}

/// Writes what each row cannot represent beside the results.
fn write_caveats(sink: &RunSink) {
    let mut out = String::new();
    out.push_str(
        "ops-0: the writer submits nothing, so the drain only sees frames with no operation and a zero-ops row measures the idle resolve\n",
    );
    out.push_str(
        "ops-1: one value is submitted to every signal per frame, and the write coalesces to the newest one\n",
    );
    out.push_str("b4: every read takes the lock, so a writer can stall every panel\n");
    out.push_str("b5: every mutation is a message, and the drain runs on the consumer thread\n");
    out.push_str(
        "b7: every panel read pins again and joins the slot array before it loads the value\n",
    );
    out.push_str(
        "b7f: each panel keeps one frame scope per signal, so the pin is reused while the version holds\n",
    );
    out.push_str(
        "all rows: the panels are loops on one thread, so panel count is a read count, not more threads\n",
    );
    out.push_str(
        "all rows: allocations are counted on the consumer thread during the frame, so a message or a value allocated by the writer thread is not in this count\n",
    );
    out.push_str(
        "all rows: the writer runs concurrently with the drain and the reads, so the read phase includes any wait for the writer\n",
    );
    out.push_str(
        "all rows: one writer per signal, so writers never contend with each other here, and that contention is measured in E3 and E4 instead\n",
    );
    std::fs::write(sink.dir().join("caveats.txt"), out).expect("caveats");
}

/// Runs every repetition of one row.
fn run_row(row: &Row, args: &Args) -> Vec<Record> {
    let reps = if args.smoke { 1 } else { args.reps(REPS) };
    let frames = if args.smoke { 2 } else { FRAMES };
    let mut records = Vec::new();
    for rep in 0..reps {
        let out = run_rep(row, frames);
        push_records(&mut records, row, rep, out);
    }
    records
}

/// Workload identifier for an operations-per-frame count.
fn workload_for(ops: usize) -> &'static str {
    match ops {
        0 => "ops-0",
        _ => "ops-1",
    }
}

/// Signals one panel is responsible for, with the ranges overlapping by half.
fn covered(signals: usize, panel: usize) -> Range<usize> {
    let start = panel * signals / PANELS;
    let length = signals.div_ceil(2).max(1);
    start..(start + length).min(signals)
}

/// Builds the drain side, the read side, and one writer handle per signal.
fn build(candidate: &str, signals: usize) -> (Drain, Reader, Vec<Box<dyn Submitter>>) {
    match candidate {
        "b4" | "b5" => {
            let cells: Vec<Arc<dyn Candidate>> = (0..signals)
                .map(|_| {
                    Arc::from(
                        candidates::build_with_subscribers(candidate, 1)
                            .unwrap_or_else(|| panic!("no candidate named {candidate}")),
                    )
                })
                .collect();
            let submitters = cells
                .iter()
                .map(|cell| cell.submitter().expect("a writer handle"))
                .collect();
            let drain_cells = cells.clone();
            (Drain::Cells(drain_cells), Reader::Cells(cells), submitters)
        }
        "b7" | "b7f" => {
            let mut drivers = Vec::with_capacity(signals);
            let mut states = Vec::with_capacity(signals);
            let mut submitters = Vec::with_capacity(signals);
            for _ in 0..signals {
                let driver = WriterDriver::<Sample>::new(Sample {
                    seq: 0,
                    submitted_ns: 0,
                });
                let state = driver.queued_state.clone();
                let op_tx = driver.op_tx.clone();
                let signal = QueuedSignal::from_parts(state.clone(), None, op_tx);
                submitters.push(Box::new(SignalWriter { signal }) as Box<dyn Submitter>);
                states.push(state);
                drivers.push(driver);
            }
            let reader = if candidate == "b7f" {
                let scopes = states.iter().map(|state| state.frame_read()).collect();
                Reader::Scoped(scopes)
            } else {
                Reader::Queued(states)
            };
            (Drain::Queued(drivers), reader, submitters)
        }
        _ => panic!("no candidate named {candidate}"),
    }
}

/// Runs one repetition and returns what it measured.
fn run_rep(row: &Row, frames: usize) -> RepOut {
    let (mut drain, mut reader, submitters) = build(row.candidate, row.signals);
    let ops = row.ops;
    let clock = Clock::new();
    let go = Arc::new(Barrier::new(2));
    let done = Arc::new(Barrier::new(2));
    let seq = Arc::new(AtomicU64::new(0));
    let submit_ns = Arc::new(AtomicU64::new(0));

    let writer = {
        let go = go.clone();
        let done = done.clone();
        let seq = seq.clone();
        let submit_ns = submit_ns.clone();
        std::thread::spawn(move || {
            for _ in 0..frames {
                go.wait();
                let start = Instant::now();
                for submitter in submitters.iter() {
                    for _ in 0..ops {
                        let sample = clock.stamp(seq.fetch_add(1, Ordering::Relaxed));
                        submitter.submit(sample);
                    }
                }
                submit_ns.store(start.elapsed().as_nanos() as u64, Ordering::Release);
                done.wait();
            }
        })
    };

    let mut submits = Vec::with_capacity(frames);
    let mut drain_ns = Vec::with_capacity(frames);
    let mut read_ns = Vec::with_capacity(frames);
    let mut sync_ns = Vec::with_capacity(frames);
    let mut allocs = Vec::with_capacity(frames);
    let mut reads = 0u64;

    for _ in 0..frames {
        go.wait();

        let mut drain_frame = 0u128;
        let mut read_frame = 0u128;
        let info = measure(|| {
            let start = Instant::now();
            drain.drain(&clock);
            let drain_done = Instant::now();
            for panel in 0..PANELS {
                for _ in 0..PASSES {
                    for index in covered(row.signals, panel) {
                        black_box(reader.read(index));
                    }
                }
            }
            let read_done = Instant::now();
            drain_frame = (drain_done - start).as_nanos();
            read_frame = (read_done - drain_done).as_nanos();
        });

        done.wait();
        let submit_frame = submit_ns.load(Ordering::Acquire) as f64;

        let covered_total: usize = (0..PANELS)
            .map(|panel| covered(row.signals, panel).len() * PASSES)
            .sum();
        reads = covered_total as u64;
        submits.push(submit_frame);
        drain_ns.push(drain_frame as f64);
        read_ns.push(read_frame as f64);
        sync_ns.push((drain_frame + read_frame) as f64);
        allocs.push(info.count_total as f64);
    }

    writer.join().expect("writer thread");
    RepOut {
        submit_ns: submits,
        drain_ns,
        read_ns,
        sync_ns,
        allocs,
        reads,
    }
}

/// Turns one repetition into CSV records.
fn push_records(records: &mut Vec<Record>, row: &Row, rep: u32, out: RepOut) {
    let RepOut {
        submit_ns,
        drain_ns,
        read_ns,
        sync_ns,
        allocs,
        reads,
    } = out;

    let submit_per_signal: Vec<f64> = submit_ns
        .iter()
        .map(|ns| ns / row.signals.max(1) as f64)
        .collect();
    let cost: Vec<f64> = submit_ns
        .iter()
        .zip(sync_ns.iter())
        .map(|(submit, sync)| submit + sync)
        .collect();
    let cost_us: Vec<f64> = cost.iter().map(|ns| ns / 1_000.0).collect();
    let sync_us: Vec<f64> = sync_ns.iter().map(|ns| ns / 1_000.0).collect();
    let budget: Vec<f64> = sync_ns
        .iter()
        .map(|ns| ns / FRAME_BUDGET_NS * 100.0)
        .collect();

    let rows = [
        ("submit", Unit::Nanoseconds, stats::summarize(submit_ns)),
        (
            "submit-per-signal",
            Unit::Nanoseconds,
            stats::summarize(submit_per_signal),
        ),
        ("drain", Unit::Nanoseconds, stats::summarize(drain_ns)),
        ("read", Unit::Nanoseconds, stats::summarize(read_ns)),
        ("sync", Unit::Microseconds, stats::summarize(sync_us)),
        ("cost", Unit::Microseconds, stats::summarize(cost_us)),
        ("budget", Unit::Percent, stats::summarize(budget)),
        ("alloc", Unit::Count, stats::summarize(allocs)),
        ("reads", Unit::Count, stats::Stats::single(reads as f64)),
    ];

    for (metric, unit, stats) in rows {
        records.push(Record {
            candidate: row.candidate,
            workload: workload_for(row.ops),
            param: "signals",
            param_value: row.signals as f64,
            metric,
            unit,
            stats,
            rep,
        });
    }
}
