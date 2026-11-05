//! E3, coalescing.
//!
//! K writers each submit M mutations per frame while one consumer reads once per frame. The row
//! reports how many submissions reach the consumer per frame, the cost of the frame on the
//! consumer thread, and whether the value it read is the last one submitted.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use queued_signal::state::{QueuedSignal, WriterDriver};
use queued_signal_benches::candidates::{self, Candidate, Clock, DriveReport, Sample, Submitter};
use queued_signal_benches::csv::{Record, RunSink, Unit};
use queued_signal_benches::isolation::{self, Args};
use queued_signal_benches::manifest;
use queued_signal_benches::pin::PinMap;
use queued_signal_benches::stats;

/// Designs that can take a mutation from another thread.
const CANDIDATES: [&str; 3] = ["b4", "b5", "b7"];
/// Writers per row.
const WRITERS: [usize; 3] = [1, 4, 16];
/// Mutations each writer submits per frame.
const MUTATIONS: [usize; 3] = [1, 8, 64];
/// Frames in one repetition.
const FRAMES: usize = 10;
/// Repetitions per row.
const REPS: u32 = 5;

/// One unit of work.
struct Row {
    candidate: &'static str,
    writers: usize,
    mutations: usize,
}

/// What a row drives on the consumer thread.
struct Bench {
    submitters: Vec<Box<dyn Submitter>>,
    drive: Box<dyn FnMut() -> u64>,
    read: Box<dyn FnMut() -> u64>,
}

/// Writer handle for the signal, which only enqueues.
struct SignalWriter {
    signal: QueuedSignal<Sample>,
}

impl Submitter for SignalWriter {
    fn submit(&mut self, sample: Sample) {
        self.signal.set_value(sample);
    }
}

/// Result of one repetition.
struct RepOut {
    drive_ns: Vec<f64>,
    read_ns: Vec<f64>,
    latency_ns: Vec<f64>,
    coalescing: Vec<f64>,
    fresh: u64,
    frames: u64,
    final_match: u64,
}

fn main() {
    let args = Args::parse();
    let rows = build_rows(args.smoke);

    if !isolation::owns_rows(&args) {
        let sink = RunSink::open("e3", args.run_id.as_deref()).expect("results directory");
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
        eprintln!("[e3] {message}");
        std::process::exit(isolation::FAILURE);
    }

    let sink = RunSink::open("e3", args.run_id.as_deref()).expect("results directory");

    for index in args.selected(rows.len()) {
        let row = &rows[index];
        eprintln!(
            "[e3] {} {} producers, {} mutations each, one resolver",
            row.candidate, row.writers, row.mutations
        );
        let records = run_row(row, &args);
        sink.write(&records).expect("csv");
    }
}

/// Builds the row list, which fixes the `--row` index order.
fn build_rows(smoke: bool) -> Vec<Row> {
    let writers: &[usize] = if smoke { &WRITERS[..1] } else { &WRITERS };
    let mutations: &[usize] = if smoke { &MUTATIONS[..1] } else { &MUTATIONS };
    let mut rows = Vec::new();
    for candidate in CANDIDATES {
        for &writer in writers {
            for &mutation in mutations {
                rows.push(Row {
                    candidate,
                    writers: writer,
                    mutations: mutation,
                });
            }
        }
    }
    rows
}

/// Workload identifier for a mutation count, so the second dimension reaches the file name.
fn workload_for(mutations: usize) -> &'static str {
    match mutations {
        1 => "m1",
        8 => "m8",
        64 => "m64",
        _ => "m-other",
    }
}

/// Writes what each row cannot represent beside the results.
fn write_caveats(sink: &RunSink) {
    let mut out = String::new();
    out.push_str(
        "b4: every mutation takes the lock, so a consumer read waits behind all of them and the \
         deliveries counted here are the consumer reads, not the writes\n",
    );
    out.push_str(
        "b5: every mutation is a message, so the consumer drains all of them and nothing is \
         dropped\n",
    );
    out.push_str(
        "b7: the driver drains the pending set values once per frame and publishes the newest, so \
         the consumer sees one publication per frame\n",
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

/// Builds the writer handles and the two consumer thread actions.
fn prepare(candidate: &str, writers: usize) -> Bench {
    match candidate {
        "b4" | "b5" => {
            let cell: Arc<dyn Candidate> = if candidate == "b4" {
                Arc::from(candidates::build_with_subscribers("b4", 1).expect("b4 exists"))
            } else {
                Arc::from(candidates::build_with_subscribers("b5", 1).expect("b5 exists"))
            };
            let submitters = (0..writers)
                .map(|_| cell.submitter().expect("a writer handle"))
                .collect();
            let clock = Clock::new();
            let mut report = DriveReport::default();
            let drive_cell = cell.clone();
            let read_cell = cell.clone();
            Bench {
                submitters,
                drive: Box::new(move || {
                    report.reset();
                    drive_cell.drive(&clock, &mut report);
                    report.wakes.max(1) as u64
                }),
                read: Box::new(move || read_cell.read()),
            }
        }
        "b7" => {
            let mut driver = WriterDriver::<Sample>::new(Sample {
                seq: 0,
                submitted_ns: 0,
            });
            let state = driver.queued_state.clone();
            let op_tx = driver.op_tx.clone();
            let signal = QueuedSignal::from_parts(state.clone(), None, op_tx);
            let submitters = (0..writers)
                .map(|_| {
                    Box::new(SignalWriter {
                        signal: signal.clone(),
                    }) as Box<dyn Submitter>
                })
                .collect();
            Bench {
                submitters,
                drive: Box::new(move || {
                    driver.tick(Duration::ZERO);
                    1
                }),
                read: Box::new(move || state.read().seq),
            }
        }
        _ => panic!("no candidate named {candidate}"),
    }
}

/// Runs one repetition and returns what it measured.
fn run_rep(row: &Row, frames: usize) -> RepOut {
    let mut bench = prepare(row.candidate, row.writers);
    let clock = Clock::new();
    let go = Arc::new(Barrier::new(row.writers + 1));
    let done = Arc::new(Barrier::new(row.writers + 1));
    let seq = Arc::new(AtomicU64::new(0));
    let last_submit = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::with_capacity(row.writers);
    for mut submitter in bench.submitters.drain(..) {
        let go = go.clone();
        let done = done.clone();
        let seq = seq.clone();
        let last_submit = last_submit.clone();
        let mutations = row.mutations;
        handles.push(std::thread::spawn(move || {
            for _ in 0..frames {
                go.wait();
                for _ in 0..mutations {
                    let sample = clock.stamp(seq.fetch_add(1, Ordering::Relaxed));
                    last_submit.fetch_max(sample.submitted_ns, Ordering::Relaxed);
                    submitter.submit(sample);
                }
                done.wait();
            }
        }));
    }

    let submitted = (row.writers * row.mutations) as f64;
    let mut drive_ns = Vec::with_capacity(frames);
    let mut read_ns = Vec::with_capacity(frames);
    let mut latency_ns = Vec::with_capacity(frames);
    let mut coalescing = Vec::with_capacity(frames);
    let mut fresh = 0u64;
    let mut last_seen = 0u64;

    for _ in 0..frames {
        last_submit.store(0, Ordering::Relaxed);
        go.wait();
        done.wait();
        let expected = seq.load(Ordering::Acquire).saturating_sub(1);

        let start = Instant::now();
        let delivered = (bench.drive)();
        drive_ns.push(start.elapsed().as_nanos() as f64);

        let start = Instant::now();
        let observed = (bench.read)();
        read_ns.push(start.elapsed().as_nanos() as f64);
        last_seen = observed;

        let submitted_ns = last_submit.load(Ordering::Relaxed);
        let read_done_ns = clock.now_ns();
        latency_ns.push(read_done_ns.saturating_sub(submitted_ns) as f64);
        coalescing.push(submitted / delivered.max(1) as f64);
        if observed == expected {
            fresh += 1;
        }
    }

    for handle in handles {
        handle.join().expect("writer thread");
    }

    let final_seq = seq.load(Ordering::Acquire).saturating_sub(1);
    RepOut {
        drive_ns,
        read_ns,
        latency_ns,
        coalescing,
        fresh,
        frames: frames as u64,
        final_match: u64::from(last_seen == final_seq),
    }
}

/// Turns one repetition into CSV records.
fn push_records(records: &mut Vec<Record>, row: &Row, rep: u32, out: RepOut) {
    let RepOut {
        drive_ns,
        read_ns,
        latency_ns,
        coalescing,
        fresh,
        frames,
        final_match,
    } = out;

    let rows = [
        ("drive", Unit::Nanoseconds, stats::summarize(drive_ns)),
        ("read", Unit::Nanoseconds, stats::summarize(read_ns)),
        ("latency", Unit::Nanoseconds, stats::summarize(latency_ns)),
        ("coalescing", Unit::Ratio, stats::summarize(coalescing)),
        ("fresh", Unit::Frames, stats::Stats::single(fresh as f64)),
        ("frames", Unit::Count, stats::Stats::single(frames as f64)),
        (
            "final-match",
            Unit::Count,
            stats::Stats::single(final_match as f64),
        ),
    ];

    for (metric, unit, stats) in rows {
        records.push(Record {
            candidate: row.candidate,
            workload: workload_for(row.mutations),
            param: "writers",
            param_value: row.writers as f64,
            metric,
            unit,
            stats,
            rep,
        });
    }
}
