//! E6, the prune policy sweep.
//!
//! One slot array faces a fixed set of readers, each reading on its own cadence. Every frame
//! advances the pass, publishes a version, and then joins the readers that read this frame. A
//! reader that skips more passes than the threshold allows loses its slot, and a reader that
//! comes back claims a slot again.
//!
//! The row reports how many slots were marked, how many were pruned, how many stayed occupied,
//! and what the join and the publish cost. The wake to render link is covered by the render count
//! test, so this row measures the slot state machine on its own.

use std::time::Instant;

use queued_signal::subscriber_slots::{PassEpoch, SubscriberSlots};
use queued_signal_benches::csv::{Record, RunSink, Unit};
use queued_signal_benches::isolation::{self, Args};
use queued_signal_benches::manifest;
use queued_signal_benches::pin::PinMap;
use queued_signal_benches::stats;

/// Readers sharing one slot array.
const READERS: usize = 1_024;
/// Prune thresholds, in passes.
const THRESHOLDS: [u64; 4] = [0, 1, 2, 3];
/// Reader cadences, in frames.
const CADENCES: [usize; 4] = [1, 2, 3, 5];
/// Frames in one repetition.
const FRAMES: usize = 40;
/// Repetitions per row.
const REPS: u32 = 5;

/// One unit of work.
struct Row {
    threshold: u64,
    cadence: usize,
}

/// Result of one repetition.
struct RepOut {
    marks: Vec<f64>,
    pruned: Vec<f64>,
    occupancy: Vec<f64>,
    join_ns: Vec<f64>,
    visit_ns: Vec<f64>,
}

fn main() {
    let args = Args::parse();
    let rows = build_rows(args.smoke);

    if !isolation::owns_rows(&args) {
        let sink = RunSink::open("e6", args.run_id.as_deref()).expect("results directory");
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
        eprintln!("[e6] {message}");
        std::process::exit(isolation::FAILURE);
    }

    let sink = RunSink::open("e6", args.run_id.as_deref()).expect("results directory");

    for index in args.selected(rows.len()) {
        let row = &rows[index];
        eprintln!("[e6] threshold {} cadence {}", row.threshold, row.cadence);
        let records = run_row(row, &args);
        sink.write(&records).expect("csv");
    }
}

/// Builds the row list, which fixes the `--row` index order.
fn build_rows(smoke: bool) -> Vec<Row> {
    let thresholds: &[u64] = if smoke { &THRESHOLDS[..2] } else { &THRESHOLDS };
    let cadences: &[usize] = if smoke { &CADENCES[..2] } else { &CADENCES };
    let mut rows = Vec::new();
    for &threshold in thresholds {
        for &cadence in cadences {
            rows.push(Row { threshold, cadence });
        }
    }
    rows
}

/// Writes what each row cannot represent beside the results.
fn write_caveats(sink: &RunSink) {
    let mut out = String::new();
    out.push_str(
        "all rows: readers are plain counters, so a mark is measured and the render it would cause is covered by the render count test\n",
    );
    out.push_str(
        "all rows: a publish happens before the joins of the same frame, so every occupied slot is marked once per publish, which is the cost a threshold pays for keeping an absent reader\n",
    );
    out.push_str(
        "all rows: a returning reader claims a slot again, so its join pays the scan and the slot may move\n",
    );
    std::fs::write(sink.dir().join("caveats.txt"), out).expect("caveats");
}

/// Runs every repetition of one row.
fn run_row(row: &Row, args: &Args) -> Vec<Record> {
    let reps = if args.smoke { 1 } else { args.reps(REPS) };
    let frames = if args.smoke { 4 } else { FRAMES };
    let mut records = Vec::new();
    for rep in 0..reps {
        let out = run_rep(row, frames);
        push_records(&mut records, row, rep, out);
    }
    records
}

/// Runs one repetition through the threshold the row names.
fn run_rep(row: &Row, frames: usize) -> RepOut {
    match row.threshold {
        0 => run_threshold::<0>(row.cadence, frames),
        1 => run_threshold::<1>(row.cadence, frames),
        2 => run_threshold::<2>(row.cadence, frames),
        _ => run_threshold::<3>(row.cadence, frames),
    }
}

/// Runs one repetition against one threshold.
fn run_threshold<const THRESHOLD: u64>(cadence: usize, frames: usize) -> RepOut {
    let epoch = PassEpoch::new();
    let slots = SubscriberSlots::<u64, THRESHOLD>::new(epoch.clone());
    let mut version = 0u64;

    let mut marks = Vec::with_capacity(frames);
    let mut pruned = Vec::with_capacity(frames);
    let mut occupancy = Vec::with_capacity(frames);
    let mut join_ns = Vec::with_capacity(frames);
    let mut visit_ns = Vec::with_capacity(frames);

    for frame in 0..frames {
        epoch.begin();
        version += 1;

        let start = Instant::now();
        let stats = slots.visit(version, |_| true);
        visit_ns.push(start.elapsed().as_nanos() as f64);

        let reads = frame.is_multiple_of(cadence);
        let start = Instant::now();
        if reads {
            for context in 0..READERS as u64 {
                slots.join(context, version);
            }
        }
        join_ns.push(start.elapsed().as_nanos() as f64);

        marks.push(stats.marked as f64);
        pruned.push(stats.pruned as f64);
        occupancy.push(slots.len() as f64);
    }

    // A reader on every frame survives any threshold above zero, because its slot is one pass old
    // when the next publish visits it.
    if cadence == 1 && THRESHOLD > 0 {
        assert_eq!(
            occupancy[occupancy.len() - 1],
            READERS as f64,
            "a reader that reads every frame must keep its slot"
        );
        assert_eq!(
            pruned.iter().sum::<f64>(),
            0.0,
            "a reader that reads every frame must never be pruned"
        );
        assert!(
            marks[1..].iter().all(|marked| *marked == READERS as f64),
            "a busy reader must be marked once per publish"
        );
    }
    // The array never holds more slots than readers.
    assert!(
        occupancy.iter().all(|slots| *slots <= READERS as f64),
        "occupancy grew past the reader count"
    );

    RepOut {
        marks,
        pruned,
        occupancy,
        join_ns,
        visit_ns,
    }
}

/// Turns one repetition into CSV records.
fn push_records(records: &mut Vec<Record>, row: &Row, rep: u32, out: RepOut) {
    let RepOut {
        marks,
        pruned,
        occupancy,
        join_ns,
        visit_ns,
    } = out;

    // Occupancy is bimodal when a reader skips frames, so the run is reported at three points
    // rather than as one median, and the prune count is reported as a total for the run.
    let last = occupancy.len() - 1;
    let mid = last / 2;
    let rows = [
        ("marks", Unit::Count, stats::summarize(marks.clone())),
        (
            "marks-total",
            Unit::Count,
            stats::Stats::single(marks.iter().sum()),
        ),
        ("pruned", Unit::Count, stats::summarize(pruned.clone())),
        (
            "prunes-total",
            Unit::Count,
            stats::Stats::single(pruned.iter().sum()),
        ),
        (
            "occupancy-start",
            Unit::Count,
            stats::Stats::single(occupancy[0]),
        ),
        (
            "occupancy-mid",
            Unit::Count,
            stats::Stats::single(occupancy[mid]),
        ),
        (
            "occupancy-end",
            Unit::Count,
            stats::Stats::single(occupancy[last]),
        ),
        ("join", Unit::Nanoseconds, stats::summarize(join_ns)),
        ("visit", Unit::Nanoseconds, stats::summarize(visit_ns)),
    ];

    for (metric, unit, stats) in rows {
        records.push(Record {
            candidate: "b7",
            workload: workload_for(row.cadence),
            param: "threshold",
            param_value: row.threshold as f64,
            metric,
            unit,
            stats,
            rep,
        });
    }
}

/// Workload identifier for a reader cadence.
fn workload_for(cadence: usize) -> &'static str {
    match cadence {
        1 => "every-1",
        2 => "every-2",
        3 => "every-3",
        5 => "every-5",
        _ => "every-other",
    }
}
