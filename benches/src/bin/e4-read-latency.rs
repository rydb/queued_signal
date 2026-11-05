//! E4, read latency under writer load.
//!
//! Writers publish at maximum rate while one reader reads in a tight loop, and a second mode
//! reads once per frame. The reader owns a core, so a stall is a stall the design caused, not
//! the scheduler moving the reader off the cpu.
//!
//! Each read is timed on its own, so the timer overhead is part of every sample. The clean
//! row in the read cost report is the floor for that overhead.

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use queued_signal_benches::candidates::{self, Candidate, Clock, DriveReport, FrameReader};
use queued_signal_benches::csv::{Record, RunSink, Unit};
use queued_signal_benches::isolation::{self, Args};
use queued_signal_benches::manifest;
use queued_signal_benches::pin::{self, PinMap};
use queued_signal_benches::settle;
use queued_signal_benches::stats::{self, Stats};

/// Candidates that can read while a writer publishes.
const CANDIDATES: [&str; 5] = ["b3", "b4", "b6", "b6r", "b7"];
/// The floor, measured with no writer so its plain cell cannot race.
const FLOOR: &str = "b0";
/// Loaded writer counts.
const WRITERS: [usize; 3] = [1, 8, 32];
/// Repetitions for the tight reader, which takes twenty thousand reads per repetition.
const REPS_TIGHT: u32 = 10;
/// Repetitions for one read per frame.
const REPS_FRAME: u32 = 10;
/// Repetitions for several reads of one signal in a frame.
const REPS_MULTI: u32 = 5;
/// Reads in one tight repetition.
const READS_TIGHT: usize = 20_000;
/// Frames in one repetition of the frame reader.
const FRAMES: usize = 20;
/// Frames in one repetition of the multi read frame reader.
const MULTI_FRAMES: usize = 10;
/// Reads of the same signal inside one frame.
const MULTI_READS: usize = 10;
/// Simulated frame gap.
const FRAME_GAP: Duration = Duration::from_millis(16);
/// Reads slower than this count as a stall.
const READ_BOUND_NS: f64 = 1_000.0;
/// Untimed reads before a repetition.
const WARMUP_READS: usize = 1_000;

/// One unit of work.
struct Row {
    candidate: &'static str,
    mode: &'static str,
    writers: usize,
}

impl Row {
    /// Frames per repetition, reads per frame, and repetitions.
    ///
    /// A zero frame count means the tight reader, which does not model a frame at all.
    fn plan(&self) -> (usize, usize, u32) {
        match self.mode {
            "frame" => (FRAMES, 1, REPS_FRAME),
            "frame-multi" => (MULTI_FRAMES, MULTI_READS, REPS_MULTI),
            _ => (0, READS_TIGHT, REPS_TIGHT),
        }
    }
}

/// Result of one repetition.
struct RepOut {
    read_ns: Vec<f64>,
    over_bound: u64,
    retained: u64,
}

fn main() {
    let args = Args::parse();
    let rows = build_rows(args.smoke);

    if !isolation::owns_rows(&args) {
        let sink = RunSink::open("e4", args.run_id.as_deref()).expect("results directory");
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
        eprintln!("[e4] {message}");
        std::process::exit(isolation::FAILURE);
    }

    let sink = RunSink::open("e4", args.run_id.as_deref()).expect("results directory");

    let cores = pin::available_cores();
    for index in args.selected(rows.len()) {
        let row = &rows[index];
        eprintln!(
            "[e4] {} {} {} producers, one resolver",
            row.candidate, row.mode, row.writers
        );
        let records = run_row(row, &args, &cores);
        sink.write(&records).expect("csv");
    }
}

/// Builds the row list, which fixes the `--row` index order.
///
/// Every candidate gets a zero writer frame row, which keeps the cold cache cost and removes
/// the cross core invalidation. The floor is a plain field, so it can only be read from one
/// thread and appears with no writer at all.
fn build_rows(smoke: bool) -> Vec<Row> {
    if smoke {
        return vec![Row {
            candidate: "b7",
            mode: "tight",
            writers: 2,
        }];
    }

    let mut rows = Vec::new();
    for &candidate in &CANDIDATES {
        for &writers in &WRITERS {
            rows.push(Row {
                candidate,
                mode: "tight",
                writers,
            });
        }
        for &writers in &[0, 1, 8, 32] {
            rows.push(Row {
                candidate,
                mode: "frame",
                writers,
            });
        }
        for &writers in &[0, 1, 32] {
            rows.push(Row {
                candidate,
                mode: "frame-multi",
                writers,
            });
        }
    }
    rows.push(Row {
        candidate: FLOOR,
        mode: "frame",
        writers: 0,
    });
    rows
}

/// Writes what each design cannot represent beside the results.
fn write_caveats(sink: &RunSink) {
    let mut out = String::new();
    for id in candidates::IMPLEMENTED {
        if let Some(candidate) = candidates::build(id) {
            out.push_str(&format!("{id}: {}\n", candidate.caveat()));
        }
    }
    std::fs::write(sink.dir().join("caveats.txt"), out).expect("caveats");
}

/// Runs every repetition of one row.
fn run_row(row: &Row, args: &Args, cores: &[usize]) -> Vec<Record> {
    let (frames, reads_per_frame, reps) = row.plan();
    let reps = if args.smoke { 1 } else { args.reps(reps) };
    let (frames, reads_per_frame) = if args.smoke {
        (0, 200)
    } else {
        (frames, reads_per_frame)
    };
    let mut records = Vec::new();

    for rep in 0..reps {
        let out = run_rep(row.candidate, row.writers, frames, reads_per_frame, cores);
        push_records(&mut records, row, rep, out);
    }
    records
}

/// Runs one repetition and returns what it measured.
///
/// A zero frame count is the tight reader, which shares one scope across the whole burst. A
/// nonzero count creates one scope per frame and drops it at the end of that frame, so a frame
/// reader pays one pin per frame and reuses it only for the reads of that frame.
fn run_rep(
    candidate: &str,
    writers: usize,
    frames: usize,
    reads_per_frame: usize,
    cores: &[usize],
) -> RepOut {
    let cell: Arc<dyn Candidate> = Arc::from(
        candidates::build_for_writers(candidate, writers)
            .unwrap_or_else(|| panic!("no candidate named {candidate}")),
    );
    let clock = Clock::new();
    let seq = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let start = Arc::new(Barrier::new(writers + 2));

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
            start.wait();
            while !stop.load(Ordering::Acquire) {
                report.reset();
                cell.drive(&clock, &mut report);
                std::hint::spin_loop();
            }
            report.reset();
            cell.drive(&clock, &mut report);
        })
    };

    let mut handles = Vec::with_capacity(writers);
    for writer in 0..writers {
        let cell = cell.clone();
        let seq = seq.clone();
        let stop = stop.clone();
        let start = start.clone();
        let core = writer_core(writer, cores);
        handles.push(std::thread::spawn(move || {
            if let Some(core) = core {
                let _ = pin::pin_current_thread(core);
            }
            let submitter = cell
                .submitter()
                .unwrap_or_else(|| panic!("{} has no writer handle", cell.id()));
            start.wait();
            while !stop.load(Ordering::Acquire) {
                let sample = clock.stamp(seq.fetch_add(1, Ordering::Relaxed));
                submitter.submit(sample);
            }
        }));
    }

    if let Some(core) = reader_core(cores) {
        let _ = pin::pin_current_thread(core);
    }

    let mut warm_reader = cell.frame_reader();
    let warmup = if frames == 0 { WARMUP_READS } else { 1 };
    for _ in 0..warmup {
        black_box(match warm_reader.as_mut() {
            Some(reader) => reader.read(),
            None => cell.read(),
        });
    }
    start.wait();

    let total = if frames == 0 {
        reads_per_frame
    } else {
        frames * reads_per_frame
    };
    let mut samples = Vec::with_capacity(total);
    let mut over_bound = 0u64;

    if frames == 0 {
        let mut reader = cell.frame_reader();
        for _ in 0..reads_per_frame {
            timed_read(&*cell, &mut reader, &mut samples, &mut over_bound);
        }
    } else {
        for frame in 0..frames {
            let mut reader = cell.frame_reader();
            for _ in 0..reads_per_frame {
                timed_read(&*cell, &mut reader, &mut samples, &mut over_bound);
            }
            if frame + 1 < frames {
                std::thread::sleep(FRAME_GAP);
            }
        }
    }

    stop.store(true, Ordering::Release);
    for handle in handles {
        handle.join().expect("writer thread");
    }
    driver.join().expect("driver thread");

    let retained = cell.retained();
    let _ = settle::settle_default(|| cell.retained());
    RepOut {
        read_ns: samples,
        over_bound,
        retained,
    }
}

/// Times one read, recording it and counting stalls.
fn timed_read(
    cell: &dyn Candidate,
    reader: &mut Option<Box<dyn FrameReader>>,
    samples: &mut Vec<f64>,
    over_bound: &mut u64,
) {
    let begin = Instant::now();
    let sample = match reader.as_mut() {
        Some(reader) => reader.read(),
        None => cell.read(),
    };
    black_box(sample);
    let ns = begin.elapsed().as_nanos() as f64;
    if ns > READ_BOUND_NS {
        *over_bound += 1;
    }
    samples.push(ns);
}

/// Core for a writer, kept away from the driver and the reader.
fn writer_core(writer: usize, cores: &[usize]) -> Option<usize> {
    if cores.len() <= 2 {
        return cores.last().copied();
    }
    Some(cores[2 + writer % (cores.len() - 2)])
}

/// Core for the reader, kept away from the driver.
fn reader_core(cores: &[usize]) -> Option<usize> {
    cores.get(1).copied().or_else(|| cores.first().copied())
}

/// Turns one repetition into CSV records.
fn push_records(records: &mut Vec<Record>, row: &Row, rep: u32, out: RepOut) {
    let RepOut {
        read_ns,
        over_bound,
        retained,
    } = out;
    let read = stats::summarize(read_ns);

    for (metric, unit, stats) in [
        ("read", Unit::Nanoseconds, read),
        ("over-bound", Unit::Count, Stats::single(over_bound as f64)),
        ("retained", Unit::Count, Stats::single(retained as f64)),
    ] {
        records.push(Record {
            candidate: row.candidate,
            workload: row.mode,
            param: "producers",
            param_value: row.writers as f64,
            metric,
            unit,
            stats,
            rep,
        });
    }
}
