//! E7, memory footprint.
//!
//! Every row builds a stated number of surfaces. A surface holds a stated number of signals, and
//! every signal mounts a stated number of subscriber scopes. A reading scope claims a slot in the
//! signal's slot array, and an idle scope mounts the same component without reading, so the two
//! rows bound the cost with and without the slot. The row reports resident memory against a
//! baseline taken in the same process before anything is built.

use std::cell::RefCell;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use dioxus::prelude::*;
use dioxus_core::{VNode, VirtualDom};
use queued_signal::atom_coerce::flush_reclamation;
use queued_signal::state::{QueuedState, WriterDriver};

use queued_signal_benches::candidates::Sample;
use queued_signal_benches::csv::{Record, RunSink, Unit};
use queued_signal_benches::isolation::{self, Args};
use queued_signal_benches::manifest;
use queued_signal_benches::pin::PinMap;
use queued_signal_benches::stats::Stats;

/// Surfaces in the sweep.
const SURFACES: [usize; 3] = [1, 2, 4];
/// Signals on one surface.
const SIGNALS_PER_SURFACE: usize = 10;
/// Subscriber scopes per signal.
const SUBSCRIBERS: usize = 100;
/// Repetitions per row.
///
/// One, because an allocator does not return its arena, so a second build in the same process
/// reports almost no growth. Every row already runs in its own process.
const REPS: u32 = 1;
/// Time for the allocator and the runtime to reach a steady resident size.
const SETTLE: Duration = Duration::from_millis(20);

thread_local! {
    /// States the panels of the surface being built read.
    static STATES: RefCell<Option<Arc<Vec<QueuedState<Sample>>>>> = const { RefCell::new(None) };
}

/// Root scope, which provides the states and mounts every panel.
#[component]
fn Root(signals: usize, subscribers: usize, reading: bool) -> Element {
    let states = use_hook(|| {
        STATES.with(|slot| {
            slot.borrow()
                .clone()
                .expect("the row set the states before the build")
        })
    });
    use_context_provider(|| states);
    rsx! {
        for signal in 0..signals {
            for _ in 0..subscribers {
                Panel { signal, reading }
            }
        }
    }
}

/// One subscriber, which reads its signal when the row asks it to.
#[component]
fn Panel(signal: usize, reading: bool) -> Element {
    if reading {
        let states: Arc<Vec<QueuedState<Sample>>> = use_context();
        black_box(states[signal].read().seq);
    }
    VNode::empty()
}

/// One unit of work.
struct Row {
    surfaces: usize,
    mode: &'static str,
}

/// Result of one repetition.
struct RepOut {
    rss: u64,
    baseline: u64,
    slots: u64,
}

fn main() {
    let args = Args::parse();
    let rows = build_rows(args.smoke);

    if !isolation::owns_rows(&args) {
        let sink = RunSink::open("e7", args.run_id.as_deref()).expect("results directory");
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
        eprintln!("[e7] {message}");
        std::process::exit(isolation::FAILURE);
    }

    let sink = RunSink::open("e7", args.run_id.as_deref()).expect("results directory");

    for index in args.selected(rows.len()) {
        let row = &rows[index];
        eprintln!("[e7] {} surfaces, {}", row.surfaces, row.mode);
        let records = run_row(row, &args);
        sink.write(&records).expect("csv");
    }
}

/// Builds the row list, which fixes the `--row` index order.
fn build_rows(smoke: bool) -> Vec<Row> {
    let surfaces: &[usize] = if smoke { &SURFACES[..1] } else { &SURFACES };
    let mut rows = Vec::new();
    for &count in surfaces {
        for mode in ["reading", "idle"] {
            rows.push(Row {
                surfaces: count,
                mode,
            });
        }
    }
    rows
}

/// Writes what each row cannot represent beside the results.
fn write_caveats(sink: &RunSink) {
    let mut out = String::new();
    out.push_str(
        "all rows: resident memory includes the allocator arenas and the renderer runtime, so compare rows inside one run and never across machines\n",
    );
    out.push_str(
        "reading rows: every subscriber is a component scope that claims a slot, so the per subscriber figure covers the scope as well as the slot, and the slot itself is 64 bytes by layout\n",
    );
    out.push_str(
        "idle rows: the same scopes mount without reading, so they hold no slot, and the difference is the slot plus the join path\n",
    );
    out.push_str(
        "all rows: one build per process, and the first surface also pays the renderer runtime, so the largest surface is the marginal figure\n",
    );
    std::fs::write(sink.dir().join("caveats.txt"), out).expect("caveats");
}

/// Runs every repetition of one row.
fn run_row(row: &Row, args: &Args) -> Vec<Record> {
    let reps = if args.smoke { 1 } else { args.reps(REPS) };
    let mut records = Vec::new();
    for rep in 0..reps {
        let out = run_rep(row);
        push_records(&mut records, row, rep, out);
    }
    records
}

/// Runs one repetition and returns resident memory before and after the build.
fn run_rep(row: &Row) -> RepOut {
    let baseline = rss_bytes();
    let reading = row.mode == "reading";
    let mut doms = Vec::with_capacity(row.surfaces);
    let mut drivers = Vec::with_capacity(row.surfaces);

    for _ in 0..row.surfaces {
        let mut surface = Vec::with_capacity(SIGNALS_PER_SURFACE);
        let mut surface_drivers = Vec::with_capacity(SIGNALS_PER_SURFACE);
        for _ in 0..SIGNALS_PER_SURFACE {
            let driver = WriterDriver::<Sample>::new(Sample {
                seq: 0,
                submitted_ns: 0,
            });
            surface.push(driver.queued_state.clone());
            surface_drivers.push(driver);
        }
        STATES.with(|slot| *slot.borrow_mut() = Some(Arc::new(surface)));
        let mut dom = VirtualDom::new_with_props(
            Root,
            RootProps {
                signals: SIGNALS_PER_SURFACE,
                subscribers: SUBSCRIBERS,
                reading,
            },
        );
        dom.rebuild_in_place();
        doms.push(dom);
        drivers.push(surface_drivers);
    }

    STATES.with(|slot| *slot.borrow_mut() = None);
    std::thread::sleep(SETTLE);
    flush_reclamation();
    let rss = rss_bytes();

    // The states live as long as their drivers, so the row keeps the drivers alive until the
    // measurement is taken.
    black_box(&drivers);
    black_box(&doms);

    RepOut {
        rss,
        baseline,
        slots: if reading {
            (row.surfaces * SIGNALS_PER_SURFACE * SUBSCRIBERS) as u64
        } else {
            0
        },
    }
}

/// Resident set size in bytes, read from the kernel.
fn rss_bytes() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kib = rest
                .split_whitespace()
                .next()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(0);
            return kib * 1024;
        }
    }
    0
}

/// Turns one repetition into CSV records.
fn push_records(records: &mut Vec<Record>, row: &Row, rep: u32, out: RepOut) {
    let RepOut {
        rss,
        baseline,
        slots,
    } = out;

    let delta = rss.saturating_sub(baseline) as f64;
    let per_surface = delta / row.surfaces as f64;
    let per_signal = delta / (row.surfaces * SIGNALS_PER_SURFACE) as f64;
    let per_subscriber = if slots > 0 { delta / slots as f64 } else { 0.0 };

    let rows = [
        ("rss", Unit::Bytes, Stats::single(rss as f64)),
        ("baseline", Unit::Bytes, Stats::single(baseline as f64)),
        ("per-surface", Unit::Bytes, Stats::single(per_surface)),
        ("per-signal", Unit::Bytes, Stats::single(per_signal)),
        ("per-subscriber", Unit::Bytes, Stats::single(per_subscriber)),
        ("slots", Unit::Count, Stats::single(slots as f64)),
    ];

    for (metric, unit, stats) in rows {
        records.push(Record {
            candidate: "b7",
            workload: row.mode,
            param: "surfaces",
            param_value: row.surfaces as f64,
            metric,
            unit,
            stats,
            rep,
        });
    }
}
