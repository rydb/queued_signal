//! E2, the fan out curve.
//!
//! One writer publishes once per frame to a fixed number of subscribers, and the row reports the
//! writer side cost beside the UI side cost it puts on each subscriber. A design that keeps the
//! publish cost flat while the per subscriber cost grows is the shape this curve is looking for.
//!
//! The two incumbents are built here rather than in the candidate ladder, because a dioxus runtime
//! and a bevy schedule own their state and have no writer handle another thread could hold.

use std::cell::{Cell, RefCell};
use std::hint::black_box;
use std::time::Instant;

use bevy_app::App;
use bevy_ecs::prelude::*;
use bevy_ecs::system::SystemState;
use dioxus::prelude::*;
use dioxus_core::VirtualDom;
use queued_signal::state::{QueuedState, WriterDriver};
use queued_signal_benches::candidates::{self, Clock, DriveReport, Sample};
use queued_signal_benches::csv::{Record, RunSink, Unit};
use queued_signal_benches::isolation::{self, Args};
use queued_signal_benches::manifest;
use queued_signal_benches::pin::{PinMap, Role};
use queued_signal_benches::settle;
use queued_signal_benches::stats::{self, Stats};

/// Designs in the curve.
const CANDIDATES: [&str; 5] = ["b1", "b2", "b5", "b6", "b7"];
/// Subscriber counts.
const SUBSCRIBERS: [usize; 7] = [1, 4, 16, 64, 128, 256, 1_024];
/// Frames in one repetition.
const FRAMES: usize = 20;
/// Repetitions per row.
const REPS: u32 = 5;

thread_local! {
    /// Handle the signal component created, so the row can write it.
    static SIGNAL_HANDLE: RefCell<Option<Signal<u64>>> = const { RefCell::new(None) };
    /// Component renders of the signal row.
    static SIGNAL_RENDERS: Cell<usize> = const { Cell::new(0) };
    /// State the queued components read during render.
    static QUEUED_STATE: RefCell<Option<QueuedState<Sample>>> = const { RefCell::new(None) };
    /// Component renders of the queued row.
    static QUEUED_RENDERS: Cell<usize> = const { Cell::new(0) };
}

/// Root component of the signal row, which owns the signal and one child per subscriber.
#[component]
fn SignalRoot(subscribers: usize) -> Element {
    let value = use_signal(|| 0u64);
    SIGNAL_HANDLE.with(|slot| *slot.borrow_mut() = Some(value));
    rsx! {
        for _ in 0..subscribers {
            SignalPanel { value }
        }
    }
}

/// One signal subscriber, which reads the handle it is given.
#[component]
fn SignalPanel(value: Signal<u64>) -> Element {
    SIGNAL_RENDERS.with(|count| count.set(count.get() + 1));
    let shown = black_box(*value.read());
    rsx! { "{shown}" }
}

/// Root component of the queued row, which mounts one subscriber scope per reader.
#[component]
fn QueuedRoot(subscribers: usize) -> Element {
    rsx! {
        for _ in 0..subscribers {
            QueuedPanel {}
        }
    }
}

/// One queued subscriber, which reads the state during render so it claims a slot.
#[component]
fn QueuedPanel() -> Element {
    QUEUED_RENDERS.with(|count| count.set(count.get() + 1));
    let shown = QUEUED_STATE.with(|slot| {
        let state = slot.borrow();
        let state = state.as_ref().expect("the row set the state");
        black_box(state.read().seq)
    });
    rsx! { "{shown}" }
}

/// Value a bevy system reads.
#[derive(Resource, Default)]
struct Value(u64);

/// One unit of work.
struct Row {
    candidate: &'static str,
    subscribers: usize,
}

/// Result of one repetition.
///
/// `publish` is the writer side call that makes the value visible, and `frame` is the UI side work
/// that delivers it, which is a render, a drain, or a poll depending on the design.
struct RepOut {
    publish_ns: Vec<f64>,
    frame_ns: Vec<f64>,
    marks: u64,
    renders: u64,
    retained: u64,
}

fn main() {
    let args = Args::parse();
    let rows = build_rows(args.smoke);

    if !isolation::owns_rows(&args) {
        let sink = RunSink::open("e2", args.run_id.as_deref()).expect("results directory");
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
        eprintln!("[e2] {message}");
        std::process::exit(isolation::FAILURE);
    }

    let sink = RunSink::open("e2", args.run_id.as_deref()).expect("results directory");

    for index in args.selected(rows.len()) {
        let row = &rows[index];
        eprintln!("[e2] {} {} subscribers", row.candidate, row.subscribers);
        let records = run_row(row, &args);
        sink.write(&records).expect("csv");
    }
}

/// Builds the row list, which fixes the `--row` index order.
///
/// The bevy row appears once, because a resource registers no reader and a fan out count would
/// describe a subscription the design does not have.
fn build_rows(smoke: bool) -> Vec<Row> {
    let sizes: &[usize] = if smoke {
        &SUBSCRIBERS[..2]
    } else {
        &SUBSCRIBERS
    };
    let mut rows = Vec::new();
    for candidate in CANDIDATES {
        for &subscribers in sizes {
            if candidate == "b2" && subscribers != 1 {
                continue;
            }
            rows.push(Row {
                candidate,
                subscribers,
            });
        }
    }
    rows
}

/// Writes what each row cannot represent beside the results.
fn write_caveats(sink: &RunSink) {
    let mut out = String::new();
    out.push_str(
        "b1: a dioxus runtime is thread confined, a write marks the reading scopes, and the \
         framework does not expose how many it marked, so marks stay at zero and renders are the \
         observable wake count\n",
    );
    out.push_str(
        "b2: returns ecs only, a reader reaches the resource through a system param, and nothing \
         registers for a wake\n",
    );
    out.push_str(
        "b5: every subscriber owns a channel and receives its own message, so the publish cost \
         carries one send per subscriber\n",
    );
    out.push_str(
        "b6: registers no reader, so its frame cost is the measured cost of N polls of one \
         snapshot\n",
    );
    out.push_str(
        "b7: subscribers are VirtualDom scopes that read the queued state during render, and the \
         row publishes once per frame on the calling thread instead of waiting for the hub tick\n",
    );
    std::fs::write(sink.dir().join("caveats.txt"), out).expect("caveats");
}

/// Runs every repetition of one row.
fn run_row(row: &Row, args: &Args) -> Vec<Record> {
    let reps = if args.smoke { 1 } else { args.reps(REPS) };
    let frames = if args.smoke { 2 } else { FRAMES };
    let mut records = Vec::new();
    for rep in 0..reps {
        let out = run_rep(row.candidate, row.subscribers, frames);
        push_records(&mut records, row, rep, out);
    }
    records
}

/// Runs one repetition through the design that names the row.
fn run_rep(candidate: &str, subscribers: usize, frames: usize) -> RepOut {
    match candidate {
        "b1" => run_dioxus_signal(subscribers, frames),
        "b2" => run_bevy_resource(subscribers, frames),
        "b5" => run_channels(subscribers, frames),
        "b6" => run_snapshot(subscribers, frames),
        "b7" => run_queued(subscribers, frames),
        _ => panic!("no candidate named {candidate}"),
    }
}

/// B1, a signal a component keeps and its children read.
///
/// The write only marks the scopes, so the frame that follows is the part that delivers the value.
fn run_dioxus_signal(subscribers: usize, frames: usize) -> RepOut {
    let mut dom = VirtualDom::new_with_props(SignalRoot, SignalRootProps { subscribers });
    dom.rebuild_in_place();
    let mut signal = SIGNAL_HANDLE
        .with(|slot| slot.borrow_mut().take())
        .expect("the root component created the signal");
    SIGNAL_RENDERS.with(|count| count.set(0));

    let mut publish_ns = Vec::with_capacity(frames);
    let mut frame_ns = Vec::with_capacity(frames);
    let mut renders = 0u64;
    for frame in 0..frames {
        let start = Instant::now();
        signal.set(frame as u64 + 1);
        publish_ns.push(start.elapsed().as_nanos() as f64);

        let start = Instant::now();
        black_box(dom.render_immediate_to_vec());
        frame_ns.push(start.elapsed().as_nanos() as f64);
        renders += SIGNAL_RENDERS.with(Cell::take) as u64;
    }

    RepOut {
        publish_ns,
        frame_ns,
        marks: 0,
        renders,
        retained: 0,
    }
}

/// B2, a bevy resource reached through a system param.
fn run_bevy_resource(_subscribers: usize, frames: usize) -> RepOut {
    let mut app = App::new();
    app.init_resource::<Value>();
    let mut write = SystemState::<ResMut<'static, Value>>::new(app.world_mut());
    let mut read = SystemState::<Res<'static, Value>>::new(app.world_mut());

    let mut publish_ns = Vec::with_capacity(frames);
    let mut frame_ns = Vec::with_capacity(frames);
    for frame in 0..frames {
        let start = Instant::now();
        write
            .get_mut(app.world_mut())
            .expect("the row registered the resource")
            .0 = frame as u64 + 1;
        publish_ns.push(start.elapsed().as_nanos() as f64);

        let start = Instant::now();
        app.update();
        frame_ns.push(start.elapsed().as_nanos() as f64);
        let value = read
            .get(app.world())
            .expect("the row registered the resource");
        black_box(value.0);
    }

    RepOut {
        publish_ns,
        frame_ns,
        marks: 0,
        renders: 0,
        retained: 0,
    }
}

/// B5, one channel per subscriber, drained once per frame.
fn run_channels(subscribers: usize, frames: usize) -> RepOut {
    let cell = candidates::build_with_subscribers("b5", subscribers).expect("b5 exists");
    let submitter = cell.submitter().expect("b5 has a writer handle");
    let clock = Clock::new();
    let mut report = DriveReport::default();

    let mut publish_ns = Vec::with_capacity(frames);
    let mut frame_ns = Vec::with_capacity(frames);
    let mut marks = 0u64;
    for frame in 0..frames {
        let start = Instant::now();
        submitter.submit(clock.stamp(frame as u64 + 1));
        publish_ns.push(start.elapsed().as_nanos() as f64);

        let start = Instant::now();
        report.reset();
        cell.drive(&clock, &mut report);
        frame_ns.push(start.elapsed().as_nanos() as f64);
        marks += report.wakes as u64;
    }

    RepOut {
        publish_ns,
        frame_ns,
        marks,
        renders: 0,
        retained: 0,
    }
}

/// B6, one snapshot that every subscriber polls.
fn run_snapshot(subscribers: usize, frames: usize) -> RepOut {
    let cell = candidates::build_with_subscribers("b6", subscribers).expect("b6 exists");
    let submitter = cell.submitter().expect("b6 has a writer handle");
    let clock = Clock::new();

    let mut publish_ns = Vec::with_capacity(frames);
    let mut frame_ns = Vec::with_capacity(frames);
    for frame in 0..frames {
        let start = Instant::now();
        submitter.submit(clock.stamp(frame as u64 + 1));
        publish_ns.push(start.elapsed().as_nanos() as f64);

        let start = Instant::now();
        for _ in 0..subscribers {
            black_box(cell.read());
        }
        frame_ns.push(start.elapsed().as_nanos() as f64);
    }

    RepOut {
        publish_ns,
        frame_ns,
        marks: 0,
        renders: 0,
        retained: 0,
    }
}

/// B7, the queued signal published once per frame.
fn run_queued(subscribers: usize, frames: usize) -> RepOut {
    let mut driver = WriterDriver::<Sample>::new(Sample {
        seq: 0,
        submitted_ns: 0,
    });
    let state = driver.queued_state.clone();
    QUEUED_STATE.with(|slot| *slot.borrow_mut() = Some(state));

    let mut dom = VirtualDom::new_with_props(QueuedRoot, QueuedRootProps { subscribers });
    dom.rebuild_in_place();
    QUEUED_RENDERS.with(|count| count.set(0));

    let mut publish_ns = Vec::with_capacity(frames);
    let mut frame_ns = Vec::with_capacity(frames);
    let mut marks = 0u64;
    let mut renders = 0u64;
    for frame in 0..frames {
        let sample = Sample {
            seq: frame as u64 + 1,
            submitted_ns: 0,
        };
        let start = Instant::now();
        let visit = driver.publish_value_counted(sample);
        publish_ns.push(start.elapsed().as_nanos() as f64);
        marks += visit.marked as u64;

        let start = Instant::now();
        black_box(dom.render_immediate_to_vec());
        frame_ns.push(start.elapsed().as_nanos() as f64);
        renders += QUEUED_RENDERS.with(Cell::take) as u64;
    }

    let retained = driver.queued_state.cell.reclaim_health().retained();
    let _ = settle::settle_default(|| driver.queued_state.cell.reclaim_health().retained());
    QUEUED_STATE.with(|slot| *slot.borrow_mut() = None);

    RepOut {
        publish_ns,
        frame_ns,
        marks,
        renders,
        retained,
    }
}

/// Divides every sample by the subscriber count.
fn scale(samples: &[f64], subscribers: usize) -> Vec<f64> {
    let divisor = subscribers.max(1) as f64;
    samples.iter().map(|ns| ns / divisor).collect()
}

/// Turns one repetition into CSV records.
fn push_records(records: &mut Vec<Record>, row: &Row, rep: u32, out: RepOut) {
    let RepOut {
        publish_ns,
        frame_ns,
        marks,
        renders,
        retained,
    } = out;

    let per_subscriber_ns = scale(&frame_ns, row.subscribers);
    let rows = [
        ("publish", Unit::Nanoseconds, stats::summarize(publish_ns)),
        ("frame", Unit::Nanoseconds, stats::summarize(frame_ns)),
        (
            "per-subscriber",
            Unit::Nanoseconds,
            stats::summarize(per_subscriber_ns),
        ),
        ("marks", Unit::Count, Stats::single(marks as f64)),
        ("renders", Unit::Count, Stats::single(renders as f64)),
        ("retained", Unit::Count, Stats::single(retained as f64)),
    ];

    for (metric, unit, stats) in rows {
        records.push(Record {
            candidate: row.candidate,
            workload: "fanout",
            param: "subscribers",
            param_value: row.subscribers as f64,
            metric,
            unit,
            stats,
            rep,
        });
    }
}
