//! Read and write cost measurements.
//!
//! This run captures the slot based path with the atomic version and health words. The report is
//! written to `tests/after_read_atomics.md` next to this file.
//!
//! Every value is a batch average on one thread with no contention. Each row
//! picks one unit from its p50 so the columns stay comparable.

use queued_signal::atom_coerce::AtomCoerce;
use queued_signal::state::WriterDriver;
use std::fs;
use std::hint::black_box;
use std::path::PathBuf;
use std::time::{Duration, Instant};

/// Number of batch samples per benchmark.
const SAMPLES: usize = 300;
/// Default number of operations per batch.
const BATCH: usize = 2_000;

/// One row of the report.
struct Stats {
    p50: f64,
    p99: f64,
    p999: f64,
    min: f64,
    max: f64,
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn finish(mut samples: Vec<f64>) -> Stats {
    samples.sort_by(|a, b| a.total_cmp(b));
    Stats {
        p50: percentile(&samples, 50.0),
        p99: percentile(&samples, 99.0),
        p999: percentile(&samples, 99.9),
        min: samples[0],
        max: samples[samples.len() - 1],
    }
}

fn stats(mut f: impl FnMut()) -> Stats {
    stats_batched(BATCH, &mut f)
}

fn stats_batched(batch: usize, f: &mut impl FnMut()) -> Stats {
    for _ in 0..batch {
        f();
    }
    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let start = Instant::now();
        for _ in 0..batch {
            f();
        }
        samples.push(start.elapsed().as_nanos() as f64 / batch as f64);
    }
    finish(samples)
}

/// Chooses one unit for a row from its median value.
fn unit_for(ns: f64) -> (f64, &'static str) {
    if ns >= 1_000_000.0 {
        (1_000_000.0, "ms")
    } else if ns >= 1_000.0 {
        (1_000.0, "us")
    } else {
        (1.0, "ns")
    }
}

fn cell(ns: f64, divisor: f64, unit: &str) -> String {
    format!("{:.2} {unit}", ns / divisor)
}

fn find<'a>(rows: &'a [(&'static str, String, Stats)], name: &str) -> &'a Stats {
    &rows
        .iter()
        .find(|(_, row_name, _)| row_name == name)
        .unwrap_or_else(|| panic!("missing benchmark row: {name}"))
        .2
}

fn write_report(rows: &[(&'static str, String, Stats)]) -> std::io::Result<PathBuf> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("after_read_atomics.md");

    let mut out = String::new();
    out.push_str("# Read and write costs after the atomic version and health words\n\n");
    out.push_str("The tracked read joins the subscriber slot array, and version and health loads use atomics.\n\n");
    out.push_str(&format!(
        "- Profile: {}\n",
        if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        }
    ));
    out.push_str(&format!(
        "- Samples: {SAMPLES} batch averages of {BATCH} operations\n"
    ));
    out.push_str("- Threads: 1, uncontended\n");
    out.push_str("- Units: ns is nanoseconds, us is microseconds, ms is milliseconds\n");

    let mut current = "";
    for (section, name, stats) in rows {
        if *section != current {
            current = section;
            out.push_str(&format!("\n## {section}\n\n"));
            out.push_str("| Operation | p50 | p99 | p999 | min | max |\n");
            out.push_str("|---|---:|---:|---:|---:|---:|\n");
        }
        let (divisor, unit) = unit_for(stats.p50);
        out.push_str(&format!(
            "| {name} | {} | {} | {} | {} | {} |\n",
            cell(stats.p50, divisor, unit),
            cell(stats.p99, divisor, unit),
            cell(stats.p999, divisor, unit),
            cell(stats.min, divisor, unit),
            cell(stats.max, divisor, unit),
        ));
    }

    let tracked = find(rows, "typed read, tracked").p50;
    let pin_only = find(rows, "typed read, pin and load").p50;
    let projection = find(rows, "read cached projection").p50;
    let peek = find(rows, "peek version, atomic load").p50;
    let health = find(rows, "health, atomic load").p50;
    let store = find(rows, "typed store").p50;
    let publish = find(rows, "publish, watch notify only").p50;
    let publish_value = find(rows, "publish value, store and notify").p50;

    out.push_str("\n## Summary\n\n");
    out.push_str(&format!(
        "- The tracked read costs {tracked:.2} ns and pin and load costs {pin_only:.2} ns, so the slot join adds {:.2} ns.\n",
        tracked - pin_only
    ));
    out.push_str(&format!(
        "- A cached projection costs {projection:.2} ns, about {:.0} percent of the tracked read.\n",
        projection / tracked * 100.0
    ));
    out.push_str(&format!(
        "- Peeking the version costs {peek:.2} ns and reading health costs {health:.2} ns, both one atomic load.\n"
    ));
    out.push_str(&format!(
        "- The typed store costs {store:.2} ns, the watch notification {publish:.2} ns, and store plus notification {publish_value:.2} ns.\n"
    ));

    fs::write(&path, out)?;
    Ok(path)
}

/// A small copyable payload with a trivial destructor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
struct Counter(u32);

#[cfg_attr(miri, ignore = "measures wall clock time")]
#[test]
fn baseline_read_write_costs() {
    let mut rows: Vec<(&'static str, String, Stats)> = Vec::new();

    let mut driver = WriterDriver::<Counter>::new(Counter(0));
    let state = driver.queued_state.clone();

    // Read path, current default: pin, load, plus the reader registry ticket.
    rows.push((
        "Read path",
        "typed read, tracked".to_owned(),
        stats(|| {
            let guard = state.read();
            black_box(&*guard);
        }),
    ));
    rows.push((
        "Read path",
        "typed read, pin and load".to_owned(),
        stats(|| {
            let guard = state.cell.read_typed();
            black_box(&*guard);
        }),
    ));

    {
        let guard = state.cell.read_typed();
        rows.push((
            "Read path",
            "typed deref".to_owned(),
            stats(|| {
                black_box(&*guard);
            }),
        ));
    }

    {
        const DROP_BATCH: usize = 200;
        let mut samples = Vec::with_capacity(SAMPLES);
        for _ in 0..SAMPLES {
            let mut guards: Vec<_> = (0..DROP_BATCH).map(|_| state.cell.read_typed()).collect();
            let start = Instant::now();
            for guard in guards.drain(..) {
                drop(guard);
            }
            samples.push(start.elapsed().as_nanos() as f64 / DROP_BATCH as f64);
        }
        rows.push(("Read path", "typed guard drop".to_owned(), finish(samples)));
    }

    rows.push((
        "Read path",
        "read cached projection".to_owned(),
        stats(|| {
            let guard = state.read_cached(|counter| &counter.0);
            black_box(&*guard);
        }),
    ));
    rows.push((
        "Read path",
        "peek version, atomic load".to_owned(),
        stats(|| {
            black_box(state.peek_version());
        }),
    ));
    rows.push((
        "Read path",
        "health, atomic load".to_owned(),
        stats(|| {
            black_box(state.health());
        }),
    ));

    // Tracked read with other guards already held.
    for registered in [1usize, 8, 64] {
        let held: Vec<_> = (0..registered.saturating_sub(1))
            .map(|_| state.read())
            .collect();
        rows.push((
            "Read path",
            format!("tracked read with {registered} held guards"),
            stats(|| {
                let guard = state.read();
                black_box(&*guard);
            }),
        ));
        drop(held);
    }

    // Writer path: version bump and watch notification with no receivers.
    rows.push((
        "Write path",
        "publish, watch notify only".to_owned(),
        stats(|| driver.publish()),
    ));
    rows.push((
        "Write path",
        "publish value, store and notify".to_owned(),
        stats(|| driver.publish_value(Counter(black_box(1)))),
    ));

    // Writer path: health scan over the registered readers.
    for registered in [1usize, 8, 64] {
        let held: Vec<_> = (0..registered).map(|_| state.read()).collect();
        rows.push((
            "Health scan",
            format!("tick health scan with {registered} registered"),
            stats(|| driver.tick(Duration::ZERO)),
        ));
        drop(held);
    }

    // Erased owned read, which clones the shared allocation handle.
    rows.push((
        "Erased read",
        "erased read, owned guard".to_owned(),
        stats(|| {
            let guard = driver.read();
            black_box(&*guard);
        }),
    ));

    // Standalone value cell, without the reader registry or dioxus.
    {
        let atom = AtomCoerce::<Counter>::new(Counter(0));
        rows.push((
            "Value cell",
            "atom read, pin and load".to_owned(),
            stats(|| {
                let guard = atom.read_typed();
                black_box(&*guard);
            }),
        ));
        rows.push((
            "Value cell",
            "typed store".to_owned(),
            stats(|| atom.store(Counter(black_box(1)))),
        ));
        rows.push((
            "Value cell",
            "typed rcu".to_owned(),
            stats(|| atom.rcu(|counter| Counter(counter.0.wrapping_add(1)))),
        ));
    }

    let path = write_report(&rows).expect("write baseline report");
    println!("baseline written to {}", path.display());
}
