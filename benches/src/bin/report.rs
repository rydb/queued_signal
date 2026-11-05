//! Generates the run summary and the figures from the CSV files.
//!
//! Every number in the summary comes from a CSV file written by an experiment binary, and every
//! figure is rendered from the same rows. The summary is written to the run directory, so a run
//! carries its own report.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use charton::prelude::*;
use serde_json::Value;

/// Metrics that also get a figure, as experiment and metric.
const FIGURES: [(&str, &str); 4] = [
    ("e2", "per-subscriber"),
    ("e4", "read"),
    ("e5", "cost"),
    ("e6", "prunes-total"),
];

/// Designs in the headline read table, with the label a reader sees.
const HEADLINE_READ: [(&str, &str); 7] = [
    ("b0", "b0 plain field, no producer, floor"),
    ("b3", "b3 rwlock"),
    ("b4", "b4 mutex"),
    ("b6", "b6 snapshot"),
    ("b6r", "b6r rotating snapshot"),
    ("b7", "b7 queued signal"),
    ("b8", "b8 queue per writer"),
];

/// Designs in the headline fan out table, with the label a reader sees.
const HEADLINE_FANOUT: [(&str, &str); 5] = [
    ("b1", "b1 dioxus signal"),
    ("b2", "b2 bevy resource"),
    ("b5", "b5 channel per subscriber"),
    ("b6", "b6 snapshot"),
    ("b7", "b7 queued signal"),
];

/// Loaded producers the headline read column uses.
const HEADLINE_PRODUCERS: f64 = 8.0;

/// Subscribers the headline fan out column uses.
const HEADLINE_SUBSCRIBERS: f64 = 1_024.0;

/// Largest frame read p99 at eight writers that the report accepts, in nanoseconds.
///
/// This is the same microsecond that E4 counts as an over-bound read, so the guard reuses the
/// experiment's own stall definition instead of a hand picked ratio.
const FRAME_READ_BOUND_NS: f64 = 1_000.0;

fn main() {
    let run_dir = match parse_run() {
        Some(dir) => dir,
        None => latest_run(),
    };
    eprintln!("[report] {}", run_dir.display());

    let manifest = read_manifest(&run_dir);
    let mut summary = String::new();
    summary.push_str(&format!("# Run {}\n\n", run_dir.display()));
    if let Some(manifest) = manifest.as_ref() {
        for key in ["profile", "cpu_model", "commit", "kernel", "governor"] {
            if let Some(value) = manifest.get(key) {
                summary.push_str(&format!("- {key}: {value}\n"));
            }
        }
        summary.push('\n');
    }

    if let Some(table) = headline(&run_dir) {
        summary.push_str(&table);
        summary.push('\n');
    }

    let mut experiments: Vec<PathBuf> = fs::read_dir(&run_dir)
        .expect("run directory")
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.is_dir() && path.file_name().is_some_and(|name| name != "figures"))
        .collect();
    experiments.sort();

    let figures_dir = run_dir.join("figures");
    fs::create_dir_all(&figures_dir).expect("figures directory");

    for experiment in experiments {
        let name = experiment
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("unknown")
            .to_string();

        if let Ok(caveats) = fs::read_to_string(experiment.join("caveats.txt")) {
            summary.push_str(&format!("## {name}\n\n"));
            summary.push_str("Caveats:\n\n");
            for line in caveats.lines().filter(|line| !line.trim().is_empty()) {
                summary.push_str(&format!("- {line}\n"));
            }
            summary.push('\n');
        } else {
            summary.push_str(&format!("## {name}\n\n"));
        }

        let tables = collect(&experiment);
        for ((metric, unit), table) in &tables {
            summary.push_str(&format!("### {metric} ({unit})\n\n"));
            summary.push_str(&render_table(table));
            summary.push('\n');

            if FIGURES.contains(&(name.as_str(), metric.as_str())) {
                let path = figures_dir.join(format!("{name}-{metric}.svg"));
                match render_figure(table, &path) {
                    Ok(()) => summary.push_str(&format!("Figure: {}\n\n", path.display())),
                    Err(error) => summary.push_str(&format!("Figure failed: {error}\n\n")),
                }
            }
        }
    }

    let frame_bound = frame_read_bound(&run_dir);
    if let Some((b7, reference)) = frame_bound {
        summary.push_str(&format!(
            "- Frame read bound: b7 p99 {b7:.0} ns at eight writers, beside b6r p50 {reference:.0} ns, limit {FRAME_READ_BOUND_NS:.0} ns\n"
        ));
    }

    let summary_path = run_dir.join("summary.md");
    fs::write(&summary_path, summary).expect("summary");
    eprintln!("[report] wrote {}", summary_path.display());

    if let Some((b7, _)) = frame_bound
        && b7 > FRAME_READ_BOUND_NS
    {
        eprintln!(
            "[report] frame read regression: b7 frame p99 {b7:.0} ns at eight writers is over the {FRAME_READ_BOUND_NS:.0} ns bound"
        );
        std::process::exit(1);
    }
}

/// Median of one column of a metric at one parameter value, when the file exists.
fn median_metric(path: &Path, metric: &str, param: f64, column: usize) -> Option<f64> {
    let text = fs::read_to_string(path).ok()?;
    let mut values = Vec::new();
    for line in text.lines().skip(1) {
        let columns: Vec<&str> = line.split(',').collect();
        if columns.len() < 14 || columns[6] != metric {
            continue;
        }
        if columns[5].parse::<f64>().ok()? != param {
            continue;
        }
        values.push(columns[column].parse::<f64>().ok()?);
    }
    values.sort_by(|a, b| a.total_cmp(b));
    values.get(values.len() / 2).copied()
}

/// b7 frame p99 and b6r frame p50 at eight writers, when both rows are present.
fn frame_read_bound(run_dir: &Path) -> Option<(f64, f64)> {
    let e4 = run_dir.join("e4");
    let b7 = median_metric(&e4.join("b7-frame.csv"), "read", HEADLINE_PRODUCERS, 9)?;
    let reference = median_metric(&e4.join("b6r-frame.csv"), "read", HEADLINE_PRODUCERS, 8)?;
    Some((b7, reference))
}

/// Reads under load and fan out at the largest subscriber count, in one table.
///
/// Every value is a median of the per repetition p50s from the CSVs, so the table cannot say
/// anything the recorded rows do not. A design missing from a run is left out rather than shown
/// with a placeholder.
fn headline(run_dir: &Path) -> Option<String> {
    let e4 = run_dir.join("e4");
    let e2 = run_dir.join("e2");
    let mut out = String::from("## Headline\n\n");
    let mut found = false;

    out.push_str(&format!(
        "Frame read at {} producers, one reader, E4:\n\n",
        HEADLINE_PRODUCERS as usize
    ));
    out.push_str("| Design | p50 | p99 |\n|---|---:|---:|\n");
    for (id, label) in HEADLINE_READ {
        let path = e4.join(format!("{id}-frame.csv"));
        // The floor has no writer, so its only frame row is the zero producer one.
        let producers = if id == "b0" { 0.0 } else { HEADLINE_PRODUCERS };
        let (Some(p50), Some(p99)) = (
            median_metric(&path, "read", producers, 8),
            median_metric(&path, "read", producers, 9),
        ) else {
            continue;
        };
        found = true;
        out.push_str(&format!("| {label} | {p50:.0} ns | {p99:.0} ns |\n"));
    }

    out.push_str(&format!(
        "\nFan out at {} subscribers, E2:\n\n",
        HEADLINE_SUBSCRIBERS as usize
    ));
    out.push_str("| Design | publish | frame per subscriber |\n|---|---:|---:|\n");
    for (id, label) in HEADLINE_FANOUT {
        let path = e2.join(format!("{id}-fanout.csv"));
        let (Some(publish), Some(per_subscriber)) = (
            median_metric(&path, "publish", HEADLINE_SUBSCRIBERS, 8),
            median_metric(&path, "per-subscriber", HEADLINE_SUBSCRIBERS, 8),
        ) else {
            continue;
        };
        found = true;
        out.push_str(&format!(
            "| {label} | {publish:.0} ns | {per_subscriber:.1} ns |\n"
        ));
    }

    if found {
        out.push_str(
            "\nThe read columns are one read timed inside a frame. The fan out columns are one \
             publish and its frame divided by subscribers, so a design that marks nobody shows no \
             per subscriber work. Caveats per design are under each experiment below.\n",
        );
    }
    found.then_some(out)
}

/// Reads `--run`, which is either a run id or a path.
fn parse_run() -> Option<PathBuf> {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--run" {
            let value = args.next()?;
            let direct = PathBuf::from(&value);
            if direct.is_dir() {
                return Some(direct);
            }
            let inferred = Path::new("results").join(&value);
            if inferred.is_dir() {
                return Some(inferred);
            }
            eprintln!("[report] no run named {value}");
            std::process::exit(1);
        }
    }
    None
}

/// Newest run directory that holds a manifest.
fn latest_run() -> PathBuf {
    let mut runs: Vec<PathBuf> = fs::read_dir("results")
        .map(|entries| {
            entries
                .filter_map(|entry| entry.ok().map(|entry| entry.path()))
                .filter(|path| path.join("manifest.json").is_file())
                .collect()
        })
        .unwrap_or_default();
    runs.sort();
    runs.pop().unwrap_or_else(|| {
        eprintln!("[report] no run found under results");
        std::process::exit(1)
    })
}

/// Reads the manifest of a run, when there is one.
fn read_manifest(run_dir: &Path) -> Option<Value> {
    let text = fs::read_to_string(run_dir.join("manifest.json")).ok()?;
    serde_json::from_str(&text).ok()
}

/// One cell of a table: every p50 for one group at one parameter value.
type Table = BTreeMap<i64, Vec<f64>>;

/// Collects every metric of one experiment, keyed by metric, unit, group, and parameter.
fn collect(experiment: &Path) -> BTreeMap<(String, String), BTreeMap<String, Table>> {
    let mut out: BTreeMap<(String, String), BTreeMap<String, Table>> = BTreeMap::new();
    let mut files: Vec<PathBuf> = fs::read_dir(experiment)
        .map(|entries| {
            entries
                .filter_map(|entry| entry.ok().map(|entry| entry.path()))
                .filter(|path| path.extension().is_some_and(|ext| ext == "csv"))
                .collect()
        })
        .unwrap_or_default();
    files.sort();

    let mut workloads: Vec<String> = Vec::new();
    let mut parsed: Vec<Row> = Vec::new();
    for file in files {
        let text = fs::read_to_string(&file).unwrap_or_default();
        for line in text.lines().skip(1) {
            if let Some(row) = Row::parse(line) {
                if !workloads.contains(&row.workload) {
                    workloads.push(row.workload.clone());
                }
                parsed.push(row);
            }
        }
    }
    let shared = workloads.len() <= 1;

    for row in parsed {
        let group = if shared {
            row.candidate.clone()
        } else {
            format!("{} {}", row.candidate, row.workload)
        };
        let key = (row.metric.clone(), row.unit.clone());
        let param = (row.param_value * 1000.0).round() as i64;
        out.entry(key)
            .or_default()
            .entry(group)
            .or_default()
            .entry(param)
            .or_default()
            .push(row.p50);
    }
    out
}

/// One parsed CSV record.
struct Row {
    candidate: String,
    workload: String,
    metric: String,
    unit: String,
    param_value: f64,
    p50: f64,
}

impl Row {
    /// Parses a record line, trusting the frozen schema.
    fn parse(line: &str) -> Option<Self> {
        let columns: Vec<&str> = line.split(',').collect();
        if columns.len() < 14 {
            return None;
        }
        Some(Self {
            candidate: columns[2].to_string(),
            workload: columns[3].to_string(),
            metric: columns[6].to_string(),
            unit: columns[7].to_string(),
            param_value: columns[5].parse().ok()?,
            p50: columns[8].parse().ok()?,
        })
    }
}

/// Renders one metric as a markdown table, with a row per group and a column per parameter.
fn render_table(table: &BTreeMap<String, Table>) -> String {
    let mut params: Vec<i64> = Vec::new();
    for cells in table.values() {
        for param in cells.keys() {
            if !params.contains(param) {
                params.push(*param);
            }
        }
    }
    params.sort();

    let mut out = String::new();
    out.push_str("| design |");
    for param in &params {
        out.push_str(&format!(" {} |", format_param(*param as f64 / 1000.0)));
    }
    out.push('\n');
    out.push_str("|---|");
    for _ in &params {
        out.push_str("---:|");
    }
    out.push('\n');

    for (group, cells) in table {
        out.push_str(&format!("| {group} |"));
        for param in &params {
            match cells.get(param).and_then(|values| median(values)) {
                Some(value) => out.push_str(&format!(" {} |", format_value(value))),
                None => out.push_str(" |"),
            }
        }
        out.push('\n');
    }
    out
}

/// Renders one metric as a line chart, one line per group.
fn render_figure(
    table: &BTreeMap<String, Table>,
    path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut x = Vec::new();
    let mut y = Vec::new();
    let mut series = Vec::new();
    for (group, cells) in table {
        let mut params: Vec<&i64> = cells.keys().collect();
        params.sort();
        for param in params {
            let Some(value) = cells.get(param).and_then(|values| median(values)) else {
                continue;
            };
            x.push(*param as f64 / 1000.0);
            y.push(value);
            series.push(group.clone());
        }
    }
    if x.len() < 3 {
        return Ok(());
    }

    chart!(x, y, series)?
        .mark_line()?
        .encode((alt::x("x"), alt::y("y"), alt::color("series")))?
        .save(path)?;
    Ok(())
}

/// Median of the samples, which is the reported centre.
fn median(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));
    Some(sorted[sorted.len() / 2])
}

/// Parameter formatting, which keeps whole counts whole.
fn format_param(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        format_value(value)
    }
}

/// Compact number formatting for a table cell.
fn format_value(value: f64) -> String {
    if value == 0.0 {
        return "0".to_string();
    }
    if value.abs() >= 100.0 {
        format!("{value:.0}")
    } else if value.abs() >= 10.0 {
        format!("{value:.1}")
    } else {
        format!("{value:.2}")
    }
}
