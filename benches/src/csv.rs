//! Fixed schema CSV output, one file per candidate and workload.
//!
//! Every value in a row is an identifier chosen by the experiment, so no field needs
//! quoting. The schema is frozen so the report generator can rely on the column order.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::stats::Stats;

/// Unit of a metric, named once and reused by every table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unit {
    /// Nanoseconds.
    Nanoseconds,
    /// Microseconds.
    Microseconds,
    /// Milliseconds.
    Milliseconds,
    /// Bytes.
    Bytes,
    /// A plain count.
    Count,
    /// A percentage of a budget.
    Percent,
    /// A count of frames.
    Frames,
    /// A ratio of two counts.
    Ratio,
    /// A rate per second.
    PerSecond,
}

impl Unit {
    /// Stable identifier used in the CSV column.
    pub const fn as_str(self) -> &'static str {
        match self {
            Unit::Nanoseconds => "ns",
            Unit::Microseconds => "us",
            Unit::Milliseconds => "ms",
            Unit::Bytes => "bytes",
            Unit::Count => "count",
            Unit::Percent => "percent",
            Unit::Frames => "frames",
            Unit::Ratio => "ratio",
            Unit::PerSecond => "per-second",
        }
    }
}

/// One measured row.
#[derive(Clone, Debug)]
pub struct Record {
    /// Baseline identifier, for example `b7`.
    pub candidate: &'static str,
    /// Workload identifier, for example `one-signal`.
    pub workload: &'static str,
    /// Name of the varying parameter.
    pub param: &'static str,
    /// Value of the varying parameter.
    pub param_value: f64,
    /// Metric name.
    pub metric: &'static str,
    /// Metric unit.
    pub unit: Unit,
    /// Distribution of the metric.
    pub stats: Stats,
    /// Repetition number within this run.
    pub rep: u32,
}

/// Output directory for one experiment run.
pub struct RunSink {
    run_id: String,
    experiment: &'static str,
    dir: PathBuf,
}

impl RunSink {
    /// Creates the directory for one experiment under the results root.
    pub fn create(experiment: &'static str) -> io::Result<Self> {
        Self::for_run(experiment, &run_id())
    }

    /// Opens the directory for a run that already has an identifier.
    ///
    /// A child process gets the identifier from its parent, so every row of one run lands in
    /// the same directory.
    /// Opens the sink for a run, reusing the run id when the caller gave one.
    ///
    /// The parent and every row process must agree on the id, so both sides open the sink here.
    pub fn open(experiment: &'static str, run_id: Option<&str>) -> io::Result<Self> {
        match run_id {
            Some(run_id) => Self::for_run(experiment, run_id),
            None => Self::create(experiment),
        }
    }

    /// Opens the sink for an existing run, creating the experiment directory.
    pub fn for_run(experiment: &'static str, run_id: &str) -> io::Result<Self> {
        let dir = results_root().join(run_id).join(experiment);
        fs::create_dir_all(&dir)?;
        Ok(Self {
            run_id: run_id.to_owned(),
            experiment,
            dir,
        })
    }

    /// Identifier shared by every artifact of one run.
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Directory holding this experiment's artifacts.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Experiment this sink writes.
    pub fn experiment(&self) -> &'static str {
        self.experiment
    }

    /// Writes records grouped by candidate and workload, one file per group.
    pub fn write(&self, records: &[Record]) -> io::Result<Vec<PathBuf>> {
        let mut groups: BTreeMap<(&str, &str), Vec<&Record>> = BTreeMap::new();
        for record in records {
            groups
                .entry((record.candidate, record.workload))
                .or_default()
                .push(record);
        }

        let mut paths = Vec::new();
        for ((candidate, workload), rows) in groups {
            let path = self
                .dir
                .join(format!("{}-{}.csv", slug(candidate), slug(workload)));
            let existed = path.exists();
            let mut out = String::new();
            if !existed {
                out.push_str(
                    "run_id,experiment,candidate,workload,param,param_value,metric,unit,\
                     p50,p99,p999,min,max,samples,rep\n",
                );
            }
            for row in rows {
                out.push_str(&format!(
                    "{},{},{},{},{},{:.6},{},{},{:.3},{:.3},{:.3},{:.3},{:.3},{},{}\n",
                    self.run_id,
                    self.experiment,
                    row.candidate,
                    row.workload,
                    row.param,
                    row.param_value,
                    row.metric,
                    row.unit.as_str(),
                    row.stats.p50,
                    row.stats.p99,
                    row.stats.p999,
                    row.stats.min,
                    row.stats.max,
                    row.stats.samples,
                    row.rep,
                ));
            }
            if existed {
                let mut file = fs::OpenOptions::new().append(true).open(&path)?;
                io::Write::write_all(&mut file, out.as_bytes())?;
            } else {
                fs::write(&path, out)?;
            }
            paths.push(path);
        }
        Ok(paths)
    }
}

/// Results root at the workspace level.
fn results_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("results")
}

/// Identifier for one run, so two runs never overwrite each other.
fn run_id() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    format!("{secs}-{profile}")
}

/// Lowercase name with every other character replaced by a dash.
fn slug(value: &str) -> String {
    value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect()
}
