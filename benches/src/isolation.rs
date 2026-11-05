//! One process per row.
//!
//! A row that retires a generation leaves the reclamation thread alive for the next row.
//! Every experiment binary therefore re-execs itself once per row by default.

use std::env;
use std::path::Path;
use std::process::{self, Command};

/// Exit code used when a row fails.
pub const FAILURE: i32 = 1;

/// Command line options shared by every experiment binary.
#[derive(Clone, Debug, Default)]
pub struct Args {
    /// Row this process owns, when one was requested.
    pub row: Option<usize>,
    /// Runs every row in this process. Development only.
    pub row_shared: bool,
    /// Uses smoke sampling and a single repetition.
    pub smoke: bool,
    /// Overrides the repetition count.
    pub reps: Option<u32>,
    /// Run identifier shared with every child process.
    pub run_id: Option<String>,
}

impl Args {
    /// Parses the shared arguments and ignores anything else.
    pub fn parse() -> Self {
        let mut args = Self::default();
        let mut rest = env::args().skip(1);
        while let Some(arg) = rest.next() {
            match arg.as_str() {
                "--row" => args.row = rest.next().and_then(|value| value.parse().ok()),
                "--row-shared" => args.row_shared = true,
                "--smoke" => args.smoke = true,
                "--reps" => args.reps = rest.next().and_then(|value| value.parse().ok()),
                "--run-id" => args.run_id = rest.next(),
                _ => {}
            }
        }
        args
    }

    /// Rows this process should run.
    pub fn selected(&self, rows: usize) -> Vec<usize> {
        match self.row {
            Some(row) if row < rows => vec![row],
            Some(_) => Vec::new(),
            None => (0..rows).collect(),
        }
    }

    /// Repetitions per row.
    pub fn reps(&self, default: u32) -> u32 {
        if self.smoke {
            1
        } else {
            self.reps.unwrap_or(default)
        }
    }
}

/// Whether this process runs rows directly rather than spawning one child per row.
pub fn owns_rows(args: &Args) -> bool {
    args.row_shared || args.row.is_some()
}

/// Re-executes the current binary once per row and waits for each child.
///
/// Every child receives the run identifier, so all rows land in one results directory.
/// Returns true when every child exited successfully.
pub fn spawn_rows(args: &Args, rows: usize, run_id: &str) -> bool {
    let Ok(exe) = env::current_exe() else {
        eprintln!("[harness] the running binary has no path");
        return false;
    };
    if args.smoke {
        eprintln!("[harness] smoke run of {rows} rows");
    }
    let mut ok = true;
    for row in 0..rows {
        ok &= run_child(&exe, args, row, run_id);
    }
    ok
}

/// Runs one child process for one row.
fn run_child(exe: &Path, args: &Args, row: usize, run_id: &str) -> bool {
    let mut cmd = Command::new(exe);
    cmd.arg("--row").arg(row.to_string());
    cmd.arg("--run-id").arg(run_id);
    if args.smoke {
        cmd.arg("--smoke");
    }
    if let Some(reps) = args.reps {
        cmd.arg("--reps").arg(reps.to_string());
    }
    eprintln!("[harness] row {row}");
    match cmd.status() {
        Ok(status) if status.success() => true,
        Ok(status) => {
            eprintln!("[harness] row {row} exited with {status}");
            false
        }
        Err(err) => {
            eprintln!("[harness] row {row} failed to start: {err}");
            false
        }
    }
}

/// Ends a parent process once its rows have run.
///
/// Diverges on both outcomes, so a parent can never fall through into a row it already spawned.
pub fn finish_parent(ok: bool) -> ! {
    process::exit(if ok { 0 } else { FAILURE });
}
