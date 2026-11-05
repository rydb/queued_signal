//! Runs every experiment once with smoke sampling.
//!
//! The test never measures anything reportable. It checks that each experiment binary builds its
//! rows, spawns its row processes, and writes at least one record, so a broken harness fails here
//! instead of during a measurement pass.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Workspace root, so every artifact lands in the same results directory as a real run.
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the bench crate sits inside the workspace")
        .to_path_buf()
}

/// Runs one experiment in smoke mode and checks its records.
fn smoke(experiment: &str, binary: &str) {
    let root = workspace_root();
    let run_id = format!("smoke-{}-{experiment}", std::process::id());
    let status = Command::new(binary)
        .args(["--smoke", "--run-id", &run_id])
        .current_dir(&root)
        .status()
        .unwrap_or_else(|error| panic!("{experiment} did not start: {error}"));
    assert!(status.success(), "{experiment} exited with {status}");

    let dir = root.join("results").join(&run_id).join(experiment);
    let mut records = 0usize;
    let mut files = 0usize;
    for entry in fs::read_dir(&dir).unwrap_or_else(|error| panic!("{experiment}: {error}")) {
        let path = entry.expect("directory entry").path();
        if path.extension().is_some_and(|extension| extension == "csv") {
            let text = fs::read_to_string(&path).expect("csv");
            let rows = text.lines().count().saturating_sub(1);
            assert!(rows > 0, "{} has no records", path.display());
            records += rows;
            files += 1;
        }
    }
    assert!(files > 0, "{experiment} wrote no csv file");
    assert!(records > 0, "{experiment} wrote no record");
    fs::remove_dir_all(root.join("results").join(&run_id)).ok();
}

#[test]
fn every_experiment_runs_in_smoke_mode() {
    smoke("e1", env!("CARGO_BIN_EXE_e1-multiwriter"));
    smoke("e2", env!("CARGO_BIN_EXE_e2-fanout"));
    smoke("e3", env!("CARGO_BIN_EXE_e3-coalescing"));
    smoke("e4", env!("CARGO_BIN_EXE_e4-read-latency"));
    smoke("e5", env!("CARGO_BIN_EXE_e5-frame-budget"));
    smoke("e6", env!("CARGO_BIN_EXE_e6-prune-policy"));
    smoke("e7", env!("CARGO_BIN_EXE_e7-memory"));
    smoke("e8", env!("CARGO_BIN_EXE_e8-hot-writer"));
}
