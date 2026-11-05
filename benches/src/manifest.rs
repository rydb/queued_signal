//! Machine and build metadata written beside every run.
//!
//! The manifest makes a row reproducible or non-reproducible without asking the author.
//! Every field that could move a number is recorded, including a failed pinning attempt.

use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::io;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::json;

use crate::csv::RunSink;
use crate::pin::{self, PinMap};

/// Writes the manifest for a run into the run directory.
pub fn write(sink: &RunSink, pin_map: &PinMap) -> io::Result<()> {
    let manifest = json!({
        "run_id": sink.run_id(),
        "experiment": sink.experiment(),
        "timestamp_unix": now_secs(),
        "profile": profile(),
        "cpu_model": cpu_model(),
        "physical_cores": physical_cores(),
        "available_cores": pin::available_cores(),
        "caches": caches(),
        "memory_kib": memory_kib(),
        "kernel": kernel(),
        "rustc": rustc_version(),
        "commit": commit(),
        "governor": pin_map.governor,
        "pinning": pin_map.summary(),
        "pinning_failure": pin_map.failure,
    });
    let text = serde_json::to_string_pretty(&manifest).map_err(io::Error::other)?;
    fs::write(sink.dir().join("manifest.json"), text + "\n")
}

/// Build profile the row was measured with.
pub fn profile() -> &'static str {
    if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    }
}

/// Reports whether the current build can produce reportable numbers.
///
/// A debug build is rejected, because its numbers are not reportable.
pub fn require_release() -> Result<(), String> {
    if cfg!(debug_assertions) {
        Err("measurements require a release or bench profile".to_owned())
    } else {
        Ok(())
    }
}

/// Model name reported by the kernel.
fn cpu_model() -> Option<String> {
    cpuinfo_field("model name")
}

/// Count of distinct physical cores, falling back to the visible core count.
fn physical_cores() -> usize {
    let text = fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
    let mut seen = BTreeSet::new();
    let mut physical = String::new();
    for line in text.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        match key.trim() {
            "physical id" => physical = value.trim().to_owned(),
            "core id" => {
                seen.insert((physical.clone(), value.trim().to_owned()));
            }
            _ => {}
        }
    }
    if seen.is_empty() {
        return pin::available_cores().len().max(1);
    }
    seen.len()
}

/// Cache sizes reported by the kernel, with duplicates removed.
fn caches() -> Option<String> {
    let text = fs::read_to_string("/proc/cpuinfo").ok()?;
    let mut sizes = BTreeSet::new();
    for line in text.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        if key.trim() == "cache size" {
            sizes.insert(value.trim().to_owned());
        }
    }
    if sizes.is_empty() {
        return None;
    }
    Some(sizes.into_iter().collect::<Vec<_>>().join(" + "))
}

/// Total memory in kibibytes.
fn memory_kib() -> Option<u64> {
    let text = fs::read_to_string("/proc/meminfo").ok()?;
    let line = text.lines().find(|line| line.starts_with("MemTotal"))?;
    line.split_whitespace().nth(1)?.parse().ok()
}

/// Kernel release string.
fn kernel() -> Option<String> {
    Some(
        fs::read_to_string("/proc/sys/kernel/osrelease")
            .ok()?
            .trim()
            .to_owned(),
    )
}

/// Compiler version string.
fn rustc_version() -> Option<String> {
    let output = Command::new("rustc").arg("-V").output().ok()?;
    Some(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// Commit the binary was built from, when the tree is a git checkout.
fn commit() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// First matching field of `/proc/cpuinfo`.
fn cpuinfo_field(name: &str) -> Option<String> {
    let text = fs::read_to_string("/proc/cpuinfo").ok()?;
    for line in text.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        if key.trim() == name {
            return Some(value.trim().to_owned());
        }
    }
    None
}

/// Seconds since the Unix epoch.
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
