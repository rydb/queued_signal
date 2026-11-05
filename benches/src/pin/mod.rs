//! Core pinning and governor reporting for the writer and reader threads.

use std::fmt;

#[cfg(any(target_os = "linux", target_os = "android"))]
#[path = "linux.rs"]
mod platform;

#[cfg(windows)]
#[path = "windows.rs"]
mod platform;

#[cfg(target_os = "macos")]
#[path = "macos.rs"]
mod platform;

#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    windows,
    target_os = "macos"
)))]
#[path = "unsupported.rs"]
mod platform;

/// Thread role in a pin map.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// The thread that publishes.
    Writer,
    /// The thread that reads.
    Reader,
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Role::Writer => f.write_str("writer"),
            Role::Reader => f.write_str("reader"),
        }
    }
}

/// The core map in force for a row.
#[derive(Clone, Debug, Default)]
pub struct PinMap {
    /// Core planned for the writer.
    pub writer: Option<usize>,
    /// Core planned for the reader.
    pub reader: Option<usize>,
    /// Scaling governor, or the reason it could not be read.
    pub governor: String,
    /// Whether the writer thread was pinned.
    pub writer_pinned: bool,
    /// Whether the reader thread was pinned.
    pub reader_pinned: bool,
    /// Reason pinning was unavailable, when it was.
    pub failure: Option<String>,
}

impl PinMap {
    /// Plans a writer core and a distinct reader core when the machine has two.
    pub fn plan() -> Self {
        let cores = available_cores();
        let writer = cores.first().copied();
        let reader = cores.get(1).copied().or(writer);
        let mut map = Self {
            writer,
            reader,
            governor: governor(),
            ..Self::default()
        };
        if cores.is_empty() {
            map.failure = Some("no cores are visible to the process".to_owned());
        }
        map
    }

    /// Pins the calling thread for `role` and records the result.
    pub fn pin_here(&mut self, role: Role) {
        let core = match role {
            Role::Writer => self.writer,
            Role::Reader => self.reader,
        };
        let Some(core) = core else {
            self.failure = Some(format!("no core was planned for the {role}"));
            return;
        };
        match pin_current_thread(core) {
            Ok(()) => match role {
                Role::Writer => self.writer_pinned = true,
                Role::Reader => self.reader_pinned = true,
            },
            Err(err) => self.failure = Some(format!("{role} core {core}: {err}")),
        }
    }

    /// One line description for the manifest.
    pub fn summary(&self) -> String {
        let writer = self
            .writer
            .map(|core| core.to_string())
            .unwrap_or_else(|| "none".to_owned());
        let reader = self
            .reader
            .map(|core| core.to_string())
            .unwrap_or_else(|| "none".to_owned());
        match &self.failure {
            Some(err) => {
                format!("writer core {writer}, reader core {reader}, pinning failed: {err}")
            }
            None => format!(
                "writer core {writer} (pinned {}), reader core {reader} (pinned {})",
                self.writer_pinned, self.reader_pinned
            ),
        }
    }
}

/// Pins the calling thread to one core.
pub fn pin_current_thread(core: usize) -> Result<(), String> {
    platform::pin_current_thread(core)
}

/// Cores this process may run on, in ascending order.
pub fn available_cores() -> Vec<usize> {
    platform::available_cores()
}

/// The scaling governor, or the reason it could not be read.
pub fn governor() -> String {
    platform::governor()
}

/// Visible core count, used when a platform cannot report an affinity mask.
fn fallback_cores() -> Vec<usize> {
    let count = std::thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1);
    (0..count).collect()
}
