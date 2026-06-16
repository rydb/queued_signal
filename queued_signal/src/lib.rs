#![warn(missing_docs)]
//! A Dioxus signal for shared, wait-free reads and queued writes across VDOMs.

/// Core state types: [`QueuedSignal`], [`WriterDriver`], [`HealthStatus`].
pub mod state;
/// High-level dioxus integration: [`QueuedSignalHub`], [`QueuedSignalHandle`].
pub mod signal;

pub(crate) mod macros;
