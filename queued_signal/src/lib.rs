#![warn(missing_docs)]
//! A Dioxus signal for shared, wait-free reads and queued writes across VDOMs.

/// High-level dioxus integration: [`QueuedSignalHub`], [`QueuedSignalHandle`].
pub mod signal;
/// Core state types: [`QueuedSignal`], [`WriterDriver`], [`HealthStatus`].
pub mod state;

pub(crate) mod macros;
