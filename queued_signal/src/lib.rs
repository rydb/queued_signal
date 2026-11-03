#![warn(missing_docs)]
//! A Dioxus signal for shared, wait-free reads and queued writes across VDOMs.

pub mod atom_coerce;
pub mod atom_coerce_dyn;
pub(crate) mod deferred;
pub(crate) mod macros;
pub mod queued_signal_dyn;
/// High-level dioxus integration: [`QueuedSignalSender`](signal::QueuedSignalSender),
/// [`QueuedSignalHandle`](signal::QueuedSignalHandle), and
/// [`create_queued_signal_hub`](signal::create_queued_signal_hub).
pub mod signal;
/// Core state types: [`QueuedSignal`](state::QueuedSignal), [`WriterDriver`](state::WriterDriver),
/// and [`HealthStatus`](state::HealthStatus).
pub mod state;
pub mod subscriber_slots;
