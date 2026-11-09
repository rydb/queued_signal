//! Shared harness for the queued_signal benchmark suite.
//!
//! Each experiment is its own binary, and by default each row of an experiment is its own
//! process. The harness owns sampling, the CSV schema, the machine manifest, core pinning,
//! and the settle step.

pub mod candidates;
pub mod csv;
pub mod isolation;
pub mod manifest;
pub mod pin;
pub mod publish;
pub mod settle;
pub mod stats;

/// Frame budget used by the composition experiment, in nanoseconds.
///
/// This is one 60 Hz frame. A row reports its cost as a percentage of this budget.
pub const FRAME_BUDGET_NS: f64 = 16_600_000.0;
