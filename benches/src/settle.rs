//! Reclamation settling between rows.
//!
//! A row that publishes leaves replaced generations for the receiver to collect. The next
//! row must start from a quiet signal, so every row settles first and records what it saw.

use std::thread::sleep;
use std::time::{Duration, Instant};

use queued_signal::atom_coerce::{drain_deferred_frees, flush_reclamation};

/// Flushes reclamation until the retained count reaches zero or the budget runs out.
///
/// Returns the retained count at the end of the wait, which every row records.
pub fn settle(retained: impl Fn() -> u64, budget: Duration) -> u64 {
    let start = Instant::now();
    loop {
        flush_reclamation();
        drain_deferred_frees();
        let count = retained();
        if count == 0 || start.elapsed() >= budget {
            return count;
        }
        sleep(Duration::from_millis(1));
    }
}

/// Settles with a one second budget.
pub fn settle_default(retained: impl Fn() -> u64) -> u64 {
    settle(retained, Duration::from_secs(1))
}
