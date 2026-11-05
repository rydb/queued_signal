//! Timing and distribution helpers shared by every experiment.
//!
//! One sample is the mean of a batch of calls, so the timing call itself is amortised.
//! Rows report p50, p99, and p999, and never a bare mean.

use std::time::Instant;

/// Distribution of batch averages in nanoseconds.
#[derive(Clone, Copy, Debug, Default)]
pub struct Stats {
    /// Median.
    pub p50: f64,
    /// 99th percentile.
    pub p99: f64,
    /// 99.9th percentile.
    pub p999: f64,
    /// Smallest sample.
    pub min: f64,
    /// Largest sample.
    pub max: f64,
    /// Number of samples behind the distribution.
    pub samples: usize,
}

impl Stats {
    /// A degenerate distribution for one measured value, such as a flag or a total.
    pub fn single(value: f64) -> Self {
        Self {
            p50: value,
            p99: value,
            p999: value,
            min: value,
            max: value,
            samples: 1,
        }
    }
}

/// One percentile of a sorted slice, clamped to the last element.
pub fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

/// Summarises samples into the reported distribution.
///
/// # Panics
///
/// Panics when `samples` is empty, because a row without samples cannot be reported.
pub fn summarize(mut samples: Vec<f64>) -> Stats {
    assert!(!samples.is_empty(), "a row needs at least one sample");
    samples.sort_by(|a, b| a.total_cmp(b));
    Stats {
        p50: percentile(&samples, 50.0),
        p99: percentile(&samples, 99.0),
        p999: percentile(&samples, 99.9),
        min: samples[0],
        max: samples[samples.len() - 1],
        samples: samples.len(),
    }
}

/// Sampling parameters for one row.
#[derive(Clone, Copy, Debug)]
pub struct Sampling {
    /// Operations per timed batch.
    pub batch: usize,
    /// Timed batches per row.
    pub samples: usize,
    /// Batches discarded before timing starts.
    pub warmup_batches: usize,
}

impl Sampling {
    /// Default sampling for a row that finishes in a few seconds.
    pub const fn standard() -> Self {
        Self {
            batch: 2_000,
            samples: 300,
            warmup_batches: 3,
        }
    }

    /// Small sampling used by the smoke test.
    pub const fn smoke() -> Self {
        Self {
            batch: 16,
            samples: 8,
            warmup_batches: 1,
        }
    }
}

impl Default for Sampling {
    fn default() -> Self {
        Self::standard()
    }
}

/// Times one batch of calls and returns nanoseconds per call.
pub fn time_batch(batch: usize, f: &mut impl FnMut()) -> f64 {
    let start = Instant::now();
    for _ in 0..batch {
        f();
    }
    start.elapsed().as_nanos() as f64 / batch as f64
}

/// Runs warmup batches, then samples the batch average.
pub fn measure(sampling: Sampling, mut f: impl FnMut()) -> Stats {
    for _ in 0..sampling.warmup_batches {
        for _ in 0..sampling.batch {
            f();
        }
    }
    let mut samples = Vec::with_capacity(sampling.samples);
    for _ in 0..sampling.samples {
        samples.push(time_batch(sampling.batch, &mut f));
    }
    summarize(samples)
}

/// Times single calls, for operations that cannot be batched.
pub fn measure_once(samples: usize, mut f: impl FnMut()) -> Stats {
    let mut values = Vec::with_capacity(samples);
    for _ in 0..samples {
        let start = Instant::now();
        f();
        values.push(start.elapsed().as_nanos() as f64);
    }
    summarize(values)
}
