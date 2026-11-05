//! Fallback for platforms without a thread affinity call.

/// Reports that the platform has no affinity call.
pub fn pin_current_thread(_core: usize) -> Result<(), String> {
    Err("this platform has no thread affinity api".to_owned())
}

/// Visible core count.
pub fn available_cores() -> Vec<usize> {
    super::fallback_cores()
}

/// Reports that the platform has no cpufreq interface.
pub fn governor() -> String {
    "unavailable: this platform has no cpufreq interface".to_owned()
}
