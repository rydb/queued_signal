//! Core pinning on macOS.
//!
//! The platform exposes a thread grouping hint, not a pinning call, so a row is left
//! unpinned and records why.

/// Reports that macOS has no public pinning call.
pub fn pin_current_thread(_core: usize) -> Result<(), String> {
    Err("macos has no public thread pinning api, only a grouping hint".to_owned())
}

/// Visible core count.
pub fn available_cores() -> Vec<usize> {
    super::fallback_cores()
}

/// Reports that macOS has no scaling governor.
pub fn governor() -> String {
    "unavailable: macos does not expose a scaling governor".to_owned()
}
