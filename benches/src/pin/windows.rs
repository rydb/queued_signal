//! Thread affinity on Windows.

#[link(name = "kernel32")]
unsafe extern "system" {
    fn GetCurrentThread() -> isize;
    fn GetCurrentProcess() -> isize;
    fn SetThreadAffinityMask(thread: isize, mask: usize) -> usize;
    fn GetProcessAffinityMask(
        process: isize,
        process_mask: *mut usize,
        system_mask: *mut usize,
    ) -> i32;
}

/// Pins the calling thread with a single bit affinity mask.
pub fn pin_current_thread(core: usize) -> Result<(), String> {
    if core >= usize::BITS as usize {
        return Err(format!("core {core} is outside the affinity mask"));
    }
    let mask = 1usize << core;
    // SAFETY: the calls take a pseudo handle and a mask, and touch no Rust memory.
    let previous = unsafe { SetThreadAffinityMask(GetCurrentThread(), mask) };
    if previous == 0 {
        Err(std::io::Error::last_os_error().to_string())
    } else {
        Ok(())
    }
}

/// Expands the process affinity mask into a core list.
pub fn available_cores() -> Vec<usize> {
    let mut process_mask = 0usize;
    let mut system_mask = 0usize;
    // SAFETY: both masks are valid out parameters for the duration of the call.
    let ok =
        unsafe { GetProcessAffinityMask(GetCurrentProcess(), &mut process_mask, &mut system_mask) };
    if ok == 0 {
        return super::fallback_cores();
    }
    (0..usize::BITS as usize)
        .filter(|core| process_mask & (1usize << core) != 0)
        .collect()
}

/// Reports that Windows power plans are not readable as a governor.
pub fn governor() -> String {
    "unavailable: windows power plans are not reported".to_owned()
}
