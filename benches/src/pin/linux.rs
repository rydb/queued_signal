//! Thread affinity on Linux and Android.

use std::fs;

/// Pins the calling thread with the scheduler affinity call.
pub fn pin_current_thread(core: usize) -> Result<(), String> {
    if core >= libc::CPU_SETSIZE as usize {
        return Err(format!("core {core} is outside the affinity set"));
    }
    // SAFETY: the set is plain data, and the call only reads it.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(core, &mut set);
        let rc = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
        if rc == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error().to_string())
        }
    }
}

/// Expands the process affinity mask into a core list.
pub fn available_cores() -> Vec<usize> {
    // SAFETY: the set is plain data, and the call only writes it.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        let rc = libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set);
        if rc == 0 {
            return (0..libc::CPU_SETSIZE as usize)
                .filter(|core| libc::CPU_ISSET(*core, &set))
                .collect();
        }
    }
    super::fallback_cores()
}

/// Reads the scaling governor from the first cpu.
pub fn governor() -> String {
    match fs::read_to_string("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor") {
        Ok(value) => value.trim().to_owned(),
        Err(err) => format!("unavailable: {err}"),
    }
}
