//! Memory of the running process (Phase 3.5, R-70): resident and anonymous
//! bytes from `/proc/self/status`, and on glibc the allocator's view
//! (`mallinfo2`): bytes in use by the program, bytes free but kept by the
//! allocator, bytes in `mmap`ed blocks. Growth of `heap_in_use` with flat
//! `heap_free` is the program holding memory; growth of `heap_free` is
//! allocator retention. Zero where the platform does not provide a value.

/// A sample of the process's memory.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Memory {
    /// Resident set (`VmRSS`).
    pub resident: u64,
    /// Anonymous resident memory (`RssAnon`).
    pub anon: u64,
    /// Allocator: bytes allocated and not freed (`uordblks + hblkhd`).
    pub heap_in_use: u64,
    /// Allocator: bytes freed but kept in arenas (`fordblks`).
    pub heap_free: u64,
    /// Allocator: bytes in `mmap`ed chunks (`hblkhd`).
    pub heap_mmap: u64,
}

/// Sample the process's memory now.
#[must_use]
pub fn memory() -> Memory {
    let mut m = Memory::default();
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            let kib = |l: &str| {
                l.split_whitespace()
                    .nth(1)
                    .and_then(|v| v.parse::<u64>().ok())
                    .map_or(0, |v| v * 1024)
            };
            if line.starts_with("VmRSS:") {
                m.resident = kib(line);
            } else if line.starts_with("RssAnon:") {
                m.anon = kib(line);
            }
        }
    }
    allocator(&mut m);
    m
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[allow(unsafe_code)]
fn allocator(m: &mut Memory) {
    // SAFETY: `mallinfo2` takes no arguments, only reads allocator
    // statistics (taking the arena locks) and returns a plain struct.
    let i = unsafe { libc::mallinfo2() };
    m.heap_mmap = i.hblkhd as u64;
    m.heap_in_use = i.uordblks as u64 + m.heap_mmap;
    m.heap_free = i.fordblks as u64;
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn allocator(_: &mut Memory) {}

#[cfg(test)]
mod tests {
    #[test]
    fn a_sample_is_plausible() {
        let m = super::memory();
        if cfg!(target_os = "linux") {
            assert!(m.resident > 0 && m.anon <= m.resident, "{m:?}");
        }
        if cfg!(all(target_os = "linux", target_env = "gnu")) {
            let before = super::memory().heap_in_use;
            let v = vec![1u8; 8 << 20];
            assert!(super::memory().heap_in_use >= before + (8 << 20) - 4096);
            drop(v);
        }
    }
}
