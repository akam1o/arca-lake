//! Test-only storage fault control (cargo feature `fault-injection`; never
//! in a release build). The node polls `<dir>/FAULT`; a line
//! `eio-read <n>` or `eio-write <n>` makes the next `n` segment reads or
//! writes fail with EIO through the storage engine's I/O hook
//! (`arca_lake_storage::faults`); an empty or missing file disarms it.
//! Used by the soak test to inject transient device errors.

use std::path::PathBuf;
use std::time::Duration;

use arca_lake_storage::faults::{IoOp, fail_times, set_io_hook};

pub(crate) fn spawn(dir: PathBuf) {
    tracing::warn!("storage fault injection is compiled in (test build)");
    std::thread::spawn(move || {
        let path = dir.join("FAULT");
        let mut last = String::new();
        loop {
            let text = std::fs::read_to_string(&path).unwrap_or_default();
            let text = text.trim().to_string();
            if text != last {
                let mut it = text.split_whitespace();
                match (it.next(), it.next().and_then(|n| n.parse::<usize>().ok())) {
                    (Some("eio-read"), Some(n)) => set_io_hook(Some(fail_times(
                        |op| matches!(op, IoOp::Read { .. }),
                        std::io::ErrorKind::Other,
                        n,
                    ))),
                    (Some("eio-write"), Some(n)) => set_io_hook(Some(fail_times(
                        |op| matches!(op, IoOp::Write { .. }),
                        std::io::ErrorKind::Other,
                        n,
                    ))),
                    _ => set_io_hook(None),
                }
                tracing::warn!(fault = %text, "storage fault control changed");
                last = text;
            }
            std::thread::sleep(Duration::from_millis(500));
        }
    });
}
