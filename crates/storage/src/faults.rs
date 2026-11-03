//! Injectable I/O faults for the storage engine (R-53, Phase 3.3 §3).
//!
//! Every file operation of the engine goes through the thin wrappers here.
//! A process-wide hook, installed by tests with [`set_io_hook`], sees an
//! [`IoOp`] describing the operation and may turn it into an error, a
//! short read or a partial write. Without a hook the wrappers are a
//! relaxed atomic load away from the plain `std::fs` calls. The hook is
//! global: tests that install one must not run concurrently with other
//! tests of the same binary (take [`serial`]).
//!
//! This reproduces the *observable* effect of device and file-system
//! failures at the system-call boundary: an `EIO` on a read, `ENOSPC` on a
//! write, a failed `fdatasync`, a write that returns short. It does not
//! model what the kernel does with dirty pages after a failed fsync, a
//! power loss, or a device that lies; those are outside what the crate's
//! tests claim (RFC-005 §8).

use std::fs::File;
use std::io::{self, Write as _};
use std::os::unix::fs::FileExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

/// One I/O operation the engine is about to perform.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IoOp {
    /// Positional read of a segment.
    Read {
        /// Segment.
        segment: u64,
        /// Offset.
        offset: u64,
        /// Bytes.
        len: usize,
    },
    /// Append (positional write) to the active segment.
    Write {
        /// Segment.
        segment: u64,
        /// Offset.
        offset: u64,
        /// Bytes.
        len: usize,
    },
    /// `fdatasync` of a segment (group commit, rollover, compaction output).
    SyncData {
        /// Segment.
        segment: u64,
    },
    /// `fsync` of a directory or a whole file (segment creation, unlink).
    SyncAll {
        /// What is synced.
        what: &'static str,
    },
    /// Creating a segment file.
    Create {
        /// Segment.
        segment: u64,
    },
    /// Truncating a segment (torn tail at open).
    SetLen {
        /// Segment.
        segment: u64,
    },
    /// Unlinking a segment (compaction, empty last segment).
    Remove {
        /// Segment.
        segment: u64,
    },
    /// A quarantine log append and its sync.
    QuarantineLog,
    /// A small metadata file next to the segments (`group-epochs.txt`,
    /// `cluster.txt`): write, sync, rename.
    SideFile {
        /// File name.
        name: &'static str,
    },
}

/// What the hook decides.
#[derive(Debug)]
pub enum FaultOutcome {
    /// Proceed normally.
    None,
    /// Fail with this error before touching the file.
    Error(io::Error),
    /// Read only this many bytes (the engine sees `UnexpectedEof`); the
    /// file is not touched beyond them.
    ShortRead(usize),
    /// Write only this many bytes, then fail with `ENOSPC`-like
    /// `StorageFull`; the bytes before are on the device.
    PartialWrite(usize),
}

/// The hook type.
pub type IoHook = Arc<dyn Fn(&IoOp) -> FaultOutcome + Send + Sync>;

static ENABLED: AtomicBool = AtomicBool::new(false);
static HOOK: OnceLock<Mutex<Option<IoHook>>> = OnceLock::new();
static SERIAL: Mutex<()> = Mutex::new(());

/// Install (or remove) the process-wide hook.
pub fn set_io_hook(hook: Option<IoHook>) {
    let slot = HOOK.get_or_init(|| Mutex::new(None));
    let mut g = slot.lock().unwrap_or_else(PoisonError::into_inner);
    ENABLED.store(hook.is_some(), Ordering::Release);
    *g = hook;
}

/// Serialise tests that install hooks (one binary, many threads).
pub fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(PoisonError::into_inner)
}

fn decide(op: &IoOp) -> FaultOutcome {
    if !ENABLED.load(Ordering::Acquire) {
        return FaultOutcome::None;
    }
    let Some(slot) = HOOK.get() else {
        return FaultOutcome::None;
    };
    let g = slot.lock().unwrap_or_else(PoisonError::into_inner);
    match g.as_ref() {
        Some(h) => h(op),
        None => FaultOutcome::None,
    }
}

/// Check an operation that has no data of its own (sync, create, rename).
///
/// # Errors
///
/// The injected error.
pub fn guard(op: &IoOp) -> io::Result<()> {
    match decide(op) {
        FaultOutcome::None | FaultOutcome::ShortRead(_) | FaultOutcome::PartialWrite(_) => Ok(()),
        FaultOutcome::Error(e) => Err(e),
    }
}

/// `read_exact_at` with faults.
///
/// # Errors
///
/// I/O errors, injected or real; a short read is `UnexpectedEof`.
pub fn read_exact_at(file: &File, buf: &mut [u8], offset: u64, segment: u64) -> io::Result<()> {
    match decide(&IoOp::Read {
        segment,
        offset,
        len: buf.len(),
    }) {
        FaultOutcome::None | FaultOutcome::PartialWrite(_) => file.read_exact_at(buf, offset),
        FaultOutcome::Error(e) => Err(e),
        FaultOutcome::ShortRead(n) => {
            let n = n.min(buf.len());
            file.read_exact_at(&mut buf[..n], offset)?;
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "short read (injected)",
            ))
        }
    }
}

/// Positional `write_all_at` with faults.
///
/// # Errors
///
/// I/O errors, injected or real; a partial write leaves the first bytes
/// on the device and returns `StorageFull`.
pub fn write_all_at(file: &File, buf: &[u8], offset: u64, segment: u64) -> io::Result<()> {
    match decide(&IoOp::Write {
        segment,
        offset,
        len: buf.len(),
    }) {
        FaultOutcome::None | FaultOutcome::ShortRead(_) => file.write_all_at(buf, offset),
        FaultOutcome::Error(e) => Err(e),
        FaultOutcome::PartialWrite(n) => {
            let n = n.min(buf.len());
            file.write_all_at(&buf[..n], offset)?;
            Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "partial write (injected)",
            ))
        }
    }
}

/// `write_all` on a file cursor with faults (side files, quarantine log).
///
/// # Errors
///
/// As [`write_all_at`].
pub fn write_all(file: &mut File, buf: &[u8], op: &IoOp) -> io::Result<()> {
    match decide(op) {
        FaultOutcome::None | FaultOutcome::ShortRead(_) => file.write_all(buf),
        FaultOutcome::Error(e) => Err(e),
        FaultOutcome::PartialWrite(n) => {
            let n = n.min(buf.len());
            file.write_all(&buf[..n])?;
            Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "partial write (injected)",
            ))
        }
    }
}

/// `sync_data` with faults.
///
/// # Errors
///
/// I/O errors, injected or real.
pub fn sync_data(file: &File, segment: u64) -> io::Result<()> {
    guard(&IoOp::SyncData { segment })?;
    file.sync_data()
}

/// `sync_all` with faults.
///
/// # Errors
///
/// I/O errors, injected or real.
pub fn sync_all(file: &File, what: &'static str) -> io::Result<()> {
    guard(&IoOp::SyncAll { what })?;
    file.sync_all()
}

/// A hook that fails the first `times` operations matching `matcher` with
/// `kind`, then passes everything (the common test shape).
#[must_use]
pub fn fail_times(
    matcher: impl Fn(&IoOp) -> bool + Send + Sync + 'static,
    kind: io::ErrorKind,
    times: usize,
) -> IoHook {
    let left = std::sync::atomic::AtomicUsize::new(times);
    Arc::new(move |op| {
        if matcher(op) && left.load(Ordering::Relaxed) > 0 {
            left.fetch_sub(1, Ordering::Relaxed);
            FaultOutcome::Error(io::Error::new(kind, "injected"))
        } else {
            FaultOutcome::None
        }
    })
}
