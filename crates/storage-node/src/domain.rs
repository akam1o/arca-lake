//! Capacity domain of a node's store (Phase 3.7, R-80): which file system
//! the node's free space comes from, so that the controller can tell when
//! several nodes draw on the same one. Capacity admission is per node: two
//! nodes on one file system each admit against the same free bytes and can
//! together cross the reserve, unless each has an allotment
//! (`--capacity-bytes`) and the allotments fit in the file system.
//!
//! The identity is the kernel's device number of the store directory's
//! file system, scoped to the running kernel (Linux `boot_id`): containers
//! on one host share the kernel, so a file system bind-mounted into two of
//! them has one identity. Overlay, FUSE and network file systems have
//! identities per mount (or per host) that do not reveal sharing: they are
//! reported as uncertain, and the controller treats an uncertain domain
//! like a shared one unless the node has an allotment.

/// A store's capacity domain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Domain {
    /// Identity (opaque; equal identities mean one file system).
    pub id: String,
    /// Whether two nodes on one file system would always report the same
    /// identity (false for overlay, FUSE and network file systems).
    pub certain: bool,
}

/// The capacity domain of `dir`; `None` when it cannot be read.
#[must_use]
pub fn of(dir: &std::path::Path) -> Option<Domain> {
    let st = rustix::fs::stat(dir).ok()?;
    let dev = st.st_dev;
    let (major, minor) = (rustix::fs::major(dev), rustix::fs::minor(dev));
    let scope = kernel_scope();
    Some(Domain {
        id: format!("{scope}/dev:{major}:{minor}"),
        certain: scope != "unscoped" && !uncertain_fs(dir),
    })
}

/// The running kernel's identity (Linux `boot_id`), "unscoped" elsewhere.
fn kernel_scope() -> String {
    std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unscoped".into())
}

/// File system types whose device number does not reveal sharing.
#[cfg(target_os = "linux")]
fn uncertain_fs(dir: &std::path::Path) -> bool {
    const OVERLAYFS: i64 = 0x794c_7630;
    const FUSE: i64 = 0x6573_5546;
    const NFS: i64 = 0x6969;
    const SMB2: i64 = 0xfe53_4d42;
    const CIFS: i64 = 0xff53_4d42;
    const CEPH: i64 = 0x00c3_6400;
    const V9FS: i64 = 0x0102_1997;
    const VIRTIOFS: i64 = 0x6a65_6a63;
    match rustix::fs::statfs(dir) {
        #[allow(clippy::useless_conversion, clippy::unnecessary_cast)]
        Ok(s) => matches!(
            i64::from(s.f_type as i64),
            OVERLAYFS | FUSE | NFS | SMB2 | CIFS | CEPH | V9FS | VIRTIOFS
        ),
        Err(_) => true,
    }
}

#[cfg(not(target_os = "linux"))]
fn uncertain_fs(_dir: &std::path::Path) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_directories_of_one_file_system_share_a_domain() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        let da = of(a.path()).unwrap();
        let db = of(b.path()).unwrap();
        assert_eq!(da.id, db.id, "one temp file system");
        assert!(of(&a.path().join("missing")).is_none());
    }
}
