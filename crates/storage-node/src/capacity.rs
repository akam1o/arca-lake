//! Capacity admission of shard writes (Phase 3.6): the point where a
//! write's physical space is decided. A write is admitted only if the
//! device keeps its reserve afterwards; the bytes are held from admission
//! until the write ends (stored or failed), so concurrent writes from any
//! number of gateways cannot together cross the reserve. Nothing of it is
//! persistent: a write in flight at a crash or restart holds nothing after
//! it.
//!
//! Classes: client writes (and tier migration and cache population) stop
//! at the reserve; repair may use the reserve down to the repair floor,
//! which stays for compaction output and segment rollover.

use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::{AtomicU64, Ordering};

use arca_lake_protocol::storage::v1::WriteClass;

/// Space a record takes beyond its payload, at most (header and alignment).
pub const RECORD_OVERHEAD: u64 = 8 << 10;

/// Reserve and floors of one device.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reserve {
    /// Client writes stop when the device would keep less than this free.
    pub reserved: u64,
    /// Repair stops when the device would keep less than this free.
    pub repair_floor: u64,
}

impl Reserve {
    /// The default for a device of `total` bytes and segments of
    /// `segment_max` bytes: two segments and 64 MiB, or 2 % of the device
    /// when larger, but never more than a quarter of it; repair keeps one
    /// segment and 16 MiB (one compaction output and a rollover).
    #[must_use]
    pub fn default_for(total: u64, segment_max: u64) -> Self {
        let reserved = (segment_max.saturating_mul(2) + (64 << 20))
            .max(total / 50)
            .min(total / 4);
        Self::new(reserved, segment_max)
    }

    /// An explicit reserve; the repair floor is one segment and 16 MiB,
    /// at most the reserve.
    #[must_use]
    pub fn new(reserved: u64, segment_max: u64) -> Self {
        Self {
            reserved,
            repair_floor: (segment_max + (16 << 20)).min(reserved),
        }
    }

    fn floor(&self, class: WriteClass) -> u64 {
        match class {
            WriteClass::Repair => self.repair_floor,
            WriteClass::Client | WriteClass::Migration | WriteClass::Cache => self.reserved,
        }
    }
}

/// Why a write was not admitted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    /// Bytes the write needs.
    pub needed: u64,
    /// Bytes available to it (free minus writes in flight).
    pub available: u64,
    /// The floor of its class.
    pub floor: u64,
    /// Its class.
    pub class: WriteClass,
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "insufficient capacity: a {} write of {} bytes would leave {} of {} available, below the {} bytes kept",
            self.class.as_str_name(),
            self.needed,
            self.available.saturating_sub(self.needed),
            self.available,
            self.floor
        )
    }
}

/// Admission state of a node.
#[derive(Debug)]
pub struct Admission {
    reserve: Reserve,
    inflight: Mutex<u64>,
    admitted: AtomicU64,
    refused: [AtomicU64; 4],
}

/// Bytes held for one admitted write; released when dropped.
#[derive(Debug)]
pub struct Held<'a> {
    admission: &'a Admission,
    bytes: u64,
}

impl Drop for Held<'_> {
    fn drop(&mut self) {
        let mut g = self
            .admission
            .inflight
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        *g = g.saturating_sub(self.bytes);
    }
}

impl Admission {
    /// Admission with `reserve`.
    #[must_use]
    pub fn new(reserve: Reserve) -> Self {
        Self {
            reserve,
            inflight: Mutex::new(0),
            admitted: AtomicU64::new(0),
            refused: Default::default(),
        }
    }

    /// The reserve and floors.
    #[must_use]
    pub fn reserve(&self) -> Reserve {
        self.reserve
    }

    /// Bytes of admitted writes not yet ended.
    #[must_use]
    pub fn inflight(&self) -> u64 {
        *self.inflight.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Writes admitted, and refused by class (client, repair, migration,
    /// cache).
    #[must_use]
    pub fn counts(&self) -> (u64, [u64; 4]) {
        (
            self.admitted.load(Ordering::Relaxed),
            std::array::from_fn(|i| self.refused[i].load(Ordering::Relaxed)),
        )
    }

    /// Admit a write of `payload` bytes of `class` on a device with `free`
    /// bytes free now; `free == None` (the platform cannot tell) admits.
    ///
    /// # Errors
    ///
    /// [`Refusal`] when the device would keep less than the class's floor.
    pub fn admit(
        &self,
        free: Option<u64>,
        class: WriteClass,
        payload: u64,
    ) -> Result<Held<'_>, Refusal> {
        self.admit_with(|| free, class, payload)
    }

    /// [`Self::admit`], sampling the free space with `free` while the
    /// admission lock is held: a sample taken before it could predate
    /// writes that were stored (and released their holds) meanwhile.
    ///
    /// # Errors
    ///
    /// As [`Self::admit`].
    pub fn admit_with(
        &self,
        free: impl FnOnce() -> Option<u64>,
        class: WriteClass,
        payload: u64,
    ) -> Result<Held<'_>, Refusal> {
        let needed = payload.saturating_add(RECORD_OVERHEAD);
        let mut g = self.inflight.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(free) = free() {
            let available = free.saturating_sub(*g);
            let floor = self.reserve.floor(class);
            if available.saturating_sub(needed) < floor || available < needed {
                self.refused[class as usize].fetch_add(1, Ordering::Relaxed);
                return Err(Refusal {
                    needed,
                    available,
                    floor,
                    class,
                });
            }
        }
        *g = g.saturating_add(needed);
        self.admitted.fetch_add(1, Ordering::Relaxed);
        Ok(Held {
            admission: self,
            bytes: needed,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1 << 20;

    #[test]
    fn client_writes_stop_at_the_reserve_and_repair_at_its_floor() {
        let a = Admission::new(Reserve::new(100 * MIB, 32 * MIB));
        assert_eq!(a.reserve().repair_floor, 48 * MIB);
        // 120 MiB free: a 10 MiB client write keeps 110 - overhead.
        let h = a
            .admit(Some(120 * MIB), WriteClass::Client, 10 * MIB)
            .unwrap();
        // The held bytes count against the next write.
        let e = a
            .admit(Some(120 * MIB), WriteClass::Client, 10 * MIB)
            .unwrap_err();
        assert_eq!(e.floor, 100 * MIB);
        assert!(
            a.admit(Some(120 * MIB), WriteClass::Cache, 10 * MIB)
                .is_err()
        );
        // Repair may go below the client reserve, not below its floor.
        let r = a
            .admit(Some(120 * MIB), WriteClass::Repair, 50 * MIB)
            .unwrap();
        assert!(
            a.admit(Some(120 * MIB), WriteClass::Repair, 20 * MIB)
                .is_err()
        );
        drop(h);
        drop(r);
        assert_eq!(a.inflight(), 0);
        let (admitted, refused) = a.counts();
        assert_eq!(admitted, 2);
        assert_eq!(refused, [1, 1, 0, 1]);
    }

    #[test]
    fn concurrent_admissions_never_cross_the_reserve() {
        let a = std::sync::Arc::new(Admission::new(Reserve::new(64 * MIB, 16 * MIB)));
        let free = 64 * MIB + 10 * (MIB + RECORD_OVERHEAD);
        let ok = std::sync::Arc::new(AtomicU64::new(0));
        // Every thread decides before any hold is released.
        let all = std::sync::Arc::new(std::sync::Barrier::new(32));
        let handles: Vec<_> = (0..32)
            .map(|_| {
                let (a, ok, all) = (
                    std::sync::Arc::clone(&a),
                    std::sync::Arc::clone(&ok),
                    std::sync::Arc::clone(&all),
                );
                std::thread::spawn(move || {
                    let h = a.admit(Some(free), WriteClass::Client, MIB);
                    if h.is_ok() {
                        ok.fetch_add(1, Ordering::SeqCst);
                    }
                    all.wait();
                    drop(h);
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(
            ok.load(Ordering::SeqCst),
            10,
            "exactly what fits above the reserve"
        );
        assert_eq!(a.inflight(), 0);
    }

    #[test]
    fn the_default_reserve_scales_with_the_device() {
        let small = Reserve::default_for(400 * MIB, 32 * MIB);
        assert_eq!(small.reserved, 100 * MIB, "a quarter of a small device");
        let big = Reserve::default_for(1 << 40, 256 * MIB);
        assert_eq!(big.reserved, (1u64 << 40) / 50, "2 % of a large one");
        assert_eq!(big.repair_floor, 272 * MIB);
    }
}
