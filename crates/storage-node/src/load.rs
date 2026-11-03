//! Load signals a node reports in `Health` for adaptive background work
//! (Phase 3.4 §5, ADR-0018): client latency over the recent window, from
//! the node's own histograms (portable), and device utilisation, queue
//! depth and read latency where the platform exposes them (Linux
//! `/proc/diskstats` for the block device holding the store; elsewhere
//! unavailable, and the controller falls back to its fixed rate).

use std::path::Path;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use arca_lake_common::timing::HistogramSnapshot;

/// The signals of one window.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct LoadSample {
    /// 99th percentile of `GetShard` serve time in the window, µs (0 when
    /// no read was served).
    pub get_p99_us: u64,
    /// 99th percentile of `PutShard` store time in the window, µs.
    pub put_p99_us: u64,
    /// Device statistics of the window, when the platform has them.
    pub device: Option<DeviceSample>,
}

/// Block device statistics of a window.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct DeviceSample {
    /// Busy time over elapsed time, permille.
    pub util_permille: u32,
    /// Average requests in flight (weighted I/O time over elapsed),
    /// thousandths.
    pub queue_depth_milli: u32,
    /// Average read latency, µs.
    pub read_await_us: u32,
}

/// Raw device counters (`/proc/diskstats` fields).
#[derive(Debug, Clone, Copy)]
struct DeviceCounters {
    reads: u64,
    read_ms: u64,
    io_ms: u64,
    weighted_ms: u64,
}

/// The block device (`major:minor`) of a path, when it is a Linux block
/// device with statistics.
#[cfg(target_os = "linux")]
fn device_of(path: &Path) -> Option<String> {
    use std::os::unix::fs::MetadataExt as _;
    let dev = std::fs::metadata(path).ok()?.dev();
    // Kernel encoding of dev_t.
    let major = ((dev >> 8) & 0xfff) | ((dev >> 32) & !0xfff);
    let minor = (dev & 0xff) | ((dev >> 12) & !0xff);
    // A partition's statistics are its own; that is the device we write.
    let name = std::fs::read_link(format!("/sys/dev/block/{major}:{minor}"))
        .ok()?
        .file_name()?
        .to_string_lossy()
        .to_string();
    Some(name)
}

#[cfg(not(target_os = "linux"))]
fn device_of(_path: &Path) -> Option<String> {
    None
}

fn read_counters(name: &str) -> Option<DeviceCounters> {
    let text = std::fs::read_to_string("/proc/diskstats").ok()?;
    let f: Vec<&str> = text.lines().map(str::split_whitespace).find_map(|mut it| {
        let v: Vec<&str> = it.by_ref().collect();
        (v.get(2) == Some(&name)).then_some(v)
    })?;
    let n = |i: usize| f.get(i).and_then(|s| s.parse::<u64>().ok());
    Some(DeviceCounters {
        reads: n(3)?,
        read_ms: n(6)?,
        io_ms: n(12)?,
        weighted_ms: n(13)?,
    })
}

/// Percentile of a histogram delta (`now - before`), µs.
fn p99_us(before: &HistogramSnapshot, now: &HistogramSnapshot) -> u64 {
    let delta: Vec<u64> = now
        .buckets
        .iter()
        .zip(before.buckets.iter())
        .map(|(a, b)| a.saturating_sub(*b))
        .collect();
    let total: u64 = delta.iter().sum();
    if total == 0 {
        return 0;
    }
    let want = total - total / 100;
    let mut seen = 0;
    for (i, n) in delta.iter().enumerate() {
        seen += n;
        if seen >= want {
            return arca_lake_common::timing::bucket_upper_us(i);
        }
    }
    arca_lake_common::timing::bucket_upper_us(delta.len() - 1)
}

struct State {
    at: Instant,
    get: HistogramSnapshot,
    put: HistogramSnapshot,
    device: Option<DeviceCounters>,
    last: LoadSample,
}

/// Samples the node's load over windows of at least `MIN_WINDOW`.
pub struct LoadSampler {
    device: Option<String>,
    state: Mutex<Option<State>>,
}

impl std::fmt::Debug for LoadSampler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadSampler")
            .field("device", &self.device)
            .finish_non_exhaustive()
    }
}

/// Shortest window a sample covers (shorter calls get the last sample).
pub const MIN_WINDOW: Duration = Duration::from_millis(500);

impl LoadSampler {
    /// A sampler for the store at `dir`.
    #[must_use]
    pub fn new(dir: &Path) -> Self {
        let device = device_of(dir).filter(|d| read_counters(d).is_some());
        Self {
            device,
            state: Mutex::new(None),
        }
    }

    /// The load since the previous sample (at least `MIN_WINDOW` ago).
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        clippy::cast_sign_loss
    )]
    pub fn sample(&self, get: HistogramSnapshot, put: HistogramSnapshot) -> LoadSample {
        let now = Instant::now();
        let counters = self.device.as_deref().and_then(read_counters);
        let mut st = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(prev) = st.as_mut() else {
            *st = Some(State {
                at: now,
                get,
                put,
                device: counters,
                last: LoadSample::default(),
            });
            return LoadSample::default();
        };
        let elapsed = now.duration_since(prev.at);
        if elapsed < MIN_WINDOW {
            return prev.last;
        }
        let ms = elapsed.as_millis().max(1) as f64;
        let device = match (prev.device, counters) {
            (Some(a), Some(b)) => {
                let reads = b.reads.saturating_sub(a.reads);
                Some(DeviceSample {
                    util_permille: ((b.io_ms.saturating_sub(a.io_ms)) as f64 / ms * 1000.0)
                        .min(1000.0) as u32,
                    queue_depth_milli: ((b.weighted_ms.saturating_sub(a.weighted_ms)) as f64 / ms
                        * 1000.0) as u32,
                    read_await_us: ((b.read_ms.saturating_sub(a.read_ms)) * 1000)
                        .checked_div(reads)
                        .unwrap_or(0) as u32,
                })
            }
            _ => None,
        };
        let s = LoadSample {
            get_p99_us: p99_us(&prev.get, &get),
            put_p99_us: p99_us(&prev.put, &put),
            device,
        };
        *prev = State {
            at: now,
            get,
            put,
            device: counters,
            last: s,
        };
        s
    }
}
