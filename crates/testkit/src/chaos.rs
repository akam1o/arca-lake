//! FoundationDB process faults for tests on a multi-process cluster:
//! `kill N` / `start N` through one of three drivers.
//!
//! - `ARCA_CHAOS_DOCKER`: the CI container (`docker exec arca-fdb fdbproc`);
//! - `ARCA_CHAOS_DIR`: a request/acknowledgement directory served by an
//!   external driver;
//! - `ARCA_CHAOS_SCRIPT` and `ARCA_CHAOS_CLUSTER_DIR`:
//!   `deploy/dev/fdb-tls-cluster.sh` on a cluster it created.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// A chaos driver.
#[derive(Debug)]
pub enum Chaos {
    /// The CI container.
    Docker,
    /// Request/acknowledgement files.
    Dir(std::path::PathBuf),
    /// The cluster script and its directory.
    Script(std::path::PathBuf, std::path::PathBuf),
}

static SEQ: AtomicUsize = AtomicUsize::new(0);

impl Chaos {
    /// The driver the environment names, if any.
    #[must_use]
    pub fn from_env() -> Option<Self> {
        if std::env::var_os("ARCA_CHAOS_DOCKER").is_some() {
            return Some(Self::Docker);
        }
        if let (Some(s), Some(d)) = (
            std::env::var_os("ARCA_CHAOS_SCRIPT"),
            std::env::var_os("ARCA_CHAOS_CLUSTER_DIR"),
        ) {
            return Some(Self::Script(s.into(), d.into()));
        }
        std::env::var_os("ARCA_CHAOS_DIR").map(|d| Self::Dir(d.into()))
    }

    /// `kill N` or `start N` (process numbers from 1).
    ///
    /// # Panics
    ///
    /// When the driver fails or does not answer within three minutes.
    pub async fn run(&self, action: &str) {
        eprintln!("chaos: {action}");
        let mut parts = action.split_whitespace();
        let verb = parts.next().expect("verb");
        let n = parts.next().expect("process number");
        match self {
            Self::Docker => {
                let st = tokio::process::Command::new("docker")
                    .args(["exec", "arca-fdb", "fdbproc", verb, n])
                    .status()
                    .await
                    .expect("docker");
                assert!(
                    st.success(),
                    "docker exec arca-fdb fdbproc {verb} {n} failed"
                );
            }
            Self::Script(script, dir) => {
                let verb = if verb == "start" { "restart" } else { verb };
                let st = tokio::process::Command::new("bash")
                    .arg(script)
                    .arg(verb)
                    .arg(dir)
                    .arg(n)
                    .status()
                    .await
                    .expect("bash");
                assert!(st.success(), "cluster script {verb} {n} failed");
            }
            Self::Dir(dir) => {
                let i = SEQ.fetch_add(1, Ordering::SeqCst);
                let req = dir.join(format!("req-{i}"));
                let ack = dir.join(format!("ack-{i}"));
                std::fs::write(&req, action).expect("chaos request");
                let start = Instant::now();
                while !ack.exists() {
                    assert!(
                        start.elapsed() < Duration::from_secs(180),
                        "chaos driver did not answer {action}"
                    );
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }
}
