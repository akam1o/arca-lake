//! S3 request accounting (RFC-013 §7): requests by operation and status
//! class, latency by operation, bytes in and out. Labels are bounded: the
//! operation is one of the S3 API's names (at most `MAX_OPS` distinct,
//! the rest counted as `other`) and the status a class (`2xx`…`5xx`);
//! never a bucket, key, principal or address.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use std::sync::Arc;

use arca_lake_common::timing::Histogram;

use crate::auth::CredentialStore;

/// Distinct operation labels kept.
const MAX_OPS: usize = 128;

/// Counters of the S3 front end.
#[derive(Debug, Default)]
pub struct S3Stats {
    requests: Mutex<BTreeMap<(String, &'static str), u64>>,
    latency: Mutex<BTreeMap<String, Histogram>>,
    /// Request body bytes (`Content-Length`).
    pub bytes_in: AtomicU64,
    /// Response body bytes (`Content-Length`).
    pub bytes_out: AtomicU64,
    credentials: Mutex<Option<Arc<CredentialStore>>>,
}

fn class(status: u16) -> &'static str {
    match status {
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        _ => "5xx",
    }
}

impl S3Stats {
    /// The credential store whose refusals and reloads are reported.
    pub fn set_credentials(&self, store: Arc<CredentialStore>) {
        *self
            .credentials
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(store);
    }

    /// Account one request.
    pub fn record(&self, op: &str, status: u16, bytes_in: u64, bytes_out: u64, took: Duration) {
        let op = if op.is_empty() { "unknown" } else { op };
        let bounded = |known: bool, len: usize| known || len < MAX_OPS;
        {
            let mut r = self.requests.lock().unwrap_or_else(PoisonError::into_inner);
            let key = (op.to_string(), class(status));
            let known = r.keys().any(|(o, _)| o == op);
            let key = if bounded(known, r.len() / 4) {
                key
            } else {
                ("other".to_string(), class(status))
            };
            *r.entry(key).or_default() += 1;
        }
        {
            let mut l = self.latency.lock().unwrap_or_else(PoisonError::into_inner);
            let name = if bounded(l.contains_key(op), l.len()) {
                op
            } else {
                "other"
            };
            l.entry(name.to_string()).or_default().observe(took);
        }
        self.bytes_in.fetch_add(bytes_in, Ordering::Relaxed);
        self.bytes_out.fetch_add(bytes_out, Ordering::Relaxed);
    }

    /// Prometheus text.
    pub fn render(&self, out: &mut String) {
        use std::fmt::Write as _;
        let _ = writeln!(out, "# TYPE arca_gateway_s3_requests_total counter");
        for ((op, class), n) in self
            .requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
        {
            let _ = writeln!(
                out,
                "arca_gateway_s3_requests_total{{op=\"{op}\",status=\"{class}\"}} {n}"
            );
        }
        let lat = self.latency.lock().unwrap_or_else(PoisonError::into_inner);
        let _ = writeln!(out, "# TYPE arca_gateway_s3_request_seconds histogram");
        for (op, h) in lat.iter() {
            h.snapshot().render_prometheus(
                out,
                "arca_gateway_s3_request_seconds",
                &format!("op=\"{op}\""),
            );
        }
        let _ = writeln!(out, "# TYPE arca_gateway_s3_bytes_total counter");
        let _ = writeln!(
            out,
            "arca_gateway_s3_bytes_total{{dir=\"in\"}} {}",
            self.bytes_in.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "arca_gateway_s3_bytes_total{{dir=\"out\"}} {}",
            self.bytes_out.load(Ordering::Relaxed)
        );
        let creds = self
            .credentials
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let (refused, reloads) = creds.map_or(([0; 3], 0), |c| {
            (
                [0, 1, 2].map(|i| c.refusals[i].load(Ordering::Relaxed)),
                c.reload_failures.load(Ordering::Relaxed),
            )
        });
        let _ = writeln!(out, "# TYPE arca_gateway_auth_failures_total counter");
        for (reason, n) in ["unknown_key", "inactive_key", "expired_key"]
            .iter()
            .zip(refused)
        {
            let _ = writeln!(
                out,
                "arca_gateway_auth_failures_total{{reason=\"{reason}\"}} {n}"
            );
        }
        let _ = writeln!(
            out,
            "# TYPE arca_gateway_credentials_reload_failures_total counter"
        );
        let _ = writeln!(
            out,
            "arca_gateway_credentials_reload_failures_total {reloads}"
        );
    }
}
