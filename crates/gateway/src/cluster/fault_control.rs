//! Test-only tiering-refresh faults (cargo feature `fault-injection`; never
//! in a release build). When `ARCA_TEST_FAULT_FILE` names a file, each
//! combined tiering read of the cluster refresh consults it: `tiering-read
//! fail` turns the read into a failure; `tiering-read delay <ms>` holds the
//! result that long before it is observed (a late reply carrying an old
//! snapshot); anything else, an empty or a missing file, does nothing.
//! Used by the ADR-0035 integration harness.

use std::time::Duration;

fn fault() -> Option<(String, Option<u64>)> {
    let path = std::env::var_os("ARCA_TEST_FAULT_FILE")?;
    let text = std::fs::read_to_string(path).ok()?;
    let mut it = text.split_whitespace();
    if it.next()? != "tiering-read" {
        return None;
    }
    let mode = it.next()?.to_string();
    Some((mode, it.next().and_then(|n| n.parse().ok())))
}

pub(crate) async fn tiering_read<T>(
    read: Result<T, arca_lake_metadata::MetaError>,
) -> Result<T, arca_lake_metadata::MetaError> {
    match fault() {
        Some((m, _)) if m == "fail" => {
            tracing::warn!("test fault: tiering read failed");
            Err(arca_lake_metadata::MetaError::InvalidArgument(
                "test fault: tiering read failed".into(),
            ))
        }
        Some((m, Some(ms))) if m == "delay" => {
            tracing::warn!(delay_ms = ms, "test fault: tiering read held");
            tokio::time::sleep(Duration::from_millis(ms)).await;
            read
        }
        _ => read,
    }
}
