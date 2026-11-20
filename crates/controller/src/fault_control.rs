//! Test-only hold of a tiering move (cargo feature `fault-injection`; never
//! in a release build). When `ARCA_TEST_FAULT_FILE` names a file reading
//! `tiering-before-switch hold`, a move a tiering pass started waits, its
//! target shards written and its switch not yet attempted, until the file
//! says otherwise. Used by the ADR-0035 integration harness to keep a
//! deposed leader's move open across a takeover.

use std::time::Duration;

use arca_lake_metadata::records::GenId;

fn holding() -> bool {
    std::env::var_os("ARCA_TEST_FAULT_FILE")
        .and_then(|p| std::fs::read_to_string(p).ok())
        .is_some_and(|t| {
            t.split_whitespace().collect::<Vec<_>>() == ["tiering-before-switch", "hold"]
        })
}

pub(crate) async fn before_tiering_switch(generation: GenId, index: u32, policy_generation: u64) {
    if !holding() {
        return;
    }
    let g = arca_lake_metadata::ops::compat::hex(&generation);
    tracing::warn!(generation = %g, index, policy_generation, "test fault: tiering move held before its switch");
    while holding() {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tracing::warn!(generation = %g, index, policy_generation, "test fault: tiering move released");
}
