# Phase 4.1 P0-2: intermittent `managed_restore` failures (R-113)

Status: done. Cause: the three tests read through a gateway started
before the restore, and passed only when they did so before that
gateway's next cluster-state refresh. The refusal is the intended
fencing. The tests now apply the refresh themselves, check that the old
gateway is fenced, and restart it as the procedure requires. No product
change.

## 1. The failures

`managed_restore::a_restore_tool_reaching_another_database_clears_nothing`,
`another_clusters_backup_is_refused_and_amended` and
`the_cluster_uid_is_kept_across_a_restore_of_a_backup_without_one`, all
three together, each at the first `s3::get(&c.backend, …)` after
`finish` (`managed_restore.rs`, formerly lines 1617, 1720, 1911): 503
`cluster not available for this request: cluster incarnation changed
since this gateway started; restart it`. Seen: Phase 4.0, host A, 3 of 8
runs of the Phase 4.0 tree (once inside a workspace test run); Phase 4.1,
the macOS host, once in a workspace test run.

## 2. A/B under the same conditions (measured)

Host A, the same toolchain, `TMPDIR`, test-thread count and build mode;
the Phase 3.14 tree (`88732f1`) and the Phase 4.1 tree before this change
(`9c1dfd7`), each with its own target directory, alternated run by run
(ABBA order), no other work on the host.

| Condition | Phase 3.14 runs / failing | Phase 4.1 runs / failing |
| --- | --- | --- |
| `--test it managed_restore::`, idle | 30 / 0 | 30 / 0 |
| the same with four CPU-bound busy loops (4 vCPUs) | 15 / 0 | 15 / 0 |
| the whole `--test it` binary (362 tests in one process: the condition of the first failure) | 8 / 0 (one run failed `parallel_read::population_retried_after_resets_records_each_chunk_once`, R-77) | 8 / 0 |
| total | 53 / 0 | 53 / 0 |

The failure did not recur in 106 matched runs, in either tree: under
these conditions its rate is low (below about 6 % per tree at 95 %
confidence) and no difference between the trees is shown. The earlier
occurrences came under conditions not reproduced here (other work on the
host, the macOS scheduler).

## 3. The mechanism (measured, deterministic)

- A gateway learns the cluster state by a refresh every `CLUSTER_REFRESH`
  (2 s, `crates/gateway/src/cluster/backend.rs`). A managed restore gives
  the cluster a new incarnation (`recovery begin`, the restore step that
  marks the store); a gateway that started under the old one refuses
  every request once its refresh has seen the new one (`ClusterWatch::admit`),
  and every writing metadata transaction it starts is refused at commit
  whatever its refresh saw (`GuardedKv`). Operations guide (RFC-013
  §10.6c) and the backup-restore guide: gateways are stopped before a
  restore and restarted after it.
- The three tests kept the gateway of the cluster's start through the
  restore and read through it right after `finish`; the main test of the
  file restarts it (`restart_backend`) and never failed. They passed only
  if no refresh of that gateway fell between the restore's new
  incarnation and the read: a race against a 2 s timer.
- **Shown on the unmodified Phase 3.14 code**: the same tests with a wait
  of 2.1 s (one refresh) inserted before that read fail every time, the
  three of them, with the same message (5 of 5 runs, host B); the
  corrected tests with the same wait pass (5 of 5).
- **Not a recovery defect**: the incarnation changes at the restore step
  designed to change it (the new helper asserts that the store's
  incarnation differs from the old gateway's); the refusing
  check is the one designed for this case; a restarted gateway serves.

## 4. Change (test synchronization; no assertion relaxed)

`restart_gateway_after_restore` in `crates/testkit/tests/it/managed_restore.rs`,
used after `finish` by the three tests and the main one:

1. the store's state after the restore carries a new incarnation;
2. a store guarded with the old gateway's incarnation (what every
   writing metadata transaction of that gateway goes through, whatever its
   refresh saw; checked on such a store directly, so that the gateway's
   refresher is not raced) cannot write: the commit-time guard refuses
   (`cluster not writable: cluster incarnation changed (process started
   under …, store is at …)`);
3. once it holds the current state (applied explicitly, as its refresh
   would), it refuses reads and writes with the incarnation message;
4. a gateway started after the restore serves the restored objects.

The read assertions after it are unchanged. With step 4 (the restart)
removed, the four tests fail in every run (3 of 3), so the old sequence
can no longer pass by timing.

## 5. Guarantees kept (code analysis, with the tests above)

- Restore fencing and incarnation checks unchanged; R-94(b)'s deferred
  reclamation untouched (no product change).
- A gateway left running through a restore (against the procedure, and
  refused by the plan when its heartbeat is fresh:
  `running_processes_stop_the_procedure_before_anything_is_restored`):
  - client metadata writes, access records, multipart changes and
    integrity reports: refused at commit (step 2), whatever its cached
    state (they go through guarded stores);
  - shard deletes: a gateway sends some (cleanup of cache copies,
    `crates/tiering/src/cache.rs`); storage nodes refuse deletes carrying
    another incarnation once they adopted the new one;
  - shard writes of a cache population can precede their guarded
    metadata insert, which then fails: cleanup work, no visible effect;
  - the heartbeat uses the unguarded store and checks only that a cluster
    state exists: an old gateway can write its process record after the
    restore, and its removal on shutdown can clear a replacement's record
    of the same id (bookkeeping only; review, Low);
  - reads: admitted while its cached state is the old one, that is until
    its next successful refresh (every 2 s; a failed refresh keeps the
    last state, and a read admitted just before a refresh can finish
    after it), then refused. Not a hard bound; by design reads are not
    refused on a stale cache so that a metadata outage does not stop
    them (RFC-011 §6). Low residual.

## 6. Risk register

R-113: **Low** (was Medium): cause found, tests fixed (test
synchronization defect; intended protection). The read and heartbeat
residuals of §5 are tracked separately as R-116 (Low), since this change
does not touch them.
R-77: the `parallel_read` flake recurred once (Phase 3.14 tree, full
`it` run); unrelated to R-113.
