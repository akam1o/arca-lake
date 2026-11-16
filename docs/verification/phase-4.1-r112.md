# Phase 4.1 P0-1: concurrent overwrite of one key through S3 (R-112)

Status: done. The reproduction through real gateways found no lost
acknowledged write and no visible version that should not be. Review of
the resolution paths it exercises found answers that misstated a write's
outcome; they are fixed with tests, and the campaign was repeated on the
fixed build. Results labelled as in the Phase 4.0 report (**measured**,
**code analysis**).

## 1. Method

Tool: `benchmarks/arcabench` `contend` (new). `--mode run`: 64 writers PUT
unique 4 KiB bodies to one key (or 16 keys) for 60 s through four
gateways, round robin, and 8 readers GET it; every request, and every
attempt of a retried PUT, is logged with its send and answer times (one
monotonic clock), status, error code and message and version id. A body
names its PUT (key, id, size) and is checked byte for byte. `--mode
check`, after the cluster has settled (no retirement pending and the
nodes' record counts stable for a minute; at least 420 s after a run
with gateway kills or a FoundationDB stall, beyond the 300 s write lease,
GC grace, minimum orphan age and retention):

- a PUT is **acknowledged** if an attempt was answered 2xx; its effect
  lies between its first attempt's send and the acknowledged answer; a
  **definite failure** if every attempt was answered 503 SlowDown (the
  gateway's meaning: not applied); anything else (500, other 503s,
  transport errors, timeouts) is **unknown** and never orders anything;
- violations: a body that is no PUT's of the run; the current object
  older than an acknowledged PUT sent after it was answered (a **lost
  acknowledged write**); versioned: an acknowledged PUT without exactly
  one version, a version id other than the one it was answered with,
  versions out of real-time order, not exactly one `IsLatest`, a delete
  marker; during the run, a stale read (an acknowledged later PUT was
  answered before the GET was sent), a read going back (an earlier GET
  returned a later PUT), a PUT read before it was sent, 404 after an
  acknowledged PUT; gateways disagreeing at the end (GET, HEAD and
  ListObjectsV2 through each of the four);
- **definite failures visible**: a PUT answered SlowDown that is the
  current object or a version (reported separately: a breach of the
  gateway's own answer, not of S3's).

Then a scrub of every group and the storage nodes' live records against
what is visible (three replicas per 4 KiB object or version; no pending
retirement). Checker reviewed independently before the campaign; its
findings (unknown outcomes treated as ordered, no read-versus-read check,
one gateway checked, version ids not compared, wall clock) were fixed
first; unit tests in `contend.rs`.

Cluster: the Phase 4.0 layout (4 storage nodes and 4 gateways on host B,
FoundationDB as one process, the controller and the generator on host A),
a fresh cluster per trial, small objects replicated, retention 60 s,
GC grace 30 s, intervals of 10–30 s, minimum orphan age 60 s.

Scenarios (3 trials each, rotated): `unv` (one key, unversioned), `ver`
(one key, versioning enabled), `unv16` (16 keys), `retry` (one key,
writers retry SlowDown three times with backoff and full jitter from
100 ms, as S3 SDKs do), `restart` (all gateways restarted gracefully at
20 s and 35 s), `gwkill` (all gateways killed with SIGKILL at about 20 s
and 40 s and restarted), `fdbstall` and `fdbstallver` (FoundationDB
stopped with SIGSTOP for 12 s at about 15 s and 47 s: transactions time
out, commit outcomes become unknown), `fdbstallold` (the same with the
gateways of the build before the fixes). Every fault time is checked to
fall inside the run. A confirmation round on the final build: one trial
each of `unv ver retry restart gwkill fdbstall fdbstallver`.

Invalid and discarded attempts (kept, not counted): a first campaign
whose fault scenarios injected their faults after the runs had ended
(the launch of the generator held its ssh session until it ended); it
also used the first checker. Its fault-free scenarios gave the same
results as below.

## 2. Results (measured)

34 valid trials (27 + 7), 0 violations, 0 definite failures visible,
every scrub without a finding, nodes holding exactly the visible objects'
records after settling.

| Scenario | Trials | PUTs | acknowledged | SlowDown | unknown (by cause) | GETs |
| --- | --- | --- | --- | --- | --- | --- |
| unv | 3 + 1 | 32,821 | 14,848 | 17,973 | 0 | 155,596 |
| ver | 3 + 1 | 37,009 | 16,732 | 20,277 | 0 | 131,843 |
| unv16 | 3 | 87,704 | 87,658 | 46 | 0 | 77,746 |
| retry (24,422 + 6,883 attempts) | 3 + 1 | 16,985 | 15,945 | 1,040 (after 3 retries) | 0 | 165,830 |
| restart | 3 + 1 | 37,933 | 16,171 | 19,498 | 2,264 transport | 134,533 |
| gwkill | 3 + 1 | 613,253 | 14,655 | 17,585 | 581,013 transport (connection refused while the gateways were down) | 222,336 |
| fdbstall | 3 + 1 | 21,990 | 9,859 | 11,768 | 284 (500 backend timeouts), 79 (503 commit outcome unknown, fenced) | 79,476 |
| fdbstallver | 3 + 1 | 23,643 | 10,628 | 12,649 | 322 (500), 44 (503 fenced) | 80,149 |
| fdbstallold (pre-fix gateways) | 3 | 16,129 | 7,228 | 8,669 | 231 (500), **1 answered 500 "generation is committed" for a write that was applied** | 60,878 |

- **Availability on one hot key** (measured): 64 writers on one key get
  about 65 acknowledged PUTs/s and 55 % of PUTs answered SlowDown
  (metadata commit conflicts on the key's current pointer, 32 attempts
  each); acknowledged PUT p50 about 200 ms, p99 0.65–1.1 s. With SDK-like
  retries 94 % succeed (6 % still SlowDown after three retries); with
  16 keys (4 writers per key) 0.05 % fail and throughput is 490 PUTs/s.
- **Integrity** (measured): no acknowledged write lost, no stale or
  backward read, no version out of order, every acknowledged versioned
  PUT exactly one version with the id it was answered with; in the
  versioned trials the nodes hold exactly three records per acknowledged
  PUT (no unknown PUT became visible in these runs).
- **Serializability argument** (code analysis, both reviews): a delayed
  commit of PUT X read the key's current pointer; FoundationDB rejects it
  if any write to the pointer committed after its read version, so X can
  never overwrite or reorder an acknowledged PUT committed after it read.

## 3. Defects found (review of the resolution paths; deterministic tests)

| # | Path | What happened before | Now | Test (fails with the fix removed) |
| --- | --- | --- | --- | --- |
| D1 | PUT `commit_resolving` (`crates/gateway/src/cluster/write.rs`) | after a commit attempt with an unknown outcome, a later attempt's error became the answer: 503 SlowDown "metadata transaction conflicts; retry" after 32 conflicts; the abort was best effort, so the first attempt could still apply: **a PUT answered SlowDown became the current object** (and an SDK retry in a versioned bucket a duplicate version). Also 412 or 500 "generation is committed" for writes that had applied (seen once in `fdbstallold`) | after an unknown attempt every later error goes through the fence: committed → success; aborted → 503 "not visible"; abort failed → 503 "outcome unknown" | `a_put_whose_outcome_is_unknown_is_never_answered_slowdown` |
| D2 | UploadPart `seal_resolving` | the same as D1 | the same | `a_part_whose_seal_is_unknown_is_never_answered_slowdown` |
| D3 | CompleteMultipartUpload `complete_resolving` (`backend.rs`) | the same (no fence: Complete may still apply) | after an unknown attempt a later error is 503 "complete outcome unknown" | `complete_and_delete_with_unknown_outcomes_are_never_answered_slowdown` |
| D4 | DELETE `delete_unconditional` | after the unknown attempt's re-run, its error (or a resolution read's) was the answer | 503 unknown | the same test |
| D5 | PUT resolution `version_of` | a versioned PUT resolved as committed after another PUT had become current was answered `null` (not its version id); a read error gave `null` too | the version record naming the generation; `null` only for a bucket without versioning (or suspended, when replaced by a later `null` write); otherwise 503 "applied, version id not resolved" | `a_resolved_versioned_put_answers_its_own_version_id` |
| D6 | UploadPart resolution, retiring part (pre-existing) | "retiring while the upload is open" was taken as "sealed, then superseded": a part whose pending write had **expired unsealed was answered as stored** | success only if the generation's header carries this part's ETag (a seal writes it; a pending part has none); otherwise 500 "upload it again"; read errors 503 | `an_expired_unsealed_part_is_not_reported_stored` |

No metadata layout, segment format or protocol changed;
`CommitFault::Conflict` is test infrastructure.

Remaining Low (review), recorded, not changed: a PUT whose retry after an
unknown attempt failed a precondition and whose abort then succeeded is
answered 503, though 412 would be correct; an unconditional DELETE whose
pin moved four times is answered 412 (pre-existing); a Complete retried
after an earlier attempt applied late is answered NoSuchUpload where S3
answers 200 (pre-existing).

## 4. Risk classification

- **R-114** (new): answers of the write paths that misstated a write's
  outcome after an unknown commit (D1–D6). Classified **High** by the
  rule of this phase (a version the client was told was refused became
  visible; an acknowledged UploadPart was not stored; a success answered
  with another version's id), with low likelihood (an unknown commit,
  then 32 conflicts or a lease expiry, then a failed abort). No
  acknowledged S3 write was lost: D1 can make visible only a write
  answered with an error, and D6's Complete then fails with InvalidPart.
  **Resolved in Phase 4.1** by the fixes above; the campaign's
  `fdbstallold` trials did not hit the window (it needs a failed abort).
- **R-112** (availability under hot-key contention) stays **Medium,
  open**: no data effect; 55 % SlowDown at 64 writers on one key, 6 %
  with SDK retries, 0.05 % at 4 writers per key. The fair per-key commit
  ordering of the plan is a performance item (Phase 4.1 P1 with the
  transaction work), not a correctness fix.

## 5. Artifacts

`benchmarks/linux/results/phase41-r112/` (summaries per trial: outcomes,
violations, settle and scrub records); `contend` in
`benchmarks/arcabench/src/contend.rs`; the tests in
`crates/testkit/tests/it/commit_timeouts.rs`.
