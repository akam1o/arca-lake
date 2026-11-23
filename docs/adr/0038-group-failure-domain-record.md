# ADR-0038: A group's failure domain is recorded and checked by every member move

Status: **accepted** (0.2.0 preparation, R-119). Implemented with its
reproduction tests. Adds one metadata key (`("al","gdomain",group)`);
no record, schema, protocol or segment format version changes.

## 1. Problem

A stripe group tolerates `m` member losses only if its members fail
independently: they are chosen across a topology label (the profile's
`failure_domain`, `host` by default) whose values must differ. 0.1.0
records the label nowhere. Every later member move (auto repair,
`admin replace-member`, a drain, a repair's retarget, the dry-run
planner) took it from the controller's `--failure-domain` flag. The
0.1.0 preparation fixed how the remaining members' domains are found
(R-119's first part: from every node record, not from the candidates),
and left:

1. a controller whose flag differs from the label the groups were
   created with moves members across the wrong label (two members of a
   `host` group on one host, if the flag says `rack`);
2. nothing reports a group whose members already share a domain (from a
   move made before the fix, a changed label, a flag mismatch);
3. a move chosen before the fix and interrupted after its fence is
   resumed onto its recorded target, whatever its domain;
4. a member whose node lacks the label made `choose_replacement` fail
   with `MissingLabel`, which `repair_pass` took as a failed durable
   group: every cache-group repair of that pass was skipped, on every
   pass, and `replace-member` and drains showed nothing; a candidate
   without the label failed every group's replacement;
5. registration accepted a node without the label.

## 2. Decision

### 2.1 The record

Each group's failure domain is recorded under a key of its own,
`("al","gdomain",group)`, holding `GroupDomain { group, label, source,
set_ms }` (postcard; `source` is `Created` or `Operator`). It is not a
`GroupRecord` field: group records are postcard, a 0.1.0 process
decodes a record with trailing bytes and drops them when it rewrites
the group (every membership change and repair-cursor update rewrites
it), so a field would be lost silently in a mixed cluster. 0.1.0 never
reads, lists or rewrites the new key.

- `admin create-groups` and `admin create-cache-groups` record the
  label in the transaction that creates the group
  (`cluster::create_group_in_domain`): a group created by 0.2.0 never
  exists without it. One-member `cache-v1` groups (nothing to differ
  from) are not recorded.
- A group created by 0.1.0 (or by a 0.1.0 `admin create-groups` during
  a rolling upgrade) has no record. It is **never inferred** from the
  members' labels: a group that already breaks its domain would be
  inferred into a label it does not keep, and two labels can both fit.
- `admin group set-failure-domain --group G --label L` (or
  `--all-unrecorded`, which lists the groups first; `--dry-run` writes
  nothing) records it; replacing a different recorded label needs
  `--change`. It moves nothing. It refuses, writing nothing for any
  group, a label that no member's node of a group carries (most likely
  a typo; `--force` records it anyway), with `--change` too. A group
  that breaks the label can be recorded and then repaired with
  `replace-member`: the command prints what the members break and exits
  2 (0: recorded cleanly; 1: refused); `admin group check` shows it
  too. It is a step of the 0.1.0 → 0.2.0 upgrade, right after the
  controllers (operations guide).

### 2.2 One rule for every membership change

`failure_domain::label_for` and `check_move` (controller) and
`group_domain::check_replacement` (metadata) are the only rule:

- the label is the group's recorded one. The controller flag is used
  only for a one-member group without a record (it then only names the
  label a candidate must carry);
- a group of several members without a record moves no member: auto
  repair, replacement requests, drains, retargets, the dry-run planner
  and a resumed epoch change refuse with "group G has no recorded
  failure domain ... `admin group set-failure-domain`";
- the incoming node must carry the label, every other member's node
  must too (a member whose node record is gone or lacks the label has
  an unknown domain), and the incoming value must differ from all of
  theirs. Missing information refuses; it never skips the check;
- the membership commit of every move
  (`epoch_intent::commit_member`, used by `repair::replace_member_with`)
  checks the rule again in its own transaction against the record and
  the node records as they are then: whoever chose the target, a move
  never commits into a domain the group uses. A refused commit leaves
  the epoch intent open; the next leader pass settles it (membership
  unchanged).

Candidates without the label are passed over (they no longer fail the
choice for every group). A refusal holds back only its group: it is
reported on the replacement request (`last_error`) or the drain
(`blocked`) as the reason, counted by
`arca_controller_failure_domain_refusals_total`, and does not keep the
pass's cache-group repair waiting, unlike a failed member that simply
has no free domain yet (unchanged).

A `Repairing` group whose next move (a retarget, a second failed
member's replacement) is refused only because the group has no record
shows the repair reason `awaiting-failure-domain`, not
`awaiting-nodes`: bringing nodes back does not help, recording the
domain does. The repair status record (`("al","repairstat",group)`) is
read by 0.1.0, which fails to decode an enum variant it does not know
(and with it its whole status listing): the reason is stored as
`AwaitingNodes` with the detail prefixed `awaiting-failure-domain: `,
and reported as the new reason by this release's reads. 0.1.0 shows it
as `awaiting-nodes` with that detail. Recording the domain ends the
wait at once (the next leader pass), not after the repair's backoff.

A recorded group whose move is refused because a member's node lacks
the label (its domain is unknown) keeps `awaiting-nodes` (or
`awaiting-capacity`), with the refusal ("member node N has no 'L'
label") as the detail. Not a reason of its own: the fix is a node's
re-registration with the label, the same kind of operator action on a
node as for `awaiting-nodes`, and a further reason would need the same
stored encoding for a case the audit already reports by node
(`admin group check`, `arca_controller_groups_failure_domain_violations`).

### 2.3 Repair that does not change membership

Allowed for a group without a record: stripe repair of damaged shards
(scrub and degraded-read repair tasks) rewrites a shard on its existing
member, and the rebuild of a slot whose membership was already
committed (a `Repairing` group, `Decision::Run`) only fills that member.
Neither adds a member to a domain; refusing them would leave known
damage unrepaired, which lowers the redundancy the group has now in
exchange for nothing. A retarget, or a second failed member's
replacement, changes membership and is refused.

### 2.4 Resumed epoch changes

An open epoch intent (`("al","epochint",group)`) names a target chosen
by whichever release started the move. The leader resumes it only if
`check_move` admits it; otherwise it is settled with the membership
unchanged (the existing safe path for an intent no longer wanted), and
the slot's request or drain chooses again under the rule. The commit
check (§2.2) covers a resume that raced with a label change.

### 2.5 Detection

`failure_domain::audit` reads nodes, groups and records and reports:
groups with two members on one value of their label, members whose node
lacks the label or has no record, groups without a record. It never
moves a member, rewrites placement or deletes data. It is shown by
`admin status` and `admin group check` (exit 1 when anything is found),
computed by the leader on every repair pass (gauges
`arca_controller_groups_failure_domain_violations` and
`arca_controller_groups_failure_domain_unrecorded`; findings logged once
per term and when their number changes), and printed for the node by
`admin register-node`. The dangerous case, an unrecorded group with a
member down or a repair under way (a failure that is not repaired
until the domain is recorded), has its own gauge
(`arca_controller_groups_failure_domain_unrecorded_at_risk`, from the
leader's health view) and a `WARNING: failure domain unrecorded on
groups ...` line in `admin status` and `admin group check` (there a
member is down when its node is retired, unregistered or unseen for
60 s, the controllers' default `--unhealthy-after-secs`). A recorded label that differs from the
controller's flag is logged once per term (the recorded one is used).

### 2.6 Registration

Refused (not only flagged): `register_node` refuses a node without a
label that the recorded domain of a group it is a member of, or could
join, requires.

"Could join" is decided by `device_class` alone: a group is joinable
when one of its members' nodes has the registering node's
`device_class` value (both absent counts as equal), the one constraint
every replacement keeps. Consequences:

- a node of a class no recorded group uses registers without the
  failure-domain labels; if a group of its class is created or
  recorded later, the node is not refused retroactively, and it is
  passed over as a candidate (an unlabeled candidate never joins) and
  reported by the audit only if it is a member;
- the rule does not look at tiers, capacity, health or draining: a
  node that could never be chosen for other reasons is still asked for
  the label of any group of its class;
- relabeling a node into another `device_class` is checked against the
  groups of the new class (and every group it is a member of);
- a node without a `device_class` label is matched with groups whose
  members have none. Such a
node could never be compared with the other members, and every move it
took part in would be refused; refusing at the command the operator is
running is the clearest place. A changed value is accepted (a topology
correction is legitimate) and the groups it now breaks are reported by
`register-node` and the audit; they are repaired with `replace-member`.
A 0.1.0 `admin register-node` does not refuse: the audit reports the
result as a member without the label.

### 2.7 Mixed versions and schema 5

Until the store reaches metadata schema 5 a 0.1.0 controller may lead,
and it moves members by its flag only, ignoring the records and the
commit check; a 0.1.0 `admin create-groups` creates unrecorded groups.
0.2.0 cannot prevent that. It therefore:

- enforces the recorded domain wherever one is recorded and refuses
  unrecorded groups (§2.2);
- reports the state as degraded (`admin status`, `admin group check`:
  "failure-domain enforcement: degraded (metadata schema 4 < 5 ...)");
- re-checks every group's membership against the records on every
  leader pass (§2.5), so a group changed under a 0.1.0 leadership is
  reported by the next 0.2.0 leader.

From schema 5 (`keys::SCHEMA_V5`; the schema change itself belongs to
the upgrade work, ADR-0039) no 0.1.0 process can start, so every
protection above holds without exception; `admin upgrade finalize` to
schema 5 or later is refused while a group of several members has no
recorded domain (a precondition of the finalize). This ADR does not
raise `SCHEMA_VERSION`.

## 3. Format and compatibility

- New key `("al","gdomain",group)`, value `GroupDomain` (postcard:
  `u64`, string, enum index, `u64`). Written by 0.2.0 at schema 4;
  0.1.0 never reads it (no 0.1.0 code lists the `al` subspace by
  decoding unknown prefixes; backups copy the subspace whole).
- A later field added to `GroupDomain` would be dropped by this
  release's `set` rewriting the record: extend it under a new key, or
  raise the record's handling with a schema change.
- No `GroupRecord`, epoch intent, request or node record changes. The
  repair status record gains no variant on disk (§2.2: the new reason
  is stored as `AwaitingNodes` with a detail prefix).
- Downgrade to 0.1.0 (before schema 5): the records stay and are
  ignored; moves made meanwhile are reported by the next 0.2.0 leader.

## 4. Tests

`crates/testkit/tests/it/failure_domain.rs` (each fails on the
previous main):

- `a_new_group_records_its_failure_domain`;
- `a_group_without_a_recorded_domain_moves_no_member_until_it_is_set`
  (a 0.1.0-style group: refused with the reason on the request, then
  moved within the domain once set);
- `the_recorded_domain_wins_over_the_controller_flag` (flag `rack`,
  group `host`: the only free-rack node shares a host; refused; the dry
  run agrees);
- `a_member_without_the_label_holds_back_only_its_group` (cache-group
  repair goes on; the request shows the missing label);
- `a_resumed_pre_fix_move_that_breaks_the_domain_is_settled`;
- `a_missing_label_is_refused_and_a_changed_one_reported` (registration;
  the audit and the leader's gauge report an existing violation and move
  nothing);
- `the_membership_commit_refuses_a_relabel_after_the_choice` and
  `the_membership_commit_refuses_a_changed_label_after_the_choice` (a
  relabel, or `set-failure-domain --change`, lands between the target's
  choice and the membership commit: the commit refuses in its
  transaction, membership unchanged);
- `a_drain_and_the_planner_are_blocked_by_an_unrecorded_group`;
- `an_unrecorded_repair_awaits_its_failure_domain` (repair reason,
  at-risk gauge and audit; the repair goes on once recorded).

CLI tests (`crates/cli`): `--all-unrecorded` (listing, dry run, write),
`--change`, the refusal of a label no member carries and `--force`, the
exit codes. Finalize: `finalize_blocker` below and at schema 5.

Unit tests: the rule and the setter (`metadata/src/ops/group_domain.rs`;
with compatibility tests: the record's pinned body, the key outside the
group listing, a group rewrite through `update_group` keeping the record),
the audit and `label_for` (`controller/src/failure_domain.rs`), an
unlabeled candidate passed over (`placement`).

## 5. Alternatives rejected

- A `GroupRecord` field: lost when a 0.1.0 process rewrites the group.
- Inferring the label of 0.1.0 groups from their members' labels: turns
  an existing violation into a wrong record, and is ambiguous.
- Keeping the controller flag as a fallback for unrecorded groups:
  that is the dependency R-119 removes; a mismatch moves members into a
  wrong domain without any trace.
- Refusing every repair of an unrecorded group: leaves known damage
  unrepaired without protecting any domain (§2.3).
