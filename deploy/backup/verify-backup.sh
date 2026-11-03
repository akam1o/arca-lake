#!/usr/bin/env bash
# Continuous metadata backup verification (RFC-015 §11 B-A5): a backup is
# trusted only once it has been restored and checked. Run it from a
# scheduler (cron, a systemd timer, a CronJob); alert on the text-file
# metrics it writes (deploy/prometheus/arca-lake-alerts.yml).
#
# A metadata backup holds no shard data: this proves the metadata can be
# restored and is consistent, not that the shards it names exist.
#
#   verify-backup.sh init-target --restore-cluster-file R
#       Mark R as a dedicated verification cluster. Refused unless R holds
#       no key at all.
#   verify-backup.sh run --source-cluster-file F --restore-cluster-file R
#       --backup-url URL [--create-backup] [--metrics-file M] [--work DIR]
#       [--clear-target] [--keep-restored]
#       1. (--create-backup) back up the arca-lake key range of F to URL;
#       2. check URL is restorable and how old its restorable point is;
#       3. restore it into R (refused unless R is marked, is not F, shares
#          no coordinator with F, and holds no arca-lake key — or
#          --clear-target clears that range of R first);
#       4-8. arca-lake admin backup verify-restored on R: schema, cluster
#          state and epochs, generations against chunk records and groups,
#          multipart references, versioning; metrics to M;
#       9. a report in DIR;
#       10. clear the restored range of R (unless --keep-restored).
#
# Environment: ARCA_BIN (arca-lake), FDBBACKUP, FDBRESTORE, FDBCLI (tools),
# BACKUP_TIMEOUT_SECS (3600). TLS: the ARCA_FDB_TLS_* settings of
# arca-lake also configure the FoundationDB tools (FDB_TLS_*); plaintext
# clusters need ARCA_FDB_INSECURE_PLAINTEXT=1 as for every arca-lake
# process. Exit: 0 verified, 2 findings, 1 refused or failed.
set -euo pipefail
ARCA_BIN="${ARCA_BIN:-arca-lake}"
FDBBACKUP="${FDBBACKUP:-fdbbackup}"
FDBRESTORE="${FDBRESTORE:-fdbrestore}"
FDBCLI="${FDBCLI:-fdbcli}"
TIMEOUT="${BACKUP_TIMEOUT_SECS:-3600}"
MARKER_KEY="arca_lake_backup_verification_target"
# Backups this job creates use their own tag, so that discontinuing them
# never touches an operator's continuous backup (default tag).
TAG="${BACKUP_TAG:-arca_lake_verify}"

die() { echo "verify-backup: $*" >&2; exit 1; }
# FoundationDB tools take TLS settings from FDB_TLS_*.
[ -n "${ARCA_FDB_TLS_CA:-}" ] && export FDB_TLS_CA_FILE="$ARCA_FDB_TLS_CA"
[ -n "${ARCA_FDB_TLS_CERT:-}" ] && export FDB_TLS_CERTIFICATE_FILE="$ARCA_FDB_TLS_CERT"
[ -n "${ARCA_FDB_TLS_KEY:-}" ] && export FDB_TLS_KEY_FILE="$ARCA_FDB_TLS_KEY"
[ -n "${ARCA_FDB_TLS_VERIFY_PEERS:-}" ] && export FDB_TLS_VERIFY_PEERS="$ARCA_FDB_TLS_VERIFY_PEERS"

cli() { timeout 60 "$FDBCLI" -C "$1" --timeout 30 --exec "$2"; }
coords() { sed -n 's/^[^#]*@//p' "$1" | tr ',' '\n' | sed 's/:tls$//' | sort -u; }

cmd="${1:-}"; shift || true
SRC="" DST="" URL="" METRICS="" WORK="." CREATE=0 CLEAR=0 KEEP=0
while [ $# -gt 0 ]; do
  case "$1" in
    --source-cluster-file) SRC="$2"; shift 2 ;;
    --restore-cluster-file) DST="$2"; shift 2 ;;
    --backup-url) URL="$2"; shift 2 ;;
    --metrics-file) METRICS="$2"; shift 2 ;;
    --work) WORK="$2"; shift 2 ;;
    --create-backup) CREATE=1; shift ;;
    --clear-target) CLEAR=1; shift ;;
    --keep-restored) KEEP=1; shift ;;
    *) die "unknown argument $1" ;;
  esac
done

[ -n "$DST" ] || die "no --restore-cluster-file: refusing (the restore target must be named explicitly)"
[ -r "$DST" ] || die "$DST is not readable"

case "$cmd" in
  init-target)
    if cli "$DST" 'getrange "" \xff 1' | grep -q '`'; then
      die "$DST holds keys: refusing to mark a cluster that is not empty"
    fi
    cli "$DST" "writemode on; set $MARKER_KEY 1" > /dev/null
    echo "marked $DST as a backup verification target"
    exit 0 ;;
  run) ;;
  *) die "usage: $0 init-target|run ..." ;;
esac

[ -n "$SRC" ] && [ -r "$SRC" ] || die "--source-cluster-file is needed"
[ -n "$URL" ] || die "--backup-url is needed"
# Never the source cluster, never a cluster sharing a coordinator with it.
[ "$(readlink -f "$SRC")" != "$(readlink -f "$DST")" ] || die "the restore target is the source cluster file"
if [ -n "$(comm -12 <(coords "$SRC") <(coords "$DST"))" ]; then
  die "the restore target shares coordinators with the source: refusing"
fi
cli "$DST" "get $MARKER_KEY" | grep -q "is \`1'" \
  || die "$DST is not marked as a verification target (run init-target on a dedicated, empty cluster)"
read -r BEGIN END < <("$ARCA_BIN" admin --cluster-file "$DST" backup key-range)
if cli "$DST" "getrange $BEGIN $END 1" | grep -q '`'; then
  [ "$CLEAR" = 1 ] || die "$DST already holds arca-lake metadata (an earlier run kept it?): use --clear-target"
  cli "$DST" "writemode on; clearrange $BEGIN $END" > /dev/null
fi
mkdir -p "$WORK"
REPORT="$WORK/backup-verification-$(date -u +%Y%m%dT%H%M%SZ).txt"
rc=0
{
  echo "source cluster file: $SRC"
  echo "restore target: $DST"
  echo "backup: $URL"
  if [ "$CREATE" = 1 ]; then
    echo "1. backup of the arca-lake range (tag $TAG)"
    timeout "$TIMEOUT" "$FDBBACKUP" start -C "$SRC" -t "$TAG" -d "$URL" -k "$BEGIN $END" -w
    # `start -w` returns once the backup completed and stopped; stop it
    # explicitly in case a version keeps it running (never the default tag).
    timeout 600 "$FDBBACKUP" discontinue -C "$SRC" -t "$TAG" -w 2>/dev/null || true
  fi
  # URL may be a container of backups: verify the newest one in it.
  BURL="$(timeout 300 "$FDBBACKUP" list -b "$URL" 2>/dev/null | sed -n 's/^[[:space:]]*\(file:.*\|blobstore:.*\)$/\1/p' | sort | tail -n 1)"
  BURL="${BURL:-$URL}"
  echo "backup verified: $BURL"
  echo "2. restorable?"
  desc="$(timeout 300 "$FDBBACKUP" describe -d "$BURL" --version_timestamps -C "$SRC")"
  echo "$desc" | grep -E 'Restorable|MaxRestorableVersion|MinRestorableVersion' || true
  echo "$desc" | grep -q 'Restorable: true' || { echo "not restorable"; exit 1; }
  # Restorable point as Unix ms (the tool prints YYYY/MM/DD.HH:MM:SS+zone).
  ts="$(echo "$desc" | sed -n 's/^MaxRestorableVersion:.*(\(.*\)).*/\1/p' | head -n 1)"
  END_MS=""
  if [ -n "$ts" ]; then
    END_MS="$(date -u -d "$(echo "$ts" | sed -E 's#^([0-9]+)/([0-9]+)/([0-9]+)\.#\1-\2-\3 #')" +%s 2>/dev/null || true)"
    [ -n "$END_MS" ] && END_MS=$((END_MS * 1000))
  fi
  echo "3. restore into the verification cluster"
  t0=$(date +%s.%N)
  timeout "$TIMEOUT" "$FDBRESTORE" start -r "$BURL" --dest-cluster-file "$DST" -k "$BEGIN $END" -w
  t1=$(date +%s.%N)
  RESTORE_S="$(echo "$t1 - $t0" | bc)"
  echo "restore took ${RESTORE_S}s"
  echo "4-8. verification of the restored metadata"
  set +e
  "$ARCA_BIN" admin --cluster-file "$DST" backup verify-restored \
    ${METRICS:+--metrics-file "$METRICS"} ${END_MS:+--backup-end-ms "$END_MS"} --restore-seconds "$RESTORE_S"
  rc=$?
  set -e
  echo "verification exit code $rc"
  if [ "$KEEP" = 0 ]; then
    echo "10. clearing the restored range of the verification cluster"
    cli "$DST" "writemode on; clearrange $BEGIN $END" > /dev/null
  fi
} 2>&1 | tee "$REPORT"
rc="$(sed -n 's/^verification exit code //p' "$REPORT" | tail -n 1)"
echo "9. report: $REPORT"
exit "${rc:-1}"
