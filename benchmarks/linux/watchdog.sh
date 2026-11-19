#!/usr/bin/env bash
# Resource watchdog for load runs on shared test hosts: every INTERVAL
# seconds it checks each host and, on the first breach, writes STOP_FILE
# with the reason, stops the load generator (arcabench, on LOAD_TARGET)
# and exits 2. A driver checks STOP_FILE before each run. Read-only
# otherwise: it never stops or reconfigures anything but the generator,
# and checks a host's system FoundationDB with `status minimal` only.
# Host addresses come only from the environment and are not logged.
#
#   WATCH_HOSTS="user@a user@b user@c" LOAD_TARGET=user@a \
#   SYSTEM_FDB_HOSTS="user@a user@c" STOP_FILE=/tmp/run.stop watchdog.sh
#
# Limits (environment, defaults in brackets): MIN_MEM_MB available memory
# [1024]; MIN_DISK_GB free on / and on DATA_MOUNT [/mnt/arca-fdb] [3];
# MAX_LOAD_PER_CPU one-minute load per CPU [4]; MAX_SSH_FAILS consecutive
# unreachable checks of one host [6]; INTERVAL seconds [10]; LOG [stderr].
set -uo pipefail
: "${WATCH_HOSTS:?}" "${LOAD_TARGET:?}" "${STOP_FILE:?}"
MIN_MEM_MB="${MIN_MEM_MB:-1024}"; MIN_DISK_GB="${MIN_DISK_GB:-3}"
MAX_LOAD_PER_CPU="${MAX_LOAD_PER_CPU:-4}"; MAX_SSH_FAILS="${MAX_SSH_FAILS:-6}"
INTERVAL="${INTERVAL:-10}"; DATA_MOUNT="${DATA_MOUNT:-/mnt/arca-fdb}"
SYSTEM_FDB_HOSTS="${SYSTEM_FDB_HOSTS:-}"
log() { echo "$(date -u +%FT%TZ) $*" >> "${LOG:-/dev/stderr}"; }
# Plain arrays (bash 3 on macOS has no associative arrays): host i is
# "host<i>" in the log.
read -r -a HOSTS <<< "$WATCH_HOSTS"
idx=${#HOSTS[@]}
fails=(); for i in "${!HOSTS[@]}"; do fails[i]=0; done
name_of() { local i; for i in "${!HOSTS[@]}"; do [ "${HOSTS[i]}" = "$1" ] && { echo "host$i"; return; }; done; echo "a host"; }

stop() {
  echo "$(date -u +%FT%TZ) $*" > "$STOP_FILE"
  log "STOP: $*"
  ssh -o BatchMode=yes -o ConnectTimeout=10 "$LOAD_TARGET" 'pkill -INT -x arcabench; sleep 5; pkill -KILL -x arcabench' > /dev/null 2>&1 || true
  exit 2
}

log "watchdog start: ${idx} hosts, mem>=${MIN_MEM_MB}MB disk>=${MIN_DISK_GB}GB load<=${MAX_LOAD_PER_CPU}/cpu"
while true; do
  [ -f "$STOP_FILE" ] && { log "stop file present: exit"; exit 0; }
  for i in "${!HOSTS[@]}"; do
    h="${HOSTS[i]}"; n="host$i"
    out=$(ssh -o BatchMode=yes -o ConnectTimeout=10 "$h" "awk '/MemAvailable/{print int(\$2/1024)}' /proc/meminfo; nproc; cut -d' ' -f1 /proc/loadavg; df -BG --output=avail / | tail -1 | tr -dc 0-9; echo; df -BG --output=avail $DATA_MOUNT 2>/dev/null | tail -1 | tr -dc 0-9; echo" 2>/dev/null)
    if [ -z "$out" ]; then
      fails[i]=$(( ${fails[i]} + 1 ))
      log "$n unreachable (${fails[i]})"
      [ "${fails[i]}" -ge "$MAX_SSH_FAILS" ] && stop "$n unreachable ${fails[i]} times"
      continue
    fi
    fails[i]=0
    read -r mem cpus load root data <<< "$(echo "$out" | tr '\n' ' ')"
    [ "${mem:-0}" -lt "$MIN_MEM_MB" ] && stop "$n memory available ${mem} MB"
    [ "${root:-0}" -lt "$MIN_DISK_GB" ] && stop "$n / free ${root} GB"
    [ -n "${data:-}" ] && [ "$data" -lt "$MIN_DISK_GB" ] && stop "$n $DATA_MOUNT free ${data} GB"
    awk -v l="$load" -v c="$cpus" -v m="$MAX_LOAD_PER_CPU" 'BEGIN{exit !(l > c * m)}' && stop "$n load ${load} on ${cpus} CPUs"
  done
  for h in $SYSTEM_FDB_HOSTS; do
    ssh -o BatchMode=yes -o ConnectTimeout=10 "$h" "timeout 20 fdbcli -C /etc/foundationdb/fdb.cluster --exec 'status minimal'" 2>/dev/null | grep -q 'is available' \
      || stop "system FoundationDB of $(name_of "$h") not available"
  done
  sleep "$INTERVAL"
done
