#!/usr/bin/env bash
# A test-only FoundationDB cluster of three user-space processes, with or
# without TLS, under one directory (Phase 3.4 FoundationDB TLS checks,
# soak tests). Nothing outside DIR is touched; processes are tracked by
# PID files and stopped by PID.
#
#   fdb-tls-cluster.sh start   DIR BASEPORT [tls|plain]
#   fdb-tls-cluster.sh kill    DIR N        # SIGKILL process N (1..3)
#   fdb-tls-cluster.sh restart DIR N
#   fdb-tls-cluster.sh stop    DIR
#
# Environment: ARCA_BIN (arca-lake binary, for the development PKI),
# FDBSERVER (default /usr/sbin/fdbserver), FDBCLI (default fdbcli),
# TRUST_DOMAIN (default fdb-test.arca-lake.invalid), FDB_EXTRA_ARGS (extra
# fdbserver flags, e.g. memory limits; kept in DIR/extra-args for restarts).
#
# With tls, DIR/pki holds: ca (the cluster CA), fdb-N (server
# certificates), admin, gateway, controller (client certificates of that
# CA), other-ca + foreign (a client of another CA), expired (an expired
# client of the right CA). DIR/env.sh exports the client settings for
# arca-lake processes.
set -euo pipefail
cmd="${1:?start|kill|restart|stop}"
DIR="${2:?directory}"
FDBSERVER="${FDBSERVER:-/usr/sbin/fdbserver}"
FDBCLI="${FDBCLI:-fdbcli}"
TD="${TRUST_DOMAIN:-fdb-test.arca-lake.invalid}"
mkdir -p "$DIR"

mode() { cat "$DIR/mode" 2>/dev/null || echo plain; }
port() { echo $(( $(cat "$DIR/baseport") + $1 - 1 )); }

start_proc() { # N
  local n="$1" p; p="$(port "$n")"
  local addr="127.0.0.1:$p" tls_args=()
  if [ "$(mode)" = tls ]; then
    addr="$addr:tls"
    tls_args=(--tls-certificate-file "$DIR/pki/fdb-$n/cert.pem" --tls-key-file "$DIR/pki/fdb-$n/key.pem" --tls-ca-file "$DIR/pki/ca/ca.pem")
  fi
  mkdir -p "$DIR/data/$n" "$DIR/logs"
  nohup "$FDBSERVER" -p "$addr" -l "$addr" -d "$DIR/data/$n" -L "$DIR/logs" -C "$DIR/fdb.cluster" \
    --locality-machineid "m$n" --locality-zoneid "z$n" "${tls_args[@]}" $(cat "$DIR/extra-args" 2>/dev/null) \
    > "$DIR/logs/proc-$n.out" 2>&1 < /dev/null &
  echo $! > "$DIR/pid-$n"
}

kill_proc() { # N
  local f="$DIR/pid-$1"
  if [ -f "$f" ]; then
    local pid; pid="$(cat "$f")"
    # Only our own fdbserver: the PID's command line must name DIR.
    if tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null | grep -q -- "$DIR/data/$1"; then
      kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$f"
  fi
}

cli() {
  local args=(-C "$DIR/fdb.cluster" --timeout 20)
  if [ "$(mode)" = tls ]; then
    args+=(--tls-certificate-file "$DIR/pki/admin/cert.pem" --tls-key-file "$DIR/pki/admin/key.pem" --tls-ca-file "$DIR/pki/ca/ca.pem")
  fi
  "$FDBCLI" "${args[@]}" --exec "$1"
}

case "$cmd" in
  start)
    base="${3:?base port}"; m="${4:-plain}"
    echo "$base" > "$DIR/baseport"; echo "$m" > "$DIR/mode"; echo "${FDB_EXTRA_ARGS:-}" > "$DIR/extra-args"
    sfx=""; [ "$m" = tls ] && sfx=":tls"
    echo "test:arca$base@127.0.0.1:$base$sfx,127.0.0.1:$((base + 1))$sfx,127.0.0.1:$((base + 2))$sfx" > "$DIR/fdb.cluster"
    if [ "$m" = tls ]; then
      : "${ARCA_BIN:?ARCA_BIN (arca-lake binary) is needed to create the PKI}"
      P="$DIR/pki"; mkdir -p "$P"
      "$ARCA_BIN" tls init-ca --dir "$P/ca" > /dev/null
      for n in 1 2 3; do "$ARCA_BIN" tls issue --ca-dir "$P/ca" --trust-domain "$TD" --role fdb --name "$n" --out "$P/fdb-$n" > /dev/null; done
      for r in admin gateway controller; do "$ARCA_BIN" tls issue --ca-dir "$P/ca" --trust-domain "$TD" --role "$r" --name "$r" --out "$P/$r" > /dev/null; done
      "$ARCA_BIN" tls init-ca --dir "$P/other-ca" --name "unrelated CA" > /dev/null
      "$ARCA_BIN" tls issue --ca-dir "$P/other-ca" --trust-domain "$TD" --role admin --name foreign --out "$P/foreign" > /dev/null
      "$ARCA_BIN" tls issue --ca-dir "$P/ca" --trust-domain "$TD" --role admin --name expired --days 1 --backdate-days 3 --out "$P/expired" > /dev/null
      cat > "$DIR/env.sh" <<ENV
export ARCA_FDB_CLUSTER_FILE=$DIR/fdb.cluster
export ARCA_FDB_TLS_CA=$P/ca/ca.pem
export ARCA_FDB_TLS_CERT=$P/admin/cert.pem
export ARCA_FDB_TLS_KEY=$P/admin/key.pem
export ARCA_FDB_TLS_VERIFY_PEERS='Check.Valid=1,S.subjectAltName>=URI:spiffe://$TD/fdb/'
ENV
    else
      echo "export ARCA_FDB_CLUSTER_FILE=$DIR/fdb.cluster" > "$DIR/env.sh"
    fi
    for n in 1 2 3; do start_proc "$n"; done
    sleep 3
    cli "configure new double ssd" || true
    for _ in $(seq 1 40); do cli "status minimal" 2>/dev/null | grep -q "is available" && break; sleep 2; done
    cli "status minimal"
    ;;
  kill) kill_proc "${3:?N}" ;;
  restart) kill_proc "${3:?N}"; start_proc "$3" ;;
  stop) for n in 1 2 3; do kill_proc "$n"; done ;;
  *) echo "usage: $0 start|kill|restart|stop DIR ..." >&2; exit 2 ;;
esac
