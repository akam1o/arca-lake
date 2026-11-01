#!/usr/bin/env bash
# Entrypoint for a test-only FoundationDB container that runs several
# fdbserver processes (ports 4500, 4501, ...) sharing one cluster file, so
# that failover tests can kill and restart individual processes with
# `docker exec <container> fdbproc kill|start|status N`.
#
# Environment:
#   FDB_PUBLIC_IP   address the processes advertise (default: the
#                   container's own address; use 127.0.0.1 with --network host)
#   FDB_PROCESSES   number of processes (default 3)
#
# This is a test configuration. It says nothing about how a deployment
# should place FoundationDB processes or coordinators.

set -euo pipefail

IP="${FDB_PUBLIC_IP:-$(hostname -i | awk '{print $1}')}"
N="${FDB_PROCESSES:-3}"
mkdir -p /var/fdb/logs /var/fdb/data
echo "docker:docker@${IP}:4500" > /var/fdb/fdb.cluster

cat > /usr/local/bin/fdbproc <<EOF
#!/usr/bin/env bash
# fdbproc start|kill|status N
set -euo pipefail
n="\$2"
port=\$((4499 + n))
case "\$1" in
  start)
    mkdir -p "/var/fdb/data/\$n"
    nohup fdbserver -p "${IP}:\$port" -d "/var/fdb/data/\$n" -L /var/fdb/logs \\
      -C /var/fdb/fdb.cluster > "/var/fdb/logs/proc-\$n.out" 2>&1 &
    echo \$! > "/var/fdb/pid-\$n"
    echo "started \$n (pid \$!)"
    ;;
  kill)
    if [[ -f "/var/fdb/pid-\$n" ]]; then
      kill -9 "\$(cat "/var/fdb/pid-\$n")" 2>/dev/null || true
      rm -f "/var/fdb/pid-\$n"
    fi
    echo "killed \$n"
    ;;
  status)
    if [[ -f "/var/fdb/pid-\$n" ]] && kill -0 "\$(cat "/var/fdb/pid-\$n")" 2>/dev/null; then
      echo up
    else
      echo down
    fi
    ;;
  *) echo "usage: fdbproc start|kill|status N" >&2; exit 2 ;;
esac
EOF
chmod +x /usr/local/bin/fdbproc

for i in $(seq 1 "$N"); do fdbproc start "$i"; done
echo "fdb processes started on ${IP}:4500..$((4499 + N))"
exec sleep infinity
