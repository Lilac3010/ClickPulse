#!/usr/bin/env bash
# Health + "are we all in sync?" check. Run on ANY VM:   bash /opt/clickpulse/scripts/check_cluster.sh
#   --all   (run on VM2) also compares config fingerprints of all 4 VMs over SSH
set -uo pipefail
set -a; . /opt/clickpulse/conf/cluster.env; set +a
ok(){ printf "  [OK]   %s\n" "$1"; }
bad(){ printf "  [FAIL] %s\n" "$1"; FAIL=1; }
FAIL=0
echo "== $(hostname): name resolution"
for h in vm1 vm2 vm3 vm4; do ip=$(getent hosts $h | awk '{print $1}'); [ -n "$ip" ] && ok "$h -> $ip" || bad "$h not resolvable"; done
echo "== ports"
for hp in vm1:9092 vm1:9094 vm1:9093 vm2:9000 vm2:9866 vm2:9870 vm3:7077 vm4:8888; do
  nc -z -w 3 "${hp%:*}" "${hp#*:}" 2>/dev/null && ok "$hp open" || bad "$hp unreachable (service down or firewall)"
done
echo "== kafka cluster (same bootstrap everywhere: $KAFKA_BOOTSTRAP)"
"$CP_VENV/bin/python" - <<'EOF' || FAIL=1
import os
from confluent_kafka.admin import AdminClient
md = AdminClient({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"]}).list_topics(timeout=10)
ids = sorted(b.id for b in md.brokers.values())
print(f"  cluster_id={md.cluster_id} brokers={ids} controller={md.controller_id}")
t = md.topics.get(os.environ["KAFKA_TOPIC"])
if t: print(f"  topic {os.environ['KAFKA_TOPIC']}: partitions={len(t.partitions)} replicas={[len(p.replicas) for p in t.partitions.values()]}")
assert ids == [1, 2], "expected both brokers 1 and 2"
print("  [OK]   both brokers up")
EOF
if command -v hdfs >/dev/null; then
  echo "== hdfs"; hdfs dfs -ls / >/dev/null 2>&1 && ok "hdfs reachable at $HDFS_URI" || bad "hdfs not reachable"
fi
echo "== config fingerprint: $(cd /opt/clickpulse && git rev-parse --short HEAD 2>/dev/null) / $(cat /opt/clickpulse/conf/cluster.env /opt/clickpulse/conf/hadoop/*.xml | md5sum | cut -c1-12)"
if [ "${1:-}" = "--all" ]; then
  echo "== fingerprints on all VMs (must be identical)"
  for h in vm1 vm2 vm3 vm4; do
    printf "  %s: " $h; ssh -o BatchMode=yes -o ConnectTimeout=5 clickpulse@$h \
      'cd /opt/clickpulse && echo "$(git rev-parse --short HEAD) / $(cat conf/cluster.env conf/hadoop/*.xml | md5sum | cut -c1-12)"' || echo "ssh failed"
  done
fi
[ $FAIL = 0 ] && echo "ALL CHECKS PASSED" || { echo "SOME CHECKS FAILED"; exit 1; }
