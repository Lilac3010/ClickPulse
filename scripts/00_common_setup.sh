#!/usr/bin/env bash
# Run on EVERY VM (as root):   sudo bash /opt/clickpulse/scripts/00_common_setup.sh vm1   (vm2 / vm3 / vm4)
# Idempotent: safe to re-run.
set -euo pipefail
ME="${1:?usage: 00_common_setup.sh vm1|vm2|vm3|vm4}"
CP=/opt/clickpulse
set -a; . "$CP/conf/cluster.env"; set +a

echo ">> [1/6] hostname -> $ME"
hostnamectl set-hostname "$ME"

echo ">> [2/6] /etc/hosts (remove 127.0.1.1 self-mapping, add the 4 VMs)"
sed -i '/^127\.0\.1\.1/d' /etc/hosts
sed -i '/# BEGIN CLICKPULSE/,/# END CLICKPULSE/d' /etc/hosts
cat >> /etc/hosts <<EOF
# BEGIN CLICKPULSE
$VM1_IP vm1
$VM2_IP vm2
$VM3_IP vm3
$VM4_IP vm4
# END CLICKPULSE
EOF

echo ">> [3/6] packages (Java 11, Python, tools)"
apt-get update -y
DEBIAN_FRONTEND=noninteractive apt-get install -y openjdk-11-jdk-headless python3 python3-venv python3-pip \
  curl wget rsync netcat-openbsd jq git chrony
ln -sfn "$(ls -d /usr/lib/jvm/java-11-openjdk-* | head -1)" /usr/lib/jvm/java-11
timedatectl set-timezone UTC || true          # all VMs on UTC: event_date = UTC date
systemctl enable --now chrony || true         # clocks in sync across VMs

echo ">> [4/6] service user + data dirs"
id clickpulse >/dev/null 2>&1 || useradd -m -s /bin/bash clickpulse
mkdir -p /data/kafka /data/hadoop /opt/clickpulse/jars /opt/clickpulse/logs
chown -R clickpulse:clickpulse /data /opt/clickpulse

echo ">> [5/6] shared Python venv (same libs everywhere)"
sudo -u clickpulse python3 -m venv "$CP_VENV"
sudo -u clickpulse "$CP_VENV/bin/pip" install -q --upgrade pip
sudo -u clickpulse "$CP_VENV/bin/pip" install -q confluent-kafka==2.6.1 requests==2.32.3 pandas==2.2.3

echo ">> [6/6] login environment for every shell"
cat > /etc/profile.d/clickpulse.sh <<'EOF'
set -a; . /opt/clickpulse/conf/cluster.env; set +a
export PATH="$CP_VENV/bin:$KAFKA_HOME/bin:$HADOOP_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
EOF
if command -v ufw >/dev/null && ufw status | grep -q active; then
  ufw allow from "$CLUSTER_SUBNET"; ufw allow 22/tcp
fi
echo "DONE on $ME. Log out and back in (or: source /etc/profile.d/clickpulse.sh)."
