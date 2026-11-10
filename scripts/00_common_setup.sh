#!/usr/bin/env bash
# Run on EVERY VM (as root):   sudo bash /opt/clickpulse/scripts/00_common_setup.sh vm1   (vm2 / vm3 / vm4)
# Idempotent: safe to re-run. Does NOT install Java/Kafka/Hadoop/Spark/Druid/Airflow:
# it finds your existing installs and links them to the standard paths used by the project:
#   /usr/lib/jvm/java-11 -> your JDK      /opt/kafka  -> your Kafka     /opt/hadoop -> your Hadoop
#   /opt/spark -> your Spark              /opt/druid  -> your Druid     /opt/clickpulse/jars/iceberg-spark-runtime.jar -> your Iceberg jar
# If something is detected wrongly, override, e.g.:  sudo KAFKA_DIR=/home/me/kafka_2.13-3.7.0 bash 00_common_setup.sh vm1
set -euo pipefail
ME="${1:?usage: 00_common_setup.sh vm1|vm2|vm3|vm4}"
CP=/opt/clickpulse
set -a; . "$CP/conf/cluster.env"; set +a
LOGIN_USER="${SUDO_USER:-root}"

echo ">> [1/7] hostname -> $ME"
hostnamectl set-hostname "$ME"

echo ">> [2/7] /etc/hosts (remove 127.0.1.1 self-mapping, add the 4 VMs)"
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

echo ">> [3/7] small OS tools only (no Java / big-data installs)"
apt-get update -y || true
DEBIAN_FRONTEND=noninteractive apt-get install -y python3-venv python3-pip curl wget rsync netcat-openbsd jq git chrony
timedatectl set-timezone UTC || true
systemctl enable --now chrony || true

echo ">> [4/7] service user + data dirs"
id clickpulse >/dev/null 2>&1 || useradd -m -s /bin/bash clickpulse
usermod -aG clickpulse "$LOGIN_USER" 2>/dev/null || true
usermod -aG "$(id -gn "$LOGIN_USER" 2>/dev/null || echo root)" clickpulse 2>/dev/null || true   # lets clickpulse enter your home dir if tools live there
mkdir -p /data/kafka /data/hadoop /opt/clickpulse/jars /opt/clickpulse/logs
chown -R clickpulse:clickpulse /data /opt/clickpulse
chmod -R g+rwX /data /opt/clickpulse

echo ">> [5/7] link EXISTING installs to the standard paths"
SEARCH="/opt /usr/local /home /root /srv"
find_home() {   # $1 = file relative to the tool home, e.g. bin/kafka-server-start.sh
  find $SEARCH -maxdepth 5 -path "*/$1" -not -path "*/site-packages/*" -not -path "/opt/clickpulse/*" 2>/dev/null \
    | head -1 | sed "s|/$1\$||"
}
link() {        # $1 = name, $2 = standard path, $3 = detected/overridden real dir
  local name=$1 std=$2 real=${3:-}
  if [ -e "$std" ] && [ ! -L "$std" ]; then echo "   $name: $std is already a real install - kept"; real=$std
  elif [ -n "$real" ]; then ln -sfn "$real" "$std"; echo "   $name: $std -> $real"
  else echo "   $name: not found on this VM (fine if this VM doesn't need it)"; return 0; fi
  chgrp -R clickpulse "$real" 2>/dev/null || true      # let the clickpulse services write logs/var
  chmod -R g+rwX "$real" 2>/dev/null || true
}
JBIN="$(sudo -iu "$LOGIN_USER" bash -lc 'command -v java' 2>/dev/null || command -v java || true)"
[ -z "$JBIN" ] && JBIN="$(find /usr/lib/jvm $SEARCH -maxdepth 6 -path '*/bin/java' -type f 2>/dev/null | head -1)"
JDIR="${JAVA_DIR:-$(dirname "$(dirname "$(readlink -f "$JBIN")")")}"
[ -x "$JDIR/bin/java" ] || { echo "Java not found - rerun with JAVA_DIR=/path/to/jdk"; exit 1; }
ln -sfn "$JDIR" /usr/lib/jvm/java-11; echo "   java : /usr/lib/jvm/java-11 -> $JDIR ($("$JDIR/bin/java" -version 2>&1 | head -1))"
link kafka  /opt/kafka  "${KAFKA_DIR:-$(find_home bin/kafka-server-start.sh)}"
link hadoop /opt/hadoop "${HADOOP_DIR:-$(find_home bin/hdfs)}"
link spark  /opt/spark  "${SPARK_DIR:-$(find_home bin/spark-submit)}"
link druid  /opt/druid  "${DRUID_DIR:-$(find_home bin/start-druid)}"
IJAR="${ICEBERG_JAR_FILE:-$(find $SEARCH -maxdepth 6 -name 'iceberg-spark-runtime-*.jar' 2>/dev/null | head -1)}"
if [ -n "$IJAR" ]; then ln -sfn "$IJAR" /opt/clickpulse/jars/iceberg-spark-runtime.jar; echo "   iceberg jar -> $IJAR"
else echo "   iceberg jar: not found (only VM3 needs it)"; fi

echo ">> [6/7] shared Python venv for project scripts (does not touch your Airflow)"
sudo -u clickpulse python3 -m venv "$CP_VENV"
sudo -u clickpulse "$CP_VENV/bin/pip" install -q --upgrade pip
sudo -u clickpulse "$CP_VENV/bin/pip" install -q confluent-kafka requests pandas
chmod -R g+rwX "$CP_VENV"

echo ">> [7/7] login environment for every shell"
cat > /etc/profile.d/clickpulse.sh <<'EOF'
set -a; . /opt/clickpulse/conf/cluster.env; set +a
export PATH="$CP_VENV/bin:$KAFKA_HOME/bin:$HADOOP_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
EOF
chmod 644 /etc/profile.d/clickpulse.sh
if command -v ufw >/dev/null && ufw status | grep -q active; then
  ufw allow from "$CLUSTER_SUBNET"; ufw allow 22/tcp
fi
echo
echo "Versions on $ME:"
[ -x /opt/kafka/bin/kafka-topics.sh ] && echo "   kafka : $(ls /opt/kafka/libs | grep -oE '^kafka_[0-9.]+-[0-9.]+' | head -1)"
[ -x /opt/hadoop/bin/hadoop ] && echo "   hadoop: $(JAVA_HOME=/usr/lib/jvm/java-11 /opt/hadoop/bin/hadoop version 2>/dev/null | head -1)"
[ -d /opt/spark/jars ] && echo "   spark : $(ls /opt/spark/jars | grep -oE '^spark-core_[0-9.]+-[0-9.]+' | head -1)"
[ -d /opt/druid/lib ] && echo "   druid : $(ls /opt/druid/lib | grep -oE '^druid-server-[0-9.]+' | head -1)"
echo "DONE on $ME. Close this terminal, open a new one, then: sudo -iu clickpulse"
