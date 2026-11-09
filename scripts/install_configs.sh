#!/usr/bin/env bash
# Copies the repo's configs into the installed tools ON THIS VM.
# Run after EVERY "git pull":   sudo bash /opt/clickpulse/scripts/install_configs.sh
# Only touches tools that are installed on this VM, so the same command works on all 4.
set -euo pipefail
CP=/opt/clickpulse
set -a; . "$CP/conf/cluster.env"; set +a

if [ -d "$HADOOP_HOME" ]; then
  cp "$CP"/conf/hadoop/{core-site.xml,hdfs-site.xml} "$HADOOP_CONF_DIR"/
  grep -q "^export JAVA_HOME=$JAVA_HOME" "$HADOOP_CONF_DIR/hadoop-env.sh" || \
    echo "export JAVA_HOME=$JAVA_HOME" >> "$HADOOP_CONF_DIR/hadoop-env.sh"
  echo "hadoop conf  -> $HADOOP_CONF_DIR"
fi
if [ -d "$SPARK_HOME" ]; then
  cp "$CP"/conf/spark/spark-defaults.conf "$CP"/conf/spark/spark-env.sh "$SPARK_HOME"/conf/
  echo "spark conf   -> $SPARK_HOME/conf"
fi
if [ -d "$DRUID_HOME" ]; then
  for d in "$DRUID_HOME"/conf/druid/auto/_common "$DRUID_HOME"/conf/druid/single-server/*/_common; do
    [ -d "$d" ] && cp "$CP"/conf/hadoop/{core-site.xml,hdfs-site.xml} "$d"/ && echo "hadoop xml   -> $d"
  done
fi
if [ -d "$KAFKA_HOME" ] && [ "$(hostname)" = "vm1" ]; then
  cp "$CP"/conf/systemd/kafka-broker{1,2}.service /etc/systemd/system/ && systemctl daemon-reload
  echo "kafka units  -> /etc/systemd/system"
fi
if [ -d "$HADOOP_HOME" ] && [ "$(hostname)" = "vm2" ]; then
  cp "$CP"/conf/systemd/hdfs-{namenode,datanode}.service /etc/systemd/system/ && systemctl daemon-reload
  echo "hdfs units   -> /etc/systemd/system"
fi
chown -R clickpulse:clickpulse "$CP" /data 2>/dev/null || true
echo "Config fingerprint on $(hostname): $(cd $CP && git rev-parse --short HEAD 2>/dev/null || echo no-git) / $(cat $CP/conf/cluster.env $CP/conf/hadoop/*.xml | md5sum | cut -c1-12)"
