#!/usr/bin/env bash
# !!! Wipes Kafka topic, HDFS /clickpulse, Iceberg tables and the Druid datasource. !!!
# Allowed ONLY while developing, BEFORE the final Day 1 run. After the final Day 1 run
# the submission rule forbids any reset. Run on VM2 (has ssh to all VMs).
set -euo pipefail
set -a; . /opt/clickpulse/conf/cluster.env; set +a
read -r -p "Type RESET to wipe ALL ClickPulse data (dev only): " x; [ "$x" = "RESET" ] || exit 1

ssh clickpulse@vm1 "bash -lc 'kafka-topics.sh --bootstrap-server \$KAFKA_BOOTSTRAP --delete --topic \$KAFKA_TOPIC || true; sleep 5; \
  kafka-topics.sh --bootstrap-server \$KAFKA_BOOTSTRAP --create --topic \$KAFKA_TOPIC --partitions 3 --replication-factor 2 --config retention.ms=-1'"
ssh clickpulse@vm1 "bash -lc 'kafka-consumer-groups.sh --bootstrap-server \$KAFKA_BOOTSTRAP --delete --group clickpulse-raw-writer'" || true
hdfs dfs -rm -r -f -skipTrash "$HDFS_BASE"      # raw, markers, warehouse (Iceberg tables), rejects, export
hdfs dfs -mkdir -p "$HDFS_BASE"/{raw,markers,warehouse,rejects,export}
curl -s -X DELETE "$DRUID_ROUTER/druid/coordinator/v1/datasources/$DRUID_DATASOURCE" || true   # mark unused
curl -s -X DELETE "$DRUID_ROUTER/druid/coordinator/v1/datasources/$DRUID_DATASOURCE/intervals/1000-01-01_3000-01-01" || true # kill
echo "Reset done. Next run must be the FINAL Day 1."
