#!/usr/bin/env bash
# VM3 - wrapper Airflow calls over SSH:  run_spark_job.sh <day> <run_id>
set -euo pipefail
set -a; . /opt/clickpulse/conf/cluster.env; set +a
DAY="${1:?day}"; RUN_ID="${2:-manual_$(date -u +%Y%m%dT%H%M%S)}"
exec "$SPARK_HOME/bin/spark-submit" \
  --master "$SPARK_MASTER_URL" --deploy-mode client \
  --name "clickpulse-day$DAY" \
  --conf spark.executor.memory=1g --conf spark.driver.memory=1g \
  /opt/clickpulse/spark/clickpulse_job.py --day "$DAY" --run-id "$RUN_ID"
