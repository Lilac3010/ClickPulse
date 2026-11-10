"""ClickPulse pipeline - Airflow on VM2.

Trigger with config {"day": 1} or {"day": 2}  (UI: Trigger DAG w/ config, or
CLI: airflow dags trigger clickpulse_pipeline -c '{"day": 1}').
Every task is idempotent, so re-triggering the same day changes nothing.
"""
from datetime import datetime, timedelta

try:                                   # Airflow 3.x
    from airflow.sdk import DAG, Param
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:                    # Airflow 2.x
    from airflow import DAG
    from airflow.models.param import Param
    from airflow.operators.bash import BashOperator

SSH = "ssh -o BatchMode=yes -o ConnectTimeout=10 clickpulse@{host} 'bash -lc \"{cmd}\"'"
DAY = "{{ params.day }}"
RUN = "{{ run_id | replace(':', '-') | replace('+', '_') }}"

with DAG(
    dag_id="clickpulse_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,                       # never two upserts at once
    params={"day": Param(1, type="integer", enum=[1, 2])},
    default_args={"owner": "clickpulse", "retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["clickpulse"],
) as dag:

    check_cluster = BashOperator(
        task_id="check_cluster",
        bash_command="bash /opt/clickpulse/scripts/check_cluster.sh ",   # trailing space: stops Airflow treating .sh as a template file
    )

    produce = BashOperator(            # VM1: file -> Kafka (skips if already produced)
        task_id="produce_to_kafka",
        bash_command=SSH.format(host="vm1", cmd=f"python /opt/clickpulse/app/producer.py --day {DAY}"),
    )

    consume = BashOperator(            # VM2: Kafka -> HDFS raw (overwrite, exact lines)
        task_id="kafka_to_hdfs_raw",
        bash_command=f"bash -lc 'python /opt/clickpulse/app/consumer.py --day {DAY}'",
    )

    verify_raw = BashOperator(         # proof: HDFS raw file == original file, byte for byte
        task_id="verify_raw_unchanged",
        bash_command=(
            f"set -e; A=$(bash -lc 'hdfs dfs -cat $HDFS_BASE/raw/day={DAY}/clicks_day{DAY}.jsonl' | md5sum | cut -c1-32); "
            f"B=$(" + SSH.format(host="vm1", cmd=f"md5sum \\$DATA_DIR/clicks_day{DAY}.jsonl") + " | cut -c1-32); "
            "echo raw_hdfs=$A source=$B; [ \"$A\" = \"$B\" ]"
        ),
    )

    spark_upsert = BashOperator(       # VM3: validate, reject, dedupe, MERGE, export parquet
        task_id="spark_validate_upsert_export",
        bash_command=SSH.format(host="vm3", cmd=f"/opt/clickpulse/scripts/run_spark_job.sh {DAY} {RUN}"),
    )

    druid_sync = BashOperator(         # VM4: re-index full snapshot (dropExisting) + verify mirror
        task_id="druid_reindex_and_verify",
        bash_command="bash -lc 'python /opt/clickpulse/scripts/druid_ingest.py'",
        execution_timeout=timedelta(minutes=15),
    )

    check_cluster >> produce >> consume >> verify_raw >> spark_upsert >> druid_sync
