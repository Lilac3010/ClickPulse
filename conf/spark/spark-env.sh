#!/usr/bin/env bash
# VM3 - copied to $SPARK_HOME/conf/spark-env.sh by install_configs.sh
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_MASTER_HOST=vm3
export SPARK_LOCAL_HOSTNAME=vm3
export SPARK_WORKER_CORES=2
export SPARK_WORKER_MEMORY=2g
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
