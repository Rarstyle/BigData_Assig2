#!/usr/bin/env bash
set -euo pipefail

HADOOP_HOME="${HADOOP_HOME:-/usr/local/hadoop}"
SPARK_HOME="${SPARK_HOME:-/usr/local/spark}"
HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-${HADOOP_HOME}/etc/hadoop}"

if [[ -f "${HADOOP_CONF_DIR}/workers" ]]; then
  printf 'cluster-slave-1\n' > "${HADOOP_CONF_DIR}/workers"
fi

if [[ -f "${HADOOP_CONF_DIR}/slaves" ]]; then
  printf 'cluster-slave-1\n' > "${HADOOP_CONF_DIR}/slaves"
fi

echo "[start-services.sh] Starting HDFS daemons."
"${HADOOP_HOME}/sbin/start-dfs.sh"

echo "[start-services.sh] Starting YARN daemons."
"${HADOOP_HOME}/sbin/start-yarn.sh"

echo "[start-services.sh] Starting MapReduce history server."
mapred --daemon start historyserver

echo "[start-services.sh] Waiting for HDFS to become available."
for _ in $(seq 1 30); do
  if hdfs dfs -ls / >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "[start-services.sh] Waiting for at least one YARN NodeManager."
for _ in $(seq 1 45); do
  if yarn node -list 2>/dev/null | grep -q "RUNNING"; then
    break
  fi
  sleep 2
done

hdfs dfsadmin -safemode leave || true
hdfs dfs -mkdir -p /apps/spark /user/root /tmp /input
hdfs dfs -chmod -R 755 /apps /user/root /tmp /input || true

echo "[start-services.sh] Spark executors will use the Spark jars already installed on each cluster node."

echo "[start-services.sh] Service processes:"
jps -lm
echo "[start-services.sh] YARN nodes:"
yarn node -list || true
echo "[start-services.sh] HDFS report:"
hdfs dfsadmin -report
