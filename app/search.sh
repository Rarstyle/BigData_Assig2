#!/usr/bin/env bash
set -euo pipefail

cd /app
source .venv/bin/activate

QUERY_TEXT="${*:-}"
if [[ -z "${QUERY_TEXT}" ]]; then
  QUERY_TEXT="$(cat)"
fi

if [[ -z "${QUERY_TEXT}" ]]; then
  echo "[search.sh] Query text is required." >&2
  exit 1
fi

export PYSPARK_DRIVER_PYTHON=/app/.venv/bin/python
SPARK_MASTER="${SEARCH_SPARK_MASTER:-yarn}"
SPARK_YARN_JARS="${SPARK_YARN_JARS:-local:/usr/local/spark/jars/*}"

spark_submit_args=(
  spark-submit
  --master "${SPARK_MASTER}"
  --py-files /app/common.py
  --conf spark.driver.memory=768m
  --conf spark.sql.shuffle.partitions=4
  --conf spark.default.parallelism=4
)

if [[ "${SPARK_MASTER}" == "yarn" ]]; then
  export PYSPARK_PYTHON=python3
  spark_submit_args+=(
    --deploy-mode client
    --conf spark.yarn.jars="${SPARK_YARN_JARS}"
    --conf spark.yarn.am.memory=768m
    --conf spark.executor.memory=768m
    --conf spark.executor.cores=1
    --conf spark.executor.instances=1
    --conf spark.dynamicAllocation.enabled=false
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=python3
    --conf spark.executorEnv.PYSPARK_PYTHON=python3
  )
else
  export PYSPARK_PYTHON=/app/.venv/bin/python
  spark_submit_args+=(
    --conf spark.executor.memory=768m
  )
fi

"${spark_submit_args[@]}" /app/query.py "${QUERY_TEXT}"
