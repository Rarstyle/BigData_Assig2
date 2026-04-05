#!/usr/bin/env bash
set -euo pipefail

cd /app
source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=/app/.venv/bin/python
export PYSPARK_PYTHON=/app/.venv/bin/python

PARQUET_LOCAL_PATH=""
if [[ -f /app/a.parquet ]]; then
  PARQUET_LOCAL_PATH=/app/a.parquet
else
  first_parquet="$(find /app -maxdepth 1 -type f -name '*.parquet' | head -n 1 || true)"
  if [[ -n "${first_parquet}" ]]; then
    PARQUET_LOCAL_PATH="${first_parquet}"
  fi
fi

PREPARE_ARGS=()
if [[ -n "${PARQUET_LOCAL_PATH}" ]]; then
  echo "[prepare_data.sh] Uploading parquet file ${PARQUET_LOCAL_PATH} to HDFS."
  hdfs dfs -put -f "${PARQUET_LOCAL_PATH}" /a.parquet
  PREPARE_ARGS+=(--parquet-path "hdfs://cluster-master:9000/a.parquet")
else
  echo "[prepare_data.sh] No parquet file found. Falling back to bundled sample docs or synthetic smoke docs."
fi

/app/.venv/bin/python /app/prepare_data.py \
  --doc-target 1000 \
  "${PREPARE_ARGS[@]}"

echo "[prepare_data.sh] HDFS /data contents:"
hdfs dfs -ls /data | sed 's/^/[prepare_data.sh] /'
echo "[prepare_data.sh] HDFS /input/data contents:"
hdfs dfs -ls /input/data | sed 's/^/[prepare_data.sh] /'
