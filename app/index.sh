#!/usr/bin/env bash
set -euo pipefail

cd /app
INPUT_PATH="${1:-/input/data}"

echo "[index.sh] Building index from ${INPUT_PATH}"
bash /app/create_index.sh "${INPUT_PATH}"
bash /app/store_index.sh
echo "[index.sh] Index creation and Cassandra loading completed."
