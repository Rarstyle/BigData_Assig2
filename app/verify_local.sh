#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

echo "[verify_local.sh] Checking required assignment files."
required_files=(
  "docker-compose.yml"
  "app/app.sh"
  "app/start-services.sh"
  "app/prepare_data.py"
  "app/prepare_data.sh"
  "app/create_index.sh"
  "app/store_index.sh"
  "app/index.sh"
  "app/query.py"
  "app/search.sh"
  "app/mapreduce/mapper1.py"
  "app/mapreduce/reducer1.py"
  "app/requirements.txt"
  "report.pdf"
)

for path in "${required_files[@]}"; do
  if [[ ! -f "${path}" ]]; then
    echo "[verify_local.sh] Missing required file: ${path}" >&2
    exit 1
  fi
done

echo "[verify_local.sh] Running shell syntax checks."
bash -n app/app.sh app/start-services.sh app/prepare_data.sh app/create_index.sh app/store_index.sh app/index.sh app/search.sh app/verify_local.sh

echo "[verify_local.sh] Running Python syntax checks."
python3 -m py_compile \
  app/common.py \
  app/prepare_data.py \
  app/app.py \
  app/query.py \
  app/local_validation.py \
  app/mapreduce/mapper1.py \
  app/mapreduce/reducer1.py \
  app/mapreduce/mapper2.py \
  app/mapreduce/reducer2.py

echo "[verify_local.sh] Running offline BM25 and MapReduce validation."
python3 app/local_validation.py --output-dir app/artifacts

echo "[verify_local.sh] Verification finished successfully."
echo "[verify_local.sh] See app/artifacts/ for generated proof artifacts."
