#!/usr/bin/env bash
set -euo pipefail

cd /app
ARTIFACTS_DIR=/app/artifacts
LOG_DIR="${ARTIFACTS_DIR}/logs"
mkdir -p "${LOG_DIR}"

log() {
  echo "[app.sh] $*"
}

run_and_log() {
  local step_name="$1"
  shift
  log "Starting ${step_name}"
  "$@" 2>&1 | tee "${LOG_DIR}/${step_name}.log"
}

cleanup_on_error() {
  local exit_code=$?
  if [[ ${exit_code} -ne 0 ]]; then
    log "Workflow failed with exit code ${exit_code}."
  fi
  exit "${exit_code}"
}
trap cleanup_on_error EXIT

log "Restarting SSH service."
service ssh restart

run_and_log start_services bash /app/start-services.sh

if ! yarn node -list 2>/dev/null | grep -q "RUNNING"; then
  log "No RUNNING YARN NodeManager was detected. Check cluster-slave-1 and Docker memory allocation."
  exit 1
fi

log "Creating Python virtual environment."
python3 -m venv /app/.venv
source /app/.venv/bin/activate
python -m pip install --upgrade pip
run_and_log pip_install pip install -r /app/requirements.txt

SEARCH_SPARK_MASTER="${SEARCH_SPARK_MASTER:-yarn}"
log "Configured search master: ${SEARCH_SPARK_MASTER}"
log "Using the local virtual environment for the driver and container Python for Spark executors."

run_and_log prepare_data bash /app/prepare_data.sh
run_and_log index bash /app/index.sh
run_and_log search_dogs bash /app/search.sh "dogs"
run_and_log search_history bash /app/search.sh "history of science"
run_and_log search_christmas bash /app/search.sh "christmas carol"

log "Workflow finished successfully. Keeping the master container alive for inspection."
trap - EXIT
tail -f /dev/null
