#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/app"
exec bash ./store_index.sh "$@"
