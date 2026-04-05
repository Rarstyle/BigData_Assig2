#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/app"
exec bash ./create_index.sh "$@"
