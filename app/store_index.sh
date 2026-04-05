#!/usr/bin/env bash
set -euo pipefail

cd /app
source .venv/bin/activate

python3 /app/app.py wait-cassandra --host cassandra-server --timeout 180
python3 /app/app.py load-index --host cassandra-server --keyspace search_engine --hdfs-root /indexer
