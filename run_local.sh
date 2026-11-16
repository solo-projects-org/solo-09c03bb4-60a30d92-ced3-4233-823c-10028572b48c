#!/usr/bin/env bash
# Run the three services locally without Docker (for environments without
# a Docker daemon). Each gets its own process and SQLite database.
set -euo pipefail
cd "$(dirname "$0")"

PY=.venv/bin/python
mkdir -p data
export APP_DB_PATH="$PWD/data/webapp.db"
export SIGN_DB_PATH="$PWD/data/sign.db"
export GATEWAY_URL="http://127.0.0.1:8001/gtfs-rt"
export SIGN_URL="http://127.0.0.1:8002"
export POLL_INTERVAL=2
export PUSH_RETRY_INTERVAL=2
export HTTP_TIMEOUT=5
export BACKGROUND_WORKERS=1

echo "starting gateway :8001, sign :8002, webapp :8000 ..."
$PY -m uvicorn gateway.app:app --host 127.0.0.1 --port 8001 \
    --log-level warning &
GWPID=$!
SIGN_DB_PATH="$SIGN_DB_PATH" $PY -m uvicorn sign.app:app --host 127.0.0.1 --port 8002 \
    --log-level warning &
SNPID=$!
$PY -m uvicorn webapp.main:app --host 127.0.0.1 --port 8000 \
    --log-level warning &
WBPID=$!

trap 'kill $GWPID $SNPID $WBPID 2>/dev/null || true' EXIT
echo "PIDs: webapp=$WBPID gateway=$GWPID sign=$SNPID"
echo "open http://127.0.0.1:8000/  (sign board: http://127.0.0.1:8002/)"
wait
