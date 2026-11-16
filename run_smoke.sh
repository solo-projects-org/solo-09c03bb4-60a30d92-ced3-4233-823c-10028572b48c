#!/usr/bin/env bash
# Three real uvicorn processes with background workers DISABLED so the
# smoke script can deterministically drive every poll/push step.
set -euo pipefail
cd "$(dirname "$0")"

PY=.venv/bin/python
mkdir -p data
rm -f data/smoke-web.db* data/smoke-sign.db*

export GATEWAY_URL="http://127.0.0.1:8001/gtfs-rt"
export SIGN_URL="http://127.0.0.1:8002"
export HTTP_TIMEOUT=5
export BACKGROUND_WORKERS=0

$PY -m uvicorn gateway.app:app --host 127.0.0.1 --port 8001 --log-level warning &
GWPID=$!
SIGN_DB_PATH="$PWD/data/smoke-sign.db" \
  $PY -m uvicorn sign.app:app --host 127.0.0.1 --port 8002 --log-level warning &
SNPID=$!
APP_DB_PATH="$PWD/data/smoke-web.db" \
  $PY -m uvicorn webapp.main:app --host 127.0.0.1 --port 8000 --log-level warning &
WBPID=$!

trap 'kill $GWPID $SNPID $WBPID 2>/dev/null || true' EXIT
echo "services: webapp=$WBPID gateway=$GWPID sign=$SNPID"
wait
