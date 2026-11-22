#!/usr/bin/env bash
# Build the server and run the official-SDK end-to-end tests (Python, then Go).
set -euo pipefail
cd "$(dirname "$0")/.."

cargo build
BIN="$PWD/target/debug/rustify-temporal"

echo "== Python SDK"
uv run --project e2e python e2e/run.py "$BIN"

echo "== Go SDK"
PORT=$(python3 -c "import socket;s=socket.socket();s.bind(('127.0.0.1',0));print(s.getsockname()[1])")
TMP=$(mktemp -d)
"$BIN" --port "$PORT" --db "$TMP/temporal.db" >"$TMP/server.log" 2>&1 &
PID=$!
trap 'kill $PID 2>/dev/null || true' EXIT
sleep 1
(cd e2e-go && go run . "127.0.0.1:$PORT" 2>&1 | grep -E "^(PASS|FAIL|all)")
