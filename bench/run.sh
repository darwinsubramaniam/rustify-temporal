#!/usr/bin/env bash
# Compare rustify-temporal with the Go Temporal server on the features both support.
#
# Both servers store data in SQLite (WAL, synchronous=NORMAL) in a fresh temporary folder, and the
# same official Go SDK client + worker runs the same workloads against each. Writes RESULTS.md.
#
#   bench/run.sh [N] [CONCURRENCY]      (defaults: 1000 workflows, 100 at a time)
#   TEMPORAL_SRC=/path/to/temporal      Go server source (default: ../temporal next to this repo)
set -euo pipefail
export PATH="${CARGO_HOME:-$HOME/.cargo}/bin:$PATH"
cd "$(dirname "$0")"
BENCH_DIR="$PWD"
ROOT="$(cd .. && pwd)"
TEMPORAL_SRC="${TEMPORAL_SRC:-$(cd "$ROOT/../temporal" && pwd)}"
N="${1:-1000}"
C="${2:-100}"
OUT="$(mktemp -d /tmp/rustify-bench.XXXXXX)"
echo "work folder: $OUT"

echo "== building"
(cd "$ROOT" && cargo build --release -q)
"$ROOT/scripts/go-server.sh" "$OUT" "$TEMPORAL_SRC"
go build -o "$OUT/bench" .
RUST_BIN="$ROOT/target/release/rustify-temporal"


wait_port() { # port timeout_s
  local end=$(( $(date +%s) + $2 ))
  while ! nc -z 127.0.0.1 "$1" 2>/dev/null; do
    [ "$(date +%s)" -lt "$end" ] || return 1
    sleep 0.02
  done
}
rss_kb() { ps -o rss= -p "$1" 2>/dev/null | tr -d ' ' || echo 0; }

bench_one() { # name port cmd...
  local name="$1" port="$2"; shift 2
  local dir="$OUT/$name"; mkdir -p "$dir"
  echo "== $name"
  ( cd "$dir" && exec /usr/bin/time -l "$@" ) >"$dir/server.log" 2>"$dir/time.txt" &
  local tpid=$!
  wait_port "$port" 120 || { echo "$name did not start"; cat "$dir/server.log" | tail -20; exit 1; }
  local spid; spid=$(pgrep -P "$tpid" | head -1)
  sleep 3
  local idle; idle=$(rss_kb "$spid")
  # Sample memory while the workload runs.
  ( peak=0; while kill -0 "$spid" 2>/dev/null; do r=$(rss_kb "$spid"); [ "${r:-0}" -gt "$peak" ] && peak=$r && echo "$peak" > "$dir/peak_rss_kb"; sleep 0.2; done ) &
  local sampler=$!
  local c0; c0=$(ps -o time= -p "$spid" | tr -d ' ')
  "$OUT/bench" -addr "127.0.0.1:$port" -n "$N" -c "$C" > "$dir/results.json" 2> "$dir/bench.log"
  local c1; c1=$(ps -o time= -p "$spid" | tr -d ' ')
  kill -INT "$spid"; wait "$tpid" 2>/dev/null || true
  { kill "$sampler"; wait "$sampler"; } 2>/dev/null || true
  # Startup time, measured separately and precisely (median of 3 starts on the same database).
  local startup; startup=$(cd "$dir" && python3 "$BENCH_DIR/startup.py" "$port" "$dir" "$@")
  local db=0
  for f in "$dir"/*.db* "$dir"/go/*.db* "$dir"/default*; do [ -f "$f" ] && db=$(( db + $(stat -f %z "$f") )); done
  cat > "$dir/meta.json" <<EOF
{"startup_ms": $startup, "idle_rss_kb": ${idle:-0}, "peak_rss_kb": $(cat "$dir/peak_rss_kb" 2>/dev/null || echo 0),
 "cpu_before": "$c0", "cpu_after": "$c1", "db_bytes": $db, "binary_bytes": $(stat -f %z "$1")}
EOF
}

mkdir -p "$OUT/go"
bench_one rustify 17333 "$RUST_BIN" --port 17333 --db "$OUT/rustify/temporal.db"
bench_one go 17233 "$OUT/temporal-server" --config-file "$OUT/go-server.yaml" --allow-no-auth start

python3 "$BENCH_DIR/report.py" "$OUT" "$N" "$C" > "$BENCH_DIR/RESULTS.md"
echo "== wrote bench/RESULTS.md"
cat "$BENCH_DIR/RESULTS.md"
