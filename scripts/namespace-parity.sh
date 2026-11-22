#!/usr/bin/env bash
# Run the namespace parity steps against the Go Temporal server and rustify-temporal and compare
# every outcome. Exits non-zero if any step differs.
#
#   scripts/namespace-parity.sh            (TEMPORAL_SRC=/path/to/temporal to pick the Go source)
set -euo pipefail
export PATH="${CARGO_HOME:-$HOME/.cargo}/bin:$PATH"
cd "$(dirname "$0")/.."
ROOT="$PWD"
OUT="$(mktemp -d /tmp/rustify-nsparity.XXXXXX)"

cargo build -q
"$ROOT/scripts/go-server.sh" "$OUT" "${TEMPORAL_SRC:-}"

"$OUT/temporal-server" --config-file "$OUT/go-server.yaml" --allow-no-auth start >"$OUT/go.log" 2>&1 &
GO_PID=$!
"$ROOT/target/debug/rustify-temporal" --port 17333 --db "$OUT/rustify.db" >"$OUT/rustify.log" 2>&1 &
RS_PID=$!
trap 'kill $GO_PID $RS_PID 2>/dev/null || true' EXIT
for p in 17233 17333; do
  for _ in $(seq 1 600); do nc -z 127.0.0.1 $p 2>/dev/null && break; sleep 0.1; done
done
sleep 2

uv run --project e2e python e2e/namespace_parity.py 127.0.0.1:17233 >"$OUT/go.json" &
A=$!
uv run --project e2e python e2e/namespace_parity.py 127.0.0.1:17333 >"$OUT/rustify.json"
wait $A

python3 - "$OUT/go.json" "$OUT/rustify.json" <<'EOF'
import json, sys
go, rs = (json.load(open(p)) for p in sys.argv[1:3])
same = 0
print(f"{'step':<46} {'Go server':<28} {'rustify':<28}")
for k in go:
    g, r = json.dumps(go[k]), json.dumps(rs.get(k))
    mark = "same" if g == r else "DIFFERENT"
    same += g == r
    print(f"{k:<46} {g[:27]:<28} {r[:27]:<28} {mark}")
print(f"\n{same}/{len(go)} steps behave the same")
sys.exit(0 if same == len(go) else 1)
EOF
