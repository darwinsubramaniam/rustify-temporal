#!/usr/bin/env bash
# Build the Go Temporal server and write a single-process SQLite config for it, on its own ports
# (frontend 17233) so it can run next to anything already using 7233.
#
#   scripts/go-server.sh OUT_DIR [TEMPORAL_SRC]
# Writes OUT_DIR/temporal-server and OUT_DIR/go-server.yaml (database: OUT_DIR/go/temporal.db).
# Start it with: OUT_DIR/temporal-server --config-file OUT_DIR/go-server.yaml --allow-no-auth start
set -euo pipefail
OUT="$1"
TEMPORAL_SRC="${2:-$(cd "$(dirname "$0")/../../temporal" && pwd)}"
mkdir -p "$OUT/go"
(cd "$TEMPORAL_SRC" && go build -o "$OUT/temporal-server" ./cmd/server)
OUT="$OUT" TEMPORAL_SRC="$TEMPORAL_SRC" perl -pe '
    s#databaseName: "default"#databaseName: "$ENV{OUT}/go/temporal.db"#;
    s/synchronous: 2/synchronous: 1/;
    s/\b(7233|7234|7235|7239|7243|6933|6934|6935|6939|7936)\b/1$1/g;
    s/127\.0\.0\.1:8000/127.0.0.1:18000/;
    s#filepath: "config/dynamicconfig/development-sql.yaml"#filepath: "$ENV{TEMPORAL_SRC}/config/dynamicconfig/development-sql.yaml"#;
  ' "$TEMPORAL_SRC/config/development-sqlite-file.yaml" > "$OUT/go-server.yaml"
