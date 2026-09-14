#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$APP_ROOT"

FEAST_URL="${FEAST_URL:-http://127.0.0.1:16580}"
MARQUEZ_URL="${MARQUEZ_URL:-http://127.0.0.1:5000}"
CONFORMANCE_SUITE_ID="${CONFORMANCE_SUITE_ID:-feast-vs-marquez}"
OUTPUT_DIR="${OUTPUT_DIR:-$APP_ROOT/build}"

mkdir -p "$OUTPUT_DIR"
UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/dr-lineage-uv-cache}" uv run python -m lineage_demo.conformance \
  --feast-url "$FEAST_URL" \
  --marquez-url "$MARQUEZ_URL" \
  --suite-id "$CONFORMANCE_SUITE_ID" \
  --json-output "$OUTPUT_DIR/feast-vs-marquez-conformance.json" \
  --markdown-output "$OUTPUT_DIR/feast-vs-marquez-conformance.md"

echo "JSON report:     $OUTPUT_DIR/feast-vs-marquez-conformance.json"
echo "Markdown report: $OUTPUT_DIR/feast-vs-marquez-conformance.md"
