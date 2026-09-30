#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR/.."

OUTPUT_DIR=${OUTPUT_DIR:-build/product-demo}
export UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/dr-lineage-uv-cache}"
uv run --extra dev lineage-demo product-demo --output-dir "$OUTPUT_DIR" "$@"
echo "Open ${OUTPUT_DIR}/cockpit.html in a browser."
echo "Decision brief: ${OUTPUT_DIR}/poc-brief.html"
echo "Optional API: uv run --extra dev lineage-demo demo-serve --data-dir ${OUTPUT_DIR}"
