#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR/.."

OUTPUT_DIR=${OUTPUT_DIR:-build/product-showcase}
export UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/dr-lineage-uv-cache}"

echo "Building the local RHOAI product showcase in ${OUTPUT_DIR}"
uv run --extra dev lineage-demo showcase --output-dir "$OUTPUT_DIR" "$@"
echo "Open ${OUTPUT_DIR}/audit-report.html in a browser."
