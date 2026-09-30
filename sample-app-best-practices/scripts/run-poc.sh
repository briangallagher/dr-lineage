#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR/.."

OUTPUT_DIR=${OUTPUT_DIR:-build/poc}
export UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/dr-lineage-uv-cache}"

echo "Building the deterministic RHOAI lineage PM/architecture POC in ${OUTPUT_DIR}"
uv run --extra dev lineage-demo poc --output-dir "$OUTPUT_DIR" "$@"
echo "Open ${OUTPUT_DIR}/poc-brief.html for the talk track and decision boundary."
echo "Open ${OUTPUT_DIR}/cockpit.html for the interactive success and recovery demo."
echo "Validation: ${OUTPUT_DIR}/native-path-validation.json"
