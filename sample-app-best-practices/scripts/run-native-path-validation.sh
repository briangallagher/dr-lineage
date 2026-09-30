#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR/.."

OUTPUT_DIR=${OUTPUT_DIR:-build/native-validation}
export UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/dr-lineage-uv-cache}"

echo "Running the local-only native KFP/RHOAI path checkpoint in ${OUTPUT_DIR}"
uv run --extra dev lineage-demo native-validation --output-dir "$OUTPUT_DIR"
echo "Inspect ${OUTPUT_DIR}/native-path-validation.json for the four checks and recommendation."
