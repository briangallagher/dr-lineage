#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
cd "$APP_DIR"

: "${UV_CACHE_DIR:=/tmp/dr-lineage-uv-cache}"
export UV_CACHE_DIR

uv run pytest -q \
  tests/test_kfp_lineage.py \
  tests/test_kfp_integration_readiness.py
uv run ruff check \
  src/lineage_demo/kfp_lineage.py \
  src/lineage_demo/kfp_integration_readiness.py \
  tests/test_kfp_lineage.py \
  tests/test_kfp_integration_readiness.py

echo "Native KFP readiness fixture checks passed."
