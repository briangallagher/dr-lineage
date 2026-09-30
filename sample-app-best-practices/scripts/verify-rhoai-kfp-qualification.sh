#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
APP_DIR=$(cd "$SCRIPT_DIR/.." && pwd)
cd "$APP_DIR"

export UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/dr-lineage-uv-cache}"
FIXTURE=${RHOAI_QUALIFICATION_FIXTURE:-examples/rhoai-kfp-qualification/rhoai-kfp-qualification-baseline-2026-09-30.json}

uv run pytest -q \
  tests/test_kfp_lineage.py \
  tests/test_kfp_integration_readiness.py \
  tests/test_rhoai_kfp_qualification.py
uv run ruff check \
  src/lineage_demo/kfp_lineage.py \
  src/lineage_demo/kfp_integration_readiness.py \
  src/lineage_demo/rhoai_kfp_qualification.py \
  tests/test_kfp_lineage.py \
  tests/test_kfp_integration_readiness.py \
  tests/test_rhoai_kfp_qualification.py
uv run python -m lineage_demo.rhoai_kfp_qualification "$FIXTURE" --expect NO_GO >/dev/null

echo "RHOAI/KFP packaged qualification checks passed; current fixture remains NO_GO."
