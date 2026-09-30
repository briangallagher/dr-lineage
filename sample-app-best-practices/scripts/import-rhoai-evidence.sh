#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR/.."

RUN_ID=${RUN_ID:-ad6efc7b-4b95-464e-b398-31df6b48e061}
OUTPUT_DIR=${OUTPUT_DIR:-build/product-demo}
export UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/dr-lineage-uv-cache}"

oc port-forward -n scenario-b svc/ds-pipeline-dspa 8888:8888 >/tmp/lineage-kfp-port-forward.log 2>&1 &
KFP_PID=$!
oc port-forward -n ol-best-practices svc/marquez 5000:80 >/tmp/lineage-marquez-port-forward.log 2>&1 &
MARQUEZ_PID=$!
oc port-forward -n redhat-ods-applications svc/feast-data-registry-registry-rest 6572:80 >/tmp/lineage-registry-port-forward.log 2>&1 &
REGISTRY_PID=$!
oc port-forward -n redhat-ods-applications svc/mlflow 8443:8443 >/tmp/lineage-mlflow-port-forward.log 2>&1 &
MLFLOW_PID=$!
cleanup() {
  kill "$KFP_PID" "$MARQUEZ_PID" "$REGISTRY_PID" "$MLFLOW_PID" 2>/dev/null || true
}
trap cleanup EXIT

sleep 4
MLFLOW_TOKEN=$(oc whoami -t)
export RHOAI_MLFLOW_TOKEN="$MLFLOW_TOKEN"
export RHOAI_DATA_REGISTRY_TOKEN="$MLFLOW_TOKEN"
export RHOAI_LINEAGE_DEPLOYMENT="${RHOAI_LINEAGE_DEPLOYMENT:-$(oc whoami --show-server | sed -E 's#^https?://##; s#:.*$##')}"
uv run --extra dev lineage-demo import-rhoai-evidence \
  --kfp-url https://127.0.0.1:8888 \
  --marquez-url http://127.0.0.1:5000 \
  --registry-url http://127.0.0.1:6572 \
  --mlflow-url https://127.0.0.1:8443/mlflow \
  --mlflow-workspace scenario-b \
  --run-id "$RUN_ID" \
  --output "$OUTPUT_DIR/live-rhoai-evidence.json"

uv run --extra dev lineage-demo product-demo \
  --output-dir "$OUTPUT_DIR" \
  --verified-evidence "$OUTPUT_DIR/live-rhoai-evidence.json"

echo "Open ${OUTPUT_DIR}/cockpit.html in a browser."
