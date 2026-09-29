#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
require_command oc
require_command uv

test -f "$BUILD_DIR/pipeline.yaml" || {
  echo "Pipeline package missing; run deploy.sh first" >&2
  exit 1
}

mkdir -p "$STATE_DIR"
declare -a pids=()
cleanup() {
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT INT TERM

oc port-forward -n "$NAMESPACE" service/ds-pipeline-dspa 8888:8888 >"$STATE_DIR/kfp-pf.log" 2>&1 & pids+=("$!")
oc port-forward -n "$NAMESPACE" service/registry 8081:8080 >"$STATE_DIR/registry-pf.log" 2>&1 & pids+=("$!")
oc port-forward -n "$NAMESPACE" service/marquez 5000:80 >"$STATE_DIR/marquez-pf.log" 2>&1 & pids+=("$!")
oc port-forward -n "$NAMESPACE" service/minio 9000:9000 >"$STATE_DIR/minio-pf.log" 2>&1 & pids+=("$!")

wait_tcp 127.0.0.1 8888
wait_http http://127.0.0.1:8081/healthz
wait_http http://127.0.0.1:5000/api/v1/namespaces
wait_http http://127.0.0.1:9000/minio/health/ready

access_key="$(oc get secret object-store-credentials -n "$NAMESPACE" -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' | base64 -d)"
secret_key="$(oc get secret object-store-credentials -n "$NAMESPACE" -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}' | base64 -d)"

{
  printf '%s\n' "$(oc whoami -t)"
  printf '%s\n' "$access_key"
  printf '%s\n' "$secret_key"
} | UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/dr-lineage-uv-cache}" \
  uv run python -m lineage_demo.scenarios \
    --credentials-stdin \
    --kfp-endpoint https://127.0.0.1:8888 \
    --registry-url http://127.0.0.1:8081 \
    --marquez-url http://127.0.0.1:5000 \
    --s3-endpoint http://127.0.0.1:9000 \
    --pipeline "$BUILD_DIR/pipeline.yaml" \
    --source-v2 "$APP_ROOT/data/source-v2.csv" \
    --namespace "$NAMESPACE" \
    --output "$BUILD_DIR/scenario-results.json"

"$SCRIPT_DIR/verify.sh"
