#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
test -f "$BUILD_DIR/scenario-results.json" || {
  echo "Scenario report missing; run run-scenarios.sh first" >&2
  exit 1
}

if ! curl --fail --silent http://127.0.0.1:5000/api/v1/namespaces >/dev/null 2>&1; then
  oc port-forward -n "$NAMESPACE" service/marquez 5000:80 >"$STATE_DIR/verify-pf.log" 2>&1 &
  port_forward_pid=$!
  trap 'kill "$port_forward_pid" 2>/dev/null || true' EXIT
  wait_http http://127.0.0.1:5000/api/v1/namespaces
fi

UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/dr-lineage-uv-cache}" uv run python -m lineage_demo.verify \
  --marquez-url http://127.0.0.1:5000 \
  --scenario-report "$BUILD_DIR/scenario-results.json" \
  --cluster "$(cluster_name)" \
  --namespace "$NAMESPACE"
