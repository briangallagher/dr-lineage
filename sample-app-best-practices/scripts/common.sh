#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$APP_ROOT"
NAMESPACE="${NAMESPACE:-ol-best-practices}"
STATE_DIR="$APP_ROOT/.state"
BUILD_DIR="$APP_ROOT/build"

if [[ "$NAMESPACE" != "ol-best-practices" ]]; then
  echo "This sample's manifests use the fixed namespace ol-best-practices" >&2
  exit 1
fi

require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Required command not found: $1" >&2
    exit 1
  }
}

cluster_name() {
  oc whoami --show-server | sed -E 's#^https?://##; s#:.*$##'
}

wait_http() {
  local url="$1"
  for _ in $(seq 1 60); do
    if curl --insecure --fail --silent --show-error "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "Timed out waiting for $url" >&2
  return 1
}

wait_tcp() {
  local host="$1"
  local port="$2"
  for _ in $(seq 1 60); do
    if (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then
      exec 3>&-
      exec 3<&-
      return 0
    fi
    sleep 1
  done
  echo "Timed out waiting for $host:$port" >&2
  return 1
}

wait_job_complete() {
  local job="$1"
  local namespace="$2"
  local attempts="${3:-60}"
  for _ in $(seq 1 "$attempts"); do
    if [[ "$(oc get job "$job" -n "$namespace" -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}' 2>/dev/null)" == "True" ]]; then
      return 0
    fi
    if [[ "$(oc get job "$job" -n "$namespace" -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null)" == "True" ]]; then
      echo "Job $job failed:" >&2
      oc logs job/"$job" -n "$namespace" --all-containers=true >&2 || true
      return 1
    fi
    sleep 5
  done
  echo "Timed out waiting for job $job" >&2
  return 1
}
