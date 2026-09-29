#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
test -f "$STATE_DIR/images.env" || {
  echo "Image state missing; run deploy.sh first" >&2
  exit 1
}
source "$STATE_DIR/images.env"
oc delete job object-store-seed -n "$NAMESPACE" --ignore-not-found >/dev/null
oc set image -f "$APP_ROOT/openshift/source-seed.yaml" \
  "seed=$APP_IMAGE" --local -o yaml | oc apply -n "$NAMESPACE" -f - >/dev/null
wait_job_complete object-store-seed "$NAMESPACE"
echo "Reset s3://sample-data/raw/documents.csv to source-v1.csv"
