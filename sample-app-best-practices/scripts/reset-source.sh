#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
oc delete job object-store-seed -n "$NAMESPACE" --ignore-not-found >/dev/null
oc apply -f "$APP_ROOT/openshift/source-seed.yaml" -n "$NAMESPACE" >/dev/null
wait_job_complete object-store-seed "$NAMESPACE"
echo "Reset s3://sample-data/raw/documents.csv to source-v1.csv"
