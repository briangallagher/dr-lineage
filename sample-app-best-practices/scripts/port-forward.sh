#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

echo "Marquez UI:  http://127.0.0.1:3000"
echo "Marquez API: http://127.0.0.1:5000"
echo "Registry:    http://127.0.0.1:8081"
echo "MinIO:       http://127.0.0.1:9001"
echo "KFP API:     https://127.0.0.1:8888"

trap 'jobs -p | xargs kill 2>/dev/null || true' EXIT INT TERM
oc port-forward -n "$NAMESPACE" service/marquez-web 3000:80 &
oc port-forward -n "$NAMESPACE" service/marquez 5000:80 &
oc port-forward -n "$NAMESPACE" service/registry 8081:8080 &
oc port-forward -n "$NAMESPACE" service/minio 9000:9000 9001:9001 &
oc port-forward -n "$NAMESPACE" service/ds-pipeline-dspa 8888:8888 &
wait
