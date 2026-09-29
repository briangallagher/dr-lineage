#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

"$SCRIPT_DIR/preflight.sh"
mkdir -p "$STATE_DIR" "$BUILD_DIR"

oc apply -f "$APP_ROOT/openshift/namespace.yaml"

if ! oc get configmap registry-data -n "$NAMESPACE" >/dev/null 2>&1; then
  oc create configmap registry-data -n "$NAMESPACE" \
    --from-literal='assets.json={"assets":{},"idempotencyKeys":{}}' >/dev/null
fi

if ! oc get secret object-store-credentials -n "$NAMESPACE" >/dev/null 2>&1; then
  access_key="demo$(openssl rand -hex 6)"
  secret_key="$(openssl rand -hex 24)"
  oc create secret generic object-store-credentials -n "$NAMESPACE" \
    --from-literal=AWS_ACCESS_KEY_ID="$access_key" \
    --from-literal=AWS_SECRET_ACCESS_KEY="$secret_key" \
    --from-literal=AWS_DEFAULT_REGION=us-east-1 \
    --from-literal=AWS_S3_BUCKET=pipeline-artifacts \
    --from-literal=AWS_S3_ENDPOINT="http://minio.$NAMESPACE.svc.cluster.local:9000" >/dev/null
fi

oc label secret object-store-credentials -n "$NAMESPACE" \
  opendatahub.io/dashboard=true opendatahub.io/managed=true --overwrite >/dev/null
oc annotate secret object-store-credentials -n "$NAMESPACE" \
  opendatahub.io/connection-type-ref=s3 \
  openshift.io/display-name="OpenLineage Learning MinIO" --overwrite >/dev/null

if ! oc get secret marquez-db-credentials -n "$NAMESPACE" >/dev/null 2>&1; then
  db_password="$(openssl rand -hex 24)"
  oc create secret generic marquez-db-credentials -n "$NAMESPACE" \
    --from-literal=POSTGRESQL_USER=marquez \
    --from-literal=POSTGRESQL_PASSWORD="$db_password" \
    --from-literal=POSTGRESQL_DATABASE=marquez >/dev/null
fi

oc create configmap application-config -n "$NAMESPACE" \
  --from-literal=PROJECT_NAMESPACE="$NAMESPACE" \
  --from-literal=CLUSTER_NAME="$(cluster_name)" \
  --from-literal=REGISTRY_URL=http://registry:8080 \
  --from-literal=MARQUEZ_URL=http://marquez:80 \
  --from-literal=S3_ENDPOINT=http://minio:9000 \
  --from-literal=S3_REGION=us-east-1 \
  --from-literal=REGISTRY_STORE_BACKEND=configmap \
  --from-literal=REGISTRY_CONFIGMAP_NAME=registry-data \
  --dry-run=client -o yaml | oc apply -f - >/dev/null

oc apply -k "$APP_ROOT/openshift"

oc rollout status deployment/minio -n "$NAMESPACE" --timeout=5m
oc start-build lineage-demo-app -n "$NAMESPACE" --from-dir="$APP_ROOT" --follow --wait

if [[ -z "$(git -C "$APP_ROOT/.." status --porcelain -- "$APP_ROOT")" ]]; then
  image_tag="$(git -C "$APP_ROOT/.." rev-parse --short=12 HEAD)"
else
  image_tag="dev-$(date -u +%Y%m%d%H%M%S)"
fi
oc tag -n "$NAMESPACE" lineage-demo-app:latest "lineage-demo-app:$image_tag"

registry="image-registry.openshift-image-registry.svc:5000/$NAMESPACE"
app_image="$registry/lineage-demo-app:$image_tag"
spark_image="$registry/lineage-demo-spark:$image_tag"

oc delete job object-store-seed -n "$NAMESPACE" --ignore-not-found >/dev/null
oc set image -f "$APP_ROOT/openshift/source-seed.yaml" \
  "seed=$app_image" --local -o yaml | oc apply -n "$NAMESPACE" -f -
wait_job_complete object-store-seed "$NAMESPACE"
oc rollout status deployment/marquez -n "$NAMESPACE" --timeout=10m
oc wait dspa/dspa -n "$NAMESPACE" --for=condition=Ready --timeout=10m
# A DSPA upgraded from route-enabled settings can retain its previously owned
# metadata Route even after deployRoute is disabled. Remove only the two exact
# DSPA-generated names so reruns preserve the internal-only contract.
oc delete route ds-pipeline-dspa ds-pipeline-md-dspa -n "$NAMESPACE" \
  --ignore-not-found >/dev/null

oc start-build lineage-demo-spark -n "$NAMESPACE" --from-dir="$APP_ROOT" --follow --wait
oc tag -n "$NAMESPACE" lineage-demo-spark:latest "lineage-demo-spark:$image_tag"

oc set image -n "$NAMESPACE" deployment/registry "registry=$app_image"
oc rollout status deployment/registry -n "$NAMESPACE" --timeout=5m

UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/dr-lineage-uv-cache}" uv sync --frozen --extra dev
UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/dr-lineage-uv-cache}" uv run python -m pipeline.compile \
  --app-image "$app_image" \
  --spark-image "$spark_image" \
  --output "$BUILD_DIR/pipeline.yaml"

{
  echo "IMAGE_TAG=$image_tag"
  echo "APP_IMAGE=$app_image"
  echo "SPARK_IMAGE=$spark_image"
  echo "CLUSTER_NAME=$(cluster_name)"
} > "$STATE_DIR/images.env"

echo "Deployment is ready. Next: $SCRIPT_DIR/upload-pipeline.sh"
