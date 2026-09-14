#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
require_command oc
require_command uv

test -f "$BUILD_DIR/pipeline.yaml" || {
  echo "Pipeline package missing; run deploy.sh first" >&2
  exit 1
}
test -f "$STATE_DIR/images.env" || {
  echo "Image state missing; run deploy.sh first" >&2
  exit 1
}
source "$STATE_DIR/images.env"

oc port-forward -n "$NAMESPACE" service/ds-pipeline-dspa 8888:8888 \
  >"$STATE_DIR/kfp-port-forward.log" 2>&1 &
port_forward_pid=$!
trap 'kill "$port_forward_pid" 2>/dev/null || true' EXIT
wait_tcp 127.0.0.1 8888

KFP_TOKEN="$(oc whoami -t)" IMAGE_TAG="$IMAGE_TAG" \
  UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/dr-lineage-uv-cache}" \
  uv run python -c '
import os
import kfp
from kfp_server_api.exceptions import ApiException

client = kfp.Client(host="https://127.0.0.1:8888", existing_token=os.environ["KFP_TOKEN"], namespace=os.environ.get("NAMESPACE", "ol-best-practices"), verify_ssl=False)
name = "openlineage-data-registry-best-practices"
description = "Registry to ingestion to Spark to mock embeddings"
pipeline_id = client.get_pipeline_id(name)
if pipeline_id is None:
    pipeline = client.upload_pipeline("build/pipeline.yaml", pipeline_name=name, description=description)
    print(pipeline.pipeline_id)
else:
    try:
        version = client.upload_pipeline_version(
            "build/pipeline.yaml",
            pipeline_version_name=os.environ["IMAGE_TAG"],
            pipeline_id=pipeline_id,
            description=description,
        )
        print(version.pipeline_id)
    except ApiException as exc:
        if exc.status != 409:
            raise
        print(pipeline_id)
'
