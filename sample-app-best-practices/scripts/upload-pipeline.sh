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

KFP_TOKEN="$(oc whoami -t)" IMAGE_TAG="$IMAGE_TAG" NAMESPACE="$NAMESPACE" \
  UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/dr-lineage-uv-cache}" \
  uv run python - <<'PY'
import os
import hashlib
import json
from pathlib import Path

import kfp
from kfp_server_api.exceptions import ApiException

client = kfp.Client(
    host="https://127.0.0.1:8888",
    existing_token=os.environ["KFP_TOKEN"],
    namespace=os.environ["NAMESPACE"],
    verify_ssl=False,
)
name = "openlineage-data-registry-best-practices"
description = "Registry to ingestion to Spark to mock embeddings"
pipeline_id = client.get_pipeline_id(name)
if pipeline_id is None:
    pipeline = client.upload_pipeline("build/pipeline.yaml", pipeline_name=name, description=description)
    pipeline_id = pipeline.pipeline_id

version_name = os.environ["IMAGE_TAG"]
try:
    version = client.upload_pipeline_version(
        "build/pipeline.yaml",
        pipeline_version_name=version_name,
        pipeline_id=pipeline_id,
        description=description,
    )
except ApiException as exc:
    if exc.status != 409:
        raise
    versions = client.list_pipeline_versions(pipeline_id, page_size=100).pipeline_versions
    matches = [
        item
        for item in versions
        if getattr(item, "display_name", None) == version_name
        or getattr(item, "name", None) == version_name
    ]
    if len(matches) != 1:
        raise RuntimeError(f"Expected one existing pipeline version named {version_name!r}")
    version = matches[0]

reference = {
    "schema_version": 1,
    "pipeline_id": pipeline_id,
    "pipeline_version_id": version.pipeline_version_id,
    "pipeline_version_name": version_name,
    "package_sha256": hashlib.sha256(Path("build/pipeline.yaml").read_bytes()).hexdigest(),
}
Path("build/pipeline-reference.json").write_text(
    json.dumps(reference, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
print(json.dumps(reference, indent=2, sort_keys=True))
PY
