# Governed asset to model: Slice 2 adapter

Status: local implementation and tests plus staged `scenario-b` infrastructure,
2026-09-30. The KFP service, trial MLflow instance, and app image are present,
but no governed KFP version or Slice 2 run has been uploaded or verified.

## What the adapter proves locally

The separate `pipeline/governed_pipeline.py` definition passes a pinned Data
Registry asset reference into a KFP task. The task obtains the live KFP run UUID
from the pod label, its attempt identity from the pod name, and a projected
service account token for the protected Data Registry API. It requires the
configured project to equal the KFP workload namespace and checks the UUID and
Data Connection Secret name returned by the Registry before reading source data.
It also requires the registered URI's bucket to match `AWS_S3_BUCKET` in that
mounted Secret; the Secret name alone does not constrain broad S3 credentials.

The task reads the registered Parquet objects using the same-project Data
Connection, hashes the exact bytes received, trains a constant majority-class
baseline, and creates an MLflow run with a JSON model artifact, one-row holdout
candidate, and a source-evidence artifact. It emits its own OpenLineage child
`START` and terminal event under the KFP root. The terminal event includes the
logical Registry asset, exact Parquet object inputs, content digests, and a model
artifact output linked to the MLflow run ID. The adapter does not claim to be a
native Data Registry, KFP, DCH, or MLflow producer.

The local test uses an authenticated HTTP mock, actual Parquet bytes, a local
MLflow tracking store, and serialized OpenLineage events. It checks both the
live server's `location` field and the maintained 0.8 contract's
`storage_location` field. Run locally with:

```bash
cd sample-app-best-practices
UV_CACHE_DIR=/tmp/dr-lineage-uv-cache uv sync --frozen --extra dev
UV_CACHE_DIR=/tmp/dr-lineage-uv-cache uv run --frozen python -m pytest
UV_CACHE_DIR=/tmp/dr-lineage-uv-cache uv run --frozen ruff check .
UV_CACHE_DIR=/tmp/dr-lineage-uv-cache uv run --frozen python -m pipeline.compile_governed \
  --app-image <immutable-app-image> --output build/governed-pipeline.yaml
```

The MLflow test sets `MLFLOW_ALLOW_FILE_STORE=true` only for its temporary
local store. The cluster path uses the separate operator-managed tracking
service and S3 artifact proxy described below.

## Repeatable acceptance check for a future cluster run

`lineage_demo.verify_governed` is a read-only checker for one completed run.
Its local test uses a real MLflow file store, serialized OpenLineage events, and
a simulated KFP run response. For a cluster run, export that run's `train-governed`
result artifact through the authenticated KFP API as `build/governed-result.json`,
and make the KFP API, Marquez API, and authorized MLflow tracking endpoint
reachable locally. Then run the checker with the actual uploaded pipeline and
version IDs:

```bash
oc whoami -t | UV_CACHE_DIR=/tmp/dr-lineage-uv-cache uv run --frozen \
  python -m lineage_demo.verify_governed \
  --result-json build/governed-result.json \
  --kfp-endpoint https://127.0.0.1:8888 \
  --kfp-run-id <run-uuid> \
  --pipeline-id <pipeline-uuid> \
  --pipeline-version-id <version-uuid> \
  --marquez-url http://127.0.0.1:5000 \
  --mlflow-tracking-uri <authorized-tracking-uri> \
  --cluster <CLUSTER_NAME-from-run-config> \
  --project scenario-b \
  --expected-asset-uuid 33fbc314-ea15-4805-a958-95b8d29dd67d \
  --expected-source-location s3://poc-underwriting/warehouse/forms/iso_form_extractions
```

The KFP token is read only from stdin, not passed on the command line or
written to the report. MLflow authentication, if required, must be supplied
through its supported environment or credential mechanism. The checker fails
if the KFP run is not successful or lacks the expected pipeline-version
reference; if the root/child lineage lifecycle, parent, logical asset, physical
object digests, or model output disagree; or if the MLflow run, model, candidate
evaluation, and source-evidence artifacts do not match the lineage event.
It prints check names only. The result artifact's provenance must be established
when it is exported from KFP; the checker cannot prove that an arbitrary local
JSON file originated from that run. Nor does it rehash live source objects or
prove their retention. No cluster acceptance has been performed yet.

## Observed cluster contract

A read-only, token-authenticated GET on 2026-09-30 found the synthetic
`scenario-b/forms/iso_form_extractions` table at UUID
`33fbc314-ea15-4805-a958-95b8d29dd67d`, format `parquet`, and location
`s3://poc-underwriting/warehouse/forms/iso_form_extractions`. Its
`connection_ref.secret_name` is `dataconnection-minio-iso-forms`. The live
response used `location`, returned `updated_at: null`, and exposed no revision.
The maintained 0.8 API contract calls the location field `storage_location`
and requires `updated_at`; its asset response also has no catalog revision.
The pipeline default pins the observed UUID so replacement under the same name
fails before model work.

Three identities remain separate:

| Identity or evidence | Meaning |
| --- | --- |
| Registry UUID | Stable logical asset identity. |
| SHA-256 of the response | Metadata representation observed by this attempt; **not** a catalog revision. |
| SHA-256 of Parquet bytes | Exact bytes read by this attempt; **not** proof those bytes remain available. |

The model path reports `OBSERVED`. It does not report `REPRODUCIBLE` because
the Registry has no revision or retained snapshot contract here and the
sample object's later availability has not been established. The one-row
holdout is an evaluation candidate for linkage testing, not model quality
evidence for promotion.

## Cluster path still to qualify

The Slice 1 KFP/DSPA remains in `ol-best-practices`. A separate `scenario-b`
DSPA is now Ready for this path, keeping the selected Registry asset, Data
Connection, and workload in the same project without copying credentials. A
dedicated KFP artifact bucket, narrow Marquez ingress rule, Registry-read
RoleBinding for `pipeline-runner-dspa`, a configured MLflow endpoint, and an
internal-registry app image are staged. The initial image is pinned by digest,
but its binary-build source tree has not yet been pinned to a Git revision. The
compiled local package uses
`image-registry.openshift-image-registry.svc:5000/scenario-b/lineage-governed-app@sha256:9b15bd1100aa0e93368b124276aff90f8dd4f37c0195297beb0b7272d6c44f33`.
The cluster has:

- the staged same-project workload service account to retain authorized GET
  access to the Registry table through the TLS `kube-rbac-proxy` endpoint;
- `data-registry-service-ca` injected into `scenario-b` and the existing
  `dataconnection-minio-iso-forms` Secret, whose expected keys are
  `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_S3_ENDPOINT`, and
  `AWS_DEFAULT_REGION`, plus `AWS_S3_BUCKET` for the bucket guard;
- a `lineage-governed-config` ConfigMap containing `PROJECT_NAMESPACE`,
  `CLUSTER_NAME`, `MARQUEZ_URL`, `DATA_REGISTRY_URL`, and
  `MLFLOW_TRACKING_URI`, `MLFLOW_TRACKING_AUTH`, and
  `MLFLOW_TRACKING_SERVER_CERT_PATH`. The latter two select MLflow's
  `kubernetes-namespaced` provider and the injected OpenShift service CA;
- a cluster-scoped `MLflow/mlflow` singleton, Ready at version 3.14.0, with one
  SQLite/PVC replica and MinIO artifact proxy. Its workspace selector exposes
  only `scenario-b`. The MLflow pod uses a dedicated bucket-scoped MinIO service
  account, not the existing Data Connection's MinIO root credentials;
- a `scenario-b` RoleBinding from the KFP runner to the operator-provided
  `mlflow-operator-mlflow-integration` role. The external gateway redirects
  anonymous requests to OpenShift OAuth; the internal HTTPS API rejects an
  anonymous request with 401 and accepts a `scenario-b` runner token with 200.

The DSC already had `mlflowoperator.managementState: Managed`; no DSC patch or
deprecated dashboard `mlflow` feature flag was needed. The remaining gate is to
pin a source revision, upload its image-backed KFP version, run it, re-read the
version reference, and perform the read-only asset-to-model acceptance check.
Data Connect Hub was source-pinned but not deployed. This path therefore has
staged infrastructure, not a verified end-to-end cluster execution.
It does not use automatic KFP retries for model training while MLflow artifact
writes and OpenLineage delivery lack a shared idempotency contract. Spark's
native events remain covered by the Slice 1 fixture; DCH event production
belongs at its future real ingestion boundary. SDG belongs to the later
unstructured/RAG slice once its source and runtime contract are available.

The current KFP package still accepts the Data Connection Secret name as a run
parameter. The adapter checks it against the Registry response after the pod
starts, but that is not permission to mount arbitrary Secrets: only controlled
submitters and a narrowly scoped workload service account are suitable for the
first cluster trial. A reusable platform path needs an explicit policy for
which Data Connections a run may mount. MLflow completion and OpenLineage
delivery are also not transactional; the verifier detects a split outcome but
does not reconcile it.
