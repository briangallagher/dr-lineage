# Governed asset to model: Slice 2 adapter

Status: local implementation and tests plus a verified `scenario-b` cluster
trial, 2026-09-30. This is an adapter demonstration, not native RHOAI component
event production or a production-ready MLflow deployment.

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

## Read-only acceptance check

`lineage_demo.verify_governed` is a read-only checker for one completed run.
Its local test uses a real MLflow file store, serialized OpenLineage events, and
a simulated KFP run response. The 2026-09-30 cluster check invoked its
`verify_governed` function with the actual KFP run, Marquez events, and MLflow
client. KFP and MLflow service traffic used OpenShift's injected service CA and
hostname-preserving localhost forwards; TLS verification was not disabled.
MLflow used a short-lived `scenario-b/pipeline-runner-dspa` token and the
`scenario-b` workspace. The result was exported to ignored
`build/governed-result.json` from the KFP ML Metadata `outputs` property for
execution 9, after matching that execution to the succeeded training pod and
its `pipeline/runid` label. This direct MLMD database read is a trial-specific
evidence extraction, not a supported output-export API or reusable product
integration. The exported JSON's SHA-256 is
`12fe4cd5f1ef58a3c6acfc1bd1315fbfe6b2f35f834433e0f329f629b1e1f404`.

The checker fails if the KFP run is not successful or lacks the expected pipeline-version
reference; if the root/child lineage lifecycle, parent, logical asset, physical
object digests, or model output disagree; or if the MLflow run, model, candidate
evaluation, and source-evidence artifacts do not match the lineage event.
It returns check names only. Eleven checks passed for the trial. The checker
cannot prove that an arbitrary local JSON file originated from KFP; that is why
the execution/pod/run linkage was checked separately during export. It also
does not rehash live source objects or prove their retention.

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

## Verified cluster trial and remaining gaps

The Slice 1 KFP/DSPA remains in `ol-best-practices`. A separate `scenario-b`
DSPA was Ready for this path, keeping the selected Registry asset, Data
Connection, and workload in the same project. The accepted run used:

| Evidence | Verified value |
| --- | --- |
| App source commit | `5322021b3a167ad138a97ff45d316cba933faf14` (local; not pushed) |
| Internal app image | `image-registry.openshift-image-registry.svc:5000/scenario-b/lineage-governed-app@sha256:80a3968eb94b490707f69f315110d6c8e9bcb87e6b77cf4efe096bfc75944ed9` |
| Compiled KFP package SHA-256 | `af8989073a17ed07e7ff62383733f47698c7e798c326f53f3dff322416abc3ad` |
| Uploaded pipeline ID | `7e44ad4e-f986-43c3-90d4-c6df8b73c783` |
| Uploaded version ID | `4707191e-5805-4314-8be6-87fadd84a7b9` (`source-5322021b-image-80a3968e`) |
| Accepted KFP run | `ad6efc7b-4b95-464e-b398-31df6b48e061` (`SUCCEEDED`) |
| Linked MLflow run | `8856dadf381141d4a5009c63d5c749b3` (`FINISHED`) |

The first run, `d7307cdd-a609-417c-8357-2a02420b4f84`, failed before asset
resolution because the ConfigMap pointed at the unrelated `scenario-b`
`data-registry-api` workbench proxy. That workbench was intentionally stopped
and its Service had no endpoints. The corrected ConfigMap targets the healthy
operator-managed Registry Service at
`https://feast-data-registry-registry.redhat-ods-applications.svc:8443`, whose
hostname is covered by the service certificate. A runner-token GET returned
the pinned Registry UUID and expected Data Connection before resubmission.
The same immutable pipeline version then succeeded; no workbench restart was
needed.

The trial cluster has:

- the staged same-project workload service account to retain authorized GET
  access to the Registry table through the operator-managed Registry's TLS
  `kube-rbac-proxy` endpoint;
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
deprecated dashboard `mlflow` feature flag was needed. A separate cluster-scoped
`MLflow/mlflow` CR supplied the trial server. `DSPA.spec.mlflow` was unset, so
this trial does **not** prove KFP's separate automatic MLflow tracking feature.
Data Connect Hub was source-pinned but not deployed. The demonstrated
OpenLineage child is emitted by this adapter, not by a native KFP, Data
Registry, DCH, SDG, or MLflow producer.
It does not use automatic KFP retries for model training while MLflow artifact
writes and OpenLineage delivery lack a shared idempotency contract. Spark's
native events remain covered by the Slice 1 fixture; DCH event production
belongs at its future real ingestion boundary. SDG belongs to the later
unstructured/RAG slice once its source and runtime contract are available.

The current KFP package still accepts the Data Connection Secret name as a run
parameter. The adapter checks it against the Registry response after the pod
starts, but that is not permission to mount arbitrary Secrets: only controlled
submitters and a narrowly scoped workload service account are suitable for the
first cluster trial. The existing source Data Connection carries broad MinIO
credentials; it should be replaced with a scoped source credential before a
production path. The MLflow server uses a separate bucket-scoped MinIO service
account. A reusable platform path also needs an explicit policy for which Data
Connections a run may mount, a production backend/HA design instead of this
single-replica SQLite/PVC MLflow trial, and a supported output export API.
MLflow completion and OpenLineage delivery are not transactional; the verifier
detects a split outcome but does not reconcile it.
