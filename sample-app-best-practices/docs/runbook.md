# OpenShift Deployment and Lineage Interpretation Runbook

## 1. Purpose

This runbook deploys and exercises the complete learning environment. It also explains
how to interpret what Marquez contains in customer and business terms.

The environment is intentionally self-contained in `ol-best-practices`. It creates no
public Routes and makes no cluster-wide operator changes.

For RHOAI 3.x, `apiServer.enableOauth: false` disables the operator-generated KFP API
Route (despite the field's historical name), and `mlmd.envoy.deployRoute: false`
disables the metadata Route. Access remains available through temporary
`oc port-forward` sessions.
On an existing DSPA that previously enabled Routes, the operator can retain the old
metadata Route after the setting changes. `deploy.sh` removes only the two exact
DSPA-generated Route names after the DSPA is Ready, making internal-only behavior
idempotent. Setting the DSPA route controls back to true recreates them.

## 2. Prerequisites

Log in before running any deployment command:

```bash
oc login <cluster>
oc whoami
```

Required cluster capabilities:

- `DataSciencePipelinesApplication` CRD from RHOAI Data Science Pipelines;
- `SparkApplication` v1beta2 CRD from the Spark Operator;
- default dynamic storage;
- internal OpenShift image registry; and
- permission to create the `ol-best-practices` project.

Required local commands are `oc`, `uv`, `jq`, `curl`, and `openssl`.

## 3. Local validation

Before using cluster resources:

```bash
cd sample-app-best-practices
uv sync --frozen --extra dev
uv run ruff check .
uv run ruff format --check .
uv run python -m pytest
```

The tests validate UUID behavior, canonical S3 identity, registry idempotency,
durable-before-delivery behavior, event retries, KFP compilation, native Spark
configuration, source constraints, and deterministic mock embeddings.

## 4. Preflight

```bash
./scripts/preflight.sh
```

Preflight is read-only. It fails when authentication, CRDs, storage, registry, local
commands, or relevant permissions are missing. The scripts do not install operators.

## 5. Deploy

```bash
./scripts/deploy.sh
```

Deployment performs these operations:

1. Creates `ol-best-practices`.
2. Generates object-store and PostgreSQL credentials as Kubernetes Secrets. Secrets are
   neither committed nor printed.
3. Creates cluster-specific application configuration.
4. Deploys MinIO, persistent Marquez/PostgreSQL, the registry stub, DSPA v2, RBAC,
   image builds, and NetworkPolicies.
5. Builds the pinned application image and uses it to create the `sample-data` and
   `pipeline-artifacts` buckets.
6. Uploads `source-v1.csv` to `s3://sample-data/raw/documents.csv` with bucket versioning
   suspended.
7. Adds a seven-day lifecycle rule to `staging/`.
8. Builds the Spark image, tags both images with the repository
   SHA or a unique dirty-worktree tag, and compiles KFP against those immutable tags.
   The dirty-worktree check includes untracked files, preventing a changed binary build
   from silently reusing the current commit tag.

The seed Job runs the application's pinned image and uses its locked `boto3`
dependency. It starts only after MinIO is ready and the application build
finishes. Failed attempts use `restartPolicy: Never` so their logs remain
available. Both `quay.io/minio/mc` and `docker.io/minio/mc` rejected the seed
image during 2026-09-29 redeploy attempts; the new Job no longer pulls a
separate client image.

The default-deny ingress policy includes one cross-namespace exception: pods in
`redhat-ods-applications` may reach only MinIO TCP port 9000. The RHOAI Pipelines
Operator performs the DSPA object-store health check from that namespace; without this
rule the DSPA remains unready even though same-project MinIO clients work.

The Spark runner Role includes `deletecollection` for ConfigMaps because Spark 4 uses
that operation to remove executor ConfigMaps during shutdown. Omitting it leaves the
workload result intact but produces cleanup 403 errors in driver logs.

Useful status commands:

```bash
oc get pods -n ol-best-practices
oc get dspa -n ol-best-practices
oc get sparkapplications -n ol-best-practices
oc logs deployment/marquez -n ol-best-practices
oc logs deployment/registry -n ol-best-practices
```

The compiled package is written to `build/pipeline.yaml`. Runtime state under `.state/`
and `build/` is ignored by Git.

## 6. Upload the pipeline

```bash
./scripts/upload-pipeline.sh
```

The script temporarily forwards the DSPA API and authenticates with the current `oc`
token. It uploads the compiled pipeline without exposing the API publicly. The KFP
resource name is the RFC 1123 value `openlineage-data-registry-best-practices`; the
description remains human-readable. Re-running the script creates an image-tagged
version, or reuses it when that version already exists.

## 7. Run the scenario suite

```bash
./scripts/run-scenarios.sh
```

The suite performs:

1. Idempotent asset registration and registry `DatasetEvent` delivery.
2. Two successful pipeline runs.
3. A direct overwrite of the raw S3 key with `source-v2.csv` and an assertion that no
   OpenLineage event was produced.
4. A successful run over the changed bytes, still using the same raw dataset identity.
5. Intentional ingestion, Spark, and embedding failures.
6. A fail-fast event-contract verification against Marquez.

Each KFP processing task retries once. Failure scenarios can therefore contain multiple
failed child attempts underneath one failed root run. This is expected and valuable
history.

The Spark failure is deliberately raised inside a Spark SQL action. A Python exception
after successful Spark work would make the SparkApplication fail but could still cause
the native listener to report only `COMPLETE` Spark operations. Failure injection must
occur in the instrumented engine when the goal is to test native failure lineage.
On the validated OpenLineage Spark 1.53.0 integration, the failed SQL event does not
include an `errorMessage` facet and a `COMPLETE` event follows `FAIL` for the same run.
Use the correlated failed KFP root and SparkApplication state as the authoritative
terminal result, and Spark driver logs for error detail; do not add a duplicate manual
Spark event. Re-test these semantics before upgrading the integration.

The suite writes `build/scenario-results.json`. `verify.sh` exits nonzero when expected
lifecycles, parents, aliases, Spark events, or exact dataset handoffs are absent.
Marquez retains older suites, so verification selects child events by the KFP roots in
this report rather than mistaking retained history for the current run.

The local scenario runner passes the temporary OpenShift token and S3 credentials
through stdin, so those values do not appear in process arguments or the scenario
report. This remains a test harness for the isolated POC namespace; a product
workflow should use managed workload identities and secret references.

## 8. Open the internal services

Run this in a separate terminal:

```bash
./scripts/port-forward.sh
```

| Service | Local address |
|---|---|
| Marquez UI | `http://127.0.0.1:3000` |
| Marquez API | `http://127.0.0.1:5000` |
| Registry API | `http://127.0.0.1:8081` |
| MinIO console | `http://127.0.0.1:9001` |
| DSPA/KFP API | `https://127.0.0.1:8888` |

Stop the foreground script with Ctrl-C to close all forwards.

## 9. Inspect the registry

```bash
curl --silent http://127.0.0.1:8081/v1/assets | jq
```

Expected fields include:

```json
{
  "assetId": "<uuid-v7>",
  "location": "s3://sample-data/raw/documents.csv",
  "lineageStatus": "DELIVERED"
}
```

`DELIVERED` means the registry successfully delivered its latest metadata event. It
does not mean downstream processing succeeded and does not validate source contents.

The stub uses one replica and a ConfigMap so its state is easy to inspect. It is not a
production durability pattern and must not be scaled horizontally; the real registry
needs transactional storage plus an outbox/reconciliation design.
`deploy.sh` creates `registry-data` only when absent. Subsequent applies deliberately
do not render that ConfigMap, so an idempotent redeploy cannot reset registered assets.

A `PENDING` response means the registry record was durable but event delivery failed.
Repeat the same registration idempotency key after Marquez recovers; do not create a new
asset to work around delivery.

For the distinction between DatasetEvent, RunEvent, and JobEvent ownership, see the
[event catalog](event-catalog.md). In particular, a `job` object inside a RunEvent is
not the same as a standalone JobEvent.

## 10. Inspect raw OpenLineage events

List persisted events:

```bash
curl --silent 'http://127.0.0.1:5000/api/v1/events/lineage?limit=1000&sort=asc' \
  | jq '.events[] | {eventType, run: .run.runId, job: .job, dataset: .dataset}'
```

Marquez 0.50's `/api/v1/events/lineage` endpoint returns `RunEvent` records; it
does not return `DatasetEvent` or `JobEvent` payloads even though Marquez accepts
and persists them. Inspect a registry `DatasetEvent` through the materialized
dataset entity API instead. URL-encode both the namespace and dataset name:

```bash
registry_namespace='dataregistry://<cluster>/ol-best-practices'
asset_id='<registry UUID>'
curl --silent \
  "http://127.0.0.1:5000/api/v1/namespaces/$(jq -rn --arg v "$registry_namespace" '$v|@uri')/datasets/$(jq -rn --arg v "$asset_id" '$v|@uri')" \
  | jq '{namespace, name, facets}'
```

The automated verifier uses the same split: raw RunEvents for execution and
dataset edges, plus the dataset entity API for registration facets and symlinks.

Inspect the registry symlink in that entity response:

```bash
curl --silent \
  "http://127.0.0.1:5000/api/v1/namespaces/$(jq -rn --arg v "$registry_namespace" '$v|@uri')/datasets/$(jq -rn --arg v "$asset_id" '$v|@uri')" \
  | jq '.facets.symlinks.identifiers'
```

Find failed runs:

```bash
curl --silent 'http://127.0.0.1:5000/api/v1/events/lineage?limit=1000' \
  | jq '.events[] | select(.eventType == "FAIL") | {job, run, error: .run.facets.errorMessage}'
```

Find child-to-root correlation:

```bash
curl --silent 'http://127.0.0.1:5000/api/v1/events/lineage?limit=1000' \
  | jq '.events[] | select(.run.facets.parent != null) | {job, child: .run.runId, parent: .run.facets.parent.run.runId}'
```

Do not put bearer tokens, credentials, or signed URLs into ad hoc queries saved as event
facets.

## 11. Interpret the Marquez graph

Start from either the registry UUID dataset or `s3://sample-data/raw/documents.csv`.
Marquez 0.50 understands the symlink relationship and should connect the logical and
physical views.

The expected data path is:

```text
raw/documents.csv
  -> read-and-stage
  -> staging/<asset>/<ingest-run>/documents.csv
  -> native Spark job
  -> transformed/<asset>/<spark-run>
  -> create-mock-embeddings
  -> embeddings/<asset>/<embedding-run>/vectors.jsonl
```

Marquez can display more than three job nodes because the native Spark integration may
represent Spark application and SQL execution details. Treat those as technical
observability, not duplicate application-level transformations. A manual Spark event
with the same I/O would be a real duplicate and is intentionally absent.

### Dataset identity check

For every handoff compare both fields:

```text
upstream output.namespace == downstream input.namespace
upstream output.name      == downstream input.name
```

A mismatch between `s3://` and `s3a://`, changed trailing slash, or different prefix is
not a minor display issue. It splits the lineage identity. `verify.sh` treats this as a
failure.

### Execution hierarchy check

Every ingestion, data-bearing Spark, and embedding run should resolve to the relevant
KFP root run. Dataset equality says data flowed between jobs; the parent facet says the
same pipeline execution orchestrated them.

## 12. Interpret each scenario in business terms

### First and second successful runs

What the customer learns:

- The same registered logical asset was used repeatedly.
- Stable job definitions produced distinct execution histories.
- Each attempt created separately identifiable staged and derived datasets.
- A downstream vector artifact can be navigated back to the governed registry record.

What the customer does not learn:

- Whether the raw bytes were identical between runs.
- Whether another writer changed the raw key between observations.

### Direct source overwrite

The graph and event count do not change. This proves an important limitation:

> Lineage reflects tracked operations, not all underlying data changes.

The subsequent pipeline run reads different rows while reporting the same raw dataset
identity. This is not a contradiction: the identity represents a mutable logical
dataset, not a content snapshot.
I'
### Ingestion failure

The customer can see attempted access and a failure before a staged output was created.
Spark and embedding should not run. This supports operational diagnosis but does not
prove whether a remote system returned partial bytes before failure.

### Spark failure

Ingestion has a successful staged output. Native Spark reports failure; no valid
transformed dataset should be consumed by embedding. The staged object supports
short-term reproduction while its seven-day retention remains.

### Embedding failure

Ingestion and Spark succeeded. The transformed Parquet is available, and only the final
mock embedding attempt failed. The graph narrows the failure domain.

## 13. Customer assurance assessment

This version has assurance level **Linked**:

- Registry and physical identifiers are reconciled.
- Instrumented operational runs and derived datasets are traceable.
- The raw source remains mutable and lacks a content/version identity.

It is not yet **Observed** because the baseline deliberately omits ETag, object version,
hash, and source manifest evidence. It is not **Reproducible** because the raw source is
not immutable and staging expires.

When communicating results, say:

> This artifact was reported as derived from this registered asset by these
> instrumented runs.

Do not say:

> This proves the exact immutable corpus used to produce the artifact.

## 14. Retention

Marquez has no automated `dbRetention` block in this deployment, so event history stays
in its PostgreSQL PVC until teardown or operator reconfiguration. This is useful for the
learning exercise, not a production retention policy.

MinIO retention differs:

- raw object: retained but mutable, with versioning suspended;
- staged objects: expire after seven days;
- transformed and embedding outputs: retained until teardown.

Keeping an event does not retain the data described by that event.

## 15. Troubleshooting

### Preflight reports unauthorized

Refresh the `oc login` session. No cluster acceptance can be performed with an expired
token.

### DSPA does not become ready

```bash
oc describe dspa dspa -n ol-best-practices
oc get pods -n ol-best-practices
oc logs deployment/minio -n ol-best-practices
```

Confirm the object-store Secret exists and the `pipeline-artifacts` bucket was created.

### Registry returns 503

```bash
oc logs deployment/registry -n ol-best-practices
oc get pods -l app.kubernetes.io/name=marquez -n ol-best-practices
```

The record should still exist with `PENDING`. Restore Marquez, then repeat the same
idempotent request.

### SparkApplication remains pending

```bash
oc get sparkapplications -n ol-best-practices
oc describe sparkapplication <name> -n ol-best-practices
oc get events -n ol-best-practices --sort-by=.lastTimestamp
```

Check image pull status, the DSPA-generated `pipeline-runner-dspa` service account, the
`pipeline-spark-runner` RoleBinding, Spark Operator health, and resource quota. KFP
tasks use the DSPA-generated account so they inherit the Argo `workflowtaskresults`
permissions required by the installed RHOAI version; the sample adds only its
SparkApplication permissions.

### Spark has S3 errors

Confirm `object-store-credentials` is present, MinIO is ready, and driver/executor pods
received `S3_ENDPOINT`. Inspect the Spark driver log:

```bash
oc logs <spark-driver-pod> -n ol-best-practices
```

### Graph is disconnected around Spark

Inspect native event inputs and outputs for `s3a://` versus `s3://`, trailing slashes,
or prefix differences. Do not add a manual duplicate Spark event to hide the mismatch;
fix the producer identity contract.

### Root has START but no terminal event

Inspect the KFP exit-handler pod. Hard pod or node loss can prevent delivery. Production
systems need stale-run reconciliation rather than assuming every emitter can always
send its terminal event.

### Reset the raw source

```bash
./scripts/reset-source.sh
```

This uploads `source-v1.csv` to the mutable raw key. It does not emit OpenLineage, which
is consistent with the direct-write demonstration.

## 16. Cleanup

```bash
./scripts/teardown.sh
```

The command asks for confirmation and deletes the complete project, including MinIO
data, Marquez events, database contents, KFP state, builds, and Secrets. It can also be
run non-interactively with `--yes` when deletion is explicitly intended.

After deletion, recovery requires redeployment; no backup is created by this sample.
