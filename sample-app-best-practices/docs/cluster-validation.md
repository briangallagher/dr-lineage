# Live OpenShift Validation Record

## Scope and result

This document records the live-cluster installation and acceptance procedure used on
2026-09-10. It complements the reusable [runbook](runbook.md): the runbook explains how
to operate the sample on any compatible cluster, while this file records what was
actually observed on the validation cluster.

The target was project `ol-best-practices` on cluster
`api.bgal-pool-ljt7l.aws.rh-ods.com`. The deployment used internal Services and local
port-forwards only; it created no Routes and made no cluster-wide configuration
changes.

## Validated platform

| Component | Observed version or state |
|---|---|
| OpenShift user | `htpasswd-cluster-admin-user` |
| RHOAI | `3.6.0-ea.1` |
| AI Pipelines | Managed; DSPA Ready; KFP server `2.16.0` |
| KFP Python client | `2.16.1` |
| Spark Operator | Managed, operator `2.4.0`; `SparkApplication` v1beta2 CRD Ready |
| Spark runtime | `4.0.1`, Scala `2.13`, Java 17 |
| OpenLineage | Python and native Spark integration `1.53.0` |
| Marquez | `0.50.0`, backed by PostgreSQL and a PVC |

The KFP setup procedure followed the local `kfp-setup` skill. This RHOAI version uses
`DataScienceCluster.spec.components.aipipelines`; older releases used
`datasciencepipelines`. The preflight script supports both. The skill normally permits
exposing the service with a Route, but this application's security design deliberately
keeps KFP, Marquez, MinIO, and the registry internal and uses temporary port-forwards.
The DSPA explicitly sets `apiServer.enableOauth: false` and
`mlmd.envoy.deployRoute: false`; on RHOAI 3.x these fields suppress the two Routes the
operator otherwise creates by default.

The operator checks can be repeated without changing the cluster:

```bash
oc get datasciencecluster -o json \
  | jq '.items[].spec.components | {aipipelines, datasciencepipelines, spark}'
oc get crd sparkapplications.sparkoperator.k8s.io \
  -o jsonpath='{.status.conditions[?(@.type=="Established")].status}{"\n"}'
oc get csv -A | grep -E 'rhods|spark'
oc api-resources | grep -E 'DataSciencePipelinesApplication|SparkApplication'
```

## Reproduction commands

From an authenticated terminal:

```bash
cd /Users/briangallagher/dev/workspaces/dr-lineage/sample-app-best-practices
oc whoami
oc whoami --show-server
./scripts/preflight.sh
./scripts/deploy.sh
./scripts/upload-pipeline.sh
./scripts/run-scenarios.sh
```

The last command runs two ordinary successes, an uninstrumented source overwrite and
success, and one failure each at ingestion, Spark, and embedding. It then invokes the
lineage contract verifier. Re-run only the persisted-event checks with:

```bash
./scripts/verify.sh
```

Local contract checks used before and after live acceptance were:

```bash
uv sync --frozen --extra dev
uv run ruff check src tests pipeline
uv run python -m pytest
```

## Cluster-specific integration findings

These findings are encoded in the manifests, scripts, tests, and runbook, rather than
being undocumented manual workarounds:

1. RHOAI 3.x reports AI Pipelines under `aipipelines`; preflight accepts that field and
   the legacy field.
2. The MinIO Data Science Connection needs the RHOAI labels/annotations and the full S3
   key set. The DSPA-generated `pipeline-runner-dspa` service account is the correct KFP
   execution identity.
3. The DSPA controller checks object storage from `redhat-ods-applications`. A narrow
   NetworkPolicy exception permits those pods to reach MinIO port 9000 while retaining
   default-deny ingress.
4. RHOAI 3.x creates API and metadata Routes unless both DSPA route controls are
   disabled. A metadata Route created by the earlier configuration was retained after
   the setting changed, so deploy also removes only the two known DSPA Route names.
   The final deployment was verified with no Routes in the project, DSPA Ready, and
   successful KFP API access through the internal Service port-forward.
5. The MinIO seed Job must run after MinIO is ready. `MC_CONFIG_DIR=/tmp/.mc` is required
   because OpenShift's arbitrary UID cannot write the image's default `/.mc` path.
6. Registry state is create-only during deploy, so a redeploy does not erase asset IDs
   or idempotency keys.
7. Spark Operator 2.4.0 accepts `env` and `envFrom` in the CRD but did not copy them to
   generated driver/executor pods on this cluster. The submitted SparkApplication uses
   the deprecated-but-implemented `envVars` and `envSecretKeyRefs` compatibility fields.
   A live smoke application proved the required values were present in the driver.
8. KFP's pipeline UUID placeholder remained literal in custom container arguments.
   Components now read `metadata.labels['pipeline/runid']` through the Downward API, so
   the OpenLineage root run UUID exactly equals the KFP run UUID.
9. Lightweight exit-handler components must set `install_kfp_package=False`; the pinned
   runtime image already contains KFP and has no `pip` command. This produces exactly
   one root terminal event.
10. Marquez 0.50 accepts and materializes `DatasetEvent`, but its lineage-event listing
   returns RunEvents only. The verifier reads registry symlinks from the dataset entity
   endpoint and execution lineage from the event endpoint.
11. A Python exception raised after successful Spark work is not a native Spark failure
    from the listener's perspective. A failing `collect()` was also skipped because it
    had no output dataset. The failure scenario therefore triggers `raise_error` in a
    data-bearing Parquet write so native OpenLineage can report the failed operation.
    The observed SQL event had `START`, `RUNNING`, and `FAIL` with input and attempted
    output datasets, but OpenLineage Spark 1.53.0 did not add an `errorMessage` facet
    and emitted `COMPLETE` for the same run roughly 50 ms after `FAIL`. Error detail
    and the authoritative single terminal state remain available on the KFP root and
    in SparkApplication state/driver logs. This is a pinned-integration limitation, not
    hidden by a manual duplicate event.
12. Spark 4 cleanup uses `deletecollection` for generated ConfigMaps. The namespace
    Role grants that verb; without it the workload result is unaffected but cleanup
    produces a 403 error.
13. Marquez intentionally retains earlier runs. Verification scopes retained child
    events to the KFP root IDs in the current scenario report, while still checking all
    events correlated to those roots.

## Acceptance evidence

The deployed pipeline ID is `76c7563e-7fcb-4fde-bbf4-61dac8a80a69`. The final build
uses image tag `dev-20260910160844`; the final pipeline version ID is
`7d755fdb-1cde-49d3-ac25-7f1067d32a40`.

The authoritative per-run IDs and actual states are written by the suite to
`build/scenario-results.json`. The verifier checks, and fails the suite if any is absent:

| Scenario | KFP/OpenLineage root run | Observed state |
|---|---|---|
| `success-1` | `1964d9fd-ced0-4206-b759-11a1f03d58ea` | `SUCCEEDED` |
| `success-2` | `8cb55802-e5e8-4204-9105-784b442b620e` | `SUCCEEDED` |
| `success-after-bypass` | `dc53acb7-7892-4efd-9ad2-813af8489908` | `SUCCEEDED` |
| `failure-ingest` | `2e0af73d-10ac-46b7-8c8f-35bf029feb79` | `FAILED` |
| `failure-spark` | `f189e54f-cc89-4897-8a0e-b5040e5a60ca` | `FAILED` |
| `failure-embed` | `2ab6d826-7425-43f2-a27d-6956b03bab55` | `FAILED` |

The direct S3 overwrite observed 417 Marquez RunEvents before and 417 after. The
successful pipeline immediately after the overwrite retained the same raw dataset
identity, demonstrating both the graph continuity and the instrumentation blind spot.
The final verifier returned `status: passed` with 18 contract checks:

- stable logical job identities with distinct run UUIDs and per-run output datasets;
- registry UUID to raw S3 identity convergence through the standard symlink facet;
- one root `START` and exactly one expected terminal state;
- ParentRun correlation from ingestion, native Spark, and embedding to the KFP root;
- exact ingestion-output/Spark-input and Spark-output/embedding-input identities;
- standard job, run, dataset, statistics, error, and column-lineage facets;
- native OpenLineage producer identity for every Spark event;
- native `START` and `FAIL` presence for the Spark failure, with the observed duplicate
  native terminal limitation documented rather than silently accepted as authoritative;
- a distinct child run for task retries and downstream suppression after failures; and
- unchanged Marquez event count across an uninstrumented direct S3 overwrite.

Failed exploratory workflows and prior image versions remain in KFP and Marquez as
diagnostic history. They are not silently deleted and are not treated as evidence for
the current acceptance suite.
