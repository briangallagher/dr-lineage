# OpenLineage Data Registry Best-Practices Application

This deployable learning application demonstrates how a logical RHOAI Data Registry
asset can be correlated with physical object storage and traced through ingestion, a
real Spark transformation, and a deterministic mock embedding job.

```text
Registry UUID ==symlink== raw S3 object
                              |
                              v
                           ingestion
                              |
                         staged object
                              |
                              v
                            Spark
                              |
                     transformed Parquet
                              |
                              v
                      mock embeddings
```

KFP and Spark are real. The registry API, source data, and embedding algorithm are
deliberately small mocks. OpenLineage 1.53.0 and Marquez 0.50.0 are pinned.

## Read this first

- [OpenLineage design guide](docs/openlineage-design.md): object model, identities,
  event ownership, correlation, JSON, customer value, audit limitations, and production
  recommendations.
- [Runbook](docs/runbook.md): deployment, scenario execution, Marquez inspection,
  interpretation, troubleshooting, and cleanup.
- [Live cluster validation](docs/cluster-validation.md): exact validated versions,
  reproduction commands, integration findings, and acceptance evidence.
- [Event catalog and registry versioning](docs/event-catalog.md): event ownership,
  lifecycle, Marquez API behavior, and the limits of manual revisions.
- [Feast versus Marquez backend gaps](docs/feast-vs-marquez-backend-gaps.md): RHOAI
  Feast consumer assessment, migration risks, and remediation actions.
- [Feast versus Marquez conformance harness](docs/feast-vs-marquez-conformance.md):
  deterministic live API verification and gap report generation.
- [Feast lineage ADR critique and end state](docs/feast-lineage-adr-critique-and-end-state.md):
  critique of ADR-DR-0003 and a proposed RHOAI-wide lineage architecture.
- [RHOAI lineage service architecture](docs/rhoai-lineage-service-architecture.md):
  service responsibilities, deployment options, and ownership boundaries.
- [KFP/DCH native OpenLineage support](docs/kfp-dch-native-openlineage-support.md):
  context propagation and native event-production requirements.
- [Jupyter notebook operational lineage](docs/jupyter-notebook-operational-lineage.md):
  notebook activities, DCH/Spark child runs, model/embedding work, and vector stores.
- [Governed asset to model local adapter](docs/governed-model-local.md): separate
  Slice 2 KFP path, live Data Registry contract observations, MLflow artifact
  linkage, and the remaining cluster prerequisites.

## Prerequisites

- An active `oc` login with permission to create a project.
- RHOAI Data Science Pipelines and Spark Operator already installed.
- A default StorageClass and functioning OpenShift internal image registry.
- `oc`, `uv`, `jq`, `curl`, and `openssl` locally.

The deployment intentionally does not install or reconfigure cluster-wide operators.
`preflight.sh` reports the exact missing CRDs and stops before creating the project.

## Quick start

```bash
cd sample-app-best-practices
./scripts/preflight.sh
./scripts/deploy.sh
./scripts/upload-pipeline.sh
./scripts/run-scenarios.sh
```

Open the internal services with:

```bash
./scripts/port-forward.sh
```

Marquez is then available at `http://127.0.0.1:3000`. No Routes are created.

Run local checks with:

```bash
uv sync --frozen --extra dev
uv run ruff check .
uv run python -m pytest
```

## Repository map

- `src/lineage_demo`: registry, emitters, processing jobs, Spark submission, scenarios,
  and verification.
- `pipeline`: KFP v2 pipeline and compiler.
- `spark`: PySpark application and Spark runtime image.
- `openshift`: namespace-scoped services, RBAC, builds, DSPA, and NetworkPolicies.
- `examples/events`: readable, schema-versioned event examples.
- `scripts`: complete operator workflow.

## Important claim boundary

The deployed Slice 1 sample demonstrates **linked operational lineage**. It can show that an
instrumented run reported using a registered asset and where derived artifacts came
from. It cannot prove the exact bytes at a mutable source URI, observe direct storage
writes, or provide audit-grade reproducibility. Those distinctions are central to the
design rather than treated as implementation gaps.

The separate Slice 2 adapter hashes the Parquet bytes it reads and reports
`OBSERVED` evidence. It has passed local tests; its same-project KFP service,
access rules, and app image are staged in `scenario-b`, but no governed run has
been verified on the cluster.

## Cleanup

```bash
./scripts/teardown.sh
```

This deletes the complete `ol-best-practices` project, including PVC data and lineage
history.
