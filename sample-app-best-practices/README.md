# OpenLineage Data Registry lineage POC

This repository is currently organized as a **PM/architecture proof of concept**.
It demonstrates one coherent question rather than claiming a production-ready
integration:

> Can we explain how a governed RHOAI Data Registry asset became a model, including
> what ran, what was observed, and what happened when part of the run failed?

The canonical experience is a deterministic, offline 10–15 minute decision-ready
demonstrator. It combines governed asset discovery, KFP-shaped orchestration,
processing and model outcome, lineage/audit inspection, failure/retry value, and an
explicit architecture/investment decision. Production qualification material is
retained below as reference and is deliberately not the main success criterion.

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

In Slice 1, KFP and Spark are real, while the registry API, source data, and
embedding algorithm are deliberately small mocks. Slice 2 uses the live Data
Registry and a Parquet-backed baseline model. OpenLineage 1.53.0 and Marquez
0.50.0 are pinned.

## Read this first

- [PM/architecture POC brief](docs/pm-architecture-poc.md): the product question,
  user journeys, 10–15 minute talk track, architecture/trust boundaries,
  capability matrix, evidence boundary, and investment decision.
- [Canonical POC runner](scripts/run-poc.sh): one offline command that builds the
  brief, interactive cockpit, replay data, and audit dossier under `build/poc`.
- [Product demonstrator reference](docs/product-demonstrator-v2.md): cockpit views,
  replay API, and implementation detail behind the canonical POC.
- [Product showcase reference](docs/product-showcase.md): the original governed
  asset-to-model stepping stone.
- [Read-only evidence importer](scripts/import-rhoai-evidence.sh): optional capture
  of sanitized KFP/Data Registry/MLflow/Marquez observations; no cluster writes.
- [Native-path validation checkpoint](docs/native-kfp-poc-validation.md): four
  local checks for root lifecycle, context injection, retry identity, and a
  workload-owned model edge, with a narrow-spike recommendation.
- The generated `build/poc/decision-package.json` and cockpit **Decision** view:
  the capability matrix, architecture options, bounded scope, and exit criteria
  used for the stakeholder review.
- [Live read-only native baseline](examples/native-kfp-validation/live-read-only-baseline-2026-09-30.json):
  sanitized evidence from one RHOAI/KFP run and the installed launcher path.

## Appendix: architecture and production reference

These documents are useful inputs to a future native-integration decision. They are
not prerequisites for the POC demonstration:

- [OpenLineage design guide](docs/openlineage-design.md), [runbook](docs/runbook.md),
  and [live cluster validation](docs/cluster-validation.md).
- [Event catalog and registry versioning](docs/event-catalog.md), [Feast versus
  Marquez gaps](docs/feast-vs-marquez-backend-gaps.md), and [conformance
  harness](docs/feast-vs-marquez-conformance.md).
- [RHOAI lineage service architecture](docs/rhoai-lineage-service-architecture.md)
  and [KFP/DCH native OpenLineage support](docs/kfp-dch-native-openlineage-support.md).
- [Native KFP context contract](docs/native-kfp-lineage-context-contract.md),
  [native-path validation checkpoint](docs/native-kfp-poc-validation.md),
  [implementation readiness ADR](docs/adr-native-kfp-lineage-implementation-readiness.md),
  [packaged qualification](docs/rhoai-kfp-packaged-qualification.md), and
  [qualification handoff](docs/rhoai-kfp-qualification-handoff.md).
- [Qualification capture](scripts/capture-rhoai-kfp-qualification.sh),
  [qualification verification](scripts/verify-rhoai-kfp-qualification.sh), and
  [native readiness check](scripts/verify-native-kfp-readiness.sh).
- [Jupyter notebook operational lineage](docs/jupyter-notebook-operational-lineage.md)
  and [governed asset-to-model local adapter](docs/governed-model-local.md).

## Prerequisites

For the PM/architecture POC, only `uv` and a local browser are required. The
optional read-only evidence importer additionally uses `oc`, `jq`, and `curl`.
The deployed learning application also requires:

- An active `oc` login with permission to create a project.
- RHOAI Data Science Pipelines and Spark Operator already installed.
- A default StorageClass and functioning OpenShift internal image registry.
- `openssl` locally.

The deployment intentionally does not install or reconfigure cluster-wide operators.
`preflight.sh` reports the exact missing CRDs and stops before creating the project.

## Quick start

### PM/architecture POC

```bash
cd sample-app-best-practices
./scripts/run-poc.sh
open build/poc/poc-brief.html
```

Open `build/poc/cockpit.html` during the talk track to switch between the successful
governed run and the failure/retry recovery journey. The runner is offline by default
and does not mutate a cluster or emit to a shared Marquez service.

### Optional read-only RHOAI evidence

With an active `oc` session, import a sanitized snapshot through temporary read-only
port-forwards and regenerate the POC with it:

```bash
./scripts/import-rhoai-evidence.sh
./scripts/run-poc.sh --verified-evidence build/product-demo/live-rhoai-evidence.json
```

### Local checks

```bash
uv sync --frozen --extra dev
uv run ruff check .
uv run python -m pytest
```

### Deployed learning application (reference)

The original deployable learning application remains available for implementation
and qualification work:

```bash
./scripts/preflight.sh
./scripts/deploy.sh
./scripts/upload-pipeline.sh
./scripts/run-scenarios.sh
```

Open the internal services with `./scripts/port-forward.sh`; Marquez is then
available at `http://127.0.0.1:3000`. No Routes are created.

The older individual generators remain useful for focused debugging:

```bash
./scripts/run-product-showcase.sh
./scripts/run-product-demo.sh
```

These are implementation/reference entry points; use `run-poc.sh` for the PM and
architecture demonstration.

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
`OBSERVED` evidence. Local tests and one `scenario-b` cluster run passed an
11-check read-only acceptance across KFP, OpenLineage, and MLflow on
2026-09-30. This remains adapter-emitted lineage, not native RHOAI component
instrumentation or proof that mutable source bytes remain available. See the
[governed trial record](docs/governed-model-local.md) for exact run and image
identities.

## Cleanup

```bash
./scripts/teardown.sh
```

This deletes the complete `ol-best-practices` project, including PVC data and lineage
history.
