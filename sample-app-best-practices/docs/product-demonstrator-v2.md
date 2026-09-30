---
title: Product demonstrator v2
document_id: DR-POC-002
status: evidence-reviewed
last_verified: 2026-09-30
owners: []
valid_for:
  implementation:
    - repository: local dr-lineage/sample-app-best-practices
      ref: 6f895b91420ed99b86f764ed14c29d2e3b94ba0d
  api_contract: rhoai-kfp:1.0
  jira_snapshot: 2026-09-30
---

# Product demonstrator v2

Status: **[Observed] implementation reference for the canonical POC**

For the PM/architecture walkthrough, use
[`./scripts/run-poc.sh`](../scripts/run-poc.sh). It generates this cockpit next
to the decision brief under `build/poc`; the commands below are retained for
focused development and API inspection.

This is the broader stakeholder experience built on the first KFP showcase. It
has two replayable scenarios and a third decision journey:

1. **Governed training succeeds** — a Data Registry asset flows through KFP,
   feature preparation, evaluation, and a candidate model.
2. **A failed attempt is retained and recovered** — source resolution fails on
   attempt one, succeeds on attempt two, and an independent lineage delivery
   path reaches dead-letter status.
3. **Decide what to invest in** — the capability matrix, trust boundaries, and
   architecture options turn the evidence into a bounded recommendation.

## Generate the cockpit

```bash
cd sample-app-best-practices
./scripts/run-product-demo.sh
open build/product-demo/cockpit.html
```

The generated pack contains a browser cockpit, PM/architecture brief,
decision-package JSON, native-path validation report, one replay data document,
and separate event/context/outbox files for each scenario. The cockpit is fully
offline and uses the same contract-validated data that the replay API serves.

## Import a real RHOAI run

With `oc` already logged into the cluster:

```bash
./scripts/import-rhoai-evidence.sh
```

The script uses temporary read-only port-forwards to the scenario-b KFP API, the
maintained Data Registry REST service, the existing Marquez service, and the
authenticated `scenario-b` MLflow workspace, imports the default verified run,
and regenerates the cockpit with live evidence. Set `RUN_ID` to import another
run. No resources are created, updated, or deleted.

The importer stores only allowlisted KFP parameters and removes credential-like
fields. It records the actual API evidence separately from the fixture
scenarios, including KFP state history, Marquez events, and MLflow metrics,
parameters, correlation tags, and artifact paths correlated to the root run.
Data Registry metadata is queried through the authenticated direct REST
port-forward and checked against the KFP asset UUID. The current `oc` token is
used only in request headers for Data Registry and MLflow and is never written
to the evidence file.

The importer also projects the sanitized KFP state history through the local
`rhoai-kfp:1.0` context, root-event, and outbox model. That record is labelled
`LOCAL_CONTRACT_PROJECTION`: it proves the producer/projector/importer boundary
and idempotency shape locally, but it is not native KFP emission and is not sent
to Marquez by the importer.

## Serve the replay API

```bash
uv run --extra dev lineage-demo demo-serve --data-dir build/product-demo
```

The browser view is available at `http://127.0.0.1:8090`. The API exposes:

- `/api/health`;
- `/api/demo`;
- `/api/dossier`;
- `/api/scenarios`; and
- `/api/scenarios/{id}`.

The API is deliberately small and local. It is a product-demo seam, not a
replacement for a future RHOAI lineage service or Marquez query API.

## Marquez and evidence

`--emit` sends the typed happy-path events to the configured Marquez endpoint.
The retry scenario remains local replay data because its purpose is to show
failure, retry, and dead-letter semantics without mutating a shared backend.

The cockpit also includes a read-only verified RHOAI snapshot from the cluster
trial. It proves KFP run identity, pipeline/version identity, and state history,
and now correlates the live MLflow run by its `kfp.root_run_id` tag. It does not
prove native KFP OpenLineage emission, and the UI labels it
`VERIFIED_READ_ONLY_WITH_GAPS` rather than presenting it as a complete product
guarantee.

The **Decision** view is the stakeholder-level readout. It shows the capability
matrix, architecture options, trust/ownership implications, and exit criteria.
The **Audit dossier** view is the product-level evidence readout. It separates system
evidence by owner, reports the contract validation state, shows the live KFP
timeline and fixture lineage edges, and lists the gaps that prevent a stronger
claim. In particular, the current live Marquez root events are existing
adapter-owned events; absence of the proposed `rhoaiKfp` facet is recorded as a
native-integration gap rather than silently filled by the local fixture.

## Product talk track

The demo is intended to answer these questions:

- What governed asset produced this model?
- Which KFP execution and task attempts were involved?
- Which transformations, metrics, and model artefacts were reported?
- What happened when the first attempt failed?
- Which facts are verified in RHOAI, which are fixture behaviour, which are
  adapter-owned, and which still require a native platform integration?
- Which architecture option and bounded investment should follow?

The `rhoai-kfp:1.0` context, root-event, and outbox contracts are enforced when
the artefact pack is generated. KFP itself is not modified by this demonstrator.

## Architecture mapping

| Product capability | Demonstrator owner | Event/metadata owner | Eventual RHOAI boundary |
| --- | --- | --- | --- |
| Governed asset identity | Data Registry fixture | Data Registry asset ID and linked source | Data Registry API and revision contract |
| Run and task hierarchy | KFP-shaped producer | KFP context and root lifecycle | KFP/RHOAI control-plane integration |
| Physical read and transformation | DCH/Spark-shaped tasks | Workload-owned input/output edges | First-party DCH/Spark adapters |
| Metrics and model decision | Training task fixture | MLflow-shaped model link and evaluation facet | MLflow integration |
| Event storage and graph | Marquez-compatible path | OpenLineage events | RHOAI lineage backend or supported Marquez deployment |
| Recovery and delivery audit | Local outbox replay | Versioned idempotency and delivery status | Durable platform outbox/reconciliation service |

This keeps the product story broad while retaining clear ownership. The KFP
root supplies context and lifecycle; it does not claim that it read a dataset,
performed a transformation, or created a model.

The recommended next investment is `FUND_BOUNDED_DISCOVERY`: preserve both user
journeys, qualify one supported native context/root path, define one workload
adapter contract, and exercise delivery/replay boundaries. This is a proposed
decision based on the observed baseline, not a production commitment.

## Evidence ledger

| Claim | Evidence state | Source and baseline | Caveat |
| --- | --- | --- | --- |
| The cockpit has happy-path, recovery, and decision views | [Observed] | `src/lineage_demo/product_demo.py`, generated `build/poc/cockpit.html`, tests passing 2026-09-30 | Browser artifact is local and deterministic. |
| The replay pack validates context, root-event, and outbox shapes | [Observed] | `contracts/`, `src/lineage_demo/native_validation.py`, local test suite | Contract validation is not native KFP support. |
| Live KFP/Registry/MLflow/OpenLineage observations can be shown beside fixture data | [Observed] | `examples/native-kfp-validation/live-read-only-baseline-2026-09-30.json` | Read-only evidence from one environment; event ownership remains adapter-owned. |
| `FUND_BOUNDED_DISCOVERY` is the recommended investment posture | [Proposed] | Generated decision package and PM brief, 2026-09-30 | Requires stakeholder agreement. |
