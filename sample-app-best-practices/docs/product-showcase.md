# Product showcase: governed model traceability in RHOAI

Status: **[Historical] implementation stepping stone**

The canonical stakeholder entry point is now
[`./scripts/run-poc.sh`](../scripts/run-poc.sh), which combines this story with
the recovery scenario, decision brief, ownership map, and evidence boundary.
This document remains useful when inspecting the first happy-path generator. It
tells one end-to-end story rather than presenting the native KFP contract as the
product:

```text
Data Registry asset
        |
        v
KFP root run -> resolve/stage -> prepare features -> train/evaluate
                                                       |
                                                       v
                                               candidate model
```

The same run produces contract-validated OpenLineage events. Marquez is the
local OpenLineage backend and graph viewer; Data Registry remains authoritative
for governed asset identity, and MLflow remains authoritative for model/run
metadata.

## Run locally

From this directory:

```bash
./scripts/run-product-showcase.sh
open build/product-showcase/audit-report.html
```

The command is offline by default. It writes:

- `audit-report.html`: a PM/architect-oriented audit and traceability view;
- `events.json`: the OpenLineage root and child events;
- `contexts.json`: the KFP context envelopes given to task producers;
- `outbox.json`: the projected root lifecycle records and idempotency keys; and
- `showcase.json`: the scenario metadata and evidence boundary.

To deliver the same events to a reachable Marquez service, set `MARQUEZ_URL`
and use:

```bash
MARQUEZ_URL=http://127.0.0.1:3000/api/v1/lineage \
  ./scripts/run-product-showcase.sh --emit
```

Use the Marquez API port (`5000` in the existing port-forward script), not the
Marquez Web UI port (`3000`):

```bash
MARQUEZ_URL=http://127.0.0.1:5000 \
  ./scripts/run-product-showcase.sh --emit
```

The OpenLineage client adds the ingestion route. The generated artefacts are
still written when delivery succeeds, so the report can be reviewed alongside
the Marquez graph at `http://127.0.0.1:3000`.

## Talk track

The showcase answers four questions a product manager or architect will ask:

1. Which governed asset was used? The report identifies the Data Registry asset
   and its observed physical source through a Symlinks facet.
2. Which execution used it? The KFP root run owns the pipeline and task context.
3. What transformations happened? Spark and workload tasks report their input
   and output datasets as child runs of the KFP root.
4. Which model came out? The final task links the candidate model to MLflow
   metadata and the producing KFP execution.
5. What quality signal supports the decision? The training task reports a
   candidate decision and evaluation metrics as workload-owned metadata.

## Evidence boundary

This is an executable local product fixture, not proof that the current KFP
deployment emits native OpenLineage events. The context, root-event, and outbox
profiles are enforced before artefacts are written or events are emitted. The
native RHOAI implementation still needs a supported KFP control-plane lifecycle
producer and task-context injection path.

The demo intentionally reports the Data Registry asset as `LINKED` rather than
inventing a revision. A production implementation must obtain revision and
reproducibility evidence from the Data Registry contract.
