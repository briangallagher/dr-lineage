# Product audit dossier: the KFP-centred RHOAI story

Status: local product-level POC, 2026-09-30. This is a demonstrator for a
product manager and architect; it is not a production lineage service and does
not modify KFP or the cluster.

## The product question

The demo is designed around one question:

> Which governed data asset produced this model, what actually happened during
> the run, and which parts of that answer can we prove?

The answer is intentionally composed across systems. KFP supplies the
orchestration identity and run lifecycle. Data Registry supplies the governed
asset identity. Workload producers report the physical data edges. MLflow owns
the model run, metrics, and artifacts. Marquez/OpenLineage provides the
operational event graph. The audit dossier makes the joins and their assurance
visible instead of presenting one system as the owner of all semantics.

## What the current demo proves

The live read-only evidence pack currently contains:

| System | Observed evidence | Assurance |
| --- | --- | --- |
| KFP | Run UUID, pipeline ID, pipeline version, task summaries, and state history | `VERIFIED_READ_ONLY` |
| Data Registry | Asset UUID, project, collection, name, format, and location from the maintained REST API; UUID checked against the KFP parameter | `VERIFIED_READ_ONLY` when imported; offline fixture remains `LINKED_FROM_KFP_PARAMETER` |
| MLflow | Correlated run, status, metrics, parameters, tags, and model/evaluation/evidence artifact directories | `VERIFIED_READ_ONLY` |
| Marquez/OpenLineage | Correlated root and child events, states, and jobs | `VERIFIED_READ_ONLY` |
| Local contract projection | Imported KFP state projected through the context, root-event, and outbox contracts | `LOCAL_CONTRACT_PROJECTION`; not native emission |
| Native KFP facet | Whether current live root events contain the proposed `rhoaiKfp` facet | Explicitly reported as observed/not observed |

The verified live run is `ad6efc7b-4b95-464e-b398-31df6b48e061`. Its MLflow
correlation is established by the `kfp.root_run_id` tag, not by guessing from a
display name or event timestamp.

## Profiles and contracts

The local demonstrator applies three related contracts under the `rhoai-kfp:1.0`
profile:

1. **Context envelope** — stable deployment/project namespace, exact KFP root
   UUID, pipeline and version identity, task identity, parent identity, and
   attempt number. It carries safe identifiers, not credentials or arbitrary
   environment state.
2. **Root event** — KFP-owned `START`, optional `RUNNING`, and one authoritative
   terminal event. The root has no fabricated dataset inputs or outputs.
3. **Outbox record** — idempotency key, event payload, attempts, and delivery
   status. Delivery retry/dead-letter state is separate from workload success or
   failure.

The context is the propagation contract; the root event is the lifecycle
contract; the outbox is the durability/reconciliation contract. A future native
implementation may change the transport, but it must preserve these semantics
or version the profile.

## Current state and target state

```text
Current RHOAI observation
  KFP run API ───────────────┐
  MLflow workspace ──────────┼─ read-only bridge ── audit dossier
  Marquez/OpenLineage ───────┤
  Data Registry identity ────┘

Target native integration
  KFP control plane
      ├─ emits the root lifecycle with exact run UUID
      ├─ injects the versioned context envelope into supported tasks
      └─ persists root events through a durable outbox
  DCH / Spark / workload producers emit observed data edges
  Data Registry and MLflow remain authoritative for their own records
  A RHOAI query layer joins, authorizes, and labels evidence
```

The target does not require KFP to become a data catalogue, Data Registry to
become an execution store, or Marquez to become an authorization policy engine.
It requires explicit joins and a product-facing evidence model.

## Evidence and trust language

The dossier uses assurance labels rather than a single boolean “lineage
complete” value:

- **Verified read-only** — a fact was returned by a live authenticated API
  without changing the cluster.
- **Linked** — two identities correlate through an explicit contract or tag, but
  the authoritative metadata has not necessarily been re-read.
- **Demo fixture** — deterministic local behaviour used to show a product path,
  retry, or delivery failure.
- **Gap** — a missing backend, unsupported native integration, or unresolved
  revision/authorization question.

The overall current trust level is therefore
`VERIFIED_READ_ONLY_WITH_GAPS`. This is stronger and more useful for an
architect than presenting fixture events as if they were current platform
behaviour.

## Explicit non-goals

- Modifying KFP, its API server, launcher, or DSPA.
- Claiming that the current RHOAI KFP deployment emits native OpenLineage root
  events or the proposed `rhoaiKfp` facet.
- Treating a Data Registry asset UUID as an immutable source revision.
- Moving model metrics or artifacts into the Data Registry.
- Making Marquez the owner of Data Registry or MLflow semantics.
- Providing production authorization, HA storage, or a durable service out of
  this local browser demo.

## Useful demo commands

```bash
cd sample-app-best-practices
./scripts/import-rhoai-evidence.sh
open build/product-demo/cockpit.html
uv run --extra dev lineage-demo demo-serve --data-dir build/product-demo
```

The repeatable importer is deliberately read-only. It accepts only local
port-forward endpoints, allowlists KFP runtime parameters, sanitizes
credential-like keys, verifies the live Data Registry UUID against the KFP
parameter, and never writes an `oc` token into the evidence pack.

## Evidence ledger

| Claim | Evidence state | Source and verification baseline | Caveat |
| --- | --- | --- | --- |
| KFP run identity, pipeline/version identity, and state history | [Observed] | Read-only KFP API import for run `ad6efc7b-4b95-464e-b398-31df6b48e061`, 2026-09-30 | One run and one RHOAI 3.6.0-ea.1 environment; not a universal release claim |
| Data Registry asset identity and metadata | [Observed] | Maintained Data Registry REST API through `feast-data-registry-registry-rest`, authenticated local port-forward, 2026-09-30 | Asset UUID is not a durable source revision |
| MLflow run, metrics, and artifact paths | [Observed] | Authenticated MLflow API search/list correlated by `kfp.root_run_id`, 2026-09-30 | Artifact listing is evidence of paths, not a model-quality certification |
| Correlated OpenLineage events | [Observed] | Read-only Marquez API query correlated to the KFP root UUID, 2026-09-30 | Existing events are adapter-owned and do not prove native KFP production |
| Context/root-event/outbox profile | [Proposed] and locally executable | `contracts/`, `src/lineage_demo/kfp_lineage.py`, and focused tests at the current workspace state | No native KFP deployment, durable platform outbox, or Marquez delivery is claimed |
| Native `rhoaiKfp` facet in current live root events | [Observed] absent | Imported Marquez root events and generated dossier, 2026-09-30 | Absence is a gap in this inspected deployment, not proof about every RHOAI release |

## Cross-project evidence routes

The implementation boundary and dated Data Registry context were checked in
[PROJECT_CONTEXT.md](/Users/briangallagher/dev/workspaces/data-registry/PROJECT_CONTEXT.md).
Broader RHOAI product and architecture context was cross-checked against the
[`rhoai-knowledge` Data Registry dossier](/Users/briangallagher/dev/git-repos/rhoai-knowledge/components/data-registry.md)
and the [RHOAI Data Strategy domain route](/Users/briangallagher/dev/git-repos/rhoai-knowledge/domains/data/README.md).
Those projects remain the sources for Data Registry implementation context and
cross-RHOAI synthesis; this workspace owns the demonstrator, fixtures, and
contract evidence.
