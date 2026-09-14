# RHOAI Lineage: Findings for Architecture Discussion

**Status:** Discussion draft  
**Audience:** RHOAI lineage/Data Registry architecture stakeholders

## Executive summary

The main gap is the RHOAI product contract, not the choice between Marquez and
Feast. The architecture should first define the supported user questions, initial
component scope, cross-component event contract, authorisation model, and visible
limitations.

The initial capability should be described as **linked operational lineage**: a
view of relationships reported by instrumented RHOAI components. It is not a
complete audit trail, proof of the exact bytes used, or proof of reproducibility.

RHOAI should publish a versioned **RHOAI Lineage Profile** built on OpenLineage.
The profile should remain stable across backend choices; Feast is a candidate
operational-lineage backend, subject to qualification, rather than a foregone
platform-wide decision.

## 1. Start with a small set of product questions

| Business question | Responsible initial answer |
|---|---|
| Where did this asset come from, and what depends on it? | Show authorised, retained upstream and downstream relationships within the instrumented boundary. |
| Which run produced this result, or where did it fail? | Show reported inputs, outputs, parameters, attempts, and the authoritative status source where available. |
| Who may see this lineage? | Apply project authorisation to assets, runs, edges, physical locations, parameters, and errors. |
| How complete and trustworthy is this answer? | Explain missing instrumentation, failed delivery, retention, redaction, unlinked data, and available source evidence. |

These questions cover the initial value: navigation, impact analysis, failure
diagnosis, and confidence in the answer. Each needs an API query, authorisation
rule, partial/empty state, and acceptance test.

## 2. Define the product boundary and assurance

The current design is closest to **operational lineage**: what instrumented
systems reported about jobs, runs, inputs, outputs, retries, failures, and
execution relationships. It should be distinguished from:

- **Source and artifact evidence:** object versions, hashes, manifests, table
  snapshots, Registry revisions, retained copies, and storage history.
- **Request-time traces:** feature retrieval, model serving, evaluation, and
  request activity, which may belong in MLflow, OpenTelemetry, or another plane.

Assurance should be computed from explicit evidence, not inferred from the
presence of a graph edge:

1. **Linked** — a logical asset is connected to a reported physical/runtime path.
2. **Observed** — a producer recorded source evidence such as an object version,
   hash, manifest, or table snapshot.
3. **Reproducible** — the required immutable inputs, outputs, code, environment,
   and execution data are retained well enough to reconstruct the run.

The current sample demonstrates the first level. A Data Registry UUID is a stable
business anchor, not a content version.

## 3. Bound the initial journeys and component scope

The first release should support three journeys:

1. Follow authorised upstream and downstream lineage from a Data Registry asset.
2. Inspect the run and evidence associated with a derived asset.
3. Diagnose a failed ingestion, Spark job, or pipeline.

For each included component, publish its release status, owner, emitted events,
required context, instrumentation method, authoritative status source, and blind
spots. Do not present target behaviour as current capability; Data Registry Phase
1 lineage visualisation is currently out of scope.

| Component | Target responsibility |
|---|---|
| Data Registry | Stable asset/revision identity, metadata, physical aliases, and asset-centric entry point. |
| DCH | Events and source evidence for remote reads or ingestion it actually observes. |
| KFP | Root pipeline lifecycle and propagation of execution context. |
| Spark | Native execution and physical data I/O events, with parent context when launched by KFP. |
| Workbench/custom jobs | Bounded activities through a supported launcher, wrapper, or SDK. |
| Feature Store and model systems | Explicitly classify as initial scope or a later linked plane. |

## 4. Define the RHOAI Lineage Profile before service topology

The versioned profile should define:

- canonical logical and physical dataset identities and normalisation;
- stable asset, revision, and observed-content identity;
- required logical-to-physical aliases, such as a symlink facet;
- project/deployment authority and producer authorisation;
- job, run, attempt, parent, lifecycle, and reconciliation semantics;
- redaction, idempotency, replay, partiality, and conformance levels.

Supported RHOAI paths should inject project, run, attempt, asset, and delivery
context. Users should not construct raw OpenLineage identifiers by hand.

This must also cover data observed before Registry registration. DCH and Spark
should report the physical datasets they actually read or write. Data Registry
can publish the stable logical identity and its physical alias; later
re-correlation must be explicit and auditable. Producers must not claim logical
I/O they did not observe.

## 5. Keep product and ingestion boundaries backend-neutral

A generic lineage graph does not by itself provide Data Registry metadata,
RHOAI authorisation, revision semantics, assurance, or explanations of
partiality. RHOAI therefore needs a stable asset-centric query/API contract,
possibly in or beside the Data Registry BFF.

RHOAI also needs a logical ingestion boundary that authenticates producers,
validates the profile, normalises identities, redacts sensitive data, handles
duplicates and replay, and reports failed delivery. This does not necessarily
require a standalone service or an extra network hop; it may be implemented by
adapters, a gateway, or a shared service.

These interfaces should target the **selected operational-lineage backend**.
Feast may provide that backend if its exact RHOAI image, operator, APIs, storage,
and upgrade path pass the conformance and security gates.

## 6. Treat security, retention, and conformance as release gates

- **Security:** bind producer identity to permitted projects/namespaces; enforce
  traversal-safe read filtering; redact sensitive physical and execution data;
  and include negative cross-project tests. API-key authentication alone is not
  RHOAI authorisation.
- **Delivery and lifecycle:** define fail-open/fail-closed behaviour, retry or
  outbox handling, replay, late/duplicate events, missing terminals,
  contradictory states, and the authoritative status source.
- **Retention and evidence:** define raw-event, run-detail, graph, revision, and
  archive retention separately. A retained edge must not imply that its evidence
  still exists.
- **Backend qualification:** test the exact supported build for identity and
  symlink convergence, parent runs, revisions, replay/idempotency, lifecycle
  conflicts, backend outages, authorisation, retention, migrations, and near-
  identical dataset names that must not silently merge.

The existing POC already demonstrates useful contracts and limitations. The next
step is a versioned conformance matrix against the candidate backend, not another
open-ended POC.

## Suggested immediate decisions

1. Agree the four product questions, initial journeys, non-goals, and component
   release scope.
2. Publish the first RHOAI Lineage Profile and conformance fixtures.
3. Decide the authorisation, delivery, retention, and assurance policies.
4. Define the stable asset-centric query contract and logical ingestion boundary.
5. Qualify the exact Feast/RHOAI build before selecting it as a shared backend.

## Review basis

- Architecture decision [PR #154](https://github.com/opendatahub-io/architecture-decision-records/pull/154), reviewed at visible revision `fa01c8d`.
- [Ana's questions](https://gitlab.cee.redhat.com/data-strategy/data-arch-design-options/-/blob/main/docs/feast-considerations/feast-openlineage-consumer-questions.md), commit `0ef4fb598ca59d5a530bcdc7054ec5cd29d735cd`.
- [Nikhil's responses](https://gitlab.cee.redhat.com/data-strategy/data-arch-design-options/-/blob/main/docs/feast-considerations/feast-openlineage-consumer-responses.md), commit `029b2c2093a7cd27b80e8fec2b70a7e251621112`.
- Local RHOAI lineage sample, contract analysis, and live-cluster validation.
