# RHOAI OpenLineage strategy: findings, gaps and required clarifications

**Status:** Discussion draft  
**Date:** 2026-09-13  
**Audience:** RHOAI Data Hub, Data Registry, Feature Store, Data Platform and architecture stakeholders

## Executive conclusion

The Jira record supports the claim that Feast is **not currently intended to be the RHOAI-wide lineage solution**.

[RHAIRFE-2744](https://redhat.atlassian.net/browse/RHAIRFE-2744) and
[RHAISTRAT-2335](https://redhat.atlassian.net/browse/RHAISTRAT-2335) define Feast OpenLineage consumption as a Feature Store 3.6 Tech Preview capability. They explicitly exclude the Data Hub/platform-wide lineage outcome, the Data Hub lineage UX, MLflow evaluation lineage, and productization of Marquez or another external OpenLineage control plane.

The broader direction is described by
[RHAISTRAT-1931](https://redhat.atlassian.net/browse/RHAISTRAT-1931) and
[RHAIRFE-2621](https://redhat.atlassian.net/browse/RHAIRFE-2621): RHOAI should provide platform-scoped provenance and lineage for data that flows through RHOAI, including temporal context, process/data relationships, authorization and Data Hub integration. However, those tickets do not yet select the backend, define the event contract, assign ownership of a shared lineage API, or specify how the participating RHOAI components will emit and correlate events.

The immediate conclusion is therefore:

> Feast is a Feature Store-scoped OpenLineage consumer and a possible backend candidate for a broader operational-lineage plane. It is not yet the approved RHOAI-wide lineage architecture. Data Hub lineage needs a broader RHOAI contract for identities, events, instrumentation, authorization, retention and user-facing answers before it can be implemented safely.

## Assessment of the cross-component assertion

The assertion that Data Hub lineage is difficult to define independently of the broader RHOAI strategy is substantially correct, for technical reasons rather than merely organizational ones.

Lineage is a graph. A Data Hub asset becomes useful lineage only when other components can refer to the same asset and execution consistently. For example, a useful path may cross:

```text
Data Registry asset -> DCH ingestion -> KFP pipeline -> Spark transformation
                    -> Feature Store materialization -> model/evaluation artifact
```

If each component chooses its own namespace, dataset name, revision identifier, run correlation or event ownership, the graph will fragment even if every component emits valid OpenLineage JSON. Data Hub cannot repair that reliably after the fact.

The assertion should, however, be bounded in two ways:

1. **Data Hub does not need to own every lineage function.** It can own the logical asset identity, registry metadata, asset-centric queries and user experience while runtime components own events about work they actually observe.
2. **RHOAI-wide does not mean enterprise-wide.** [RHAISTRAT-1931](https://redhat.atlassian.net/browse/RHAISTRAT-1931) explicitly says the platform should trace data that flows through RHOAI, not become a general enterprise catalog or governance system.

The right architectural boundary is therefore a shared RHOAI lineage contract, with separate ownership of asset metadata, runtime observations, evidence and presentation.

## What the current Jira record establishes

| Area | Current position | Evidence |
|---|---|---|
| Feast OpenLineage | Feature Store TP consumer for external producers such as Spark and Airflow; includes ingestion, SQL persistence and graph queries. | [RHAIRFE-2744](https://redhat.atlassian.net/browse/RHAIRFE-2744), [RHAISTRAT-2335](https://redhat.atlassian.net/browse/RHAISTRAT-2335) |
| Feast dashboard | Feature Store views for Feast-native versus broader OpenLineage-backed lineage. | [RHAIRFE-2745](https://redhat.atlassian.net/browse/RHAIRFE-2745), [RHAISTRAT-2296](https://redhat.atlassian.net/browse/RHAISTRAT-2296) |
| Data Hub lineage | Intended platform capability for automatic provenance, temporal context and upstream/downstream relationships. | [RHAISTRAT-1931](https://redhat.atlassian.net/browse/RHAISTRAT-1931), [RHAIRFE-2621](https://redhat.atlassian.net/browse/RHAIRFE-2621) |
| Data Registry | Asset registration, discovery, metadata and authorization. Phase 1 lineage visualization is explicitly out of scope. | [RHAIRFE-2618](https://redhat.atlassian.net/browse/RHAIRFE-2618), [RHAIRFE-2619](https://redhat.atlassian.net/browse/RHAIRFE-2619), [RHAISTRAT-2381](https://redhat.atlassian.net/browse/RHAISTRAT-2381) |
| Model/evaluation provenance | Separate use case involving model inputs, source documents, chunks and evaluation lineage; MLflow is a proposed direction. | [RHAIRFE-163](https://redhat.atlassian.net/browse/RHAIRFE-163) |
| Feast/Data Hub relationship | Still investigative: assess whether Feast OpenLineage support can be leveraged by Data Registry or Data Hub and produce a proposal/ADR. | [RHOAIENG-93246](https://redhat.atlassian.net/browse/RHOAIENG-93246) |

This is a coherent set of intentions, but not yet a complete strategy. Notably, [RHOAIENG-93246](https://redhat.atlassian.net/browse/RHOAIENG-93246) remains an investigation, while [RHAIRFE-2621](https://redhat.atlassian.net/browse/RHAIRFE-2621) remains New.

## Clarifications needed, ordered by urgency

### P0 — Decide the product boundary and ownership

The architecture decision should state, in one place:

- whether RHOAI is standardizing on OpenLineage as an operational-lineage interchange contract;
- whether Feast is only a Feature Store consumer, a reusable operational-lineage backend, or both;
- whether the platform-wide capability is owned by Data Hub, a shared RHOAI service, or another product area;
- what Data Registry owns versus what the lineage service owns;
- what is Tech Preview, what is a committed product capability, and what remains exploratory.

Suggested direction, subject to formal approval:

> RHOAI Data Hub owns asset-centric lineage experiences and joins to Data Registry metadata. RHOAI runtime components emit events for operations they observe. A shared operational-lineage service consumes and queries those events. Feast may provide that service for Feature Store and possibly broader RHOAI use cases, but this requires a separate compatibility, security and product decision. This does not claim complete data provenance, immutable source history or query-time serving lineage.

### P0 — Define the user questions before selecting the backend

The strategy should define the first user journeys and the authoritative answer for each. At minimum:

- Where did this registered asset come from?
- Which pipelines, jobs or models consumed it?
- What downstream assets may be affected by a change?
- Which run produced this derived table, feature set or document collection?
- Who owns the logical asset and which project is allowed to see it?
- What failed, and which system is authoritative for the failure state?
- Can this result be reproduced, or is the evidence only a reported linkage?

Each journey needs a defined API query, authorization rule, empty/partial state and evidence level. A generic Feast graph endpoint is not by itself an answer to these questions.

### P0 — Establish one cross-RHOAI identity and event contract

This is the most direct consequence of the cross-component assertion. The contract must cover at least:

- canonical namespace and dataset naming for Data Registry, S3/object stores, tables, volumes and external databases;
- stable Data Registry asset identity versus revision/content identity;
- physical aliases and the required logical-to-physical relationship, such as an OpenLineage symlink;
- project/namespace/cluster authority and portability across deployments;
- run, job and pipeline identifiers, including parent-child execution relationships;
- timestamps, retries, cancellation, failure and terminal-state semantics;
- schema, statistics and column-lineage expectations where available;
- idempotency and duplicate-event behavior;
- redaction rules for locations, connection names, parameters and errors.

The contract must be tested across at least Data Registry, DCH, KFP, Spark and Feature Store. Valid events with incompatible identities are not an interoperable strategy.

### P0 — Decide who emits events and how context is propagated

The platform must explicitly decide whether lineage emission is:

- native in the component, as with Spark or a future DCH integration;
- added to RHOAI-owned reusable pipeline components;
- provided through a platform wrapper or sidecar;
- manually implemented by customers in their own pipelines; or
- a combination, with stated completeness limitations.

This decision affects KFP, Spark and every component that reads or writes data. It must answer:

- How does a component obtain the Data Registry asset ID and revision?
- How is the root pipeline/run ID propagated to child tasks?
- Is the event emitted by the pipeline orchestration layer or by the component that observes the actual I/O?
- What happens when an ingestion component is third-party or cannot be modified?
- Can a wrapper report the operation without falsely claiming details it did not observe?
- What is the supported customer experience: hand-written code, reusable pipelines, SDK helpers or automatic platform instrumentation?

The current JIRAs identify automatic capture as a goal but leave these implementation choices open.

### P0 — Define authorization and tenant isolation for the graph

Lineage can disclose asset names, physical locations, ownership, pipeline parameters and failure details. The strategy needs separate policies for:

- who may emit events and which namespace/project they may assert;
- who may query an asset’s upstream and downstream graph;
- whether physical aliases and connection metadata are visible;
- how explicitly shared assets appear across namespaces;
- how cluster administrators receive broader visibility;
- how parent runs and error messages are filtered.

Feast namespace filtering and API-key ingestion cannot be assumed to equal RHOAI authorization. The chosen design needs negative cross-tenant tests and a named enforcement point.

### P1 — Select and qualify the shared operational-lineage backend

The current record does not resolve Feast versus Marquez versus another service. [RHAISTRAT-2335](https://redhat.atlassian.net/browse/RHAISTRAT-2335) proves that Feast has a consumer and graph implementation; it does not prove that it satisfies the broader RHOAI contract.

The decision should compare candidates against:

- OpenLineage event and facet support;
- identity and symlink behavior;
- multi-tenant authorization;
- retention and archival;
- query/API extensibility;
- delivery, replay and reconciliation;
- upgrade and migration behavior;
- operational scale and database isolation;
- ability to support a RHOAI asset-centric API without exposing backend-specific concepts.

[RHOAIENG-93246](https://redhat.atlassian.net/browse/RHOAIENG-93246) is the natural vehicle for this comparison and should not be treated as complete until it produces an approved architecture decision.

### P1 — Define a RHOAI lineage API/BFF boundary

The product should not make Data Hub and Feature Store UIs independently understand Feast internals, OpenLineage graph semantics and Data Registry authorization.

A shared API should compose:

- Data Registry asset identity, metadata, ownership and revisions;
- operational lineage events, runs and edges;
- evidence and assurance state;
- authorization-filtered partiality warnings;
- links to Feature Store lineage and model/evaluation provenance where relevant.

The API should be asset-centric and stable even if the backend changes. This is also how RHOAI can present a customer-friendly experience when Feast or Marquez remains a technical backend.

### P1 — Separate operational lineage, data evidence and runtime traces

The strategy should distinguish:

1. **Linked:** a logical asset is connected to reported physical/runtime identities.
2. **Observed:** a producer recorded evidence such as an object version, ETag, hash, manifest or table snapshot.
3. **Reproducible:** the required source and derived artifacts are retained and immutable enough to reconstruct the run.

OpenLineage events primarily provide operational lineage. Data Registry revisions, DCH observations, object storage/table metadata and retained artifacts provide evidence. Model evaluation, retrieval and serving traces may require MLflow or another observability system. These should be connected by shared identifiers but not presented as interchangeable proof.

### P1 — Define delivery, retention and historical semantics

The strategy needs explicit policies for:

- whether workload execution fails when lineage delivery fails;
- retry, outbox, queue and dead-letter behavior;
- duplicate and late events;
- incomplete runs and contradictory terminal states;
- raw event retention versus graph retention;
- archival, replay, backup and restore;
- registry revision history and physical source versions;
- deletion, legal hold and tenant off-boarding.

Without these policies, a graph may look complete while the evidence needed to explain it has already been pruned.

### P2 — Define the initial component matrix and rollout

The strategy should publish a versioned matrix showing, for each supported component:

| Component | Emits | Consumes | Required context | Native or wrapper instrumentation | Known blind spots |
|---|---|---|---|---|---|
| Data Registry | Logical asset/revision event and physical aliases | Asset-centric lineage queries | Asset ID, revision, project | To be decided | Registration does not prove data was read |
| DCH | Source observation/ingestion events | Asset and connection metadata | Connection ID, source identity, root run | To be decided | Third-party connectors may be opaque |
| KFP | Root orchestration lifecycle | Child run context | Pipeline/run/project IDs | To be decided | Orchestration is not proof of actual I/O |
| Spark | Actual reads/writes and transformations | Parent context, asset mappings | Dataset identities, run IDs | Native integration preferred | Direct uninstrumented writes |
| Feature Store | Feast-native and possibly external events | Operational graph | Feature/project context | Existing Feast support | Not a complete Data Hub provenance record |
| Workbench/custom jobs | Workload-specific events | Registry and run context | User, project, assets | SDK/wrapper likely | Customer code may omit events |

The rollout should state which rows are supported in each release and how the UI communicates missing coverage.

## Recommended immediate action

Create or update one architecture decision that links the Feast TP work to the Data Hub lineage outcome. It should produce these artifacts before committing to a platform-wide implementation:

1. agreed user journeys and assurance vocabulary;
2. canonical identity/event contract with conformance fixtures;
3. producer/instrumentation matrix for KFP, Spark, DCH, Data Registry and Feature Store;
4. backend comparison and decision, including the role of Feast;
5. RHOAI lineage API/BFF contract;
6. authorization, retention and failure-handling policy.

Until those exist, the safe description is that Feast provides a useful Feature Store TP and a promising integration point, while the RHOAI-wide lineage strategy remains incomplete.

