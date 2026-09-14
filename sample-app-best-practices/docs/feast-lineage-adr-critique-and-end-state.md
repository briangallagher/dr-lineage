# Critique of ADR-DR-0003 and a clearer RHOAI lineage end state

Status: working architecture analysis, 2026-09-11

This document critiques the draft
[ODH-ADR-DR-0003: Data Registry and Data Connect Hub — Future Integration Scenarios](https://github.com/opendatahub-io/architecture-decision-records/pull/154),
with emphasis on the OpenLineage/Feast scenario. The ADR is a useful first draft:
it identifies stable asset UUIDs, separates Data Registry orchestration from DCH
introspection, prefers decoupled event emission, and calls out security and mutable
storage risks. It is not yet a sufficient architecture decision for a RHOAI lineage
product.

The central problem is not that the ADR chose the wrong implementation. It is that it
does not yet define the product and evidence boundary clearly enough to decide
whether the implementation is complete.

> The ADR needs to state what a RHOAI user can know, from which authoritative
> evidence, through which components, over what time period, and with what degree
> of confidence.

The sample application provides a useful standard for doing that. Its conclusion is
that the initial capability is **linked operational lineage**: it shows how
instrumented components reported using and producing registered assets. It does not
prove immutable source bytes, complete storage history, or reproducible model input.
See [OpenLineage Design and Best-Practices Guide](openlineage-design.md),
[Event Catalog](event-catalog.md), and
[Code Deep Dive](code-deep-dive.md).

## 1. What the draft ADR does well

The draft makes several sound decisions that should be retained.

### 1.1 It gives the Data Registry a real identity role

The ADR treats a Data Registry asset UUID as a durable identity rather than using a
display name or mutable physical location as the primary key. That is the correct
foundation for cross-component lineage.

### 1.2 It separates schema discovery from ingestion

The revised Scenario 4 makes Data Registry the orchestrator of a separate schema
discovery request while DCH performs connector-specific introspection. That is a
better ownership boundary than making registration itself perform an implicit DCH
operation or making DCH write directly into the registry.

The ADR should, however, remove the stale wording in the introductory “What” section
that still describes asynchronous schema population when the detailed scenario now
describes a synchronous, separate API. The example and the decision text must agree.

### 1.3 It prefers independent event emission

The “decoupled event emission” principle is compatible with the sample:

- Data Registry emits a design-time `DatasetEvent` for its logical asset.
- DCH emits a runtime `RunEvent` because DCH observes the remote read.
- KFP emits orchestration lifecycle.
- Native Spark emits Spark execution and I/O.
- Other components emit their own work.

This avoids a central emitter making claims about operations it did not observe.

### 1.4 It identifies the logical/physical identity bridge

The proposed registry identity plus physical source identity is the right shape. The
sample makes this concrete with a standard `SymlinksDatasetFacet`:

```text
dataregistry://<authority>/<project>, <asset-uuid>
    ==symlink==
s3://<bucket>, <object-or-prefix>
```

The ADR should make the symlink behavior normative rather than leaving it as an
example or an implementation choice.

### 1.5 It recognizes the hard problems

The ADR already calls out several risks that should become explicit acceptance
criteria: credential sanitization, schema drift, DCH availability, UUID migration,
instrumentation coverage, lineage retention, and point-in-time limitations. Those are
the right risks. They currently appear as open questions or notes rather than as a
complete end-state contract.

## 2. The biggest gap: the use case is not defined sharply enough

The ADR says “automated schema discovery” and “cross-component lineage,” but those
are capabilities, not use cases. It does not identify:

- the primary user or persona;
- the moment in a workflow when the user needs lineage;
- the decision the user is trying to make;
- the assets, runs, and evidence they need to see;
- the authoritative source for each displayed fact;
- the expected result when instrumentation or retention is incomplete; or
- how success will be measured.

The sample application starts from user questions instead of from the existence of
an OpenLineage server:

| User question | What the initial design can responsibly provide |
|---|---|
| Where did this vector dataset or model input come from? | Known registered asset, physical source alias, and instrumented ingestion/transformation path. |
| Where is this asset used? | Known downstream runs and datasets reported by instrumented producers. |
| What might be affected by a source/schema/location change? | Impact graph over retained, authorized operational edges. |
| Why does a derived artifact look wrong? | Producing runs, parameters, timestamps, engine information, failures, and known input identities. |
| Who owns the input? | Join from the logical Data Registry asset to registry ownership metadata. |
| Can I reproduce the result? | Only if immutable source evidence and retained execution/data artifacts exist; a link alone is insufficient. |

The ADR should adopt a small set of user journeys such as:

1. A user opens a Data Registry asset and follows upstream and downstream lineage.
2. A data scientist inspects the run that produced a derived table or feature view.
3. An operator diagnoses a failed DCH/Spark/KFP operation and sees which status is
   authoritative.
4. A governance user asks what changed between two registry revisions.
5. A user sees that a path is incomplete because a direct storage writer or an
   uninstrumented component is outside the lineage boundary.

For each journey, the ADR should define the minimum data, the backend query, the
authorization rule, the expected empty/partial state, and the evidence level shown
to the user.

## 3. Operational lineage and data lineage are not distinguished

This is the clearest conceptual omission.

### 3.1 Operational lineage

Operational lineage records what instrumented systems reported about executions and
their inputs/outputs:

- jobs, runs, lifecycle states, retries, failures;
- dataset inputs and outputs;
- execution hierarchy such as `ParentRunFacet`;
- schemas, statistics, column lineage, and processing engines;
- ownership and runtime parameters that are safe to expose.

Feast's OpenLineage consumer is a plausible backend for this plane.

### 3.2 Data provenance/evidence

Data provenance asks what data actually existed or was consumed:

- object version, table snapshot, manifest, ETag, hash, or observed timestamp;
- registry revision and pointer history;
- retained copies or immutable storage;
- schema change history and approval context;
- deletion, retention, residency, and legal-hold state.

The Data Registry, DCH, object store, and table format—not a generic OpenLineage
consumer alone—must provide these facts.

### 3.3 Query-time/runtime trace

Feature retrieval, model serving, evaluation, and request-level traces are another
plane. The Feast maintainer response explicitly places query-time serving lineage
outside the OpenLineage consumer, pointing to metrics/OTel. MLflow may hold related
training and evaluation context.

The ADR should not call all three planes simply “lineage” without defining their
relationship. A user can have a correct operational graph while still lacking proof
of the bytes used or a trace of a serving request.

### 3.4 Recommended assurance vocabulary

Adopt the sample's vocabulary:

1. **Linked:** a registry identity and a reported physical/runtime path are connected.
2. **Observed:** a producer recorded evidence such as ETag, hash, object version,
   manifest, or table snapshot.
3. **Reproducible:** the required source and derived artifacts are immutable or
   retained well enough to reconstruct the run.

Every user-facing lineage result should state which level is supported. A stable
asset UUID is a business anchor; it is not automatically a data version.

## 4. The ADR does not define a complete end-state architecture

The draft describes Data Registry, DCH, OpenLineage, and a possible Marquez server,
but it does not decide how these fit into the broader RHOAI experience. In particular,
it does not resolve the Feast backend direction now being pursued in
[RHAISTRAT-2335](https://redhat.atlassian.net/browse/RHAISTRAT-2335).

The architecture needs an explicit component boundary.

| Component | In scope for the end state | Authority / responsibility |
|---|---|---|
| Data Registry | Yes | Asset identity, metadata, location references, schema metadata, revisions, ownership, project authorization. |
| DCH | Yes | Connector access, remote introspection, ingestion observation, source evidence, connection context without credentials. |
| KFP / pipeline runtime | Yes | Root orchestration lifecycle, context propagation, pipeline parameters, retry context. |
| Spark / Ray / component runtimes | Yes where instrumented | Actual execution and data I/O, preferably through native integrations. |
| Feast OpenLineage consumer/server | Candidate backend | Generic event ingestion, SQL persistence, graph construction, retention, generic queries, Feast-native lineage. |
| RHOAI lineage API/BFF | Missing from the draft | Joins Feast graph with Data Registry, applies RHOAI authorization, exposes asset-centric queries, labels evidence and partiality. |
| Feature Store dashboard | Related but distinct | Feature-centric views and a route into broader lineage. |
| Data Registry dashboard | Related but distinct | Asset-centric views and a route into operational lineage. |
| MLflow / OTel / serving traces | Separate plane, linked | Query-time retrieval, model, evaluation, and request traces. |
| Marquez | Candidate/reference backend only | Not the desired RHOAI product contract if Feast is selected. |
| Storage audit/versioning | Separate authority | Direct writes, object versions, table snapshots, manifests, and retention evidence. |

The absence of the RHOAI lineage API/BFF is consequential. It leaves each UI to
understand Feast internals, Data Registry identifiers, authorization, and the
operational/data-lineage distinction independently.

## 5. Recommended end state

The end state should be stated in the ADR as a set of planes with explicit owners.

```text
                         RHOAI user experience
                  asset detail | run detail | impact
                              |
                     RHOAI lineage API / BFF
             authorization | joins | evidence | partiality
                 /                    |                    \
                /                     |                     \
       Data Registry          Feast OL consumer          MLflow / OTel
  asset/revision/schema       operational graph           query/runtime
  location/ownership           events/runs/edges           traces
          |                            |                     |
          +--------- shared IDs -------+---------------------+
          |                            |
      DCH / KFP / Spark / Ray / notebooks / feature operations
                emit owned OpenLineage events
```

### 5.1 Data Registry plane

Data Registry owns:

- stable asset UUID and project-scoped authority;
- display name, ownership, classification, and metadata;
- physical location references and sanitized connection references;
- schema metadata and schema-discovery result;
- explicit registry revisions;
- revision-to-evidence links where evidence exists;
- authorization for the logical asset.

Registration emits a `DatasetEvent` for the logical asset. The event includes a
standard symlink to the physical source when the Registry knows that relationship.
It does not claim that registration read or verified the bytes.

### 5.2 Operational lineage plane

DCH, KFP, native Spark, notebooks, feature operations, and future runtimes emit
OpenLineage events for work they observe. Feast consumes them in a separate lineage
server or otherwise isolated service profile.

Feast owns generic event ingestion and graph persistence. It should not become the
source of truth for Data Registry asset metadata, revisions, credentials, or
project policy.

### 5.3 RHOAI integration plane

An RHOAI API/BFF should provide stable, asset-centric operations such as:

```text
GET /lineage/assets/{asset-id}
GET /lineage/assets/{asset-id}/upstream
GET /lineage/assets/{asset-id}/downstream
GET /lineage/assets/{asset-id}/runs
GET /lineage/assets/{asset-id}/revisions
GET /lineage/runs/{run-id}
```

The exact paths are not important. The contract is. Responses should include:

- logical asset and physical aliases;
- graph nodes and edges with source/authority;
- run hierarchy and dataset dependencies separately;
- revision and assurance state;
- timestamps and retention limitations;
- authorization-filtered metadata;
- warnings for uninstrumented or unlinked portions;
- links to Feast-native lineage and MLflow/OTel where relevant.

### 5.4 Query/runtime plane

Feature retrieval and serving traces remain separate. The integration should use
shared identifiers where possible, but the UI must tell the user whether a link is an
operational event, a data-evidence record, or a request trace.

## 6. Gaps the ADR needs to address

### 6.1 A precise decision statement is missing

The ADR's decision should answer all of these questions:

- Is this decision about schema discovery, lineage, or both?
- Is Feast the intended OpenLineage backend for RHOAI, or is the backend still a
  separate future decision?
- Is the goal a generic event store, a Data Registry integration, a user-facing
  lineage service, or all three?
- Which parts are TP, which are GA, and which are explicitly future work?
- What replaces Marquez in the sample's architecture, and what does not?

Suggested decision wording:

> RHOAI will use OpenLineage as the interchange contract for operational lineage.
> Data Registry will own logical asset identity, registry metadata, and revision
> semantics. DCH and runtime components will emit events for operations they observe.
> Feast's OpenLineage consumer/server is the current backend candidate for receiving,
> storing, and querying those events. A RHOAI lineage API will compose Feast with
> Data Registry and enforce RHOAI authorization. This decision does not claim
> immutable data provenance or query-time serving lineage.

That statement is much more actionable than “emit events to a shared OL-compatible
server.”

### 6.2 The canonical identity rules are incomplete

The ADR gives examples for Data Registry, external databases, and S3, but examples
are not a producer contract. It needs to define:

- canonical namespace authority and deployment portability;
- name granularity for table, file, prefix, bucket, database, and collection;
- URI schemes and normalization;
- case, escaping, encoding, and trailing slash rules;
- asset rename and physical-location change behavior;
- symlink versus containment versus collection relationships;
- how a revision is represented without changing the stable asset identity;
- legacy/foreign IDs and explicit alias mappings;
- whether cluster names in namespaces are stable enough for long-lived history.

The sample currently uses a cluster-qualified registry namespace. The ADR's proposed
example is project/namespace-oriented. RHOAI must choose one, publish it, and test
migration before multiple producers emit incompatible identities.

### 6.3 Event ownership and event catalog are underspecified

The ADR lists possible event types but does not define a complete catalog. It should
specify, for each component:

| Producer | Required event | Required input/output identity | Required context |
|---|---|---|---|
| Data Registry | `DatasetEvent` on registration/revision | Logical asset plus physical symlink(s) | asset ID, revision, owner, project, evidence level |
| DCH | `RunEvent` for introspection/ingestion | Actual external input and Registry output where known | connection ID, root run, source evidence, redacted parameters |
| KFP | Root `RunEvent` | Orchestration job | KFP run ID, project, pipeline, outcome |
| Spark/native runtime | Native `RunEvent` | Actual read/write datasets | Spark run, parent context, schema/statistics/column lineage |
| Notebook/custom job | `RunEvent` | Registered or explicitly unregistered inputs/outputs | user, project, root context, runtime |
| Feature operations | Feast/OpenLineage events | Feature data and materialization artifacts | feature service/registry context |

It also needs rules for `START`, `RUNNING`, terminal states, retries, cancellation,
duplicate delivery, missing terminal events, late events, and contradictory native
events. The sample's observed Spark `FAIL` then `COMPLETE` behavior makes this more
than theoretical documentation.

### 6.4 Versioning and evidence are not a UUID problem

The ADR correctly wants stable asset UUIDs, but a UUID answers “which logical asset?”
not “which contents?” The draft mentions that direct storage writes are invisible and
that point-in-time lineage needs retention, but it does not decide what RHOAI will
provide.

The ADR should define:

- stable asset identity;
- registry revision identity;
- observed source evidence;
- immutable source/table version;
- retained derived artifacts;
- evidence retention and deletion;
- what “reproducible” means for files, tables, documents, and features.

Without that, a graph from `asset UUID -> Spark run -> feature output` will be
mistaken for an audit trail. The sample explicitly refuses that claim.

### 6.5 Feast integration is missing from the architecture decision

The ADR currently uses Marquez as an example and says the OpenLineage server choice
is a separate decision. That was reasonable before the Feast strategy was developed,
but the architecture now needs a visible relationship to
[RHAISTRAT-2335](https://redhat.atlassian.net/browse/RHAISTRAT-2335):

- Feast consumes all three event types in the proposed integration.
- It joins primarily on exact dataset identity, with symlink support.
- It can store graph state but does not infer Data Registry ownership or revisions.
- It has default raw-event retention and no cold archive in the current response.
- Its read filtering and producer-authentication model need an RHOAI binding.
- It has a separate lineage-server option and a generic API, not the complete RHOAI
  asset-centric user contract.

The ADR should either incorporate these constraints or explicitly say that the
Feast-backed consumer is out of scope for this ADR and create a follow-up decision
with a dependency link. Leaving the server abstract hides decisions that affect
identity, retention, auth, deployment, and user value.

### 6.6 Producer context propagation is missing

The sample passes `asset_id` into the pipeline, resolves the Registry record at
runtime, and propagates the KFP root run ID to child components. It also discovered
that a KFP placeholder passed as a custom argument could remain a literal, so the
implementation reads the live Kubernetes label instead.

The ADR should define how real RHOAI users obtain and propagate:

- project/namespace;
- asset ID and revision;
- root pipeline/run ID;
- DCH connection ID;
- user/service identity;
- parent job/run context;
- failure and cancellation outcome.

If this is left to each pipeline author, the “cross-component” graph will be
sporadic. If it is hidden inside a platform shim, the shim must document what it can
and cannot observe.

### 6.7 Authorization and information disclosure are under-specified

The ADR mentions lineage data access control but does not define it. It needs separate
answers for:

- who can emit an event;
- which namespaces/projects a producer may assert;
- who can query a logical asset;
- whether a user can see a physical alias or connection name;
- how cross-project shared sources behave;
- how parent facets and error messages are filtered;
- what is logged for audit.

The Feast response describes API-key writes and application-layer namespace filtering.
That cannot be assumed to equal Data Registry authorization. The ADR should require
negative authorization tests and name the component that enforces policy.

### 6.8 Delivery, failure, and reconciliation are missing operational semantics

The phrase “components independently emit” leaves unanswered what happens when the
lineage service is down. The sample uses bounded retries and exposes registry
delivery state, but it does not claim a distributed transaction. A production RHOAI
design needs to decide:

- fail-open or fail-closed for workload execution;
- transactional outbox or durable broker;
- retry and dead-letter behavior;
- duplicate/idempotency key;
- replay authorization;
- stale `START` reconciliation;
- authoritative status for KFP, SparkApplication, DCH, and storage.

These choices affect whether users can trust a graph that appears complete.

### 6.9 Retention and lifecycle are absent as a product policy

The ADR correctly warns that graph history depends on retention, but does not state a
retention profile. The Feast proposal currently describes pruning raw events and run
detail while preserving current graph state, with no cold archive. The sample's
Marquez/Postgres deployment has different behavior because it does not configure
`dbRetention`.

The ADR needs a retention matrix for:

- raw OpenLineage events;
- run/event detail;
- graph edges and symlinks;
- Data Registry revisions;
- schemas and schema-discovery results;
- physical evidence/manifests;
- derived artifacts;
- deletion, backup, restore, and legal hold.

It should state whether an edge without retained source events is still shown and how
that limitation is communicated.

### 6.10 The user experience is intentionally absent, but the architecture cannot be
      user-neutral

The ADR treats UI as outside scope, and the RHOAI strategy separates backend and
dashboard work. That separation is useful for delivery, but the architecture still
needs an end-to-end UX contract.

At minimum, the user should be able to:

- open a Data Registry asset and view lineage without manually translating UUIDs;
- distinguish Feast-native feature lineage from broader OpenLineage lineage;
- see registry metadata beside graph nodes;
- navigate from a physical source to its governed asset and back;
- inspect run attempts and authoritative status;
- see revision/evidence level and retention age;
- understand missing links, uninstrumented writers, and authorization filtering;
- navigate to query-time traces when they exist;
- receive stable empty, loading, partial, and error states.

The ADR should define one shared lineage service and two entry points—Data Registry
asset-centric and Feature Store feature-centric—rather than allowing independent
views to evolve incompatible identity and semantics.

### 6.11 Schema discovery needs its own correctness contract

The detailed ADR scenario is an improvement, but it still needs to decide:

- synchronous versus asynchronous behavior for slow connectors;
- timeout and retry semantics;
- whether discovery is a snapshot, a revision, or current mutable state;
- what schema fields are guaranteed;
- how nullability, constraints, defaults, nested fields, and statistics are modeled;
- how schema drift is detected and surfaced;
- whether a failed discovery changes the asset state;
- how DCH connection credentials are referenced without appearing in lineage.

The draft says the current Data Registry schema model may need extension. That is a
decision dependency, not merely a future enhancement, because schema is part of the
lineage value proposition.

### 6.12 Acceptance criteria and measurable end state are missing

The ADR should have a testable acceptance matrix. A minimum set is:

| Scenario | Required outcome |
|---|---|
| Register S3-backed asset | Registry persists asset; DatasetEvent and symlink are emitted exactly once per idempotency key. |
| DCH reads only physical S3 identity | Feast graph connects the physical input to the registry asset. |
| DCH emits registry output | Upstream and downstream edges are connected to the same logical/revision identity. |
| KFP orchestrates DCH, Spark, embedding | Root/child run hierarchy and dataset edges are both queryable. |
| Retry after failure | New attempt is visible; prior failure remains truthful. |
| Native Spark emits conflicting lifecycle states | Derived status follows documented authority; raw events remain available. |
| Direct storage overwrite | No false event is invented; UI explains the instrumentation boundary. |
| Unauthorized producer/reader | Write or read is rejected/filtered, including physical aliases and facets. |
| Retention/pruning | Graph behavior and historical limitations match policy. |
| Feast unavailable | Producer behavior, outbox/retry, and recovery are deterministic. |
| Schema discovery failure/drift | Asset and revision state are explicit and user-visible. |
| Cross-cluster/project identities | No accidental merge or unexplainable fragmentation. |

## 7. Proposed ADR structure

The draft would become substantially stronger with this structure:

1. **Context and problem statement**
   - user/persona;
   - business questions;
   - operational versus data lineage;
   - current sample-app evidence and limitations.
2. **Decision**
   - component responsibilities;
   - Feast consumer role;
   - Data Registry and DCH ownership;
   - API/UI boundary;
   - explicit non-goals.
3. **Identity and event contract**
   - canonical IDs, symlinks, revisions, event catalog, facets, lifecycle, retries.
4. **Security and authorization**
   - producer, user, namespace, physical URI, connection, and audit policy.
5. **Retention and evidence**
   - hot events, graph state, archive, revision/evidence, deletion.
6. **End-state architecture**
   - Data Registry, DCH, Feast, RHOAI API/BFF, dashboards, MLflow/OTel,
     storage/table evidence.
7. **User journeys and value**
   - asset, impact, failure, revision, and partiality experiences.
8. **Phasing and acceptance criteria**
   - TP, GA, and later extensions.
9. **Alternatives and consequences**
   - Marquez, Feast, a dedicated lineage service, and hybrid options.

## 8. Immediate issues visible in the current project/sample

The sample is valuable precisely because it exposes boundaries that the ADR should
not hide. These are the obvious issues to carry into the architecture work.

### 8.1 Namespace convention is not yet a RHOAI-wide contract

The sample uses a cluster-qualified Data Registry namespace, while the ADR's example
uses the RHOAI namespace. Either can be defensible, but emitted history cannot be
renamed safely later. Decide authority, portability, and authorization semantics now.

### 8.2 The Registry is a stub, not a production outbox

The sample persists before delivery and exposes pending status, which is the right
shape. Its ConfigMap-backed implementation is intentionally not a transactional
database or an outbox. The real Data Registry must provide durable delivery,
idempotency, replay, and a clear failure state.

### 8.3 Mutable raw storage remains the largest evidence gap

The sample explicitly demonstrates that a direct S3 overwrite produces no event and
that the next run can read different bytes under the same identity. This is not a
Marquez or Feast problem. The end-state must decide whether RHOAI adds observed
evidence, immutable revisions, storage events, or simply presents the limitation.

### 8.4 Manual revisions are useful but not immutable versions

The existing event catalog correctly warns that a manually incremented registry
version does not prove bytes changed or identify the bytes consumed. The ADR should
retain that distinction and avoid calling a revision a `DatasetVersionFacet` unless
its semantics are genuinely immutable.

### 8.5 Native Spark lifecycle needs a product-level authority

The live sample found a possible native `FAIL` followed by `COMPLETE` for one SQL
run. The sample uses KFP and SparkApplication state as authoritative. RHOAI needs a
common status/reconciliation policy before a generic backend is allowed to present
one green/red status as fact.

### 8.6 Root context propagation is fragile unless platformized

The sample had to work around a KFP placeholder behavior by reading a live pod label.
That is a useful implementation discovery, but it shows why asset and root-run
context should be supplied through supported platform integration rather than copied
into every custom component.

### 8.7 Event ownership must remain native where possible

The sample deliberately does not emit a manual Spark event from the Spark submitter;
the native listener owns Spark I/O. The ADR should make “one observed operation, one
owner” a conformance rule to avoid duplicate or contradictory lineage.

### 8.8 The sample's assurance claim should be carried into the product

The sample calls itself **Linked**, not **Observed** or **Reproducible**. If RHOAI
uses more confident language in the UI, it will overstate what the backend proves.

### 8.9 Marquez-specific verification needs a Feast equivalent

The sample's event catalog and verifier know Marquez API behavior, including the
distinction between RunEvent history and DatasetEvent/entity inspection. Replacing
Marquez requires equivalent Feast API and graph assertions, not merely changing the
endpoint URL. The verifier should compare semantics: connected identities, parents,
facets, statuses, retention, and authorization.

### 8.10 Query-time lineage remains a conscious gap

The sample's operational graph and the separate MLflow/query-tracing story should be
documented as separate but linkable planes. Otherwise future feature retrieval or
serving work will either overload OpenLineage or leave users with an unexplained gap.

## 9. Phased end state

### TP: connected operational lineage

- Feast consumer/server receives external OpenLineage.
- Data Registry emits logical asset DatasetEvents with symlinks.
- DCH, KFP, and native Spark emit owned runtime events.
- Exact identity and parent/run contracts are tested.
- RHOAI authorization and basic retention are enforced.
- Feast graph and generic APIs are available.
- UI clearly says linked operational lineage and shows partiality.

### GA: supported RHOAI lineage product

- RHOAI lineage API/BFF joins Feast with Data Registry.
- Asset-centric and Feature Store-centric entry points share a graph contract.
- Revisions, evidence levels, retention, replay, reconciliation, and audit are
  documented and supported.
- Separate lineage-server/database topology is benchmarked and operable.
- User journeys and negative authorization tests pass.

### Later: stronger provenance and runtime correlation

- observed source evidence and immutable snapshots where product requirements justify
  them;
- safe alias/re-correlation service;
- storage audit/event integration;
- richer MLflow/OTel cross-links;
- document-level or column-level evidence where required;
- historical/cold archive and point-in-time impact queries.

## 10. Final assessment

The ADR is directionally correct but currently describes an integration mechanism,
not a sufficiently defined RHOAI lineage capability. Its strongest ideas—stable
asset identity, explicit symlinks, DCH observation, independent event emission, and
credential sanitization—should be retained.

Before approval, it should add a crisp use case, define operational lineage versus
data provenance, name Feast's role in the end state, specify the shared identity and
event contracts, define authorization/retention/reconciliation, and describe the
user experience across Data Registry and Feature Store.

The clearest end state is:

> Data Registry owns governed asset identity and evidence; DCH and runtime systems
> report the work they observe through OpenLineage; Feast stores and queries the
> operational graph; an RHOAI lineage service joins that graph to Registry metadata,
> authorization, revisions, and evidence; MLflow/OTel remains the query/runtime
> trace plane. The product presents a linked operational path by default and only
> claims observed or reproducible provenance when the corresponding evidence exists.

That gives the user a useful answer, gives each component a defensible authority,
and leaves room to extend Feast without pretending that a feature-store consumer is
the whole RHOAI lineage architecture.

