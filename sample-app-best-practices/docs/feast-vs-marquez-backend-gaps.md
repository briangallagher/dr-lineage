# Feast as the RHOAI OpenLineage backend: gaps versus Marquez

Status: working architecture analysis, 2026-09-11

This document assesses the proposal to use Feast's OpenLineage consumer as the
backend for RHOAI lineage, replacing the Marquez deployment used by the sample
application. It focuses on the backend and the contracts around it. Feast UI
details are included only where they affect the backend or the end-to-end user
experience.

The conclusion is deliberately qualified:

> Feast is a credible OpenLineage ingestion and graph component for RHOAI
> operational lineage, but it is not a drop-in semantic replacement for Marquez
> and it is not, by itself, a Data Registry lineage product.

The replacement can work if RHOAI treats Feast as one part of a lineage service:
the Data Registry remains authoritative for asset identity, metadata, locations,
and revisions; instrumented runtime components emit OpenLineage; Feast consumes
and queries those events; and a shared RHOAI API/UI turns the graph into answers
about governed assets. Several additional contracts and probably a small number
of Feast extensions are required before that is a safe architectural assumption.

## 1. Evidence and scope

The analysis combines four sources of evidence:

1. The sample application's implementation and design documents, especially
   [OpenLineage Design and Best-Practices Guide](openlineage-design.md),
   [Event Catalog](event-catalog.md), and
   [Code Deep Dive](code-deep-dive.md).
2. The RHOAI strategy and engineering trail for
   [RHAISTRAT-2335](https://redhat.atlassian.net/browse/RHAISTRAT-2335), its
   related [Feature Store backend RFE](https://redhat.atlassian.net/browse/RHAIRFE-2744),
   [Feature Store dashboard RFE](https://redhat.atlassian.net/browse/RHAIRFE-2745),
   and the resolved engineering epic
   [RHOAIENG-79659](https://redhat.atlassian.net/browse/RHOAIENG-79659).
3. The internal questions and maintainer responses:
   [questions from Ana](https://gitlab.cee.redhat.com/data-strategy/data-arch-design-options/-/blob/main/docs/feast-considerations/feast-openlineage-consumer-questions.md)
   and [responses from Nikhil](https://gitlab.cee.redhat.com/data-strategy/data-arch-design-options/-/blob/main/docs/feast-considerations/feast-openlineage-consumer-responses.md?ref_type=heads).
4. Upstream Feast and OpenLineage documentation, including the
   [Feast OpenLineage reference](https://github.com/feast-dev/feast/blob/master/docs/reference/openlineage.md),
   [Feast operator API](https://github.com/feast-dev/feast/blob/master/infra/feast-operator/docs/api/markdown/ref.md),
   [OpenLineage's Feast integration page](https://openlineage.io/docs/integrations/feast/),
   [Feast issue #5882](https://github.com/feast-dev/feast/issues/5882), and
   [Feast releases](https://github.com/feast-dev/feast/releases).

The upstream documentation describes current Feast `master` behavior. RHOAI's
3.6 TP is a pinned, qualified distribution and may expose less. Every capability
described below as “current Feast” must therefore be checked against the exact
RHOAI image, operator, CRD, and dashboard build before it becomes a product
contract.

## 2. What RHOAI is proposing

RHAISTRAT-2335 is a TP integration of Feast's existing OpenLineage consumer. The
strategy is not proposing to make Feast the universal owner of all RHOAI metadata.
The intended backend behavior is approximately:

```text
Airflow / Spark / dbt / DCH / KFP / other OL producers
                          |
                          | POST /api/v1/lineage or /batch
                          v
             Feast OpenLineage consumer
                          |
              SQL event + graph tables
                          |
              graph/query REST endpoints
                          v
        Feature Store / future RHOAI lineage views
```

Feast's producer continues to emit lineage for Feast operations. The consumer can
receive `RunEvent`, `DatasetEvent`, and `JobEvent`, and the current API exposes
event, job, dataset, run, run-detail, namespace, and graph queries. The consumer
stores a raw/event history model alongside graph tables. The operator supports an
embedded consumer and a separate lineage-server deployment, with optional
separate SQL connectivity.

The engineering trail also makes the current boundary clear:

- RHAISTRAT-2335 focuses on backend hardening, RBAC wiring, operator support,
  producer documentation, qualification, and a modest performance target.
- RHAIRFE-2744 describes accepting external OpenLineage but explicitly excludes
  the dashboard experience.
- RHAIRFE-2745 treats the dashboard as a separate feature. Backend consumption
  without a deliberate user view is acknowledged as insufficient customer value.
- The engineering epic includes separate work for a Feast lineage server,
  discovery ConfigMap, retention, producer enhancements, and a Feast UI graph.
  That should not be confused with a complete RHOAI-wide Data Registry lineage
  experience.

This is an important architectural distinction: “Feast can consume OpenLineage”
is a backend capability; “a user can understand the history and impact of a Data
Registry asset” is the product outcome still needing a contract.

## 3. Baseline: what the sample application actually requires

The sample deliberately defines a narrow but useful operational lineage model. Its
requirements are stronger than simply accepting JSON:

| Concern | Sample-app contract | Why it matters to a replacement backend |
|---|---|---|
| Logical identity | A registry asset is a stable `(namespace, name)` pair, with the asset UUID as the name. | A Feast graph must preserve the logical node and not reduce it to a physical URI. |
| Physical bridge | The registry `DatasetEvent` contains a standard `SymlinksDatasetFacet` from the registry asset to the S3 identity. | Feast must index or resolve the symlink, not merely retain the facet JSON. |
| Event ownership | Registry emits design-time `DatasetEvent`; DCH/ingestion, native Spark, and embedding emit runtime `RunEvent`. | The backend must accept mixed producers without inventing work that it did not observe. |
| Execution hierarchy | Child runs use `ParentRunFacet` to point to the KFP root. | Dataset joins and execution joins must remain distinct. |
| Identity equality | Dataset edges are formed by exact `(namespace, name)` equality. | Producer naming is an API contract; fuzzy matching is unsafe. |
| Lifecycle | Finite work uses `START` then exactly one terminal state, subject to real integration anomalies. | The backend must preserve failures and handle duplicate or contradictory events explicitly. |
| Retry semantics | Each attempt gets a new run UUID and unique output path. | A retry must not overwrite a prior attempt or rewrite a failed run into success. |
| Evidence boundary | The normal scenario is **Linked**, not **Observed** or **Reproducible**. | A graph must not imply immutable bytes, source completeness, or audit-grade reproducibility. |
| Direct writes | Storage changes outside instrumented producers are invisible. | Product messaging and completeness indicators must show this boundary. |
| User questions | Provenance, downstream impact, ownership, failure diagnosis, and component interaction. | Query/API design must expose business answers, not only nodes and edges. |

The full assumptions are in [openlineage-design.md](openlineage-design.md). A
backend is suitable only if these semantics either remain true or are consciously
changed and documented.

## 4. The central difference: Feast is not Marquez with a different URL

Marquez is a general OpenLineage metadata service. In the sample it is the
destination for events from the registry, KFP, DCH/ingestion, native Spark, and
embedding components. Its most important role is to materialize a graph from
generic OpenLineage identities and facets.

Feast is a feature store with an OpenLineage producer and a consumer layered into
its registry/API architecture. That gives it useful RHOAI advantages—an existing
operator, SQL persistence, Feast-native lineage, namespace-aware feature-store
context, and a path to one deployed service—but it also creates different defaults
and product gravity:

- Feast's registry and feature concepts are first-class; arbitrary RHOAI assets are
  external inputs to the consumer.
- Feast's graph can join independent producers at common datasets, but it does not
  automatically understand Data Registry asset ownership, revisions, connection
  references, or RHOAI business semantics.
- Feast's current retention model preserves current graph state while pruning raw
  event/run detail by default. That is not equivalent to retaining a complete
  historical operational record.
- Feast's namespace filtering is an application-layer read policy and its external
  producer write model is not the same as Kubernetes/RHOAI authorization.
- Feast's query-time feature retrieval lineage is intentionally separate from the
  OpenLineage consumer. That is correct technically, but it means RHOAI must define
  how operational lineage, training lineage, and serving/query traces relate.

The migration question is therefore not “does Feast accept OpenLineage?” It is:

> Can Feast preserve the RHOAI identity, lifecycle, evidence, authorization,
> retention, and query contracts that the sample has made explicit, while
> remaining a useful native Feature Store backend?

## 5. Gap summary

The following table is the short version. “Priority” means architectural priority,
not a Jira severity.

| Priority | Gap when moving from the sample's Marquez backend to Feast | Design impact | Remediation |
|---|---|---|---|
| P0 | No complete RHOAI identity contract | A Feast graph can contain disconnected registry, S3, DCH, and KFP nodes. | Publish canonical namespace/name rules, required symlink behavior, URI normalization, and conformance fixtures. |
| P0 | Registry-to-physical linkage depends on producer facets | If Data Registry does not emit the symlink, Feast cannot infer the relationship. | Make a registry `DatasetEvent` with `SymlinksDatasetFacet` a production contract; test it through Feast queries. |
| P0 | Operational lineage and data/version provenance are conflated easily | Users may read a current graph as proof of historical bytes or reproducibility. | Add explicit assurance levels, revision/evidence semantics, and UI/API labels. |
| P0 | Authorization is not automatically RHOAI authorization | An authenticated producer may be able to claim a namespace; graph reads may cross project boundaries if filtering is incomplete. | Bind producer identity and namespace permissions to RHOAI/Kubernetes policy; test negative cases and cross-project graphs. |
| P1 | Run hierarchy is not the same as data correlation | Dataset convergence may show data flow but lose a coherent user-level pipeline story; parent facets may be absent or inconsistent. | Require root context propagation and expose both data edges and execution hierarchy. |
| P1 | Retention prunes event detail and has no cold archive | Current graph state can survive after the evidence needed to explain it has gone. | Define raw-event, run-detail, graph, and archive retention separately; add export/archive if historical audit questions matter. |
| P1 | No documented durable delivery/reconciliation contract | Synchronous HTTP failure can result in missing or delayed lineage; producer behavior may differ. | Use outbox/broker or bounded retry policy, dead letters, reconciliation, idempotent ingestion, and completeness metrics. |
| P1 | Feast API is generic rather than Data Registry-aware | A graph endpoint alone does not answer “what is this asset?” or provide launch context. | Add a shared RHOAI lineage API/BFF that joins Feast graph data to Data Registry metadata and authorization. |
| P1 | Version/revision semantics are absent from the TP | A stable asset UUID is not a stable content version; mutable S3 can merge unrelated observations. | Add registry revision identities and evidence facets, then carry revision IDs through producers. |
| P1 | Feast and RHOAI component version compatibility is unspecified | Facet support, endpoints, schema, and operator fields can differ between upstream and the RHOAI pin. | Create a versioned conformance suite and record exact supported OpenLineage/Feast/operator versions. |
| P2 | Re-correlation and alias management are deferred | An event arriving before its matching symlink remains disconnected; renamed/legacy IDs need manual repair. | Add a safe alias/mapping service or re-correlation job with auditability and collision checks. |
| P2 | Query-time lineage is out of scope | MLflow/OTel traces and operational graphs may remain disconnected in the user's mental model. | Define an explicit cross-plane model and links, even if query-time lineage is a later phase. |
| P2 | Consumer database migrations and pool behavior are immature | Operator upgrades and high-volume deployments may be operationally fragile. | Use versioned migrations, backup/restore tests, pool configuration, and schema compatibility policy. |

The remainder of this document explains these gaps and actionable responses.

## 6. Detailed gaps and remediation actions

### 6.1 Identity is exact; Feast does not infer intent

The Feast consumer uses exact `(namespace, name)` identity. Its secondary bridge is
the standard symlink facet and, where applicable, a data-source URI. It does not
provide a general normalization or alias-transformation layer. This is consistent
with OpenLineage and is the right safety default: silently treating similar strings
as the same dataset can create false provenance.

The sample currently uses identities like:

```text
dataregistry://<cluster>/<project>, <asset-uuid>
s3://sample-data, raw/documents.csv
```

The draft RHOAI ADR proposes `dataregistry://{rhai-namespace}` with the asset UUID
as the name, while the sample includes the cluster in its namespace. These are not
the same contract. A cluster-qualified namespace provides isolation but fragments
history when an asset moves between clusters. A project-qualified namespace is more
portable but must still be globally unambiguous for the graph and authorization
model.

Actions:

1. Decide whether a Data Registry namespace is cluster-qualified, project-qualified,
   or an authority URI independent of deployment location.
2. Define escaping, case, URL encoding, trailing slash, host aliases, S3 versus
   S3A/S3N, database host normalization, and table/path granularity.
3. Make the logical asset UUID the stable name. Do not use display names or mutable
   locations as the canonical registry identity.
4. Require the registry to publish a `SymlinksDatasetFacet` for every known physical
   identifier. Treat `dataSource.uri` as supporting metadata, not a substitute for
   an explicit logical alias when the identity is otherwise ambiguous.
5. Add producer conformance fixtures: a registry event, a DCH input, a native Spark
   input, and a downstream output must produce the expected connected graph in
   Feast.
6. Define a governed migration procedure before changing any emitted namespace.

### 6.2 The registry must emit the bridge, and Feast must prove it indexed it

Nikhil's response to the internal questions is directionally right: for a Registry
asset to converge with an S3 object, the Registry has to emit a symlink or equivalent
URI relationship. Feast cannot discover the relationship from the fact that a UUID
and an S3 path happen to appear in different events.

The sample already models the correct event ownership. Registration persists the
asset, then emits a `DatasetEvent` whose logical dataset is the registry UUID and
whose symlink points to the raw S3 dataset. Ingestion reports the S3 identity it
actually reads. This avoids pretending that the registry performed a read.

The gap is not conceptual; it is end-to-end qualification. The RHOAI implementation
must demonstrate that Feast's consumer:

- accepts the `DatasetEvent`;
- stores the symlink relationship in its dataset-symlink model;
- connects an independently emitted S3 input to the registry node;
- retains the display/ownership facets needed by RHOAI; and
- applies authorization consistently to both sides of the alias.

Remediation: make this a release acceptance test, not only a unit test of event
parsing. The test should start with a registry asset, emit a DCH/Spark event using
only the physical identity, and query a graph centered on either identity.

### 6.3 Dataset convergence does not replace execution correlation

Feast's consumer connects producers when their dataset inputs and outputs converge.
It stores a `ParentRunFacet` when one is present, but cross-producer lineage does not
require a common run-level parent. That is useful for heterogeneous tools, but it
does not automatically produce a coherent “one user pipeline execution” view.

The sample intentionally maintains both relationships:

```text
dataset output == dataset input       -> data dependency
ParentRunFacet(child -> KFP root)     -> orchestration hierarchy
```

If a DCH job emits the correct physical input and registry-linked output but loses
the KFP root context, the data graph can still be connected while the execution story
is incomplete. Conversely, sharing a `pipeline_run_id` without matching datasets
does not create a data edge.

Actions:

- Define a required root-context envelope for KFP, DCH, Spark, notebooks, and future
  component integrations.
- Preserve `ParentRunFacet` where an actual parent exists; do not invent a parent
  merely to force a graph shape.
- Make the shared RHOAI API expose data lineage and execution hierarchy separately,
  with a clear visual relationship.
- Add tests for: data-connected but parent-missing, parent-connected but data-missing,
  retry children, and cross-component jobs with different job namespaces.

### 6.4 Stable asset identity is not a data version

Both the sample and the Feast responses leave a major product risk untouched:
OpenLineage dataset identity is not immutable content identity. A registry UUID can
remain stable while an S3 object is overwritten. Feast can preserve the reported
event and graph, but it cannot manufacture evidence of which bytes were read.

The sample calls this assurance level **Linked**. It explicitly avoids emitting a
`DatasetVersionFacet` for a mutable source and documents a ladder from pointer and
metadata revision through ETag/hash/manifest, object version, retained copy, or table
snapshot.

Actions:

1. Add a Data Registry revision model distinct from the stable asset UUID.
2. Give each revision a stable identity that can be used in OpenLineage inputs and
   outputs, for example `asset-uuid@revision`, without renaming the stable asset.
3. Define what makes a revision `LINKED`, `OBSERVED`, or `REPRODUCIBLE`.
4. Carry revision and evidence references in a versioned RHOAI facet or standard
   facet where the standard semantics genuinely fit.
5. Decide whether the Data Registry, DCH, object store, or table format owns the
   authoritative evidence.
6. Make the UI and API state “known operational path” versus “verified source
   evidence” explicitly.

This is an extension around Feast, not necessarily a Feast core change. The Feast
consumer needs to preserve and expose the facet; the Registry and producers need to
define its meaning.

### 6.5 Lifecycle and status conflicts need an explicit policy

The sample found a real integration edge: a failed Spark write can result in native
`FAIL` followed by `COMPLETE` for the same SQL run, with no useful native error
facet. KFP and `SparkApplication` status are authoritative for the sample's final
outcome, while Spark driver logs provide detail.

Feast's consumer can store lifecycle events, but a generic consumer is not the same
thing as a RHOAI reconciliation authority. The design needs to state what a user sees
when events disagree, arrive late, are duplicated, or never receive a terminal state.

Actions:

- Define a per-run state machine and precedence rules for terminal events.
- Preserve the raw events and expose the derived status and its authority.
- Identify authoritative status sources for KFP, SparkApplication, DCH, notebooks,
  and feature-store operations.
- Never rewrite a failed retry into success; represent each attempt separately.
- Add a reconciliation worker for stale `START` runs and known platform resources.
- Add a “status confidence/authority” or equivalent API field; do not hide ambiguity.

If Feast cannot represent the derived status without losing raw evidence, add a
small RHOAI projection rather than overloading the generic event tables.

### 6.6 Event delivery is synchronous, and durability is a product decision

The Feast consumer accepts HTTP events and a batch endpoint. The responses describe
synchronous processing, SQL upserts, and a single transaction for a batch. This is
adequate for a small-to-medium TP deployment, but it is not a durable event bus.

The sample emitter uses bounded retries and makes delivery failure visible; the
registry persists the asset first and marks delivery state. That still is not a
distributed transaction among the Registry database, S3, KFP, and Marquez. Moving to
Feast does not remove that problem.

Actions:

1. State whether a producer is allowed to fail a workload when lineage delivery is
   unavailable, or whether lineage is fail-open with an outbox.
2. Define at-least-once delivery, duplicate keys, retry age, and batch partial
   failure behavior.
3. Add a transactional outbox beside authoritative Registry mutations.
4. For runtime producers, use durable retry/dead-letter handling where lineage
   completeness is material.
5. Add completeness metrics: events emitted, accepted, rejected, delayed, retried,
   dead-lettered, and reconciled.
6. Provide an operator-visible way to replay an event safely.

The API should not imply exactly-once semantics merely because SQL upserts make a
duplicate request harmless.

### 6.7 Feast's authorization model needs a RHOAI binding

The strategy and maintainer response describe API-key authentication for external
producer writes and OIDC/Kubernetes-bearer authentication for queries. Namespace
filtering is applied in application SQL using Feast permissions and
`namespace_mapping`; the response explicitly says this is not database row-level
security. It also says an authenticated producer may send any namespace unless an
additional policy is applied.

That is materially different from the Data Registry's project-scoped RHOAI/Kubernetes
authorization model. A valid producer credential must not automatically grant the
right to assert lineage for another project, and a graph query must not reveal
physical paths, connection names, owners, or facets from an unauthorized project.

Actions:

- Define separate write and read authorization contracts.
- Bind producer identity to allowed project/namespace scopes, not only to a shared
  API key.
- Reuse Data Registry authorization decisions or an equivalent RHOAI policy layer
  for asset nodes and their aliases.
- Define cross-project behavior for shared physical sources and shared downstream
  outputs.
- Filter symlinks, job facets, error messages, and physical URIs as well as nodes.
- Test namespace spoofing, cross-project parent facets, invalid API keys, expired
  bearer tokens, and query result pagination.
- Audit who submitted, queried, replayed, or deleted lineage.

This may require a RHOAI gateway/BFF even if Feast keeps its own API-key and
namespace-mapping mechanisms for native use.

### 6.8 Retention preserves topology, not necessarily history

Current Feast documentation describes a default raw-event/run retention of 30 days,
with periodic pruning. The graph tables—jobs, datasets, edges, and symlinks—are kept
as current topology. The responses say there is no cold archive and that retention
and TTL were deferred after TP.

That is a different failure mode from a Marquez deployment where PostgreSQL may retain
the sample's history until teardown because no `dbRetention` was configured. Neither
default is automatically correct, but they produce different answers to “what
happened last quarter?” and “why does this edge exist?”

Actions:

1. Specify retention separately for raw events, run detail, graph state, registry
   revisions, audit records, and physical evidence.
2. Decide whether graph edges may outlive the events that justify them and how the
   API labels that condition.
3. Provide export/cold archive if regulatory, incident, or historical impact
   analysis needs outlive the hot store.
4. Define deletion and legal-hold semantics, including aliases and facets containing
   personal or sensitive information.
5. Test restart, pruning, backup, restore, and graph behavior after raw-event expiry.

Without this, Feast may answer “what is connected now?” while users believe they are
seeing “what was reported at time T.”

### 6.9 Re-correlation and alias repair are deliberately deferred

The maintainer response says events are correlated at ingest. If a source event arrives
before a matching symlink or registry event, it remains disconnected until a later
event references both. The schema can support a symlink relationship, but there is no
automatic re-correlation loop. A mapping/alias table was also described as a post-GA
idea.

This is acceptable for a controlled producer rollout, but it conflicts with a
multi-component platform where producers restart independently or are upgraded in
different orders.

Actions:

- Make the registry identity event/outbox available before runtime events where
  possible.
- Add a re-correlation job that is deterministic, audited, collision-safe, and
  bounded; never merge datasets solely because names look similar.
- Support an explicit, user-approved alias/mapping record with source, target,
  reason, author, timestamp, and expiry/review status.
- Expose “unlinked physical dataset” as a visible condition rather than silently
  presenting a partial graph.

### 6.10 Storage, migrations, and deployment topology are not yet production contracts

Feast supports an embedded consumer and a separate lineage server. A separate server
is the safer default for meaningful external volume because it isolates ingestion and
query workload from the feature registry API, but the operator commonly shares the
SQL database unless configured otherwise. The responses say no connection-pool
settings are currently exposed and that schema setup uses idempotent `create_all()`;
destructive migrations are a future Alembic concern.

The strategy's 50 events/sec target is useful as a TP floor, not a capacity model for
all RHOAI deployments. Event volume can spike during backfills, Airflow fan-out,
Spark SQL execution, or retries. Query latency and write throughput must be measured
with realistic facet sizes and tenant counts.

Actions:

- Deploy a separate lineage server for RHOAI production profiles unless a measured
  small deployment intentionally embeds it.
- Decide whether lineage has a separate database/schema/connection pool from the
  feature registry.
- Add versioned, non-destructive migrations and an upgrade/rollback test matrix.
- Measure single-event and batch throughput, p95/p99 query latency, facet size,
  concurrent tenants, pruning load, and restart behavior.
- Define back-pressure and resource limits; do not let a lineage spike take down
  feature registration or serving.
- Publish the supported Feast/operator/OpenLineage version matrix.

### 6.11 Feast's API is not yet the RHOAI business API

The current endpoints are valuable primitives: graph, centered graph, namespaces,
jobs, datasets, events, runs, and run details. They do not by themselves provide:

- Data Registry asset metadata and ownership;
- a stable asset-detail launch context;
- revision/evidence state;
- a consistent RHOAI authorization decision;
- source connection display without leaking credentials;
- a distinction between Feast-native, external operational, and query-time lineage;
- a clear indication that an edge is inferred from a symlink; or
- a user-friendly explanation of missing instrumentation and retention gaps.

Actions:

1. Define a RHOAI lineage API/BFF contract that composes Data Registry and Feast.
2. Use stable resource IDs, pagination, depth limits, filters, and explicit partial
   result/error states.
3. Include source and authority metadata in graph responses.
4. Provide asset-centric queries: upstream, downstream, runs, revisions, owners,
   and evidence level.
5. Keep the Feast generic API available for native users, but do not make every RHOAI
   client understand Feast's internal tables or namespace mapping.

### 6.12 Query-time lineage is a separate plane

The internal response explicitly places query-time serving lineage outside the
OpenLineage consumer and points toward metrics/OTel. Feast's producer/consumer work
also does not make MLflow training/evaluation lineage part of the same graph.

That is a sensible separation. It becomes a design gap only if the product presents
all of these as one undifferentiated “lineage” feature. RHOAI should name the planes:

- **Operational lineage:** reported inputs, outputs, jobs, runs, lifecycle, and
  execution hierarchy. Feast/OpenLineage is the candidate backend.
- **Data provenance/evidence:** registry revisions, observed object/table evidence,
  manifests, snapshots, and retention. Data Registry and storage/table systems own
  this.
- **Query/runtime trace:** feature retrieval, model serving, evaluation, and request
  traces. MLflow/OTel or another trace system owns this.

Actions: define stable cross-links between planes (asset ID, model ID, run ID, trace
ID) without pretending they have the same event semantics. The user should be able
to move from an operational edge to a feature retrieval or model run when available,
and see “not instrumented” when it is not.

### 6.13 Custom and standard facets need compatibility governance

The sample intentionally prefers standard facets and currently uses OpenLineage
1.53.0 with Marquez 0.50. Feast's dependency and RHOAI pin may use a different
OpenLineage Python version and a different set of supported facets. The TP strategy
does not need custom facets, but RHOAI's longer-term needs include asset revisions,
connection context, display paths, evidence levels, and possibly source references.

Actions:

- Maintain a facet compatibility table for the exact supported builds.
- Treat unknown facets as preserved payload first and indexed/queryable only when
  explicitly supported.
- Version any RHOAI facet schema and define redaction rules.
- Do not encode credentials, bearer tokens, signed URLs, arbitrary environment
  variables, or large payloads in facets.
- Keep standard `SymlinksDatasetFacet`, `ParentRunFacet`, schema, statistics, and
  column-lineage semantics intact for external producers.

### 6.14 Data Registry and DCH ownership must remain observable

The ADR and sample agree on an important boundary: Data Registry owns the governed
asset identity; DCH owns the ingestion event because it observes the remote read.
The registry should orchestrate schema discovery but should not pretend that it
performed the introspection.

The open question is how real RHOAI components obtain and propagate `asset_id`,
revision, project, and root run context. If every customer pipeline must hand-build
these values, adoption and correctness will be poor. If the platform silently derives
them, ownership and evidence become ambiguous.

Actions:

- Provide reusable producer libraries and KFP/DCH integration templates.
- Define how a pipeline selects a registry asset and how that selection becomes an
  OpenLineage dataset identity.
- Define how DCH carries connection ID without exposing credentials.
- Make native Spark, KFP, and future notebook integrations use the same context
  envelope.
- Document what happens for pipelines that use unregistered data or write directly
  to storage.

## 7. Thoughts on Ana's questions and Nikhil's responses

The questions are well targeted: they identify identity convergence, event types,
run correlation, scale, deployment isolation, retention, storage, RBAC, query-time
lineage, and migration as the real architectural issues. They go substantially
beyond “does the API accept a RunEvent?”

The responses are strongest on the OpenLineage mechanics:

| Topic | Maintainer response | Assessment |
|---|---|---|
| Event types | Consumer accepts `RunEvent`, `DatasetEvent`, and `JobEvent`. | Good coverage. Release-test the exact RHOAI build and preserve event type semantics in queries. |
| Registry to S3 | Use `SymlinksDatasetFacet` or a data-source URI. | Correct and directly compatible with the sample. Make the symlink mandatory for the registry bridge. |
| Naming | Exact `(namespace, name)`; no general mapping layer. | Correct safety default. It increases the importance of a RHOAI naming contract and a later alias workflow. |
| Cross-producer graph | Joins at dataset convergence; parent facet is optional. | Technically sound but insufficient for a user-level execution view unless context propagation is also required. |
| API | REST ingestion, batch ingestion, graph and entity queries. | Good primitive surface; not an asset-centric RHOAI contract. |
| Scale | Suitable for small/medium; separate deployment for higher volume; benchmarks pending. | Treat 50 events/sec as a TP acceptance target, not an architecture limit. Benchmark before committing to shared production topology. |
| Retention | Default 30 days for raw event/run detail; current graph remains; no cold archive. | This is a material product decision, not an implementation detail. It needs RHOAI policy and user-visible semantics. |
| Database | Shared by default; separate connection supported; `create_all()` startup; future Alembic. | Suitable for TP experimentation, but migration and isolation are not yet a mature platform contract. |
| RBAC | API-key writes; query filtering by namespace mapping in application SQL; no DB RLS. | Must be reconciled with RHOAI project authorization and producer identity. |
| Re-correlation | Deferred; events are linked at ingest. | Fine for an initial controlled rollout, risky for asynchronous multi-team producers. |
| Query-time lineage | Out of scope; use metrics/OTel for serving. | Correct separation, but RHOAI must explain the cross-plane UX. |
| Upstream contribution | Generic SQL registry implementation; no RHOAI-specific path. | Healthy upstream posture. RHOAI-specific identity, auth, retention, and UX should remain integration layers unless broadly useful. |

The responses should be treated as an implementation snapshot and maintainer intent,
not as proof that every requirement is solved. The phrases “in progress,” “pending
benchmarks,” “post-GA,” and “out of scope” are the most important parts of the
answers.

## 8. Recommended target contract for the Feast migration

Use Feast for the generic operational lineage store, but put a RHOAI contract around
it:

```text
Data Registry
  authoritative asset/revision/metadata/location
  emits DatasetEvent + SymlinksDatasetFacet
             |
DCH / KFP / Spark / notebooks / feature operations
  emit RunEvent and JobEvent with canonical identities
             |
      Feast OpenLineage consumer/server
  raw events + graph + retention + generic queries
             |
       RHOAI lineage API/BFF
  joins graph to registry, auth, revisions, evidence, UI context
             |
             v
     asset-centric user experience
```

The backend migration should not be considered complete until the following are
true:

1. A registry asset event and a physical-only DCH/Spark event converge in Feast.
2. A KFP root, DCH child, native Spark child, and embedding child remain distinct
   runs with correct parent relationships.
3. A retry and a failed Spark write retain their raw status and have a documented
   derived status.
4. A registry revision can be traced separately from the stable asset, with its
   evidence level visible.
5. An unauthorized producer cannot assert another project's namespace, and an
   unauthorized reader cannot discover the physical alias or facets.
6. Retention, archive, deletion, and replay behavior are documented and tested.
7. The RHOAI API can answer asset-centric queries without exposing Feast internals.
8. The exact RHOAI Feast/operator/OpenLineage versions pass a conformance suite.

## 9. Migration and extension plan

### Phase A: prove semantic compatibility

- Pin the exact Feast/operator/OpenLineage versions in RHOAI.
- Run the sample event catalog against Feast instead of Marquez.
- Verify DatasetEvent, symlink, RunEvent, JobEvent, parent, schema, statistics,
  column-lineage, and error facets.
- Compare graph and API results, not only HTTP status codes.
- Record intentional differences from Marquez.

### Phase B: harden platform contracts

- Publish identity/naming and context-propagation specifications.
- Implement Registry outbox/retry and DCH/KFP producer helpers.
- Integrate authorization and namespace mapping with RHOAI policy.
- Decide separate lineage server and database topology.
- Add lifecycle reconciliation, duplicate handling, and completeness metrics.

### Phase C: close evidence and product gaps

- Add revision/evidence semantics around Data Registry.
- Add retention/archive/deletion policy.
- Add safe alias/re-correlation support if real producers need it.
- Ship the RHOAI lineage API/BFF and shared UX.
- Define links to MLflow/OTel query-time lineage.

### Phase D: decide whether to extend Feast upstream

Prefer upstream contributions for generic behavior:

- OpenLineage parsing and standard facet support;
- safe idempotency and re-correlation primitives;
- scalable indexing/query behavior;
- migration and retention mechanisms;
- generic RBAC hooks and deployment configuration.

Keep RHOAI-specific behavior in the Data Registry or RHOAI lineage service unless it
has a general feature-store value:

- Registry asset/revision/evidence semantics;
- project and Kubernetes authorization binding;
- user-facing asset-centric API;
- cross-plane links to MLflow/OTel;
- RHOAI-specific display and policy semantics.

## 10. Decision recommendation

Proceed with Feast as the candidate RHOAI operational-lineage backend, subject to a
semantic conformance gate. Do not describe the change as “replacing Marquez” until
RHOAI has specified which Marquez behaviors it intends to preserve, which it is
deliberately dropping, and which it will provide in a surrounding service.

The highest-risk assumption to resolve first is the identity bridge. The sample has
already chosen the right pattern—stable registry identity plus standard symlink to
the physical identity—but RHOAI needs to make it a tested, authorized, retained,
and user-visible contract. The second is evidence: Feast can store operational
lineage, but the Data Registry must own the difference between a known link and proof
of the data that was actually consumed.

