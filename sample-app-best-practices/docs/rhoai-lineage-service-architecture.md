# What an RHOAI lineage service would provide

Status: working architecture proposal, 2026-09-11

This document expands the proposed RHOAI lineage API/service layer mentioned in
[Feast versus Marquez backend gaps](feast-vs-marquez-backend-gaps.md). It explains
what the service would do, why Feast alone does not automatically provide all of
it, and where the capability could live.

The short answer is that RHOAI probably needs a product-facing integration layer,
but it does not necessarily need a brand-new persistence system or Operator on day
one.

## 1. The problem it solves

Feast can provide a generic OpenLineage consumer and graph API. Data Registry can
provide governed asset metadata. Neither component, by itself, should become the
owner of all the other component's semantics.

Without an integration layer, clients must understand several independent APIs:

```text
Data Registry API       -> asset name, owner, location, schema, authorization
Feast lineage API       -> jobs, runs, datasets, events, graph edges
KFP API                 -> pipeline runs and status
Spark/Kubernetes APIs   -> actual application status and logs
MLflow/OTel             -> query and runtime traces
```

The user, however, asks one question:

> What happened to this RHOAI data asset, where did it come from, what used it,
> and how trustworthy is that answer?

The lineage service would provide the RHOAI-facing answer by composing these
systems. It is best understood as a control-plane integration and query layer over
Feast and Data Registry, not as a second OpenLineage collector competing with
Feast.

## 2. Proposed responsibility

The service would provide five related capabilities:

1. **Graph/metadata joins** — connect Feast graph nodes to Data Registry assets and
   return Registry metadata alongside operational lineage.
2. **Authorization** — apply one RHOAI policy decision across Registry assets,
   physical aliases, lineage events, runs, facets, and cross-project edges.
3. **Revision interpretation** — distinguish a stable asset from a Registry revision
   or physical evidence identity and query lineage at the correct level.
4. **Evidence interpretation** — state whether an edge is merely linked, observed,
   or backed by reproducible evidence.
5. **Product-facing queries** — offer asset-centric and run-centric APIs that hide
   Feast's internal tables and make partial, stale, or unavailable data explicit.

These are deliberately different from “receive and store OpenLineage.” Feast can
remain the generic operational-lineage store while this layer defines what the
graph means in RHOAI.

## 3. How the service would work

### 3.1 Registration and event flow

An example flow would be:

```text
1. Data Registry creates asset A and revision A@7
2. Data Registry emits DatasetEvent for A/A@7
   with SymlinksDatasetFacet -> s3://bucket/path
3. KFP starts pipeline P and emits root RunEvent
4. DCH reads the physical source and emits child RunEvent
5. Spark emits native child RunEvents
6. Feast consumes and materializes the operational graph
7. RHOAI lineage service queries Feast + Data Registry
8. UI receives an authorized, asset-centric response
```

The service does not need to copy every OpenLineage event into a second database.
It should query Feast for operational graph data and Data Registry for authoritative
asset metadata. It may maintain a small projection or cache for joins, authorization
decisions, and revision/evidence indexes if query performance requires it.

### 3.2 Example asset-centric response

The exact API is open, but a response should look conceptually like this:

```json
{
  "asset": {
    "id": "asset-123",
    "namespace": "dataregistry://project-a",
    "displayName": "Customer documents",
    "owner": "team-a",
    "revision": "7",
    "revisionIdentity": "asset-123@7"
  },
  "assurance": "LINKED",
  "upstream": [
    {
      "namespace": "s3://bucket",
      "name": "raw/documents.csv",
      "relationship": "symlink",
      "source": "data-registry"
    }
  ],
  "downstream": [
    {
      "namespace": "s3://bucket",
      "name": "staging/asset-123/run-456/documents.csv",
      "producedByRun": "run-456"
    }
  ],
  "warnings": [
    "Source bytes were not versioned by the Data Registry"
  ]
}
```

The important property is not this JSON shape. It is that the response makes the
source of each fact and its confidence visible.

## 4. What each responsibility means

### 4.1 Joins the Feast graph to Data Registry metadata

Feast knows about OpenLineage entities:

- dataset namespace/name;
- jobs and runs;
- inputs and outputs;
- lifecycle events;
- standard and supported facets;
- graph edges and symlinks.

Data Registry knows about governed assets:

- asset UUID and display name;
- project/namespace;
- owner and access policy;
- source type and sanitized location reference;
- schema and schema-discovery state;
- connection reference;
- registry lifecycle and revision metadata.

The join service resolves relationships such as:

```text
Feast dataset:
  namespace = dataregistry://project-a
  name      = asset-123

Data Registry asset:
  assetId   = asset-123
  project   = project-a
```

It also resolves physical aliases where the Feast graph contains a symlink:

```text
dataregistry://project-a / asset-123
    == SymlinksDatasetFacet ==
s3://bucket / raw/documents.csv
```

The service should not blindly replace the physical node with the Registry node.
It should retain both identities and label the relationship. This lets a user see
both “governed asset” and “what the runtime actually reported reading.”

The join must support:

- an asset with multiple physical aliases;
- a physical location that changes between revisions;
- a physical dataset that is not registered;
- an asset whose Registry record exists but whose OpenLineage event has not arrived;
- an OpenLineage event whose symlink has not arrived yet;
- unauthorized or redacted physical aliases.

This is why the service needs more than a simple foreign-key lookup. It needs a
defined identity and partiality model.

### 4.2 Authorization

Authorization means more than protecting the HTTP endpoint. It must control both
what can be asserted and what can be viewed.

#### Producer/write authorization

When a producer submits an event, the service or Feast gateway should establish:

- who the producer is;
- which RHOAI project/namespace it represents;
- which Registry assets or physical namespaces it may assert;
- whether it may create, update, or replay lineage;
- whether it may submit events for another component.

An API key that proves “this is an authenticated producer” is not automatically proof
that it may claim any arbitrary namespace. A DCH instance in project A should not be
able to assert events for project B merely by changing the event namespace.

#### User/read authorization

When a user queries lineage, authorization must apply to:

- Registry asset nodes;
- physical URI aliases;
- owners and classifications;
- connection names and source metadata;
- job and run facets;
- parameters and error messages;
- parent pipeline context;
- cross-project edges;
- historical revisions.

Filtering only the central asset node is insufficient. A graph can leak information
through an adjacent S3 path, job name, connection ID, error message, or parent run.

#### Recommended policy model

The initial model should use Data Registry/RHOAI authorization as the authority for
logical assets and project membership, with the lineage service applying equivalent
filtering to Feast results. It should not depend on each UI implementing its own
filtering.

The service should return one of three outcomes for a related object:

1. visible;
2. hidden because the caller is unauthorized; or
3. unavailable because the backend could not resolve it.

Those states should not be silently collapsed into “no lineage.”

### 4.3 Revisions

An asset ID identifies a logical business object. It does not necessarily identify
the contents used by a run. A Registry revision provides a declared boundary such as:

```text
stable asset:  asset-123
revision:     asset-123@7
previous:     asset-123@6
```

The service would use revisions to answer questions such as:

- Which physical location was associated with revision 7?
- Which runs consumed revision 7?
- What changed between revisions 6 and 7?
- Which downstream outputs were produced from revision 7?
- Was revision 7 only metadata-linked, or was source evidence recorded?

Revision handling requires a clear relationship between the Registry and OpenLineage:

```text
Data Registry revision A@7
        |
        | revision facet / canonical dataset identity
        v
OpenLineage dataset A@7
        |
        | inputs and outputs in RunEvents
        v
derived datasets and runs
```

The service should not invent a revision from a timestamp, UUID, or event arrival
order. The Registry must own revision creation. The service may interpret and query
revisions, but it should not claim that a revision is immutable unless the Registry
or storage system provides the evidence.

There are two practical identity patterns:

1. Keep the stable asset as the OpenLineage dataset and attach revision metadata as a
   facet. This is easier to adopt but risks merging runs across revisions.
2. Use a revision-specific dataset identity such as `asset-123@7`, while retaining
   the stable asset as a parent/business anchor. This gives cleaner impact queries
   but requires every producer to propagate the revision.

The second pattern is stronger for revision-scoped lineage. The service should
support both during migration, with explicit warnings when lineage is only asset-level.

### 4.4 Evidence

Evidence is the difference between “the system says these things are related” and
“we can substantiate what data was involved.”

The service should classify evidence rather than treating every edge equally:

| Level | Meaning | Example |
|---|---|---|
| `LINKED` | Logical and physical identities were connected by Registry metadata or a symlink. | Registry asset `asset-123` aliases `s3://bucket/raw.csv`. |
| `OBSERVED` | A runtime observed additional source evidence. | DCH recorded ETag, size, modification time, object version, or manifest. |
| `REPRODUCIBLE` | Required inputs and outputs are immutable or retained sufficiently to reconstruct the run. | Iceberg snapshot plus retained artifacts and run parameters. |

The service would collect evidence references from Data Registry and producer facets,
then present them with their authority and timestamp. It should not calculate a
reproducibility claim merely because an OpenLineage event exists.

Examples of evidence the service may display:

- object version ID;
- ETag or content hash;
- table snapshot ID;
- document manifest;
- schema hash;
- observed source timestamp;
- retained copy location;
- source audit event;
- deletion or retention status.

Evidence can also be negative or incomplete:

- the object was mutable;
- the producer reported a path but no content evidence;
- the source was written directly outside an instrumented workflow;
- the event was retained but the referenced object expired;
- only the logical asset relationship is known.

The service should expose these limitations as warnings and evidence fields, not hide
them behind a green lineage graph.

### 4.5 Product-facing query semantics

The service should translate generic graph operations into questions RHOAI users can
actually ask:

- “Show upstream sources for this asset or revision.”
- “Show downstream assets and runs affected by this revision.”
- “Which run produced this feature/table/vector dataset?”
- “Show all attempts, including failed attempts.”
- “What changed between revisions?”
- “Which edges are linked versus observed?”
- “Why is this graph incomplete?”
- “Show the Feature Store lineage and broader operational lineage separately.”

This is also where the service applies depth limits, pagination, time bounds, and
partial-result warnings. Feast's generic graph API should remain available below
this layer, but RHOAI clients should not need to know Feast's table model or query
quirks.

## 5. Is this a new component?

Conceptually, yes: RHOAI needs a product-facing lineage integration capability.
Operationally, it does not have to begin as a new standalone microservice.

There are four deployment choices.

### Option A: Put the facade in an existing Data Registry BFF/API

```text
Data Registry API/BFF
  -> Data Registry metadata and auth
  -> Feast lineage API
  -> asset-centric lineage response
```

This is the most pragmatic first implementation if the primary entry point is the
Data Registry dashboard. It avoids a new Operator and avoids a second lineage store.
It does, however, make the Registry BFF responsible for Feast availability, graph
query performance, cross-component authorization, and Feature Store entry points.

Use this when:

- the service is initially read-oriented;
- Data Registry is the main product owner;
- Feast remains the generic backend;
- the API can be kept modular and independently tested.

### Option B: Add a dedicated RHOAI lineage API service

```text
rhoai-lineage-api Deployment
  -> Feast lineage server
  -> Data Registry API
  -> KFP/DCH status APIs as needed
  -> shared RHOAI auth
```

This becomes attractive when both Feature Store and Data Registry need the same
lineage contract, or when authorization, caching, revision projections, and evidence
logic become substantial.

Benefits:

- one API contract for multiple UIs;
- independent scaling and availability;
- clear ownership of cross-component semantics;
- ability to add projections/caches without modifying Feast.

Costs:

- another deployment, API, release stream, and support boundary;
- service-to-service authentication and RBAC integration;
- operational dependency on Feast and Data Registry;
- possible pressure to duplicate data and graph logic.

### Option C: Extend Feast itself

Feast could expose RHOAI-specific resolvers or endpoints that call the Data Registry.
This keeps the deployment compact and may be reasonable for generic features such as
pluggable authorization or external dataset metadata.

It is risky to put all RHOAI semantics inside Feast because:

- Feast does not own Data Registry assets or revisions;
- RHOAI project authorization is not identical to Feast permissions;
- evidence and storage provenance are broader than feature-store semantics;
- RHOAI-specific API behavior could become a fork or an upstream rejection;
- Feature Store and Data Registry release cycles become tightly coupled.

Prefer upstreaming generic hooks to Feast and keeping the RHOAI policy/metadata
adapter outside Feast.

### Option D: A new Operator-managed lineage product

A new Operator would be justified only if RHOAI owns a distinct lineage product with
its own lifecycle, storage, backups, CRD, scaling, upgrades, and policy. It should not
be introduced merely to proxy two APIs.

A dedicated Operator might eventually manage:

- a lineage API Deployment;
- Feast lineage server configuration;
- separate lineage database/schema;
- retention and archive jobs;
- service certificates and authorization bindings;
- migration status and backups;
- optional broker/outbox/reconciliation workers.

That is a valid later product shape, but it is a significant commitment. The first
phase should prove the API contract and ownership model before adding another CRD and
Operator.

## 6. Recommended ownership and placement

### Initial recommendation

For an initial RHOAI release:

1. Use the Feast Operator to manage the Feast OpenLineage consumer/server because
   Feast already owns that deployment concern.
2. Add lineage aggregation endpoints to the existing Data Registry BFF or a small
   modular service owned by the Data Registry/RHOAI platform team.
3. Keep Data Registry authoritative for asset metadata, revisions, evidence, and
   logical authorization.
4. Keep Feast authoritative for generic OpenLineage event storage and graph queries.
5. Make the facade stateless initially, with short-lived caching only if needed.
6. Reassess a dedicated `rhoai-lineage-api` Deployment after the Feature Store and
   Data Registry clients require the same contract.

This avoids prematurely creating a new Operator while still acknowledging that a
cross-component product API has to exist somewhere.

### Longer-term ownership

The likely ownership split is:

| Capability | Suggested owner |
|---|---|
| OpenLineage event consumer and generic graph | Feast/upstream integration team |
| Registry asset/revision/evidence model | Data Registry team |
| KFP/DCH producer integrations | Pipeline and Data Hub Connect teams, with platform contract ownership |
| RHOAI asset-centric lineage API | RHOAI platform/data experience team, jointly with Data Registry and Feature Store |
| Shared dashboard experience | RHOAI dashboard/experience team |
| Source evidence and immutable snapshots | Storage/table/DCH owners, surfaced through Data Registry |
| Cross-plane MLflow/OTel links | Model-serving/observability owners with lineage API integration |

The critical point is that no one existing component naturally owns the entire
cross-component contract. RHOAI needs a named product owner even if the first
implementation lives in an existing BFF.

## 7. Data flow and failure behavior

The service must define what happens when dependencies disagree or are unavailable.

| Condition | Expected behavior |
|---|---|
| Data Registry available, Feast unavailable | Return asset metadata and an explicit operational-lineage-unavailable warning. |
| Feast available, Data Registry unavailable | Return generic graph only if policy permits; do not label unknown nodes as governed assets. |
| Registry event not yet consumed | Show asset metadata with “lineage event pending.” |
| Feast graph has physical node but no symlink | Show unlinked physical lineage; do not infer the asset. |
| Unauthorized related node | Redact it with an authorization indication, subject to product policy. |
| Revision has no immutable evidence | Return revision plus `LINKED`/`OBSERVED` status, not `REPRODUCIBLE`. |
| Raw events expired but graph edge remains | Show that the edge is current graph state without complete event history. |
| Backend returns partial page | Preserve pagination and partial-result warnings. |

The service should not fail an entire asset page merely because one optional backend
is unavailable, but it must never present an incomplete result as complete.

## 8. Minimal implementation sequence

### Phase 1: contract and read facade

- Define canonical asset, physical, revision, and evidence identifiers.
- Define authorization inputs and redaction behavior.
- Implement a read-only asset-centric facade over Data Registry and Feast.
- Prove the sample app's registration, symlink, KFP, DCH, and Spark graph.
- Add explicit warnings and assurance fields.

### Phase 2: producer and reconciliation integration

- Integrate native KFP and DCH context propagation as described in
  [KFP/DCH native OpenLineage support](kfp-dch-native-openlineage-support.md).
- Add authoritative status lookup and reconciliation.
- Add revision-aware producer context.
- Add outbox/replay and completeness metrics.

### Phase 3: platform service decision

- Measure query load from both Data Registry and Feature Store.
- Decide whether a shared standalone lineage API is necessary.
- Decide whether it needs a CRD/Operator or can remain an application deployment.
- Add caching/projections only where measured query and authorization costs justify
  them.

## 9. Decision criteria for a new Operator

Create a new Operator only if most of these become true:

- the lineage API has an independent lifecycle from Data Registry and Feast;
- customers need configurable topology, retention, archive, or database selection;
- it owns durable reconciliation/outbox workers;
- it requires managed upgrades and schema migrations;
- it must scale independently of both Data Registry and Feature Store;
- multiple RHOAI components depend on it as a platform service;
- the API and CRD are stable enough to support operational lifecycle semantics.

If the initial requirement is only “join two APIs and enforce existing authorization,”
an existing BFF or a normal Deployment is a better first step.

## 10. Bottom line

The RHOAI lineage service is not necessarily a new lineage database. It is the place
where RHOAI-specific meaning is applied to a generic operational graph:

```text
Feast graph
   + Data Registry metadata
   + RHOAI authorization
   + revisions
   + evidence
   + dependency/partiality semantics
   ---------------------------------
   = RHOAI lineage product contract
```

The recommended initial placement is an asset-centric facade in or beside the Data
Registry BFF, using the Feast Operator for the underlying OpenLineage service. A
dedicated service and Operator should remain a deliberate later decision driven by
shared consumers, scale, lifecycle, and ownership—not an assumption hidden in the
first Feast integration.

