# Native OpenLineage production in KFP and DCH

Status: working architecture proposal, 2026-09-11

This document describes what would be required for native OpenLineage support in
Kubeflow Pipelines (KFP) and Data Hub Connect (DCH), using the sample application's
lineage model as the reference.

The key user-experience conclusion is:

> Users should select or reference governed assets, but they should not have to
> discover, copy, and manually wire raw OpenLineage parent IDs through every
> component.

KFP should create the root execution context. First-party components and platform
integrations should propagate it automatically. DCH and Spark should emit child
events using that context. Custom components may need an SDK or an explicit opt-in.
The normal RHOAI path should not require users to understand OpenLineage internals,
but RHOAI cannot automatically infer the parent of an arbitrary Spark job that it
does not launch or control.

## 1. Why parent context matters

There are two different relationships in a lineage graph:

```text
Dataset output == Dataset input       -> data dependency
ParentRunFacet(child -> root run)     -> execution hierarchy
```

If Spark receives the correct input and output dataset identities but no KFP parent
context, Feast can still connect the data edge. The Spark run will, however, appear
as an independent execution. Users may not be able to see that it was caused by a
particular pipeline run.

If DCH receives a KFP parent context but emits no correct input/output identities,
the execution tree may be connected while the data graph is not.

For a useful RHOAI experience, both need to be propagated:

1. **Execution context:** root job/run, parent job/run, project, pipeline, attempt.
2. **Data context:** governed asset ID, revision, physical source identity, and
   output identity where known.

The parent ID is therefore necessary for a coherent pipeline story, but it is not a
substitute for dataset identity.

## 2. Current sample behavior

The sample application currently does this explicitly:

1. The KFP lifecycle shim emits the root `START` and terminal `COMPLETE`/`FAIL`.
2. The pipeline passes `asset_id` to the ingestion component.
3. Ingestion creates a child run and attaches `ParentRunFacet` to the KFP root.
4. The Spark submission helper creates a `SparkApplication` but emits no duplicate
   event.
5. The Spark application receives OpenLineage configuration and uses the native
   Spark listener.
6. The embedding component creates another child run with the same parent.

The sample also found that a KFP placeholder passed as a custom container argument
could remain a literal. The implementation therefore obtains the live KFP run ID
from a pod label through the Downward API. That is a useful warning: context
propagation must be a supported platform contract, not an assumption about how
template substitution behaves.

The sample is correct as an experiment, but it asks too much of every pipeline author
if adopted as the normal RHOAI user experience.

## 3. The required lineage context

RHOAI should define a versioned, non-secret context envelope. It could be transported
through environment variables, a mounted file, pod annotations, API headers, or
structured request fields depending on the integration. The shape should be common.

Conceptually:

```json
{
  "version": "v1",
  "project": "ol-best-practices",
  "root": {
    "jobNamespace": "kfp://cluster/project",
    "jobName": "customer-pipeline",
    "runId": "kfp-run-uuid"
  },
  "parent": {
    "jobNamespace": "kfp://cluster/project",
    "jobName": "customer-pipeline",
    "runId": "kfp-run-uuid"
  },
  "assetRefs": [
    {
      "assetId": "asset-123",
      "revision": "7",
      "role": "input"
    }
  ],
  "execution": {
    "pipelineRunId": "kfp-run-uuid",
    "taskName": "ingest",
    "attempt": 1
  }
}
```

The exact field names can change. The requirements are:

- root and parent run IDs are explicit;
- asset references are logical and revision-aware;
- project/namespace is explicit;
- attempt identity is not confused with root identity;
- no credentials, tokens, signed URLs, or arbitrary environment variables are
  included;
- the envelope is versioned and can be propagated by first-party integrations.

The context is not itself an OpenLineage event. It is input to the producer that
creates the event.

## 4. Native KFP support

### 4.1 KFP should emit the root lifecycle

KFP is the authoritative owner of pipeline orchestration. Native support should
produce a root OpenLineage `RunEvent` when a pipeline run starts and a terminal event
when KFP knows the final result.

The root event should identify:

- stable KFP job/pipeline identity;
- KFP run UUID;
- project/namespace;
- pipeline name and version where available;
- safe pipeline parameters;
- execution start/end times;
- terminal status and failure summary;
- source code or pipeline definition reference where available.

The root event does not need to pretend that the orchestration run itself transformed
data. Its purpose is to provide the execution anchor for child jobs.

KFP should emit from a control-plane lifecycle path or durable run-state mechanism,
not rely only on a user-authored exit handler. A user exit handler is still useful
for application-specific cleanup, but the platform should know when a run starts,
completes, fails, is cancelled, or is terminated unexpectedly.

### 4.2 KFP should persist and propagate context

Once KFP creates the root run ID, it should make the context available to every task:

- normal container components;
- `SparkApplication` submission components;
- DCH invocation components;
- notebook or custom-job launchers;
- retry attempts.

The platform should inject context automatically through a supported mechanism. Good
options include a mounted context file, standard environment variables, pod
annotations consumed by launchers, or a KFP runtime context API. The implementation
should not require every pipeline author to add a raw `pipeline_run_id` parameter.

The user may still need to declare a governed input asset or select it in a UI. That
is a business input, not an implementation detail. Once selected, KFP should store
the reference and inject it into the task context.

### 4.3 KFP must distinguish root, task, and attempt

The following IDs should not be collapsed:

| ID | Meaning |
|---|---|
| Pipeline/job ID | Stable definition of the pipeline. |
| Root run ID | One KFP pipeline execution. |
| Task/component ID | One logical task within the pipeline. |
| Child run ID | One execution of the task or external job. |
| Attempt ID | One retry attempt, if different from child run ID. |

For a retry, a new child run should point to the same root run, while the previous
failed child remains visible. This is the behavior implemented by the sample.

### 4.4 KFP should propagate data context without rewriting data identity

KFP should pass `asset_id` and revision references to components that need them, but
it should not replace all physical datasets with the asset ID. The component that
actually reads data should report the physical identity it observed and use a symlink
or revision facet to connect it to the governed asset.

This preserves the distinction between:

```text
pipeline context:      asset-123@7
runtime input observed: s3://bucket/raw/documents.csv
```

### 4.5 KFP should provide first-party helpers

The KFP integration should provide reusable helpers for:

- reading the current lineage context;
- creating a child run ID;
- constructing `ParentRunFacet`;
- attaching asset/revision references;
- redacting parameters;
- emitting start and terminal events;
- recording delivery failures and retry state.

Application authors should not have to copy the sample's `identities.py`,
`facets.py`, and transport logic into every project.

## 5. Native Spark support when launched by KFP

### 5.1 Does Spark need the KFP parent ID?

Yes, if Spark is intended to appear as a child of the KFP pipeline run. The Spark
OpenLineage listener needs enough context to emit a `ParentRunFacet`, typically:

```text
spark.openlineage.parentJobNamespace
spark.openlineage.parentJobName
spark.openlineage.parentRunId
```

It also needs its own Spark job/application identity and a valid OpenLineage
transport destination. The Spark application should generate or receive its own
child run ID; it should not reuse the KFP root run ID as its Spark run ID.

### 5.2 Supported RHOAI/KFP Spark paths should configure this automatically

If a Spark application is launched through an RHOAI/KFP-supported component, the
Spark submission integration should translate the KFP lineage context into the
native Spark OpenLineage configuration automatically:

```text
KFP context
    |
    v
SparkApplication spec / launcher configuration
    |
    v
native Spark OpenLineage listener
    |
    v
RunEvent with Spark child + KFP ParentRunFacet
```

The user should choose the input asset and perhaps a Spark application name. They
should not need to know the OpenLineage property names or manually copy the KFP UUID
for this supported path.

This is an integration promise, not something Spark can provide by itself. If a
customer writes a raw `SparkApplication` manifest, invokes `spark-submit` from a
custom container, or launches Spark outside KFP, RHOAI has no reliable way to infer
which pipeline run should be the parent. That customer must either configure the
context explicitly or use an RHOAI lineage SDK/launcher.

There are three possible levels of support:

1. **First-party KFP Spark component:** context injection is automatic and should be
   the recommended experience.
2. **RHOAI-provided custom launcher/SDK:** the customer opts into the integration,
   but does not construct raw OpenLineage facets or discover run IDs manually.
3. **Unmanaged Spark:** the customer is responsible for context, or the Spark run is
   recorded as an independent root with no fabricated KFP parent.

An admission webhook or Spark Operator integration could mutate arbitrary
`SparkApplication` resources, but that introduces a new coupling and still needs a
reliable way to identify the intended parent pipeline. It should not be assumed as
the default solution.

### 5.3 Spark integration requirements

The integration should:

- inject namespace, parent job, parent run, and transport configuration;
- assign a unique Spark application/run identity per execution/attempt;
- preserve the user-selected asset/revision context where it can be passed safely;
- use the native Spark listener as the sole Spark event owner;
- wait for SparkApplication status and reconcile native event status;
- surface missing or contradictory terminal events;
- avoid duplicate manual events from the KFP submitter;
- redact secrets and signed URLs from facets.

The sample's observed native Spark `FAIL` followed by `COMPLETE` demonstrates why
the KFP/Spark integration needs a documented status authority rather than assuming
that the last received event is correct.

### 5.4 What if Spark runs outside KFP?

The Spark integration should support two modes:

1. **KFP child mode:** parent context is injected automatically.
2. **Independent mode:** Spark creates its own root run and emits operational
   lineage without a KFP parent.

An independently started Spark job should not invent a KFP parent. It can still
connect datasets through canonical identities and Registry symlinks.

## 6. Native DCH support

### 6.1 DCH should own the ingestion event

DCH is the system that observes the remote connector access. It should emit the
ingestion `RunEvent`, including:

- DCH job identity;
- DCH child run ID;
- actual external input identity;
- Registry logical/revision output identity where known;
- KFP parent context when invoked by KFP;
- connection ID or source reference, without credentials;
- observed evidence such as object version, ETag, manifest, or schema snapshot;
- start, terminal, failure, and retry state.

KFP should not manufacture a DCH event because KFP did not perform the remote read.
KFP should supply context; DCH should report the observed operation.

### 6.2 DCH should receive context structurally

When KFP invokes DCH, the invocation should carry a versioned lineage context. It
could be a request body field, a standard header, or a platform-generated connection
object. For example:

```json
{
  "lineageContext": {
    "rootRun": {
      "jobNamespace": "kfp://cluster/project",
      "jobName": "customer-pipeline",
      "runId": "kfp-run-uuid"
    },
    "assetRef": {
      "assetId": "asset-123",
      "revision": "7"
    },
    "taskName": "ingest",
    "attempt": 1
  }
}
```

The DCH API should not require the user to add a separate raw “pipeline ID” field
to every call. The KFP integration should populate this automatically. DCH should
validate that the context is authorized for the target project and asset.

### 6.3 DCH should resolve the governed asset

The best DCH flow is:

1. Receive an authorized asset reference or a permitted external source reference.
2. Resolve the current Registry asset/revision and physical location.
3. Perform the connector read.
4. Emit the physical input identity it actually read.
5. Emit the Registry logical/revision identity it produced or populated.
6. Add a symlink or explicit relationship where the physical and logical identities
   refer to the same governed dataset.
7. Record safe connection and evidence facets.

If DCH is given only a physical source, it should still emit physical lineage and
mark the asset relationship as unresolved rather than guessing. A later Registry
association or safe re-correlation process can repair that state.

### 6.4 DCH should support independent invocation

DCH may also be invoked by a scheduler, a user, or an external system without KFP.
In that case:

- DCH creates its own root run or external parent reference;
- it still emits the same event schema;
- it still uses Registry asset/revision context when available;
- it does not fabricate a KFP parent;
- the UI shows that the ingestion was independent of KFP.

This avoids making KFP a hidden prerequisite for all Data Hub Connect lineage.

## 7. How the user experience should work

### 7.1 The user selects business inputs once

A user may need to choose a Data Registry asset, revision policy, connection, or
pipeline input. That is meaningful configuration. The user should not need to:

- generate a UUID;
- copy a KFP run ID into Spark configuration;
- set `spark.openlineage.parentRunId` manually when using the supported RHOAI/KFP
  integration;
- add a DCH-specific pipeline ID parameter;
- handcraft `ParentRunFacet` JSON;
- know whether the consumer is Feast or Marquez.

### 7.2 Platform propagation sequence

The desired experience is:

```text
User selects asset in pipeline/UI
             |
             v
KFP creates root run and stores asset reference
             |
             v
KFP injects context into DCH/Spark/custom tasks
             |
             +--> DCH emits child ingestion event
             |
             +--> Spark launcher injects native listener context
             |
             +--> Spark emits child execution event
             |
             v
Feast graph contains data edges + parent hierarchy
```

### 7.3 What custom users still need to do

There will always be a boundary for arbitrary custom code. A user who launches an
unmanaged external job may need to install a library or explicitly provide a context
file. RHOAI should make that an opt-in integration path, not the default for
first-party KFP/DCH/Spark workflows. The product documentation must say clearly which
submission paths are first-party and which require customer-managed context.

The platform should provide:

- a documented SDK;
- standard context environment/file format;
- examples and reusable components;
- a CLI or validation command;
- clear warnings when context is missing;
- a way to mark data as unregistered/unlinked without false claims.

## 8. Required APIs and platform changes

### KFP

KFP native support likely needs:

1. root run lifecycle event production;
2. root/context persistence;
3. automatic task context injection;
4. retry/attempt identity;
5. SparkApplication context translation;
6. DCH invocation context translation;
7. outbox/retry or durable delivery;
8. status reconciliation for lost pods and missing terminal events;
9. authorization of asset references and project context;
10. query/API support for the root lineage ID.

### DCH

DCH native support likely needs:

1. a lineage context field in ingestion/discovery APIs;
2. automatic ParentRunFacet construction;
3. asset/revision resolution against Data Registry;
4. physical input/output identity emission;
5. connection ID and source evidence facets;
6. lifecycle/retry/failure event production;
7. credential and URI redaction;
8. independent-run mode when KFP is absent;
9. idempotency and replay behavior;
10. authorization checks on asserted project/asset context.

### Shared RHOAI contract

Both systems need agreement on:

- canonical namespace/name rules;
- root and child run semantics;
- asset/revision references;
- standard context envelope;
- event delivery and duplicate handling;
- status authority;
- facet/version compatibility;
- privacy and redaction;
- Feast/RHOAI endpoint discovery.

## 9. Ownership model

| Capability | Likely owner |
|---|---|
| KFP root lifecycle and context injection | KFP/RHOAI pipeline team |
| Spark context translation and native listener configuration | KFP/Spark integration team |
| DCH ingestion/discovery events | DCH team |
| Asset/revision identity and authorization | Data Registry team |
| Generic event storage and graph | Feast/integration team |
| Shared context/facet specification | RHOAI architecture/platform team |
| Asset-centric user experience | RHOAI dashboard/data experience team |

No single team can make this work through a local implementation detail. The shared
context and identity contract needs a named platform owner.

## 10. Acceptance scenarios

### Scenario A: KFP -> DCH -> Spark

1. User selects Registry asset `asset-123@7`.
2. KFP emits root `START` with run ID `kfp-run-1`.
3. KFP invokes DCH with the lineage context automatically.
4. DCH emits child run `dch-run-1` with `ParentRunFacet(kfp-run-1)`.
5. DCH reports physical input and Registry output.
6. KFP submits Spark with the same context automatically.
7. Spark emits child run `spark-run-1` with parent `kfp-run-1`.
8. Feast contains the dataset chain and execution hierarchy.
9. KFP emits one authoritative terminal root event.

### Scenario B: DCH independent ingestion

1. User starts DCH without KFP.
2. DCH emits an independent root run.
3. Registry asset and physical source connect through the declared symlink.
4. The UI shows an ingestion path without inventing a pipeline parent.

### Scenario C: retry

1. DCH attempt 1 emits `START` then `FAIL`.
2. DCH attempt 2 emits a new run ID and `ParentRunFacet` to the same KFP root.
3. The graph preserves both attempts and the final authoritative status.

### Scenario D: missing context

1. A custom Spark job is launched without a KFP context.
2. Spark emits its own root or independent run.
3. RHOAI reports that parent context is unavailable; it does not infer one from a
   similarly named pipeline.

## 11. Rollout strategy

### Phase 1: first-party integrations

- Add KFP root lifecycle emission.
- Define and inject the context envelope.
- Add supported DCH and Spark components that consume it.
- Run the existing sample without user-supplied raw IDs.

### Phase 2: reliability and authorization

- Add outbox/retry/replay.
- Reconcile KFP/Spark/DCH status.
- Enforce project/asset authorization.
- Add conformance tests and visible missing-context warnings.

### Phase 3: custom workloads

- Provide SDK and context-file integration.
- Add notebook/custom-job launchers.
- Support independent external jobs and explicit parent association.

## 12. Bottom line

The user experience should not be “know that KFP has a run ID, pass it to Spark,
then pass it again to DCH.” The platform should create a root execution context and
propagate it automatically through supported integrations.

Users should provide business context—such as which Registry asset or revision they
intend to use. KFP, DCH, and Spark should provide technical context—parent IDs,
child IDs, lifecycle events, actual physical identities, and retries.

That division gives RHOAI a meaningful native-lineage feature rather than a set of
OpenLineage examples that only work when users understand the internals.
