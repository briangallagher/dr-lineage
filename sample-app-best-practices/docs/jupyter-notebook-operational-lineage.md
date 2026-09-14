# Operational lineage for Jupyter notebooks in RHOAI

Status: working architecture proposal, 2026-09-11

This document examines the RHOAI notebook scenario in which a user works
interactively from a Jupyter notebook rather than through KFP:

```text
Jupyter notebook
    -> query Data Registry
    -> invoke DCH ingestion
    -> submit Spark transformation
    -> train a model or create embeddings
    -> write a model/vector database
```

The central question is how to preserve useful operational lineage when there is no
KFP pipeline run to provide the root execution context.

The recommended answer is:

> A notebook should create a bounded, explicit “notebook activity” run when it
> initiates a piece of work. DCH, Spark, training, embedding, and vector-database
> operations should become child runs of that activity where they are genuinely
> launched by it. A long-lived notebook session should not automatically become one
> parent run for every unrelated action performed in the session.

This gives users the benefits of a parent context without pretending that a Jupyter
kernel is a durable workflow engine.

## 1. The scenario

A typical user flow might be:

1. Open a RHOAI workbench/Jupyter notebook.
2. Query Data Registry for available assets.
3. Select an asset and perhaps a specific Registry revision.
4. Ask DCH to ingest or stage the data.
5. Submit a Spark job to clean, transform, or join it.
6. Train a model, generate embeddings, or both.
7. Write a model artifact, embedding file, or vector database collection.
8. Inspect the resulting asset and its lineage later.

There is no KFP root run. The notebook is the user-facing orchestrator, but some
work may run asynchronously in DCH, Spark, a training service, or a vector database.
The notebook kernel may restart, disconnect, or remain open while those jobs continue.

## 2. What the lineage should look like

The logical execution tree could be:

```text
Notebook activity: build-customer-embeddings
  +-- DCH ingestion run
  +-- Spark transformation run
  +-- Embedding/training run
  +-- Vector database load run
```

The data graph is separate:

```text
Registry asset/revision
        ==symlink==
physical source
        --> DCH staged data
        --> Spark transformed data
        --> embedding/model input
        --> vectors/model artifact
        --> vector database collection or model registry
```

The notebook activity connects the executions. Dataset identities connect the data.
One must not be used as a substitute for the other.

An illustrative event sequence is:

```text
1. Notebook activity START, run notebook-run-1
2. DCH child START, parent notebook-run-1
3. DCH child COMPLETE, physical input -> staged output
4. Spark child START, parent notebook-run-1
5. Spark native events, staged input -> transformed output
6. Embedding child START, parent notebook-run-1
7. Embedding child COMPLETE, transformed input -> vectors output
8. Vector load child COMPLETE, vectors input -> vector collection output
9. Notebook activity COMPLETE, if the activity itself has a known final outcome
```

The notebook activity may have no direct dataset inputs or outputs if all actual I/O
is performed by child systems. That is preferable to duplicating every child edge.

## 3. Does the notebook need to be a parent?

### 3.1 A parent is useful, but not mandatory

OpenLineage can still connect data without a parent:

```text
DCH output == Spark input == embedding input
```

However, without a parent, the user may not be able to answer:

- which notebook action launched the Spark job;
- which DCH ingestion belongs to this embedding run;
- whether two jobs were part of one user operation or independent work;
- which user/session initiated an asynchronous child job.

For a notebook-driven workflow, a parent activity improves navigation and auditability
even though dataset identity remains the mechanism for data correlation.

### 3.2 Do not use the entire notebook session as one forever-running run

A Jupyter session is usually long-lived and contains unrelated actions:

```text
09:00 inspect asset A
09:30 experiment with sample B
11:00 rerun transformation A
14:00 inspect model C
16:00 load a vector collection
```

Making the notebook kernel one parent run would merge those activities into a single
execution. The run might never receive a terminal event, and a kernel restart could
make its lifecycle ambiguous.

### 3.3 Use a bounded notebook activity

The recommended parent is a user-visible activity with a clear purpose and scope:

```text
Notebook session:     workbench-abc / notebook-xyz
Notebook activity:    build-customer-embeddings
Activity run:         notebook-run-1
Child runs:           DCH, Spark, embedding, vector load
```

The activity can be started and completed explicitly, or created by a higher-level
notebook helper when the user invokes a supported workflow. The activity should have:

- a stable notebook job identity;
- a unique run ID per activity;
- user and project context;
- a display name and purpose;
- a start time and optional terminal state;
- a link to the notebook/workbench;
- safe parameters and selected asset/revision references.

If the user only queries metadata or explores a dataset, no lineage activity is
necessarily required. If the user starts asynchronous processing, the activity should
remain durable independently of the kernel.

## 4. Notebook identity model

The notebook needs a stable job identity and a per-activity run identity.

One possible convention is:

```text
Notebook job namespace:  jupyter://<cluster>/<project>
Notebook job name:       <workbench-or-notebook-id>
Notebook activity run:   UUIDv7
```

The exact namespace is an RHOAI contract decision. It should not include mutable
display text or a kernel process ID.

The notebook activity run should be separate from:

- the Jupyter session ID;
- the kernel process ID;
- the user identity;
- the Data Registry asset ID;
- a child DCH/Spark/training run ID.

This separation allows a notebook to launch multiple independent activities and lets
child jobs finish after the notebook disconnects.

## 5. What happens when the notebook queries Data Registry?

A metadata query is not automatically a data-read operation.

For example:

```python
assets = registry.list_assets(project="project-a")
asset = registry.get_asset("asset-123")
```

This establishes that the user selected or inspected an asset, but it does not prove
that any data bytes were read. It should not create a fake data-processing `RunEvent`.

The notebook integration may record the selected asset as activity context:

```text
activity notebook-run-1
  selected input asset asset-123@7
```

The actual ingestion, transformation, training, or embedding producer should report
the data identity it observed. This preserves the distinction between:

```text
Registry lookup:     user intent / metadata access
DCH read:            observed source access
Spark read/write:    observed transformation I/O
Embedding output:    observed derived data
```

If the notebook directly reads an object with pandas or a database client rather than
using DCH, that direct read is outside automatic instrumentation unless the notebook
SDK wraps it or the storage client is instrumented.

## 6. Notebook context propagation

The notebook should receive or create a standard context envelope. The context should
not require users to copy IDs manually between cells.

Conceptually:

```json
{
  "version": "v1",
  "project": "project-a",
  "notebook": {
    "jobNamespace": "jupyter://cluster/project-a",
    "jobName": "workbench-abc/notebook-xyz"
  },
  "activity": {
    "runId": "notebook-run-1",
    "name": "build-customer-embeddings"
  },
  "assetRefs": [
    {
      "assetId": "asset-123",
      "revision": "7",
      "role": "input"
    }
  ],
  "user": {
    "subject": "user-identity-reference"
  }
}
```

The envelope should be available through a notebook SDK, environment/context file,
or a managed notebook integration. It must not contain bearer tokens, connection
passwords, signed URLs, or arbitrary environment variables.

The SDK should make the common path simple:

```python
from rhoai_lineage import activity

with activity("build-customer-embeddings", inputs=[asset_ref]) as ctx:
    staged = dch.ingest(asset_ref, lineage_context=ctx)
    transformed = spark.submit(staged, lineage_context=ctx)
    vectors = embeddings.create(transformed, lineage_context=ctx)
    vector_store.load(vectors, lineage_context=ctx)
```

This is illustrative rather than a proposed final API. The important behavior is
that `ctx` is structured and automatically passed to supported clients.

## 7. DCH invoked from a notebook

### 7.1 DCH remains the event owner

The notebook initiates the DCH call, but DCH observes the connector access. DCH
should emit the ingestion `RunEvent`.

The event should contain:

- DCH job identity;
- child run ID;
- `ParentRunFacet` pointing to the notebook activity;
- actual physical input identity;
- Registry asset/revision output or association where known;
- connection ID without credentials;
- observed source evidence such as object version, ETag, hash, schema, or manifest;
- start/terminal state and safe failure details.

The notebook should not fabricate the DCH event merely because it called the API.

### 7.2 DCH API contract

The DCH API should accept a lineage context alongside the business request:

```json
{
  "assetRef": {
    "assetId": "asset-123",
    "revision": "7"
  },
  "destination": "staging",
  "lineageContext": {
    "parentJobNamespace": "jupyter://cluster/project-a",
    "parentJobName": "workbench-abc/notebook-xyz",
    "parentRunId": "notebook-run-1"
  }
}
```

The notebook client should fill the context automatically. A user may choose an
asset or revision, but should not type the parent run ID.

### 7.3 Asynchronous DCH work

If DCH accepts the request and returns before ingestion completes:

1. DCH must persist the child run ID and parent context.
2. DCH must emit events after the notebook cell returns.
3. The notebook activity must remain queryable after kernel disconnect.
4. DCH must expose status and reconciliation behavior.
5. A later DCH retry must use a new child run ID under the same notebook activity.

The parent relationship is therefore a durable reference, not an in-memory callback.

## 8. Spark submitted from a notebook

### 8.1 Spark needs the notebook activity parent

If the notebook submits Spark, the Spark integration should receive:

```text
parent job namespace = jupyter://<cluster>/<project>
parent job name      = <notebook identity>
parent run ID        = notebook activity run ID
```

Spark should then emit its own child run through the native OpenLineage listener.
The user should not manually set `spark.openlineage.parentRunId` when using a
supported RHOAI notebook/Spark launcher.

### 8.2 Supported and unsupported submission paths

| Submission path | Parent behavior |
|---|---|
| RHOAI notebook Spark client | Automatically inject notebook activity context. |
| RHOAI notebook component that creates `SparkApplication` | Launcher translates context into Spark properties. |
| User-written raw `SparkApplication` YAML | User must opt into context SDK/fields, or Spark is independent. |
| `spark-submit` from an unmanaged container | User-managed context or independent run. |
| Spark job launched by another service | That service becomes the parent if it owns the execution. |

The platform should make the first two paths easy and clearly label the boundary for
the latter paths.

### 8.3 Spark data identity

Spark should report the physical datasets it actually reads and writes. The notebook
activity context can carry the Registry asset/revision, but it should not replace
Spark's native dataset identities.

For example:

```text
notebook activity context: asset-123@7
Spark input:              s3://bucket/staging/asset-123/activity-1
Spark output:             s3://bucket/transformed/asset-123/activity-1
```

DCH output and Spark input should match exactly. The Registry symlink and revision
metadata should provide the logical connection.

## 9. Training from a notebook

There are at least three training modes.

### 9.1 Training executes inside the notebook kernel

The notebook activity itself may be the execution that reads data and writes a model.
In that case, the notebook SDK can emit or wrap the operation as a child run:

```text
Notebook activity
  +-- local training child run
```

It should report actual inputs, model outputs, parameters, engine, and terminal state.
It should not claim that the notebook activity observed data if the training library
read data through an uninstrumented client without an SDK hook.

### 9.2 Training launches a remote job

The notebook launcher passes the activity context to the training service. The remote
training job emits its own child event and links to MLflow or the model registry.

```text
Notebook activity
  +-- remote training run
        +-- model artifact
```

MLflow may remain authoritative for model metrics and run details while OpenLineage
records operational dataset relationships. The two should share stable links where
possible.

### 9.3 Training runs independently after notebook submission

If the notebook only creates a job and the job is later operated by a scheduler, the
notebook activity can remain the initiating parent, but the system must define whether
the scheduler introduces an additional parent. Do not silently overwrite parent
context. A possible hierarchy is:

```text
Notebook activity
  +-- scheduler submission/run
        +-- training run
```

The user experience should explain the hierarchy rather than presenting all jobs as
direct children of the notebook.

## 10. Embeddings and vector databases

### 10.1 Embedding production

Embedding generation should be a child run of the notebook activity when launched
from the notebook. It should report:

- input transformed dataset;
- embedding/model identity;
- model version or MLflow link;
- safe execution parameters;
- output vectors dataset or staging artifact;
- terminal status and retries.

If embedding generation happens in a remote service, that service should own the
event and receive the notebook context through its API.

### 10.2 Vector database load

Loading vectors into Milvus, pgvector, or another vector database is itself a data
operation. It may be represented as:

```text
embedding output --input--> vector-load run --output--> vector collection/index
```

The vector collection/index identity needs an explicit convention. Possible dataset
granularities include:

- database/schema/table;
- collection;
- collection plus partition;
- index;
- collection plus embedding model/version;
- immutable snapshot or build ID.

The chosen identity must not make every upsert look like the same immutable dataset
if the collection is mutable. A collection-level graph may be useful for navigation,
but reproducibility may require a snapshot, manifest, or build identity.

### 10.3 Vector database instrumentation

If the vector database has no native OpenLineage integration, RHOAI needs one of:

- an embedding/vector-load SDK;
- a service-side event emitter;
- a connector integration;
- a post-operation adapter that emits a child RunEvent;
- a manifest/event generated by the notebook client.

The emitter must describe what it actually loaded and should not claim that a vector
collection is immutable or complete without evidence.

## 11. Notebook lifecycle and failure semantics

The notebook is not a reliable terminal-status authority for asynchronous child jobs.

### 11.1 Kernel disconnect

If the kernel disconnects after submitting DCH or Spark:

- child jobs should continue with their stored parent context;
- the notebook activity should become `UNKNOWN`, `DETACHED`, or remain open according
  to an explicit policy;
- the system should not automatically mark the child jobs failed;
- a user should be able to reconcile or close the activity later.

### 11.2 Cell re-execution

Re-running a cell should create a new activity or child attempt, not reuse the prior
run ID. Otherwise two different transformations may be merged into one run.

### 11.3 Notebook restart

The activity context must be durable outside the kernel. It can be stored in a
Registry/lineage service record, a context file, or a notebook metadata object. It
should not depend solely on Python memory.

### 11.4 Partial completion

A notebook activity may have:

- DCH complete;
- Spark failed;
- embeddings never started;
- vector load still running.

The UI should show these child states rather than reducing the activity to a single
ambiguous green/red result.

## 12. User experience requirements

The normal notebook experience should be approximately:

```python
asset = registry.select_asset("Customer documents")

with rhoai.lineage.activity(
    name="build-customer-embeddings",
    inputs=[asset],
):
    staged = dch.ingest(asset)
    transformed = spark.transform(staged)
    vectors = embeddings.create(transformed)
    vector_store.load(vectors)
```

The user should see:

- the selected asset and revision;
- the notebook activity and its status;
- child DCH/Spark/training/embedding/vector-load runs;
- upstream and downstream dataset relationships;
- failures and retries;
- evidence level and warnings;
- links to the notebook, model, feature/vector store, and Registry record.

The user should not need to see:

- raw UUID generation;
- Feast table names;
- OpenLineage HTTP endpoints;
- parent facet JSON;
- Spark parent property names;
- DCH correlation headers;
- API keys or transport configuration.

## 13. Components required

### Notebook-side support

- RHOAI lineage SDK/client;
- asset/revision selection helper;
- activity context creation and persistence;
- automatic propagation to DCH and Spark clients;
- local training/embedding instrumentation;
- redaction and user authorization;
- clear warnings when a call is unsupported or uninstrumented.

### Data Registry integration

- asset lookup and revision API;
- logical/physical symlink information;
- authorization decisions;
- stable launch context;
- optional activity-to-asset association.

### DCH integration

- context-aware ingestion/discovery APIs;
- child run event production;
- source evidence and connection facets;
- asynchronous status/reconciliation;
- independent mode when no parent exists.

### Spark integration

- notebook activity context injection;
- native listener configuration;
- parent job/run properties;
- physical dataset identity preservation;
- SparkApplication status reconciliation.

### Training and embedding integrations

- local SDK and remote-job context propagation;
- MLflow/model-registry cross-links;
- model/embedding version metadata;
- child run lifecycle and retry semantics.

### Vector database integration

- collection/index identity contract;
- load/upsert/snapshot event production;
- evidence and manifest support;
- mutable-collection warnings.

### Backend

- Feast OpenLineage consumer/server for generic graph storage;
- RHOAI lineage facade for Registry joins, authorization, revisions, evidence, and
  notebook-centric queries;
- retention and replay policy;
- reconciliation for asynchronous jobs.

## 14. Should the notebook context be a new RHOAI component?

The notebook use case strengthens the case for a shared lineage context SDK/service.
It does not necessarily require a separate notebook Operator.

The likely initial shape is:

```text
RHOAI notebook SDK
  -> Data Registry client
  -> DCH client with context propagation
  -> Spark launcher with context propagation
  -> Feast/RHOAI lineage API
```

The notebook image/workbench can include the SDK and obtain project/user/notebook
identity from its existing RHOAI environment. A separate service may issue activity
IDs, persist activity state, and provide status queries, but the notebook itself
should not become a new orchestration control plane.

A dedicated Operator would only be justified if RHOAI needs to manage a notebook
lineage controller, shared sidecars, context injection admission, or durable activity
resources across all workbenches. Start with an SDK and supported clients; add an
Operator only when automatic injection and lifecycle management require it.

## 15. Design decisions still needed

1. Is a notebook activity explicitly started by the user, implicitly created by a
   supported client, or both?
2. What is the activity terminal state if the notebook disconnects?
3. Can one notebook activity launch multiple independent child branches?
4. Does a notebook activity have a stable run ID across kernel restart?
5. How are asset revisions selected and propagated?
6. What happens when the user reads data directly with pandas, PyArrow, or a database
   client rather than through DCH?
7. Which training/model systems emit native events, and which need adapters?
8. What is the canonical vector database dataset identity?
9. How are collection upserts distinguished from immutable vector snapshots?
10. How are notebook user identity and service-account identity represented?
11. How are parent contexts authorized across asynchronous services?
12. Which UI shows notebook activities: Data Registry, Feature Store, or a shared view?

## 16. Acceptance scenarios

### Scenario A: notebook -> DCH -> Spark -> embeddings -> vector DB

- User selects asset `asset-123@7`.
- Notebook creates activity `notebook-run-1`.
- DCH and Spark receive the context automatically.
- DCH emits physical source input and staged output.
- Spark emits native input/output events with notebook parent.
- Embedding generation emits a child run and vector output.
- Vector load emits a child run and collection/index output.
- Feast contains both data edges and execution hierarchy.
- The RHOAI API joins all governed nodes to Registry metadata.

### Scenario B: notebook restart

- Notebook activity context is recovered.
- Existing DCH/Spark child jobs remain associated.
- A rerun creates a new child attempt, not a duplicate event for the old run.

### Scenario C: direct notebook read

- User reads S3/database directly without DCH.
- No false DCH event is emitted.
- The SDK either emits a supported observed-read event or marks the data as
  uninstrumented.

### Scenario D: independent Spark launch

- User launches raw Spark without the supported notebook launcher.
- Spark emits an independent root run or requires explicit SDK context.
- RHOAI does not infer a notebook parent from names or timestamps.

### Scenario E: mutable vector collection

- Embeddings are upserted into an existing collection.
- The graph records the load operation and collection identity.
- The UI warns that collection state is mutable unless a snapshot/manifest exists.

## 17. Recommended end state

The notebook path should be a first-class RHOAI lineage scenario rather than a
special case of KFP:

```text
Jupyter notebook
  creates bounded activity context
        |
        +--> DCH emits ingestion child run
        +--> Spark launcher injects native parent context
        +--> training/embedding client emits child run
        +--> vector database adapter emits load child run
        |
        v
Feast operational graph
        + Data Registry asset/revision metadata
        + authorization
        + evidence and partiality
        v
RHOAI lineage experience
```

The notebook becomes a parent when it genuinely initiates and groups the work. It
does not need to be a parent for every direct or unrelated operation, and RHOAI
should not manufacture one when context is unavailable.

The user should provide business intent once—asset, revision, activity name—and the
platform should provide technical correlation automatically. That is the notebook
equivalent of KFP's root-run propagation, adapted to an interactive and partially
asynchronous execution model.

