# Marquez basics: a beginner's guide to OpenLineage in this sample

This guide explains what the Marquez UI is showing for the
sample-app-best-practices application and how to interpret it without already
knowing OpenLineage or Marquez.

It is written around the questions that commonly arise while looking at this
application:

- Why are there two jobs under the KFP namespace?
- Why does read-and-stage have 37 runs?
- Why does a dataset have only one version when a job has many runs?
- Why do the graphs contain Dataset and Job boxes if the application emitted
  RunEvents?
- Why does the KFP pipeline not appear as one complete graph?
- Why are ingestion and Spark not listed as KFP jobs?
- Where does the registered Data Registry asset fit into the graph?

The short answer is that Marquez materializes a searchable graph from events.
The UI boxes are persistent, aggregated entities; the events are the historical
facts that create or update those entities and their relationships.

---

## 1. The essential mental model

There are four concepts to keep separate:

~~~text
Dataset identity  = a logical piece of data
Job identity      = a repeatable kind of work
Run identity      = one execution of that work
Event             = a message describing a fact about a dataset, job, or run
~~~

For example:

~~~text
Job:    read-and-stage
Run:    01a08c28-7222-757c-be3f-9f84226ef4cf
Input:  s3://sample-data / raw/documents.csv
Output: s3://sample-data / staging/asset-id/run-id/documents.csv
~~~

The job is reused. The run is new each time the job executes. The input and
output are dataset identities. A RunEvent records the relationship between the
run, the job, and those datasets.

Marquez then materializes those references into a graph:

~~~text
                         RunEvent
                            |
              +-------------+-------------+
              |                           |
       materialized Job node       materialized Dataset nodes
              |                           |
       run history and states       input/output edges
~~~

The Job and Dataset boxes are therefore not individual RunEvents. They are the
stable entities that Marquez uses to organize many events.

---

## 2. What OpenLineage is providing

OpenLineage is an event model and a standard vocabulary for describing data
provenance. It does not itself provide the entire customer-facing product
experience.

An OpenLineage event can say things such as:

- a registered dataset exists;
- a job started;
- a job completed successfully;
- a job failed;
- a job consumed a dataset;
- a job produced a dataset;
- one run was caused by or belongs to another run;
- a dataset has a schema, statistics, or column lineage.

Marquez receives those events, stores them, indexes them, and exposes them
through its API and UI.

The important boundary is:

~~~text
OpenLineage events = facts emitted by producers
Marquez             = backend and exploratory UI for those facts
Customer product    = potentially a richer UI built using those facts
~~~

Generating JSON is not the same as delivering customer value. Customer value
comes from resolving identities, traversing upstream and downstream
relationships, joining ownership metadata, explaining failures, and presenting
the result in a useful workflow.

---

## 3. The three main event types

This sample uses three OpenLineage event types.

### 3.1 DatasetEvent

A DatasetEvent describes a dataset as an entity. It is normally used for
design-time or catalog-style metadata.

The Data Registry emits a DatasetEvent when an asset is registered. That event
does not mean that a pipeline read the data. It means that the logical dataset
exists and has metadata.

In this application, the registered asset has an identity like:

~~~text
namespace: dataregistry://<cluster>/<project>
name:      <asset-uuid>
~~~

The event also contains a symlink to the physical source:

~~~text
dataregistry://<cluster>/<project> / <asset-uuid>
    == symlink ==
s3://sample-data / raw/documents.csv
~~~

The symlink tells Marquez that the logical registry identity and the physical
storage identity are related. It does not prove that the bytes were copied,
hashed, or versioned.

### 3.2 RunEvent

A RunEvent describes a runtime execution. It contains:

- a run ID;
- a job identity;
- a lifecycle state such as START, COMPLETE, or FAIL;
- optional input datasets;
- optional output datasets;
- optional run, job, and dataset facets.

Typical lifecycle sequences are:

~~~text
START -> COMPLETE
START -> FAIL
START -> ABORT
~~~

One execution or retry attempt normally has one run ID and one terminal state.

The ingestion component emits RunEvents. The embedding component emits
RunEvents. Native Spark emits RunEvents through the OpenLineage Spark listener.
The KFP lifecycle shim emits RunEvents for the root orchestration run.

### 3.3 JobEvent

A JobEvent describes a reusable job definition without claiming that a run
occurred. It can be useful for publishing job documentation before the first
execution.

The standard scenario does not require standalone JobEvents. Its RunEvents
already contain the job identity and job facets. Consequently, the current
Marquez job nodes are primarily materialized from the job object embedded in
RunEvents, not from standalone JobEvents.

This distinction is important:

~~~text
job object inside RunEvent = "this job actually ran"
standalone JobEvent        = "this reusable job is defined"
~~~

---

## 4. Dataset identity: what makes two datasets the same?

OpenLineage identifies a dataset using the complete pair:

~~~text
(namespace, name)
~~~

For example:

~~~text
namespace: s3://sample-data
name:      raw/documents.csv
~~~

The namespace and name together are the identity. Facets such as schema,
ownership, or statistics do not form part of the identity.

These are different datasets:

~~~text
s3://sample-data  / raw/documents.csv
s3a://sample-data / raw/documents.csv
s3://sample-data  / raw/documents.csv/
~~~

Changing the URI scheme, path spelling, case, or trailing slash can fragment a
lineage graph unless producers normalize the identity or use an explicit alias.

The application centralizes dataset naming in
[identities.py](../src/lineage_demo/identities.py).

### 4.1 Dataset identities used by this application

| Logical object | Namespace | Name pattern |
|---|---|---|
| Registered asset | dataregistry://<cluster>/<project> | <asset-uuid> |
| Raw source | s3://<bucket> | raw/documents.csv |
| Staged CSV | s3://<bucket> | staging/<asset-id>/<ingest-run-id>/documents.csv |
| Transformed data | s3://<bucket> | transformed/<asset-id>/<spark-run-id> |
| Embeddings | s3://<bucket> | embeddings/<asset-id>/<embedding-run-id>/vectors.jsonl |

The registry asset and raw source are logical anchors. The staged,
transformed, and embedding outputs are deliberately run-specific artifacts.

---

## 5. Job identity: what makes two jobs the same?

An OpenLineage job identity is also a pair:

~~~text
(job namespace, job name)
~~~

For example:

~~~text
namespace: kfp://<cluster>/<project>
name:      create-mock-embeddings
~~~

A job identity means “this is the same logical kind of work.” It does not mean
“this is one particular execution.”

The relationship looks like this:

~~~text
Job identity: create-mock-embeddings
    |
    +-- Run A: one execution
    +-- Run B: another execution
    +-- Run C: a retry or later pipeline run
~~~

### 5.1 Good practice for job identities

A good job identity should:

1. use a stable namespace representing the execution authority or integration;
2. use a stable name describing the logical operation;
3. avoid timestamps, random IDs, asset IDs, and run IDs in the job name;
4. remain unchanged across normal executions and retries;
5. use facets for code location, version, parameters, ownership, and engine;
6. receive a new identity only when the logical job or contract is materially
   different.

Good examples:

~~~text
dch-mock://cluster/project / read-and-stage
spark://cluster/project    / transform-documents
kfp://cluster/project      / create-mock-embeddings
~~~

Bad examples would be:

~~~text
read-and-stage-2026-09-10-1630
embedding-<random-uuid>
transform-for-asset-<asset-id>
~~~

Those names would cause Marquez to create a different job node for every
execution, which would destroy useful aggregation.

### 5.2 Job identities in this application

The current application uses these logical jobs:

| Job | Namespace | Meaning |
|---|---|---|
| sample-app-best-practices | kfp://... | KFP root orchestration lifecycle |
| read-and-stage | dch-mock://... | Resolve, read, validate, and stage the asset |
| Native Spark jobs | spark://... | Spark application and SQL execution lineage |
| create-mock-embeddings | kfp://... | Create vectors from Spark output |

The two jobs under the KFP namespace are therefore the KFP orchestration job
and the KFP embedding component. Ingestion is intentionally in the DCH mock
namespace, and Spark is intentionally in the Spark namespace.

The namespace is not merely a display folder. It is part of the job identity.
It describes the authority or execution system under which the job is being
reported.

---

## 6. KFP pipeline identity versus OpenLineage job identity

This is one of the easiest places to become confused.

The KFP pipeline is defined as:

~~~text
OpenLineage Data Registry Best Practices
~~~

That is a KFP pipeline/template name.

The OpenLineage root job is emitted as:

~~~text
kfp://<cluster>/<project> / sample-app-best-practices
~~~

That is an OpenLineage job identity chosen by the application.

At runtime, KFP creates a pipeline run ID. The application uses that as the
root OpenLineage run ID when possible. The root run is then referenced by the
child ingestion, Spark, and embedding runs through parent/root metadata.

The identities are therefore:

~~~text
KFP pipeline definition:   one reusable DAG/template
KFP pipeline run:          one execution of that DAG
OpenLineage root job:      the orchestration job identity
OpenLineage root run:      the execution of that orchestration job
Child OpenLineage jobs:    the component kinds of work
Child OpenLineage runs:    the component executions
~~~

The root lifecycle is implemented in
[lifecycle.py](../src/lineage_demo/lifecycle.py). The DAG and task dependencies
are implemented in [pipeline.py](../pipeline/pipeline.py).

---

## 7. What appears under the Datasets view?

The Datasets view shows materialized dataset entities known to Marquez. It is
not a list of DatasetEvents and it is not a list of all files in object storage.

A dataset entry is normally identified by:

~~~text
namespace + name
~~~

The details page can show information such as:

- namespace and name;
- current version;
- creation and update timestamps;
- schema facet;
- storage and data-source facets;
- ownership and documentation;
- input/output relationships to jobs;
- symlinks to related identities;
- column lineage where it was emitted.

### 7.1 Why is there one Data Registry dataset?

The registry asset has one stable identity: its asset UUID in the Data Registry
namespace. Registering or updating that asset enriches the same logical
dataset entity.

That is why it is normal to see one registry dataset for one registered asset.

The asset's human-readable name, such as Customer documents, is metadata. The
UUID is the canonical identity used for joins and URLs.

### 7.2 Why are there many staging or embedding datasets?

The output names contain run IDs:

~~~text
staging/<asset-id>/<ingest-run-id>/documents.csv
embeddings/<asset-id>/<embedding-run-id>/vectors.jsonl
~~~

Each run therefore creates a different (namespace, name) pair. Marquez
materializes each pair as a separate dataset entity.

This is intentional. The application uses create-only storage semantics and
per-run paths so that one attempt does not overwrite another attempt's output.

### 7.3 Why are there no datasets named “ingest” or “Spark”?

Ingest and Spark are jobs, not datasets. Their output datasets appear under the
storage namespace, for example:

~~~text
s3://sample-data / staging/...
s3://sample-data / transformed/...
~~~

You should not expect a dataset named read-and-stage merely because that job
exists. A job consumes and produces datasets; it is not itself a dataset.

---

## 8. What appears under the Jobs view?

The Jobs view shows materialized job identities. It is not a list of individual
RunEvents and it is not necessarily a list of KFP task definitions.

Marquez groups events by the exact job identity:

~~~text
(job namespace, job name)
~~~

For example, every event containing:

~~~json
{
  "job": {
    "namespace": "dch-mock://cluster/project",
    "name": "read-and-stage"
  }
}
~~~

contributes to the same Marquez job node.

The job page can then show:

- the job's run history;
- lifecycle states;
- input and output datasets;
- job facets such as documentation and ownership;
- the lineage graph associated with the job;
- failures and execution details.

### 8.1 Why are there two jobs under KFP?

There are two because the application emits two distinct KFP job identities:

~~~text
kfp://... / sample-app-best-practices
kfp://... / create-mock-embeddings
~~~

The root is the orchestration job. The embedding component is a separate
logical operation that happens to run as a KFP task and therefore uses the KFP
namespace.

The ingestion job does not appear under KFP because its job identity is:

~~~text
dch-mock://... / read-and-stage
~~~

Native Spark jobs do not appear under KFP because the Spark listener emits them
with the Spark namespace.

### 8.2 Why not give every component the same job identity?

That would make the system harder to understand and query. Ingest, Spark, and
embedding have different responsibilities, runtimes, and event producers.

If all events used one job identity, Marquez would group them into one job and
you would lose the ability to distinguish:

- ingestion failures from embedding failures;
- Python work from Spark work;
- component-specific parameters;
- component-specific inputs and outputs;
- the number of runs for each logical operation.

The better model is one root job plus distinct child jobs, connected by parent
relationships and dataset edges.

---

## 9. What does “37 runs” mean?

If the read-and-stage job page says that there are 37 runs, Marquez has found
37 distinct run IDs associated with this stable job identity in the retained
history.

The current observed data showed:

~~~text
read-and-stage runs: 37
START events:        37
COMPLETE events:     27
FAIL events:         10
~~~

This means there were 37 ingestion execution attempts associated with the
read-and-stage job. It does not mean there are 37 different ingestion jobs.

The count can include:

- separate KFP pipeline runs;
- scenario test runs;
- failed executions;
- KFP retries;
- historical events retained in Marquez.

One KFP pipeline run can also create more than one child attempt if KFP retries
the component. The application gives each child attempt a new UUIDv7 run ID,
while retaining the same job identity.

The relationship is:

~~~text
stable job identity: read-and-stage
    |
    +-- run ID 1: COMPLETE
    +-- run ID 2: FAIL
    +-- run ID 3: COMPLETE
    +-- ...
    +-- run ID 37: COMPLETE or FAIL
~~~

Marquez puts them together because their job namespace and name are identical.
That aggregation is what makes it possible to inspect the history of a job over
time.

---

## 10. Why does a dataset have one version instead of 37?

The run count and the dataset version count are different measurements.

~~~text
Job run count       = how many executions of a job occurred
Dataset version     = materialization history of one dataset identity
~~~

The application creates output identities containing run IDs:

~~~text
run 1 -> staging/asset-123/run-1/documents.csv
run 2 -> staging/asset-123/run-2/documents.csv
run 3 -> staging/asset-123/run-3/documents.csv
~~~

Those are three separate datasets, not three versions of one dataset.

Therefore, a common result is:

~~~text
read-and-stage job       -> 37 runs
successful staging runs  -> 27 output dataset identities
one selected output      -> 1 version
~~~

The failed runs do not produce a staging output.

### 10.1 What does currentVersion mean?

On a Marquez dataset entity, currentVersion refers to the latest materialized
version for that exact (namespace, name) pair.

It does not necessarily mean “the latest run of the whole pipeline.” If every
run uses a different dataset name, each dataset has its own current version.

The UI may show the latest facets and metadata for the selected dataset. To
inspect version history for that same identity, use the dataset versions API:

~~~text
GET /api/v1/namespaces/{encoded-namespace}/datasets/{encoded-name}/versions
~~~

To find earlier run-specific outputs, list datasets in the storage namespace
and filter by the asset prefix. Those older artifacts are usually separate
dataset entities rather than older versions of the selected entity.

### 10.2 What would create more versions?

More versions would be associated with one dataset if repeated events used the
same exact dataset identity:

~~~text
namespace: s3://sample-data
name:      staging/asset-123/documents.csv
~~~

and multiple materializations or updates were recorded for that identity.

That design could be appropriate for a stable logical table or dataset whose
revisions are explicitly tracked. It needs a clear versioning policy, such as:

- object version ID;
- content hash;
- ETag and modification timestamp;
- table snapshot ID;
- manifest ID;
- explicit registry revision such as asset-123@v3.

Simply overwriting the same S3 object and emitting the same dataset name does
not prove that the data changed or establish reproducibility.

The current design prefers separate run-specific output paths because they are
easy to correlate with an execution and safe for retries.

---

## 11. Why the graphs contain Job and Dataset boxes

A graph is built from multiple kinds of information.

Suppose the application emits this RunEvent:

~~~json
{
  "eventType": "COMPLETE",
  "run": {
    "runId": "run-123"
  },
  "job": {
    "namespace": "dch-mock://cluster/project",
    "name": "read-and-stage"
  },
  "inputs": [
    {
      "namespace": "s3://sample-data",
      "name": "raw/documents.csv"
    }
  ],
  "outputs": [
    {
      "namespace": "s3://sample-data",
      "name": "staging/asset-123/run-123/documents.csv"
    }
  ]
}
~~~

Marquez can materialize this as:

~~~text
s3://sample-data / raw/documents.csv
                |
                v
dch-mock://... / read-and-stage
                |
                v
s3://sample-data / staging/asset-123/run-123/documents.csv
~~~

The boxes are the graph's reusable nodes. The event is the historical message
that supplies the relationship and the run details.

Multiple events can contribute to the same Job box:

~~~text
read-and-stage Job box
  +-- run 1 START
  +-- run 1 COMPLETE
  +-- run 2 START
  +-- run 2 FAIL
  +-- run 3 START
  +-- run 3 COMPLETE
~~~

Likewise, multiple events can refer to the same Dataset box if they use the
same namespace and name.

### 11.1 What creates an edge?

Dataset edges are created from input/output references in runtime events. An
upstream output and downstream input join only when their dataset identities
match exactly.

Parent facets create execution relationships, not data relationships:

~~~text
ParentRunFacet:      which orchestration caused this run?
Input/output lists:  what data did this run consume or produce?
~~~

These are complementary. A parent relationship alone does not tell Marquez
which data moved between components.

### 11.2 Why is the root KFP graph simple?

The KFP root events mainly report orchestration lifecycle:

~~~text
root START
root COMPLETE or FAIL
~~~

They do not report the full data input/output chain. The child components report
those data edges.

The embedding job looks richer because its RunEvents explicitly contain:

~~~text
transformed dataset -> embedding job -> embeddings dataset
~~~

The root job can still be the parent of the child runs even when its own graph
is visually simple.

### 11.3 Does a failed run explain a one-node graph?

Failure can reduce the visible graph. For example, if ingestion fails:

~~~text
root START
ingest START
ingest FAIL
~~~

there is no staging output, so Spark and embedding may never execute.

However, a one-node root graph is not necessarily caused by failure. The main
reason is that the root events are orchestration events without detailed
dataset edges, and the Marquez job page is not the KFP DAG viewer.

---

## 12. Why there is not one automatic end-to-end pipeline graph

It is reasonable to expect this:

~~~text
Data Registry asset
        -> ingest
        -> Spark
        -> embeddings
~~~

The event data contains enough information to derive much of this path, but the
standard Marquez UI does not necessarily present it as one customer-facing
pipeline diagram.

There are two separate relationships:

### Data lineage

~~~text
registry asset ~= raw S3 object
raw S3 object -> staged CSV
staged CSV -> transformed Parquet
transformed Parquet -> embeddings JSONL
~~~

### Execution hierarchy

~~~text
KFP root run
  +-- ingestion run
  +-- Spark run
  +-- embedding run
~~~

The Data Registry symlink connects the logical asset to the physical raw source.
The input/output datasets connect processing stages. Parent facets connect child
executions to the KFP root.

Marquez can store these pieces, but its standard job page is not automatically a
complete KFP-aware business workflow view. A richer RHOAI experience could query
Marquez and combine them into one view.

Using one job identity for every component is not the right solution. It would
make a single node, but that node would hide the distinction between ingest,
Spark, and embedding rather than displaying a useful pipeline.

The ideal product experience is likely a combination of:

- KFP UI for the execution DAG and task status;
- Marquez for runtime lineage, datasets, jobs, and run history;
- Data Registry for asset names, ownership, and current business metadata;
- a lineage-focused UI that joins all three for provenance and impact analysis.

---

## 13. Namespaces: why everything is not in one namespace

A namespace is an identity scope or authority. It is not simply a UI folder.

This sample uses namespaces to show where the event came from:

~~~text
dataregistry://<cluster>/<project>  Data Registry logical assets
s3://sample-data                    physical datasets
kfp://<cluster>/<project>           KFP-owned jobs
dch-mock://<cluster>/<project>      ingestion/DCH-owned jobs
spark://<cluster>/<project>         native Spark jobs
~~~

This helps prevent accidental identity collisions. For example, a job named
transform-documents in two different execution systems does not necessarily
mean the same thing.

The trade-off is that a namespace-separated system is not automatically shown as
one visual group in the Marquez UI. A product-level UI can group these related
namespaces using application metadata, parent runs, or an explicit workflow
model.

---

## 14. The complete execution walkthrough

For one successful pipeline run, the conceptual order is:

### Step 1: Register the asset

The registry persists an asset and emits a DatasetEvent:

~~~text
dataregistry://cluster/project / asset-uuid
~~~

The event contains documentation, ownership, lifecycle, and a symlink to:

~~~text
s3://sample-data / raw/documents.csv
~~~

### Step 2: Start the KFP root run

The KFP root lifecycle shim emits a RunEvent for:

~~~text
kfp://cluster/project / sample-app-best-practices
~~~

The KFP pipeline run ID becomes the root OpenLineage run ID where possible.

### Step 3: Run ingestion

The ingestion component creates a new child run ID and emits:

~~~text
job:    dch-mock://cluster/project / read-and-stage
input:  s3://sample-data / raw/documents.csv
output: s3://sample-data / staging/asset-id/ingest-run-id/documents.csv
~~~

Its parent facet points to the KFP root run.

### Step 4: Run Spark

The KFP task submits a SparkApplication. The Spark application uses the native
OpenLineage listener and emits Spark job/run events:

~~~text
job namespace: spark://cluster/project
input:         staged CSV
output:        transformed Parquet
parent:        KFP root run
~~~

The Python submitter intentionally does not emit duplicate Spark lineage.

### Step 5: Run embeddings

The embedding task consumes the Spark output and emits:

~~~text
job:    kfp://cluster/project / create-mock-embeddings
input:  transformed Parquet
output: embeddings/<asset-id>/<embedding-run-id>/vectors.jsonl
~~~

It also emits schema, statistics, and column-lineage facets.

### Step 6: Finish the root run

The KFP ExitHandler invokes the root completion logic. The root run becomes
COMPLETE, FAIL, or ABORT according to the final KFP status.

---

## 15. Design choices in this sample

### 15.1 Separate job identities

The application uses separate job identities because each component has a
different responsibility and producer:

~~~text
KFP       orchestrates
DCH       ingests
Spark     transforms
Embedding creates vectors
~~~

This is a good OpenLineage design. The downside is that Marquez's default UI
does not automatically turn the hierarchy into a single business diagram.

### 15.2 Separate job namespaces

The namespace describes the execution system or authority. This makes the event
producers visible and avoids pretending that DCH and Spark are KFP jobs.

### 15.3 One root run with child runs

The root run gives the pipeline a common execution anchor. Child runs preserve
component-specific lifecycle and data lineage.

This is preferable to assigning the same run ID to all components. A run ID is
intended to identify one execution of one job; the parent relationship is the
correct way to express orchestration.

### 15.4 Per-run output names

Staged, transformed, and embedding output paths contain UUIDs. This avoids
collisions and preserves artifacts from separate attempts. It also means that
each run normally creates a separate dataset entity instead of another version
of one stable dataset.

### 15.5 UUIDv7

The application uses UUIDv7 for asset and child run IDs. UUIDv7 provides:

- uniqueness;
- approximate chronological ordering;
- useful sorting characteristics;
- no extra package dependency in this sample.

It is not what makes a job stable. A new UUIDv7 is expected for each child run.

### 15.6 Registry UUID as canonical identity

The Data Registry asset UUID is a strong machine identity and business anchor.
The friendly asset name should be displayed by a customer-facing UI, but the
UUID should remain the canonical identifier for URLs, joins, and APIs.

The current design uses a symlink facet to connect the registry asset to its
physical S3 location. Because the raw source can be mutable, this establishes a
linked operational path but not immutable proof of the exact bytes consumed.

### 15.7 Native Spark listener

Spark lineage is emitted by the native OpenLineage Spark listener rather than by
the Python submission helper. This allows Spark to report its own application,
SQL, schema, and column-lineage information and avoids duplicate manual Spark
events.

---

## 16. How to inspect Marquez as a beginner

### Start with namespaces

Namespaces tell you which identity domain you are looking at:

~~~text
GET /api/v1/namespaces
~~~

Expect to find registry, storage, KFP, ingestion, and Spark namespaces.

### Then inspect jobs

Group the event history by exact job identity:

~~~bash
jq -r '.events[] | [.job.namespace, .job.name] | @tsv' \
  /tmp/marquez-lineage.json | sort -u
~~~

This answers: “Which logical jobs have emitted events?”

### Inspect runs for one job

Filter by job namespace and name, then inspect:

~~~text
eventType
eventTime
run.runId
run.facets.parent
run.facets.executionParameters
run.facets.errorMessage
inputs
outputs
~~~

This answers: “Which executions occurred, what state did they reach, and what
data did they report?”

### Inspect a dataset entity

Use the dataset endpoint:

~~~text
GET /api/v1/namespaces/{encoded-namespace}/datasets/{encoded-name}
~~~

Read its:

- identity;
- current version;
- facets;
- symlinks;
- timestamps.

### Inspect dataset versions

Use:

~~~text
GET /api/v1/namespaces/{encoded-namespace}/datasets/{encoded-name}/versions
~~~

Remember that this is history for one exact dataset identity. It is not a
global list of all pipeline outputs.

### Find the producer of a dataset

Search RunEvents whose outputs contain the target dataset identity. Then read
that event's job, run ID, parameters, and inputs. Repeat with the input dataset
to walk upstream.

The expected chain in this application is:

~~~text
embeddings
    <- transformed Parquet
    <- staged CSV
    <- raw S3 object
    <- registered Data Registry asset via symlink
~~~

### Find downstream consumers

Search RunEvents whose inputs contain the dataset identity. This provides a
starting point for impact analysis.

---

## 17. Common incorrect assumptions

### “A Job box is a JobEvent.”

Not necessarily. A Job box can be materialized from the job object inside
RunEvents. A standalone JobEvent is optional.

### “A Dataset box is one DatasetEvent.”

Not necessarily. A DatasetEvent can create or enrich a dataset entity, but
RunEvent input/output references can also materialize dataset nodes.

### “A run is a job.”

No. A job is the reusable work definition. A run is one execution.

### “37 runs should create 37 versions.”

Only if all 37 runs emit the same dataset identity and Marquez records multiple
versions for it. Run-specific dataset names create separate dataset entities.

### “The namespace is just a UI category.”

No. It is part of the dataset or job identity.

### “The KFP pipeline name automatically becomes the Marquez job.”

No. The application explicitly emits the OpenLineage root job identity.

### “The Marquez graph is the KFP DAG.”

No. The KFP DAG describes task scheduling. The Marquez graph describes reported
jobs, datasets, and lineage relationships.

### “A successful event proves the source bytes.”

No. This sample reports what instrumented components observed and claimed. The
raw S3 object can be mutable. Stronger assurance would require hashes, ETags,
object versions, immutable snapshots, or manifests.

---

## 18. What a customer-facing lineage experience should add

Marquez is a useful lineage backend and exploratory UI, but customers usually
want task-oriented answers rather than raw event navigation.

A higher-level RHOAI experience could provide:

### Provenance view

Start with an embedding output and show:

~~~text
embedding output
  <- Spark transformed data
  <- staged ingestion output
  <- raw source
  <- friendly Data Registry asset name and owner
~~~

### Impact view

Start with a registered asset and show downstream artifacts, jobs, owners, and
pipeline runs.

### Run diagnosis view

Show the parameters, engine versions, schemas, statistics, failures, retries,
and logs associated with one root run and its child runs.

### Component view

Collapse technical Spark SQL nodes into a business-level “Transform” stage while
retaining the technical detail on demand.

### Assurance view

Label the strength of the lineage:

~~~text
Linked       = asset and operational path are connected
Observed     = evidence such as hash, ETag, or statistics was captured
Reproducible = immutable source and execution evidence are retained
~~~

The current sample primarily demonstrates the Linked level.

---

## 19. Final summary

When reading Marquez, ask these questions in order:

1. What exact (namespace, name) identifies this dataset or job?
2. Am I looking at a materialized entity, an event, or a run?
3. Which runId represents the individual execution?
4. What eventType and terminal state did that run reach?
5. Which datasets were inputs and outputs?
6. Does a parent facet connect this run to a KFP root run?
7. Is this one stable dataset identity with versions, or a run-specific dataset?
8. Is this a logical registry asset or a physical storage dataset?
9. Is the graph showing data lineage, execution hierarchy, or both?
10. Do I need Marquez, KFP, the Data Registry, or a custom joined view to answer
    the question?

The most important distinction is this:

~~~text
Marquez groups repeated executions under stable job identities,
materializes dataset identities from events,
and draws data edges from input/output references.
~~~

It is not automatically a complete customer-facing rendering of the KFP DAG.
The current application emits the right kinds of information to build that
experience, but a polished end-to-end pipeline view would need to combine job
identity, run hierarchy, dataset lineage, and Data Registry metadata.

