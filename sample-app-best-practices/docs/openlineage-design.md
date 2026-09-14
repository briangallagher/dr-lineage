# OpenLineage Design and Best-Practices Guide

## 1. Purpose and conclusion

This application investigates OpenLineage in the constraints of RHOAI Data Registry.
The registry owns metadata and a stable UUID, but the underlying data may be a mutable
S3 object, prefix, bucket, or relational object that has no registry-controlled version
identifier.

There is genuine customer value under that constraint, provided the capability is
described accurately:

> RHOAI operational lineage shows how registered data assets are reported as being
> used across instrumented ingestion, pipeline, transformation, and AI workflows.

It should not initially be marketed as an immutable audit trail or proof of exact model
reproducibility.

## 2. Customer and business value

The value is not the existence of OpenLineage JSON or a Marquez graph. The value is the
questions RHOAI can help a customer answer.

| Customer question | Value supplied by this design |
|---|---|
| Where did this vector dataset or model come from? | Navigate through the registered asset, ingestion, Spark transformation, and embedding runs that reported producing it. |
| Where is this registered asset used? | Find known pipelines and derived artifacts downstream of its logical identity. |
| What could be affected if access, schema, or location changes? | Inspect the known downstream dependency and impact graph. |
| Why does a derived artifact look wrong? | Find producing runs, parameters, timestamps, processing engines, and failures. |
| Who is responsible for the input? | Join physical storage activity to registry ownership and descriptive metadata. |
| How do RHOAI components interact? | Correlate events from Data Registry, DCH, KFP, Spark, and future model or vector services. |

The Data Registry UUID is therefore useful even though it is not a physical data
version. It is a durable **business anchor**: a customer-facing identity around which
ownership, purpose, classification, location, and downstream usage can be organized.
The symlink facet connects that anchor to the physical identifier seen by processing
engines.

OpenLineage defines the alias statement, while the backend decides how to index and
render it. Marquez 0.50 stores dataset symlinks and incorporates them into its lineage
graph. A future Feast-backed consumer must implement and conformance-test equivalent
behavior; merely retaining the facet JSON would not make the two identities converge
in queries.

### 2.1 Assurance levels

RHOAI should distinguish three levels instead of presenting all lineage edges with the
same evidentiary strength.

1. **Linked**: a run refers to a registry asset and a mutable location. This sample
   implements this level.
2. **Observed**: ingestion also records evidence it observed, such as object ETag,
   content hash, size, modification time, object version, or a manifest.
3. **Reproducible**: the run refers to an immutable object version, retained copy,
   table snapshot, or equivalent immutable manifest.

The per-run staged copy in this application is create-only and survives for seven days.
It improves short-term debugging after ingestion. It does not prove what existed at the
raw location at registration time, and expiry eventually removes the bytes.

### 2.2 Safe and unsafe product claims

Safe initial claims include navigation, ownership, operational provenance, known
dependencies, impact analysis, and failure diagnosis.

Do not claim that this initial design establishes:

- the exact bytes or document corpus used by a run;
- complete capture of changes to underlying storage;
- legal chain of custody;
- point-in-time reconstruction of mutable source data;
- audit-grade model reproducibility; or
- visibility into uninstrumented systems or failed event delivery.

Tracing a vector dataset or model to a registry record is useful. It establishes the
intended governed source and known operational path. It is not sufficient evidence of
the exact training or embedding corpus.

## 3. Architecture

The logical data flow is:

```text
Registry asset ==same identity== raw S3 object
                                  |
                                  | input
                                  v
                              ingestion
                                  |
                                  | output
                                  v
                       per-run staged CSV
                                  |
                                  | input
                                  v
                         Spark transformation
                                  |
                                  | output
                                  v
                    per-run transformed Parquet
                                  |
                                  | input
                                  v
                         mock embedding job
                                  |
                                  | output
                                  v
                         per-run vectors JSONL
```

The execution hierarchy is separate:

```text
KFP root run
  +-- ingestion attempt run
  +-- native Spark application/query run
  +-- embedding attempt run
```

Dataset edges answer **what data was used or produced**. `ParentRunFacet` answers
**which orchestrated execution caused a child execution to exist**. One is not a
substitute for the other.

The registry implementation is intentionally a one-replica ConfigMap-backed stub. A
process lock serializes requests and Kubernetes resource versions prevent blind
replacement, but it is not a production transactional store: ConfigMaps have size
limits, simultaneous writers are not composed, and delivery state is not an outbox.
The stub makes persistence and failure behavior visible without pretending to model
the real Data Registry database.

## 4. OpenLineage mental model

### 4.1 Dataset

A dataset represents a logical collection of data. Its identity is exactly:

```text
(namespace, name)
```

Facets do not form part of identity. A change from `s3://bucket` to `s3a://bucket`, an
extra slash, changed case, or a different prefix creates a different dataset unless
producers normalize it or explicitly declare an alias.

### 4.2 Job

A job is the stable definition of recurring work. Its identity is also a
`(namespace, name)` pair. Repeated pipeline executions should reuse the same job
identity.

### 4.3 Run

A run is one execution or retry attempt of a job and is identified by a UUID. This
sample generates UUIDv7 child IDs. If KFP supplies a valid UUID, that becomes the root
run ID. A non-UUID KFP identifier is deterministically converted with UUIDv5.

### 4.4 RunEvent

A `RunEvent` records a runtime lifecycle transition and can carry inputs and outputs.
Finite batch work emits:

```text
START -> optional RUNNING -> exactly one COMPLETE, FAIL, or ABORT
```

`RUNNING` is omitted because these jobs are short. `FAIL` means execution failed.
`ABORT` is used for observed cancellation or termination. A hard process or node loss
can prevent an emitter from sending any terminal event; reconciliation is still
required in production.

### 4.5 DatasetEvent and JobEvent

`DatasetEvent` and `JobEvent` describe static or design-time entities without claiming
that a processing run occurred. Registration emits a `DatasetEvent`; it does not emit a
fake job run.

Publishing a registry `DatasetEvent` creates or enriches the logical dataset node. The
operational data flow begins only when ingestion reports an input and output in its
`RunEvent`.

### 4.6 Facets

Facets add metadata to runs, jobs, datasets, inputs, and outputs. Standard facets are
preferred because independent producers and consumers share their semantics. No custom
facet is needed for this version.

## 5. Namespace and naming strategy

A namespace is the scope or authority in which a name is unique. It is part of identity,
not a display category and not a tag.

Dataset namespaces normally describe the data source or storage authority. Job
namespaces normally describe the execution platform and environment.

| Entity | Namespace | Name |
|---|---|---|
| Registry asset | `dataregistry://<cluster>/<project>` | `<asset-uuid>` |
| Raw S3 object | `s3://sample-data` | `raw/documents.csv` |
| Staged CSV | `s3://sample-data` | `staging/<asset-id>/<ingest-run-id>/documents.csv` |
| Transformed data | `s3://sample-data` | `transformed/<asset-id>/<spark-run-id>` |
| Embeddings | `s3://sample-data` | `embeddings/<asset-id>/<embedding-run-id>/vectors.jsonl` |
| KFP root job | `kfp://<cluster>/<project>` | `sample-app-best-practices` |
| Ingestion job | `dch-mock://<cluster>/<project>` | `read-and-stage` |
| Spark job | `spark://<cluster>/<project>` | `transform-documents` or native sub-job name |
| Embedding job | `kfp://<cluster>/<project>` | `create-mock-embeddings` |

The cluster name is derived from the OpenShift API hostname, making identities distinct
across clusters. Changing this convention after events are emitted fragments history,
so it must eventually become a governed RHOAI producer contract.

## 6. Registration and the symlink bridge

Assume the registry creates this record:

```json
{
  "assetId": "01994c8e-6f80-7a31-a427-4a8aa7061f51",
  "name": "Customer documents",
  "location": "s3://sample-data/raw/documents.csv",
  "lineageStatus": "DELIVERED"
}
```

It publishes a `DatasetEvent` whose target is the logical registry identity and whose
symlink points at the storage identity:

```json
{
  "eventTime": "2026-09-10T09:00:00Z",
  "producer": "https://github.com/briangallagher/dr-lineage/tree/main/sample-app-best-practices#v0.1.0",
  "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/DatasetEvent",
  "dataset": {
    "namespace": "dataregistry://demo-cluster/ol-best-practices",
    "name": "01994c8e-6f80-7a31-a427-4a8aa7061f51",
    "facets": {
      "symlinks": {
        "_producer": "https://github.com/briangallagher/dr-lineage/tree/main/sample-app-best-practices#v0.1.0",
        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SymlinksDatasetFacet.json#/$defs/SymlinksDatasetFacet",
        "identifiers": [
          {
            "namespace": "s3://sample-data",
            "name": "raw/documents.csv",
            "type": "OBJECT"
          }
        ]
      }
    }
  }
}
```

The statement means:

> The registry UUID and S3 location are alternative identifiers for one logical
> dataset.

It does not mean the registry created, read, validated, copied, or versioned the bytes.

### 6.1 Where symlinks are appropriate

A registry asset for one S3 object can alias that object. A registered relational table
can alias its physical table identity. A prefix can be the dataset if the prefix is the
agreed logical dataset granularity across producers.

A whole database or bucket containing unrelated datasets is not the same dataset as
each child table or file. Those are containment or hierarchy relationships, not
symlinks. In that case choose one of these strategies:

- register the individual logical tables or prefixes;
- model the bucket/database as a collection with separate child assets;
- use hierarchy metadata to describe storage organization; or
- introduce a governed registry relationship outside alias semantics.

Using symlinks for containment would collapse distinct assets and create misleading
lineage.

## 7. End-to-end runtime correlation

### 7.1 KFP root

KFP starts the orchestration run:

```json
{
  "eventType": "START",
  "run": {"runId": "3a6e0886-40b3-4cee-bddd-c983fd693155"},
  "job": {
    "namespace": "kfp://demo-cluster/ol-best-practices",
    "name": "sample-app-best-practices"
  },
  "inputs": [],
  "outputs": []
}
```

The root represents orchestration, not data transformation. Its exit handler maps KFP
success, failure, and cancellation to an OpenLineage terminal state.

The root run UUID must be the real KFP run UUID. In this deployment the custom
containers obtain `metadata.labels['pipeline/runid']` through the Kubernetes Downward
API. A KFP placeholder embedded directly in a custom container argument was observed
to remain literal, which would collapse unrelated executions onto one derived UUID.
Correlation identifiers are therefore live-tested rather than assumed from compiled
YAML.

### 7.2 Ingestion

The pipeline passes `asset_id`. Ingestion uses the registry API to resolve the current
location, but reports the physical S3 identity it actually reads:

```json
{
  "eventType": "COMPLETE",
  "run": {
    "runId": "01994c93-0748-77f1-aeb4-f31c5ef88543",
    "facets": {
      "parent": {
        "job": {
          "namespace": "kfp://demo-cluster/ol-best-practices",
          "name": "sample-app-best-practices"
        },
        "run": {"runId": "3a6e0886-40b3-4cee-bddd-c983fd693155"}
      }
    }
  },
  "job": {
    "namespace": "dch-mock://demo-cluster/ol-best-practices",
    "name": "read-and-stage"
  },
  "inputs": [
    {"namespace": "s3://sample-data", "name": "raw/documents.csv"}
  ],
  "outputs": [
    {
      "namespace": "s3://sample-data",
      "name": "staging/01994c8e-6f80-7a31-a427-4a8aa7061f51/01994c93-0748-77f1-aeb4-f31c5ef88543/documents.csv"
    }
  ]
}
```

Three independent correlations now exist:

1. The pipeline parameter `asset_id` lets the component resolve the registry record.
2. The symlink reconciles the registry identity with the physical input identity.
3. `ParentRunFacet` attaches the child execution to the KFP root execution.

The unique, create-only staging path prevents a retry from overwriting another
attempt's output.

### 7.3 Native Spark events

The KFP Spark submission component emits no lineage. It creates and waits for a
`SparkApplication`. The real Spark application uses
`io.openlineage.spark.agent.OpenLineageSparkListener` and receives:

```properties
spark.openlineage.namespace=spark://demo-cluster/ol-best-practices
spark.openlineage.appName=transform-documents
spark.openlineage.applicationRunId=01994c99-64b7-72ac-a9c3-c35330905e77
spark.openlineage.parentJobNamespace=kfp://demo-cluster/ol-best-practices
spark.openlineage.parentJobName=sample-app-best-practices
spark.openlineage.parentRunId=3a6e0886-40b3-4cee-bddd-c983fd693155
```

The significant native event has the staged dataset as input and transformed Parquet
as output:

```json
{
  "eventType": "COMPLETE",
  "run": {
    "runId": "01994c99-64b7-72ac-a9c3-c35330905e77",
    "facets": {
      "parent": {
        "job": {
          "namespace": "kfp://demo-cluster/ol-best-practices",
          "name": "sample-app-best-practices"
        },
        "run": {"runId": "3a6e0886-40b3-4cee-bddd-c983fd693155"}
      }
    }
  },
  "job": {
    "namespace": "spark://demo-cluster/ol-best-practices",
    "name": "transform-documents"
  },
  "inputs": [
    {
      "namespace": "s3://sample-data",
      "name": "staging/01994c8e-6f80-7a31-a427-4a8aa7061f51/01994c93-0748-77f1-aeb4-f31c5ef88543/documents.csv"
    }
  ],
  "outputs": [
    {
      "namespace": "s3://sample-data",
      "name": "transformed/01994c8e-6f80-7a31-a427-4a8aa7061f51/01994c99-64b7-72ac-a9c3-c35330905e77"
    }
  ]
}
```

The native listener may expose additional Spark technical jobs or SQL executions. The
application therefore does not promise exactly one Spark node. It requires that the
data-bearing native event joins the agreed datasets and root context.

A failure must occur inside instrumented Spark work if native failure lineage is being
tested. Raising an exception only in the Python driver after a successful Spark SQL
action can make the Spark Operator report a failed application while the native
listener legitimately reports `COMPLETE` for all Spark operations it observed. This
sample's failure injection executes Spark SQL `raise_error` inside the Parquet write,
so the failed operation is data-bearing and the native listener can report it. A
failing `collect()` was insufficient with the tested listener because the operation
had no output dataset and its failed SQL end event was skipped. The distinction
illustrates why backend status and lineage lifecycle should be reconciled in
production.

With OpenLineage Spark 1.53.0 the resulting SQL job emits `START`, `RUNNING`, and
`FAIL`, including its input and attempted output, but no `errorMessage` run facet was
observed. It then emits `COMPLETE` for the same run about 50 milliseconds after
`FAIL`. The failed KFP root carries one authoritative `FAIL` plus an error facet, and
the Spark driver log contains the engine exception. The sample does not emit a
competing manual Spark event merely to fill those gaps: doing so would violate producer
ownership and risk duplicate nodes. Product-level diagnostics should reconcile native
events with SparkApplication and KFP state, and should re-test this behavior on
listener upgrades. Do not interpret the latest Spark SQL terminal event in isolation
with this pinned combination.

The application passes `s3://` arguments to Spark, while S3A performs I/O. The live
verifier makes identity equality an acceptance condition. If a future listener version
reports `s3a://` rather than canonical `s3://`, producers must change together or an
explicit normalization/alias rule must be added. Similar-looking strings are not
correlation.

### 7.4 Embedding job

The embedding component receives the exact Spark output URI. It creates a new run and
output dataset:

```json
{
  "eventType": "COMPLETE",
  "run": {
    "runId": "01994ca1-e864-7dc0-8a47-c837230ec1ce",
    "facets": {
      "parent": {
        "job": {
          "namespace": "kfp://demo-cluster/ol-best-practices",
          "name": "sample-app-best-practices"
        },
        "run": {"runId": "3a6e0886-40b3-4cee-bddd-c983fd693155"}
      },
      "executionParameters": {
        "parameters": [
          {"key": "algorithm", "value": "sha256-demo"},
          {"key": "dimensions", "value": "8"},
          {"key": "mock", "value": "true"}
        ]
      }
    }
  },
  "job": {
    "namespace": "kfp://demo-cluster/ol-best-practices",
    "name": "create-mock-embeddings"
  },
  "inputs": [
    {
      "namespace": "s3://sample-data",
      "name": "transformed/01994c8e-6f80-7a31-a427-4a8aa7061f51/01994c99-64b7-72ac-a9c3-c35330905e77"
    }
  ],
  "outputs": [
    {
      "namespace": "s3://sample-data",
      "name": "embeddings/01994c8e-6f80-7a31-a427-4a8aa7061f51/01994ca1-e864-7dc0-8a47-c837230ec1ce/vectors.jsonl"
    }
  ]
}
```

The transformed and embedding datasets are not symlinked to the registry source. They
are new derived datasets. Their input/output edges express derivation.

### 7.5 Why all children have the KFP root as parent

Ingestion does not launch Spark; KFP launches Spark after ingestion succeeds. Spark does
not launch the embedding component; KFP does. Their direct parent is therefore KFP.
The shared datasets describe the data dependency and KFP describes task ordering.
Falsely making Spark a child of ingestion would confuse invocation hierarchy with data
flow.

## 8. Facet choices

| Entity | Facet | Why it is present |
|---|---|---|
| Registry dataset | `symlinks` | Reconciles the stable registry UUID and physical URI. |
| Dataset | `documentation` | Gives customer-readable context. |
| Dataset | `dataSource` and `storage` | Describes the owning source/storage technology. |
| Dataset | `schema` | Enables structural discovery and future compatibility analysis. |
| Output | `outputStatistics` | Reports rows, size, and file count when the emitter can know them. |
| Embedding output | `columnLineage` | Shows title identity and that embedding derives from normalized text. |
| Job | `jobType` | Distinguishes DAG, Spark/batch, and processing jobs. |
| Job | `documentation`, `ownership`, `tags` | Adds human meaning and accountability. |
| Job | `sourceCodeLocation` | Points maintainers at the implementation. |
| Run | `parent` | Preserves KFP execution hierarchy across systems. |
| Run | `processing_engine` | Identifies Python, Spark, or KFP runtime context. |
| Run | `executionParameters` | Records non-secret behavior-affecting inputs. |
| Failed run | `errorMessage` | Supports diagnosis without exposing credentials. |

Credentials, bearer tokens, arbitrary environment variables, and signed URLs must
never be copied into facets.

No `DatasetVersionFacet` is emitted for the raw object or registry asset. Inventing a
version from the registry UUID would falsely imply that the UUID identifies source
contents. A custom RHOAI facet is also unnecessary until RHOAI has a genuine immutable
evidence model with governed semantics and a published versioned schema.

## 9. Event ownership

| Producer | What it emits | What it must not emit |
|---|---|---|
| Registry | `DatasetEvent` for its logical asset and aliases | A fake processing run or a claim that it read the bytes. |
| KFP lifecycle shim | Root lifecycle | Duplicate child data lineage. |
| Ingestion/DCH mock | Its own read/stage lifecycle and datasets | Spark or embedding events. |
| Native Spark listener | Spark lifecycle, I/O, engine, and column lineage | Nothing should duplicate it manually. |
| Embedding component | Its lifecycle and input/output | A claim that the mock vector is a trained production model. |
| Spark submit/wait helper | Nothing | A second Spark job event. |
| Scenario verifier | Nothing | Test activity is not a business data operation. |
| Direct object overwrite | Nothing in this sample | Its invisibility is intentional evidence of the boundary. |

In the future, DCH should own ingestion events because it observes the remote access. It
should receive root context and, where possible, registry asset context. If it knows
only the physical URI, the registry symlink still allows a capable consumer to converge
the identities.

## 10. Retries, failures, and delivery

### 10.1 Processing retries

KFP retries each processing task once. Every attempt has a new child UUID and unique
output path. A failed attempt keeps its `START`/`FAIL`; a later attempt does not rewrite
it into success. This preserves operational truth.

The root run is the pipeline execution, so it has one lifecycle across all attempts.
It completes only when the pipeline succeeds and fails when retry policy is exhausted.

### 10.2 Event-delivery retries

Manual emitters use a five-second timeout and three bounded exponential attempts. If
delivery still fails, the processing component fails. The registry first persists its
record with `PENDING`, then tries delivery; a failure returns HTTP 503 with the stable
asset ID. Repeating the same idempotency key retries the event rather than creating a
second asset.

OpenLineage transports are normally at-least-once. Consumers must tolerate duplicate
events with the same job/run/state rather than relying on exactly-once delivery.

### 10.3 Production evolution

Failing a data job when lineage cannot be delivered makes completeness visible, but may
be too disruptive for every production workload. A scalable RHOAI design should
consider:

- a transactional outbox beside authoritative registry changes;
- durable broker transport between producers and consumers;
- exponential retries with jitter and bounded age;
- dead-letter storage with operator-visible reason and payload identity;
- reconciliation against KFP, Spark, registry, and DCH authoritative states;
- detection of `START` runs with no terminal event after a defined threshold;
- completeness service-level objectives per producer; and
- idempotent consumer keys based on event/run identity and lifecycle transition.

Fail-open versus fail-closed should be an explicit product policy. It must never be an
accidental difference between producers.

## 11. Mutable data and the instrumentation boundary

OpenLineage records what instrumented producers report. It does not watch storage.

The scenario performs this direct write:

```text
PUT s3://sample-data/raw/documents.csv
```

No registry, KFP, DCH, or Spark component observes the write, so no event is emitted and
the graph does not change. The next instrumented pipeline run still reports the same
dataset identity but reads different bytes.

This is normal lineage behavior, not a Marquez defect. Completeness requires either:

- all relevant writers to be instrumented;
- storage audit/event integration that reports changes;
- immutable or versioned storage identities; or
- an ingestion manifest that records observed evidence.

### 11.1 Improvement ladder

From weakest to strongest evidence:

1. Registry UUID and current pointer.
2. Registry metadata revision and pointer history.
3. Observed object size, modification time, and ETag.
4. Cryptographic content hash or document manifest.
5. Native object-store version ID.
6. Retained create-only ingestion copy.
7. Immutable table snapshot such as Iceberg snapshot identity.
8. Document-level manifest where document provenance is required.

Registry metadata revisioning alone records that metadata changed. It does not establish
that physical bytes changed or remained unchanged.

### 11.1.1 Manual registry revisions and lineage

Adding a registry `version` field and allowing a user to increment it can improve
lineage as a declared revision boundary. It can separate the graph for revision 2 from
revision 3, support revision-scoped impact analysis, and associate a change reason,
approval, ownership, or later observed evidence with a specific revision.

It remains an assertion by the registry user. It does not detect direct writes, prove
that bytes changed, identify the bytes consumed by a run, or make the source
reconstructible. A revision is not an immutable data version unless it is backed by an
object version, retained copy, content hash/manifest, or table snapshot.

Do not rename an existing OpenLineage dataset in place. Dataset identity is the exact
`(namespace, name)` pair, so an in-place rename fragments history. Keep the stable
business anchor and create immutable revision identities, for example:

```text
stable asset: dataregistry://<cluster>/<project>, <asset-id>
revision:    dataregistry://<cluster>/<project>, <asset-id>@v3
```

`<asset-id>-<version>` is possible, but `@v3` or `/revisions/3` is easier to govern.
Emit a DatasetEvent for each revision and require ingestion, Spark, and downstream
producers to use that revision identity consistently. A registry-specific revision
facet may record `assetId`, `revision`, `previousRevision`, `changeReason`, and
`evidenceLevel`; it must not be presented as the standard `DatasetVersionFacet` unless
the semantics are genuinely immutable. See the [event catalog](event-catalog.md) for
the proposed model and its assurance limits.

### 11.2 Permanent ingestion copies

Permanent copies can improve reproducibility, but make RHOAI a new data custodian. The
design must then address duplication cost, consistency, access-control propagation,
residency, retention, deletion, encryption, legal holds, and which copy is authoritative.
It is a product architecture choice, not a free lineage enhancement.

## 12. Event and data retention

Event history and data history are different:

- Retained events can show what producers reported at an earlier time.
- Retained bytes or immutable snapshots are required to reconstruct the data itself.

This sample omits Marquez `dbRetention`, so PostgreSQL retains history until teardown or
an operator changes the deployment. Staged MinIO objects expire after seven days.
Transformed and embedding outputs remain until teardown.

Production retention must be explicit for raw events, modeled graph state, storage
artifacts, personal information in facets, deletion requests, and backup/restore. A UI
that shows only the latest graph must not be confused with absence of raw event history;
point-in-time capabilities depend on backend storage, APIs, and retention configuration.

## 13. Scaling correlation across RHOAI

When many systems emit events, correct correlation cannot depend on teams independently
guessing names. RHOAI should establish:

1. A canonical identity specification for storage, catalogs, registry assets, jobs,
   clusters, projects, paths, case, escaping, and trailing slashes.
2. Shared producer helpers for UUIDs, root propagation, redaction, and standard facets.
3. Conformance fixtures proving that producer A's output equals producer B's input.
4. A registry alias service or governed identity mappings where systems see different
   identifiers.
5. Durable transport, delivery health, reconciliation, and completeness metrics.
6. Tenant isolation and authorization for event submission and graph access.
7. Versioned facet governance and compatibility review.
8. Explicit backend event and graph retention policy.

Pipeline IDs and tags help search, but do not create data edges. Exact dataset identity
and declared aliases create data convergence; parent facets create execution hierarchy.

## 14. Answers to the Data Registry correlation questions

### Should registration emit a DatasetEvent?

Yes. Registration is when the registry has authority to declare its logical identity,
metadata, and known aliases. The event starts the catalog/identity story, not the
operational processing story.

### What if ingestion only knows S3?

It reports S3 because that is what it actually read. The registry's symlink says the S3
identifier and registry UUID refer to the same logical dataset. Passing the asset UUID
as a job parameter remains valuable for API resolution and search, but is not a
replacement for dataset identity.

### What if different producers use different names?

Without an exact shared identity or a valid alias, they produce disconnected nodes.
Producer naming conformance is therefore an API contract, not cosmetic cleanup.

### Does a common pipeline run ID connect datasets?

No. A shared root connects executions. Dataset outputs and inputs connect data flow.

### What about relational databases or multi-file buckets?

Choose the logical dataset granularity first. A table can be one dataset. A coherent
prefix can be one dataset. A bucket containing unrelated collections should not be
collapsed into one alias relationship with its children.

## 15. References

- [OpenLineage object model](https://openlineage.io/docs/spec/object-model/)
- [OpenLineage naming](https://openlineage.io/docs/spec/naming/)
- [OpenLineage symlinks facet](https://openlineage.io/docs/spec/facets/dataset-facets/symlinks/)
- [OpenLineage parent run facet](https://openlineage.io/docs/next/spec/facets/run-facets/parent_run/)
- [OpenLineage Spark integration](https://openlineage.io/docs/integrations/spark/)
- [Marquez](https://marquezproject.ai/)
- [Marquez symlink support history](https://github.com/MarquezProject/marquez/blob/main/CHANGELOG.md#0270---2022-10-24)
