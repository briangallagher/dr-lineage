# OpenLineage Event Catalog

This page summarizes which components create each OpenLineage event in the sample. See
the [design guide](openlineage-design.md) for the complete identity and assurance
discussion.

## Event types

| Event | Producer | When | Meaning |
|---|---|---|---|
| `DatasetEvent` | Data Registry stub | Asset registration | A logical dataset exists and has metadata or aliases. It does not claim that a job ran. |
| `RunEvent` | KFP shim, ingestion, native Spark, embedding | Job start, progress, or finish | One execution or retry attempt, including state, inputs, and outputs. |
| `JobEvent` | Optional `publish-jobs` command | Static job publication | A reusable job definition exists. It has no run UUID and does not claim execution. |

The `job` object embedded in every `RunEvent` is not a standalone `JobEvent`. A
`RunEvent` identifies the job that actually ran; a `JobEvent` is an optional static
declaration.

## DatasetEvent: registration

The registry persists an asset before delivery and emits a `DatasetEvent` for:

```text
namespace: dataregistry://<cluster>/<project>
name:      <asset-uuid>
```

The event includes documentation, ownership, dataset type, data source, lifecycle, and
the standard `SymlinksDatasetFacet`, connecting the logical identity to the physical
identity:

```text
dataregistry://<cluster>/<project>, <asset-uuid>
    ==symlink==
s3://<bucket>, <object-or-prefix>
```

In this sample the physical identity is `s3://sample-data` plus
`raw/documents.csv`. The symlink does not claim that the registry read, copied, hashed,
or versioned the bytes. There is intentionally no `DatasetVersionFacet` because the
mutable source has no immutable content identity.

The registry retries delivery three times. If Marquez is unavailable, the record
remains durable with pending delivery status and the same UUID can be retried
idempotently.

Staged, transformed, and embedding datasets are not standalone DatasetEvents in this
sample. They appear as inputs and outputs in RunEvents, from which Marquez materializes
dataset nodes and edges.

## RunEvent: execution history

Each execution or retry attempt has a new run UUID. Child runs use `ParentRunFacet` to
point to the KFP root. Dataset equality carries the data dependency:

```text
upstream output namespace/name == downstream input namespace/name
```

### KFP root

The lifecycle shim emits:

```text
START -> COMPLETE
START -> FAIL
```

for the stable job:

```text
namespace: kfp://<cluster>/<project>
name:      sample-app-best-practices
```

The root represents orchestration, not transformation, and has exactly one terminal
state in the application contract.

### Ingestion

The ingestion/DCH mock emits one child run per attempt:

```text
job:    dch-mock://<cluster>/<project>, read-and-stage
input:  s3://sample-data, raw/documents.csv
output: s3://sample-data, staging/<asset>/<ingest-run>/documents.csv
```

It emits `START` followed by `COMPLETE`, `FAIL`, or `ABORT`, with the KFP root as
parent. In a future deployment DCH should own this event because it observes the real
remote read and can add evidence such as ETag, object version, hash, or manifest.

### Spark

The KFP Spark submission helper creates and waits for a `SparkApplication` but emits no
lineage event. The Spark application uses the native
`io.openlineage.spark.agent.OpenLineageSparkListener`:

```text
input:  s3://sample-data, staging/<asset>/<ingest-run>/documents.csv
output: s3://sample-data, transformed/<asset>/<spark-run>
```

The listener may create application and SQL-execution jobs. These technical nodes are
expected and must not be duplicated by the submitter. Native events provide Spark
engine, storage, schema, column-lineage, and parent/root facets.

The live-tested Spark 1.53.0 integration has two limitations: a Python-only driver
exception can leave only `COMPLETE` native events, and a data-bearing failed write can
emit `FAIL` followed by `COMPLETE` for the same SQL run without an `errorMessage` facet.
The KFP root and SparkApplication state are authoritative for final status; Spark
driver logs provide failure detail. The sample does not add a duplicate manual Spark
event.

### Embedding

The mock embedding component emits:

```text
job:    kfp://<cluster>/<project>, create-mock-embeddings
input:  s3://sample-data, transformed/<asset>/<spark-run>
output: s3://sample-data, embeddings/<asset>/<embedding-run>/vectors.jsonl
```

It emits its own lifecycle and points to the KFP root. The embedding output is derived
data, not an alias of the registered source.

## JobEvent: static declarations

The CLI's `publish-jobs` command can publish static JobEvents for the KFP root,
ingestion, Spark, and embedding jobs. They carry job type, documentation, ownership,
tags, source location, and processing-engine facets, but no run UUID and no execution
inputs or outputs.

The standard scenario runner does not need standalone JobEvents: its RunEvents already
carry stable job identities and facets. A standalone JobEvent is useful when a catalog
wants job definitions before their first execution.

## Ownership rules

| Component | Emits | Must not emit |
|---|---|---|
| Registry | Logical DatasetEvent and symlink | A fake read or transformation run |
| KFP shim | Root RunEvent lifecycle | Duplicate child data lineage |
| DCH/ingestion | Read/stage RunEvents | Spark or embedding events |
| Native Spark listener | Spark application/SQL RunEvents | A second manual Spark event |
| Embedding job | Embedding RunEvents | A claim that mock vectors are production output |
| Spark submit/wait helper | Nothing | A duplicate Spark event |
| Direct storage writer | Nothing unless instrumented | An implied automatic storage audit |

## Marquez API behavior

Marquez 0.50 accepts and persists all three event types, but
`/api/v1/events/lineage` returns RunEvents. The verifier therefore reads execution
history and data edges from that endpoint, while it reads the registry DatasetEvent and
symlink from the materialized dataset entity endpoint. Standalone JobEvents are static
metadata, not run history.

## Manual registry versions: useful but limited

Adding a registry `version` field and manually incrementing it can improve lineage
somewhat. It creates an explicit user-declared revision boundary and can prevent
unrelated revisions from merging into one logical dataset identity—if every producer
uses the revision consistently.

It does not prove that bytes changed, identify which bytes were consumed, detect direct
storage writes, or enable point-in-time reconstruction. It is a registry revision, not
an immutable data version.

Do not mutate an existing OpenLineage dataset name in place. Dataset identity is the
exact `(namespace, name)` pair, so changing it fragments the graph. Keep a stable
business anchor and create immutable revision identities, for example:

```text
stable asset:  dataregistry://<cluster>/<project>, <asset-id>
revision:     dataregistry://<cluster>/<project>, <asset-id>@v3
```

`<asset-id>-<version>` is workable, but `@v3` or `/revisions/3` is easier to parse and
govern. Retain old revisions rather than overwriting them. Emit a DatasetEvent for each
revision and pass that revision identity consistently through ingestion and downstream
RunEvents.

A revision facet can record `assetId`, `revision`, `previousRevision`, `changeReason`,
and `evidenceLevel`. For example:

```json
{
  "dataset": {
    "namespace": "dataregistry://demo/ol-best-practices",
    "name": "01abc...@v3",
    "facets": {
      "dataregistryRevision": {
        "assetId": "01abc...",
        "revision": 3,
        "previousRevision": 2,
        "changeReason": "customer-declared refresh",
        "evidenceLevel": "LINKED"
      },
      "symlinks": {
        "identifiers": [
          {"namespace": "s3://sample-data", "name": "raw/documents.csv", "type": "OBJECT"}
        ]
      }
    }
  }
}
```

This should be clearly called a registry revision facet, not an OpenLineage
`DatasetVersionFacet`, unless the registry has genuine immutable content semantics.

Manual revisions improve navigation, impact analysis, ownership and approval context,
and the ability to associate later ETag/hash/manifest evidence with a declared change.
They still do not prove that the source was not bypass-written, that the pointer
resolved to the same bytes, or that a model/vector output is reproducible. Assurance
remains **Linked** until the revision is tied to observed evidence and, ideally, an
immutable object version, retained copy, or table snapshot.
