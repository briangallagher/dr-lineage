# OpenLineage Review: Scenario B P&C Underwriting RAG POC

## Purpose and review scope

This document reviews the data-lineage design and implementation in the following projects:

- [Scenario B Phase 2 POC](https://gitlab.cee.redhat.com/data-strategy/scenarioB-phase2-poc), reviewed at commit `d29063f4e1c6`
- [Scenario B Phase 1 POC](https://gitlab.cee.redhat.com/data-strategy/scenarioB-phase1-poc), which contains the earlier and more detailed lineage implementation
- [`rhoai-lineage`](https://github.com/briangallagher/rhoai-lineage), the custom lineage helper library used by Phase 1
- [`odh-dashboard` datahub branch](https://github.com/MStokluska/odh-dashboard/tree/datahub/packages/data-hub), which contains the Phase 2 application-registration and Marquez integration

The review covers:

- The end-to-end business and technical use case
- Initial data, transformations and end state
- OpenLineage events, entities, metadata and facets
- Reasons behind the main design choices
- What was done well
- Problems and gaps in the implementation
- A recommended design that follows the intention of the OpenLineage specification more closely

## Executive summary

This is a RAG, or Retrieval-Augmented Generation, solution for property-and-casualty insurance underwriting knowledge.

It takes insurance bulletins, guidelines and forms, extracts their text, divides the text into smaller sections, converts those sections into numerical embeddings and stores them in the Milvus vector database. RAG applications then search Milvus for sections relevant to a user's question and give those sections to a language model so it can produce a grounded answer with sources.

The main systems are:

- **Unity Catalog:** Catalogues and governs the source documents and Delta tables.
- **MinIO/S3:** Stores the original documents and Delta table files.
- **KFP and Ray:** Orchestrate and execute the processing pipelines.
- **Docling:** Extracts structured text from PDFs and other documents.
- **Granite or OGX:** Creates embeddings and generates answers.
- **Milvus:** Stores embeddings and searchable text chunks.
- **Marquez/OpenLineage:** Records how data moved through the ingestion pipeline.
- **MLflow:** Records individual RAG questions, retrievals and answers.
- **Document Registry:** Gives documents stable identities and helps correlate metadata across systems in Phase 1.

The broad architecture is sensible. In particular, separating pipeline lineage in Marquez from query tracing in MLflow is a good decision. However, the Phase 2 OpenLineage implementation has several semantic and operational weaknesses. It sends only successful `COMPLETE` events, uses inconsistent dataset identities, loses useful Phase 1 correlation metadata and represents static application registration as if a real processing run completed.

## End-to-end use case

### Business use case

An underwriter or compliance officer needs to ask questions such as:

- What exclusions apply to commercial property coverage?
- What does the California regulator say about wildfire claims?
- How does an internal underwriting guideline compare with an NAIC model law?
- Which source document and section support this answer?

The source material is spread across insurance guidelines, policy forms and regulatory publications. Manually finding and comparing the relevant sections is slow. The POC creates a governed knowledge base that a RAG assistant can search.

The lineage requirement is important because an insurer must be able to explain:

- Which documents were available to the assistant
- Which document version was processed
- How the document was parsed and embedded
- Which pipeline execution produced a vector
- Which sources were retrieved for a particular answer

### Initial data

The Phase 2 demo uses five PDFs stored in MinIO under:

```text
s3://poc-underwriting/volumes/
```

The documented files are three California Department of Insurance bulletins and two NAIC guidelines or model laws. The S3 location is registered in Unity Catalog as the external volume:

```text
underwriting.claims.raw_documents
```

The larger Phase 1 demonstration contains 22 documents divided into three logical collections:

| Collection | Documents | Example content |
|---|---:|---|
| `underwriting_guidelines` | 10 | Internal and regulator-issued underwriting guidance |
| `iso_forms` | 5 | ISO and ACORD insurance forms |
| `regulatory_bulletins` | 7 | State DOI, FEMA and NAIC publications |

The Phase 1 pipeline produced 363, 11 and 93 Milvus entities respectively for these collections.

### Core transformations

The main transformation is:

```text
Original document
  -> text and structure extracted by Docling
  -> text split into sections or paragraphs
  -> each section converted into a 768-dimensional embedding
  -> embedding, text and source metadata stored in Milvus
```

The basic Milvus records contain:

- Embedding vector
- Original chunk text
- Source-document name
- Section or chunk position
- Line of business

The stronger Phase 1 schema also contains:

- Stable document ID
- Pipeline run ID
- Page numbers
- Section path
- Category and subcategory
- Document date

These fields are important because an embedding alone cannot explain where an answer came from.

### Ingestion paths

#### Direct volume flow

```text
Unity Catalog volume
  -> scan S3 files
  -> Docling parsing
  -> paragraph chunking
  -> Granite embeddings
  -> Milvus: underwriting_guidelines
  -> deterministic RAG application
```

This is the simpler path. It reprocesses documents directly and does not maintain a governed intermediate table.

#### Versioned Delta flow

```text
Unity Catalog volume
  -> scan and compare files
  -> Delta document-registry table
  -> read current document records
  -> Docling parsing and embeddings
  -> Milvus: underwriting_versioned
  -> versioned RAG application
```

This path demonstrates document updates. The Delta table records added and superseded files. The demo changes a document from saying that an audit occurs “quarterly” to “monthly”, creates a new Delta version, re-embeds the document and shows the changed answer.

#### OGX flow

```text
Unity Catalog volume
  -> Docling section-aware parsing
  -> OGX embedding API
  -> OGX Vector I/O
  -> Milvus: underwriting_ogx
  -> multi-hop RAG application
```

This demonstrates OGX as a common API layer over embedding and vector storage. It records richer chunk metadata, although the deployed search interface does not return all that metadata. The implementation therefore places source information inside the chunk text as a workaround.

#### Managed Delta flow

This additional flow demonstrates Unity Catalog managed tables and credential vending. The pipeline requests a managed location, writes Parquet and Delta log files using vended credentials, and registers the commit with Unity Catalog.

It is mainly a governance and table-management demonstration. It is not the primary RAG ingestion path.

### End state

The main end state is three Milvus collections:

| Collection | Purpose |
|---|---|
| `underwriting_guidelines` | Direct-volume deterministic RAG |
| `underwriting_versioned` | Delta-versioned deterministic RAG |
| `underwriting_ogx` | OGX multi-hop RAG |

At query time:

1. The application converts the user's question into an embedding.
2. Milvus finds the most similar document chunks.
3. The chunks are provided to a Granite language model.
4. The model produces an answer grounded in those chunks.
5. The application displays the answer and sources.
6. MLflow stores a trace of the question, response and selected source information.

The deterministic applications perform a fixed search-and-generate sequence. The agentic application performs multiple searches using different terms, combines and deduplicates the results, and asks the language model to produce a cross-document answer.

## OpenLineage concepts used by the solution

### Important terminology

The solution mainly emits **RunEvent messages**. A RunEvent contains three core types of information:

- **Run:** One execution of a process at a particular time.
- **Job:** The repeatable process being executed.
- **Datasets:** The data read and written during the run.

These are not three separate event types in the deployed pipelines. The job and datasets are objects inside a RunEvent.

The current OpenLineage specification also defines standalone design-time messages:

- **JobEvent:** Describes static job metadata, declared inputs and outputs, documentation or source-code location without claiming that the job ran.
- **DatasetEvent:** Describes dataset metadata such as schema, ownership, documentation or a logical lineage relationship without associating it with a run.

No standalone JobEvent emission was found in the POC. DatasetEvent support exists in the `rhoai-lineage` MLflow bridge, but that bridge is disabled in the deployed design.

### Why RunEvents were used

RunEvents are appropriate for ingestion because a real transformation occurred. They allow Marquez to answer:

- Which job ran?
- When did it run?
- Did it start, complete or fail?
- Which datasets did it read?
- Which datasets did it produce?
- What version, quantities and processing details applied to this execution?

Phase 1 used the `rhoai-lineage` context manager to emit a proper lifecycle:

```text
START -> COMPLETE
```

or:

```text
START -> FAIL
```

The same run UUID was maintained across lifecycle events.

Phase 2 simplified this to a single `COMPLETE` event emitted after processing succeeded.

### Why jobs were represented

A job represents a repeatable operation such as:

- `acquire_documents`
- `parse_and_chunk`
- `ingest_to_milvus`
- `table_sync`
- `table_embed`
- `volume_ingest`
- `ogx_ingest`
- `delta_managed_ingest`

Each execution should be a new run of the same stable job. This gives Marquez a history of the operation and shows its normal inputs and outputs.

### Why datasets were represented

Datasets form the connecting points in the lineage graph. For example:

```text
S3 documents
  -> acquire job
  -> S3 staging dataset
  -> parse job
  -> S3 chunk dataset
  -> embedding/ingest job
  -> Milvus collection
```

An output from one job must use exactly the same namespace and name as the next job's input. Otherwise the backend creates two unrelated dataset nodes and the graph is broken.

## Current Phase 2 RunEvents

### `table_sync`

The event is intended to mean:

```text
Raw documents -> table_sync -> Delta document registry
```

It records:

- Job name: `table_sync`
- Delta table version
- Storage layer: Delta
- File format: Parquet
- Total rows
- Rows added
- Rows superseded
- Operation such as `CREATE`, `APPEND` or `NOOP`
- List of changed files

### `table_embed`

The event means:

```text
Delta document registry -> table_embed -> Milvus vectors
```

It records:

- Number of chunks added
- Total chunk count
- Actual Milvus collection name inside a custom facet

### `volume_ingest`

The event means:

```text
Unity Catalog raw-document volume -> volume_ingest -> Milvus vectors
```

It records:

- Number of files processed
- Number of chunks added
- Milvus collection name inside a custom facet

### `ogx_ingest`

The event means:

```text
Unity Catalog raw-document volume -> ogx_ingest -> OGX/Milvus vectors
```

It records:

- Number of files processed
- Number of chunks inserted
- Vector store name
- Ingestion method: `ogx-vector-io`

### `delta_managed_ingest`

The event means:

```text
Unity Catalog raw-document volume -> managed Delta ingestion -> managed Delta table
```

It records:

- Latest Delta version
- Rows added and superseded
- Operation
- Unity Catalog table ID
- Commit method: `uc-delta-api`

### Application registration

The Data Hub BFF emits a `COMPLETE` RunEvent when an application is registered or updated. It creates a job for the application, notional input datasets and an application-response output dataset.

The aim was to close the Marquez graph from the Milvus collection to its consuming RAG application without emitting an event for every query.

The aim is reasonable, but a design-time JobEvent would express the meaning more accurately. Registering an application does not mean that a runtime processing job completed.

## Facets and metadata

### Standard facets used

#### Dataset version

`DatasetVersionDatasetFacet` records the Delta table version. This is useful because it identifies the exact table state produced by a run.

#### Storage

`StorageDatasetFacet` records:

- Storage layer: Delta
- File format: Parquet

This tells consumers how the dataset is physically stored.

#### Parent run

Phase 1's `rhoai-lineage` library supports `ParentRunFacet`. It links a KFP component run to the pipeline run that started it.

This is the intended mechanism for representing a hierarchy such as:

```text
KFP pipeline run
  -> acquire component run
  -> parse component run
  -> ingest component run
```

#### Job type

Phase 1 used `JobTypeJobFacet` to describe KFP components and application jobs. The purpose was to distinguish batch KFP work from a longer-running query application.

Some values used by the POC, such as `KFP_COMPONENT` and `APPLICATION`, are custom or outside the usual standard values and may not pass strict validation against newer facet versions.

#### Error message

Phase 1 used `ErrorMessageRunFacet` on failed runs. It captured the error message, programming language and optional stack trace.

#### Processing engine

Phase 1 application-registration events used `ProcessingEngineRunFacet` to identify LangGraph or OGX.

### Custom facets used

#### `deltaStats`

Contains:

- Delta version
- Total rows
- Rows added
- Rows superseded
- Operation
- Changed-file details
- UC table ID and commit method in the managed-table flow

This is valuable business-specific metadata, especially for demonstrating document updates.

#### `vectorStats`

Contains:

- Number of chunks written
- Number of files processed
- Milvus collection or vector store
- Ingestion method

There is no complete standard OpenLineage facet for a RAG vector index, so a custom facet is justified.

#### `pipelineRunId`

Phase 1 used a custom run facet containing the KFP pipeline run ID. The same value was also written into Milvus metadata, MLflow tags and the Document Registry.

This was the main cross-system correlation key.

#### Document metadata

Phase 1 input metadata included:

- Source URL
- Source system
- Document type
- Line of business
- Jurisdiction
- Effective date
- Target collection

#### Processing metrics

Phase 1 custom output metadata included:

- Documents fetched and skipped
- Number of input files
- Chunk size
- Tokenizer
- Worker count
- Embedding model and dimension
- Milvus index type
- Vector count
- Processing duration

## The `rhoai-lineage` library

`rhoai-lineage` is a custom Python library created for this POC. It is not an official OpenLineage or RHOAI component.

Its purpose is to prevent every pipeline component from rebuilding OpenLineage JSON and naming rules independently.

It provides:

- A KFP context manager
- Automatic `START`, `COMPLETE` and `FAIL` events
- Run UUID generation
- Job and dataset naming helpers
- Parent-run relationships
- Job-type and error facets
- Optional schema extraction
- HTTP delivery to Marquez
- A manual SDK for non-KFP applications
- An optional MLflow/OpenLineage bridge

A normal usage pattern is:

```python
with kfp_lineage(
    "parse_and_chunk",
    inputs=[staged_documents],
    outputs=[chunk_dataset],
):
    process_documents()
```

The context manager emits `START` on entry, `COMPLETE` on normal exit and `FAIL` if the block raises an exception.

Lineage delivery is deliberately non-blocking: failure to contact Marquez produces a warning but does not fail the data or ML pipeline.

## The MLflow/OpenLineage bridge

The bridge is an optional adapter inside `rhoai-lineage`. It wraps MLflow's tracking store and translates MLflow operations into OpenLineage messages while still delegating normal tracking operations to MLflow.

Conceptually:

```text
Application logs to MLflow
  -> normal MLflow metadata is stored
  -> equivalent OpenLineage metadata is sent to Marquez
```

Examples include:

- Starting an MLflow run emits an OpenLineage `START` RunEvent.
- Ending or failing an MLflow run emits `COMPLETE` or `FAIL`.
- Logging an MLflow input dataset can emit a DatasetEvent containing its source, digest and schema.
- Logging a model can represent that model as an output dataset.
- Nested MLflow runs can be represented using `ParentRunFacet`.

The bridge is enabled using a special tracking URI such as:

```bash
MLFLOW_TRACKING_URI="openlineage+postgresql://..."
OPENLINEAGE_URL="http://marquez:5000"
```

### Why the bridge was disabled

The ingestion pipelines already emitted OpenLineage directly. Enabling the bridge at the same time introduced:

- Duplicate or synthetic jobs and datasets
- Noisier lineage graphs
- Less control over dataset naming
- MLflow experiment concepts represented as data-lineage nodes
- More difficult troubleshooting

The chosen design was therefore:

- Direct pipeline emission to Marquez for ingestion lineage
- Normal MLflow traces for individual RAG questions and answers
- Shared document and pipeline identifiers to connect the two systems

This is a sensible separation of concerns.

## Design decisions that were justified

### Marquez for ingestion and MLflow for queries

OpenLineage is designed primarily to describe how datasets are created and transformed. A RAG query is a short request containing prompts, model calls, tool calls, retrieved chunks and generated text. MLflow traces or OpenTelemetry spans are a better fit for that high-volume request-level detail.

Using Marquez for the slower-changing ingestion graph and MLflow for query traces avoids filling Marquez with a job and response dataset for every user question.

### Pipeline and application emitters, not the Document Registry

The registry stores identity and metadata but does not transform data. Making the pipeline the source of runtime lineage means a document appears in Marquez only when it is actually processed.

This also avoids the registry and pipeline emitting competing versions of the same relationship.

### Consistent naming helper

A reusable naming helper is a good idea because OpenLineage joins graph nodes solely through exact namespace and name matches. A minor change in hostname, URI scheme or path can split one real dataset into several lineage nodes.

### Custom vector metadata

OpenLineage does not currently provide a complete standard RAG-vector facet. Recording embedding model, dimension, chunk count, vector-store name and index method is valuable for reproducibility. A custom facet is therefore appropriate if it is properly defined and versioned.

### Static application registration

Registering an application once rather than emitting a RunEvent per user query prevents event volume from growing rapidly. The intent is sound, although it should be represented as design-time job metadata rather than as a fake successful run.

## What was done well

### Clear and credible RAG scenario

The system demonstrates a real business requirement rather than a synthetic data transformation. Being able to trace an insurance answer back to a regulator's document and section is meaningful.

### Good separation of observability concerns

Marquez records data movement. MLflow records AI requests. Unity Catalog records governance. The Document Registry handles stable identity. No single system is forced to contain every form of metadata.

### Strong Phase 1 correlation design

Using one `pipeline_run_id` across KFP, Milvus, MLflow, Marquez and the Document Registry is one of the strongest parts of the solution. It gives users a practical join key between systems with different metadata models.

### Stable document and chunk metadata

The Phase 1 fields `doc_id`, `chunk_index`, `section_path` and `page_numbers` provide the information needed to trace a retrieved passage to its source.

### Meaningful use of Delta versions

The standard dataset-version facet and the changed-file details make the document-update demonstration understandable and auditable.

### Sensible ownership of lineage emission

Having the pipeline describe transformations prevents duplicate emission by storage services and the registry.

### Correct treatment of Milvus as a dataset

Milvus stores the output of an embedding transformation. It is therefore best represented as an output dataset, not as a processing job.

### Reusable library

The `rhoai-lineage` library reduces repeated code and provides a better lifecycle implementation than the raw Phase 2 JSON emitters.

## Flaws and limitations

### Phase 2 emits only successful completion

The current Phase 2 emitters create a new UUID and send one `COMPLETE` event after processing.

This means:

- A failed pipeline may leave no lineage record.
- There is no start time from OpenLineage's point of view.
- Duration cannot be derived reliably.
- The OpenLineage run cannot be directly matched to the KFP run.
- A separate lineage component is presented as if it were the data-processing run.

The Phase 1 `START`/`COMPLETE`/`FAIL` pattern should be restored.

### Dataset namespaces do not follow datasource identity

Phase 2 commonly uses the UC catalog, such as `underwriting`, as both job and dataset namespace.

OpenLineage's naming intention is:

- Job namespace identifies the scheduler or processing environment.
- Dataset namespace identifies the physical or logical datasource.

Better examples are:

```text
Job namespace:      kfp://rhoai-cluster/mstoklus
S3 namespace:       s3://poc-underwriting
UC namespace:       unitycatalog://unity-catalog-auth.unity-catalog.svc:8443
Milvus namespace:   milvus://milvus.milvus.svc:19530
```

### Different Milvus collections collapse into one dataset

Both the direct and versioned pipelines declare an output named `claims.vectors`, even though they write to different Milvus collections.

The actual collection name appears only inside `vectorStats`. Facets are metadata; they are not part of a dataset's identity. Marquez will therefore merge different real collections into the same node.

The correct identity should look like:

```text
namespace: milvus://milvus.milvus.svc:19530
name: default.underwriting_versioned
```

### `table_sync` declares the wrong input

The event uses the Delta table's own location as the input rather than the source PDF volume. It therefore implies that the output table was generated from itself.

The input should be the raw-document S3 prefix or UC volume. The Delta table should be the output.

### Logical and physical datasets are not reconciled

Unity Catalog gives a logical identifier while S3 gives a physical location. Both may describe the same data.

The implementation uses these identifiers inconsistently. It should choose one canonical identity and use `SymlinksDatasetFacet` to declare the alternative identity.

### Application registration is a fake runtime event

Registering `uc-chat` sends a successful RunEvent even though the application did not process data at that moment.

A JobEvent should declare:

- Application name and type
- Source-code location and version
- Documentation
- Declared Milvus inputs
- Owning team
- Expected emission behaviour

Individual query details should remain in MLflow traces.

### Current application registration does not reliably connect the graph

The current dashboard BFF derives artificial `{schema}.rag_endpoint` inputs and a response dataset. Current pipeline code no longer emits the documented `rag_model_ready` events that were supposed to produce the `rag_endpoint` node.

As a result, the documented end-to-end graph and the code are no longer aligned.

### Documentation has drifted from implementation

The Phase 2 documentation claims that both embedding pipelines emit `rag_model_ready`. The current source does not.

The lineage ADR also describes a different producer and namespace arrangement from the current BFF code.

This is especially dangerous for lineage because a plausible-looking diagram can give false confidence that an audit chain exists.

### Custom facets are not specification-quality

The custom keys `deltaStats`, `vectorStats`, `custom_metrics` and `document_metadata` are not consistently project-prefixed.

The Phase 2 custom schema URLs are not versioned and the schemas are not stored in the reviewed repository. A conformant custom facet should have:

- A project-specific key such as `rhoai_vectorStats`
- A class/schema name such as `RhoaiVectorStatsOutputDatasetFacet`
- A published JSON schema
- An immutable URL containing a release version or Git commit
- Contract tests validating emitted JSON against that schema

Some Phase 1 custom data points at generic base facet schemas even though it adds fields not defined by those schemas. That does not make the custom content valid.

### Standard facets are underused

The implementation could use:

- `SchemaDatasetFacet`
- `DatasetTypeDatasetFacet`
- `DataSourceDatasetFacet`
- `LifecycleStateChangeDatasetFacet`
- `SymlinksDatasetFacet`
- `OwnershipDatasetFacet`
- `DocumentationDatasetFacet`
- `OutputStatisticsOutputDatasetFacet`
- `InputStatisticsInputDatasetFacet`
- `DataQualityMetricsInputDatasetFacet`
- `DataQualityAssertionsDatasetFacet`
- `SourceCodeLocationJobFacet`
- `DocumentationJobFacet`
- `OwnershipJobFacet`
- `ExecutionParametersRunFacet`
- `ProcessingEngineRunFacet`
- `ParentRunFacet`
- `ErrorMessageRunFacet`

### Some JobType values are non-standard

Values such as `KFP_COMPONENT` and `APPLICATION` do not match the usual OpenLineage job types. KFP component jobs should normally be represented as batch `TASK` jobs under a parent `DAG`. An application should normally be described as a service or job using values supported by the selected facet schema version.

### Phase 2 lost Phase 1 provenance metadata

The basic Phase 2 Milvus schema stores a filename-derived `source_doc` but not:

- Stable document ID
- Pipeline run ID
- Source URL
- Source document version or checksum
- Page numbers
- Full section path

This weakens the claim of end-to-end auditability.

### Vector collection versions are not explicit

A Delta version identifies the registry table, but it does not identify an immutable version of a Milvus collection. Re-embedding may delete and recreate document vectors in place.

It may therefore be impossible to reproduce exactly which vectors were available for an old query unless the collection or entities carry their own version and content hashes.

### Processing errors can be hidden by successful lineage

Some embedding code substitutes zero-filled vectors if the embedding endpoint fails. Parsing failures may skip documents and continue. The final OpenLineage event can still say `COMPLETE`.

At minimum, the event should record:

- Documents attempted, succeeded and failed
- Chunks attempted and embedded
- Zero or invalid vector count
- Data-quality assertion results
- A partial-success status or failure policy

### Emission is unreliable

Most Phase 2 emitters:

- Disable TLS verification
- Ignore or barely inspect the Marquez response
- Do not retry
- Do not buffer failed events
- Do not expose delivery metrics

Best-effort delivery is reasonable for a POC. A production audit system needs retries, monitoring and a dead-letter path.

### Lineage has no access control

The documented Marquez deployment has no authentication. Anyone with network access may be able to read or submit lineage events. In a multi-tenant platform this risks both information exposure and graph poisoning.

## Recommended OpenLineage design

### 1. Establish canonical identities

Use one central naming module and publish the rules.

Examples:

```text
Raw S3 volume
  namespace: s3://poc-underwriting
  name: volumes/

Unity Catalog table
  namespace: unitycatalog://unity-catalog-auth.unity-catalog.svc:8443
  name: underwriting.claims.document_registry

Physical Delta table
  namespace: s3://poc-underwriting
  name: tables/claims/document_registry/

Milvus collection
  namespace: milvus://milvus.milvus.svc:19530
  name: default.underwriting_versioned

KFP component job
  namespace: kfp://rhoai-cluster/mstoklus
  name: uc-table-embed.parse-and-embed
```

Use `SymlinksDatasetFacet` to associate the Unity Catalog logical table with its physical Delta location.

### 2. Emit design-time metadata

At pipeline build or deployment time, emit JobEvents for:

- Parent KFP pipeline
- Each data-transforming component
- RAG applications

Include:

- Documentation
- Source repository, file path and Git SHA
- Job type
- Owner
- Declared inputs and outputs
- Expected lifecycle emission pattern

When a UC volume, Delta table or Milvus collection is registered or changed, emit a DatasetEvent containing:

- Schema
- Type
- Owner
- Documentation
- Storage/data-source information
- Logical/physical aliases

Only do this if the chosen backend version supports these design-time events correctly.

### 3. Emit a parent pipeline lifecycle

Create one parent run for the KFP pipeline:

```text
START
  -> RUNNING, optionally
  -> COMPLETE, FAIL or ABORT
```

Use the real KFP run UUID as the OpenLineage parent `runId`, where possible.

Record run-level parameters such as:

- Collection
- Input volume
- Delta table
- Embedding model
- Embedding dimension
- Chunking strategy
- Pipeline image digest
- Pipeline version

### 4. Emit child component runs

Emit child lifecycle events for durable data transformations:

```text
acquire documents
parse and chunk
sync Delta table
embed chunks
write Milvus vectors
```

Each child should use `ParentRunFacet` pointing to the parent KFP run.

The same child run ID must be used for its `START` and terminal event.

### 5. Model the actual datasets

A recommended versioned path is:

```text
s3://poc-underwriting / volumes/
  -> table_sync
  -> unitycatalog://... / underwriting.claims.document_registry
     [symlink to physical Delta S3 location]
  -> table_embed
  -> milvus://... / default.underwriting_versioned
  -> uc-chat-versioned service declaration
```

Do not use a generic `claims.vectors` identity for multiple physical collections.

### 6. Use standard facets first

For the Delta output:

- `datasetVersion`
- `storage`
- `schema`
- `datasetType`
- `lifecycleStateChange`
- `outputStatistics`
- `symlinks`

For a Milvus output:

- `schema` for vector and metadata fields
- `datasetType`
- `outputStatistics`
- `dataSource`
- Proper custom vector facet for model, dimension and index information

For jobs:

- `jobType`
- `documentation`
- `sourceCodeLocation`
- `ownership`

For runs:

- `parent`
- `executionParameters`
- `processing_engine`
- `errorMessage`
- Tags for KFP, environment and deployment identifiers

### 7. Define proper RAG custom facets

Possible project-specific facets include:

#### `rhoai_documentProcessing`

- Documents attempted
- Documents parsed
- Documents skipped
- Parser and version
- Chunking strategy
- Chunk count

#### `rhoai_vectorIndex`

- Embedding model and immutable version
- Embedding dimension
- Normalisation method
- Vector database and collection
- Index type and parameters
- Vectors inserted, updated and deleted

#### `rhoai_documentBatch`

- Manifest URI and digest
- Document count
- Stable document IDs
- Batch or corpus version

The schemas should be versioned, immutable and validated in CI.

### 8. Avoid millions of graph nodes

For a very large corpus, creating one OpenLineage dataset node per document or chunk would make the graph unusable.

Use collection-level datasets in OpenLineage and retain document/chunk-level detail in:

- The Document Registry
- A versioned manifest
- Milvus metadata
- MLflow query traces

The OpenLineage event should reference the manifest and its digest so the exact membership of a collection can be recovered.

### 9. Keep query tracing in MLflow

MLflow should continue to capture:

- User or service identity
- Question
- Application and model version
- Collections searched
- Retrieved document and chunk IDs
- Similarity scores
- Source page and section
- Pipeline run IDs that created the chunks
- Generated response
- Latency, token usage and status

Do not create a Marquez RunEvent for every ordinary question unless there is a clearly defined lineage use case that MLflow cannot satisfy.

### 10. Make application registration design-time metadata

Represent `uc-chat`, `uc-chat-versioned` and `uc-agentic` as static jobs or services consuming their actual Milvus collection identities.

Do not create an artificial stored response dataset unless responses really are materialised in a named store.

If the backend lacks JobEvent support, document the compatibility workaround clearly rather than presenting a synthetic `COMPLETE` event as a real run.

### 11. Improve delivery reliability

A production emitter should provide:

- TLS verification
- Authentication
- Retry with backoff
- Local or message-queue buffering
- Dead-letter storage
- Delivery counters and alerts
- Idempotency handling
- Schema validation before submission

Lineage delivery may remain non-blocking for the data pipeline, but failed delivery must be visible and recoverable.

### 12. Test the graph, not only individual events

CI should validate:

- Every event against the selected OpenLineage schema
- Every custom facet against its custom schema
- Output identity from one job exactly matches the next input
- Different Milvus collections create different dataset identities
- `START` and terminal events use the same run ID
- Child events point at the correct parent run
- Failure paths emit `FAIL`
- Documentation examples are generated from tested fixtures

An integration test should submit a complete sample pipeline to a temporary Marquez instance and assert that the expected end-to-end graph can be traversed.

## Recommended target architecture

```text
DESIGN TIME

KFP compiler/deployment
  -> JobEvents for pipeline and component definitions

Unity Catalog / provisioning process
  -> DatasetEvents for volumes, tables and vector collections


RUNTIME INGESTION

KFP parent RunEvent START
  -> child acquire RunEvent START/COMPLETE
       -> raw S3 volume -> versioned staging manifest
  -> child parse RunEvent START/COMPLETE
       -> staging manifest -> parsed chunk dataset
  -> child embed RunEvent START/COMPLETE
       -> parsed chunks -> exact Milvus collection
  -> KFP parent RunEvent COMPLETE or FAIL


RUNTIME QUERY

User question
  -> MLflow/OpenTelemetry trace
       -> actual collection searched
       -> retrieved doc_id, document version, chunk and page
       -> originating pipeline_run_id
       -> model and prompt version
       -> generated answer
```

## Overall assessment

The POC demonstrates the right business problem and broadly selects the right systems. The Phase 1 lineage approach contains several strong ideas: lifecycle events, stable document identity, chunk-level provenance and a shared pipeline correlation ID. Phase 2 adds valuable Unity Catalog governance and Delta version history.

The main issue is that Phase 2 regressed from operational lineage into hand-built graph construction. Generic dataset names, completion-only events, synthetic application runs and unvalidated custom facets can make Marquez display a plausible graph without accurately representing what happened.

The best next step is not to redesign the entire platform. It is to combine:

- Phase 1's lifecycle and correlation model
- Phase 2's Unity Catalog and Delta versioning
- Canonical datasource-based identities
- Design-time JobEvents and DatasetEvents where supported
- Standard facets before custom ones
- Properly published RAG-specific custom schemas
- Automated schema and graph-continuity tests

This would preserve the useful POC architecture while making the lineage evidence far more accurate, reproducible and aligned with the intention of the OpenLineage specification.

## OpenLineage references

- [OpenLineage object model](https://openlineage.io/docs/spec/object-model/)
- [OpenLineage naming conventions](https://openlineage.io/docs/spec/naming/)
- [Facets and extensibility](https://openlineage.io/docs/spec/facets/)
- [Run facets](https://openlineage.io/docs/spec/facets/run-facets/)
- [Job facets](https://openlineage.io/docs/spec/facets/job-facets/)
- [Dataset facets](https://openlineage.io/docs/spec/facets/dataset-facets/)
- [Job type facet](https://openlineage.io/docs/spec/facets/job-facets/job-type/)
- [Symlinks dataset facet](https://openlineage.io/docs/1.46.0/spec/facets/dataset-facets/symlinks/)
- [Processing engine run facet](https://openlineage.io/docs/next/spec/facets/run-facets/processing_engine/)
