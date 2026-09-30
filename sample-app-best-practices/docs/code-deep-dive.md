# Code deep dive: from asset registration to lineage

This guide follows one successful scenario chronologically. The key path is:

    register asset -> DatasetEvent -> submit KFP run -> root START
        -> ingest -> Spark submit/transform -> embeddings -> root terminal event

Keep three layers separate:

1. KFP definition: when tasks run and which values flow between them.
2. Application image: the CLI, registry, ingestion, and embedding implementations.
3. Spark image: the actual PySpark transformation.

## 1. Before a run: compile and upload

[scripts/deploy.sh](../scripts/deploy.sh) creates the ol-best-practices OpenShift resources, builds the application and Spark images, and calls the compiler.

[pipeline/compile.py](../pipeline/compile.py) calls build_pipeline(app_image, spark_image) from [pipeline/pipeline.py](../pipeline/pipeline.py), producing build/pipeline.yaml. KFP executes that compiled YAML, not the Python source directly.

Pipeline-related files:

| File | Role |
| --- | --- |
| pipeline/pipeline.py | KFP DAG, component interfaces, dependencies, retries, and environment injection. |
| pipeline/compile.py | Compiles the Python definition to build/pipeline.yaml. |
| scripts/upload-pipeline.sh | Uploads the compiled package to KFP. |
| src/lineage_demo/scenarios.py | Creates KFP runs and waits for them. |
| src/lineage_demo/cli.py | Maps container commands to implementation functions. |
| src/lineage_demo/processing.py | Ingestion and mock embeddings. |
| src/lineage_demo/spark_submit.py | Creates and waits for a SparkApplication. |
| spark/transform.py | Actual PySpark transformation. |
| src/lineage_demo/lifecycle.py | KFP root START and terminal events. |

`scripts/upload-pipeline.sh` records the selected pipeline and uploaded version in
`build/pipeline-reference.json`. The scenario runner uses those IDs to submit runs
from the KFP template, so each report can prove which uploaded version was executed.

pipeline.py does not normally emit events itself. Its components invoke commands in the application image. Those commands emit events at runtime. root_end is the exception in implementation style: its generated component imports and calls finish_root directly.

## 2. Where the first DatasetEvent is emitted

The first DatasetEvent in a normal scenario is emitted during asset registration, before the first KFP run:

    scenarios.execute()
      -> register_asset()
      -> POST /v1/assets
      -> RegistryService.create()
      -> RegistryService._deliver()
      -> LineageEmitter.dataset_event()

The source locations are:

- [scenarios.py](../src/lineage_demo/scenarios.py): register_asset posts the asset with idempotency key default-source.
- [registry_api.py](../src/lineage_demo/registry_api.py): RegistryService.create allocates and persists the asset, then calls _deliver.
- [events.py](../src/lineage_demo/events.py): LineageEmitter.dataset_event constructs and sends the DatasetEvent.

RegistryService._deliver creates the logical identity:

    namespace = dataregistry://<cluster>/<project>
    name      = <asset UUID>

It attaches a SymlinksDatasetFacet pointing to:

    namespace = s3://sample-data
    name      = raw/documents.csv

The record is persisted before delivery. If delivery fails it remains PENDING, so a later idempotent registration can retry. Reusing the same idempotency key after successful delivery returns the existing asset without emitting a second DatasetEvent.

The first RunEvent is different: it is the KFP root START event emitted after KFP begins the run.

## 3. What triggers the first run?

After registering the asset, scenarios.py calls run_pipeline. It invokes the KFP client
method `run_pipeline` with the uploaded pipeline and version IDs from
`build/pipeline-reference.json`:

    experiment_id = <OpenLineage best practices experiment>
    pipeline_id = <uploaded pipeline ID>
    version_id = <uploaded pipeline version ID>
    params = {asset_id: asset_id, failure_mode: failure_mode}

After KFP reports the run complete, the runner calls `get_run` and fails unless the
returned `pipeline_version_reference` matches those same IDs. The compiled
`build/pipeline.yaml` remains the upload input and reproducibility artifact; it is not
submitted as an inline run spec.

That explicit KFP API call triggers the first run. There is no message queue, storage notification, or automatic event subscription in this demo.

KFP schedules root_start, which runs the application command:

    lineage-demo root-start --root-run-id-path <output file>

The CLI dispatches to lifecycle.start_root, which emits the root RunEvent START. The ingest task is then enabled by the .after(started) dependency in pipeline.py.

## 4. Reading pipeline.py

build_pipeline defines five component shapes:

1. root_start
2. root_end
3. ingest_component
4. spark_component
5. embedding_component

The component definitions specify image, command, arguments, and input/output paths. For example, ingest_component only says to run lineage-demo ingest; it does not contain the ingestion implementation.

The resulting DAG is:

    root_start
        |
        v
    ingest_component -- staged_uri --> spark_component
                                          |
                                          v
                                  embedding_component

root_end runs through dsl.ExitHandler regardless of success or failure.

The Spark transformed_uri output becomes the embedding input. These outputs are files written by the containers and mapped by KFP into downstream string inputs. Caching is disabled so each run produces fresh lineage. Ingest, Spark submission, and embedding each have one retry.

## 5. How lineage parameters reach components

There are four parameter channels.

### Pipeline parameters

The user-facing pipeline parameters are:

    asset_id       logical Data Registry asset UUID
    failure_mode   none, ingest, spark, or embed

They enter through the KFP run request and are substituted into component command arguments.

### KFP output files

| Producer | Output | Consumer |
| --- | --- | --- |
| root_start | root run ID file | component output; other tasks reconstruct the root from KFP_RUN_ID |
| ingest | staged URI and ingest run ID | Spark consumes the staged URI |
| spark-submit | transformed URI and Spark run ID | Embedding consumes the transformed URI |
| embed | embedding URI and embedding run ID | final artifact/output |

cli.py has a _write helper that creates the parent directory and writes these values. KFP handles the file-to-file wiring.

### ConfigMap and Secret environment

configure_task in pipeline.py injects values from application-config:

    PROJECT_NAMESPACE
    CLUSTER_NAME
    REGISTRY_URL
    MARQUEZ_URL
    S3_ENDPOINT
    S3_REGION

Storage tasks also receive AWS credentials from object-store-credentials. [config.py](../src/lineage_demo/config.py) loads this environment into Settings.

### KFP run correlation

configure_task injects:

    KFP_RUN_ID = metadata.labels['pipeline/runid']

This uses the Kubernetes Downward API. The RHOAI KFP version used here left a normal placeholder literal when it was passed as a custom argument, but it did put the real UUID on the pod label. The components therefore read the label through the environment.

The CLI defaults pipeline-job-id to KFP_RUN_ID. Each component passes it to root_run_id, so ingestion, Spark, and embeddings all refer to the same OpenLineage root run.

## 6. Root lifecycle and event order

[lifecycle.py](../src/lineage_demo/lifecycle.py) owns the KFP root events.

start_root uses the KFP UUID as the root run ID and emits a START event with job namespace kfp://<cluster>/<project> and job name sample-app-best-practices.

Ingestion and embeddings each create their own UUIDv7 run ID and emit a START event before doing work. Their ParentRunFacet points to the KFP root. Spark has a child run too, but its events are emitted by the native Spark listener.

dsl.ExitHandler schedules root_end when the pipeline finishes. finish_root maps KFP status as follows:

| KFP status | OpenLineage state |
| --- | --- |
| SUCCEEDED or COMPLETE | COMPLETE |
| FAILED or FAIL | FAIL |
| CANCELLED, CANCELED, or ABORTED | ABORT |

Failures also receive an errorMessage facet.

## 7. Ingestion: where it lives and what triggers it

The implementation is [processing.py](../src/lineage_demo/processing.py), function ingest. The KFP task runs:

    lineage-demo ingest --asset-id <id> --failure-mode <mode>
      --staged-uri-path <file> --ingest-run-id-path <file>

The CLI dispatch is in [cli.py](../src/lineage_demo/cli.py). ingest:

1. Creates an attempt-specific UUIDv7 run ID.
2. Calls registry_client.get_asset over HTTP.
3. Converts the asset location into the canonical raw dataset identity.
4. Derives s3://<bucket>/staging/<asset-id>/<ingest-run-id>/documents.csv.
5. Emits RunEvent START with the raw dataset as input.
6. Reads and validates the CSV from S3.
7. Writes the staged CSV with create-only semantics.
8. Emits RunEvent COMPLETE with the staged dataset as output.
9. Emits RunEvent FAIL on an exception, then re-raises for KFP retry/failure.

The trigger is ordinary DAG scheduling by KFP. The registry lookup inside ingestion is an HTTP read, not an event subscription.

## 8. Spark: submission versus execution

The Spark task runs lineage-demo spark-submit, which dispatches to submit_and_wait in [spark_submit.py](../src/lineage_demo/spark_submit.py). It:

1. Creates a UUIDv7 Spark run ID.
2. Builds a SparkApplication custom resource.
3. Passes the staged URI and derived output URI as Spark arguments.
4. Configures the native OpenLineage listener, Marquez URL, Spark namespace, application run ID, parent job, and root run ID.
5. Creates the resource through the Kubernetes API and polls its status.
6. Writes the transformed URI and Spark run ID to KFP output files.

The actual transformation is [spark/transform.py](../spark/transform.py). It reads the staged CSV, normalizes title/category/text, calculates word_count, and writes Parquet to:

    s3://<bucket>/transformed/<asset-id>/<spark-run-id>/

The SparkSession installs io.openlineage.spark.agent.OpenLineageSparkListener. That listener observes Spark reads and writes and emits native Spark events. The Python application deliberately does not emit duplicate Spark RunEvents.

spark_submit.py is the control-plane side; spark/transform.py is the data-plane job.

## 9. Embeddings: where it lives and what triggers it

The final KFP task runs:

    lineage-demo embed --asset-id <id> --transformed-uri <URI>
      --failure-mode <mode> --embedding-uri-path <file>
      --embedding-run-id-path <file>

The implementation is processing.py, function embed. KFP triggers it because it consumes the Spark task's transformed_uri output; there is no separate event listener or queue.

embed:

1. Creates a UUIDv7 embedding run ID.
2. Derives s3://<bucket>/embeddings/<asset-id>/<run-id>/vectors.jsonl.
3. Emits RunEvent START with transformed Parquet as input.
4. Lists and downloads Parquet objects from S3.
5. Creates deterministic eight-dimensional vectors using SHA-256.
6. Writes JSONL with create-only semantics.
7. Adds schema, statistics, and column-lineage facets.
8. Emits RunEvent COMPLETE with embeddings as output, or FAIL on error.

## 10. What UUIDv7 is used for

The implementation is identities.py, function uuid7. It creates an RFC 9562 UUIDv7 using only the standard library: a millisecond timestamp followed by UUID version/variant bits and randomness.

It is used for:

- Data Registry asset IDs.
- Ingestion run IDs.
- Spark run IDs and transformed-output directory names.
- Embedding run IDs and embedding-output directory names.

The benefits are uniqueness and approximate chronological ordering, without adding a UUIDv7 package dependency.

The KFP root run is different: root_run_id uses KFP's native UUID when available, and a deterministic UUIDv5 fallback when the supplied ID is not a UUID. Child runs use fresh UUIDv7 values and connect to the root through ParentRunFacet.

## 11. Why wrap OpenLineageClient?

[events.py](../src/lineage_demo/events.py) wraps the official client in LineageEmitter so all producers share the same delivery behavior and event construction.

The wrapper provides:

- One place to configure the HTTP transport and Marquez URL.
- Client-level retries disabled and application-controlled retries with exponential backoff.
- A clear LineageDeliveryError after all attempts fail.
- Consistent DatasetEvent, RunEvent, and JobEvent construction.
- One producer declaration and timestamp helper.
- Dependency injection of client and sleep function for tests.
- A close hook for clients that own transport resources.

This prevents a network failure from silently disappearing inside a library retry policy. It also lets the registry persist first and mark an asset DELIVERED only after the DatasetEvent has been accepted by the client call. It is not a distributed transaction between the registry store, S3, and Marquez.

## 12. Where Marquez is configured

Yes: the intended destination for the application's OpenLineage events is the Marquez API backend.

For Python events, Settings.marquez_url defaults to http://marquez:80 in [config.py](../src/lineage_demo/config.py). OpenShift overrides it through application-config; pipeline.py injects MARQUEZ_URL into containers. LineageEmitter passes it to the official client's HTTP transport. The registry service uses the same setting.

For Spark events, build_spark_application places these settings in sparkConf:

    spark.openlineage.transport.type = http
    spark.openlineage.transport.url  = settings.marquez_url

It also supplies the Spark namespace, application run ID, parent information, and root information. The native listener sends its events to the same Marquez service.

The Marquez deployment and service are in [openshift/storage.yaml](../openshift/storage.yaml). The scripts expose it locally as 127.0.0.1:5000 only for inspection; containers use http://marquez:80 in-cluster.

## 13. Dataset identities and graph joins

OpenLineage identifies a dataset by the complete pair (namespace, name). [identities.py](../src/lineage_demo/identities.py) centralizes those pairs so components cannot silently split the graph with different spellings.

| Logical object | Namespace | Name pattern |
| --- | --- | --- |
| Registered asset | dataregistry://<cluster>/<project> | <asset-id> |
| Raw source | s3://<bucket> | raw/documents.csv |
| Staged CSV | s3://<bucket> | staging/<asset-id>/<ingest-run-id>/documents.csv |
| Transformed Parquet | s3://<bucket> | transformed/<asset-id>/<spark-run-id> |
| Embeddings | s3://<bucket> | embeddings/<asset-id>/<run-id>/vectors.jsonl |

The registry symlink connects the logical asset to the raw source. Ingestion connects raw to staged, Spark connects staged to Parquet, and embedding connects Parquet to JSONL. Those exact identity pairs allow Marquez to draw the graph.

The job namespaces are intentionally distinct:

    kfp://<cluster>/<project>
    dch-mock://<cluster>/<project>
    spark://<cluster>/<project>

## 14. Suggested source walkthrough

For one successful run, inspect in this order:

1. [scripts/run-scenarios.sh](../scripts/run-scenarios.sh): services and scenario runner.
2. [scenarios.py](../src/lineage_demo/scenarios.py): registration before KFP submission.
3. [registry_api.py](../src/lineage_demo/registry_api.py): first DatasetEvent and symlink.
4. [pipeline.py](../pipeline/pipeline.py): DAG dependencies and parameters.
5. [lifecycle.py](../src/lineage_demo/lifecycle.py): root START.
6. [processing.py](../src/lineage_demo/processing.py): ingestion and embeddings.
7. [spark_submit.py](../src/lineage_demo/spark_submit.py): Spark CR and native OpenLineage configuration.
8. [spark/transform.py](../spark/transform.py): actual Spark read/write.
9. [events.py](../src/lineage_demo/events.py) and [facets.py](../src/lineage_demo/facets.py): delivery and event metadata.
10. [event-catalog.md](event-catalog.md): event ownership and meaning.
11. [runbook.md](runbook.md): live inspection and troubleshooting.

While stepping through, ask: which exact (namespace, name) is being emitted; which run owns it; what is its parent/root; and is the event emitted by Python, the Spark listener, or KFP?

## 15. Boundaries worth remembering

- The registry is a small mock service backed by an OpenShift ConfigMap, not the full RHOAI Data Registry product.
- The raw S3 object is mutable. A direct S3 overwrite emits no event.
- The demo's assurance level is Linked: it records what an instrumented run reported, not immutable proof of source bytes.
- Spark lineage comes from the native listener; adding a manual Spark event would risk duplicates or conflicting terminal states.
- KFP root status and SparkApplication status are authoritative for final status. The pinned Spark/OpenLineage integration has documented failure-event ordering limitations.
- facets.py is the vocabulary layer; processing code decides when facets are attached.

The key distinction is: pipeline.py decides when containers run and which values flow between them; src/lineage_demo and spark/ decide what they do and which events they emit.
