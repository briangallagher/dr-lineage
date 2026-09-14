# OpenLineage Producers and Upstream Integration Boundary

Status: design summary, 2026-09-13

## Purpose

This document captures the main conclusions about Spark, KFP, and DCH as
OpenLineage producers, with particular attention to parent-run propagation and
the boundary between generic upstream contributions and RHOAI-specific logic.

The intended audience is the authors and reviewers of the RHOAI lineage
architecture and related ADRs.

## Executive summary

An OpenLineage producer is the component that observes an execution or dataset
operation and emits OpenLineage events. It is not necessarily a separate
service.

- Spark is a data-plane producer. Its native OpenLineage listener observes Spark
  reads, writes, jobs, and SQL executions and emits lineage events.
- DCH is an ingestion or connector producer. It reports the external data it
  actually read, the output it created or populated, and any safe source
  evidence it observed.
- KFP is primarily an orchestration producer. It owns the pipeline root run and
  its lifecycle, then propagates execution context to child systems.
- The Data Registry is a governed dataset identity producer. It can emit a
  `DatasetEvent` and a symlink between a Registry asset and a physical dataset;
  it should not manufacture a processing run that it did not perform.

The desired user experience is that users select business inputs, such as a
Registry asset or revision, while the platform propagates technical context
such as parent run IDs, child run IDs, attempts, and event-delivery settings.
Users should not have to copy OpenLineage IDs between components.

## Two kinds of lineage relationships

The design must keep these relationships separate:

```text
Dataset output == Dataset input
    means: data dependency

ParentRunFacet(child -> parent run)
    means: execution hierarchy
```

For example, Spark can report that it read a staged CSV and wrote transformed
Parquet. A `ParentRunFacet` separately records that the Spark execution was
caused by a KFP pipeline run.

Neither relationship replaces the other.

## Spark as a producer

Spark becomes a producer by installing and enabling the OpenLineage Spark
listener in the Spark runtime. The listener observes Spark's own execution
events and sends `RunEvent` messages to the configured OpenLineage endpoint.

The minimum configuration includes:

```text
spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener
spark.openlineage.transport.type=http
spark.openlineage.transport.url=<lineage-endpoint>
spark.openlineage.namespace=<spark-job-namespace>
spark.openlineage.appName=<stable-application-name>
spark.openlineage.applicationRunId=<unique-spark-run-id>
```

When Spark is launched by KFP, the launcher also supplies the parent identity:

```text
spark.openlineage.parentJobNamespace=<kfp-namespace>
spark.openlineage.parentJobName=<kfp-job-name>
spark.openlineage.parentRunId=<kfp-run-id>
```

The Spark run must retain its own unique run ID. It must not reuse the KFP root
run ID.

The normal producer setup is:

1. Build a Spark image with a compatible OpenLineage runtime.
2. Enable the native listener.
3. Configure the transport and Spark job/run identity.
4. Configure parent context when Spark has a real parent.
5. Run normal Spark reads and writes using canonical dataset identities.
6. Verify lifecycle events, inputs, outputs, facets, retries, and failures.
7. Reconcile native lineage status with SparkApplication status where the
   integration can produce contradictory or incomplete terminal events.

The native listener should be the sole owner of Spark lineage events. A KFP
submission helper should create and wait for the Spark application, but should
not emit a second manual Spark event.

## Passing parameters and facets to Spark

Three channels should be distinguished:

1. **Application arguments** control the Spark program, for example input URI,
   output URI, asset reference, or revision.
2. **Spark configuration** controls the OpenLineage integration, including
   transport, Spark identity, parent identity, and facet enablement.
3. **OpenLineage facets** are metadata attached to the run, job, input, or
   output event.

An arbitrary Spark configuration value does not automatically become an
OpenLineage facet. For example, setting `spark.asset.id` does not by itself
create an `assetId` facet.

Standard Spark-derived metadata, such as supported input/output, schema, and
column-lineage information, can be produced by the native listener. Other
metadata should be handled by a supported integration, a custom facet builder,
or an explicit application-level producer that does not duplicate Spark's
native events.

Important rules:

- Use `ParentRunFacet` for execution hierarchy.
- Use `ExecutionParametersRunFacet` for safe, behavior-affecting parameters.
- Use dataset facets or Registry symlinks for dataset metadata and identity
  relationships.
- Do not put credentials, bearer tokens, signed URLs, or arbitrary environment
  variables into facets.
- Prefer standard facets before introducing RHOAI-specific custom facets.

In the current sample, `asset_id` and the staged URI are ordinary component or
application inputs. The Spark submission helper uses them to construct the
output identity and Spark arguments. The KFP parent ID is translated into
OpenLineage Spark configuration, which the listener turns into a
`ParentRunFacet`.

## Parent-context propagation

The reliable pattern is:

```text
Parent creates or owns context
        |
        v
Launcher transports context
        |
        v
Child producer emits ParentRunFacet
```

The child should not start by querying the lineage backend to guess its parent.
That is ambiguous in the presence of concurrent runs, retries, duplicate job
names, delayed events, and eventual consistency. The lineage backend stores and
resolves a relationship that the execution system already knows; it should not
invent that relationship.

The transport does not need to be the same for every component:

- KFP task pods may receive context through environment variables, a mounted
  file, pod metadata, or a runtime API.
- A Spark launcher should translate the context into Spark configuration because
  that is Spark's native configuration channel.
- DCH may receive the context in an API request field, header, connection
  object, environment variable, or context file.

The environment variable is therefore an implementation detail, not the
lineage contract. Spark does not need to be hard-coded specifically for KFP; it
needs to consume generic parent job/run information. DCH has the same
requirement.

OpenLineage standardizes the `ParentRunFacet` event shape, but the transport
mechanism remains integration-specific. A generic JSON context envelope is a
useful architectural pattern, but it should not be assumed to be universally
implemented across all current integrations.

## KFP's producer role

KFP should produce the root orchestration lifecycle:

```text
pipeline START -> pipeline COMPLETE / FAIL / ABORT
```

Native KFP support should:

1. Use the real KFP pipeline run ID as the root run ID.
2. Emit the root lifecycle from a reliable control-plane or durable run-state
   mechanism.
3. Persist or expose a versioned execution context.
4. Inject that context into supported task pods and external-job launchers.
5. Distinguish pipeline, task, child-run, and retry/attempt identities.
6. Emit safe pipeline parameters and failure information.
7. Support delivery retry, idempotency, and reconciliation.

KFP should not emit DCH or Spark data-lineage events, because KFP did not
perform those operations. It should provide the context that lets DCH and
Spark report themselves as children of the pipeline.

## DCH's producer role

DCH should own the ingestion or connector event because it observes the remote
access:

```text
external source -> DCH -> staged or registered dataset
```

DCH support should include:

1. An optional generic parent-context input.
2. A unique DCH run ID per execution or retry attempt.
3. `START`, terminal, and failure events.
4. The physical source identity actually read.
5. The Registry asset or revision produced or populated, where known.
6. Safe connection metadata and observed evidence such as ETag, object version,
   size, schema snapshot, or manifest.
7. Idempotency, replay, delivery retry, and authorization checks.
8. Independent-run mode when DCH is invoked without KFP.

KFP should not manufacture a DCH event. If DCH is invoked by KFP, the DCH run
should contain a parent reference to KFP. If DCH is invoked independently, it
should create its own root run and must not fabricate a KFP parent.

## End-to-end result

The desired graph is:

```text
Registry asset
   == symlink ==
physical external dataset
   |
   v
DCH ingestion run
   |
   v
staged dataset
   |
   v
Spark transformation run
   |
   v
transformed dataset
```

The execution hierarchy is separate:

```text
KFP root run
   +-- DCH child run
   +-- Spark child run
   +-- other child runs
```

This allows RHOAI to answer questions such as:

- Where did a governed asset come from?
- Which pipeline and runs used it?
- Which derived datasets were produced?
- Which parameters and processing engines were involved?
- Where did a failure occur?
- Which downstream work may be affected by a change?

This remains operational lineage. A Registry UUID or lineage event does not by
itself prove the exact bytes processed. Immutable versions, manifests, object
versions, or equivalent evidence are needed for stronger reproducibility claims.

## Upstream contribution boundary

Upstream-first contributions are desirable and should be the default strategy.
The upstream projects should receive generic capabilities that are useful beyond
RHOAI.

### Appropriate upstream contributions

**KFP:**

- generic run, task, and attempt context;
- supported context injection into task pods;
- lifecycle observer or exporter extension points;
- propagation hooks for external job launchers;
- optional OpenLineage integration;
- durable lifecycle and delivery semantics.

**DCH:**

- optional parent-execution context in ingestion/discovery APIs;
- generic lifecycle/event hooks;
- source/output identity and evidence hooks;
- optional OpenLineage producer integration;
- independent and parented execution modes.

**Spark or Spark launchers:**

- generic parent-context translation;
- native OpenLineage configuration;
- custom facet extension points where appropriate;
- clear behavior for unmanaged jobs without parent context.

### RHOAI-specific responsibilities

RHOAI should own:

- Data Registry asset and revision references;
- Registry-to-physical symlink rules;
- RHOAI project authorization and redaction policy;
- canonical RHOAI namespace conventions;
- endpoint discovery and backend configuration;
- Registry-aware UI and asset-centric queries;
- assurance levels such as Linked, Observed, and Reproducible;
- Feast/Marquez integration and reconciliation policy.

Upstream projects should not directly depend on RHOAI Data Registry, Feast,
Marquez, or RHOAI-specific environment variables.

## Main risks and design constraints

- The shared context must be versioned and extensible.
- Parent context is untrusted input and requires validation and authorization.
- Parent context must not contain credentials or signed locations.
- KFP, DCH, and Spark must agree on stable identity and retry semantics.
- Native Spark events must not be duplicated by KFP launchers.
- Missing context should produce an independent run or an explicit warning, not
  a guessed parent.
- Upstream OpenLineage support should be optional where a hard dependency would
  create adoption or maintenance concerns.
- Final status may require reconciliation between OpenLineage events and the
  authoritative KFP, DCH, or Spark status APIs.

## Open questions for the ADR

1. Should KFP upstream expose a generic execution-context API, a lifecycle
   exporter interface, or both?
2. Where should the shared context schema be governed and versioned?
3. Should RHOAI initially use the existing individual Spark parent properties,
   or contribute a generic JSON context mechanism upstream?
4. What is the supported transport for DCH context: request field, header,
   connection object, environment variable, or file?
5. Which first-party KFP components are guaranteed to propagate context?
6. How are Registry revisions represented and authorized across all producers?
7. Which system is authoritative when native OpenLineage and platform status
   disagree?
8. What delivery guarantees, outbox behavior, replay, retention, and duplicate
   handling are required for production?

## Related material

- [OpenLineage design guide](./openlineage-design.md)
- [KFP/DCH native OpenLineage support](./kfp-dch-native-openlineage-support.md)
- [RHOAI lineage service architecture](./rhoai-lineage-service-architecture.md)
- [Event catalog](./event-catalog.md)
- [Spark submission implementation](../src/lineage_demo/spark_submit.py)
