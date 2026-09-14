# RHOAI Lineage Contract and Integration Model

## Purpose

RHOAI lineage crosses independently developed systems such as Data Registry, DCH,
KFP, Spark, and Feature Store. RHOAI does not control the internals or release
cycles of all these upstream projects, but it can still define a stable contract at
the RHOAI integration boundary.

The purpose of this document is to describe that practical contract: what must be
standardized, where validation belongs, how Feast can be reused, and how users are
protected from having to construct complex lineage identities manually.

## Core conclusion

RHOAI should define a versioned **RHOAI Lineage Profile** built on OpenLineage.

OpenLineage supplies the general event model:

- datasets;
- jobs;
- runs;
- inputs and outputs;
- lifecycle events; and
- standard facets.

The RHOAI profile supplies platform-specific semantics:

- Data Registry asset and revision identity;
- project, tenant, and deployment authority;
- canonical physical dataset naming;
- logical-to-physical aliases;
- KFP root and child-run relationships;
- authorization and redaction rules;
- idempotency and duplicate-event behavior;
- assurance and partiality states; and
- supported integration and conformance levels.

Upstream projects do not need to adopt all of these concepts internally. RHOAI
integrations translate their native information into the profile.

## Reference architecture

```text
KFP / Spark / DCH / Feature Store integrations
                  |
                  v
        RHOAI Lineage Ingestion API
        - authenticate producer
        - validate RHOAI contract
        - normalize identities
        - redact sensitive fields
        - deduplicate and support replay
        - enrich with registry context
                  |
                  v
        Feast OpenLineage consumer/backend
                  |
                  v
        RHOAI Lineage Query API/BFF
        - asset-centric user questions
        - joins to Data Registry
        - authorization-filtered results
        - assurance and partiality status
```

The ingestion and query interfaces may be implemented by one RHOAI lineage service,
but they should remain separate logical responsibilities. Feast can remain the
operational event consumer and graph backend without becoming the public owner of
the entire RHOAI semantic contract.

An intermediary is therefore logically required, but it does not have to be a
separate product or network hop. It could be an RHOAI adapter, gateway, or ingestion
module in the lineage service.

## Identity contract

### Canonical dataset identities

OpenLineage identifies a dataset by the exact pair `(namespace, name)`. Facets do
not change dataset identity. RHOAI must define and normalize identities for each
supported data type.

Illustrative identities are:

```text
Data Registry asset:
  namespace: dataregistry://<deployment>/<project>
  name:      <asset-uuid>

S3 object or prefix:
  namespace: s3://<object-store>/<bucket>
  name:      <normalized-key-or-prefix>

Database table:
  namespace: jdbc://<database-authority>/<database>
  name:      <schema>.<table>

Volume:
  namespace: volume://<deployment>/<project>/<volume-id>
  name:      <path-or-prefix>
```

The exact schemes require an architecture decision. Once chosen, the contract must
define normalization rules for schemes, trailing slashes, encoding, case
sensitivity, table names, prefixes, and authorities. For example, `s3://` and
`s3a://`, or a path with and without a trailing slash, must not accidentally create
different datasets when they mean the same thing.

The OpenLineage namespace is an identity and authority scope, not merely a UI
folder. Deployment and project identifiers should be stable identifiers rather than
unreliable display names or ephemeral hostnames.

### Asset, revision, and content identity

These are separate concepts:

```text
Stable asset identity:
  Customer documents
  asset UUID = 01994c8e-...

Registry revision:
  asset UUID @ revision 3

Observed content identity:
  object version, hash, manifest, or table snapshot
```

The asset UUID is the durable business anchor. A registry revision may represent a
user-declared update, but it is not automatically proof that the underlying bytes
changed. A mutable S3 location does not become reproducible merely because the
registry revision was incremented.

RHOAI should distinguish at least:

- **Linked:** the logical asset is connected to a physical location;
- **Observed:** a run recorded evidence such as an object version, hash, ETag, or
  manifest; and
- **Reproducible:** the run used an immutable or retained source representation.

### Logical-to-physical aliases

A registry asset and the physical dataset seen by Spark or DCH are distinct
identities unless the contract connects them explicitly:

```text
dataregistry://cluster/project / <asset-uuid>
        == symlink ==
s3://sample-data / raw/documents.csv
```

The symlink means that the two identifiers refer to the same logical dataset. It
does not claim that the registry read, copied, hashed, or versioned the data. The
actual DCH or Spark event must still report the physical input it observed.

The contract must define whether assets may have multiple aliases, how aliases
change over time, and which users may see physical locations.

## Execution contract

Jobs, pipelines, runs, and attempts must not be collapsed into one identity.

```text
Pipeline definition: reusable KFP DAG or template
Job:                stable kind of recurring work
Run:                one execution of a job
Attempt:            one execution attempt, including a retry
Parent run:         orchestration relationship
```

KFP can own the root orchestration run. DCH, Spark, and Feature Store can own child
runs for work they actually observe. A `ParentRunFacet` expresses which execution
caused another execution to exist; dataset inputs and outputs express the data
dependency. These are complementary relationships.

Retries should receive new run IDs while retaining the same stable job identity and
parent pipeline context. Reusing a run ID for a retry would merge failures and
successes into one ambiguous execution.

The lifecycle contract should define a state model such as:

```text
START -> RUNNING* -> exactly one of COMPLETE, FAIL, or ABORT
```

It must also define event time, late events, cancellation, missing terminal events,
contradictory statuses, and the authoritative source when systems disagree. A
process crash that prevents a terminal event should not be represented as a false
`COMPLETE`; the query API may expose a derived `UNKNOWN` or `INCOMPLETE` state.

## Responsibilities of the independent projects

| Component | RHOAI integration responsibility |
|---|---|
| Data Registry | Own logical asset identity, revisions, metadata, and physical aliases. |
| KFP | Emit root pipeline lifecycle and propagate lineage context. It should not claim child I/O it did not observe. |
| DCH | Report source reads, ingestion, and observed source evidence for supported connectors. |
| Spark | Use native OpenLineage instrumentation for actual reads, writes, schemas, and transformations. |
| Feature Store | Report relevant materializations and preserve mappings to RHOAI assets and runs. |
| RHOAI ingestion service | Authenticate, validate, normalize, redact, deduplicate, and route events. |
| Feast | Persist and query operational lineage through an adapter. |
| RHOAI query service | Answer user questions by joining lineage events with Data Registry metadata and authorization. |

This allows RHOAI to standardize the supported integration without requiring
changes to KFP or Spark core.

## Validation and invalid values

Validation should happen in more than one place:

1. **Producer-side preflight:** catch errors before a workload starts where possible.
2. **RHOAI ingestion validation:** enforce the contract at the service boundary.
3. **Backend conformance:** verify that Feast or another backend preserves the
   required semantics.

Examples:

| Condition | Recommended behavior |
|---|---|
| Malformed asset ID | Reject during preflight or ingestion. |
| Asset does not exist | Reject with a useful correction. |
| Asset belongs to another project | Reject with an authorization error. |
| Invalid revision | Reject or mark the event unmapped. |
| Missing optional statistics | Accept with reduced detail. |
| Missing parent run | Reject in conformant mode; allow standalone operation in relaxed mode. |
| Non-canonical path | Normalize only when unambiguous; otherwise reject. |
| Duplicate event | Accept idempotently without creating another run. |
| Lineage backend unavailable | Queue and retry in normal operation; apply an explicit policy for governed mode. |

The service should not silently infer a registry asset from a similar-looking path,
or rewrite a user-supplied identity into another project. It should return a
machine-readable error with the field, reason, remediation, and correlation ID.

A Spark workload and lineage delivery can have separate statuses:

```text
Spark workload:       COMPLETE
Lineage delivery:     REJECTED
```

This prevents a lineage problem from being mistaken for a data-processing result,
while still making the missing lineage visible.

## User experience and communication

Documentation is necessary but insufficient. Requirements should be communicated
and enforced through several layers.

### Product documentation

Explain the RHOAI contract in user terms:

- how to connect a Data Registry asset to a pipeline;
- what linked, observed, and reproducible mean;
- which integrations are supported;
- what happens when lineage is incomplete; and
- what information is redacted.

### Component-specific guides

Spark users should receive a focused guide with supported images or launchers,
configuration examples, KFP examples, preflight checks, and common error messages.
KFP users need equivalent guidance for pipeline inputs, root runs, retries, and
child components. Users should not need to construct OpenLineage namespaces by hand.

### Paved paths and tooling

RHOAI should provide reusable KFP components, Spark launchers or wrappers, SDK
helpers, pipeline templates, and a preflight or lint command. The supported path
should derive deployment, project, root-run, and canonical namespace values wherever
possible.

### Runtime diagnostics

The UI and APIs should distinguish:

```text
LINKED
OBSERVED
REPRODUCIBLE
PARTIAL
UNMAPPED
DELIVERY_PENDING
DELIVERY_FAILED
```

These states are more useful than a generic success/failure label.

## Strict and relaxed operation

Not every exploratory workload should fail because a lineage backend is temporarily
unavailable. RHOAI should support explicit operating modes.

### Relaxed mode

The workload proceeds. Lineage is queued, marked partial, or reported as delivery
failed. This is appropriate for exploratory work.

### Conformant or governed mode

The workload must pass preflight validation. Selected lineage failures may block
execution or fail the pipeline, depending on the governance policy. This is
appropriate when lineage is part of a production acceptance requirement.

The mode must be visible to users and recorded with the run.

## Conformance testing

A conformance suite should exercise the full path:

1. Register an asset and publish its physical alias.
2. Start a KFP root run.
3. Emit a DCH child run for source ingestion.
4. Emit a native Spark run for transformation.
5. Emit a Feature Store materialization or consumption event.
6. Verify dataset identities, aliases, parent runs, and data edges.
7. Replay events and verify idempotency.
8. Test retries, cancellation, failure, late events, and missing facets.
9. Test authorization and redaction.
10. Test slightly different identities and ensure they do not silently merge.

The result should be a versioned capability matrix showing which features each
integration supports and where coverage is partial.

## Recommended implementation boundary

The practical recommendation is:

1. Define a versioned RHOAI Lineage Profile.
2. Build RHOAI adapters for KFP, Spark, DCH, and Feature Store.
3. Make the RHOAI lineage service the supported ingestion boundary.
4. Forward validated events to Feast through an adapter.
5. Use the same service, or a companion BFF, for asset-centric lineage queries.
6. Accept external OpenLineage events with reduced RHOAI coverage rather than
   presenting them as fully conformant.
7. Make validation and conformance behavior visible through documentation, tooling,
   runtime diagnostics, and UI status.

The objective is not to control every upstream project. It is to ensure that every
RHOAI-supported integration produces lineage that is predictable, joinable, secure,
and honest about its coverage.

## Related material

- [RHOAI OpenLineage strategy](rhoai-ol-strategy.md)
- [OpenLineage event catalog](sample-app-best-practices/docs/event-catalog.md)
- [OpenLineage design guide](sample-app-best-practices/docs/openlineage-design.md)
- [Feast versus Marquez conformance](sample-app-best-practices/docs/feast-vs-marquez-conformance.md)
