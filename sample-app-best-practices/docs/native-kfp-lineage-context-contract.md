# Native KFP lineage context and root-event contract

Status: proposed RHOAI integration contract with an executable fixture model,
2026-09-30. This document does not claim that the pinned KFP 2.16.1 deployment
is a native OpenLineage producer.

## Purpose and boundary

KFP owns the orchestration execution. A future RHOAI/KFP integration should
emit the OpenLineage root lifecycle from the KFP control plane and make the
same context available to first-party task integrations. The current fixture
still emits a user-authored root shim and workload-owned child events; it is a
test of the proposed shape, not implementation evidence for KFP itself.

The contract has four deliberately separate ownership boundaries:

| Concern | Owner | KFP contract contribution |
| --- | --- | --- |
| Data-asset identity and optional revision | Data Registry | Carry an authorized logical reference; do not invent a revision. |
| Orchestration root and task context | KFP/RHOAI integration | Emit the root lifecycle and propagate context. |
| Actual source read or transformation | DCH, Spark, or workload producer | Emit the child event and the physical inputs/outputs it observed. |
| AI runs, artifacts, evaluations, and traces | MLflow | Link to the root/asset context without becoming the data-asset catalogue. |

This preserves the Data Registry boundary recorded in the RHOAI knowledge
project: Data Registry owns data assets, while MLflow owns AI-asset records.
The context is a correlation contract, not a replacement for either system.

## Evidence boundary

The fixture and the 2026-09-30 Slice 2 trial provide these observed facts:

- the accepted KFP run was re-read with its uploaded `pipeline_id` and
  `pipeline_version_id`;
- the running KFP 2.16.1 path exposed the real run UUID through the pod label
  `pipeline/runid`; and
- the adapter produced a governed child run under that UUID and linked the
  observed Data Registry asset, source bytes, MLflow run, and model artifact.

They do not prove a KFP control-plane OpenLineage producer, a supported KFP
context API, automatic KFP-to-MLflow tracking, or a durable Registry revision.
The `pipeline/runid` label is therefore a compatibility observation, not the
native contract. A native implementation must provide a supported API or
versioned injection mechanism and must not depend on a user-authored exit
handler to establish the root lifecycle.

## Identity rules

The following identities must remain distinct:

| Identity | Required meaning |
| --- | --- |
| Pipeline ID | Immutable KFP pipeline definition identity. |
| Pipeline version ID | Immutable uploaded definition/version identity used for the run. |
| Root run ID | The exact KFP run UUID for one pipeline execution. |
| Task ID/name | One logical task in the pipeline definition. Stable across retries. |
| Task run ID | One producer-owned child execution. It must differ for every attempt. |
| Attempt | Positive ordinal for the task execution attempt, starting at 1. |

The root OpenLineage job uses:

```text
namespace: kfp://<stable-deployment-id>/<project>
name:      <pipeline-display-name>
```

`pipeline_id` and `pipeline_version_id` are retained in the KFP context facet
because a display name is not sufficient provenance. The deployment ID must be
stable across restarts and must not contain a token, hostname-derived secret,
or per-pod value. The fixture's current cluster-derived namespace is historical
POC behavior until a platform-wide deployment identity is agreed.

The native path must use the exact KFP run UUID as the OpenLineage root run ID.
The fixture's UUIDv5 fallback for non-UUID test identifiers is adapter
compatibility behavior and is not conformant native-KFP behavior.

## Versioned context envelope

The transport-neutral envelope is versioned independently of OpenLineage event
serialization:

```json
{
  "profile": "rhoai-kfp",
  "version": "1.0",
  "deployment": "stable-deployment-id",
  "project": "scenario-b",
  "root": {
    "jobNamespace": "kfp://stable-deployment-id/scenario-b",
    "jobName": "governed-asset-to-model",
    "runId": "42db060a-d89d-40d1-a7cb-28d6703b5d07"
  },
  "pipeline": {
    "id": "7e44ad4e-f986-43c3-90d4-c6df8b73c783",
    "versionId": "4707191e-5805-4314-8be6-87fadd84a7b9",
    "name": "governed-asset-to-model"
  },
  "parent": {
    "jobNamespace": "kfp://stable-deployment-id/scenario-b",
    "jobName": "governed-asset-to-model",
    "runId": "42db060a-d89d-40d1-a7cb-28d6703b5d07"
  },
  "task": {
    "name": "train-governed-baseline",
    "id": "train-governed-baseline",
    "runId": "a2fef4ea-9654-4e6d-9b64-53599bbf33cd",
    "attempt": 1
  }
}
```

The root envelope omits `parent` and `task`. A direct task's parent is the KFP
root. An integration that creates a nested child may replace `parent.runId`
with the immediate parent while retaining the unchanged root. A child must
always retain enough context to reconstruct both relationships.

The envelope contains identifiers and safe metadata only. It must not contain
service-account tokens, passwords, Secret values, signed URLs, raw credentials,
or arbitrary environment-variable snapshots. Data Registry references may be
carried as a separate, authorized business input, but a physical location is
not silently substituted for a logical asset identity.

### Proposed transport

For first-party KFP integrations, KFP should inject a read-only context file and
the file path through a documented runtime contract, for example:

```text
RHOAI_LINEAGE_CONTEXT_PATH=/var/run/rhoai/lineage/context.json
RHOAI_LINEAGE_PROFILE_VERSION=1.0
```

Pod labels/annotations may provide safe selectors and diagnostics, but are not
the context API. A raw pipeline parameter is also not the context API: users
should select a governed asset, not copy a live root UUID through every task.

The exact mount and API mechanism remains an implementation decision for the
KFP/RHOAI integration. The envelope, identity semantics, redaction rules, and
compatibility behavior are the contract that must remain stable if transport
changes.

The local Draft 2020-12 schema is
`contracts/rhoai-kfp-lineage-context-1.0.schema.json`. Its `urn:` identifier
is intentionally not a published product URL; schema publication remains an
open decision.

The fixture reader in `src/lineage_demo/kfp_lineage.py` accepts only this
profile/version, validates the root and direct-parent identities, and exposes
no context contents in its file-read error. Unsupported or malformed context
must fail closed; a task must not guess a parent from a pod name, display name,
or pipeline parameter.

## Root OpenLineage event

KFP is the only owner of the orchestration root lifecycle in the native path.
It emits standard OpenLineage `RunEvent` records:

1. `START` once the run is accepted and the root identity is known;
2. optional `RUNNING` transitions only if the producer and consumer support
   them consistently; and
3. exactly one terminal `COMPLETE`, `FAIL`, or `ABORT` event from the
   authoritative KFP run state.

The root event has no fabricated data inputs or outputs. It anchors execution;
the task that actually reads or writes data owns those dataset edges.

The root `Run` carries a proposed `rhoaiKfp` context facet containing:

```json
{
  "profile": "rhoai-kfp",
  "profileVersion": "1.0",
  "deployment": "stable-deployment-id",
  "project": "scenario-b",
  "pipelineId": "7e44ad4e-f986-43c3-90d4-c6df8b73c783",
  "pipelineVersionId": "4707191e-5805-4314-8be6-87fadd84a7b9",
  "pipelineName": "governed-asset-to-model",
  "rootRunId": "42db060a-d89d-40d1-a7cb-28d6703b5d07"
}
```

The facet is profile-specific and proposed; it is not an OpenLineage standard
facet. Its schema URL and producer revision must be governed before a product
implementation advertises it as supported. Safe pipeline parameters may be
included through a standard execution-parameters facet, subject to an explicit
redaction policy. KFP must not serialize the complete parameter map by default.

The root job may also be published as a `JobEvent`, but a `JobEvent` is not a
run and does not replace the root `RunEvent` lifecycle.

## Child context and parent facet

KFP supplies context; it does not claim an operation it did not observe. DCH,
Spark, MLflow integrations, or a custom workload own their child event when
they own the corresponding operation.

A direct child uses the standard `ParentRunFacet` with both the immediate
parent and explicit root:

```json
{
  "parent": {
    "job": {
      "namespace": "kfp://stable-deployment-id/scenario-b",
      "name": "governed-asset-to-model"
    },
    "run": {"runId": "42db060a-d89d-40d1-a7cb-28d6703b5d07"},
    "root": {
      "job": {
        "namespace": "kfp://stable-deployment-id/scenario-b",
        "name": "governed-asset-to-model"
      },
      "run": {"runId": "42db060a-d89d-40d1-a7cb-28d6703b5d07"}
    }
  }
}
```

The child `rhoaiKfp` facet repeats the pipeline/version context and adds task
name, task ID, child run ID, attempt, and parent run ID. This makes a child
event independently inspectable without treating the child as the root.

For retries, the task ID and root run ID remain unchanged, while the child run
ID and attempt change. A failed attempt is never rewritten as a later success.
The idempotency key for a lifecycle transition is deterministic:

```text
rhoai-kfp:1.0:<run-id>:<event-state>
```

The transport should carry that key so replay is accepted idempotently without
requiring event timestamps to match.

## Data Registry and MLflow integration rules

KFP may carry a logical Data Registry asset reference and an optional Registry
revision reference. The current Registry response and maintained 0.8 contract
expose no durable catalog revision, so the fixture records metadata and source
hashes as `OBSERVED` evidence only. A metadata-response digest must not be
serialized as `revision`.

The component that reads the source emits:

- the logical Registry dataset identity;
- the physical dataset identity actually read;
- an explicit symlink or profile relationship where the two represent one
  governed asset; and
- evidence facets only for observations it made.

MLflow remains the owner of the MLflow run, model artifact, candidate
evaluation, and trace. A link may carry the KFP root and Registry asset IDs,
but KFP must not create a fake MLflow run and MLflow must not become the source
of truth for Data Registry asset identity. This is consistent with the dated
RHOAI knowledge signal that keeps AI-asset catalog/registry roles separate from
Data Registry data assets.

## Authority and failure semantics

The following statuses are separate and must not be collapsed:

| Status | Authority |
| --- | --- |
| KFP root state | KFP run API/control plane |
| DCH source-read state | DCH connector/workload |
| Spark processing state | Spark Operator/application, reconciled with native events |
| MLflow artifact/run state | MLflow |
| Lineage delivery state | Lineage ingestion service/outbox |

An OpenLineage delivery failure must not turn a successful workload into a
false failure, but it must remain visible as incomplete or delivery-failed
lineage. A lost pod or missing terminal event must not be represented as
`COMPLETE`; a query layer may derive `UNKNOWN`/`INCOMPLETE` while KFP
reconciliation is pending. Native events that disagree with an authoritative
component status require reconciliation rather than last-event-wins logic.

## Acceptance matrix for the native implementation

| Scenario | Required result |
| --- | --- |
| Start | One root `START`, exact KFP UUID, pipeline/version context present. |
| Success | One root `COMPLETE`, no fabricated root data edges. |
| Failure | One root `FAIL` with safe error summary; child failures remain visible. |
| Cancellation | One root `ABORT`; no later terminal rewrite. |
| Task retry | New child run ID and attempt, same task/root IDs, prior failure retained. |
| Spark child | Native Spark producer owns the child event; KFP submitter emits no duplicate. |
| DCH child | DCH owns the source-read event and evidence; KFP only propagates context. |
| Missing context | Independent/unlinked execution with an explicit diagnostic; never a guessed parent. |
| Replay | Same deterministic transition key is idempotent. |
| Delivery failure | Outbox becomes `RETRY`/`DEAD_LETTER`; KFP root state is unchanged. |
| State-history reconciliation | Available KFP transition timestamps are preserved; duplicate history is suppressed. |
| Cross-project asset | Authorization rejects the context before source access. |
| Secret scan | Context and facets contain no token, credential, password, or signed URL. |
| Version provenance | KFP run API and root event agree on pipeline/version IDs. |

The executable model and focused tests are in
`src/lineage_demo/kfp_lineage.py` and `tests/test_kfp_lineage.py`. They validate
identity, context redaction, root/child event shape, explicit root ancestry,
UUID rules, and replay/retry keys. They do not promote the proposal to shipped
KFP behavior.

## Open decisions before an upstream KFP change

1. Name the stable RHOAI deployment identity used in KFP namespaces.
2. Govern the custom facet schema and producer/version publication path.
3. Select the supported context injection API/file contract in KFP. The local
   fixture reader establishes a candidate file contract but is not a KFP
   runtime feature.
4. Select the durable outbox backing, delivery worker, replay policy, and
   root-state reconciliation owner for the defined record contract.
5. Define authorized asset-reference propagation and Data Registry revision
   behavior when the Registry adds snapshots or revisions.
6. Pin the supported KFP/RHOAI release matrix and run the acceptance matrix on
   a native implementation before changing a KFP fork.
7. Obtain the maintained KFP/RHOAI server or launcher source baseline and name
   the owner for the native extension and upgrade contract.

## Local KFP capability observation

[Observed, KFP Python client 2.16.1 in this fixture on 2026-09-30] The client
model exposes a server-generated `run_id`, `pipeline_version_reference` with
`pipeline_id` and `pipeline_version_id`, terminal `state`, and `state_history`.
The inspected client package contains no OpenLineage producer or context
injection API. This supports using the KFP run API as the root-state source, but
does not establish what a separately deployed KFP server or RHOAI integration
supports. A native implementation must be inspected and tested at its actual
control-plane revision before an upstream fork or deployment change.

## Live KFP control-plane observation

[Observed, RHOAI 3.6.0-ea.1 on 2026-09-30] Read-only inspection of the two Ready
DSPA installations used by this fixture found the standard KFP API server,
metadata service, persistence agent, scheduled-workflow service, and workflow
controller. Both API servers used the same immutable image digest:

```text
registry.redhat.io/rhoai/odh-ml-pipelines-api-server-v2-rhel9@sha256:1c90ade8fa4e3d23929bbdb846e36d25d00bbbf6770fecad3785f04f0359570f
```

The API-server command line exposed configuration, TLS, Kubernetes-backed
pipeline storage, and webhook controls, but no OpenLineage provider, event
outbox, or context-injection setting. The inspected server configuration and
launcher configuration likewise contained no lineage provider. This is a
deployment observation, not a claim about the implementation of every KFP
release or RHOAI distribution.

The KFP run API was then queried through a temporary local port-forward and
returned the following sanitized fields for the accepted Slice 2 run:

| Field | Observed value |
| --- | --- |
| `run_id` | `ad6efc7b-4b95-464e-b398-31df6b48e061` |
| `pipeline_id` | `7e44ad4e-f986-43c3-90d4-c6df8b73c783` |
| `pipeline_version_id` | `4707191e-5805-4314-8be6-87fadd84a7b9` |
| `state` | `SUCCEEDED` |
| `state_history` | `PENDING` → `RUNNING` → `SUCCEEDED` |
| `state_history` timestamps | `09:26:45` → `09:26:46` → `09:28:38` UTC |
| `plugins_input` / `plugins_output` | absent |

This confirms that the run API supplies the root identity, version provenance,
and authoritative terminal state needed by the proposed contract. It does not
provide native OpenLineage emission or task-context propagation. The next
implementation boundary is therefore a KFP/RHOAI control-plane integration (or
an explicitly supported extension at that boundary) that observes those run
transitions, emits the root lifecycle, and injects the versioned context into
task execution. The current fixture remains an adapter compatibility model and
must not be described as native support.

## Extension-point assessment

[Observed, local source baselines checked 2026-09-30] The available local
implementations separate the compatibility adapter from the native seam:

| Candidate | Evidence | What it can own | Qualification |
| --- | --- | --- | --- |
| KFP API server/run service | Live RHOAI deployment exposes the run UUID, pipeline-version reference, state, and state history; the [KFP architecture reference](https://github.com/kubeflow/pipelines/blob/master/docs/agents/architecture.md) places IR submission and workflow creation in the API server. | Root-state observation and root outbox production. | **Candidate seam; no lineage hook or outbox was observed in the deployed image/configuration.** |
| KFP launcher/executor path | The [KFP architecture reference](https://github.com/kubeflow/pipelines/blob/master/docs/agents/architecture.md) separates the launcher, which transfers artifacts and invokes the executor, from the API server; the live DSPA deploys launcher/driver images. | Context propagation into first-party task execution. | **Candidate seam; a supported RHOAI injection API was not found.** |
| Argo workflow controller | The live DSPA deploys the managed workflow controller; historical fixture evidence used controller-level environment injection as a workaround. | Broad pod defaults or context injection if productized. | **Operational workaround only until a supported DSPA/launcher contract exists.** |
| `rhoai-lineage` KFP adapter | Local `rhoai-lineage` `main` at `f501586` provides `kfp_lineage` in `src/rhoai_lineage/kfp/lineage.py`; it generates a UUID inside component code, reads `KFP_RUN_ID`/`KFP_PIPELINE_NAME`, and emits child events from a context manager. | User-authored child events and tool-specific facets. | **Observed adapter; not native root lifecycle, exact KFP root identity, context injection, or durable delivery.** |

The local KFP source baseline at commit
`36c8437ddc9a2e3493b312d8af63d6af96d6bfdb` confirms the launcher seam in
`backend/src/v2/compiler/argocompiler/container.go` and
`backend/src/v2/driver/driver.go`: the compiler selects a launcher image and
command through `V2_LAUNCHER_IMAGE`/`V2_LAUNCHER_COMMAND`, and the driver passes
the exact run ID plus executor, pod, and execution inputs. The same checkout's
`kfp-kubernetes` extension can add ConfigMap volumes, field-path environment
variables, and pod metadata, but those are task-authored platform settings, not
a control-plane-owned lineage API.

This narrows the implementation uncertainty but does not eliminate it. The
local RHOAI operator source at `9f571dbd9b6075447df30ed1268d66e4834ad5ba`
exposes only the Data Science Pipelines component and Argo controller
management; it does not expose a deployment ID, context resolver, root emitter,
or launcher-wrapper setting. The exact downstream source corresponding to the
live RHOAI API-server image and the owner-approved extension contract therefore
remain **Unknown**. The companion [implementation-readiness ADR](adr-native-kfp-lineage-implementation-readiness.md)
selects the launcher-compatible wrapper plus an RHOAI context resolver as the
candidate seam and lists the qualification required before a fork or deployment
change. Extending the component adapter alone would not satisfy the native
contract.

## Executable control-plane projection

[Proposed, executable fixture model] `KFPRunSnapshot` represents the sanitized
fields a native control-plane adapter needs from the KFP run API:
`run_id`, `pipeline_id`, `pipeline_version_id`, pipeline display name, state,
the observation timestamp, and optionally the ordered `state_history` entries.
`KFPRootEventProjector` validates that those identities agree with the versioned
root context before producing an event.

The fixture's state mapping is intentionally narrow:

| KFP run state | OpenLineage root event |
| --- | --- |
| `PENDING` | `START` |
| `RUNNING`, `CANCELING` | `RUNNING` |
| `SUCCEEDED` | `COMPLETE` |
| `FAILED`, `ERROR` | `FAIL` |
| `CANCELED` | `ABORT` |

If the first observed state is already `RUNNING` or terminal, the projector
emits `START` followed by that state so the root lifecycle is not missing its
beginning. When KFP state history is available, each projected event preserves
the corresponding transition timestamp. Each transition carries
`rhoai-kfp:1.0:<root-run-id>:<state>` as the outbox idempotency key. Replayed
observations produce no new event, and a different terminal state after a
terminal event fails closed. The root events contain no data edges.

This process-local projector is only a testable contract model. A native KFP
implementation must persist emitted keys, terminal state, and delivery
failure/retry state in a durable outbox or equivalent control-plane mechanism.
It should use KFP state-history timestamps where available rather than
inventing timestamps during reconciliation. The implementation and focused
tests are in `src/lineage_demo/kfp_lineage.py` and
`tests/test_kfp_lineage.py`. The root-event shape is checked by
`contracts/rhoai-kfp-root-event-1.0.schema.json`; it requires the versioned
KFP facet and empty root `inputs`/`outputs` arrays.

## Durable delivery record

[Proposed, storage-neutral] Before a root event is handed to a lineage
ingestion service, the native integration should persist an outbox record with
the following fields:

| Field | Rule |
| --- | --- |
| `idempotencyKey` | `rhoai-kfp:1.0:<root-run-id>:<event-state>`; immutable. |
| `rootRunId` / `eventType` | Must agree with the event and KFP context facet. |
| `event` | The complete root event, with empty data-edge arrays. |
| `createdAt` | Producer timestamp for the outbox record. |
| `attempts` | Non-negative delivery-attempt count. |
| `deliveryStatus` | `PENDING`, `RETRY`, `DELIVERED`, or `DEAD_LETTER`. |

Delivery status is deliberately separate from KFP run state: a lineage delivery
failure must not rewrite a successful KFP run as failed. A duplicate key with a
different event payload is a contract error, not a second lineage transition.
The storage document is versioned in
`contracts/rhoai-kfp-lineage-outbox-1.0.schema.json`, and the fixture model is
`KFPOutboxRecord` in `src/lineage_demo/kfp_lineage.py`. The schema does not
choose PostgreSQL, KFP's metadata store, or a particular lineage backend; that
deployment decision remains open.

## Implementation-ready checkpoint

The contract is ready to hand to a KFP/RHOAI implementation owner, with the
following qualification:

| Area | Current result |
| --- | --- |
| Root identity | Exact KFP run UUID, pipeline ID, and pipeline-version ID. **Observed** in the live run API. |
| Context transport | Strict non-secret envelope and candidate read-only file path. **Proposed**; no deployed injection hook observed. |
| Root lifecycle | State mapping, no fabricated data edges, terminal rewrite protection. **Proposed and executable in the fixture.** |
| Delivery | Versioned outbox record, idempotency key, retry/dead-letter statuses. **Proposed and schema-validated.** |
| Data Registry | Logical asset identity remains Registry-owned; no invented revision. **Observed boundary and proposed integration rule.** |
| MLflow | AI runs, models, artifacts, evaluations, and traces remain MLflow-owned. **Observed architectural boundary.** |
| Native deployment | No native KFP producer, context injection, or outbox was found in the inspected RHOAI/KFP deployment. **Observed.** |

The next work item is therefore an implementation-source decision: identify a
supported KFP server/launcher extension point or authorize an upstream RHOAI/KFP
change, then bind that implementation to these schemas and acceptance tests.
Until that happens, this repository demonstrates the contract shape and
compatibility behavior only.
