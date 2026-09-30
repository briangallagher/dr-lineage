---
title: Native RHOAI/KFP lineage implementation readiness
document_id: DR-LIN-ADR-0004
status: evidence-reviewed
last_verified: 2026-09-30
owners:
  - RHOAI lineage integration owner
valid_for:
  implementation:
    - repository: kubeflow/pipelines
      ref: 36c8437ddc9a2e3493b312d8af63d6af96d6bfdb
    - repository: feast-dev/feast
      ref: 1669661e15d3ba3b5ab9a9fffd19248d9c0da211
    - repository: briangallagher/opendatahub-operator
      ref: 9f571dbd9b6075447df30ed1268d66e4834ad5ba
  api_contract: rhoai-kfp:1.0 local contract fixture
  jira_snapshot: not-applicable
---

# Native RHOAI/KFP lineage implementation readiness

## Executive decision

[Proposed] Treat native KFP lineage as an RHOAI integration boundary with two
separate responsibilities:

1. an RHOAI-owned control-plane reconciler observes the authoritative KFP run
   identity and state history, projects the root lifecycle, and persists the
   versioned outbox record; and
2. a launcher-compatible RHOAI wrapper resolves the same root context by the
   exact KFP run UUID, validates the `rhoai-kfp:1.0` envelope, and exposes it
   as a read-only file before the normal user command runs.

The wrapper should be selected through the KFP v2 launcher image/command seam
only after the RHOAI owner confirms that seam for the supported distribution.
The local prototype in
`src/lineage_demo/kfp_integration_readiness.py` models this boundary without
changing KFP, RHOAI, or a cluster.

This decision keeps KFP responsible for orchestration identity and lifecycle,
keeps DCH/Spark/workloads responsible for data edges they actually observe,
keeps Data Registry responsible for logical asset identity, and keeps MLflow
responsible for AI runs, artifacts, evaluations, and traces.

## Scope and non-goals

In scope:

- a stable deployment identity rule;
- the root observer, context resolver, and launcher wrapper responsibilities;
- the relationship between the existing context, root-event, and outbox schemas;
- source-baseline findings for KFP, the RHOAI operator integration, and Feast;
- acceptance tests and upgrade risks for an implementation owner.

Out of scope:

- changing the maintained KFP or RHOAI source repositories;
- forking or patching KFP;
- deploying a resolver, controller, launcher image, or CRD;
- cluster writes, operator changes, Data Registry schema changes, or a new
  lineage backend;
- claiming that the current RHOAI deployment emits native OpenLineage events.

## Evidence baseline

The source rows below are exact local checkouts inspected on 2026-09-30. They
are evidence about those revisions, not a promise that a later or differently
packaged RHOAI release behaves identically.

| Claim | Evidence state | Baseline and relevant seam | Result |
| --- | --- | --- | --- |
| KFP v2 creates a launcher init container and passes the run identity into the task launcher | [Observed] | `kubeflow/pipelines` at `36c8437ddc9a2e3493b312d8af63d6af96d6bfdb`; [`container.go`](/Users/briangallagher/dev/workspaces/pipelines/pipelines-repo/backend/src/v2/compiler/argocompiler/container.go:38) and [`driver.go`](/Users/briangallagher/dev/workspaces/pipelines/pipelines-repo/backend/src/v2/driver/driver.go:266) | Exact `--run_id`, pipeline name, execution ID, executor input, pod name, and pod UID are already launcher inputs. |
| KFP exposes global launcher image and command configuration | [Observed] | Same KFP ref; [`container.go`](/Users/briangallagher/dev/workspaces/pipelines/pipelines-repo/backend/src/v2/compiler/argocompiler/container.go:41) | `V2_LAUNCHER_IMAGE` and `V2_LAUNCHER_COMMAND` select the launcher image/command at compiler process scope. This is a candidate seam, not yet an RHOAI lineage API. |
| KFP Kubernetes extensions can mount ConfigMaps and expose safe pod metadata | [Observed] | Same KFP ref; [`config_map.py`](/Users/briangallagher/dev/workspaces/pipelines/pipelines-repo/kubernetes_platform/python/kfp/kubernetes/config_map.py:66), [`field.py`](/Users/briangallagher/dev/workspaces/pipelines/pipelines-repo/kubernetes_platform/python/kfp/kubernetes/field.py:21), [`pod_metadata.py`](/Users/briangallagher/dev/workspaces/pipelines/pipelines-repo/kubernetes_platform/python/kfp/kubernetes/pod_metadata.py:20) | These are task-authored platform extensions. They do not establish a per-run RHOAI context service or control-plane injection contract. |
| The RHOAI operator component API exposes no lineage/context settings | [Observed] | Local operator fork at `9f571dbd9b6075447df30ed1268d66e4834ad5ba`; [`datasciencepipelines_types.go`](/Users/briangallagher/dev/git-repos/opendatahub-operator/api/components/v1alpha1/datasciencepipelines_types.go:53). The [maintained DSPO repository](https://github.com/opendatahub-io/data-science-pipelines-operator) corroborates the namespace-scoped DSPA/APIServer/Persistence Agent/Scheduled Workflow deployment boundary. | The local fork is immutable evidence for its revision; the public DSPO page is a moving corroborating source, not proof of the live RHOAI image. |
| The packaged RHOAI deployment uses a launcher image override but no context injection | [Observed] | [`rhoai-kfp-packaged-qualification.md`](rhoai-kfp-packaged-qualification.md); read-only `scenario-b` Deployment and completed task pod, 2026-09-30 | Exact downstream source provenance and owner support for the override are [Unknown]. |
| Feast operator OpenLineage configuration is backend configuration, not KFP context propagation | [Observed] | `feast-dev/feast` at `1669661e15d3ba3b5ab9a9fffd19248d9c0da211`; [`featurestore_types.go`](/Users/briangallagher/dev/git-repos/feast/infra/feast-operator/api/v1/featurestore_types.go:87) and [`repo_config.go`](/Users/briangallagher/dev/git-repos/feast/infra/feast-operator/internal/controller/services/repo_config.go:430) | The CRD configures Feast OpenLineage producer/consumer behavior and secrets. It does not own KFP run identity, task context, or RHOAI asset authorization. |
| The existing adapter generates child identities in workload code | [Observed] | `rhoai-lineage` local `main` at `f5015860a9830cc9e35d25938ee8783d5ef98653`; [`lineage.py`](/Users/briangallagher/dev/git-repos/rhoai-lineage/src/rhoai_lineage/kfp/lineage.py:100) | Useful historical adapter evidence, but not native root lifecycle, exact KFP root identity, context injection, or durable delivery. |
| Data Registry owns project-scoped logical asset metadata and locations | [Observed] | [`PROJECT_CONTEXT.md`](/Users/briangallagher/dev/workspaces/data-registry/PROJECT_CONTEXT.md) and the maintained Data Registry documentation route | The KFP context may carry an authorized reference; it must not invent a revision or replace Registry authorization. |

The local application also has a live, read-only RHOAI observation recorded in
[`native-kfp-lineage-context-contract.md`](native-kfp-lineage-context-contract.md):
the accepted KFP run exposed its UUID, pipeline/version references, terminal
state, and state history, while the inspected deployment exposed no native
OpenLineage producer, context-injection setting, or lineage outbox. That live
observation remains deployment-specific. The companion
[`rhoai-kfp-packaged-qualification.md`](rhoai-kfp-packaged-qualification.md)
records the exact operator/API-server/launcher digests and the completed task
pod shape: the packaged launcher path is real, but the source-to-image mapping
from the installed RHOAI release to component source commits, and the supported
context-injection API, remain [Unknown]. The installed OLM CSV does provide an
immutable release-level mapping for the observed image digests.

## Stable deployment identity

[Proposed] The namespace portion of the RHOAI KFP OpenLineage job identity is:

```text
kfp://<stable-rhoai-deployment-id>/<project>
```

The deployment ID must be an opaque, validated identifier configured at the
RHOAI installation/control-plane level and persisted for the lifetime of the
deployment. It must not be derived from:

- an API hostname or Route;
- a namespace UID;
- a pod name or UID;
- a KFP run or pipeline UUID; or
- a service-account token, secret, or signed location.

The eventual operator/configuration API is an open product decision. Until one
exists, an implementation must fail startup or mark lineage `UNCONFIGURED`;
it must not silently fall back to a hostname. The local context model already
validates the deployment identifier as safe metadata and keeps it separate from
the run identity.

## Proposed runtime flow

```text
KFP run API / authoritative state
              |
              v
RHOAI root reconciler ----> durable outbox ----> lineage ingestion backend
              |
              v
       context resolver <---- exact KFP root run UUID
              ^
              |
KFP launcher-compatible wrapper
              |
              v
read-only context.json -> DCH / Spark / MLflow / workload adapters
```

### Root reconciler

[Proposed] A control-plane adapter reads the KFP run UUID, pipeline ID,
pipeline-version ID, display name, authoritative state, and ordered state
history. It feeds those values to `KFPRootEventProjector`-equivalent logic:

- `PENDING` becomes `START`;
- `RUNNING` and `CANCELING` become `RUNNING`;
- `SUCCEEDED` becomes `COMPLETE`;
- `FAILED` and `ERROR` become `FAIL`; and
- `CANCELED` becomes `ABORT`.

The root event has no fabricated data edges. One deterministic idempotency key
is persisted per root transition. A conflicting terminal transition is a
contract error requiring reconciliation, not a last-event-wins update.

### Context resolver and launcher wrapper

[Proposed] The RHOAI wrapper preserves the current KFP launcher command-line
contract, resolves `/v1/lineage/contexts/<exact-kfp-root-run-id>` using its
in-cluster service identity, validates the returned envelope, and writes:

```text
/var/run/rhoai/lineage/context.json
RHOAI_LINEAGE_CONTEXT_PATH=/var/run/rhoai/lineage/context.json
RHOAI_LINEAGE_PROFILE_VERSION=1.0
```

The wrapper then invokes the normal KFP launcher behavior. The context file is
read-only to workload integrations and contains only safe IDs and metadata. A
missing or unavailable context produces an explicit `UNLINKED_DIAGNOSTIC`
condition; the workload must not guess ancestry from pod names, display names,
or user parameters. A malformed context is rejected for lineage emission and
is never echoed in an error.

The reason to use the launcher boundary is specific and evidence-backed: the
current KFP compiler already provides the exact run ID and launcher command
boundary. The reason not to use `kfp-kubernetes` task helpers as the product
contract is equally specific: those helpers are user-authored task configuration
and cannot, by themselves, guarantee control-plane-owned root identity,
authorization, or per-run context publication.

The local prototype's plan is generated by
`build_launcher_context_plan()` and materialized by
`materialize_runtime_context()`. It is deliberately storage-neutral and does
not make an HTTP request or write a cluster object.

## Contract binding

The existing contracts remain the implementation target:

| Contract | Native owner | Binding rule |
| --- | --- | --- |
| `rhoai-kfp-lineage-context:1.0` | KFP/RHOAI integration | Root run UUID is the exact KFP UUID; task IDs and attempts are separate. |
| `rhoai-kfp-root-event:1.0` | Root reconciler | Lifecycle is authoritative KFP state; root inputs/outputs are empty. |
| `rhoai-kfp-lineage-outbox:1.0` | Root reconciler/delivery worker | Transition key is deterministic and delivery status is independent of workload state. |
| OpenLineage `ParentRunFacet` | Child producer | Child producer owns its actual operation and points to the immediate parent plus root. |
| Data Registry asset reference | Data Registry/authorized integration | Carry logical asset identity; never infer a revision from a metadata digest. |
| MLflow correlation | MLflow integration | Link the KFP root and Registry asset without moving model/artifact ownership into KFP. |

## Acceptance matrix

The first four rows are already exercised by the local contract fixture. Rows
marked `Native qualification` require an implementation and an integration
environment; they are not claims about the current deployment.

| ID | Scenario | Required result | Evidence target |
| --- | --- | --- | --- |
| C-01 | Root start | One `START`, exact KFP UUID, pipeline/version IDs, stable deployment ID | Native qualification |
| C-02 | Root success | One `COMPLETE`, no fabricated root data edges | Native qualification |
| C-03 | Root failure/cancel | One `FAIL` or `ABORT`; no later terminal rewrite | Native qualification |
| C-04 | State history | Reconciliation preserves KFP transition timestamps and suppresses duplicate keys | Native qualification |
| C-05 | Task context | Wrapper resolves by exact root UUID and writes schema-valid read-only context | Local prototype + native qualification |
| C-06 | Missing resolver | Workload remains explicitly unlinked; no guessed parent or fabricated root event | Local prototype + native qualification |
| C-07 | Retry | New child run ID and attempt; failed attempt remains visible | Local contract tests + native qualification |
| C-08 | Child ownership | Spark/DCH/workload owns actual I/O; KFP emits no duplicate child data edge | Local contract tests + native qualification |
| C-09 | Delivery outage | Outbox moves `RETRY`/`DEAD_LETTER`; KFP state remains unchanged | Native qualification |
| C-10 | Replay | Same key and payload is idempotent; same key with a different payload is rejected | Local contract tests + native qualification |
| C-11 | Authorization | Cross-project context or asset access is rejected before source access | Native qualification |
| C-12 | Secret safety | Context, plan, logs, and events contain no token, secret, password, credential, or signed URL | Local prototype + native qualification |
| C-13 | Launcher compatibility | Wrapper preserves all current launcher flags and normal completion behavior | KFP source test + native qualification |
| C-14 | Upgrade | Supported KFP/RHOAI image and operator upgrade reruns C-01–C-13 before release | Upgrade qualification |

## Upgrade and operational risks

| Risk | Why it matters | Required mitigation |
| --- | --- | --- |
| Launcher CLI or init-container ABI changes | The wrapper depends on current flags, launcher volume, and command ordering | Pin the supported KFP/RHOAI matrix; compile generated workflows and run a wrapper compatibility test on every upgrade. |
| Global launcher override is not a public RHOAI API | `V2_LAUNCHER_IMAGE` and `V2_LAUNCHER_COMMAND` are compiler-process configuration, not a lineage contract | Obtain owner confirmation or add a first-class RHOAI/KFP extension; do not silently rely on a fork or user pipeline code. |
| KFP source and packaged RHOAI image diverge | An upstream checkout cannot prove the behavior of a downstream image | Record the exact image digest and inspect the corresponding downstream source or release manifest before qualification; see the packaged qualification record. |
| Root polling misses a transition or observes it late | A generic run API may not provide watch semantics | Use state history, durable cursors, reconciliation, and an explicit `UNKNOWN`/incomplete state; never synthesize success. |
| Resolver outage blocks or changes workload behavior | Context is useful but must not turn a successful workload into a false failure | Keep delivery/context status separate from KFP workload state; expose unlinked diagnostics and alert on missing context. |
| Service-account overreach leaks cross-project context | Resolver access may cross namespace or project boundaries | Scope RBAC to the required read path, authorize project and asset references server-side, and add negative tests. |
| Deployment ID changes on reinstall | Job identities would split across one logical RHOAI deployment | Persist and migrate the deployment ID explicitly; treat a change as a new deployment with documented lineage continuity. |
| Feast/OpenLineage backend semantics change | Backend support does not automatically provide RHOAI asset/revision or authorization semantics | Run the backend conformance suite against the exact supported image and keep the RHOAI ingestion/query contract above it. |
| Data Registry revision remains unavailable | A stable asset UUID is not immutable content evidence | Keep `Linked`, `Observed`, and `Reproducible` separate; do not promote a metadata digest to a Registry revision. |
| User task configuration bypasses the platform path | Task-level helpers can produce partial or spoofed context | Treat platform-injected context as authoritative input, validate it, and mark manually emitted events as adapter evidence. |

## Implementation sequence

1. Confirm the RHOAI owner and supported KFP/RHOAI source/image baseline. The
   current packaged qualification is no-go until every observed digest maps to
   an immutable supported source/release ref.
2. Add the stable deployment-ID configuration at the RHOAI control-plane boundary.
3. Implement the root reconciler and durable outbox against the existing three
   schemas, with a test double for the KFP run API.
4. Implement the context resolver with project authorization and safe response
   filtering.
5. Implement the launcher-compatible wrapper and verify flag/exit-code
   compatibility against the supported launcher image.
6. Add first-party DCH/Spark/workload readers for the context file; preserve
   producer ownership of actual data edges.
7. Run C-01–C-14 in a disposable qualification environment, then record the
   exact image digests, source revisions, and unresolved gaps.

No step above authorizes a cluster mutation in this repository. Deployment and
fork work require a separate implementation decision and explicit authorization.

## Open questions

- Which maintained RHOAI repository owns the root reconciler and resolver?
- Is the launcher override seam owner-approved for the supported DSPA image, or
  should KFP expose a first-class context-provider interface?
- Which storage provides durable outbox semantics and what is its retention?
- What exact RHOAI configuration API persists the deployment ID?
- Which Data Registry revision contract, if any, will be available to context
  producers?
- What backend and BFF surface answers asset-centric lineage queries with RBAC?

## Evidence ledger

| Claim | Evidence state | Source and revision | Verified | Caveat |
| --- | --- | --- | --- | --- |
| KFP v2 launcher receives exact run identity and task execution inputs | [Observed] | `kubeflow/pipelines` `36c8437ddc9a2e3493b312d8af63d6af96d6bfdb`, compiler and driver source cited above | 2026-09-30 | Local upstream checkout; not proof of the deployed downstream image. |
| KFP provides global launcher image/command overrides | [Observed] | Same KFP revision, `V2_LAUNCHER_IMAGE` and `V2_LAUNCHER_COMMAND` | 2026-09-30 | Candidate seam; public RHOAI support is [Unknown]. |
| RHOAI operator API has no lineage/context field | [Observed] | Local operator fork `9f571dbd9b6075447df30ed1268d66e4834ad5ba`; public DSPO repository checked as corroborating navigation on 2026-09-30 | 2026-09-30 | Fork/revision is not a merged RHOAI release baseline; the live downstream image source still needs owner confirmation. |
| Packaged RHOAI launcher path is active | [Observed] | RHOAI 3.6.0-ea.1 read-only observation: API-server `V2_LAUNCHER_IMAGE` and completed task pod `launcher-v2`; exact digests and pod shape are in the packaged qualification record | 2026-09-30 | No context file, resolver, outbox, or command override was observed; native support is not established. |
| Packaged RHOAI digest-to-release mapping | [Observed] | Installed `rhods-operator.3.6.0-ea.1` OLM CSV `relatedImages`, including the observed DSPO/API-server/launcher/driver/Argo digests | 2026-09-30 | Release-level mapping is immutable for this environment; component source commits/build inputs remain unknown. |
| Packaged RHOAI component source/image mapping | [Unknown] | Local RHOAI 3.5 manifest map and public DSPO/KFP repository navigation | 2026-09-30 | Must be supplied by the supported RHOAI release/build owner. |
| Feast operator has OpenLineage producer/consumer configuration | [Observed] | `feast-dev/feast` `1669661e15d3ba3b5ab9a9fffd19248d9c0da211` | 2026-09-30 | Backend configuration is not proof of KFP integration or RHOAI authorization. |
| Live RHOAI run API supplied root identity/state history | [Observed] | `scenario-b`, read-only observation recorded in the companion native contract doc | 2026-09-30 | Environment- and image-specific; tokens and secrets were not recorded. |
| Native root producer, resolver, launcher wrapper, and outbox | [Proposed] | This ADR and the versioned local schemas | 2026-09-30 | Must be implemented and qualified before being described as supported. |
| Data Registry asset identity boundary | [Observed] | [`PROJECT_CONTEXT.md`](/Users/briangallagher/dev/workspaces/data-registry/PROJECT_CONTEXT.md) | 2026-09-30 | Dated companion context; implementation claims require maintained Data Registry evidence. |
