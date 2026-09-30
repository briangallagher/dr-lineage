---
title: Packaged RHOAI/KFP lineage qualification
document_id: DR-LIN-QUAL-0001
status: evidence-reviewed
last_verified: 2026-09-30
scope: read-only qualification of the scenario-b RHOAI 3.6.0-ea.1 AI Pipelines deployment
owners:
  - RHOAI AI Pipelines integration owner
valid_for:
  implementation:
    - repository: kubeflow/pipelines
      ref: 36c8437ddc9a2e3493b312d8af63d6af96d6bfdb
  api_contract: rhoai-kfp:1.0 local contract fixture
  jira_snapshot: not-applicable
---

# Packaged RHOAI/KFP lineage qualification

## Decision

[Observed] The current packaged deployment is **no-go for native lineage by
configuration alone**. It has a usable KFP launcher image seam, but it does
not expose a supported RHOAI lineage/context contract in the observed
deployment:

- the DSPO operator configures `V2_LAUNCHER_IMAGE` and `V2_DRIVER_IMAGE` on the
  API server, but not `V2_LAUNCHER_COMMAND`;
- a completed task pod uses the configured launcher image and the default
  `launcher-v2` command;
- the task pod has no platform-injected lineage context path, context volume,
  resolver endpoint, or root-event/outbox configuration; and
- the DSPA resource has no lineage/context field in its observed spec.

[Proposed] There is a **conditional go** for a first-party RHOAI/KFP
extension. The owning team must map the deployed image digests to immutable
downstream source/release manifests, approve the extension boundary, and then
ship and qualify the root reconciler, resolver, and launcher-compatible wrapper
described in the [implementation-readiness ADR](adr-native-kfp-lineage-implementation-readiness.md).

This record is evidence about one read-only observation, not a claim that all
RHOAI releases have the same image composition. No cluster resource was
created, changed, or deleted.

## Observation identity

| Field | Value |
| --- | --- |
| Verified | 2026-09-30 |
| RHOAI platform setting | `DSPO_PLATFORMVERSION=3.6.0-ea.1` |
| Cluster | `api.bgal-pool-ljt7l.aws.rh-ods.com` |
| DSPA namespace/name | `scenario-b/dspa` |
| API-server Deployment | `scenario-b/ds-pipeline-dspa` |
| API-server template config hash | `aa05d75c5d37cb100e89f0d5ea1adf0c22e5e70f46d1818b571900d63a3f8d2a` |
| Installed OLM release | `rhoai-model-registries/rhods-operator.3.6.0-ea.1`, phase `Succeeded` |
| OLM release repository | `https://github.com/red-hat-data-services/rhods-operator` |
| OLM release manifest created | `2026-09-02T11:54:43Z` |
| Evidence mode | authenticated, read-only OpenShift API inspection |

## Packaged image and pod evidence

The following values are immutable image references observed from the operator,
API-server Deployment, and one completed task pod. Image digests are recorded
instead of mutable tags.

| Component | Observed image or setting | Evidence state | Interpretation |
| --- | --- | --- | --- |
| DSPO controller | `registry.redhat.io/rhoai/odh-data-science-pipelines-operator-controller-rhel9@sha256:f5c36e87054387a8f630e5b0ada31fdac2f5e39d547dc2a28b890b9504f23a6e` | [Observed] | The operator owns the DSPA-to-component reconciliation boundary in this environment. |
| KFP API server | `registry.redhat.io/rhoai/odh-ml-pipelines-api-server-v2-rhel9@sha256:1c90ade8fa4e3d23929bbdb846e36d25d00bbbf6770fecad3785f04f0359570f` | [Observed] | Packaged API-server image for `scenario-b/dspa`. |
| API-server launcher image setting | `V2_LAUNCHER_IMAGE` is set to `registry.redhat.io/rhoai/odh-ml-pipelines-launcher-rhel9@sha256:c9ffd6cf7adb887fe7859e23831a9c94280ffb050022576a14846314ee320b32` | [Observed] | The image override is active in the packaged API server. |
| API-server launcher command setting | `V2_LAUNCHER_COMMAND` is absent | [Observed] | The observed deployment does not configure a command override. The task pod therefore provides the stronger runtime observation of the selected command. |
| KFP driver image setting | `V2_DRIVER_IMAGE` is set to `registry.redhat.io/rhoai/odh-ml-pipelines-driver-rhel9@sha256:9f36b302daabe4916a0075b6ccd40a3af664e7e3b4e50be9dba7ed2ac08b7692` | [Observed] | Driver is separately packaged; it is not a lineage context provider. |
| Completed task pod | `governed-asset-to-model-8gddt-system-container-impl-3191863202`, phase `Succeeded` | [Observed] | Runtime pod shape was inspected without reading logs or secret values. |
| Launcher init container | The configured launcher digest, command `launcher-v2`, args `--copy /kfp-launcher/launch --cache_disabled` | [Observed] | Confirms the packaged task path and the default command selected in this pod. |
| User container launcher invocation | `/kfp-launcher/launch` followed by the standard KFP identity/metadata flags, including `--run_id`, `--execution_id`, `--executor_input`, `--component_spec`, `--pod_name`, and `--pod_uid` | [Observed] | The exact KFP root UUID is already available to the launcher-compatible boundary. |
| Context injection | No `RHOAI_LINEAGE_CONTEXT_PATH`, `RHOAI_LINEAGE_PROFILE_VERSION`, context volume, resolver URL, or lineage outbox setting was observed | [Observed] | The proposed `rhoai-kfp:1.0` transport is not packaged or injected in this deployment. |

The API-server command is the standard `/bin/apiserver` path with config,
TLS, Kubernetes pipeline-store, and webhook flags. The inspected API-server
volumes are TLS/config/sample-pipeline volumes; no lineage context volume is
present. The server and sample ConfigMaps contain no lineage-specific field
paths. Values from credential-bearing configuration were not recorded.

## Source and release baseline

The source evidence separates what is known from what still needs an owner
answer.

| Baseline | Evidence state | What it proves | Remaining qualification gap |
| --- | --- | --- | --- |
| Local `kubeflow/pipelines` checkout at `36c8437ddc9a2e3493b312d8af63d6af96d6bfdb` | [Observed] | `container.go` defines `V2_LAUNCHER_IMAGE` and `V2_LAUNCHER_COMMAND`; the compiler/driver pass the standard KFP run and task inputs. | This source revision is not proven to build the live RHOAI launcher digest. |
| Local ODH operator manifest map | [Observed] | `get_all_manifests.sh` records a RHOAI 3.5-ea.1 DSPO source ref `9cf998a9ecf9cf36a57e8b40d855f4077bc9362d`. | It predates the observed 3.6.0-ea.1 operator and cannot be used as the live source baseline. |
| Installed RHOAI OLM release manifest | [Observed] | `rhods-operator.3.6.0-ea.1` CSV `relatedImages` includes the observed DSPO, API-server, launcher, driver, and Argo digests; CSV repository annotation points to `red-hat-data-services/rhods-operator`. | This maps images to the RHOAI release, but not to the component source commits or build inputs. |
| Maintained DSPO architecture boundary | [Observed] | The [official DSPO repository](https://github.com/opendatahub-io/data-science-pipelines-operator) documents the operator-managed DSPA/KFP component boundary. | The public moving branch does not identify the exact downstream source commit for the observed proprietary image digest. |
| Downstream component source commits/build inputs for API server, launcher, driver, and DSPO | [Unknown] | The release-level image mapping is observed, but the installed CSV does not identify the component source commits or build provenance. | The RHOAI owner must provide the component source refs/build metadata before implementation qualification. |
| Deployed launcher binary ABI relative to local KFP source | [Unknown] | Runtime flags are compatible with the inspected KFP source shape. | Compatibility is an observation, not proof of binary provenance or future stability. |

The [downstream KFP repository](https://github.com/red-hat-data-services/data-science-pipelines)
is a useful source route, but its moving default branch is not an immutable
mapping for these image digests. The source-to-image mapping must be captured
from the supported RHOAI release/build metadata, not inferred from repository
names or current branch contents. A read-only `oc image info` lookup against
the two observed external references returned `manifest unknown`, so no image
config labels or source revisions were available from that query. The installed
OLM CSV remains the authoritative release-level mapping for this observation.

## Owning boundary and recommendation

The observed ownership chain is:

```text
DataSciencePipelinesApplication (project)
              |
              v
       DSPO controller
              |
              v
       KFP API server ---- V2_LAUNCHER_IMAGE / V2_DRIVER_IMAGE
              |
              v
        generated Argo task pod
              |
              v
        KFP launcher-v2
```

[Observed] The owner for a native extension is therefore the maintained
RHOAI/DSPO/KFP integration boundary. Data Registry and Feast are downstream
or adjacent consumers: Data Registry can authorize a logical asset reference,
and Feast can emit feature-store lineage, but neither owns the KFP root
lifecycle or launcher injection. This respects the Data Registry boundary in
[`PROJECT_CONTEXT.md`](/Users/briangallagher/dev/workspaces/data-registry/PROJECT_CONTEXT.md).

[Proposed] The implementation owner should choose one of these paths:

1. **Preferred:** add a first-party, versioned DSPO/KFP context-provider and
   root-reconciler contract, with an operator-configured deployment ID,
   resolver authorization, and durable outbox.
2. **Acceptable only with owner approval:** ship a supported wrapper image at
   the launcher boundary, preserving the observed CLI and exit behavior, and
   have DSPO inject the wrapper plus the context transport.
3. **Not sufficient:** rely on user-authored `kfp-kubernetes` task helpers,
   arbitrary environment variables, exit handlers, or the current application
   adapter as a claim of native RHOAI support.

## Go/no-go gates

| Gate | Go condition | Current result |
| --- | --- | --- |
| G-00 release mapping | Every supported image digest maps to the immutable installed RHOAI release manifest | **Pass at release level; component source commits [Unknown]** |
| G-01 owner/API | A maintained owner accepts the DSPO/KFP extension boundary and versioning policy | **No-go; [Unknown]** |
| G-02 root lifecycle | Reconciler emits exact KFP-root `START` and one authoritative terminal event with durable idempotency | **No-go; [Proposed]** |
| G-03 context transport | Task pod receives schema-valid, read-only context by exact root UUID | **No-go; [Unknown]** |
| G-04 security | Resolver enforces project authorization and no secrets enter context, logs, or events | **No-go; [Proposed]** |
| G-05 compatibility | Wrapper preserves the packaged launcher flags, command ordering, exit status, retries, and cancellation semantics | **No-go; [Proposed]** |
| G-06 upgrade | C-01 through C-14 pass against every supported image/operator upgrade | **No-go; not run** |

The current deployment is suitable for the local demonstrator and for a
future compatibility prototype. It is not sufficient evidence to advertise
native RHOAI lineage support.

## Required implementation and upgrade tests

The existing [C-01–C-14 acceptance matrix](adr-native-kfp-lineage-implementation-readiness.md#acceptance-matrix)
remains the normative test set. The packaged qualification adds these gates:

The checked-in baseline and validator are described in the
[owner handoff](rhoai-kfp-qualification-handoff.md). The capture script is
read-only and produces a dated sanitized fixture; it must not be used as
evidence that native support exists merely because the script succeeds.

| Test group | Required check | Release evidence |
| --- | --- | --- |
| Image provenance | Resolve every image digest to the supported RHOAI release manifest and source revision; fail if any mapping is missing | Immutable manifest plus owner approval |
| Pod shape | Compile a minimal pipeline and assert launcher image, command, args, volume, and context environment exactly | Redacted generated pod fixture and integration result |
| Root semantics | Exercise success, failure, cancellation, retry, duplicate reconciliation, and delayed KFP state observation | Root event/outbox record set keyed by exact KFP UUID |
| Context semantics | Resolve by exact root UUID; reject malformed, cross-project, missing, and stale context; preserve workload success when context service is unavailable | Resolver audit plus negative tests |
| Ownership | Verify KFP emits only orchestration-root events and actual data producers own physical inputs/outputs | OpenLineage query with no duplicate edges |
| Secret safety | Scan context, events, logs, generated manifests, and test artifacts for tokens, Secret values, passwords, credentials, and signed URLs | Automated redaction test and artifact review |
| Upgrade | Repeat all groups after DSPO, API-server, launcher, driver, Argo, or KFP SDK changes | Before/after digest matrix and C-01–C-14 report |

## Explicit unknowns and owner questions

1. What exact downstream commits or signed release manifests produced the
   observed DSPO, API-server, launcher, driver, and Argo images?
2. Is `V2_LAUNCHER_IMAGE` an owner-supported extension point for this RHOAI
   release, or only an internal KFP compiler setting?
3. Which maintained component owns the root reconciler, context resolver, and
   outbox, and what is its supported API boundary?
4. Which configuration API persists the stable RHOAI deployment identity?
5. What launcher ABI and upgrade policy does the supported RHOAI release
   guarantee?
6. Which RHOAI/Data Registry contract supplies an authorized asset reference
   without fabricating a Registry revision?

## Evidence ledger

| Claim | Evidence state | Source and revision | Caveat |
| --- | --- | --- | --- |
| DSPO platform version and image digest | [Observed] | Read-only `scenario-b` operator Deployment, 2026-09-30 | Environment-specific. |
| API-server and launcher image digests | [Observed] | Read-only `scenario-b/ds-pipeline-dspa` Deployment, 2026-09-30 | Image provenance is not yet mapped to an immutable downstream source ref. |
| Runtime launcher command and standard KFP identity flags | [Observed] | Completed task pod `governed-asset-to-model-8gddt-system-container-impl-3191863202`, 2026-09-30 | One completed pod; upgrade behavior remains unqualified. |
| No packaged lineage/context injection seam | [Observed] | Redacted API-server and task-pod specs plus ConfigMap field-path inspection, 2026-09-30 | Absence in one deployment does not prove absence in all RHOAI releases. |
| Digest-to-release mapping | [Observed] | Installed `rhods-operator.3.6.0-ea.1` CSV `relatedImages`, 2026-09-30 | Release-level mapping is immutable for this environment; it does not identify component source commits. |
| Exact component source-to-image mapping | [Unknown] | Installed CSV, local KFP/ODH checkouts, and official repository navigation | Requires component build metadata or owner confirmation. |
| Native root reconciler, resolver, wrapper, and outbox | [Proposed] | Local versioned contracts and implementation-readiness ADR | Not deployed or supported. |

No token, password, credential, Secret value, or signed URL is recorded in this
qualification document.
