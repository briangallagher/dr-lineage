---
title: RHOAI/KFP qualification owner handoff
document_id: DR-LIN-HANDOFF-0001
status: evidence-reviewed
last_verified: 2026-09-30
owners:
  - RHOAI AI Pipelines integration owner
valid_for:
  implementation:
    - repository: kubeflow/pipelines
      ref: 36c8437ddc9a2e3493b312d8af63d6af96d6bfdb
  api_contract: rhoai-kfp:1.0 local contract fixture
  jira_snapshot: not-applicable
---

# RHOAI/KFP qualification owner handoff

## Executive summary

[Observed] The checked-in baseline records RHOAI `3.6.0-ea.1` as installed by
`rhods-operator.3.6.0-ea.1`. Its OLM `relatedImages` pins the DSPO, KFP API
server, launcher, driver, and Argo images by digest. A completed task pod
confirms the launcher image and `launcher-v2` ABI.

[Observed] The baseline is **NO_GO** for native RHOAI/KFP lineage because no
platform-injected context file, context environment variable, resolver, or
outbox is present. Component source commits and owner-approved API support are
also not represented in the release manifest.

This is an owner handoff, not an implementation request. It asks the
maintainer to supply provenance and make the minimum API decisions required to
turn the local contract prototype into a supported product integration.

## Reproducible qualification package

| Artifact | Purpose |
| --- | --- |
| [`rhoai-kfp-qualification-baseline-2026-09-30.json`](../examples/rhoai-kfp-qualification/rhoai-kfp-qualification-baseline-2026-09-30.json) | Sanitized release, deployment, task-pod, provenance, and coverage evidence |
| [`rhoai-kfp-qualification-baseline-1.0.schema.json`](../contracts/rhoai-kfp-qualification-baseline-1.0.schema.json) | Machine-readable fixture contract |
| [`rhoai_kfp_qualification.py`](../src/lineage_demo/rhoai_kfp_qualification.py) | Structural validator and release-gate report |
| [`capture-rhoai-kfp-qualification.sh`](../scripts/capture-rhoai-kfp-qualification.sh) | Read-only OpenShift capture and sanitization path |
| [`verify-rhoai-kfp-qualification.sh`](../scripts/verify-rhoai-kfp-qualification.sh) | Local schema, contract, fixture, and lint gate |
| [`verify-native-kfp-readiness.sh`](../scripts/verify-native-kfp-readiness.sh) | Local contract and launcher-prototype checks |
| [`adr-native-kfp-lineage-implementation-readiness.md`](adr-native-kfp-lineage-implementation-readiness.md) | Normative proposed context/root/outbox contract and C-01–C-14 matrix |

Run the local fixture gate with:

```bash
cd /Users/briangallagher/dev/workspaces/dr-lineage/sample-app-best-practices
UV_CACHE_DIR=/tmp/dr-lineage-uv-cache \
  uv run python -m lineage_demo.rhoai_kfp_qualification \
  examples/rhoai-kfp-qualification/rhoai-kfp-qualification-baseline-2026-09-30.json \
  --expect NO_GO
```

Regenerate a sanitized baseline only from an authenticated, read-only cluster
session with:

```bash
RHOAI_QUALIFICATION_OBSERVED_AT=2026-09-30 \
RHOAI_QUALIFICATION_OUTPUT=examples/rhoai-kfp-qualification/rhoai-kfp-qualification-baseline-2026-09-30.json \
./scripts/capture-rhoai-kfp-qualification.sh
```

The capture script records image references, command/flag names, and volume
presence only. It does not record environment values, pod command values that
may contain run/artifact data, Secret data, tokens, or logs.

## Owner decisions required

Please answer each item with an immutable release/source reference where
possible. `[Unknown]` means the current evidence pack does not answer it.

| ID | Question | Current state |
| --- | --- | --- |
| O-01 | Which component source commit/build inputs produced each DSPO, API-server, launcher, driver, and Argo digest in the installed OLM CSV? | [Unknown] |
| O-02 | Is `V2_LAUNCHER_IMAGE` an owner-supported RHOAI extension point for this release, or only an internal KFP compiler setting? | [Unknown] |
| O-03 | Which maintained repository owns the root reconciler, context resolver, and durable outbox? | [Unknown] |
| O-04 | Which supported configuration API persists the stable RHOAI deployment ID? | [Unknown] |
| O-05 | Is the context transport a projected file, an injected environment variable, a resolver API, or a first-class launcher interface? | [Proposed] local file/resolver shape; product mechanism [Unknown] |
| O-06 | What are the supported failure semantics for resolver outage, malformed context, retry, cancellation, and terminal-state replay? | [Proposed] local contract; product behavior [Unknown] |
| O-07 | What upgrade compatibility promise covers launcher flags, command ordering, exit status, and image ABI? | [Unknown] |

## Acceptance required to change the decision

The owner response must be followed by an implementation or supported
prototype that passes all gates below. A release-level image mapping alone is
not sufficient for native lineage support.

| Gate | Required evidence | Current result |
| --- | --- | --- |
| G-00 | OLM release manifest pins every supported image digest | [Observed] pass |
| G-01 | Owner-approved repository/API boundary and versioning policy | [Unknown] |
| G-02 | Root reconciler emits exact KFP-root lifecycle with durable idempotency | [Proposed] |
| G-03 | Task pod receives schema-valid context by exact root UUID | [Observed] blocked in current baseline |
| G-04 | Project authorization and secret-safety tests pass | [Proposed] |
| G-05 | Wrapper preserves launcher flags, retries, cancellation, and exit semantics | [Proposed] |
| G-06 | C-01–C-14 pass after each supported upgrade | [Proposed] |

The current local validator must continue to report `NO_GO` until G-01
through G-06 are evidenced. This prevents an image override or user-authored
task helper from being mistaken for a supported platform integration.

## Coverage and upgrade plan

The fixture records all C-01–C-14 entries. Local contract/prototype tests
exercise identity, lifecycle projection, retries, replay, context validation,
flag preservation, and secret safety. Native outbox outage, cross-project
authorization, and image-upgrade checks remain planned.

The four upgrade scenarios are:

1. **U-01 DSPO image:** verify the operator release mapping and DSPA
   reconciliation output.
2. **U-02 API-server image:** verify `V2_LAUNCHER_IMAGE`, absent/present
   command override semantics, and generated task-pod shape.
3. **U-03 launcher ABI:** verify all forwarded flags, retry/cancel behavior,
   exit status, and normal workload completion.
4. **U-04 context contract:** verify schema, authorization, missing-context,
   outage, replay, and secret-safety behavior.

Each upgrade run must preserve the previous fixture and add a new dated
baseline. A changed digest without a corresponding source/release mapping is
an automatic provenance gate failure.

## Evidence ledger

| Claim | Evidence state | Source and immutable revision | Verified | Caveat |
| --- | --- | --- | --- | --- |
| Installed RHOAI release pins the component images | [Observed] | `rhods-operator.3.6.0-ea.1` OLM CSV and checked-in baseline fixture | 2026-09-30 | Release-level mapping does not identify component source commits. |
| Packaged launcher uses the observed image and `launcher-v2` | [Observed] | Completed task pod captured by the read-only script | 2026-09-30 | One completed pod; upgrade behavior is not yet qualified. |
| Current task path lacks platform context injection | [Observed] | Sanitized API-server/task-pod deployment evidence | 2026-09-30 | Environment-specific observation. |
| Component source/build provenance | [Unknown] | OLM CSV and local source baselines | 2026-09-30 | Requires owner-provided build metadata or immutable source refs. |
| Native context/root/outbox implementation | [Proposed] | Local schemas and implementation-readiness ADR | 2026-09-30 | Not deployed or product-supported. |

No cluster mutation, deployment, fork, push, PR, or external owner message is
part of this handoff.
