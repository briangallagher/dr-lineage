# ruff: noqa: E501

"""Build the decision layer for the stakeholder-facing lineage demonstrator."""

from __future__ import annotations

from typing import Any


def build_decision_package(document: dict[str, Any]) -> dict[str, Any]:
    """Return the product and architecture decision view over one demo pack.

    This deliberately sits above the event contracts.  It turns the same
    replay/live evidence into a stakeholder decision aid without implying that
    proposed capabilities are already implemented.
    """

    evidence = document.get("verifiedEvidence", {})
    kfp = evidence.get("kfp", {}) or {}
    registry = evidence.get("dataRegistry", {}) or {}
    mlflow = evidence.get("mlflow", {}) or {}
    openlineage = evidence.get("openLineage", {}) or {}
    native = document.get("nativeValidation", {})
    retry = next(
        (
            scenario.metadata
            for scenario in document.get("scenarios", [])
            if scenario.scenario_id == "failure-retry"
        ),
        {},
    )
    native_facet_observed = any(
        isinstance(event, dict)
        and "rhoaiKfp" in event.get("run", {}).get("facets", {})
        for event in openlineage.get("events", [])
    )

    live_run_id = evidence.get("runId") or kfp.get("runId")
    live_state = evidence.get("state") or kfp.get("state")
    live_task_count = len(kfp["tasks"]) if "tasks" in kfp else None
    live_event_count = (
        openlineage.get("eventCount") if "eventCount" in openlineage else None
    )

    return {
        "schemaVersion": "rhoai-lineage-decision-package:1.0",
        "title": "Decision-ready governed asset-to-model demonstrator",
        "objective": (
            "Give project management and architecture a coherent way to decide whether "
            "governed asset-to-model traceability is worth further investment."
        ),
        "audience": ["Project management", "Solution architecture", "Platform owners"],
        "productQuestion": (
            "Can a team discover which governed asset produced a model, understand the "
            "execution and evidence behind it, and explain recovery when something fails?"
        ),
        "baseline": {
            "fixtureContract": document.get("contractProfile"),
            "liveEvidenceLevel": evidence.get("evidenceLevel"),
            "liveRunId": live_run_id,
            "liveKfpState": live_state,
            "liveTaskCount": live_task_count,
            "liveRegistryAssetId": registry.get("assetId"),
            "liveMlflowRunId": mlflow.get("runId"),
            "liveOpenLineageEventCount": live_event_count,
            "nativeKfpFacetObserved": native_facet_observed,
        },
        "userJourneys": [
            {
                "id": "happy-path",
                "title": "Explain a governed model outcome",
                "question": "Which governed asset produced this candidate model?",
                "steps": [
                    "Find the registered asset and its location reference.",
                    "Follow the KFP root and task lifecycle.",
                    "Inspect workload-owned inputs, outputs, metrics, and model evidence.",
                    "Explain the result with explicit evidence and ownership boundaries.",
                ],
                "result": "A coherent asset-to-model explanation.",
                "evidence": "[Observed] deterministic fixture; live system identities where available.",
            },
            {
                "id": "recovery-path",
                "title": "Explain a recovered failure",
                "question": "What failed, what retried, and what was delivered?",
                "steps": [
                    "Show the failed source-resolution attempt and its error.",
                    "Show the successful retry as a distinct task attempt.",
                    "Show delivery failure separately from workload outcome.",
                    "Use the history to identify an operational owner and follow-up.",
                ],
                "result": "Recovery is visible without rewriting history.",
                "evidence": "[Observed] deterministic local recovery fixture; live retry not exercised.",
            },
            {
                "id": "decision-review",
                "title": "Decide what to invest in",
                "question": "Which capability is real, which is an adapter, and what should happen next?",
                "steps": [
                    "Review the capability matrix and evidence labels.",
                    "Walk the trust boundaries and ownership model.",
                    "Compare architecture options and their trade-offs.",
                    "Agree bounded discovery scope and exit criteria.",
                ],
                "result": "A shared investment decision instead of a production-readiness debate.",
                "evidence": "[Observed] live read-only baseline plus [Proposed] option analysis.",
            },
        ],
        "capabilityMatrix": [
            {
                "id": "CAP-01",
                "capability": "Governed asset identity",
                "state": "[Observed]",
                "owner": "Data Registry",
                "evidence": "Asset UUID, project, collection, name, format, and location are available.",
                "decisionImpact": "Keep the Registry as the governed starting point; do not copy data into lineage.",
            },
            {
                "id": "CAP-02",
                "capability": "Run and task explanation",
                "state": "[Observed] read-only + fixture",
                "owner": "KFP / orchestration",
                "evidence": "Live run identity/state history and local task contexts are both inspectable.",
                "decisionImpact": "A cross-system view needs a stable root identity and task hierarchy.",
            },
            {
                "id": "CAP-03",
                "capability": "Workload-owned data edges",
                "state": "[Observed] adapter-owned",
                "owner": "DCH / Spark / workload",
                "evidence": "The local path models input/output ownership; live task events contain adapter-owned edges.",
                "decisionImpact": "Adapters must report facts they observed; the orchestrator must not invent them.",
            },
            {
                "id": "CAP-04",
                "capability": "Model and evaluation explanation",
                "state": "[Observed]",
                "owner": "Training / MLflow",
                "evidence": "Live MLflow correlation includes metrics, tags, and artifact paths.",
                "decisionImpact": "Model outcome belongs beside lineage, not inside the Registry identity itself.",
            },
            {
                "id": "CAP-05",
                "capability": "Failure and recovery visibility",
                "state": "[Observed] local fixture",
                "owner": "Orchestration + delivery",
                "evidence": "Distinct failed/successful attempts and independent dead-letter delivery are replayable.",
                "decisionImpact": "Recovery is part of the user value, not a later operational add-on.",
            },
            {
                "id": "CAP-06",
                "capability": "Native context and root producer",
                "state": "[Unknown] not evidenced in inspected path",
                "owner": "RHOAI/KFP platform",
                "evidence": f"Native rhoaiKfp facet observed: {native_facet_observed}; local contract status: {native.get('status', 'UNKNOWN')}.",
                "decisionImpact": "Qualify the platform seam before claiming native support.",
            },
            {
                "id": "CAP-07",
                "capability": "Durable delivery and reconciliation",
                "state": "[Proposed] local model only",
                "owner": "Lineage platform / operations",
                "evidence": f"Local recovery includes {len(retry.get('deliveryHistory', []))} delivery states; live delivery was not exercised.",
                "decisionImpact": "Treat delivery as an owned product capability with retry, replay, and support semantics.",
            },
        ],
        "trustBoundaries": [
            {
                "boundary": "Registry metadata → workload read",
                "trustedFact": "The Registry supplies governed identity and a location reference.",
                "proofRequired": "The workload proves what it actually read; a URI alone is not immutable-byte evidence.",
                "owner": "Data Registry + workload",
            },
            {
                "boundary": "KFP control plane → task process",
                "trustedFact": "KFP supplies orchestration identity, lifecycle, and retry context.",
                "proofRequired": "A supported context-injection seam must be observable in the task runtime.",
                "owner": "RHOAI/KFP platform",
            },
            {
                "boundary": "Workload → lineage service",
                "trustedFact": "The workload reports input/output edges and model facts it observed.",
                "proofRequired": "Delivery, idempotency, authorization, and reconciliation must preserve those facts.",
                "owner": "Workload adapters + lineage platform",
            },
            {
                "boundary": "Evidence → decision",
                "trustedFact": "Observed, proposed, unknown, and historical claims are kept distinct.",
                "proofRequired": "Every investment decision names its baseline, owner, and exit criteria.",
                "owner": "Product + architecture",
            },
        ],
        "architectureOptions": [
            {
                "id": "OPTION-A",
                "name": "Native-first platform integration",
                "classification": "[Proposed]",
                "value": "Lowest adapter duplication if the platform owns context, root events, and delivery.",
                "tradeoff": "High dependency on an unverified supported launcher/context seam and platform ownership.",
                "recommendation": "Do not fund as the immediate implementation path until the seam is qualified.",
            },
            {
                "id": "OPTION-B",
                "name": "Adapter-led vertical slice with explicit contracts",
                "classification": "[Proposed]",
                "value": "Fastest route to a useful cross-system experience while keeping ownership explicit.",
                "tradeoff": "Requires adapters, delivery operations, and later migration if native support arrives.",
                "recommendation": "Recommended bounded discovery path for the next decision.",
            },
            {
                "id": "OPTION-C",
                "name": "Lineage view only, defer collection",
                "classification": "[Proposed]",
                "value": "Smallest investment and lowest platform dependency.",
                "tradeoff": "Does not answer the governed asset-to-model question reliably or support recovery ownership.",
                "recommendation": "Retain as a fallback, not the target product direction.",
            },
        ],
        "investmentRecommendation": {
            "decision": "FUND_BOUNDED_DISCOVERY",
            "headline": "Invest in a bounded cross-system demonstrator-to-architecture spike.",
            "rationale": (
                "The user value is visible and broad enough to matter, while the live baseline "
                "shows that native support and durable delivery remain unresolved."
            ),
            "scope": [
                "Keep the two user journeys: explain the model and explain recovery.",
                "Qualify one supported KFP/RHOAI context and root identity path.",
                "Define one workload adapter contract for asset, data edge, and model evidence.",
                "Exercise retry, delivery, replay, and authorization boundaries in a disposable environment.",
                "Return with an owner map, cost/risk view, and go/no-go recommendation.",
            ],
            "exitCriteria": [
                "A stakeholder can trace one governed asset to one model outcome.",
                "A failed attempt and recovered attempt remain distinguishable.",
                "Observed native, adapter-owned, and proposed behavior is explicit.",
                "The architecture owner accepts the context, delivery, and security boundaries.",
                "The next investment has a named owner and measurable success criteria.",
            ],
            "defer": [
                "Production HA, scale, retention, upgrade matrix, and full support qualification.",
                "Broad multi-engine adapter coverage.",
                "Claims of immutable source-byte reproducibility without a Registry revision contract.",
            ],
        },
    }
