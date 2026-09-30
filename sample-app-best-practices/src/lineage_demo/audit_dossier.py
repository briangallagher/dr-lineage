"""Build the product-facing audit dossier from replay and live evidence."""

from __future__ import annotations

from typing import Any


def _event_edges(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    edges: list[dict[str, Any]] = []
    for event in events:
        job = event.get("job", {})
        job_id = f"{job.get('namespace', '')}/{job.get('name', '')}"
        for dataset in event.get("inputs", []):
            edges.append(
                {
                    "direction": "input",
                    "dataset": f"{dataset.get('namespace', '')}/{dataset.get('name', '')}",
                    "job": job_id,
                    "eventType": event.get("eventType"),
                }
            )
        for dataset in event.get("outputs", []):
            edges.append(
                {
                    "direction": "output",
                    "dataset": f"{dataset.get('namespace', '')}/{dataset.get('name', '')}",
                    "job": job_id,
                    "eventType": event.get("eventType"),
                }
            )
    return edges


def _live_root_events(evidence: dict[str, Any]) -> list[dict[str, Any]]:
    openlineage = evidence.get("openLineage", {})
    return [
        event
        for event in openlineage.get("events", [])
        if isinstance(event, dict) and event.get("run", {}).get("runId") == evidence.get("runId")
    ]


def build_audit_dossier(document: dict[str, Any]) -> dict[str, Any]:
    """Return a stable, evidence-labelled product view of one demo pack."""

    evidence = document.get("verifiedEvidence", {})
    kfp = evidence.get("kfp", {})
    registry = evidence.get("dataRegistry", {})
    mlflow = evidence.get("mlflow", {})
    openlineage = evidence.get("openLineage", {})
    projection = evidence.get("contractProjection", {})
    root_events = _live_root_events(evidence)
    native_facet_observed = any(
        "rhoaiKfp" in event.get("run", {}).get("facets", {}) for event in root_events
    )
    scenarios = document.get("scenarios", [])
    happy = next((item for item in scenarios if item.scenario_id == "happy-path"), None)
    retry = next((item for item in scenarios if item.scenario_id == "failure-retry"), None)
    happy_events = list(happy.events) if happy else []
    retry_metadata = retry.metadata if retry else {}
    limitations = list(evidence.get("limitations", []))
    if root_events and not native_facet_observed:
        limitations.append(
            "Live Marquez root events did not contain the proposed rhoaiKfp native facet; "
            "the current evidence is adapter-owned, not proof of native KFP support."
        )

    systems = [
        {
            "system": "KFP",
            "status": "VERIFIED_READ_ONLY" if kfp else "LINKED",
            "facts": {
                "runId": evidence.get("runId"),
                "pipelineId": evidence.get("pipelineId"),
                "pipelineVersionId": evidence.get("pipelineVersionId"),
                "state": evidence.get("state"),
                "stateTransitions": len(kfp.get("stateHistory", [])),
                "taskCount": len(kfp.get("tasks", [])),
            },
        },
        {
            "system": "Data Registry",
            "status": registry.get("evidenceLevel", "NOT_QUERIED"),
            "facts": {
                "assetId": registry.get("assetId"),
                "project": registry.get("project"),
                "collection": registry.get("collection"),
                "name": registry.get("name"),
            },
        },
        {
            "system": "MLflow",
            "status": mlflow.get("evidenceLevel", "NOT_AVAILABLE"),
            "facts": {
                "runId": mlflow.get("runId"),
                "status": mlflow.get("status"),
                "metrics": mlflow.get("metrics", []),
                "artifactPaths": [
                    item.get("path")
                    for item in mlflow.get("artifacts", {}).get("files", [])
                    if isinstance(item, dict) and item.get("path")
                ],
            },
        },
        {
            "system": "Marquez/OpenLineage",
            "status": openlineage.get("evidenceLevel", "NOT_AVAILABLE"),
            "facts": {
                "eventCount": openlineage.get("eventCount", 0),
                "states": openlineage.get("states", []),
                "jobs": openlineage.get("jobs", []),
            },
        },
    ]

    return {
        "schemaVersion": "rhoai-audit-dossier:1.0",
        "title": "Governed asset to model audit dossier",
        "trustLevel": "VERIFIED_READ_ONLY_WITH_GAPS",
        "purpose": (
            "A product-level view that separates verified RHOAI observations, local demo "
            "behaviour, and the remaining native integration gaps."
        ),
        "subject": {
            "project": kfp.get("runtimeParameters", {}).get("project") or registry.get("project"),
            "asset": registry,
            "kfpRun": {
                "runId": evidence.get("runId"),
                "pipelineId": evidence.get("pipelineId"),
                "pipelineVersionId": evidence.get("pipelineVersionId"),
                "state": evidence.get("state"),
            },
            "mlflowRun": {
                "runId": mlflow.get("runId"),
                "status": mlflow.get("status"),
                "modelEvidence": mlflow.get("artifacts", {}).get("files", []),
            },
        },
        "systems": systems,
        "contracts": {
            "profile": document.get("contractProfile"),
            "context": "VALIDATED_ON_GENERATION",
            "rootEvent": "VALIDATED_ON_GENERATION",
            "outbox": "VALIDATED_ON_GENERATION",
            "liveProjection": projection.get("evidenceLevel", "NOT_AVAILABLE"),
            "liveProjectionRootEvents": len(projection.get("rootEvents", [])),
            "liveProjectionOutbox": len(projection.get("outbox", [])),
            "nativeKfpFacetObserved": native_facet_observed,
        },
        "liveTimeline": {
            "stateHistory": kfp.get("stateHistory", []),
            "openLineageRootEvents": [
                {"eventType": event.get("eventType"), "eventTime": event.get("eventTime")}
                for event in root_events
            ],
        },
        "lineage": {
            "fixtureEdges": _event_edges(happy_events),
            "ownership": (
                "KFP owns orchestration context and lifecycle; workload producers own "
                "the data edges and model output they actually observed."
            ),
        },
        "recovery": {
            "scenario": "failure-retry",
            "workloadOutcome": retry_metadata.get("scenarioStatus"),
            "retry": retry_metadata.get("retry"),
            "deliveryHistory": retry_metadata.get("deliveryHistory", []),
            "deliveryAndWorkloadAreIndependent": True,
        },
        "limitations": sorted(set(limitations)),
        "claims": [
            "The live KFP run identity and state history were read through the KFP API.",
            "The live MLflow run is correlated by the kfp.root_run_id tag when available.",
            "The Data Registry asset is linked by governed asset UUID, with metadata assurance "
            "shown separately.",
            "The proposed native KFP context/root-event/outbox profile is validated locally, "
            "not deployed into KFP.",
        ],
    }
