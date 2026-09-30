"""Integrated, local-only checkpoint for the proposed native KFP lineage path."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from lineage_demo.kfp_integration_readiness import build_launcher_context_plan
from lineage_demo.kfp_lineage import (
    KFPLineageContext,
    KFPOutboxRecord,
)

VALIDATION_PROFILE = "rhoai-native-kfp-poc-validation"
VALIDATION_VERSION = "1.0"
RESOLVER_URL = "https://lineage-resolver.example.invalid"


def _root_context(task_context: KFPLineageContext) -> KFPLineageContext:
    return KFPLineageContext(
        deployment=task_context.deployment,
        project=task_context.project,
        pipeline_id=task_context.pipeline_id,
        pipeline_version_id=task_context.pipeline_version_id,
        pipeline_name=task_context.pipeline_name,
        root_run_id=task_context.root_run_id,
    )


def _check(
    check_id: str,
    area: str,
    passed: bool,
    evidence: str,
    limitation: str,
) -> dict[str, str]:
    return {
        "id": check_id,
        "area": area,
        "status": "PASS" if passed else "FAIL",
        "evidence": evidence,
        "limitation": limitation,
    }


def build_native_path_validation(document: dict[str, Any]) -> dict[str, Any]:
    """Validate the four native-path seams against the deterministic POC payload."""

    scenarios = document["scenarios"]
    happy = next(item for item in scenarios if item.scenario_id == "happy-path")
    retry = next(item for item in scenarios if item.scenario_id == "failure-retry")

    root_events = [
        event
        for event in happy.events
        if event["job"]["namespace"].startswith("kfp://")
        and not event["inputs"]
        and not event["outputs"]
    ]
    root_types = [event["eventType"] for event in root_events]
    root_ids = {event["run"]["runId"] for event in root_events}
    root_facets = [event["run"]["facets"].get("rhoaiKfp", {}) for event in root_events]
    outbox_records = [KFPOutboxRecord.from_document(item) for item in happy.outbox]
    root_lifecycle_passed = (
        root_types == ["START", "RUNNING", "COMPLETE"]
        and len(root_ids) == 1
        and len(outbox_records) == 3
        and all(facet.get("rootRunId") == happy.metadata["rootRunId"] for facet in root_facets)
    )

    parsed_task_contexts = [KFPLineageContext.from_document(item) for item in happy.contexts]
    root_context = _root_context(parsed_task_contexts[0])
    launcher_plan = build_launcher_context_plan(root_context, resolver_url=RESOLVER_URL)
    context_injection_passed = (
        all(
            context.task_name and context.parent_run_id == context.root_run_id
            for context in parsed_task_contexts
        )
        and launcher_plan.lookup_url.endswith(root_context.root_run_id)
        and launcher_plan.runtime_environment()["RHOAI_LINEAGE_PROFILE_VERSION"] == "1.0"
    )

    resolve_events = [
        event
        for event in retry.events
        if event["job"]["name"] == "resolve-and-stage-data"
    ]
    resolve_attempts = {
        event["run"]["facets"]["rhoaiKfp"]["task"]["attempt"]: event
        for event in resolve_events
        if event["eventType"] in {"FAIL", "COMPLETE"}
    }
    retry_passed = (
        resolve_attempts[1]["eventType"] == "FAIL"
        and resolve_attempts[2]["eventType"] == "COMPLETE"
        and resolve_attempts[1]["run"]["runId"] != resolve_attempts[2]["run"]["runId"]
        and resolve_attempts[1]["run"]["facets"]["rhoaiKfp"]["rootRunId"]
        == resolve_attempts[2]["run"]["facets"]["rhoaiKfp"]["rootRunId"]
    )

    model_events = [
        event
        for event in happy.events
        if event["job"]["name"] == "train-and-evaluate-model"
        and event["eventType"] == "COMPLETE"
    ]
    model_event = model_events[0] if model_events else {}
    model_output = model_event.get("outputs", [{}])[0]
    model_facets = model_output.get("facets", {})
    evaluation = model_event.get("run", {}).get("facets", {}).get("rhoaiEvaluation", {})
    workload_edge_passed = (
        bool(model_event.get("inputs"))
        and bool(model_event.get("outputs"))
        and model_facets.get("rhoaiModelLink", {}).get("runId")
        and evaluation.get("decision") == "CANDIDATE"
        and {metric["name"] for metric in evaluation.get("metrics", [])}
        >= {"holdoutAccuracy", "trainingRows"}
    )

    checks = [
        _check(
            "NATIVE-01",
            "Root lifecycle",
            root_lifecycle_passed,
            "KFPRootEventProjector + rhoai-kfp:1.0 outbox fixture",
            "The projector is local and process-scoped; a native reconciler and durable store "
            "remain unimplemented.",
        ),
        _check(
            "NATIVE-02",
            "Task context injection",
            context_injection_passed,
            "KFPLineageContext + KFPLauncherContextPlan fixture",
            "The resolver URL is a non-routable fixture value; no launcher or resolver was "
            "deployed.",
        ),
        _check(
            "NATIVE-03",
            "Retry identity",
            retry_passed,
            "Failure/retry scenario with distinct task run IDs and attempts",
            "Retry semantics are replayed locally; KFP retry observation is not native evidence.",
        ),
        _check(
            "NATIVE-04",
            "Workload edge and model evaluation",
            workload_edge_passed,
            "Training COMPLETE event with input/output, model link, and evaluation facet",
            "The edge and model facts are fixture/workload evidence, not first-party RHOAI "
            "emission.",
        ),
    ]
    status = "PASS" if all(check["status"] == "PASS" for check in checks) else "FAIL"
    return {
        "profile": VALIDATION_PROFILE,
        "version": VALIDATION_VERSION,
        "status": status,
        "evidenceState": "OBSERVED_LOCAL_CONTRACT_FIXTURE",
        "checks": checks,
        "recommendation": "GO_FOR_NARROW_NATIVE_SPIKE" if status == "PASS" else "HOLD_FOR_FIX",
        "notProductionEvidence": True,
        "scope": [
            "No cluster or external service calls",
            "No OpenLineage emission",
            "Deterministic fixture and versioned local contracts",
        ],
        "limitations": [
            "A passing local fixture validates the proposed shape, not native KFP/RHOAI support.",
            "Native deployment ownership, supported launcher injection, and durable outbox "
            "behavior remain unknown.",
            "Data Registry revision/reproducibility and exact source-byte evidence remain "
            "outside this checkpoint.",
        ],
    }


def write_native_path_validation(
    validation: dict[str, Any], output_dir: Path
) -> dict[str, str]:
    """Write the sanitized local validation result without contacting a service."""

    output_dir.mkdir(parents=True, exist_ok=True)
    output = output_dir / "native-path-validation.json"
    output.write_text(json.dumps(validation, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"validation": str(output)}
