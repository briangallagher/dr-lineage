"""Read-only import of live RHOAI evidence into the local product demo."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
from jsonschema import Draft202012Validator
from openlineage.client.serde import Serde

from lineage_demo.kfp_lineage import (
    KFPLineageContext,
    KFPOutboxRecord,
    KFPRootEventProjector,
    KFPRunSnapshot,
    KFPStateHistoryEntry,
)

SAFE_PARAMETER_NAMES = {
    "asset_name",
    "collection",
    "expected_asset_uuid",
    "project",
    "target_column",
}
UUID_PATTERN = re.compile(r"^[0-9a-fA-F-]{36}$")


class EvidenceImportError(RuntimeError):
    """Raised when the live evidence cannot be normalized safely."""


def _local_endpoint(url: str) -> str:
    parsed = urlparse(url)
    if parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
        raise EvidenceImportError("live importer accepts only a local port-forward endpoint")
    return url.rstrip("/")


def _get_json(
    url: str, *, verify: bool = False, headers: dict[str, str] | None = None
) -> dict[str, Any]:
    try:
        response = httpx.get(url, timeout=30, verify=verify, headers=headers)
        response.raise_for_status()
        payload = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise EvidenceImportError(f"read-only evidence request failed: {url}") from exc
    if not isinstance(payload, dict):
        raise EvidenceImportError(f"evidence response was not an object: {url}")
    return payload


def _post_json(
    url: str,
    body: dict[str, Any],
    *,
    verify: bool = False,
    headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    try:
        response = httpx.post(url, json=body, timeout=30, verify=verify, headers=headers)
        response.raise_for_status()
        payload = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise EvidenceImportError(f"evidence request failed: {url}") from exc
    if not isinstance(payload, dict):
        raise EvidenceImportError(f"evidence response was not an object: {url}")
    return payload


def _bearer_headers(token: str | None) -> dict[str, str] | None:
    return {"Authorization": f"Bearer {token}"} if token else None


def _sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, child in value.items():
            lowered = str(key).lower()
            if any(secret in lowered for secret in ("token", "password", "credential", "secret")):
                continue
            result[str(key)] = _sanitize(child)
        return result
    if isinstance(value, list):
        return [_sanitize(child) for child in value]
    return value


def _state_history(payload: dict[str, Any]) -> list[dict[str, str]]:
    return [
        {"state": str(item.get("state", "")), "eventTime": str(item.get("update_time", ""))}
        for item in payload.get("state_history", [])
        if item.get("state") and item.get("update_time")
    ]


def normalize_kfp_run(payload: dict[str, Any], *, api_url: str) -> dict[str, Any]:
    """Normalize only stable, non-secret KFP evidence fields."""

    run_id = str(payload.get("run_id", ""))
    if not UUID_PATTERN.fullmatch(run_id):
        raise EvidenceImportError("KFP response did not contain a UUID run_id")
    reference = payload.get("pipeline_version_reference") or {}
    pipeline_id = str(reference.get("pipeline_id", ""))
    version_id = str(reference.get("pipeline_version_id", ""))
    for label, value in (("pipeline_id", pipeline_id), ("pipeline_version_id", version_id)):
        if not UUID_PATTERN.fullmatch(value):
            raise EvidenceImportError(f"KFP response did not contain a UUID {label}")
    parameters = payload.get("runtime_config", {}).get("parameters", {})
    safe_parameters = {
        name: value for name, value in parameters.items() if name in SAFE_PARAMETER_NAMES
    }
    tasks = []
    for task in payload.get("run_details", {}).get("task_details", []):
        tasks.append(
            {
                "displayName": task.get("display_name"),
                "taskId": task.get("task_id"),
                "state": task.get("state"),
                "startTime": task.get("start_time"),
                "endTime": task.get("end_time"),
                "stateHistory": _state_history(task),
            }
        )
    return {
        "evidenceLevel": "VERIFIED_READ_ONLY",
        "capturedAt": datetime.now(UTC).isoformat(),
        "source": {"kind": "KFP_API", "endpoint": api_url},
        "runId": run_id,
        "displayName": payload.get("display_name"),
        "pipelineId": pipeline_id,
        "pipelineVersionId": version_id,
        "state": payload.get("state"),
        "createdAt": payload.get("created_at"),
        "scheduledAt": payload.get("scheduled_at"),
        "finishedAt": payload.get("finished_at"),
        "stateHistory": _state_history(payload),
        "runtimeParameters": safe_parameters,
        "tasks": tasks,
    }


def normalize_registry_asset(
    payload: dict[str, Any],
    *,
    endpoint: str,
    project: str | None = None,
    collection: str | None = None,
    name: str | None = None,
) -> dict[str, Any]:
    """Normalize registry metadata without carrying connection credentials."""

    asset_uuid = str(payload.get("uuid", ""))
    if not UUID_PATTERN.fullmatch(asset_uuid):
        raise EvidenceImportError("Data Registry response did not contain a UUID asset")
    return {
        "evidenceLevel": "VERIFIED_READ_ONLY",
        "source": {"kind": "DATA_REGISTRY_API", "endpoint": endpoint},
        "assetId": asset_uuid,
        "name": payload.get("name") or name,
        "collection": payload.get("collection") or collection,
        "project": payload.get("project") or project,
        "assetType": payload.get("asset_type"),
        "format": payload.get("format"),
        "location": payload.get("storage_location") or payload.get("location"),
        "owner": payload.get("owner"),
    }


def normalize_mlflow_run(
    payload: dict[str, Any],
    *,
    endpoint: str,
    root_run_id: str,
    artifacts: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Normalize a correlated MLflow run without carrying credentials or raw artifacts."""

    run = payload.get("run") if isinstance(payload.get("run"), dict) else payload
    info = run.get("info", {})
    data = run.get("data", {})
    tags = {
        str(item.get("key")): item.get("value")
        for item in data.get("tags", [])
        if isinstance(item, dict) and item.get("key") and item.get("value") is not None
    }
    if tags.get("kfp.root_run_id") != root_run_id:
        raise EvidenceImportError("MLflow run was not correlated to the requested KFP root run")

    params = {
        str(item.get("key")): item.get("value")
        for item in data.get("params", [])
        if isinstance(item, dict) and item.get("key") and item.get("value") is not None
    }
    metrics = [
        {
            "key": item.get("key"),
            "value": item.get("value"),
            "step": item.get("step"),
            "timestamp": item.get("timestamp"),
        }
        for item in data.get("metrics", [])
        if isinstance(item, dict) and item.get("key") is not None
    ]
    safe_tags = _sanitize(tags)
    return {
        "evidenceLevel": "VERIFIED_READ_ONLY",
        "source": {"kind": "MLFLOW_API", "endpoint": endpoint},
        "runId": info.get("run_id") or info.get("run_uuid"),
        "experimentId": info.get("experiment_id"),
        "runName": info.get("run_name"),
        "status": info.get("status"),
        "startTime": info.get("start_time"),
        "endTime": info.get("end_time"),
        "artifactUri": info.get("artifact_uri"),
        "metrics": _sanitize(metrics),
        "params": _sanitize(params),
        "tags": safe_tags,
        "artifacts": _sanitize(artifacts or {}),
    }


def _mlflow_headers(token: str | None, workspace: str | None) -> dict[str, str]:
    headers: dict[str, str] = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if workspace:
        headers["X-Mlflow-Workspace"] = workspace
    return headers


def normalize_marquez_events(
    payload: dict[str, Any], *, root_run_id: str, endpoint: str
) -> dict[str, Any]:
    events = payload.get("events", [])
    if not isinstance(events, list):
        raise EvidenceImportError("Marquez response did not contain an events list")

    def correlated(event: dict[str, Any]) -> bool:
        if event.get("run", {}).get("runId") == root_run_id:
            return True
        facets = event.get("run", {}).get("facets") or {}
        parent = facets.get("parent") or {}
        parent_run = parent.get("run") or {}
        parent_root = parent.get("root") or {}
        return (
            parent_run.get("runId") == root_run_id
            or (parent_root.get("run") or {}).get("runId") == root_run_id
        )

    selected = [
        _sanitize(event) for event in events if isinstance(event, dict) and correlated(event)
    ]
    return {
        "evidenceLevel": "VERIFIED_READ_ONLY",
        "source": {"kind": "MARQUEZ_API", "endpoint": endpoint},
        "eventCount": len(selected),
        "states": sorted({str(event.get("eventType")) for event in selected}),
        "jobs": sorted(
            {
                f"{event.get('job', {}).get('namespace', '')}/"
                f"{event.get('job', {}).get('name', '')}"
                for event in selected
            }
        ),
        "events": selected,
    }


def project_kfp_contract(
    kfp: dict[str, Any], *, deployment: str, project: str
) -> dict[str, Any]:
    """Project imported KFP state through the local rhoai-kfp:1.0 contract."""

    if not deployment or not project:
        raise EvidenceImportError("a deployment and project are required for KFP projection")
    state_history = tuple(
        KFPStateHistoryEntry(item["state"], item["eventTime"])
        for item in kfp.get("stateHistory", [])
        if item.get("state") and item.get("eventTime")
    )
    event_time = (
        state_history[-1].event_time
        if state_history
        else str(kfp.get("finishedAt") or kfp.get("capturedAt") or "")
    )
    context = KFPLineageContext(
        deployment=deployment,
        project=project,
        pipeline_id=kfp["pipelineId"],
        pipeline_version_id=kfp["pipelineVersionId"],
        pipeline_name=str(kfp.get("displayName") or ""),
        root_run_id=kfp["runId"],
    )
    snapshot = KFPRunSnapshot(
        run_id=kfp["runId"],
        pipeline_id=kfp["pipelineId"],
        pipeline_version_id=kfp["pipelineVersionId"],
        pipeline_name=context.pipeline_name,
        state=kfp["state"],
        event_time=event_time,
        state_history=state_history,
    )
    emissions = KFPRootEventProjector(context).observe(snapshot)
    outbox = tuple(
        KFPOutboxRecord.from_emission(emission, created_at=event_time).document()
        for emission in emissions
    )
    contracts_dir = Path(__file__).resolve().parents[2] / "contracts"
    context_validator = Draft202012Validator(
        json.loads(
            (contracts_dir / "rhoai-kfp-lineage-context-1.0.schema.json").read_text(
                encoding="utf-8"
            )
        )
    )
    root_validator = Draft202012Validator(
        json.loads(
            (contracts_dir / "rhoai-kfp-root-event-1.0.schema.json").read_text(encoding="utf-8")
        )
    )
    outbox_validator = Draft202012Validator(
        json.loads(
            (contracts_dir / "rhoai-kfp-lineage-outbox-1.0.schema.json").read_text(encoding="utf-8")
        )
    )
    context_document = context.document()
    root_documents = [json.loads(Serde.to_json(emission.event)) for emission in emissions]
    for label, validator, document in (
        ("context", context_validator, context_document),
        *[("root event", root_validator, item) for item in root_documents],
        *[("outbox", outbox_validator, item) for item in outbox],
    ):
        errors = list(validator.iter_errors(document))
        if errors:
            raise EvidenceImportError(f"local {label} contract validation failed")
    return {
        "evidenceLevel": "LOCAL_CONTRACT_PROJECTION",
        "profile": "rhoai-kfp:1.0",
        "context": context_document,
        "rootEvents": root_documents,
        "outbox": list(outbox),
        "limitations": [
            "This is a local projection of imported KFP state, not native KFP emission.",
            "The projected events are not sent to Marquez by the evidence importer.",
        ],
    }


def import_live_evidence(
    *,
    kfp_url: str,
    run_id: str,
    deployment: str | None = None,
    marquez_url: str | None = None,
    registry_url: str | None = None,
    registry_token: str | None = None,
    mlflow_url: str | None = None,
    mlflow_token: str | None = None,
    mlflow_workspace: str | None = None,
) -> dict[str, Any]:
    """Fetch a real run and optional related systems through local port-forwards."""

    if not UUID_PATTERN.fullmatch(run_id):
        raise EvidenceImportError("run_id must be a UUID")
    kfp_endpoint = _local_endpoint(kfp_url)
    kfp = normalize_kfp_run(
        _get_json(f"{kfp_endpoint}/apis/v2beta1/runs/{run_id}"), api_url=kfp_endpoint
    )
    parameters = kfp.get("runtimeParameters", {})
    evidence: dict[str, Any] = {
        "evidenceLevel": "VERIFIED_READ_ONLY",
        "summary": "Read-only evidence imported from a live RHOAI KFP run.",
        "runId": kfp["runId"],
        "pipelineId": kfp["pipelineId"],
        "pipelineVersionId": kfp["pipelineVersionId"],
        "state": kfp["state"],
        "kfp": kfp,
        "limitations": [
            "This evidence verifies reported KFP state and identity; it does not prove "
            "native KFP OpenLineage support.",
            "No cluster resources were changed by the importer.",
        ],
    }
    project = kfp.get("runtimeParameters", {}).get("project")
    if deployment and isinstance(project, str) and project:
        try:
            evidence["contractProjection"] = project_kfp_contract(
                kfp, deployment=deployment, project=project
            )
        except EvidenceImportError as exc:
            evidence["limitations"].append(f"Local KFP contract projection unavailable: {exc}")
    else:
        evidence["limitations"].append(
            "The live KFP state was not locally projected because deployment or project identity "
            "was unavailable."
        )
    asset_uuid = parameters.get("expected_asset_uuid")
    if asset_uuid and UUID_PATTERN.fullmatch(str(asset_uuid)):
        evidence["dataRegistry"] = {
            "evidenceLevel": "LINKED_FROM_KFP_PARAMETER",
            "assetId": asset_uuid,
            "project": parameters.get("project"),
            "collection": parameters.get("collection"),
            "name": parameters.get("asset_name"),
        }
    else:
        evidence["limitations"].append(
            "The KFP runtime parameters did not expose a governed asset UUID."
        )

    if registry_url:
        registry_endpoint = _local_endpoint(registry_url)
        project = parameters.get("project")
        collection = parameters.get("collection")
        name = parameters.get("asset_name")
        try:
            if not all(
                isinstance(value, str) and value for value in (project, collection, name)
            ):
                raise EvidenceImportError(
                    "KFP runtime parameters did not contain project, collection, and asset name"
                )
            registry_payload = _get_json(
                f"{registry_endpoint}/v1/{project}/namespaces/{collection}/generic-tables/{name}",
                headers=_bearer_headers(registry_token),
            )
            registry_asset = normalize_registry_asset(
                registry_payload,
                endpoint=registry_endpoint,
                project=project,
                collection=collection,
                name=name,
            )
            if asset_uuid and registry_asset["assetId"] != asset_uuid:
                raise EvidenceImportError(
                    "Data Registry UUID did not match the KFP expected_asset_uuid parameter"
                )
            evidence["dataRegistry"] = registry_asset
        except EvidenceImportError as exc:
            evidence["limitations"].append(f"Data Registry read unavailable: {exc}")
    else:
        evidence["limitations"].append(
            "Data Registry metadata was not queried; the asset identity is linked from a KFP "
            "parameter."
        )

    if marquez_url:
        marquez_endpoint = _local_endpoint(marquez_url)
        try:
            payload = _get_json(f"{marquez_endpoint}/api/v1/events/lineage?limit=10000&sort=asc")
            evidence["openLineage"] = normalize_marquez_events(
                payload, root_run_id=run_id, endpoint=marquez_endpoint
            )
        except EvidenceImportError as exc:
            evidence["limitations"].append(f"Marquez read unavailable: {exc}")

    if mlflow_url:
        mlflow_endpoint = _local_endpoint(mlflow_url)
        headers = _mlflow_headers(mlflow_token, mlflow_workspace or parameters.get("project"))
        try:
            experiments = _post_json(
                f"{mlflow_endpoint}/api/2.0/mlflow/experiments/search",
                {"max_results": 100},
                headers=headers,
            )
            experiment_ids = [
                str(item.get("experiment_id"))
                for item in experiments.get("experiments", [])
                if isinstance(item, dict) and item.get("experiment_id") is not None
            ]
            search = _post_json(
                f"{mlflow_endpoint}/api/2.0/mlflow/runs/search",
                {
                    "experiment_ids": experiment_ids,
                    "filter": f"tags.kfp.root_run_id = '{run_id}'",
                    "max_results": 100,
                },
                headers=headers,
            )
            runs = search.get("runs", [])
            if not isinstance(runs, list) or not runs:
                evidence["limitations"].append(
                    "No MLflow run was found with the requested KFP root-run correlation tag."
                )
            else:
                if len(runs) > 1:
                    evidence["limitations"].append(
                        f"Multiple MLflow runs ({len(runs)}) matched the KFP root-run "
                        "correlation tag."
                    )
                selected = runs[0]
                info = selected.get("info", {}) if isinstance(selected, dict) else {}
                mlflow_run_id = info.get("run_id") or info.get("run_uuid")
                artifacts = {}
                if mlflow_run_id:
                    try:
                        artifacts = _get_json(
                            f"{mlflow_endpoint}/api/2.0/mlflow/artifacts/list?run_id={mlflow_run_id}",
                            headers=headers,
                        )
                    except EvidenceImportError as exc:
                        evidence["limitations"].append(
                            f"MLflow artifact listing unavailable: {exc}"
                        )
                evidence["mlflow"] = normalize_mlflow_run(
                    selected, endpoint=mlflow_endpoint, root_run_id=run_id, artifacts=artifacts
                )
        except EvidenceImportError as exc:
            evidence["limitations"].append(f"MLflow read unavailable: {exc}")
    else:
        evidence["limitations"].append("MLflow evidence was not queried by this import.")

    return evidence


def write_evidence(evidence: dict[str, Any], output: Path) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return output
