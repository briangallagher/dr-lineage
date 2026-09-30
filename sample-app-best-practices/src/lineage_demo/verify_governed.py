"""Verify one governed KFP run across OpenLineage and MLflow read APIs.

This checks reported evidence and cross-system identity. It does not independently
rehash the source objects or prove that mutable source bytes are retained.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from collections import Counter
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from urllib.parse import urlparse

import kfp
from mlflow.tracking import MlflowClient

from lineage_demo.governed_model import (
    JOB_NAME,
    MAX_SOURCE_BYTES,
    MAX_SOURCE_OBJECTS,
    MODEL_ARTIFACT,
    ROOT_JOB_NAME,
)
from lineage_demo.identities import canonical_s3_dataset, root_run_id
from lineage_demo.verify import load_events

SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class GovernedContractError(AssertionError):
    """One or more reported asset-to-model relationships did not agree."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise GovernedContractError(message)


def _run_events(events: list[dict], *, namespace: str, name: str, run_id: str) -> list[dict]:
    return [
        event
        for event in events
        if event.get("job", {}).get("namespace") == namespace
        and event.get("job", {}).get("name") == name
        and event.get("run", {}).get("runId") == run_id
    ]


def _complete(events: list[dict], label: str) -> dict:
    states = {event.get("eventType") for event in events}
    _require(states == {"START", "COMPLETE"}, f"{label} lacks an unambiguous START/COMPLETE")
    return next(event for event in events if event.get("eventType") == "COMPLETE")


def _parameters(event: dict) -> dict[str, str]:
    entries = (
        event.get("run", {}).get("facets", {}).get("executionParameters", {}).get("parameters", [])
    )
    return {entry["key"]: entry["value"] for entry in entries}


def _artifact_json(client: MlflowClient, run_id: str, path: str) -> dict:
    with TemporaryDirectory(prefix="lineage-governed-verify-") as destination:
        downloaded = client.download_artifacts(run_id, path, dst_path=destination)
        value = json.loads(Path(downloaded).read_text(encoding="utf-8"))
    _require(isinstance(value, dict), f"MLflow artifact {path} is not a JSON object")
    return value


def verify_governed(
    *,
    result: dict[str, Any],
    events: list[dict],
    mlflow_client: MlflowClient,
    kfp_run: Any,
    kfp_run_id: str,
    pipeline_id: str,
    pipeline_version_id: str,
    cluster: str,
    project: str,
    expected_asset_uuid: str,
    expected_source_location: str,
) -> list[str]:
    """Fail closed if a reported run, lineage event, or artifact breaks the link."""
    checks: list[str] = []
    _require(result.get("asset_uuid") == expected_asset_uuid, "Result has the wrong asset UUID")
    _require(result.get("catalog_revision") is None, "Result invents a catalog revision")
    _require(result.get("assurance") == "OBSERVED", "Result overstates source assurance")
    for field in ("catalog_metadata_sha256", "source_manifest_sha256"):
        _require(bool(SHA256.fullmatch(str(result.get(field, "")))), f"Invalid {field}")
    checks.append("pinned-asset-and-observed-evidence")

    state = getattr(kfp_run, "state", None)
    state = getattr(state, "value", state)
    reference = getattr(kfp_run, "pipeline_version_reference", None)
    _require(getattr(kfp_run, "run_id", None) == kfp_run_id, "KFP run ID differs")
    _require(str(state).upper() == "SUCCEEDED", "KFP run is not successful")
    _require(
        (getattr(reference, "pipeline_id", None), getattr(reference, "pipeline_version_id", None))
        == (pipeline_id, pipeline_version_id),
        "KFP run is not linked to the expected pipeline version",
    )
    checks.append("kfp-terminal-state-and-pipeline-version")

    job_namespace = f"kfp://{cluster}/{project}"
    root_id = root_run_id(kfp_run_id)
    _complete(
        _run_events(events, namespace=job_namespace, name=ROOT_JOB_NAME, run_id=root_id),
        "KFP root",
    )
    checks.append("openlineage-root-lifecycle")

    child_id = str(result.get("openlineage_child_run_id", ""))
    child_events = _run_events(events, namespace=job_namespace, name=JOB_NAME, run_id=child_id)
    child = _complete(child_events, "governed child")
    for event in child_events:
        parent = event.get("run", {}).get("facets", {}).get("parent", {})
        _require(
            parent.get("job") == {"namespace": job_namespace, "name": ROOT_JOB_NAME}
            and parent.get("run", {}).get("runId") == root_id,
            "Governed child is not parented to the KFP root",
        )
    checks.append("openlineage-child-lifecycle-and-parent")

    parameters = _parameters(child)
    for key, value in (
        ("assetUuid", expected_asset_uuid),
        ("catalogMetadataSha256", result["catalog_metadata_sha256"]),
        ("sourceManifestSha256", result["source_manifest_sha256"]),
        ("mlflowRunId", result.get("mlflow_run_id")),
    ):
        _require(parameters.get(key) == value, f"Child run parameter {key} differs")
    checks.append("child-evidence-correlation")

    registered = canonical_s3_dataset(expected_source_location)
    logical = [
        item
        for item in child.get("inputs", [])
        if (item.get("namespace"), item.get("name"))
        == (f"dataregistry://{cluster}/{project}", expected_asset_uuid)
    ]
    _require(len(logical) == 1, "Child lacks the exact Registry asset input")
    symlinks = logical[0].get("facets", {}).get("symlinks", {}).get("identifiers", [])
    _require(
        {"namespace": registered.namespace, "name": registered.name, "type": "TABLE"} in symlinks,
        "Registry input does not link to the expected S3 location",
    )
    checks.append("logical-asset-and-location-link")

    inputs = child.get("inputs", [])
    physical = [item for item in inputs if item.get("namespace") == registered.namespace]
    _require(bool(physical), "Child has no physical Parquet object inputs")
    _require(len(inputs) == len(physical) + 1, "Child has unexpected additional data inputs")
    _require(len(physical) <= MAX_SOURCE_OBJECTS, "Child exceeds the source object limit")
    observed: Counter[tuple[str, int, int]] = Counter()
    names: set[str] = set()
    total_bytes = 0
    total_rows = 0
    for item in physical:
        name = item.get("name", "")
        _require(
            name == registered.name or name.startswith(registered.name.rstrip("/") + "/"),
            "Physical object is outside the registered S3 location",
        )
        _require(name not in names, "Child repeats a physical object input")
        names.add(name)
        version = item.get("facets", {}).get("version", {}).get("datasetVersion", "")
        _require(
            version.startswith("sha256:")
            and bool(SHA256.fullmatch(version.removeprefix("sha256:"))),
            "Physical input lacks an observed SHA-256 version",
        )
        statistics = item.get("inputFacets", {}).get("inputStatistics", {})
        size, row_count = statistics.get("size"), statistics.get("rowCount")
        _require(
            isinstance(size, int) and size > 0 and isinstance(row_count, int) and row_count > 0,
            "Physical input lacks source size or row-count evidence",
        )
        total_bytes += size
        total_rows += row_count
        observed[(version.removeprefix("sha256:"), size, row_count)] += 1
    _require(total_bytes <= MAX_SOURCE_BYTES, "Child exceeds the source byte limit")
    _require(total_rows >= 3, "Child lacks the minimum holdout candidate rows")
    checks.append("bounded-physical-object-evidence")

    mlflow_run_id = result.get("mlflow_run_id", "")
    output_name = f"{mlflow_run_id}/{MODEL_ARTIFACT}"
    _require(
        [(item.get("namespace"), item.get("name")) for item in child.get("outputs", [])]
        == [(f"mlflow://{cluster}/{project}", output_name)],
        "OpenLineage model output does not exactly identify the MLflow artifact",
    )
    checks.append("openlineage-model-output")

    run = mlflow_client.get_run(mlflow_run_id)
    _require(run.info.status == "FINISHED", "MLflow run is not finished")
    _require(run.data.tags.get("kfp.root_run_id") == root_id, "MLflow root tag differs")
    _require(run.data.tags.get("openlineage.child_run_id") == child_id, "MLflow child tag differs")
    _require(
        run.data.tags.get("data_registry.asset_uuid") == expected_asset_uuid,
        "MLflow asset tag differs",
    )
    _require(
        run.data.tags.get("source.manifest_sha256") == result["source_manifest_sha256"],
        "MLflow source-manifest tag differs",
    )
    _require(
        run.data.params.get("catalog_metadata_sha256") == result["catalog_metadata_sha256"],
        "MLflow catalog-metadata parameter differs",
    )
    _require(
        result.get("model_artifact_uri") == f"{run.info.artifact_uri.rstrip('/')}/{MODEL_ARTIFACT}",
        "Result points to a different MLflow artifact URI",
    )
    checks.append("mlflow-run-and-cross-system-tags")

    model = _artifact_json(mlflow_client, mlflow_run_id, MODEL_ARTIFACT)
    _require(
        model.get("algorithm") == "majority-classifier" and bool(model.get("predicted_label")),
        "MLflow model artifact is not the reported baseline",
    )
    evaluation = _artifact_json(mlflow_client, mlflow_run_id, "evaluation/candidate.json")
    _require(evaluation == result.get("evaluation"), "MLflow evaluation artifact differs")
    _require(evaluation.get("kind") == "candidate", "Evaluation is not marked as a candidate")
    for name in ("train_accuracy", "holdout_accuracy"):
        _require(
            name in run.data.metrics
            and math.isclose(run.data.metrics[name], evaluation[name], rel_tol=0, abs_tol=1e-9),
            f"MLflow {name} metric differs from the evaluation artifact",
        )
    checks.append("mlflow-model-and-candidate-evaluation")

    evidence = _artifact_json(mlflow_client, mlflow_run_id, "evidence/observed-source.json")
    for key, value in (
        ("asset_uuid", expected_asset_uuid),
        ("catalog_metadata_sha256", result["catalog_metadata_sha256"]),
        ("source_manifest_sha256", result["source_manifest_sha256"]),
    ):
        _require(evidence.get(key) == value, f"MLflow evidence {key} differs")
    source_objects = evidence.get("objects", [])
    _require(isinstance(source_objects, list), "MLflow source evidence is not an object list")
    artifact_observed = Counter(
        (item.get("sha256"), item.get("size"), item.get("row_count")) for item in source_objects
    )
    _require(artifact_observed == observed, "MLflow and OpenLineage source observations differ")
    checks.append("source-evidence-artifact-matches-lineage")
    return checks


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result-json", required=True)
    parser.add_argument("--kfp-endpoint", required=True)
    parser.add_argument("--kfp-run-id", required=True)
    parser.add_argument("--pipeline-id", required=True)
    parser.add_argument("--pipeline-version-id", required=True)
    parser.add_argument("--marquez-url", required=True)
    parser.add_argument("--mlflow-tracking-uri", required=True)
    parser.add_argument("--cluster", required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--expected-asset-uuid", required=True)
    parser.add_argument("--expected-source-location", required=True)
    args = parser.parse_args()
    endpoint = urlparse(args.kfp_endpoint)
    if (
        endpoint.scheme not in {"http", "https"}
        or not endpoint.hostname
        or endpoint.username
        or endpoint.password
        or endpoint.query
        or endpoint.fragment
        or endpoint.path not in {"", "/"}
    ):
        parser.error("KFP endpoint must be an HTTP(S) origin without credentials or a path")
    if endpoint.scheme == "http" and endpoint.hostname not in {"127.0.0.1", "localhost"}:
        parser.error("Plain HTTP KFP access is allowed only through a localhost port-forward")
    token = sys.stdin.readline().strip()
    if not token:
        parser.error("Pipe a KFP bearer token on stdin; it is never printed or stored")
    client = kfp.Client(
        host=args.kfp_endpoint,
        existing_token=token,
        namespace=args.project,
        verify_ssl=not (
            endpoint.scheme == "https" and endpoint.hostname in {"127.0.0.1", "localhost"}
        ),
    )
    result = json.loads(Path(args.result_json).read_text(encoding="utf-8"))
    checks = verify_governed(
        result=result,
        events=load_events(args.marquez_url),
        mlflow_client=MlflowClient(tracking_uri=args.mlflow_tracking_uri),
        kfp_run=client.get_run(args.kfp_run_id),
        kfp_run_id=args.kfp_run_id,
        pipeline_id=args.pipeline_id,
        pipeline_version_id=args.pipeline_version_id,
        cluster=args.cluster,
        project=args.project,
        expected_asset_uuid=args.expected_asset_uuid,
        expected_source_location=args.expected_source_location,
    )
    print(json.dumps({"status": "passed", "checks": checks}, indent=2))


if __name__ == "__main__":
    main()
