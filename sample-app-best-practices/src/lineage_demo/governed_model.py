"""Local Slice 2 adapter: governed Parquet asset to a tracked baseline model.

The adapter reports only facts it observes. A metadata response digest is not a
Data Registry revision, and a digest of bytes read does not imply their retention.
"""

from __future__ import annotations

import hashlib
import io
import json
import re
import uuid
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
import pyarrow.parquet as parquet
from mlflow.tracking import MlflowClient
from openlineage.client.event_v2 import RunState

from lineage_demo import facets
from lineage_demo.config import Settings
from lineage_demo.events import LineageEmitter, input_dataset, output_dataset
from lineage_demo.identities import (
    DatasetIdentity,
    canonical_s3_dataset,
    parse_s3_uri,
    root_run_id,
    uuid7,
)
from lineage_demo.processing import _job_facets
from lineage_demo.storage import s3_client

JOB_NAME = "train-governed-baseline"
ROOT_JOB_NAME = "governed-asset-to-model"
MODEL_ARTIFACT = "model/majority-classifier.json"
MAX_SOURCE_BYTES = 64 * 1024 * 1024
MAX_SOURCE_OBJECTS = 16
IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")


@dataclass(frozen=True)
class AssetReference:
    project: str
    collection: str
    name: str
    expected_uuid: str

    def __post_init__(self) -> None:
        for value in (self.project, self.collection, self.name):
            if value in {".", ".."} or not IDENTIFIER.fullmatch(value):
                raise ValueError("Asset reference contains an invalid identifier")
        uuid.UUID(self.expected_uuid)


@dataclass(frozen=True)
class ResolvedAsset:
    reference: AssetReference
    uuid: str
    location: str
    connection_secret: str
    metadata_sha256: str
    response_etag: str | None
    owner: str | None

    def logical_identity(self, cluster_name: str) -> DatasetIdentity:
        return DatasetIdentity(
            f"dataregistry://{cluster_name}/{self.reference.project}",
            self.uuid,
        )

    @property
    def physical_identity(self) -> DatasetIdentity:
        return canonical_s3_dataset(self.location)


@dataclass(frozen=True)
class ObservedObject:
    identity: DatasetIdentity
    sha256: str
    size: int
    row_count: int
    etag: str | None
    version_id: str | None

    def manifest_record(self) -> dict[str, Any]:
        return {
            "namespace": self.identity.namespace,
            "name": self.identity.name,
            "sha256": self.sha256,
            "size": self.size,
            "rowCount": self.row_count,
            "etag": self.etag,
            "versionId": self.version_id,
        }


def _digest(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _validate_registry_endpoint(registry_url: str) -> None:
    parsed = urlparse(registry_url)
    if (
        not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or parsed.path not in ("", "/")
        or not (
            parsed.scheme == "https"
            or (parsed.scheme == "http" and parsed.hostname in {"127.0.0.1", "localhost"})
        )
    ):
        raise ValueError("Data Registry must use a TLS endpoint or a localhost test forward")


def resolve_asset(
    client: httpx.Client,
    registry_url: str,
    reference: AssetReference,
    token: str,
    source_secret_name: str,
) -> ResolvedAsset:
    _validate_registry_endpoint(registry_url)
    if not token:
        raise ValueError("A Data Registry bearer token is required")
    response = client.get(
        f"{registry_url.rstrip('/')}/v1/{reference.project}/namespaces/"
        f"{reference.collection}/generic-tables/{reference.name}",
        headers={
            "Authorization": f"Bearer {token}",
            "X-Namespace": reference.project,
        },
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Data Registry did not return an asset object")
    asset_uuid = str(uuid.UUID(payload["uuid"]))
    if asset_uuid != str(uuid.UUID(reference.expected_uuid)):
        raise ValueError("Data Registry asset UUID differs from the pinned reference")
    if payload.get("name") != reference.name or payload.get("collection") != reference.collection:
        raise ValueError("Data Registry response differs from the requested asset")
    if payload.get("asset_type") not in (None, "table") or payload.get("format") != "parquet":
        raise ValueError("The governed baseline accepts only a Parquet table asset")
    location = payload.get("storage_location") or payload.get("location")
    if not isinstance(location, str):
        raise ValueError("The asset has no direct S3 storage location")
    if (
        payload.get("storage_location")
        and payload.get("location")
        and payload["storage_location"] != payload["location"]
    ):
        raise ValueError("Registry storage location fields disagree")
    canonical_s3_dataset(location)
    connection = payload.get("connection_ref")
    if not isinstance(connection, dict) or connection.get("type") != "rhai":
        raise ValueError("The asset must use a same-project RHOAI Data Connection")
    if connection.get("secret_name") != source_secret_name:
        raise ValueError("Mounted Data Connection does not match the registry asset")
    return ResolvedAsset(
        reference=reference,
        uuid=asset_uuid,
        location=location,
        connection_secret=source_secret_name,
        metadata_sha256=_digest(payload),
        response_etag=response.headers.get("etag"),
        owner=payload.get("owner"),
    )


def read_parquet_source(
    s3: Any,
    asset: ResolvedAsset,
    target_column: str,
) -> tuple[list[str], list[ObservedObject]]:
    if not IDENTIFIER.fullmatch(target_column):
        raise ValueError("Invalid target column")
    bucket, prefix = parse_s3_uri(asset.location)
    exact_file = prefix.endswith(".parquet")
    boundary = prefix if exact_file else prefix.rstrip("/") + "/"
    paginator = s3.get_paginator("list_objects_v2")
    keys: list[str] = []
    listed_size = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=boundary):
        for item in page.get("Contents", []):
            key = item["Key"]
            if not key.endswith(".parquet") or not key.startswith(boundary):
                continue
            if exact_file and key != boundary:
                continue
            keys.append(key)
            listed_size += item.get("Size", 0)
            if len(keys) > MAX_SOURCE_OBJECTS or listed_size > MAX_SOURCE_BYTES:
                raise ValueError("Parquet source exceeds the bounded demo reader limit")
    if not keys:
        raise ValueError("No Parquet objects found at the registered location")
    values: list[str] = []
    evidence: list[ObservedObject] = []
    bytes_read = 0
    for key in sorted(keys):
        response = s3.get_object(Bucket=bucket, Key=key)
        body = response["Body"]
        try:
            payload = body.read(MAX_SOURCE_BYTES - bytes_read + 1)
        finally:
            body.close()
        bytes_read += len(payload)
        if bytes_read > MAX_SOURCE_BYTES or len(payload) != response["ContentLength"]:
            raise ValueError("Parquet source changed or exceeded the bounded reader limit")
        table = parquet.read_table(io.BytesIO(payload), columns=[target_column])
        labels = table.column(target_column).to_pylist()
        if any(value is None or not isinstance(value, str) or not value for value in labels):
            raise ValueError("Baseline target must contain nonempty string labels")
        values.extend(labels)
        evidence.append(
            ObservedObject(
                identity=DatasetIdentity(f"s3://{bucket}", key),
                sha256=hashlib.sha256(payload).hexdigest(),
                size=len(payload),
                row_count=len(labels),
                etag=response.get("ETag"),
                version_id=response.get("VersionId"),
            )
        )
    if len(values) < 3:
        raise ValueError("At least three source rows are needed for a holdout candidate")
    return values, evidence


def train_majority_candidate(labels: list[str]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Fit a constant classifier with one deterministic holdout row."""
    ordered = sorted(labels, key=lambda value: (hashlib.sha256(value.encode()).hexdigest(), value))
    training, holdout = ordered[:-1], ordered[-1]
    counts = Counter(training)
    majority = sorted(counts, key=lambda label: (-counts[label], label))[0]
    model = {"algorithm": "majority-classifier", "predicted_label": majority}
    evaluation = {
        "kind": "candidate",
        "train_rows": len(training),
        "holdout_rows": 1,
        "train_accuracy": sum(value == majority for value in training) / len(training),
        "holdout_accuracy": float(holdout == majority),
    }
    return model, evaluation


def log_mlflow_candidate(
    tracking_uri: str,
    asset: ResolvedAsset,
    root_id: str,
    child_id: str,
    target_column: str,
    model: dict[str, Any],
    evaluation: dict[str, Any],
    objects: list[ObservedObject],
) -> tuple[str, str]:
    if not tracking_uri:
        raise ValueError("MLflow tracking URI is required")
    client = MlflowClient(tracking_uri=tracking_uri)
    experiment_name = "lineage-governed-asset-baseline"
    experiment = client.get_experiment_by_name(experiment_name)
    experiment_id = (
        experiment.experiment_id if experiment else client.create_experiment(experiment_name)
    )
    manifest_sha256 = _digest([item.manifest_record() for item in objects])
    run = client.create_run(
        experiment_id,
        tags={
            "kfp.root_run_id": root_id,
            "openlineage.child_run_id": child_id,
            "data_registry.project": asset.reference.project,
            "data_registry.asset_uuid": asset.uuid,
            "source.manifest_sha256": manifest_sha256,
        },
    )
    try:
        client.log_param(run.info.run_id, "algorithm", model["algorithm"])
        client.log_param(run.info.run_id, "target_column", target_column)
        client.log_param(run.info.run_id, "catalog_metadata_sha256", asset.metadata_sha256)
        for name in ("train_accuracy", "holdout_accuracy"):
            client.log_metric(run.info.run_id, name, evaluation[name])
        client.log_dict(run.info.run_id, model, MODEL_ARTIFACT)
        client.log_dict(run.info.run_id, evaluation, "evaluation/candidate.json")
        # Object locations stay in the lineage input identities. The MLflow
        # artifact records observed bytes without copying source locations.
        client.log_dict(
            run.info.run_id,
            {
                "asset_uuid": asset.uuid,
                "catalog_metadata_sha256": asset.metadata_sha256,
                "source_manifest_sha256": manifest_sha256,
                "objects": [
                    {"sha256": item.sha256, "size": item.size, "row_count": item.row_count}
                    for item in objects
                ],
            },
            "evidence/observed-source.json",
        )
        client.set_terminated(run.info.run_id, status="FINISHED")
    except BaseException:
        client.set_terminated(run.info.run_id, status="FAILED")
        raise
    return run.info.run_id, f"{run.info.artifact_uri.rstrip('/')}/{MODEL_ARTIFACT}"


def _input_datasets(asset: ResolvedAsset, objects: list[ObservedObject], cluster_name: str) -> list:
    physical = asset.physical_identity
    logical = asset.logical_identity(cluster_name)
    logical_facets = {
        "dataSource": facets.data_source("RHOAI Data Registry", logical.namespace),
        "symlinks": facets.symlinks(physical.namespace, physical.name, "TABLE"),
    }
    if asset.owner:
        logical_facets["ownership"] = facets.dataset_ownership(asset.owner)
    datasets = [input_dataset(logical, logical_facets)]
    for item in objects:
        datasets.append(
            input_dataset(
                item.identity,
                {
                    "storage": facets.storage("S3", "PARQUET"),
                    "version": {
                        **facets.base("DatasetVersionDatasetFacet.json"),
                        "datasetVersion": f"sha256:{item.sha256}",
                    },
                },
                {
                    "inputStatistics": {
                        **facets.base("InputStatisticsInputDatasetFacet.json"),
                        "rowCount": item.row_count,
                        "size": item.size,
                        "fileCount": 1,
                    }
                },
            )
        )
    return datasets


def run_governed_training(
    *,
    settings: Settings,
    registry_url: str,
    registry_ca_file: str,
    token_file: str,
    reference: AssetReference,
    source_secret_name: str,
    source_bucket: str,
    target_column: str,
    tracking_uri: str,
    pipeline_job_id: str,
    pod_name: str,
    emitter: LineageEmitter | None = None,
    registry_client: httpx.Client | None = None,
    s3: Any = None,
) -> dict[str, Any]:
    if not pipeline_job_id or not pod_name:
        raise ValueError("KFP run ID and pod name are required")
    if reference.project != settings.project_namespace:
        raise ValueError("The KFP workload must run in the asset's project")
    _validate_registry_endpoint(registry_url)
    token = Path(token_file).read_text(encoding="utf-8").strip()
    if not token:
        raise ValueError("Projected service account token is empty")
    if not Path(registry_ca_file).is_file():
        raise ValueError("Data Registry service CA is missing")
    emitter = emitter or LineageEmitter(settings)
    root_id = root_run_id(pipeline_job_id)
    child_id = str(uuid7())
    run_facets = {
        "parent": facets.parent_run(
            parent_namespace=settings.kfp_namespace,
            parent_name=ROOT_JOB_NAME,
            parent_run_id=root_id,
            root_namespace=settings.kfp_namespace,
            root_name=ROOT_JOB_NAME,
            root_run_id=root_id,
        ),
        "executionParameters": facets.execution_parameters(
            {"assetProject": reference.project, "kfpPodName": pod_name}
        ),
    }
    job_facets = _job_facets(
        integration="KFP",
        job_type_name="JOB",
        description="Read a governed Parquet asset and train a tracked baseline candidate.",
        path="src/lineage_demo/governed_model.py",
    )
    emitter.run_event(
        state=RunState.START,
        run_id=child_id,
        job_namespace=settings.kfp_namespace,
        job_name=JOB_NAME,
        run_facets=run_facets,
        job_facets=job_facets,
    )
    phase = "registry"
    try:
        if registry_client is None:
            with httpx.Client(verify=registry_ca_file, timeout=20, trust_env=False) as client:
                asset = resolve_asset(client, registry_url, reference, token, source_secret_name)
        else:
            asset = resolve_asset(
                registry_client, registry_url, reference, token, source_secret_name
            )
        phase = "source"
        registered_bucket, _ = parse_s3_uri(asset.location)
        if not source_bucket:
            raise ValueError("Mounted Data Connection bucket is missing")
        if registered_bucket != source_bucket:
            raise ValueError("Registry location bucket differs from mounted Data Connection")
        labels, objects = read_parquet_source(s3 or s3_client(settings), asset, target_column)
        model, evaluation = train_majority_candidate(labels)
        phase = "mlflow"
        mlflow_run_id, artifact_uri = log_mlflow_candidate(
            tracking_uri, asset, root_id, child_id, target_column, model, evaluation, objects
        )
        manifest_sha256 = _digest([item.manifest_record() for item in objects])
        completed_facets = {
            **run_facets,
            "executionParameters": facets.execution_parameters(
                {
                    "assetUuid": asset.uuid,
                    "catalogMetadataSha256": asset.metadata_sha256,
                    "kfpPodName": pod_name,
                    "mlflowRunId": mlflow_run_id,
                    "sourceManifestSha256": manifest_sha256,
                }
            ),
        }
        output_identity = DatasetIdentity(
            f"mlflow://{settings.cluster_name}/{reference.project}",
            f"{mlflow_run_id}/{MODEL_ARTIFACT}",
        )
        phase = "lineage"
        emitter.run_event(
            state=RunState.COMPLETE,
            run_id=child_id,
            job_namespace=settings.kfp_namespace,
            job_name=JOB_NAME,
            run_facets=completed_facets,
            job_facets=job_facets,
            inputs=_input_datasets(asset, objects, settings.cluster_name),
            outputs=[output_dataset(output_identity)],
        )
        return {
            "asset_uuid": asset.uuid,
            "catalog_metadata_sha256": asset.metadata_sha256,
            "catalog_revision": None,
            "source_manifest_sha256": manifest_sha256,
            "openlineage_child_run_id": child_id,
            "mlflow_run_id": mlflow_run_id,
            "model_artifact_uri": artifact_uri,
            "evaluation": evaluation,
            "assurance": "OBSERVED",
        }
    except BaseException as exc:
        terminal = (
            RunState.ABORT if isinstance(exc, (KeyboardInterrupt, SystemExit)) else RunState.FAIL
        )
        emitter.run_event(
            state=terminal,
            run_id=child_id,
            job_namespace=settings.kfp_namespace,
            job_name=JOB_NAME,
            run_facets={
                **run_facets,
                "errorMessage": facets.error_message(
                    f"{phase} phase failed ({type(exc).__name__}); inspect authorized workload logs"
                ),
            },
            job_facets=job_facets,
        )
        raise
